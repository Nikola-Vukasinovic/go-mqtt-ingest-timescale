package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	//Runtime vars
	var logger *zap.Logger
	//var dbPool *pgxpool.Pool
	//var ctx *context.Context
	//Buffer channel for MQTT messages
	//TODO Make buffer size conf from env
	MSG_BUFF_SIZE := 67108864 //64 MB
	var messageBuffer = make(chan []byte, MSG_BUFF_SIZE)
	//Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Use a preset configuration for the logger
	var err error
	var topic string

	// Configure the logger with a timestamp
	logConfig := zap.NewProductionConfig()
	logConfig.EncoderConfig.TimeKey = "time"
	logConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err = logConfig.Build()

	if err != nil {
		panic("Failed to create logger")
	}
	defer logger.Sync() // Flush any buffered log entries

	//Env vars
	var broker string
	var port string
	var user string
	var pass string
	var service string
	var db string
	var qos int

	//TODO: Add check is local or k8 and adjust broker address for dev/test/stage
	_, inCluster := os.LookupEnv("KUBERNETES_SERVICE_HOST")

	if inCluster {
		logger.Info("Running inside k8 cluster")
		user = os.Getenv("DB_USERNAME")
		pass = os.Getenv("DB_PASSWORD")
		service = os.Getenv("SERVICE")
		db = os.Getenv("DB_NAME")
		broker = os.Getenv("MQTT_BROKER_HOST")
		port = os.Getenv("MQTT_BROKER_PORT")
		qos, _ = strconv.Atoi(os.Getenv("MQTT_QOS"))
		// Read the topic name from the environment variable
		topic = os.Getenv("MQTT_BROKER_SUB_TOPIC")
		if topic == "" {
			logger.Warn("MQTT_TOPIC environment variable is not set or empty. Using default topic devices/telemetry.")
			topic = "devices/telemetry"
		}
	} else {
		if err := godotenv.Load(); err != nil {
			log.Fatalf("Error loading .env file: %v", err)
		}
		logger.Info("Running outside k8 cluster")
		user = os.Getenv("DB_USERNAME")
		pass = os.Getenv("DB_PASSWORD")
		service = os.Getenv("SERVICE")
		db = os.Getenv("DB_NAME")
		broker = os.Getenv("MQTT_BROKER_HOST")
		port = os.Getenv("MQTT_BROKER_PORT")
		qos, _ = strconv.Atoi(os.Getenv("MQTT_QOS"))
		topic = "devices/telemetry"
	}
	//Use default port number 5432
	connString := fmt.Sprintf("postgres://%s:%s@%s/%s", user, pass, service, db)
	dbPool, err := connectDb(logger, ctx, connString)
	tableName := "iot"
	col := "data"
	schema := "id SERIAL PRIMARY KEY, data TEXT"

	//Check if table exists if not create it
	err = createTableIfNotExists(logger, dbPool, tableName, schema)
	if err != nil {
		logger.Error("Error while creating ", zap.String("table:", tableName))
		//panic(err)
	}
	// Start a goroutine to handle the MQTT messages
	opts := MQTT.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("mqtt://%s:%s", broker, port))
	opts.SetClientID("test_dev_ingestion")
	opts.OnConnect = connectHandler(logger)
	opts.OnConnectionLost = connectLostHandler(logger)
	opts.SetDefaultPublishHandler(messagePubHandler(logger, messageBuffer))
	client := MQTT.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	// Subscribe to your MQTT topic
	if token := client.Subscribe(topic, byte(qos), messagePubHandler(logger, messageBuffer)); token.Wait() && token.Error() != nil {
		logger.Error("Failed to subscribe to the MQTT topic", zap.Error(token.Error()))
		return
	}
	logger.Info("Subscribed to the MQTT topic")

	// Start the goroutine to insert messages into the database
	go insertIntoDB(logger, dbPool, messageBuffer, tableName, col)

	// Keep the main goroutine running until canceled
	select {
	case <-ctx.Done():
		logger.Warn("Shutting down app")
	}

	/*client.Unsubscribe(topic)
	client.Disconnect(500)*/
}
