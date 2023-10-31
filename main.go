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
	//Buffer channel for MQTT messages
	//TODO Make buffer size conf from env
	MSG_BUFF_SIZE := 67108864 //64 MB
	var messageBuffer = make(chan []byte, MSG_BUFF_SIZE)
	dataBuffer := make(chan PQA_Message, 1000)
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
			logger.Warn("MQTT_BROKER_SUB_TOPIC environment variable is not set or empty. Using default topic devices/telemetry.")
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
	connString := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, pass, service, db)
	dbPool, err := connectDb(logger, &ctx, connString)
	defer dbPool.Close()
	tableName := "sensor_data"
	queryCreateHypertable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
		time TIMESTAMPTZ NOT NULL,
		device_id TEXT,
		slave_id TEXT,
		date TIMESTAMP,
		address INTEGER,
		value REAL NOT NULL,
		key TEXT NOT NULL
		);
		SELECT create_hypertable('%[1]s', 'time');`, tableName)

	exists, err := tableExists(dbPool, tableName)
	if err != nil {
		logger.Error("Error while checking if the table exists:", zap.Error(err))
		return
	}

	if !exists {
		//Create if not exists hypertable
		logger.Info("Table %s does not exist in DB, creating ...", zap.String("table", tableName))
		err = createHyper(logger, dbPool, queryCreateHypertable)
		//TODO Handle table creation failure more gracefully
		if err != nil {
			ctx.Done()
		}
	} else {
		logger.Info("Table %s already exists in DB", zap.String("table", tableName))
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
	//Start the go routine to parse messages
	go unmarshallMsg(logger, messageBuffer, dataBuffer)
	//Start the go routine to batch insert messages
	//go batchInsert(logger, dbPool, 20, 20, "sensor_data")
	go batchInsertDb(logger, dbPool, dataBuffer, 20, "sensor_data")

	for {
		select {
		/*case payload := <-messageBuffer:
		logger.Info("Recieved %s", zap.String("payload:", string(payload)))*/

		case <-ctx.Done():
			client.Unsubscribe(topic)
			client.Disconnect(500)
			logger.Warn("Shutting down app")
			return
		}
	}
}
