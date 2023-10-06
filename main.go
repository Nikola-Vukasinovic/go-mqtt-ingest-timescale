package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

var logger *zap.Logger
var inCluster bool

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	logger.Info("Received a message",
		zap.String("topic", msg.Topic()),
		zap.ByteString("payload", msg.Payload()),
		zap.Int32("qos", int32(msg.Qos())),
	)
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	logger.Info("Connected to broker")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	logger.Info("Connection to broker lost")
}

func publish(client mqtt.Client) {
	num := 10
	for i := 0; i < num; i++ {
		text := fmt.Sprintf("Message %d", i)
		token := client.Publish("topic/test", 0, false, text)
		token.Wait()
		time.Sleep(time.Second)
	}
}

func subscribe(client mqtt.Client, topic string) {
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	logger.Info("Subscribed to topic:", zap.String("topic", topic))
}

// Create TLS config for client
func NewTlsConfig() *tls.Config {
	certpool := x509.NewCertPool()
	ca, err := os.ReadFile("ca.pem")
	if err != nil {
		log.Fatalln(err.Error())
	}
	certpool.AppendCertsFromPEM(ca)
	// Import client certificate/key pair
	clientKeyPair, err := tls.LoadX509KeyPair("client-crt.pem", "client-key.pem")
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientKeyPair},
	}
}

/*
	func NewTlsConfig() *tls.Config {
	    certpool := x509.NewCertPool()
	    ca, err := os.ReadFile("ca.pem")
	    if err != nil {
	        log.Fatalln(err.Error())
	    }
	    certpool.AppendCertsFromPEM(ca)
	    return &tls.Config{
	        RootCAs: certpool,
	}
*/

func main() {
	// Use a preset configuration for the logger
	var err error
	var topic string

	logger, err = zap.NewProduction()
	defer logger.Sync() // Flush any buffered log entries

	if err != nil {
		panic(err)
	}
	//TODO: Add check is local or k8 and adjust broker address for dev/test/stage
	_, inCluster := os.LookupEnv("KUBERNETES_SERVICE_HOST")

	var broker string
	var port string

	if inCluster {
		logger.Info("Running inside k8 cluster")
		broker = os.Getenv("MQTT_BROKER_HOST")
		port = os.Getenv("MQTT_BROKER_PORT")
		// Read the topic name from the environment variable
		topic = os.Getenv("MQTT_BROKER_SUB_TOPIC")
		if topic == "" {
			logger.Warn("MQTT_TOPIC environment variable is not set or empty. Using default topic devices/telemetry.")
			topic = "devices/telemetry"
		}
	} else {
		logger.Info("Running outside k8 cluster")
		broker = "98.64.51.139"
		port = "1883"
		topic = "devices/telemetry"
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("mqtt://%s:%s", broker, port))
	opts.SetClientID("go_mqtt_client_2")
	//opts.SetUsername("emqx")
	//opts.SetPassword("public")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	subscribe(client, topic)

	// Wait for a signal to exit the program gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	client.Unsubscribe(topic)
	client.Disconnect(500)
}
