package main

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

var messagePubHandler = func(logger *zap.Logger, messageBuffer chan<- []byte) mqtt.MessageHandler {
	return func(client MQTT.Client, msg MQTT.Message) {
		//TODO Add here from which device id message is received
		/*logger.Info("Received a message",
			zap.String("topic", msg.Topic()),
			zap.ByteString("payload", msg.Payload()),
			zap.Int32("qos", int32(msg.Qos())),
		)*/

		messageBuffer <- msg.Payload()
	}
}

var connectHandler = func(logger *zap.Logger) mqtt.OnConnectHandler {
	return func(client MQTT.Client) {
		logger.Info("Connected to broker")
	}
}

var connectLostHandler = func(logger *zap.Logger) mqtt.ConnectionLostHandler {
	return func(client MQTT.Client, err error) {
		//TODO Check connection lost handling
		logger.Info("Connection to broker lost")
	}
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
