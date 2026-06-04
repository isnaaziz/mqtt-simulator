package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

type WSMessage struct {
	Type     string        `json:"type"`
	Breakers []interface{} `json:"breakers"`
}

func main() {
	_ = godotenv.Load("/Users/macbookairm3/grita/mqtt_simulator_ups/.env")
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://192.168.212.13:1883"
	}
	username := os.Getenv("MQTT_USERNAME")
	password := os.Getenv("MQTT_PASSWORD")

	// 1. Connect to WebSocket
	u := url.URL{Scheme: "ws", Host: "localhost:8080", Path: "/ws"}
	fmt.Printf("Connecting to WebSocket: %s...\n", u.String())
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("WebSocket connection failed: %v", err)
	}
	defer c.Close()

	// Goroutine to read from WebSocket
	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("WebSocket read error: %v", err)
				return
			}
			var msg WSMessage
			if err := json.Unmarshal(message, &msg); err == nil {
				fmt.Printf("[%s] WS Update received! Breakers: %+v\n", time.Now().Format("15:04:05"), msg.Breakers)
			}
		}
	}()

	// Wait 2 seconds for initial WS message
	time.Sleep(2 * time.Second)

	// 2. Connect to MQTT and Publish Command
	fmt.Printf("Connecting to MQTT Broker: %s...\n", broker)
	opts := mqtt.NewClientOptions().AddBroker(broker)
	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}
	opts.SetClientID("scratch_test_publisher")

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT connection failed: %v", token.Error())
	}
	defer client.Disconnect(250)

	// We publish "closed" to see if it changes from "open"
	cmdTopic := "UPS_07_DUY/CMD"
	payload := `{"action":"cb_set","tag":"CB_INC","state":"open"}`
	fmt.Printf("Publishing command to MQTT: topic=%s payload=%s\n", cmdTopic, payload)
	token := client.Publish(cmdTopic, 0, false, payload)
	token.Wait()

	// Wait 5 seconds to observe any WebSocket state changes
	fmt.Println("Observing for 5 seconds...")
	time.Sleep(5 * time.Second)
	fmt.Println("Done")
}
