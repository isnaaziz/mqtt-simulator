package simulator

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"mqtt_simulator_ups/internal/model"
	"mqtt_simulator_ups/internal/ws"
)

func New(broker, rtuID, prefix, username, password string, tags []model.TagState, breakers []model.Breaker) (*Simulator, error) {
	s := &Simulator{
		rtuID:    rtuID,
		prefix:   prefix,
		tags:     make(map[string]*model.TagState),
		breakers: make(map[string]*model.Breaker),
		hub:      ws.NewHub(),
	}

	for i := range breakers {
		b := breakers[i]
		if b.State == "" {
			b.State = "closed"
		}
		s.breakers[b.Name] = &b
		s.breakerOrder = append(s.breakerOrder, b.Name)
	}

	for i := range tags {
		t := tags[i]
		t.Mode = "auto"
		t.Value = t.Base
		t.Manual = t.Base
		if t.Min == 0 && t.Max == 0 {
			t.Min = t.Base - t.Variance
			t.Max = t.Base + t.Variance
		}
		s.tags[t.Name] = &t
		s.order = append(s.order, t.Name)
	}

	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("sim_%s_%d", rtuID, rand.Intn(1000)))
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(15 * time.Second)
	opts.SetConnectTimeout(5 * time.Second)
	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		s.connected.Store(true)
		log.Println("MQTT broker connected")

		// 1. Subscribe to simulator override topic (CMD)
		simCmdTopic := prefix + "/CMD"
		if token := c.Subscribe(simCmdTopic, 1, func(_ mqtt.Client, msg mqtt.Message) {
			log.Printf("MQTT CMD (global) raw: topic=%s payload=%s", msg.Topic(), string(msg.Payload()))
			var ctrl model.ControlMsg
			if err := json.Unmarshal(msg.Payload(), &ctrl); err != nil {
				log.Printf("MQTT CMD (global) parse error: %v", err)
				return
			}
			s.HandleControl(ctrl)
		}); token.Wait() && token.Error() != nil {
			log.Printf("MQTT subscribe GAGAL %s: %v", simCmdTopic, token.Error())
		} else {
			log.Printf("MQTT subscribe OK: %s (QoS 1)", simCmdTopic)
		}

		// 2. Subscribe to remote control command topics (CMD)
		cmdTopicPattern := prefix + "/CMD/+"
		if token := c.Subscribe(cmdTopicPattern, 1, func(_ mqtt.Client, msg mqtt.Message) {
			log.Printf("MQTT CMD raw: topic=%s payload=%s", msg.Topic(), string(msg.Payload()))
			parts := strings.Split(msg.Topic(), "/")
			if len(parts) < 3 {
				log.Printf("MQTT CMD topic format invalid: %s", msg.Topic())
				return
			}
			breakerName := parts[len(parts)-1]

			var rc model.RCCommand
			if err := json.Unmarshal(msg.Payload(), &rc); err != nil {
				log.Printf("MQTT CMD parse error: %v", err)
				return
			}
			s.HandleRC(breakerName, rc)
		}); token.Wait() && token.Error() != nil {
			log.Printf("MQTT subscribe GAGAL %s: %v", cmdTopicPattern, token.Error())
		} else {
			log.Printf("MQTT subscribe OK: %s (QoS 1)", cmdTopicPattern)
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		s.connected.Store(false)
		log.Printf("MQTT connection lost: %v (retry di background)", err)
	})

	s.client = mqtt.NewClient(opts)

	go func() {
		token := s.client.Connect()
		token.Wait()
		if err := token.Error(); err != nil {
			log.Printf("MQTT initial connect gagal: %v (akan terus retry)", err)
		}
	}()

	return s, nil
}
