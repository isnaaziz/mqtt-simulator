package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
)

type TagValue struct {
	Timestamp string  `json:"timestamp"`
	Type      string  `json:"type"`
	Unit      string  `json:"unit"`
	Value     float64 `json:"value"`
}

type Simulator struct {
	client     mqtt.Client
	rtuID      string
	lastValues map[string]float64
}

func NewSimulator(broker string, rtuID string, username string, password string) (*Simulator, error) {
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("sim_%s_%d", rtuID, rand.Intn(1000)))

	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &Simulator{
		client:     client,
		rtuID:      rtuID,
		lastValues: make(map[string]float64),
	}, nil
}

func (s *Simulator) publishTag(topic string, data TagValue) {
	payload, _ := json.Marshal(data)
	token := s.client.Publish(topic, 0, false, payload)
	token.Wait()
	//fmt.Printf("[%s] Published to %s: %v\n", time.Now().Format(time.RFC3339), topic, data.Value)
}

func (s *Simulator) nextValue(tag string, unit string, base, variance float64, isCumulative bool) TagValue {
	prev, ok := s.lastValues[tag]
	if !ok {
		prev = base
	}

	var val float64
	if isCumulative {
		val = prev + rand.Float64()*variance
	} else {
		drift := (rand.Float64()*2 - 1) * (variance * 0.1)
		val = prev + drift

		if val > base+variance {
			val = base + variance - rand.Float64()*(variance*0.1)
		}
		if val < base-variance {
			val = base - variance + rand.Float64()*(variance*0.1)
		}

		if base >= 0 && val < 0 {
			val = 0
		}
	}

	s.lastValues[tag] = val

	return TagValue{
		Timestamp: time.Now().Format("2006-01-02T15:04:05-0700"),
		Type:      "MeasureValue",
		Unit:      unit,
		Value:     val,
	}
}

func main() {
	godotenv.Load()

	broker := "tcp://emqx-broker:1883"
	if b := os.Getenv("MQTT_BROKER"); b != "" {
		broker = b
	}

	username := os.Getenv("MQTT_USERNAME")
	password := os.Getenv("MQTT_PASSWORD")

	fmt.Printf("Connecting to broker: %s as user: %s\n", broker, username)

	sim, err := NewSimulator(broker, "UPS_07_DUY", username, password)
	if err != nil {
		fmt.Printf("Failed to connect to %s: %v\n", broker, err)
		return
	}
	fmt.Println("MQTT UPS Simulator connected successfully")

	for {
		prefix := "UPS_07_DUY"

		tags := []struct {
			name     string
			category string
			unit     string
			base     float64
			variance float64
			cum      bool
		}{
			{"LOAD_VL3N", "LOAD", "V", 230, 2, false},
			{"LOAD_VL12", "LOAD", "V", 400, 5, false},
			{"LOAD_VL23", "LOAD", "V", 400, 5, false},
			{"LOAD_VL31", "LOAD", "V", 400, 5, false},
			{"LOAD_VL1N", "LOAD", "V", 230, 2, false},
			{"LOAD_VL2N", "LOAD", "V", 230, 2, false},
			{"LOAD_Freq", "LOAD", "Hz", 50, 0.1, false},
			{"LOAD_IL1", "LOAD", "A", 10, 2, false},
			{"LOAD_IL3", "LOAD", "A", 10, 2, false},
			{"LOAD_IL2", "LOAD", "A", 10, 2, false},
			{"LOAD_Ptot", "LOAD", "kW", 5, 1, false},
			{"LOAD_pf", "LOAD", "", 0.98, 0.02, false},
			{"LOAD_Qtot", "LOAD", "kVAR", 1, 0.5, false},
			{"LOAD_Energy_Imp", "LOAD", "kWh", 26362, 0.1, true},
			{"LOAD_Stot", "LOAD", "kVAR", 5, 1, false},
			{"LOAD_Energy_Exp", "LOAD", "kWh", 0, 0, true},

			{"INC1_VL12", "INC", "V", 400, 5, false},
			{"INC1_VL23", "INC", "V", 400, 5, false},
			{"INC1_VL31", "INC", "V", 400, 5, false},
			{"INC1_VL1N", "INC", "V", 230, 2, false},
			{"INC1_VL2N", "INC", "V", 230, 2, false},
			{"INC1_VL3N", "INC", "V", 230, 2, false},
			{"INC1_IL1", "INC", "A", 50, 5, false},
			{"INC1_IL2", "INC", "A", 50, 5, false},
			{"INC1_IL3", "INC", "A", 50, 5, false},
			{"INC1_Freq", "INC", "Hz", 50, 0.1, false},
			{"INC1_pf", "INC", "", 0.95, 0.05, false},
			{"INC1_Ptot", "INC", "kW", 30, 5, false},
			{"INC1_Qtot", "INC", "kVAR", 5, 2, false},
			{"INC1_Stot", "INC", "kVAR", 30, 5, false},
			{"INC1_Energy_Exp", "INC", "kWh", 0, 0, true},
			{"INC1_Energy_Imp", "INC", "kWh", 15000, 0.5, true},

			{"UPS_V1_Inc", "UPS", "V", 230, 2, false},
			{"UPS_V2_Inc", "UPS", "V", 230, 2, false},
			{"UPS_IL1_Inc", "UPS", "A", 11, 1, false},
			{"UPS_IL3_Inc", "UPS", "A", 12, 1, false},
			{"UPS_V2_Out", "UPS", "V", 400, 2, false},
			{"UPS_IL2_Inc", "UPS", "A", 13, 1, false},
			{"UPS_V3_Inc", "UPS", "V", 230, 2, false},
			{"UPS_V1_Out", "UPS", "V", 400, 2, false},
			{"UPS_P_Load1", "UPS", "%", 45, 5, false},
			{"UPS_V3_Out", "UPS", "V", 400, 2, false},
			{"UPS_P_Load2", "UPS", "%", 42, 5, false},
			{"UPS_P_Load3", "UPS", "%", 44, 5, false},
			{"UPS_P_LoadTotal", "UPS", "%", 44, 2, false},
			{"UPS_F", "UPS", "Hz", 50, 0.1, false},
			{"UPS_P", "UPS", "kW", 15, 2, false},
			{"UPS_IL3_Out", "UPS", "A", 20, 2, false},
			{"UPS_IL2_Out", "UPS", "A", 21, 2, false},
			{"UPS_IL1_Out", "UPS", "A", 22, 2, false},
			{"UPS_Temp", "UPS", "degC", 35, 2, false},
			{"UPS_SOC_Battery", "UPS", "%", 100, 0, false},
			{"UPS_V_Battery", "UPS", "V", 650, 5, false},
		}

		for _, t := range tags {
			val := sim.nextValue(t.name, t.unit, t.base, t.variance, t.cum)
			sim.publishTag(fmt.Sprintf("%s/%s/%s", prefix, t.category, t.name), val)
		}

		time.Sleep(1 * time.Second)
	}
}
