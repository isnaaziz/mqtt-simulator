package simulator

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"mqtt_simulator_ups/internal/model"
	"mqtt_simulator_ups/internal/timeutil"
	"mqtt_simulator_ups/internal/ws"
)

type Simulator struct {
	client mqtt.Client
	rtuID  string
	prefix string

	tags  map[string]*model.TagState
	order []string

	breakers     map[string]*model.Breaker
	breakerOrder []string

	mu        sync.RWMutex
	connected atomic.Bool

	hub *ws.Hub
}

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
		s.tags[t.Name] = &t
		s.order = append(s.order, t.Name)
	}

	cmdTopic := prefix + "/CMD"

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
		token := c.Subscribe(cmdTopic, 1, func(_ mqtt.Client, msg mqtt.Message) {
			log.Printf("MQTT CMD raw: topic=%s payload=%s", msg.Topic(), string(msg.Payload()))
			var ctrl model.ControlMsg
			if err := json.Unmarshal(msg.Payload(), &ctrl); err != nil {
				log.Printf("MQTT CMD parse error: %v — payload: %s", err, string(msg.Payload()))
				return
			}
			log.Printf("MQTT CMD: action=%s tag=%s state=%s", ctrl.Action, ctrl.Tag, ctrl.State)
			s.HandleControl(ctrl)
		})
		token.Wait()
		if err := token.Error(); err != nil {
			log.Printf("MQTT subscribe GAGAL %s: %v", cmdTopic, err)
		} else {
			log.Printf("MQTT subscribe OK: %s (QoS 1)", cmdTopic)
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

func (s *Simulator) Hub() *ws.Hub {
	return s.hub
}

func (s *Simulator) Connected() bool {
	return s.connected.Load()
}

func (s *Simulator) TagCount() int {
	return len(s.order)
}

func (s *Simulator) ServeWs(w http.ResponseWriter, r *http.Request) {
	ws.ServeWs(s.hub, s, w, r)
}

func (s *Simulator) HandleControl(msg model.ControlMsg) {
	s.mu.Lock()
	changedCB := s.applyControl(msg)
	s.mu.Unlock()

	if changedCB != "" && s.connected.Load() {
		s.publishCBState(changedCB)
	}

	s.hub.Send(s.Snapshot())
}

func (s *Simulator) applyControl(msg model.ControlMsg) (changedCB string) {
	if msg.Action == "reset_all" {
		for _, t := range s.tags {
			t.Mode = "auto"
		}
		log.Println("control: reset all tags to AUTO")
		return ""
	}

	if msg.Action == "cb_set" || msg.Action == "cb_toggle" {
		b, ok := s.breakers[msg.Tag]
		if !ok {
			return ""
		}
		if msg.Action == "cb_toggle" {
			if b.State == "closed" {
				b.State = "open"
			} else {
				b.State = "closed"
			}
		} else if msg.State == "open" || msg.State == "closed" {
			b.State = msg.State
		}
		log.Printf("control: CB %s -> %s\n", b.Name, b.State)
		return b.Name
	}

	t, ok := s.tags[msg.Tag]
	if !ok {
		return ""
	}

	switch msg.Action {
	case "set_mode":
		if msg.Mode == "manual" {
			t.Mode = "manual"
			t.Manual = t.Value
		} else {
			t.Mode = "auto"
		}
		log.Printf("control: %s -> mode %s\n", t.Name, t.Mode)
	case "set_value":
		t.Mode = "manual"
		t.Manual = msg.Value
		t.Value = msg.Value
		log.Printf("control: %s -> manual %.3f\n", t.Name, msg.Value)
	}
	return ""
}

func (s *Simulator) publishCBState(name string) {
	s.mu.RLock()
	b, ok := s.breakers[name]
	if !ok {
		s.mu.RUnlock()
		return
	}
	v := 0
	if b.State == "closed" {
		v = 1
	}
	sv := model.StatusValue{
		Timestamp: time.Now().In(timeutil.JakartaLoc).Format("2006-01-02T15:04:05-0700"),
		Type:      "SinglePoint",
		Value:     v,
		Status:    b.State,
	}
	topic := fmt.Sprintf("%s/%s/%s", s.prefix, b.Category, b.Name)
	payload, _ := json.Marshal(sv)
	s.mu.RUnlock()

	s.client.Publish(topic, 0, true, payload)
}

func (s *Simulator) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tags := make([]model.TagState, 0, len(s.order))
	for _, name := range s.order {
		tags = append(tags, *s.tags[name])
	}
	brks := make([]model.Breaker, 0, len(s.breakerOrder))
	for _, name := range s.breakerOrder {
		brks = append(brks, *s.breakers[name])
	}
	out, _ := json.Marshal(map[string]interface{}{
		"type":     "update",
		"rtu":      s.rtuID,
		"prefix":   s.prefix,
		"mqtt":     s.connected.Load(),
		"ts":       time.Now().In(timeutil.JakartaLoc).Format(time.RFC3339),
		"tags":     tags,
		"breakers": brks,
	})
	return out
}

func (s *Simulator) Tick() {
	s.mu.Lock()
	var wg sync.WaitGroup
	for _, name := range s.order {
		t := s.tags[name]
		if t.Mode == "manual" {
			t.Value = t.Manual
		} else {
			t.Value = computeAuto(t)
		}
		tv := model.TagValue{
			Timestamp: time.Now().In(timeutil.JakartaLoc).Format("2006-01-02T15:04:05-0700"),
			Type:      "MeasureValue",
			Unit:      t.Unit,
			Value:     t.Value,
		}
		topic := fmt.Sprintf("%s/%s/%s", s.prefix, t.Category, t.Name)
		payload, _ := json.Marshal(tv)
		if s.connected.Load() {
			wg.Add(1)
			go func(topic string, payload []byte) {
				defer wg.Done()
				s.client.Publish(topic, 0, false, payload).Wait()
			}(topic, payload)
		}
	}
	s.mu.Unlock()
	wg.Wait()

	if s.connected.Load() {
		s.mu.RLock()
		for _, name := range s.breakerOrder {
			b := s.breakers[name]
			v := 0
			if b.State == "closed" {
				v = 1
			}
			sv := model.StatusValue{
				Timestamp: time.Now().In(timeutil.JakartaLoc).Format("2006-01-02T15:04:05-0700"),
				Type:      "SinglePoint",
				Value:     v,
				Status:    b.State,
			}
			topic := fmt.Sprintf("%s/%s/%s", s.prefix, b.Category, b.Name)
			payload, _ := json.Marshal(sv)
			s.client.Publish(topic, 0, true, payload)
		}
		s.mu.RUnlock()
	}

	s.hub.Send(s.Snapshot())
}

func computeAuto(t *model.TagState) float64 {
	prev := t.Value
	if t.Cum {
		return prev + rand.Float64()*t.Variance
	}
	drift := (rand.Float64()*2 - 1) * (t.Variance * 0.1)
	val := prev + drift
	if val > t.Base+t.Variance {
		val = t.Base + t.Variance - rand.Float64()*(t.Variance*0.1)
	}
	if val < t.Base-t.Variance {
		val = t.Base - t.Variance + rand.Float64()*(t.Variance*0.1)
	}
	if t.Base >= 0 && val < 0 {
		val = 0
	}
	return val
}
