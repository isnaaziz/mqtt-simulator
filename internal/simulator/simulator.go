package simulator

import (
	"net/http"
	"sync"
	"sync/atomic"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"mqtt_simulator_ups/internal/model"
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
