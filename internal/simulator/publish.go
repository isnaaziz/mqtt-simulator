package simulator

import (
	"encoding/json"
	"fmt"
	"time"

	"mqtt_simulator_ups/internal/model"
	"mqtt_simulator_ups/internal/timeutil"
)

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

func (s *Simulator) publishTagState(name string) {
	s.mu.RLock()
	t, ok := s.tags[name]
	if !ok {
		s.mu.RUnlock()
		return
	}
	tv := model.TagValue{
		Timestamp: time.Now().In(timeutil.JakartaLoc).Format("2006-01-02T15:04:05-0700"),
		Type:      "MeasureValue",
		Unit:      t.Unit,
		Value:     t.Value,
	}
	topic := fmt.Sprintf("%s/%s/%s", s.prefix, t.Category, t.Name)
	payload, _ := json.Marshal(tv)
	s.mu.RUnlock()

	s.client.Publish(topic, 0, false, payload)
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
