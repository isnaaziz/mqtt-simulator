package simulator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"mqtt_simulator_ups/internal/model"
	"mqtt_simulator_ups/internal/timeutil"
)

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
		if t.Mode == "auto" && s.connected.Load() {
			tv := model.TagValue{
				Timestamp: time.Now().In(timeutil.JakartaLoc).Format("2006-01-02T15:04:05-0700"),
				Type:      "MeasureValue",
				Unit:      t.Unit,
				Value:     t.Value,
			}
			topic := fmt.Sprintf("%s/%s/%s", s.prefix, t.Category, t.Name)
			payload, _ := json.Marshal(tv)
			wg.Add(1)
			go func(topic string, payload []byte) {
				defer wg.Done()
				s.client.Publish(topic, 0, false, payload).Wait()
			}(topic, payload)
		}
	}
	s.mu.Unlock()
	wg.Wait()

	s.hub.Send(s.Snapshot())
}

func computeAuto(t *model.TagState) float64 {
	prev := t.Value
	if t.Cum {
		return prev + rand.Float64()*t.Variance
	}

	span := t.Max - t.Min
	if span <= 0 {
		return t.Min
	}

	drift := (rand.Float64()*2 - 1) * (span * 0.05)
	val := prev + drift

	if val > t.Max {
		val = t.Max - rand.Float64()*(span*0.05)
	}
	if val < t.Min {
		val = t.Min + rand.Float64()*(span*0.05)
	}
	return val
}
