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
