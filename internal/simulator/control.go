package simulator

import (
	"log"
	"strconv"

	"mqtt_simulator_ups/internal/model"
)

func (s *Simulator) HandleControl(msg model.ControlMsg) {
	s.mu.Lock()
	changedCB := s.applyControl(msg)
	s.mu.Unlock()

	if s.connected.Load() {
		if changedCB != "" {
			s.publishCBState(changedCB)
		} else if msg.Action == "set_value" || msg.Action == "set_mode" {
			s.publishTagState(msg.Tag)
		} else if msg.Action == "manual_all" || msg.Action == "auto_all" || msg.Action == "reset_all" {
			for _, name := range s.order {
				s.publishTagState(name)
			}
		}
	}

	s.hub.Send(s.Snapshot())
}

func (s *Simulator) HandleRC(targetName string, cmd model.RCCommand) {
	s.mu.Lock()

	// 1. Check if it's a breaker (Telesignal / digital status control)
	if b, ok := s.breakers[targetName]; ok {
		targetState := ""
		if cmd.Value != nil {
			var valFloat float64
			hasFloat := false
			switch v := cmd.Value.(type) {
			case float64:
				valFloat = v
				hasFloat = true
			case int:
				valFloat = float64(v)
				hasFloat = true
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					valFloat = f
					hasFloat = true
				}
			}
			if hasFloat {
				switch valFloat {
				case 0:
					targetState = "open"
				case 1:
					targetState = "closed"
				}
			}
		}

		changed := false
		if targetState != "" && b.State != targetState {
			b.State = targetState
			changed = true
			log.Printf("control: RC CB %s -> %s\n", b.Name, b.State)
		}
		s.mu.Unlock()

		if changed && s.connected.Load() {
			s.publishCBState(targetName)
		}
		s.hub.Send(s.Snapshot())
		return
	}

	// 2. Check if it's a tag (Telemetering / analog setpoint control)
	if t, ok := s.tags[targetName]; ok {
		var valFloat float64
		hasFloat := false
		switch v := cmd.Value.(type) {
		case float64:
			valFloat = v
			hasFloat = true
		case int:
			valFloat = float64(v)
			hasFloat = true
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				valFloat = f
				hasFloat = true
			}
		}

		if hasFloat {
			t.Mode = "manual"
			t.Manual = valFloat
			t.Value = valFloat
			log.Printf("control: RC Setpoint %s -> manual %.3f\n", t.Name, valFloat)
			s.mu.Unlock()

			if s.connected.Load() {
				s.publishTagState(targetName)
			}
			s.hub.Send(s.Snapshot())
			return
		}
	}

	s.mu.Unlock()
	log.Printf("control: RC target %s not found or invalid payload\n", targetName)
}

func (s *Simulator) applyControl(msg model.ControlMsg) (changedCB string) {
	if msg.Action == "reset_all" || msg.Action == "auto_all" {
		for _, t := range s.tags {
			t.Mode = "auto"
		}
		log.Println("control: reset all tags to AUTO")
		return ""
	}

	if msg.Action == "manual_all" {
		for _, t := range s.tags {
			t.Mode = "manual"
			t.Manual = t.Value
		}
		log.Println("control: set all tags to MANUAL")
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
		} else {
			st := msg.State
			switch st {
			case "close":
				st = "closed"
			case "opened":
				st = "open"
			}
			if st == "open" || st == "closed" {
				b.State = st
			}
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
