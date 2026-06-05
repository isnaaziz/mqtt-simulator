package simulator

import (
	"testing"

	"mqtt_simulator_ups/internal/model"
)

func TestHandleRC(t *testing.T) {
	// Initialize simulator with dummy data
	breakers := []model.Breaker{
		{Name: "CB_INC", State: "closed"},
		{Name: "CB_UPS", State: "closed"},
	}
	tags := []model.TagState{
		{Name: "LOAD_VL1N", Base: 230, Value: 230, Mode: "auto"},
	}
	s, err := New("tcp://127.0.0.1:1883", "TEST_RTU", "TEST_PREFIX", "", "", tags, breakers)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}

	// 1. Test value 0 (open)
	s.HandleRC("CB_INC", model.RCCommand{Value: 0})
	if s.breakers["CB_INC"].State != "open" {
		t.Errorf("Expected CB_INC state to be open for value 0, got %s", s.breakers["CB_INC"].State)
	}

	// 2. Test value 1 (close)
	s.HandleRC("CB_INC", model.RCCommand{Value: 1})
	if s.breakers["CB_INC"].State != "closed" {
		t.Errorf("Expected CB_INC state to be closed for value 1, got %s", s.breakers["CB_INC"].State)
	}

	// 3. Test analog setpoint tag override
	s.HandleRC("LOAD_VL1N", model.RCCommand{Value: 225.5})
	if s.tags["LOAD_VL1N"].Value != 225.5 || s.tags["LOAD_VL1N"].Mode != "manual" {
		t.Errorf("Expected LOAD_VL1N to be overridden to manual 225.5, got %s %.1f", s.tags["LOAD_VL1N"].Mode, s.tags["LOAD_VL1N"].Value)
	}
}
