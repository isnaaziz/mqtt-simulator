package model

type TagValue struct {
	Timestamp string  `json:"timestamp"`
	Type      string  `json:"type"`
	Unit      string  `json:"unit"`
	Value     float64 `json:"value"`
}

type StatusValue struct {
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Value     int    `json:"value"`
	Status    string `json:"status"`
}

type Breaker struct {
	Name     string `json:"name"`
	Category string `json:"category"`
	Label    string `json:"label"`
	State    string `json:"state"`
}

type TagState struct {
	Name     string  `json:"name"`
	Category string  `json:"category"`
	Unit     string  `json:"unit"`
	Base     float64 `json:"base"`
	Variance float64 `json:"variance"`
	Cum      bool    `json:"cum"`
	Mode     string  `json:"mode"`
	Manual   float64 `json:"manual"`
	Value    float64 `json:"value"`
}

type ControlMsg struct {
	Action   string  `json:"action"`
	Tag      string  `json:"tag"`
	Mode     string  `json:"mode"`
	Value    float64 `json:"value"`
	State    string  `json:"state"`
	Category string  `json:"category,omitempty"`
	Unit     string  `json:"unit,omitempty"`
	Base     float64 `json:"base,omitempty"`
	Variance float64 `json:"variance,omitempty"`
	Cum      bool    `json:"cum,omitempty"`
	Label    string  `json:"label,omitempty"`
}

type RCCommand struct {
	Value interface{} `json:"value"` // 0/1 or analog value (float or string)
}
