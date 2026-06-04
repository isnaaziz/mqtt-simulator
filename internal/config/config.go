package config

import (
	"os"
	"strings"

	"mqtt_simulator_ups/internal/model"
)

type AppConfig struct {
	Broker   string
	Username string
	Password string
	HTTPAddr string
	RTUID    string
	Prefix   string
}

func Load() AppConfig {
	broker := "tcp://emqx-broker:1883"
	if b := os.Getenv("MQTT_BROKER"); b != "" {
		broker = b
	}
	httpAddr := ":8080"
	if a := os.Getenv("HTTP_ADDR"); a != "" {
		httpAddr = a
		if !strings.HasPrefix(httpAddr, ":") && !strings.Contains(httpAddr, ":") {
			httpAddr = ":" + httpAddr
		}
	}
	return AppConfig{
		Broker:   broker,
		Username: os.Getenv("MQTT_USERNAME"),
		Password: os.Getenv("MQTT_PASSWORD"),
		HTTPAddr: httpAddr,
		RTUID:    "UPS_07_DUY",
		Prefix:   "UPS_07_DUY",
	}
}

func DefaultTags() []model.TagState {
	return []model.TagState{
		{Name: "LOAD_VL3N", Category: "LOAD", Unit: "V", Base: 230, Variance: 2},
		{Name: "LOAD_VL12", Category: "LOAD", Unit: "V", Base: 400, Variance: 5},
		{Name: "LOAD_VL23", Category: "LOAD", Unit: "V", Base: 400, Variance: 5},
		{Name: "LOAD_VL31", Category: "LOAD", Unit: "V", Base: 400, Variance: 5},
		{Name: "LOAD_VL1N", Category: "LOAD", Unit: "V", Base: 230, Variance: 2},
		{Name: "LOAD_VL2N", Category: "LOAD", Unit: "V", Base: 230, Variance: 2},
		{Name: "LOAD_Freq", Category: "LOAD", Unit: "Hz", Base: 50, Variance: 0.1},
		{Name: "LOAD_IL1", Category: "LOAD", Unit: "A", Base: 10, Variance: 2},
		{Name: "LOAD_IL3", Category: "LOAD", Unit: "A", Base: 10, Variance: 2},
		{Name: "LOAD_IL2", Category: "LOAD", Unit: "A", Base: 10, Variance: 2},
		{Name: "LOAD_Ptot", Category: "LOAD", Unit: "kW", Base: 5, Variance: 1},
		{Name: "LOAD_pf", Category: "LOAD", Unit: "", Base: 0.98, Variance: 0.02},
		{Name: "LOAD_Qtot", Category: "LOAD", Unit: "kVAR", Base: 1, Variance: 0.5},
		{Name: "LOAD_Energy_Imp", Category: "LOAD", Unit: "kWh", Base: 26362, Variance: 0.1, Cum: true},
		{Name: "LOAD_Stot", Category: "LOAD", Unit: "kVAR", Base: 5, Variance: 1},
		{Name: "LOAD_Energy_Exp", Category: "LOAD", Unit: "kWh", Base: 0, Variance: 0, Cum: true},

		{Name: "INC1_VL12", Category: "INC", Unit: "V", Base: 400, Variance: 5},
		{Name: "INC1_VL23", Category: "INC", Unit: "V", Base: 400, Variance: 5},
		{Name: "INC1_VL31", Category: "INC", Unit: "V", Base: 400, Variance: 5},
		{Name: "INC1_VL1N", Category: "INC", Unit: "V", Base: 230, Variance: 2},
		{Name: "INC1_VL2N", Category: "INC", Unit: "V", Base: 230, Variance: 2},
		{Name: "INC1_VL3N", Category: "INC", Unit: "V", Base: 230, Variance: 2},
		{Name: "INC1_IL1", Category: "INC", Unit: "A", Base: 50, Variance: 5},
		{Name: "INC1_IL2", Category: "INC", Unit: "A", Base: 50, Variance: 5},
		{Name: "INC1_IL3", Category: "INC", Unit: "A", Base: 50, Variance: 5},
		{Name: "INC1_Freq", Category: "INC", Unit: "Hz", Base: 50, Variance: 0.1},
		{Name: "INC1_pf", Category: "INC", Unit: "", Base: 0.95, Variance: 0.05},
		{Name: "INC1_Ptot", Category: "INC", Unit: "kW", Base: 30, Variance: 5},
		{Name: "INC1_Qtot", Category: "INC", Unit: "kVAR", Base: 5, Variance: 2},
		{Name: "INC1_Stot", Category: "INC", Unit: "kVAR", Base: 30, Variance: 5},
		{Name: "INC1_Energy_Exp", Category: "INC", Unit: "kWh", Base: 0, Variance: 0, Cum: true},
		{Name: "INC1_Energy_Imp", Category: "INC", Unit: "kWh", Base: 15000, Variance: 0.5, Cum: true},

		{Name: "UPS_V1_Inc", Category: "UPS", Unit: "V", Base: 230, Variance: 2},
		{Name: "UPS_V2_Inc", Category: "UPS", Unit: "V", Base: 230, Variance: 2},
		{Name: "UPS_IL1_Inc", Category: "UPS", Unit: "A", Base: 11, Variance: 1},
		{Name: "UPS_IL3_Inc", Category: "UPS", Unit: "A", Base: 12, Variance: 1},
		{Name: "UPS_V2_Out", Category: "UPS", Unit: "V", Base: 400, Variance: 2},
		{Name: "UPS_IL2_Inc", Category: "UPS", Unit: "A", Base: 13, Variance: 1},
		{Name: "UPS_V3_Inc", Category: "UPS", Unit: "V", Base: 230, Variance: 2},
		{Name: "UPS_V1_Out", Category: "UPS", Unit: "V", Base: 400, Variance: 2},
		{Name: "UPS_P_Load1", Category: "UPS", Unit: "%", Base: 45, Variance: 5},
		{Name: "UPS_V3_Out", Category: "UPS", Unit: "V", Base: 400, Variance: 2},
		{Name: "UPS_P_Load2", Category: "UPS", Unit: "%", Base: 42, Variance: 5},
		{Name: "UPS_P_Load3", Category: "UPS", Unit: "%", Base: 44, Variance: 5},
		{Name: "UPS_P_LoadTotal", Category: "UPS", Unit: "%", Base: 44, Variance: 2},
		{Name: "UPS_F", Category: "UPS", Unit: "Hz", Base: 50, Variance: 0.1},
		{Name: "UPS_P", Category: "UPS", Unit: "kW", Base: 15, Variance: 2},
		{Name: "UPS_IL3_Out", Category: "UPS", Unit: "A", Base: 20, Variance: 2},
		{Name: "UPS_IL2_Out", Category: "UPS", Unit: "A", Base: 21, Variance: 2},
		{Name: "UPS_IL1_Out", Category: "UPS", Unit: "A", Base: 22, Variance: 2},
		{Name: "UPS_Temp", Category: "UPS", Unit: "degC", Base: 35, Variance: 2},
		{Name: "UPS_SOC_Battery", Category: "UPS", Unit: "%", Base: 100, Variance: 0},
		{Name: "UPS_V_Battery", Category: "UPS", Unit: "V", Base: 650, Variance: 5},
	}
}

func DefaultBreakers() []model.Breaker {
	return []model.Breaker{
		{Name: "CB_INC", Category: "CB", Label: "Incomer CB", State: "closed"},
		{Name: "CB_UPS", Category: "CB", Label: "UPS Input CB", State: "closed"},
		{Name: "CB_BYPASS", Category: "CB", Label: "Bypass CB", State: "open"},
		{Name: "CB_LOAD", Category: "CB", Label: "Load CB", State: "closed"},
	}
}
