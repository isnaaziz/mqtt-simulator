package main

import (
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"mqtt_simulator_ups/internal/config"
	"mqtt_simulator_ups/internal/simulator"

	"github.com/joho/godotenv"
)

//go:embed static
var staticFiles embed.FS

func main() {
	godotenv.Load()
	cfg := config.Load()

	fmt.Printf("Connecting to broker: %s as user: %s\n", cfg.Broker, cfg.Username)
	sim, err := simulator.New(cfg.Broker, cfg.RTUID, cfg.Prefix, cfg.Username, cfg.Password, config.DefaultTags(), config.DefaultBreakers())
	if err != nil {
		log.Fatalf("Init simulator gagal: %v\n", err)
	}
	fmt.Println("Simulator berjalan. (Broker MQTT di-connect di background; UI tetap jalan walau broker mati.)")

	go sim.Hub().Run()

	staticFS, _ := fs.Sub(staticFiles, "static")
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", sim.ServeWs)
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	go func() {
		fmt.Printf("HMI UI tersedia di http://localhost%s\n", cfg.HTTPAddr)
		if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
			log.Fatal("http server:", err)
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		sim.Tick()

	}
}
