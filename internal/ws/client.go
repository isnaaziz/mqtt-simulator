package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"mqtt_simulator_ups/internal/model"
)

type Controller interface {
	HandleControl(model.ControlMsg)
	Snapshot() []byte
}

type Client struct {
	hub  *Hub
	ctrl Controller
	conn *websocket.Conn
	send chan []byte
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func ServeWs(hub *Hub, ctrl Controller, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	client := &Client{hub: hub, ctrl: ctrl, conn: conn, send: make(chan []byte, 32)}
	hub.register <- client
	client.send <- ctrl.Snapshot()
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(4096)
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg model.ControlMsg
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		c.ctrl.HandleControl(msg)
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
