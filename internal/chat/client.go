package chat

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"

	"messageGO/internal/models"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
)

// Client represents a single active WebSocket socket connection
type Client struct {
	UserID string
	hub    *Hub
	conn   *websocket.Conn
	Send   chan *models.Envelope
}

func NewClient(userID string, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		UserID: userID,
		hub:    hub,
		conn:   conn,
		Send:   make(chan *models.Envelope, 256),
	}
}

// ReadPump continuously listens for incoming WebSocket frames
func (c *Client) ReadPump() {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		// Refresh online presence in Redis
		c.hub.RenewPresence(c.UserID)
		return nil
	})

	for {
		_, bytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[CLIENT] Disconnect for user %s: %v", c.UserID, err)
			}
			break
		}

		var env models.Envelope
		if err := json.Unmarshal(bytes, &env); err != nil {
			log.Printf("[CLIENT] JSON decode error from %s: %v", c.UserID, err)
			c.sendError("INVALID_PAYLOAD", "Failed to parse JSON envelope")
			continue
		}

		c.hub.HandleIncomingEnvelope(c, &env)
	}
}

// WritePump pushes outgoing envelopes down the WebSocket connection
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case env, ok := <-c.Send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			if err := json.NewEncoder(w).Encode(env); err != nil {
				log.Printf("[CLIENT] Write error for %s: %v", c.UserID, err)
				return
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) sendError(code, message string) {
	errPayload, _ := json.Marshal(models.ErrorPayload{Code: code, Message: message})
	select {
	case c.Send <- &models.Envelope{Type: "error", Data: errPayload}:
	default:
	}
}
