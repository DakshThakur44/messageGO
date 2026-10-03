package chat

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"

	"messageGO/internal/models"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong frame from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period (must be less than pongWait)
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer (512 KB)
	maxMessageSize = 512 * 1024
)

// Client represents an active WebSocket connection attached to a single user.
type Client struct {
	// Unique identifier for the connected user (Must be exported/capitalized)
	UserID string

	// Reference to the central hub
	hub *Hub

	// The actual underlying WebSocket connection
	conn *websocket.Conn

	// Buffered channel for outbound envelopes waiting to be sent to this client
	Send chan *models.Envelope
}

// NewClient constructs a new Client instance.
func NewClient(userID string, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		UserID: userID,
		hub:    hub,
		conn:   conn,
		Send:   make(chan *models.Envelope, 256), // Buffered channel prevents slow readers from blocking immediately
	}
}

// ReadPump listens for incoming raw WebSocket frames from the user's device.
// Runs in its own goroutine per connected client.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, bytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[CLIENT] Read error for user %s: %v", c.UserID, err)
			}
			break
		}

		// Deserialize raw JSON into Envelope wrapper
		var env models.Envelope
		if err := json.Unmarshal(bytes, &env); err != nil {
			log.Printf("[CLIENT] Unmarshal error from user %s: %v", c.UserID, err)
			continue
		}

		// Pass the envelope to the central Hub for routing
		c.hub.broadcast <- &env
	}
}

// WritePump listens on the c.Send channel and pushes outgoing frames down the WebSocket.
// Runs in its own goroutine per connected client.
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
				// The hub closed the channel -> close the connection cleanly
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			// Encode envelope back to JSON over the wire
			if err := json.NewEncoder(w).Encode(env); err != nil {
				log.Printf("[CLIENT] Write error for user %s: %v", c.UserID, err)
				return
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			// Send periodic heartbeat Ping frame to keep connection alive
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
