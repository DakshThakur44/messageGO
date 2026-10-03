package chat

import (
	"encoding/json"
	"log"
	"sync"

	"messageGO/internal/models"
	"messageGO/internal/storage"
)

type Hub struct {
	clients map[string]*Client

	register   chan *Client
	unregister chan *Client
	broadcast  chan *models.Envelope

	offlineStore storage.OfflineStore

	mu sync.RWMutex
}

func NewHub(store storage.OfflineStore) *Hub {
	return &Hub{
		clients:      make(map[string]*Client),
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		broadcast:    make(chan *models.Envelope),
		offlineStore: store,
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)

		case client := <-h.unregister:
			h.unregisterClient(client)

		case env := <-h.broadcast:
			h.routeEnvelope(env)
		}
	}
}

func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	h.clients[client.UserID] = client
	h.mu.Unlock()

	log.Printf("[HUB] User connected: %s", client.UserID)

	go h.flushOfflineMessages(client)
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	existing, ok := h.clients[client.UserID]
	if ok && existing == client {
		delete(h.clients, client.UserID)
		close(client.Send)
		log.Printf("[HUB] User disconnected: %s", client.UserID)
	}
	h.mu.Unlock()
}

func (h *Hub) routeEnvelope(env *models.Envelope) {
	recipientID := h.extractRecipientID(env)
	if recipientID == "" {
		log.Printf("[HUB] Drop envelope: Missing or unparseable recipient ID")
		return
	}

	h.mu.RLock()
	recipient, online := h.clients[recipientID]
	h.mu.RUnlock()

	if online {

		recipient.Send <- env
	} else {

		log.Printf("[HUB] User %s offline. Persisting envelope to store.", recipientID)
		if err := h.offlineStore.Save(recipientID, env); err != nil {
			log.Printf("[HUB] Error saving offline envelope for %s: %v", recipientID, err)
		}
	}
}

func (h *Hub) flushOfflineMessages(client *Client) {
	envelopes, err := h.offlineStore.GetAndClear(client.UserID)
	if err != nil {
		log.Printf("[HUB] Error reading offline envelopes for %s: %v", client.UserID, err)
		return
	}

	if len(envelopes) == 0 {
		return
	}

	log.Printf("[HUB] Delivering %d offline envelopes to %s", len(envelopes), client.UserID)
	for _, env := range envelopes {
		client.Send <- env
	}
}

func (h *Hub) extractRecipientID(env *models.Envelope) string {
	switch env.Type {
	case "chat":
		var msg models.Message
		if err := json.Unmarshal(env.Data, &msg); err == nil {
			return msg.RecipientID
		}
	case "ack":
		var ack models.DeliveryACK
		if err := json.Unmarshal(env.Data, &ack); err == nil {
			return ack.RecipientID
		}
	}
	return ""
}

func (h *Hub) Register(client *Client) {
	h.register <- client
}
