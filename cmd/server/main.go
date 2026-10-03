package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"

	"messageGO/internal/chat"
	"messageGO/internal/storage"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Permissive origin check for local development
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	// 1. Initialize memory holding store
	store := storage.NewMemoryStore()

	// 2. Instantiate central message router
	hub := chat.NewHub(store)

	// 3. Start central event loop in background
	go hub.Run()

	// 4. Register WebSocket endpoint
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})

	log.Println("[SERVER] Offline-first chat server running on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("[SERVER] Server crashed: %v", err)
	}
}

func serveWS(hub *chat.Hub, w http.ResponseWriter, r *http.Request) {
	// Extract user_id from query string (e.g. ws://localhost:8080/ws?user_id=alice)
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing required 'user_id' query parameter", http.StatusBadRequest)
		return
	}

	// Upgrade HTTP connection to WebSocket protocol
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[SERVER] Upgrade error for %s: %v", userID, err)
		return
	}

	// Create client wrapper and register with Hub
	client := chat.NewClient(userID, hub, conn)
	hub.Register(client)

	// Spawn dedicated read/write goroutines for this socket
	go client.WritePump()
	go client.ReadPump()
}
