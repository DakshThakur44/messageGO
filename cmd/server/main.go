package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"messageGO/internal/auth"
	"messageGO/internal/chat"
	"messageGO/internal/config"
	"messageGO/internal/storage/postgres"
	"messageGO/internal/storage/redis"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Permissive origin check for development
	},
}

func main() {
	log.Println("[SERVER] Initializing messageGO Chat Gateway...")

	// 1. Load configuration
	cfg := config.LoadConfig()

	// 2. Initialize PostgreSQL connection
	db, err := postgres.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[SERVER] PostgreSQL connection failed: %v", err)
	}
	defer db.Close()

	// Auto-apply schema migrations if file exists
	schemaSQL, err := os.ReadFile("internal/storage/postgres/schema.sql")
	if err == nil {
		if err := db.InitSchema(string(schemaSQL)); err != nil {
			log.Printf("[SERVER] Warning: Failed to apply schema SQL: %v", err)
		}
	}

	// 3. Initialize Redis connection
	redisClient, err := redis.Connect(cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		log.Fatalf("[SERVER] Redis connection failed: %v", err)
	}
	defer redisClient.Close()

	// 4. Instantiate central message hub
	hub := chat.NewHub(db, redisClient)
	go hub.Run()

	// 5. Register HTTP & WebSocket routes
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","service":"messageGO"}`))
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(cfg, hub, w, r)
	})

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: nil,
	}

	// 6. Graceful shutdown handler
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit
		log.Println("[SERVER] Shutting down gracefully...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Fatalf("[SERVER] Server forced to shutdown: %v", err)
		}
	}()

	log.Printf("[SERVER] messageGO Chat Gateway listening on :%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[SERVER] Server crashed: %v", err)
	}
}

func serveWS(cfg *config.Config, hub *chat.Hub, w http.ResponseWriter, r *http.Request) {
	// 1. Extract Token from Query parameter or Authorization header
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			tokenStr = strings.TrimPrefix(authHeader, "Bearer ")
		}
	}

	var userID, email, username string

	// 2. Validate token or allow query user_id in development mode as fallback
	if tokenStr != "" {
		claims, err := auth.ValidateAccessToken(tokenStr, cfg.JWTAccessSecret)
		if err != nil {
			log.Printf("[SERVER] Handshake rejected: invalid token: %v", err)
			http.Error(w, "Unauthorized: Invalid or expired token", http.StatusUnauthorized)
			return
		}
		userID = claims.UserID
		email = claims.Email
		if strings.Contains(email, "@") {
			username = strings.Split(email, "@")[0]
		} else {
			username = email
		}
	} else if cfg.Env == "development" && r.URL.Query().Get("user_id") != "" {
		// Dev fallback for quick manual testing without JWT
		userID = r.URL.Query().Get("user_id")
		email = userID
		username = userID
		log.Printf("[SERVER] Handshake in dev mode for user_id=%s without token", userID)
	} else {
		log.Println("[SERVER] Handshake rejected: missing authentication token")
		http.Error(w, "Unauthorized: Missing authentication token", http.StatusUnauthorized)
		return
	}

	// 3. Upgrade HTTP connection to WebSocket protocol
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[SERVER] Upgrade error for user %s: %v", userID, err)
		return
	}

	// 4. Create client wrapper and register with Hub
	client := chat.NewClient(userID, email, username, hub, conn)
	hub.Register(client)

	// 5. Spawn read/write pumps
	go client.WritePump()
	go client.ReadPump()
}
