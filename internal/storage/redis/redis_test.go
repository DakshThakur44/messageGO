package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"messageGO/internal/models"
)

func TestOfflineQueue(t *testing.T) {
	client, err := Connect("localhost:6379", "")
	if err != nil {
		t.Skip("Redis not reachable locally, skipping integration test")
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	testUser := "test_user_offline_queue"
	// Ensure clean state
	_, _ = client.GetAndFlushOfflineQueue(ctx, testUser)

	payload, _ := json.Marshal(map[string]string{"text": "hello offline"})
	env := &models.Envelope{
		Type: "chat",
		Data: payload,
	}

	if err := client.EnqueueOffline(ctx, testUser, env); err != nil {
		t.Fatalf("EnqueueOffline failed: %v", err)
	}

	items, err := client.GetAndFlushOfflineQueue(ctx, testUser)
	if err != nil {
		t.Fatalf("GetAndFlushOfflineQueue failed: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(items))
	}

	if items[0].Type != "chat" {
		t.Fatalf("Expected type chat, got %s", items[0].Type)
	}

	// Verify second flush is empty
	secondFlush, err := client.GetAndFlushOfflineQueue(ctx, testUser)
	if err != nil {
		t.Fatalf("Second flush failed: %v", err)
	}
	if len(secondFlush) != 0 {
		t.Fatalf("Expected 0 items after flush, got %d", len(secondFlush))
	}
}
