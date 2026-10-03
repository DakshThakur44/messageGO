package models

import (
	"encoding/json"
	"time"
)

type Envelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type Message struct {
	ID          string    `json:"id"`
	SenderID    string    `json:"sender_id"`
	RecipientID string    `json:"recipient_id"`
	Content     string    `json:"content"`
	Timestamp   time.Time `json:"timestamp"`
}

type DeliveryACK struct {
	MessageID   string    `json:"message_id"`
	SenderID    string    `json:"sender_id"`
	RecipientID string    `json:"recipient_id"`
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
}
