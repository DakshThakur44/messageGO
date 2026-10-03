package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"messageGO/internal/models"
)

type DB struct {
	pool *sql.DB
}

func Connect(databaseURL string) (*DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	log.Println("[POSTGRES] Connected successfully to database")
	return &DB{pool: db}, nil
}

func (d *DB) Close() error {
	return d.pool.Close()
}

// InitSchema applies the initial tables if they don't exist
func (d *DB) InitSchema(schemaSQL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := d.pool.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to execute schema initialization: %w", err)
	}
	log.Println("[POSTGRES] Schema initialized successfully")
	return nil
}

// EnsureDirectConversation retrieves or creates a 1:1 direct conversation between two users
func (d *DB) EnsureDirectConversation(ctx context.Context, userA, userB string) (string, error) {
	// Query to find if a direct conversation already contains both participants
	query := `
		SELECT cp1.conversation_id
		FROM conversation_participants cp1
		JOIN conversation_participants cp2 ON cp1.conversation_id = cp2.conversation_id
		JOIN conversations c ON c.id = cp1.conversation_id
		WHERE c.type = 'direct' AND cp1.user_id = $1 AND cp2.user_id = $2
		LIMIT 1;
	`
	var convID string
	err := d.pool.QueryRowContext(ctx, query, userA, userB).Scan(&convID)
	if err == nil {
		return convID, nil
	}

	// Create new direct conversation inside a transaction
	tx, err := d.pool.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	insertConv := `INSERT INTO conversations (type) VALUES ('direct') RETURNING id;`
	if err := tx.QueryRowContext(ctx, insertConv).Scan(&convID); err != nil {
		return "", fmt.Errorf("failed to create conversation: %w", err)
	}

	insertPart := `INSERT INTO conversation_participants (conversation_id, user_id) VALUES ($1, $2), ($1, $3);`
	if _, err := tx.ExecContext(ctx, insertPart, convID, userA, userB); err != nil {
		return "", fmt.Errorf("failed to insert participants: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return convID, nil
}

// SaveMessageAtomic writes a message with an atomic monotonic sequence number
func (d *DB) SaveMessageAtomic(ctx context.Context, convID, senderID, clientMsgID, contentType, content string) (*models.Message, error) {
	if contentType == "" {
		contentType = "text"
	}

	// Single atomic CTE query that locks the conversation row, increments last_seq_id, and writes message
	atomicQuery := `
		WITH next_seq AS (
			UPDATE conversations 
			SET last_seq_id = last_seq_id + 1, updated_at = NOW() 
			WHERE id = $1 
			RETURNING last_seq_id
		)
		INSERT INTO messages (conversation_id, seq_id, sender_id, client_msg_id, content_type, content)
		SELECT $1, next_seq.last_seq_id, $2, $3, $4, $5 FROM next_seq
		RETURNING id, conversation_id, seq_id, sender_id, client_msg_id, content_type, content, created_at;
	`

	var msg models.Message
	err := d.pool.QueryRowContext(ctx, atomicQuery, convID, senderID, clientMsgID, contentType, content).Scan(
		&msg.ID,
		&msg.ConversationID,
		&msg.SeqID,
		&msg.SenderID,
		&msg.ClientMsgID,
		&msg.ContentType,
		&msg.Content,
		&msg.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed atomic message insert: %w", err)
	}

	return &msg, nil
}

// GetMessagesSince fetches paginated messages with seq_id > sinceSeqID
func (d *DB) GetMessagesSince(ctx context.Context, convID string, sinceSeqID int64, limit int) ([]*models.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	query := `
		SELECT id, conversation_id, seq_id, sender_id, client_msg_id, content_type, content, created_at
		FROM messages
		WHERE conversation_id = $1 AND seq_id > $2
		ORDER BY seq_id ASC
		LIMIT $3;
	`

	rows, err := d.pool.QueryContext(ctx, query, convID, sinceSeqID, limit)
	if err != nil {
		return nil, fmt.Errorf("query failed for messages since seq: %w", err)
	}
	defer rows.Close()

	messages := make([]*models.Message, 0)
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(
			&m.ID,
			&m.ConversationID,
			&m.SeqID,
			&m.SenderID,
			&m.ClientMsgID,
			&m.ContentType,
			&m.Content,
			&m.CreatedAt,
		); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}

	return messages, nil
}

// UpdateReadCursor updates the watermark read cursor for a user in a conversation
func (d *DB) UpdateReadCursor(ctx context.Context, convID, userID string, seqID int64) error {
	query := `
		UPDATE conversation_participants
		SET last_read_seq_id = GREATEST(last_read_seq_id, $3)
		WHERE conversation_id = $1 AND user_id = $2;
	`
	_, err := d.pool.ExecContext(ctx, query, convID, userID, seqID)
	return err
}

// UpdateDeliveryCursor updates the watermark delivery cursor for a user
func (d *DB) UpdateDeliveryCursor(ctx context.Context, convID, userID string, seqID int64) error {
	query := `
		UPDATE conversation_participants
		SET last_delivered_seq_id = GREATEST(last_delivered_seq_id, $3)
		WHERE conversation_id = $1 AND user_id = $2;
	`
	_, err := d.pool.ExecContext(ctx, query, convID, userID, seqID)
	return err
}

// GetConversationParticipants returns all user IDs in a conversation
func (d *DB) GetConversationParticipants(ctx context.Context, convID string) ([]string, error) {
	query := `SELECT user_id FROM conversation_participants WHERE conversation_id = $1;`
	rows, err := d.pool.QueryContext(ctx, query, convID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var participants []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		participants = append(participants, uid)
	}
	return participants, nil
}
