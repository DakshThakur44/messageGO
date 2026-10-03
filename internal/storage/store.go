package storage

import (
	"sync"

	"messageGO/internal/models"
)

type OfflineStore interface {
	Save(recipientID string, env *models.Envelope) error
	GetAndClear(userID string) ([]*models.Envelope, error)
}

type MemoryStore struct {
	mu    sync.RWMutex
	queue map[string][]*models.Envelope
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		queue: make(map[string][]*models.Envelope),
	}
}

// Save appends an undelivered envelope to a recipient's offline queue.
func (m *MemoryStore) Save(recipientID string, env *models.Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queue[recipientID] = append(m.queue[recipientID], env)
	return nil
}

// GetAndClear retrieves all queued envelopes for a user and immediately purges them.
func (m *MemoryStore) GetAndClear(userID string) ([]*models.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	envelopes, exists := m.queue[userID]
	if !exists {
		return nil, nil
	}

	delete(m.queue, userID)
	return envelopes, nil
}
