package session

import (
	"sync"
	"time"

	"github.com/z9905080/affinity-lane/pkg/types"
)

// Manager manages session to worker mappings using consistent hashing
type Manager struct {
	mu            sync.RWMutex
	consistentHash *ConsistentHash
	sessions      map[string]*SessionState // sessionID -> session state
	workerStats   map[string]*WorkerSessionStats // workerID -> stats
}

// SessionState tracks the state of a session
type SessionState struct {
	SessionID  string
	WorkerID   string
	TaskCount  int64
	LastTaskAt time.Time
}

// WorkerSessionStats tracks session statistics for a worker
type WorkerSessionStats struct {
	WorkerID     string
	SessionCount int
	TaskCount    int64
}

// NewManager creates a new session manager
func NewManager(virtualNodes int) *Manager {
	return &Manager{
		consistentHash: NewConsistentHash(virtualNodes),
		sessions:      make(map[string]*SessionState),
		workerStats:   make(map[string]*WorkerSessionStats),
	}
}

// AddWorker adds a worker to the session manager
func (m *Manager) AddWorker(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.consistentHash.AddNode(workerID)
	if _, exists := m.workerStats[workerID]; !exists {
		m.workerStats[workerID] = &WorkerSessionStats{
			WorkerID:     workerID,
			SessionCount: 0,
			TaskCount:    0,
		}
	}
}

// RemoveWorker removes a worker from the session manager
func (m *Manager) RemoveWorker(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.consistentHash.RemoveNode(workerID)

	// Remove sessions associated with this worker
	for sessionID, state := range m.sessions {
		if state.WorkerID == workerID {
			delete(m.sessions, sessionID)
		}
	}

	delete(m.workerStats, workerID)
}

// GetWorkerForSession returns the worker ID for a given session
// If the session doesn't exist, it creates a new mapping
func (m *Manager) GetWorkerForSession(sessionID string) (string, error) {
	// Fast path: check if session already exists (read lock)
	m.mu.RLock()
	if state, exists := m.sessions[sessionID]; exists {
		workerID := state.WorkerID
		m.mu.RUnlock()
		return workerID, nil
	}
	m.mu.RUnlock()

	// Slow path: create new session mapping (write lock)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if state, exists := m.sessions[sessionID]; exists {
		return state.WorkerID, nil
	}

	// Get worker from consistent hash
	workerID, ok := m.consistentHash.GetNode(sessionID)
	if !ok {
		return "", types.ErrNoWorkersAvailable
	}

	// Create new session state
	m.sessions[sessionID] = &SessionState{
		SessionID:  sessionID,
		WorkerID:   workerID,
		TaskCount:  0,
		LastTaskAt: time.Now(),
	}

	// Update worker stats
	if stats, exists := m.workerStats[workerID]; exists {
		stats.SessionCount++
	}

	return workerID, nil
}

// RecordTask records a task execution for a session
func (m *Manager) RecordTask(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state, exists := m.sessions[sessionID]; exists {
		state.TaskCount++
		state.LastTaskAt = time.Now()

		if stats, exists := m.workerStats[state.WorkerID]; exists {
			stats.TaskCount++
		}
	}
}

// GetSessionInfo returns information about a session
func (m *Manager) GetSessionInfo(sessionID string) (*types.SessionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.sessions[sessionID]
	if !exists {
		return nil, types.ErrSessionNotFound
	}

	return &types.SessionInfo{
		SessionID:  state.SessionID,
		WorkerID:   state.WorkerID,
		TaskCount:  state.TaskCount,
		LastTaskAt: state.LastTaskAt,
	}, nil
}

// GetWorkerStats returns statistics for a worker
func (m *Manager) GetWorkerStats(workerID string) *WorkerSessionStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if stats, exists := m.workerStats[workerID]; exists {
		// Return a copy to avoid race conditions
		return &WorkerSessionStats{
			WorkerID:     stats.WorkerID,
			SessionCount: stats.SessionCount,
			TaskCount:    stats.TaskCount,
		}
	}
	return nil
}

// GetAllWorkerStats returns statistics for all workers
func (m *Manager) GetAllWorkerStats() map[string]*WorkerSessionStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*WorkerSessionStats)
	for workerID, stats := range m.workerStats {
		result[workerID] = &WorkerSessionStats{
			WorkerID:     stats.WorkerID,
			SessionCount: stats.SessionCount,
			TaskCount:    stats.TaskCount,
		}
	}
	return result
}

// GetActiveSessions returns the number of active sessions
func (m *Manager) GetActiveSessions() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// GetWorkerCount returns the number of workers
func (m *Manager) GetWorkerCount() int {
	return m.consistentHash.NodeCount()
}

// GetSessionsForWorker returns all session IDs assigned to a worker
func (m *Manager) GetSessionsForWorker(workerID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sessions := make([]string, 0)
	for sessionID, state := range m.sessions {
		if state.WorkerID == workerID {
			sessions = append(sessions, sessionID)
		}
	}
	return sessions
}

// CleanupIdleSessions removes sessions that haven't had tasks in the specified duration
func (m *Manager) CleanupIdleSessions(idleTimeout time.Duration) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	removed := 0

	for sessionID, state := range m.sessions {
		if now.Sub(state.LastTaskAt) > idleTimeout {
			if stats, exists := m.workerStats[state.WorkerID]; exists {
				stats.SessionCount--
			}
			delete(m.sessions, sessionID)
			removed++
		}
	}

	return removed
}
