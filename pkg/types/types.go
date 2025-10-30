package types

import (
	"context"
	"time"
)

// InfTask defines the interface that all tasks must implement
type InfTask interface {
	GetID() string
	GetSessionID() string
	GetCreatedAt() time.Time
	Execute(ctx context.Context) (any, error)
}

// Task represents a generic unit of work to be executed
type Task[T any] struct {
	ID        string      // Unique task identifier
	SessionID string      // Session identifier for routing and ordering
	Payload   T           // Task data (generic type)
	CreatedAt time.Time   // Task creation timestamp
	OnExecute func(ctx context.Context, task *Task[T]) (any, error) // Execution function
}

// GetID returns the task ID
func (t *Task[T]) GetID() string {
	return t.ID
}

// GetSessionID returns the session ID
func (t *Task[T]) GetSessionID() string {
	return t.SessionID
}

// GetCreatedAt returns the task creation time
func (t *Task[T]) GetCreatedAt() time.Time {
	return t.CreatedAt
}

// Execute executes the task
func (t *Task[T]) Execute(ctx context.Context) (any, error) {
	if t.OnExecute != nil {
		return t.OnExecute(ctx, t)
	}
	return nil, ErrNoExecutor
}

// TaskResult represents the result of task execution
type TaskResult[T any] struct {
	TaskID    string        // Task ID
	SessionID string        // Session ID
	Result    T             // Execution result (generic type)
	Error     error         // Error if execution failed
	Duration  time.Duration // Execution duration
	StartedAt time.Time     // Task start time
	EndedAt   time.Time     // Task end time
}

// WorkerConfig defines configuration for the worker pool
type WorkerConfig struct {
	PoolSize         int           // Number of workers in the pool
	QueueSize        int           // Buffer size for each worker's task queue
	MaxSessions      int           // Maximum sessions per worker (0 = unlimited)
	TaskTimeout      time.Duration // Timeout for individual task execution
	ShutdownTimeout  time.Duration // Timeout for graceful shutdown
	VirtualNodeCount int           // Number of virtual nodes for consistent hashing (default: 150)
}

// DefaultConfig returns a default configuration
func DefaultConfig() *WorkerConfig {
	return &WorkerConfig{
		PoolSize:         10,
		QueueSize:        1000,
		MaxSessions:      0, // unlimited
		TaskTimeout:      30 * time.Second,
		ShutdownTimeout:  30 * time.Second,
		VirtualNodeCount: 150,
	}
}

// Validate checks if the configuration is valid
func (c *WorkerConfig) Validate() error {
	if c.PoolSize <= 0 {
		return ErrInvalidPoolSize
	}
	if c.QueueSize <= 0 {
		return ErrInvalidQueueSize
	}
	if c.TaskTimeout <= 0 {
		return ErrInvalidTaskTimeout
	}
	if c.ShutdownTimeout <= 0 {
		return ErrInvalidShutdownTimeout
	}
	if c.VirtualNodeCount <= 0 {
		c.VirtualNodeCount = 150 // default
	}
	return nil
}

// DispatcherStats provides statistics about the dispatcher
type DispatcherStats struct {
	TotalSubmitted int64 // Total number of tasks submitted
	TotalCompleted int64 // Total number of tasks completed
	TotalFailed    int64 // Total number of tasks that failed
	ActiveSessions int   // Number of active sessions
	QueuedTasks    int   // Number of tasks currently queued
	Workers        int   // Number of active workers
}

// WorkerStats provides statistics about a single worker
type WorkerStats struct {
	WorkerID        string        // Worker identifier
	SessionCount    int           // Number of sessions assigned to this worker
	TasksProcessed  int64         // Total tasks processed
	TasksFailed     int64         // Total tasks failed
	AvgTaskDuration time.Duration // Average task execution time
	QueueLength     int           // Current queue length
	IsRunning       bool          // Whether the worker is running
}

// SessionInfo provides information about a session
type SessionInfo struct {
	SessionID   string    // Session identifier
	WorkerID    string    // Worker handling this session
	TaskCount   int64     // Number of tasks processed for this session
	LastTaskAt  time.Time // Timestamp of last task
	QueueLength int       // Current queue length for this session
}
