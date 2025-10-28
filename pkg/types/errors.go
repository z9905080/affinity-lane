package types

import "errors"

// Configuration errors
var (
	ErrInvalidPoolSize         = errors.New("invalid pool size: must be greater than 0")
	ErrInvalidQueueSize        = errors.New("invalid queue size: must be greater than 0")
	ErrInvalidTaskTimeout      = errors.New("invalid task timeout: must be greater than 0")
	ErrInvalidShutdownTimeout  = errors.New("invalid shutdown timeout: must be greater than 0")
)

// Dispatcher errors
var (
	ErrDispatcherClosed   = errors.New("dispatcher is closed")
	ErrDispatcherNotReady = errors.New("dispatcher is not ready")
	ErrTaskQueueFull      = errors.New("task queue is full")
	ErrInvalidTask        = errors.New("invalid task: missing required fields")
	ErrNoWorkersAvailable = errors.New("no workers available")
)

// Worker errors
var (
	ErrWorkerStopped  = errors.New("worker is stopped")
	ErrWorkerBusy     = errors.New("worker is busy")
	ErrTaskTimeout    = errors.New("task execution timeout")
	ErrTaskPanic      = errors.New("task execution panicked")
)

// Session errors
var (
	ErrSessionNotFound = errors.New("session not found")
	ErrInvalidSessionID = errors.New("invalid session ID")
)
