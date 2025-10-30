package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/z9905080/affinity-lane/pkg/types"
)

// Worker processes tasks for multiple sessions
type Worker struct {
	id     string
	config *types.WorkerConfig

	// Session queues: sessionID -> task channel
	sessionQueues sync.Map

	// Statistics
	tasksProcessed atomic.Int64
	tasksFailed    atomic.Int64
	totalDuration  atomic.Int64 // nanoseconds

	// Control channels
	stopCh chan struct{}
	doneCh chan struct{}

	// State
	running atomic.Bool
	mu      sync.RWMutex
}

// sessionQueue represents a queue for a single session
type sessionQueue struct {
	sessionID string
	taskCh    chan types.InfTask
	ctx       context.Context
	cancel    context.CancelFunc
	active    atomic.Bool
}

// New creates a new worker
func New(id string, config *types.WorkerConfig) *Worker {
	return &Worker{
		id:     id,
		config: config,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start starts the worker
func (w *Worker) Start(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("worker %s already running", w.id)
	}

	go w.run(ctx)
	return nil
}

// Stop stops the worker gracefully
func (w *Worker) Stop(ctx context.Context) error {
	if !w.running.Load() {
		return nil
	}

	close(w.stopCh)

	// Wait for worker to finish with timeout
	select {
	case <-w.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AddTask adds a task to the worker's queue
func (w *Worker) AddTask(task types.InfTask) error {
	if !w.running.Load() {
		return types.ErrWorkerStopped
	}

	if task == nil || task.GetSessionID() == "" {
		return types.ErrInvalidTask
	}

	// Get or create session queue
	queue := w.getOrCreateSessionQueue(task.GetSessionID())

	// Try to send task to queue (non-blocking)
	select {
	case queue.taskCh <- task:
		return nil
	default:
		return types.ErrTaskQueueFull
	}
}

// getOrCreateSessionQueue gets or creates a session queue
func (w *Worker) getOrCreateSessionQueue(sessionID string) *sessionQueue {
	// Try to load existing queue
	if val, ok := w.sessionQueues.Load(sessionID); ok {
		return val.(*sessionQueue)
	}

	// Create new queue
	ctx, cancel := context.WithCancel(context.Background())
	queue := &sessionQueue{
		sessionID: sessionID,
		taskCh:    make(chan types.InfTask, w.config.QueueSize),
		ctx:       ctx,
		cancel:    cancel,
	}
	queue.active.Store(true)

	// Store in map (may race with another goroutine)
	actual, loaded := w.sessionQueues.LoadOrStore(sessionID, queue)
	if loaded {
		// Another goroutine created it first, use that one
		cancel()
		return actual.(*sessionQueue)
	}

	// Start processing goroutine for this session
	go w.processSessionQueue(queue)

	return queue
}

// run is the main worker loop
func (w *Worker) run(ctx context.Context) {
	defer close(w.doneCh)
	defer w.running.Store(false)

	<-w.stopCh

	// Stop all session queues
	w.sessionQueues.Range(func(key, value interface{}) bool {
		queue := value.(*sessionQueue)
		queue.cancel()
		return true
	})

	// Wait for all session queues to drain with timeout
	deadline := time.Now().Add(w.config.ShutdownTimeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		allDone := true
		w.sessionQueues.Range(func(key, value interface{}) bool {
			queue := value.(*sessionQueue)
			if queue.active.Load() {
				allDone = false
				return false // stop iteration
			}
			return true
		})

		if allDone {
			break
		}

		<-ticker.C
	}
}

// processSessionQueue processes tasks for a single session
func (w *Worker) processSessionQueue(queue *sessionQueue) {
	defer queue.active.Store(false)

	for {
		select {
		case <-queue.ctx.Done():
			// Drain remaining tasks before exiting
			w.drainQueue(queue)
			return

		case task := <-queue.taskCh:
			w.executeTask(queue.ctx, task)
		}
	}
}

// drainQueue processes remaining tasks in the queue
func (w *Worker) drainQueue(queue *sessionQueue) {
	for {
		select {
		case task := <-queue.taskCh:
			w.executeTask(context.Background(), task)
		default:
			return
		}
	}
}

// executeTask executes a single task with timeout and panic recovery
func (w *Worker) executeTask(ctx context.Context, task types.InfTask) {
	startTime := time.Now()

	// Create context with timeout
	taskCtx, cancel := context.WithTimeout(ctx, w.config.TaskTimeout)
	defer cancel()

	// Execute with panic recovery
	err := w.safeExecute(taskCtx, task)

	// Update statistics
	duration := time.Since(startTime)
	w.totalDuration.Add(duration.Nanoseconds())

	if err != nil {
		w.tasksFailed.Add(1)
	} else {
		w.tasksProcessed.Add(1)
	}
}

// safeExecute executes a task with panic recovery
func (w *Worker) safeExecute(ctx context.Context, task types.InfTask) (err error) {
	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", types.ErrTaskPanic, r)
		}
	}()

	// Execute task
	_, err = task.Execute(ctx)

	// Handle timeout
	if ctx.Err() == context.DeadlineExceeded {
		err = types.ErrTaskTimeout
	}

	return err
}

// Stats returns worker statistics
func (w *Worker) Stats() *types.WorkerStats {
	processed := w.tasksProcessed.Load()
	failed := w.tasksFailed.Load()
	totalDuration := time.Duration(w.totalDuration.Load())

	var avgDuration time.Duration
	if processed > 0 {
		avgDuration = totalDuration / time.Duration(processed)
	}

	sessionCount := 0
	queueLength := 0
	w.sessionQueues.Range(func(key, value interface{}) bool {
		sessionCount++
		queue := value.(*sessionQueue)
		queueLength += len(queue.taskCh)
		return true
	})

	return &types.WorkerStats{
		WorkerID:        w.id,
		SessionCount:    sessionCount,
		TasksProcessed:  processed,
		TasksFailed:     failed,
		AvgTaskDuration: avgDuration,
		QueueLength:     queueLength,
		IsRunning:       w.running.Load(),
	}
}

// ID returns the worker ID
func (w *Worker) ID() string {
	return w.id
}

// IsRunning returns whether the worker is running
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}

// GetSessionQueueLength returns the queue length for a specific session
func (w *Worker) GetSessionQueueLength(sessionID string) int {
	if val, ok := w.sessionQueues.Load(sessionID); ok {
		queue := val.(*sessionQueue)
		return len(queue.taskCh)
	}
	return 0
}
