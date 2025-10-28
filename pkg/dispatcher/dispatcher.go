package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/z9905080/affinity-lane/pkg/session"
	"github.com/z9905080/affinity-lane/pkg/types"
	"github.com/z9905080/affinity-lane/pkg/worker"
)

// Dispatcher distributes tasks to workers based on session affinity
type Dispatcher struct {
	config         *types.WorkerConfig
	handler        types.TaskHandler
	sessionManager *session.Manager
	workers        map[string]*worker.Worker
	workersMu      sync.RWMutex // protects workers map
	nextWorkerID   atomic.Int64 // for generating unique worker IDs

	// Statistics
	totalSubmitted atomic.Int64
	totalCompleted atomic.Int64
	totalFailed    atomic.Int64

	// State
	running atomic.Bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(config *types.WorkerConfig, handler types.TaskHandler) (*Dispatcher, error) {
	if config == nil {
		config = types.DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if handler == nil {
		return nil, fmt.Errorf("task handler cannot be nil")
	}

	d := &Dispatcher{
		config:         config,
		handler:        handler,
		sessionManager: session.NewManager(config.VirtualNodeCount),
		workers:        make(map[string]*worker.Worker),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}

	// Initialize workers
	if err := d.initializeWorkers(); err != nil {
		return nil, err
	}

	d.running.Store(true)

	return d, nil
}

// initializeWorkers creates and starts all workers
func (d *Dispatcher) initializeWorkers() error {
	for i := 0; i < d.config.PoolSize; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		d.nextWorkerID.Store(int64(i))

		w := worker.New(workerID, d.config, d.handler)
		d.workers[workerID] = w

		// Add worker to session manager
		d.sessionManager.AddWorker(workerID)

		// Start worker
		if err := w.Start(context.Background()); err != nil {
			return fmt.Errorf("failed to start worker %s: %w", workerID, err)
		}
	}

	return nil
}

// Submit submits a task to the dispatcher
func (d *Dispatcher) Submit(ctx context.Context, task *types.Task) error {
	if !d.running.Load() {
		return types.ErrDispatcherClosed
	}

	if task == nil || task.SessionID == "" || task.ID == "" {
		return types.ErrInvalidTask
	}

	// Set creation time if not set
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	// Get worker for this session
	workerID, err := d.sessionManager.GetWorkerForSession(task.SessionID)
	if err != nil {
		return err
	}

	// Get worker (with read lock)
	d.workersMu.RLock()
	w, exists := d.workers[workerID]
	d.workersMu.RUnlock()

	if !exists {
		return types.ErrNoWorkersAvailable
	}

	// Submit task to worker
	if err := w.AddTask(task); err != nil {
		return err
	}

	// Update statistics
	d.totalSubmitted.Add(1)
	d.sessionManager.RecordTask(task.SessionID)

	return nil
}

// SubmitBatch submits multiple tasks in batch
func (d *Dispatcher) SubmitBatch(ctx context.Context, tasks []*types.Task) error {
	if !d.running.Load() {
		return types.ErrDispatcherClosed
	}

	// Submit each task
	for _, task := range tasks {
		if err := d.Submit(ctx, task); err != nil {
			return fmt.Errorf("failed to submit task %s: %w", task.ID, err)
		}
	}

	return nil
}

// Shutdown gracefully shuts down the dispatcher
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	if !d.running.CompareAndSwap(true, false) {
		return nil // already stopped
	}

	close(d.stopCh)

	// Create shutdown context with timeout
	shutdownCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(ctx, d.config.ShutdownTimeout)
		defer cancel()
	}

	// Get workers snapshot (with read lock)
	d.workersMu.RLock()
	workersList := make([]*worker.Worker, 0, len(d.workers))
	for _, w := range d.workers {
		workersList = append(workersList, w)
	}
	d.workersMu.RUnlock()

	// Stop all workers
	errCh := make(chan error, len(workersList))
	for _, w := range workersList {
		go func(worker *worker.Worker) {
			errCh <- worker.Stop(shutdownCtx)
		}(w)
	}

	// Wait for all workers to stop
	var lastErr error
	for i := 0; i < len(workersList); i++ {
		if err := <-errCh; err != nil {
			lastErr = err
		}
	}

	close(d.doneCh)
	return lastErr
}

// Stats returns dispatcher statistics
func (d *Dispatcher) Stats() *types.DispatcherStats {
	d.workersMu.RLock()
	queuedTasks := 0
	workerCount := len(d.workers)
	for _, w := range d.workers {
		stats := w.Stats()
		queuedTasks += stats.QueueLength
	}
	d.workersMu.RUnlock()

	return &types.DispatcherStats{
		TotalSubmitted: d.totalSubmitted.Load(),
		TotalCompleted: d.getTotalCompleted(),
		TotalFailed:    d.getTotalFailed(),
		ActiveSessions: d.sessionManager.GetActiveSessions(),
		QueuedTasks:    queuedTasks,
		Workers:        workerCount,
	}
}

// getTotalCompleted returns total completed tasks across all workers
func (d *Dispatcher) getTotalCompleted() int64 {
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()

	var total int64
	for _, w := range d.workers {
		stats := w.Stats()
		total += stats.TasksProcessed
	}
	return total
}

// getTotalFailed returns total failed tasks across all workers
func (d *Dispatcher) getTotalFailed() int64 {
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()

	var total int64
	for _, w := range d.workers {
		stats := w.Stats()
		total += stats.TasksFailed
	}
	return total
}

// GetWorkerStats returns statistics for all workers
func (d *Dispatcher) GetWorkerStats() []*types.WorkerStats {
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()

	stats := make([]*types.WorkerStats, 0, len(d.workers))
	for _, w := range d.workers {
		stats = append(stats, w.Stats())
	}
	return stats
}

// GetSessionInfo returns information about a session
func (d *Dispatcher) GetSessionInfo(sessionID string) (*types.SessionInfo, error) {
	info, err := d.sessionManager.GetSessionInfo(sessionID)
	if err != nil {
		return nil, err
	}

	// Add queue length information
	d.workersMu.RLock()
	w, exists := d.workers[info.WorkerID]
	d.workersMu.RUnlock()

	if exists {
		info.QueueLength = w.GetSessionQueueLength(sessionID)
	}

	return info, nil
}

// GetSessionDistribution returns the distribution of sessions across workers
func (d *Dispatcher) GetSessionDistribution() map[string]int {
	distribution := make(map[string]int)
	allStats := d.sessionManager.GetAllWorkerStats()

	for workerID, stats := range allStats {
		distribution[workerID] = stats.SessionCount
	}

	return distribution
}

// IsRunning returns whether the dispatcher is running
func (d *Dispatcher) IsRunning() bool {
	return d.running.Load()
}

// CleanupIdleSessions removes sessions that haven't had tasks in the specified duration
func (d *Dispatcher) CleanupIdleSessions(idleTimeout time.Duration) int {
	return d.sessionManager.CleanupIdleSessions(idleTimeout)
}

// AddWorker adds a new worker to the pool
func (d *Dispatcher) AddWorker(ctx context.Context) (string, error) {
	if !d.running.Load() {
		return "", types.ErrDispatcherClosed
	}

	// Generate unique worker ID
	workerNum := d.nextWorkerID.Add(1)
	workerID := fmt.Sprintf("worker-%d", workerNum)

	// Create new worker
	w := worker.New(workerID, d.config, d.handler)

	// Start worker
	if err := w.Start(ctx); err != nil {
		return "", fmt.Errorf("failed to start worker %s: %w", workerID, err)
	}

	// Add to workers map (with write lock)
	d.workersMu.Lock()
	d.workers[workerID] = w
	d.workersMu.Unlock()

	// Add to session manager
	d.sessionManager.AddWorker(workerID)

	return workerID, nil
}

// RemoveWorker removes a worker from the pool
func (d *Dispatcher) RemoveWorker(ctx context.Context, workerID string) error {
	if !d.running.Load() {
		return types.ErrDispatcherClosed
	}

	// Get worker (with read lock first)
	d.workersMu.RLock()
	w, exists := d.workers[workerID]
	d.workersMu.RUnlock()

	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	// Stop worker
	if err := w.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop worker %s: %w", workerID, err)
	}

	// Remove from session manager first (to stop routing new tasks)
	d.sessionManager.RemoveWorker(workerID)

	// Remove from workers map (with write lock)
	d.workersMu.Lock()
	delete(d.workers, workerID)
	d.workersMu.Unlock()

	return nil
}

// ResizePool adjusts the pool to the target size
func (d *Dispatcher) ResizePool(ctx context.Context, targetSize int) error {
	if !d.running.Load() {
		return types.ErrDispatcherClosed
	}

	if targetSize <= 0 {
		return fmt.Errorf("target size must be greater than 0")
	}

	currentSize := d.GetPoolSize()

	if targetSize == currentSize {
		return nil // Already at target size
	}

	if targetSize > currentSize {
		// Add workers
		numToAdd := targetSize - currentSize
		for i := 0; i < numToAdd; i++ {
			if _, err := d.AddWorker(ctx); err != nil {
				return fmt.Errorf("failed to add worker: %w", err)
			}
		}
	} else {
		// Remove workers
		numToRemove := currentSize - targetSize

		// Get list of workers to remove
		d.workersMu.RLock()
		workersToRemove := make([]string, 0, numToRemove)
		for workerID := range d.workers {
			if len(workersToRemove) >= numToRemove {
				break
			}
			workersToRemove = append(workersToRemove, workerID)
		}
		d.workersMu.RUnlock()

		// Remove selected workers
		for _, workerID := range workersToRemove {
			if err := d.RemoveWorker(ctx, workerID); err != nil {
				return fmt.Errorf("failed to remove worker %s: %w", workerID, err)
			}
		}
	}

	return nil
}

// GetPoolSize returns the current number of workers in the pool
func (d *Dispatcher) GetPoolSize() int {
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	return len(d.workers)
}
