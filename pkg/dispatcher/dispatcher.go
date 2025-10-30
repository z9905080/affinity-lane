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
func NewDispatcher(config *types.WorkerConfig) (*Dispatcher, error) {
	if config == nil {
		config = types.DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	d := &Dispatcher{
		config:         config,
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

		w := worker.New(workerID, d.config)
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
func (d *Dispatcher) Submit(ctx context.Context, task types.InfTask) error {
	if !d.running.Load() {
		return types.ErrDispatcherClosed
	}

	if task == nil || task.GetSessionID() == "" || task.GetID() == "" {
		return types.ErrInvalidTask
	}

	// Get worker for this session
	workerID, err := d.sessionManager.GetWorkerForSession(task.GetSessionID())
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
	d.sessionManager.RecordTask(task.GetSessionID())

	return nil
}

// SubmitBatch submits multiple tasks in batch
func (d *Dispatcher) SubmitBatch(ctx context.Context, tasks []types.InfTask) error {
	if !d.running.Load() {
		return types.ErrDispatcherClosed
	}

	// Submit each task
	for _, task := range tasks {
		if err := d.Submit(ctx, task); err != nil {
			return fmt.Errorf("failed to submit task %s: %w", task.GetID(), err)
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
	var totalCompleted, totalFailed int64

	for _, w := range d.workers {
		stats := w.Stats()
		queuedTasks += stats.QueueLength
		totalCompleted += stats.TasksProcessed
		totalFailed += stats.TasksFailed
	}
	d.workersMu.RUnlock()

	return &types.DispatcherStats{
		TotalSubmitted: d.totalSubmitted.Load(),
		TotalCompleted: totalCompleted,
		TotalFailed:    totalFailed,
		ActiveSessions: d.sessionManager.GetActiveSessions(),
		QueuedTasks:    queuedTasks,
		Workers:        workerCount,
	}
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
	w := worker.New(workerID, d.config)

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

// ResizePool adjusts the pool size (only supports scaling up)
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

	if targetSize < currentSize {
		return fmt.Errorf("scaling down is not supported; current size: %d, target size: %d", currentSize, targetSize)
	}

	// Add workers
	numToAdd := targetSize - currentSize
	for i := 0; i < numToAdd; i++ {
		if _, err := d.AddWorker(ctx); err != nil {
			return fmt.Errorf("failed to add worker: %w", err)
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
