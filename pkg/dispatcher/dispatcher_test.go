package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/z9905080/affinity-lane/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TaskTracker tracks executed tasks for testing
type TaskTracker struct {
	mu       sync.Mutex
	executed map[string][]string // sessionID -> taskIDs
}

func NewTaskTracker() *TaskTracker {
	return &TaskTracker{
		executed: make(map[string][]string),
	}
}

func (t *TaskTracker) RecordTask(sessionID, taskID string) {
	t.mu.Lock()
	t.executed[sessionID] = append(t.executed[sessionID], taskID)
	t.mu.Unlock()
}

func (t *TaskTracker) GetExecutedTaskIDs(sessionID string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.executed[sessionID]
}

// createTestTask creates a generic task for testing
func createTestTask(id, sessionID string, payload any, delay time.Duration, tracker *TaskTracker) *types.Task[any] {
	return &types.Task[any]{
		ID:        id,
		SessionID: sessionID,
		Payload:   payload,
		CreatedAt: time.Now(),
		OnExecute: func(ctx context.Context, task *types.Task[any]) (any, error) {
			if delay > 0 {
				time.Sleep(delay)
			}
			if tracker != nil {
				tracker.RecordTask(task.SessionID, task.ID)
			}
			return "success", nil
		},
	}
}

func TestNewDispatcher(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        5,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	require.NotNil(t, d)

	assert.True(t, d.IsRunning())
	assert.Equal(t, 5, len(d.workers))

	// Cleanup
	err = d.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestDispatcher_Submit(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        3,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit a task
	task := createTestTask("task1", "session1", "data", 10*time.Millisecond, nil)

	err = d.Submit(context.Background(), task)
	assert.NoError(t, err)

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)

	stats := d.Stats()
	assert.Equal(t, int64(1), stats.TotalSubmitted)
	assert.GreaterOrEqual(t, stats.TotalCompleted, int64(1))
}

func TestDispatcher_SessionAffinity(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        3,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit multiple tasks with same session
	sessionID := "session1"
	numTasks := 10

	for i := 0; i < numTasks; i++ {
		task := createTestTask(fmt.Sprintf("task%d", i), sessionID, i, 10*time.Millisecond, nil)
		err := d.Submit(context.Background(), task)
		require.NoError(t, err)
	}

	// Wait for all tasks to be processed
	time.Sleep(200 * time.Millisecond)

	// Verify all tasks went to the same worker
	workerID := ""
	for _, w := range d.workers {
		stats := w.Stats()
		if stats.TasksProcessed > 0 {
			if workerID == "" {
				workerID = w.ID()
			} else {
				// All tasks should be on one worker for this session
				assert.Equal(t, int64(0), stats.TasksProcessed,
					"Tasks should only be on one worker")
			}
		}
	}

	assert.NotEmpty(t, workerID, "Should have found worker with tasks")
}

func TestDispatcher_TaskOrdering(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        2,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	tracker := NewTaskTracker()

	// Submit tasks in order
	sessionID := "session1"
	numTasks := 20
	expectedOrder := make([]string, numTasks)

	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("task%d", i)
		expectedOrder[i] = taskID

		task := createTestTask(taskID, sessionID, i, 5*time.Millisecond, tracker)
		err := d.Submit(context.Background(), task)
		require.NoError(t, err)
	}

	// Wait for all tasks to complete
	time.Sleep(300 * time.Millisecond)

	// Verify execution order
	executedTasks := tracker.GetExecutedTaskIDs(sessionID)
	require.Len(t, executedTasks, numTasks)

	for i, taskID := range executedTasks {
		assert.Equal(t, expectedOrder[i], taskID,
			"Task %d should be %s but got %s", i, expectedOrder[i], taskID)
	}
}

func TestDispatcher_MultipleSessionsConcurrent(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        5,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	tracker := NewTaskTracker()

	// Submit tasks from multiple sessions concurrently
	numSessions := 20
	tasksPerSession := 10

	var wg sync.WaitGroup
	for s := 0; s < numSessions; s++ {
		sessionID := fmt.Sprintf("session%d", s)
		wg.Add(1)

		go func(sid string) {
			defer wg.Done()
			for i := 0; i < tasksPerSession; i++ {
				task := createTestTask(fmt.Sprintf("%s-task%d", sid, i), sid, i, 5*time.Millisecond, tracker)
				err := d.Submit(context.Background(), task)
				assert.NoError(t, err)
			}
		}(sessionID)
	}

	wg.Wait()

	// Wait for all tasks to complete
	time.Sleep(500 * time.Millisecond)

	stats := d.Stats()
	expectedTotal := int64(numSessions * tasksPerSession)
	assert.Equal(t, expectedTotal, stats.TotalSubmitted)

	// Allow some tasks to still be processing
	assert.GreaterOrEqual(t, stats.TotalCompleted, expectedTotal-10)

	// Verify each session's tasks are in order
	for s := 0; s < numSessions; s++ {
		sessionID := fmt.Sprintf("session%d", s)
		executedTasks := tracker.GetExecutedTaskIDs(sessionID)

		// Verify order
		for i, taskID := range executedTasks {
			expectedID := fmt.Sprintf("%s-task%d", sessionID, i)
			assert.Equal(t, expectedID, taskID)
		}
	}
}

func TestDispatcher_LoadBalancing(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:         5,
		QueueSize:        100,
		TaskTimeout:      5 * time.Second,
		ShutdownTimeout:  5 * time.Second,
		VirtualNodeCount: 150,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit tasks from many sessions
	numSessions := 100
	for s := 0; s < numSessions; s++ {
		task := createTestTask(fmt.Sprintf("task%d", s), fmt.Sprintf("session%d", s), s, 5*time.Millisecond, nil)
		err := d.Submit(context.Background(), task)
		require.NoError(t, err)
	}

	// Wait for tasks to be processed
	time.Sleep(200 * time.Millisecond)

	// Check distribution
	distribution := d.GetSessionDistribution()
	t.Logf("Session distribution: %v", distribution)

	// Each worker should have some sessions (roughly numSessions/PoolSize ± 40%)
	expectedPerWorker := numSessions / config.PoolSize
	for workerID, count := range distribution {
		t.Logf("Worker %s: %d sessions", workerID, count)

		// Allow 40% deviation
		minExpected := int(float64(expectedPerWorker) * 0.6)
		maxExpected := int(float64(expectedPerWorker) * 1.4)

		assert.GreaterOrEqual(t, count, minExpected,
			"Worker %s has too few sessions", workerID)
		assert.LessOrEqual(t, count, maxExpected,
			"Worker %s has too many sessions", workerID)
	}
}

func TestDispatcher_Shutdown(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        3,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)

	// Submit some tasks
	for i := 0; i < 10; i++ {
		task := createTestTask(fmt.Sprintf("task%d", i), fmt.Sprintf("session%d", i%3), i, 10*time.Millisecond, nil)
		err := d.Submit(context.Background(), task)
		require.NoError(t, err)
	}

	// Shutdown
	err = d.Shutdown(context.Background())
	assert.NoError(t, err)
	assert.False(t, d.IsRunning())

	// Submitting after shutdown should fail
	task := createTestTask("task-after-shutdown", "session1", "data", 0, nil)
	err = d.Submit(context.Background(), task)
	assert.Error(t, err)
	assert.Equal(t, types.ErrDispatcherClosed, err)
}

func TestDispatcher_AddWorker(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        3,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Initial pool size
	assert.Equal(t, 3, d.GetPoolSize())

	// Add a worker
	workerID, err := d.AddWorker(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, workerID)
	assert.Equal(t, 4, d.GetPoolSize())

	// Add another worker
	workerID2, err := d.AddWorker(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, workerID2)
	assert.NotEqual(t, workerID, workerID2)
	assert.Equal(t, 5, d.GetPoolSize())

	// Verify new workers are functional
	task := createTestTask("test-task", "test-session", "data", 0, nil)
	err = d.Submit(context.Background(), task)
	assert.NoError(t, err)
}

func TestDispatcher_ResizePool(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        5,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Initial size
	assert.Equal(t, 5, d.GetPoolSize())

	// Scale up
	err = d.ResizePool(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 10, d.GetPoolSize())

	// Scale up more
	err = d.ResizePool(context.Background(), 15)
	require.NoError(t, err)
	assert.Equal(t, 15, d.GetPoolSize())

	// Same size (no-op)
	err = d.ResizePool(context.Background(), 15)
	require.NoError(t, err)
	assert.Equal(t, 15, d.GetPoolSize())

	// Scale down should return error
	err = d.ResizePool(context.Background(), 3)
	assert.Error(t, err)
	assert.Equal(t, 15, d.GetPoolSize()) // Size should remain unchanged

	// Invalid size
	err = d.ResizePool(context.Background(), 0)
	assert.Error(t, err)

	err = d.ResizePool(context.Background(), -1)
	assert.Error(t, err)

	// Verify dispatcher still works after resizing
	for i := 0; i < 20; i++ {
		task := createTestTask(fmt.Sprintf("task-%d", i), fmt.Sprintf("session-%d", i%5), i, 10*time.Millisecond, nil)
		err = d.Submit(context.Background(), task)
		assert.NoError(t, err)
	}

	time.Sleep(300 * time.Millisecond)

	stats := d.Stats()
	assert.Equal(t, int64(20), stats.TotalSubmitted)
	assert.GreaterOrEqual(t, stats.TotalCompleted, int64(15))
}

func TestDispatcher_DynamicResizeUnderLoad(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        3,
		QueueSize:       200,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Start submitting tasks continuously
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var submitWg sync.WaitGroup
	submitWg.Add(1)
	go func() {
		defer submitWg.Done()
		taskNum := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				task := createTestTask(fmt.Sprintf("task-%d", taskNum), fmt.Sprintf("session-%d", taskNum%10), taskNum, 20*time.Millisecond, nil)
				d.Submit(context.Background(), task)
				taskNum++
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Let some tasks accumulate
	time.Sleep(100 * time.Millisecond)

	// Scale up while under load
	err = d.ResizePool(context.Background(), 8)
	require.NoError(t, err)
	assert.Equal(t, 8, d.GetPoolSize())

	time.Sleep(200 * time.Millisecond)

	// Scale up more while under load
	err = d.ResizePool(context.Background(), 12)
	require.NoError(t, err)
	assert.Equal(t, 12, d.GetPoolSize())

	time.Sleep(200 * time.Millisecond)

	// Stop submitting
	cancel()
	submitWg.Wait()

	// Wait for tasks to complete
	time.Sleep(500 * time.Millisecond)

	stats := d.Stats()
	t.Logf("Total submitted: %d, completed: %d, failed: %d",
		stats.TotalSubmitted, stats.TotalCompleted, stats.TotalFailed)

	// Should have processed most tasks successfully
	assert.Greater(t, stats.TotalCompleted, int64(50))
	assert.Equal(t, int64(0), stats.TotalFailed)
}

func BenchmarkDispatcher_Submit(b *testing.B) {
	config := &types.WorkerConfig{
		PoolSize:        10,
		QueueSize:       10000,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}

	d, err := NewDispatcher(config)
	require.NoError(b, err)
	defer d.Shutdown(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := createTestTask(fmt.Sprintf("task%d", i), fmt.Sprintf("session%d", i%100), i, 0, nil)
		d.Submit(context.Background(), task)
	}
}
