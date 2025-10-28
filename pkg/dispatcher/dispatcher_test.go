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

// MockHandler is a simple task handler for testing
type MockHandler struct {
	mu       sync.Mutex
	executed map[string][]*types.Task // sessionID -> tasks
	delay    time.Duration
}

func NewMockHandler(delay time.Duration) *MockHandler {
	return &MockHandler{
		executed: make(map[string][]*types.Task),
		delay:    delay,
	}
}

func (h *MockHandler) Handle(ctx context.Context, task *types.Task) (*types.TaskResult, error) {
	if h.delay > 0 {
		time.Sleep(h.delay)
	}

	h.mu.Lock()
	h.executed[task.SessionID] = append(h.executed[task.SessionID], task)
	h.mu.Unlock()

	return &types.TaskResult{
		TaskID:    task.ID,
		SessionID: task.SessionID,
		Result:    "success",
	}, nil
}

func (h *MockHandler) GetExecutedTasks(sessionID string) []*types.Task {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.executed[sessionID]
}

func TestNewDispatcher(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:    5,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(0)

	d, err := NewDispatcher(config, handler)
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
		PoolSize:    3,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(10 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit a task
	task := &types.Task{
		ID:        "task1",
		SessionID: "session1",
		Payload:   "data",
	}

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
		PoolSize:    3,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(10 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit multiple tasks with same session
	sessionID := "session1"
	numTasks := 10

	for i := 0; i < numTasks; i++ {
		task := &types.Task{
			ID:        fmt.Sprintf("task%d", i),
			SessionID: sessionID,
			Payload:   i,
		}
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
		PoolSize:    2,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(5 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit tasks in order
	sessionID := "session1"
	numTasks := 20
	expectedOrder := make([]string, numTasks)

	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("task%d", i)
		expectedOrder[i] = taskID

		task := &types.Task{
			ID:        taskID,
			SessionID: sessionID,
			Payload:   i,
		}
		err := d.Submit(context.Background(), task)
		require.NoError(t, err)
	}

	// Wait for all tasks to complete
	time.Sleep(300 * time.Millisecond)

	// Verify execution order
	executedTasks := handler.GetExecutedTasks(sessionID)
	require.Len(t, executedTasks, numTasks)

	for i, task := range executedTasks {
		assert.Equal(t, expectedOrder[i], task.ID,
			"Task %d should be %s but got %s", i, expectedOrder[i], task.ID)
	}
}

func TestDispatcher_MultipleSessionsConcurrent(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:    5,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(5 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

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
				task := &types.Task{
					ID:        fmt.Sprintf("%s-task%d", sid, i),
					SessionID: sid,
					Payload:   i,
				}
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
		executedTasks := handler.GetExecutedTasks(sessionID)

		// Verify order
		for i, task := range executedTasks {
			expectedID := fmt.Sprintf("%s-task%d", sessionID, i)
			assert.Equal(t, expectedID, task.ID)
		}
	}
}

func TestDispatcher_LoadBalancing(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:    5,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
		VirtualNodeCount: 150,
	}
	handler := NewMockHandler(5 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Submit tasks from many sessions
	numSessions := 100
	for s := 0; s < numSessions; s++ {
		task := &types.Task{
			ID:        fmt.Sprintf("task%d", s),
			SessionID: fmt.Sprintf("session%d", s),
			Payload:   s,
		}
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
		PoolSize:    3,
		QueueSize:   100,
		TaskTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(10 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)

	// Submit some tasks
	for i := 0; i < 10; i++ {
		task := &types.Task{
			ID:        fmt.Sprintf("task%d", i),
			SessionID: fmt.Sprintf("session%d", i%3),
			Payload:   i,
		}
		err := d.Submit(context.Background(), task)
		require.NoError(t, err)
	}

	// Shutdown
	err = d.Shutdown(context.Background())
	assert.NoError(t, err)
	assert.False(t, d.IsRunning())

	// Submitting after shutdown should fail
	task := &types.Task{
		ID:        "task-after-shutdown",
		SessionID: "session1",
		Payload:   "data",
	}
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
	handler := NewMockHandler(0)

	d, err := NewDispatcher(config, handler)
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
	task := &types.Task{
		ID:        "test-task",
		SessionID: "test-session",
		Payload:   "data",
	}
	err = d.Submit(context.Background(), task)
	assert.NoError(t, err)
}

func TestDispatcher_RemoveWorker(t *testing.T) {
	config := &types.WorkerConfig{
		PoolSize:        5,
		QueueSize:       100,
		TaskTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
	handler := NewMockHandler(10 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Initial pool size
	assert.Equal(t, 5, d.GetPoolSize())

	// Get a worker ID
	stats := d.GetWorkerStats()
	require.Greater(t, len(stats), 0)
	workerID := stats[0].WorkerID

	// Remove the worker
	err = d.RemoveWorker(context.Background(), workerID)
	require.NoError(t, err)
	assert.Equal(t, 4, d.GetPoolSize())

	// Try to remove non-existent worker
	err = d.RemoveWorker(context.Background(), "non-existent")
	assert.Error(t, err)

	// Verify remaining workers still work
	task := &types.Task{
		ID:        "test-task",
		SessionID: "test-session",
		Payload:   "data",
	}
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
	handler := NewMockHandler(10 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
	require.NoError(t, err)
	defer d.Shutdown(context.Background())

	// Initial size
	assert.Equal(t, 5, d.GetPoolSize())

	// Scale up
	err = d.ResizePool(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 10, d.GetPoolSize())

	// Scale down
	err = d.ResizePool(context.Background(), 3)
	require.NoError(t, err)
	assert.Equal(t, 3, d.GetPoolSize())

	// Same size (no-op)
	err = d.ResizePool(context.Background(), 3)
	require.NoError(t, err)
	assert.Equal(t, 3, d.GetPoolSize())

	// Invalid size
	err = d.ResizePool(context.Background(), 0)
	assert.Error(t, err)

	err = d.ResizePool(context.Background(), -1)
	assert.Error(t, err)

	// Verify dispatcher still works after resizing
	for i := 0; i < 20; i++ {
		task := &types.Task{
			ID:        fmt.Sprintf("task-%d", i),
			SessionID: fmt.Sprintf("session-%d", i%5),
			Payload:   i,
		}
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
	handler := NewMockHandler(20 * time.Millisecond)

	d, err := NewDispatcher(config, handler)
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
				task := &types.Task{
					ID:        fmt.Sprintf("task-%d", taskNum),
					SessionID: fmt.Sprintf("session-%d", taskNum%10),
					Payload:   taskNum,
				}
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

	// Scale down while under load
	err = d.ResizePool(context.Background(), 4)
	require.NoError(t, err)
	assert.Equal(t, 4, d.GetPoolSize())

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
	handler := NewMockHandler(0) // No delay

	d, err := NewDispatcher(config, handler)
	require.NoError(b, err)
	defer d.Shutdown(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := &types.Task{
			ID:        fmt.Sprintf("task%d", i),
			SessionID: fmt.Sprintf("session%d", i%100),
			Payload:   i,
		}
		d.Submit(context.Background(), task)
	}
}
