package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/z9905080/affinity-lane/pkg/dispatcher"
	"github.com/z9905080/affinity-lane/pkg/types"
)

// OrderTracker tracks the order of task execution per session
type OrderTracker struct {
	mu       sync.Mutex
	sessions map[string][]string // sessionID -> ordered task IDs
}

func NewOrderTracker() *OrderTracker {
	return &OrderTracker{
		sessions: make(map[string][]string),
	}
}

func (ot *OrderTracker) RecordTask(sessionID, taskID string) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.sessions[sessionID] = append(ot.sessions[sessionID], taskID)
}

func (ot *OrderTracker) VerifyOrder(sessionID string, expectedOrder []string) bool {
	ot.mu.Lock()
	defer ot.mu.Unlock()

	actual := ot.sessions[sessionID]
	if len(actual) != len(expectedOrder) {
		return false
	}

	for i := range actual {
		if actual[i] != expectedOrder[i] {
			return false
		}
	}
	return true
}

func (ot *OrderTracker) GetOrder(sessionID string) []string {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	return append([]string{}, ot.sessions[sessionID]...)
}

// AdvancedTaskHandler demonstrates more complex task handling
type AdvancedTaskHandler struct {
	orderTracker *OrderTracker
	resultChan   chan *types.TaskResult
}

func NewAdvancedTaskHandler(orderTracker *OrderTracker) *AdvancedTaskHandler {
	return &AdvancedTaskHandler{
		orderTracker: orderTracker,
		resultChan:   make(chan *types.TaskResult, 1000),
	}
}

func (h *AdvancedTaskHandler) Handle(ctx context.Context, task *types.Task) (*types.TaskResult, error) {
	startTime := time.Now()

	// Record execution order
	h.orderTracker.RecordTask(task.SessionID, task.ID)

	// Simulate different types of work based on payload
	payload, ok := task.Payload.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid payload type")
	}

	workType, _ := payload["type"].(string)

	var processingTime time.Duration
	var result interface{}

	switch workType {
	case "fast":
		processingTime = 10 * time.Millisecond
		result = "fast work completed"
	case "slow":
		processingTime = 100 * time.Millisecond
		result = "slow work completed"
	case "compute":
		// Simulate CPU-intensive work
		sum := 0
		for i := 0; i < 1000000; i++ {
			sum += i
		}
		result = fmt.Sprintf("computed sum: %d", sum)
	default:
		processingTime = 50 * time.Millisecond
		result = "default work completed"
	}

	if processingTime > 0 {
		time.Sleep(processingTime)
	}

	taskResult := &types.TaskResult{
		TaskID:    task.ID,
		SessionID: task.SessionID,
		Result:    result,
		StartedAt: startTime,
		EndedAt:   time.Now(),
		Duration:  time.Since(startTime),
	}

	// Send result to channel
	select {
	case h.resultChan <- taskResult:
	default:
		log.Printf("Result channel full, dropping result for task %s", task.ID)
	}

	return taskResult, nil
}

func (h *AdvancedTaskHandler) GetResultChan() <-chan *types.TaskResult {
	return h.resultChan
}

func (h *AdvancedTaskHandler) Close() {
	close(h.resultChan)
}

func main() {
	log.Println("Starting Advanced Worker Pool example...")
	log.Println("This example demonstrates:")
	log.Println("  1. Task ordering verification")
	log.Println("  2. Different task types with varying processing times")
	log.Println("  3. Result collection via channels")
	log.Println("  4. High concurrent load testing")
	log.Println()

	// Create order tracker
	orderTracker := NewOrderTracker()

	// Create configuration
	config := &types.WorkerConfig{
		PoolSize:         10,
		QueueSize:        500,
		TaskTimeout:      5 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		VirtualNodeCount: 200,
	}

	// Create handler
	handler := NewAdvancedTaskHandler(orderTracker)

	// Create dispatcher
	d, err := dispatcher.NewDispatcher(config, handler)
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}

	// Start result collector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		completedCount := 0
		for range handler.GetResultChan() {
			completedCount++
			if completedCount%100 == 0 {
				log.Printf("Collected %d results", completedCount)
			}
		}
		log.Printf("Result collector finished. Total results: %d", completedCount)
	}()

	// Test 1: Verify ordering with sequential tasks
	log.Println("\n=== Test 1: Task Ordering Verification ===")
	testSession := "test-ordering"
	expectedOrder := make([]string, 10)
	for i := 0; i < 10; i++ {
		taskID := fmt.Sprintf("ordered-task-%d", i)
		expectedOrder[i] = taskID

		task := &types.Task{
			ID:        taskID,
			SessionID: testSession,
			Payload: map[string]interface{}{
				"type":  "fast",
				"index": i,
			},
		}
		d.Submit(context.Background(), task)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify order
	actualOrder := orderTracker.GetOrder(testSession)
	orderCorrect := orderTracker.VerifyOrder(testSession, expectedOrder)
	log.Printf("Expected order: %v", expectedOrder)
	log.Printf("Actual order:   %v", actualOrder)
	log.Printf("Order correct: %v", orderCorrect)

	// Test 2: High concurrent load with multiple sessions
	log.Println("\n=== Test 2: High Concurrent Load ===")
	numSessions := 100
	tasksPerSession := 20

	log.Printf("Submitting %d tasks from %d sessions...", numSessions*tasksPerSession, numSessions)

	startTime := time.Now()
	var submitWg sync.WaitGroup

	// Submit tasks concurrently
	for s := 0; s < numSessions; s++ {
		submitWg.Add(1)
		go func(sessionIdx int) {
			defer submitWg.Done()

			sessionID := fmt.Sprintf("session-%d", sessionIdx)
			taskTypes := []string{"fast", "slow", "compute", "default"}

			for t := 0; t < tasksPerSession; t++ {
				task := &types.Task{
					ID:        fmt.Sprintf("task-%d-%d", sessionIdx, t),
					SessionID: sessionID,
					Payload: map[string]interface{}{
						"type":  taskTypes[t%len(taskTypes)],
						"index": t,
					},
				}

				err := d.Submit(context.Background(), task)
				if err != nil {
					log.Printf("Failed to submit task: %v", err)
				}
			}
		}(s)
	}

	submitWg.Wait()
	submitDuration := time.Since(startTime)
	log.Printf("All tasks submitted in %v", submitDuration)

	// Monitor progress
	log.Println("\nMonitoring progress...")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for i := 0; i < 10; i++ {
		<-ticker.C
		stats := d.Stats()
		progress := float64(stats.TotalCompleted) / float64(stats.TotalSubmitted) * 100
		log.Printf("[%ds] Progress: %.1f%% (%d/%d) | Queued: %d | Failed: %d",
			i+1,
			progress,
			stats.TotalCompleted,
			stats.TotalSubmitted,
			stats.QueuedTasks,
			stats.TotalFailed,
		)

		if stats.QueuedTasks == 0 && stats.TotalCompleted == stats.TotalSubmitted {
			break
		}
	}

	// Final statistics
	stats := d.Stats()
	log.Println("\n=== Final Statistics ===")
	log.Printf("Total Submitted: %d", stats.TotalSubmitted)
	log.Printf("Total Completed: %d", stats.TotalCompleted)
	log.Printf("Total Failed: %d", stats.TotalFailed)
	log.Printf("Active Sessions: %d", stats.ActiveSessions)
	log.Printf("Throughput: %.2f tasks/sec", float64(stats.TotalCompleted)/time.Since(startTime).Seconds())

	// Worker statistics
	log.Println("\n=== Worker Performance ===")
	workerStats := d.GetWorkerStats()
	for _, ws := range workerStats {
		if ws.TasksProcessed > 0 {
			log.Printf("Worker %s: Processed=%d, Sessions=%d, AvgDuration=%v",
				ws.WorkerID,
				ws.TasksProcessed,
				ws.SessionCount,
				ws.AvgTaskDuration,
			)
		}
	}

	// Session distribution analysis
	log.Println("\n=== Session Distribution ===")
	distribution := d.GetSessionDistribution()
	minSessions := 999999
	maxSessions := 0
	totalSessions := 0

	for workerID, count := range distribution {
		log.Printf("%s: %d sessions", workerID, count)
		if count < minSessions {
			minSessions = count
		}
		if count > maxSessions {
			maxSessions = count
		}
		totalSessions += count
	}

	avgSessions := float64(totalSessions) / float64(len(distribution))
	log.Printf("\nDistribution stats: Min=%d, Max=%d, Avg=%.1f, StdDev=%.1f%%",
		minSessions,
		maxSessions,
		avgSessions,
		float64(maxSessions-minSessions)/avgSessions*100,
	)

	// Test 3: Verify ordering for all sessions
	log.Println("\n=== Test 3: Verify All Sessions Ordering ===")
	orderingErrors := 0
	for s := 0; s < numSessions; s++ {
		sessionID := fmt.Sprintf("session-%d", s)
		expectedOrder := make([]string, tasksPerSession)
		for t := 0; t < tasksPerSession; t++ {
			expectedOrder[t] = fmt.Sprintf("task-%d-%d", s, t)
		}

		if !orderTracker.VerifyOrder(sessionID, expectedOrder) {
			orderingErrors++
			if orderingErrors <= 3 {
				log.Printf("Ordering error in %s", sessionID)
				log.Printf("  Expected: %v", expectedOrder)
				log.Printf("  Actual:   %v", orderTracker.GetOrder(sessionID))
			}
		}
	}

	if orderingErrors == 0 {
		log.Printf("All %d sessions maintained correct task ordering!", numSessions)
	} else {
		log.Printf("WARNING: %d sessions had ordering errors", orderingErrors)
	}

	// Graceful shutdown
	log.Println("\n=== Shutting Down ===")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = d.Shutdown(shutdownCtx)
	if err != nil {
		log.Printf("Error during shutdown: %v", err)
	} else {
		log.Println("Dispatcher shut down successfully!")
	}

	// Close handler and wait for result collector
	handler.Close()
	wg.Wait()

	log.Println("\n=== Example Complete ===")
}
