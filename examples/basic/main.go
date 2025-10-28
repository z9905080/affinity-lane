package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/z9905080/affinity-lane/pkg/dispatcher"
	"github.com/z9905080/affinity-lane/pkg/types"
)

// SimpleTaskHandler is a basic implementation of TaskHandler
type SimpleTaskHandler struct{}

func (h *SimpleTaskHandler) Handle(ctx context.Context, task *types.Task) (*types.TaskResult, error) {
	// Simulate some work
	processingTime := time.Duration(rand.Intn(100)) * time.Millisecond

	log.Printf("[%s] Processing task %s (session: %s, payload: %v)",
		time.Now().Format("15:04:05.000"),
		task.ID,
		task.SessionID,
		task.Payload,
	)

	time.Sleep(processingTime)

	// Return successful result
	return &types.TaskResult{
		TaskID:    task.ID,
		SessionID: task.SessionID,
		Result:    fmt.Sprintf("Processed in %v", processingTime),
	}, nil
}

func main() {
	log.Println("Starting Worker Pool example...")

	// Create configuration
	config := &types.WorkerConfig{
		PoolSize:         5,                  // 5 workers
		QueueSize:        100,                // 100 tasks per session queue
		TaskTimeout:      30 * time.Second,   // 30s timeout per task
		ShutdownTimeout:  10 * time.Second,   // 10s shutdown timeout
		VirtualNodeCount: 150,                // 150 virtual nodes for consistent hashing
	}

	// Create task handler
	handler := &SimpleTaskHandler{}

	// Create dispatcher
	d, err := dispatcher.NewDispatcher(config, handler)
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}

	log.Printf("Dispatcher created with %d workers", config.PoolSize)

	// Submit tasks from multiple sessions
	numSessions := 10
	tasksPerSession := 5

	log.Printf("Submitting %d tasks from %d sessions...", tasksPerSession*numSessions, numSessions)

	for s := 0; s < numSessions; s++ {
		sessionID := fmt.Sprintf("session-%d", s)

		for t := 0; t < tasksPerSession; t++ {
			task := &types.Task{
				ID:        fmt.Sprintf("task-%d-%d", s, t),
				SessionID: sessionID,
				Payload: map[string]interface{}{
					"session": sessionID,
					"index":   t,
					"data":    fmt.Sprintf("payload-%d-%d", s, t),
				},
			}

			err := d.Submit(context.Background(), task)
			if err != nil {
				log.Printf("Failed to submit task %s: %v", task.ID, err)
			}
		}
	}

	log.Println("All tasks submitted!")

	// Wait a bit to let tasks process
	time.Sleep(2 * time.Second)

	// Print statistics
	stats := d.Stats()
	log.Println("\n=== Dispatcher Statistics ===")
	log.Printf("Total Submitted: %d", stats.TotalSubmitted)
	log.Printf("Total Completed: %d", stats.TotalCompleted)
	log.Printf("Total Failed: %d", stats.TotalFailed)
	log.Printf("Active Sessions: %d", stats.ActiveSessions)
	log.Printf("Queued Tasks: %d", stats.QueuedTasks)
	log.Printf("Workers: %d", stats.Workers)

	// Print worker statistics
	log.Println("\n=== Worker Statistics ===")
	workerStats := d.GetWorkerStats()
	for _, ws := range workerStats {
		log.Printf("Worker %s: Sessions=%d, Processed=%d, Failed=%d, AvgDuration=%v, QueueLen=%d",
			ws.WorkerID,
			ws.SessionCount,
			ws.TasksProcessed,
			ws.TasksFailed,
			ws.AvgTaskDuration,
			ws.QueueLength,
		)
	}

	// Print session distribution
	log.Println("\n=== Session Distribution ===")
	distribution := d.GetSessionDistribution()
	for workerID, count := range distribution {
		log.Printf("%s: %d sessions", workerID, count)
	}

	// Print session info for a few sessions
	log.Println("\n=== Sample Session Info ===")
	for i := 0; i < 3; i++ {
		sessionID := fmt.Sprintf("session-%d", i)
		info, err := d.GetSessionInfo(sessionID)
		if err == nil {
			log.Printf("Session %s: WorkerID=%s, TaskCount=%d, QueueLength=%d",
				info.SessionID,
				info.WorkerID,
				info.TaskCount,
				info.QueueLength,
			)
		}
	}

	// Wait for all tasks to complete
	log.Println("\nWaiting for remaining tasks to complete...")
	time.Sleep(3 * time.Second)

	// Final statistics
	stats = d.Stats()
	log.Println("\n=== Final Statistics ===")
	log.Printf("Total Submitted: %d", stats.TotalSubmitted)
	log.Printf("Total Completed: %d", stats.TotalCompleted)
	log.Printf("Total Failed: %d", stats.TotalFailed)

	// Graceful shutdown
	log.Println("\nShutting down dispatcher...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = d.Shutdown(ctx)
	if err != nil {
		log.Printf("Error during shutdown: %v", err)
	} else {
		log.Println("Dispatcher shut down successfully!")
	}
}
