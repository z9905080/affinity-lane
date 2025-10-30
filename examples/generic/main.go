package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/z9905080/affinity-lane/pkg/dispatcher"
	"github.com/z9905080/affinity-lane/pkg/types"
)

// UserPayload represents a user-specific task payload
type UserPayload struct {
	UserID   string
	Action   string
	Metadata map[string]any
}

// OrderPayload represents an order-specific task payload
type OrderPayload struct {
	OrderID    string
	CustomerID string
	Items      []string
	Total      float64
}

func main() {
	log.Println("Starting Generic Task example...")

	// Create configuration
	config := &types.WorkerConfig{
		PoolSize:         5,
		QueueSize:        100,
		TaskTimeout:      30 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		VirtualNodeCount: 150,
	}

	// Create dispatcher (no handler needed anymore!)
	d, err := dispatcher.NewDispatcher(config)
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}

	log.Printf("Dispatcher created with %d workers", config.PoolSize)

	// Example 1: Task with UserPayload
	userTask := &types.Task[UserPayload]{
		ID:        "user-task-1",
		SessionID: "user-session-1",
		Payload: UserPayload{
			UserID: "user123",
			Action: "login",
			Metadata: map[string]any{
				"ip":        "192.168.1.1",
				"timestamp": time.Now().Unix(),
			},
		},
		CreatedAt: time.Now(),
		OnExecute: func(ctx context.Context, task *types.Task[UserPayload]) (any, error) {
			log.Printf("[%s] Processing user task: UserID=%s, Action=%s",
				time.Now().Format("15:04:05.000"),
				task.Payload.UserID,
				task.Payload.Action,
			)
			time.Sleep(100 * time.Millisecond)
			return fmt.Sprintf("User %s %s successfully", task.Payload.UserID, task.Payload.Action), nil
		},
	}

	err = d.Submit(context.Background(), userTask)
	if err != nil {
		log.Printf("Failed to submit user task: %v", err)
	}

	// Example 2: Task with OrderPayload
	orderTask := &types.Task[OrderPayload]{
		ID:        "order-task-1",
		SessionID: "order-session-1",
		Payload: OrderPayload{
			OrderID:    "ORD-12345",
			CustomerID: "CUST-67890",
			Items:      []string{"item1", "item2", "item3"},
			Total:      299.99,
		},
		CreatedAt: time.Now(),
		OnExecute: func(ctx context.Context, task *types.Task[OrderPayload]) (any, error) {
			log.Printf("[%s] Processing order: OrderID=%s, CustomerID=%s, Total=$%.2f",
				time.Now().Format("15:04:05.000"),
				task.Payload.OrderID,
				task.Payload.CustomerID,
				task.Payload.Total,
			)
			time.Sleep(150 * time.Millisecond)
			return map[string]any{
				"order_id": task.Payload.OrderID,
				"status":   "processed",
				"total":    task.Payload.Total,
			}, nil
		},
	}

	err = d.Submit(context.Background(), orderTask)
	if err != nil {
		log.Printf("Failed to submit order task: %v", err)
	}

	// Example 3: Task with string payload (simple case)
	stringTask := &types.Task[string]{
		ID:        "string-task-1",
		SessionID: "string-session-1",
		Payload:   "Hello, World!",
		CreatedAt: time.Now(),
		OnExecute: func(ctx context.Context, task *types.Task[string]) (any, error) {
			log.Printf("[%s] Processing string task: %s",
				time.Now().Format("15:04:05.000"),
				task.Payload,
			)
			time.Sleep(50 * time.Millisecond)
			return fmt.Sprintf("Processed: %s", task.Payload), nil
		},
	}

	err = d.Submit(context.Background(), stringTask)
	if err != nil {
		log.Printf("Failed to submit string task: %v", err)
	}

	// Example 4: Batch submit multiple user tasks
	log.Println("\nSubmitting batch of user tasks...")
	var userTasks []types.InfTask
	for i := 0; i < 5; i++ {
		task := &types.Task[UserPayload]{
			ID:        fmt.Sprintf("user-task-%d", i+2),
			SessionID: "user-session-1", // Same session for ordering
			Payload: UserPayload{
				UserID: fmt.Sprintf("user%d", i+100),
				Action: "update_profile",
				Metadata: map[string]any{
					"index": i,
				},
			},
			CreatedAt: time.Now(),
			OnExecute: func(ctx context.Context, t *types.Task[UserPayload]) (any, error) {
				log.Printf("[%s] Batch processing user: UserID=%s",
					time.Now().Format("15:04:05.000"),
					t.Payload.UserID,
				)
				time.Sleep(80 * time.Millisecond)
				return fmt.Sprintf("User %s updated", t.Payload.UserID), nil
			},
		}
		userTasks = append(userTasks, task)
	}

	err = d.SubmitBatch(context.Background(), userTasks)
	if err != nil {
		log.Printf("Failed to submit batch: %v", err)
	}

	// Wait for tasks to process
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
		log.Printf("Worker %s: Sessions=%d, Processed=%d, Failed=%d, AvgDuration=%v",
			ws.WorkerID,
			ws.SessionCount,
			ws.TasksProcessed,
			ws.TasksFailed,
			ws.AvgTaskDuration,
		)
	}

	// Wait for all tasks to complete
	time.Sleep(2 * time.Second)

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
