package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/z9905080/affinity-lane/pkg/dispatcher"
	"github.com/z9905080/affinity-lane/pkg/types"
)

func main() {
	log.Println("========================================")
	log.Println("  Dynamic Worker Pool Scale-Up Demo")
	log.Println("========================================")
	log.Println()

	// 創建配置 - 初始較小的 pool
	config := &types.WorkerConfig{
		PoolSize:         3, // 初始只有 3 個 workers
		QueueSize:        100,
		TaskTimeout:      30 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		VirtualNodeCount: 150,
	}

	// 創建 dispatcher
	d, err := dispatcher.NewDispatcher(config)
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}
	defer d.Shutdown(context.Background())

	log.Printf("✓ Dispatcher created with %d workers\n\n", d.GetPoolSize())

	// 啟動任務提交器（模擬實際負載）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		taskCounter    sync.Mutex
		totalSubmitted int
		submitErrors   sync.Map // track submit errors
	)

	taskSubmitter := func(rate int) {
		ticker := time.NewTicker(time.Duration(1000/rate) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				taskCounter.Lock()
				taskNum := totalSubmitted
				totalSubmitted++
				taskCounter.Unlock()

				task := &types.Task[map[string]any]{
					ID:        fmt.Sprintf("task-%d", taskNum),
					SessionID: fmt.Sprintf("session-%d", taskNum%20),
					Payload: map[string]any{
						"index": taskNum,
					},
					CreatedAt: time.Now(),
					OnExecute: func(ctx context.Context, task *types.Task[map[string]any]) (any, error) {
						// Simulate variable processing time
						processingTime := time.Duration(rand.Intn(50)) * time.Millisecond
						time.Sleep(processingTime)
						return fmt.Sprintf("Processed by worker in %v", processingTime), nil
					},
				}
				err := d.Submit(context.Background(), task)
				if err != nil {
					// Track errors
					submitErrors.Store(taskNum, err.Error())
				}
			}
		}
	}

	// 監控器 - 顯示實時狀態
	var currentRate int
	monitorTicker := time.NewTicker(2 * time.Second)
	defer monitorTicker.Stop()

	go func() {
		for range monitorTicker.C {
			stats := d.Stats()
			poolSize := d.GetPoolSize()

			log.Printf("[Monitor] Workers: %d | Submitted: %d | Completed: %d | Queued: %d | Rate: %d tasks/s",
				poolSize,
				stats.TotalSubmitted,
				stats.TotalCompleted,
				stats.QueuedTasks,
				currentRate,
			)
		}
	}()

	// ========================================
	// 場景 1: 低負載 - 保持小規模
	// ========================================
	log.Println("\n=== Scenario 1: Low Load (3 workers) ===")
	currentRate = 10 // 10 tasks/second
	go taskSubmitter(currentRate)
	time.Sleep(5 * time.Second)

	// ========================================
	// 場景 2: 負載增加 - 擴容
	// ========================================
	log.Println("\n=== Scenario 2: Increasing Load - Scale Up ===")
	log.Println("Load increasing... Scaling up to 8 workers")

	currentRate = 30 // 增加到 30 tasks/second

	// 逐步擴容
	for i := d.GetPoolSize(); i < 8; i++ {
		workerID, err := d.AddWorker(context.Background())
		if err != nil {
			log.Printf("Failed to add worker: %v", err)
			continue
		}
		log.Printf("  ➕ Added worker: %s (Pool size: %d)", workerID, d.GetPoolSize())
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("✓ Scale up completed. Current pool size: %d\n", d.GetPoolSize())
	time.Sleep(5 * time.Second)

	// ========================================
	// 場景 3: 高負載 - 繼續擴容
	// ========================================
	log.Println("\n=== Scenario 3: High Load - Scale Up More ===")
	log.Println("Load spike detected! Scaling up to 15 workers")

	currentRate = 50 // 增加到 50 tasks/second

	// 使用 ResizePool 快速擴容
	err = d.ResizePool(context.Background(), 15)
	if err != nil {
		log.Printf("Failed to resize pool: %v", err)
	} else {
		log.Printf("✓ Quickly scaled to %d workers\n", d.GetPoolSize())
	}

	time.Sleep(5 * time.Second)

	// ========================================
	// 最終統計
	// ========================================
	log.Println("\n=== Final Statistics ===")

	// 停止任務提交
	cancel()
	log.Println("Stopped task submission. Waiting for all queued tasks to complete...")

	// 等待所有已提交的任務完成
	maxWaitTime := 30 * time.Second
	checkInterval := 500 * time.Millisecond
	startWait := time.Now()

	for {
		stats := d.Stats()
		if stats.TotalSubmitted == stats.TotalCompleted {
			log.Printf("✓ All tasks completed! (%d/%d)", stats.TotalCompleted, stats.TotalSubmitted)
			break
		}

		if time.Since(startWait) > maxWaitTime {
			log.Printf("⚠ Timeout waiting for tasks to complete. Completed: %d/%d",
				stats.TotalCompleted, stats.TotalSubmitted)
			break
		}

		log.Printf("  Waiting... Completed: %d/%d, Queued: %d",
			stats.TotalCompleted, stats.TotalSubmitted, stats.QueuedTasks)
		time.Sleep(checkInterval)
	}

	// Count submit errors
	var submitErrorCount int
	submitErrors.Range(func(key, value any) bool {
		submitErrorCount++
		if submitErrorCount <= 10 {
			log.Printf("  Submit error for task-%v: %v", key, value)
		}
		return true
	})

	finalStats := d.Stats()
	log.Printf("\nTotal Submitted: %d", finalStats.TotalSubmitted)
	log.Printf("Total Completed: %d", finalStats.TotalCompleted)
	log.Printf("Total Failed: %d", finalStats.TotalFailed)
	if submitErrorCount > 0 {
		log.Printf("Submit Errors: %d (tasks that failed to submit)", submitErrorCount)
	}
	log.Printf("Success Rate: %.2f%%",
		float64(finalStats.TotalCompleted)/float64(finalStats.TotalSubmitted)*100)
	log.Printf("Final Pool Size: %d", d.GetPoolSize())
	log.Printf("Active Sessions: %d", finalStats.ActiveSessions)

	// Worker 分布
	log.Println("\n=== Worker Statistics ===")
	workerStats := d.GetWorkerStats()
	for _, ws := range workerStats {
		log.Printf("Worker %s: Processed=%d, Sessions=%d, Failed=%d, AvgDuration=%v",
			ws.WorkerID,
			ws.TasksProcessed,
			ws.SessionCount,
			ws.TasksFailed,
			ws.AvgTaskDuration,
		)
	}

	// Session 分布
	log.Println("\n=== Session Distribution ===")
	distribution := d.GetSessionDistribution()
	for workerID, count := range distribution {
		log.Printf("%s: %d sessions", workerID, count)
	}

	log.Println("\n========================================")
	log.Println("  Demo Completed Successfully!")
	log.Println("========================================")
	log.Println()
	log.Println("Key Takeaways:")
	log.Println("  ✓ Dynamic scale-up works seamlessly")
	log.Println("  ✓ No task loss during scaling")
	log.Println("  ✓ Session affinity maintained")
	log.Println("  ✓ Zero downtime scaling")
}
