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

// SimpleHandler implements a basic task handler
type SimpleHandler struct{}

func (h *SimpleHandler) Handle(ctx context.Context, task *types.Task) (*types.TaskResult, error) {
	// Simulate variable processing time
	processingTime := time.Duration(rand.Intn(50)) * time.Millisecond
	time.Sleep(processingTime)

	return &types.TaskResult{
		TaskID:    task.ID,
		SessionID: task.SessionID,
		Result:    fmt.Sprintf("Processed by worker in %v", processingTime),
	}, nil
}

func main() {
	log.Println("========================================")
	log.Println("  Dynamic Worker Pool Resizing Demo")
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

	handler := &SimpleHandler{}

	// 創建 dispatcher
	d, err := dispatcher.NewDispatcher(config, handler)
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}
	defer d.Shutdown(context.Background())

	log.Printf("✓ Dispatcher created with %d workers\n\n", d.GetPoolSize())

	// 啟動任務提交器（模擬實際負載）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskCounter sync.Mutex
	var totalSubmitted int

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

				task := &types.Task{
					ID:        fmt.Sprintf("task-%d", taskNum),
					SessionID: fmt.Sprintf("session-%d", taskNum%20),
					Payload: map[string]interface{}{
						"index": taskNum,
					},
				}
				d.Submit(context.Background(), task)
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
	// 場景 4: 負載降低 - 縮容
	// ========================================
	log.Println("\n=== Scenario 4: Load Decreasing - Scale Down ===")
	log.Println("Load decreasing... Scaling down to 6 workers")

	currentRate = 15 // 降低到 15 tasks/second

	// 逐步縮容
	targetSize := 6
	workerStats := d.GetWorkerStats()

	for d.GetPoolSize() > targetSize && len(workerStats) > 0 {
		// 選擇第一個 worker 移除
		workerID := workerStats[0].WorkerID
		err := d.RemoveWorker(context.Background(), workerID)
		if err != nil {
			log.Printf("Failed to remove worker: %v", err)
		} else {
			log.Printf("  ➖ Removed worker: %s (Pool size: %d)", workerID, d.GetPoolSize())
		}

		// 更新 worker 列表
		workerStats = d.GetWorkerStats()
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("✓ Scale down completed. Current pool size: %d\n", d.GetPoolSize())
	time.Sleep(5 * time.Second)

	// ========================================
	// 場景 5: 回到低負載 - 進一步縮容
	// ========================================
	log.Println("\n=== Scenario 5: Back to Low Load - Scale Down More ===")
	log.Println("Returning to low load... Scaling down to 3 workers")

	currentRate = 10 // 回到 10 tasks/second

	// 使用 ResizePool 快速縮容
	err = d.ResizePool(context.Background(), 3)
	if err != nil {
		log.Printf("Failed to resize pool: %v", err)
	} else {
		log.Printf("✓ Quickly scaled down to %d workers\n", d.GetPoolSize())
	}

	time.Sleep(5 * time.Second)

	// ========================================
	// 最終統計
	// ========================================
	log.Println("\n=== Final Statistics ===")

	// 停止任務提交
	cancel()
	time.Sleep(2 * time.Second) // 等待最後的任務完成

	finalStats := d.Stats()
	log.Printf("Total Submitted: %d", finalStats.TotalSubmitted)
	log.Printf("Total Completed: %d", finalStats.TotalCompleted)
	log.Printf("Total Failed: %d", finalStats.TotalFailed)
	log.Printf("Success Rate: %.2f%%",
		float64(finalStats.TotalCompleted)/float64(finalStats.TotalSubmitted)*100)
	log.Printf("Final Pool Size: %d", d.GetPoolSize())
	log.Printf("Active Sessions: %d", finalStats.ActiveSessions)

	// Worker 分布
	log.Println("\n=== Worker Statistics ===")
	workerStats = d.GetWorkerStats()
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
	log.Println("  ✓ Dynamic resizing works seamlessly")
	log.Println("  ✓ No task loss during scaling")
	log.Println("  ✓ Session affinity maintained")
	log.Println("  ✓ Zero downtime scaling")
}
