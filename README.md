# Worker Pool with Session Affinity

A high-performance Go worker pool library that ensures session-based task ordering and load balancing.

## Features

- **Session Affinity**: Tasks with the same session_id are always routed to the same worker
- **Order Guarantee**: Tasks within a session are executed in FIFO order
- **Load Balancing**: Uses consistent hashing to evenly distribute sessions across workers
- **Dynamic Resizing**: Add or remove workers at runtime without disrupting task processing
- **High Performance**: Low latency task dispatching with high throughput
- **Graceful Shutdown**: Proper cleanup and task completion on shutdown
- **Thread-Safe**: Concurrent-safe operations across all components

## Installation

```bash
go get github.com/z9905080/affinity-lane    
```

## Quick Start

See [examples/basic/main.go](examples/basic/main.go) for a complete example.

```go
import (
    "context"
    "github.com/z9905080/affinity-lane/pkg/dispatcher"
    "github.com/z9905080/affinity-lane/pkg/types"
)

// Implement task handler
type MyHandler struct{}

func (h *MyHandler) Handle(ctx context.Context, task *types.Task) (*types.TaskResult, error) {
    // Your task processing logic
    return &types.TaskResult{
        TaskID:    task.ID,
        SessionID: task.SessionID,
        Result:    "processed",
    }, nil
}

// Create and use dispatcher
func main() {
    config := &types.WorkerConfig{
        PoolSize:    10,
        QueueSize:   1000,
        TaskTimeout: 30 * time.Second,
    }

    d, err := dispatcher.NewDispatcher(config, &MyHandler{})
    if err != nil {
        log.Fatal(err)
    }

    // Submit task
    task := &types.Task{
        ID:        "task-1",
        SessionID: "session-123",
        Payload:   map[string]interface{}{"data": "value"},
    }

    err = d.Submit(context.Background(), task)
    if err != nil {
        log.Printf("Failed to submit: %v", err)
    }

    // Graceful shutdown
    d.Shutdown(context.Background())
}
```

## Dynamic Resizing

The worker pool supports dynamic resizing at runtime:

```go
// Get current pool size
currentSize := d.GetPoolSize()

// Add a single worker
workerID, err := d.AddWorker(context.Background())

// Remove a specific worker
err = d.RemoveWorker(context.Background(), workerID)

// Resize to target size (scales up or down)
err = d.ResizePool(context.Background(), 20)
```

This allows you to:
- Scale up during high load periods
- Scale down during low load to save resources
- Adjust capacity based on real-time metrics
- All without disrupting existing task processing or session affinity

## Architecture

See [SYSTEM_SPEC.md](SYSTEM_SPEC.md) for detailed system specification.

## Examples

### 1. Basic Usage
```bash
go run examples/basic/main.go
```
基礎使用範例，展示如何創建 dispatcher 和提交任務。

### 2. Advanced Features
```bash
go run examples/advanced/main.go
```
進階功能展示，包括：
- 任務順序驗證
- 高並發負載測試 (100 sessions, 2000 tasks)
- 結果收集
- 性能監控

### 3. Dynamic Resizing
```bash
go run examples/dynamic_resize/main.go
```
動態調整範例，展示 5 個實際場景：
1. **低負載** (3 workers, 10 tasks/s)
2. **負載增加** - 逐步擴容到 8 workers (30 tasks/s)
3. **高負載** - 快速擴容到 15 workers (50 tasks/s)
4. **負載降低** - 逐步縮容到 6 workers (15 tasks/s)
5. **回到低負載** - 快速縮容到 3 workers (10 tasks/s)

演示了如何根據實時負載動態調整 worker 數量。

## Testing

```bash
go test ./...
```

## License

MIT
