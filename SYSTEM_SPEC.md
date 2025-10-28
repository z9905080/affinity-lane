# Worker Pool System Specification

## 概述

本套件提供一個基於 Golang 的 worker pool 系統，用於處理需要按 session 順序執行的任務分發。

## 核心需求

### 1. 任務分發 (Task Dispatching)
- **Dispatcher** 負責接收任務並分發到 worker pool
- 支持動態任務提交
- 提供非阻塞的任務提交接口

### 2. 順序保證 (Order Guarantee)
- 同一個 `session_id` 的任務必須按照提交順序依序執行
- 不同 `session_id` 的任務可以並行執行
- 任務執行順序嚴格遵循 FIFO (First In First Out)

### 3. Session 親和性 (Session Affinity)
- 相同 `session_id` 的任務必須路由到同一個 worker
- Worker 與 session 的綁定關係在 session 活躍期間保持穩定
- 確保同一 session 的任務不會在多個 worker 間切換

### 4. 負載均衡 (Load Balancing)
- Worker pool 有固定上限（可配置）
- Session 數量無上限
- 一個 worker 可以處理多個 session
- Session 分配需要均勻分散到各個 worker
- 使用一致性哈希或類似算法確保負載平衡

## 系統架構

### 組件說明

#### Dispatcher
```
職責：
- 接收外部提交的任務
- 根據 session_id 計算目標 worker
- 維護 session 到 worker 的映射關係
- 將任務分發到對應的 worker 隊列
```

#### Worker Pool
```
職責：
- 管理固定數量的 worker goroutines
- 動態創建/銷毀 worker（可選）
- 監控 worker 健康狀態
- 提供 worker 統計信息
```

#### Worker
```
職責：
- 維護自己的任務隊列（每個 session 一個隊列）
- 按順序執行分配給自己的任務
- 處理任務執行結果和錯誤
- 上報執行狀態
```

#### Session Manager
```
職責：
- 維護 session_id 到 worker 的映射
- 使用一致性哈希算法分配 session
- 處理 worker 故障時的 session 重新分配
- 提供 session 分布統計
```

## 數據結構

### Task
```go
type Task struct {
    ID        string      // 任務唯一標識
    SessionID string      // Session 標識
    Payload   interface{} // 任務數據
    CreatedAt time.Time   // 創建時間
}
```

### TaskResult
```go
type TaskResult struct {
    TaskID    string      // 任務 ID
    SessionID string      // Session ID
    Result    interface{} // 執行結果
    Error     error       // 錯誤信息
    Duration  time.Duration // 執行時長
}
```

### WorkerConfig
```go
type WorkerConfig struct {
    PoolSize       int           // Worker 數量
    QueueSize      int           // 每個 worker 的隊列大小
    MaxSessions    int           // 每個 worker 最大 session 數（0 表示無限制）
    TaskTimeout    time.Duration // 任務超時時間
    ShutdownTimeout time.Duration // 關閉超時時間
}
```

## 核心算法

### Session 路由算法
```
1. 使用一致性哈希 (Consistent Hashing) 計算 session_id 到 worker 的映射
2. Hash 函數：CRC32 或 FNV-1a
3. 虛擬節點數：每個 worker 150-200 個虛擬節點
4. 好處：
   - Worker 增減時最小化 session 遷移
   - 負載均勻分布
   - 查找效率 O(log N)
```

### 任務排隊策略
```
每個 worker 維護：
- session_id -> TaskQueue 的映射
- 每個 session 獨立的 FIFO 隊列
- Worker 輪詢所有 session 隊列（或使用優先級策略）
```

### Worker 選擇策略（備選）
```
如果不使用一致性哈希，可選：
1. Round Robin + Session Sticky
2. Least Connections
3. Weighted Round Robin
```

## API 設計

### Dispatcher Interface
```go
type Dispatcher interface {
    // 提交任務
    Submit(ctx context.Context, task *Task) error

    // 批量提交任務
    SubmitBatch(ctx context.Context, tasks []*Task) error

    // 獲取任務結果（可選）
    GetResult(taskID string) (*TaskResult, error)

    // 關閉 dispatcher
    Shutdown(ctx context.Context) error

    // 獲取統計信息
    Stats() *DispatcherStats
}
```

### Worker Interface
```go
type Worker interface {
    // 啟動 worker
    Start(ctx context.Context) error

    // 停止 worker
    Stop(ctx context.Context) error

    // 添加任務
    AddTask(task *Task) error

    // 獲取統計信息
    Stats() *WorkerStats
}
```

### TaskHandler Interface
```go
// 用戶需要實現的任務處理器
type TaskHandler interface {
    Handle(ctx context.Context, task *Task) (*TaskResult, error)
}
```

## 並發安全

### 同步機制
- 使用 `sync.RWMutex` 保護 session 映射表
- 每個 worker 的任務隊列使用 channel 或線程安全隊列
- 避免全局鎖，使用分段鎖或無鎖數據結構

### Goroutine 管理
- Worker goroutines 在初始化時創建
- 使用 `context.Context` 進行優雅關閉
- 使用 `sync.WaitGroup` 等待所有 worker 完成

## 錯誤處理

### 任務執行錯誤
- 捕獲 panic 並轉換為 error
- 支持任務重試機制（可配置）
- 錯誤回調通知機制

### Worker 故障處理
- Worker panic 自動恢復並繼續運行
- 提供 worker 健康檢查
- 故障 worker 的 session 重新分配（如需要）

### 超時處理
- 任務級別超時控制
- 使用 context.WithTimeout
- 超時任務返回錯誤並繼續下一個任務

## 監控與統計

### Dispatcher 統計
```go
type DispatcherStats struct {
    TotalSubmitted   int64 // 總提交任務數
    TotalCompleted   int64 // 總完成任務數
    TotalFailed      int64 // 總失敗任務數
    ActiveSessions   int   // 活躍 session 數
    QueuedTasks      int   // 隊列中的任務數
}
```

### Worker 統計
```go
type WorkerStats struct {
    WorkerID         string        // Worker ID
    SessionCount     int           // 處理的 session 數量
    TasksProcessed   int64         // 已處理任務數
    TasksFailed      int64         // 失敗任務數
    AvgTaskDuration  time.Duration // 平均任務執行時間
    QueueLength      int           // 當前隊列長度
}
```

### 日誌
- 使用結構化日誌（如 zap, logrus）
- 記錄關鍵事件：任務提交、任務完成、錯誤、worker 狀態變化
- 支持日誌級別配置

## 性能考慮

### 優化目標
- 低延遲：任務分發延遲 < 1ms
- 高吞吐：支持 10k+ tasks/second
- 內存效率：避免任務積壓導致內存溢出

### 優化策略
- 使用 buffered channel 減少阻塞
- 批量處理任務減少鎖競爭
- 對象池（sync.Pool）重用任務對象
- 避免不必要的內存拷貝

### 背壓機制
- 當 worker 隊列滿時，Submit 返回錯誤或阻塞（可配置）
- 提供隊列容量監控
- 支持動態調整隊列大小（可選）

## 測試策略

### 單元測試
- 測試 session 路由正確性
- 測試任務順序保證
- 測試並發安全性
- 測試錯誤處理

### 集成測試
- 測試完整的任務提交到執行流程
- 測試多 session 並發場景
- 測試 worker pool 擴縮容（如支持）

### 壓力測試
- 大量 session 測試（100k+ sessions）
- 高併發任務提交
- 長時間運行穩定性測試

### 性能基準測試
- 任務分發延遲
- 吞吐量測試
- 內存使用測試

## 使用示例

```go
// 創建 dispatcher
config := &WorkerConfig{
    PoolSize:    10,
    QueueSize:   1000,
    TaskTimeout: 30 * time.Second,
}

handler := &MyTaskHandler{} // 實現 TaskHandler interface

dispatcher, err := NewDispatcher(config, handler)
if err != nil {
    log.Fatal(err)
}

// 提交任務
task := &Task{
    ID:        "task-1",
    SessionID: "session-123",
    Payload:   map[string]interface{}{"action": "process"},
}

err = dispatcher.Submit(context.Background(), task)
if err != nil {
    log.Printf("Failed to submit task: %v", err)
}

// 優雅關閉
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
dispatcher.Shutdown(ctx)
```

## 擴展性

### 未來可能的擴展
- 支持任務優先級
- 支持任務依賴關係
- 支持分布式部署（多機器）
- 支持動態調整 worker 數量
- 支持任務持久化（重啟恢復）
- 提供 metrics 導出（Prometheus）
- 支持任務取消

## 依賴項

### 核心依賴
- Go 1.21+
- 無第三方核心依賴（標準庫實現）

### 可選依賴
- 日誌：`go.uber.org/zap` 或 `github.com/sirupsen/logrus`
- Metrics：`github.com/prometheus/client_golang`
- 測試：`github.com/stretchr/testify`

## 交付物

1. 核心包代碼
2. 完整的單元測試（覆蓋率 > 80%）
3. 使用文檔和示例
4. 性能基準測試報告
5. API 文檔（godoc）

## 開發里程碑

### Phase 1: 核心功能
- 實現基本的 dispatcher 和 worker pool
- 實現 session 路由和任務排隊
- 基本的錯誤處理

### Phase 2: 穩定性
- 完善錯誤處理和恢復機制
- 添加完整的單元測試
- 性能優化

### Phase 3: 監控和工具
- 添加統計和監控
- 完善文檔
- 提供使用示例

### Phase 4: 高級特性（可選）
- 任務優先級
- 動態擴縮容
- 分布式支持
