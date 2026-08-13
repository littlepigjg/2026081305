package stats

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"delayed-task-scheduler/internal/queue"
	"delayed-task-scheduler/pkg/logger"
)

type Reporter struct {
	interval      time.Duration
	enableGC      bool
	enableMem     bool
	logToStdout   bool
	taskQueue     *queue.TaskQueue
	serverStarted time.Time
	reportCount   int64
	lastReport    time.Time
	mu            sync.RWMutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
	latencySum    int64
	latencyCount  int64
	errorCount    int64
	requestCount  int64
}

func NewReporter(
	interval time.Duration,
	enableGC bool,
	enableMem bool,
	logToStdout bool,
	taskQueue *queue.TaskQueue,
) *Reporter {
	return &Reporter{
		interval:      interval,
		enableGC:      enableGC,
		enableMem:     enableMem,
		logToStdout:   logToStdout,
		taskQueue:     taskQueue,
		serverStarted: time.Now(),
		stopCh:        make(chan struct{}),
	}
}

func (r *Reporter) Start() {
	r.wg.Add(1)
	go r.reportLoop()
	logger.Infof("Stats reporter started: interval=%v", r.interval)
}

func (r *Reporter) Stop() {
	close(r.stopCh)
	r.wg.Wait()
	logger.Info("Stats reporter stopped")
}

func (r *Reporter) reportLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.reportOnce()

	for {
		select {
		case <-r.stopCh:
			logger.Info("Stats report loop exiting")
			return
		case <-ticker.C:
			r.reportOnce()
		}
	}
}

func (r *Reporter) reportOnce() {
	r.mu.Lock()
	r.lastReport = time.Now()
	r.reportCount++
	r.mu.Unlock()

	counts := r.taskQueue.CountByStatus()
	total := r.taskQueue.TotalCount()
	pending := r.taskQueue.PendingCount()
	ready := r.taskQueue.ReadyCount()
	completed := r.taskQueue.CompletedCount()

	var memStats runtime.MemStats
	if r.enableMem {
		runtime.ReadMemStats(&memStats)
	}

	var gcStats string
	if r.enableGC {
		gcStats = fmt.Sprintf(" | gc_cycles=%d, gc_pause=%v",
			memStats.NumGC,
			time.Duration(memStats.PauseTotalNs))
	}

	var memInfo string
	if r.enableMem {
		memInfo = fmt.Sprintf(" | mem_alloc=%dKB, mem_sys=%dKB, goroutines=%d",
			memStats.Alloc/1024,
			memStats.Sys/1024,
			runtime.NumGoroutine())
	}

	avgLatency := r.getAverageLatency()

	uptime := time.Since(r.serverStarted)
	uptimeStr := formatDuration(uptime)

	reportMsg := fmt.Sprintf(
		"[STATS] uptime=%s | total=%d, pending=%d, ready=%d, completed=%d | "+
			"requests=%d, errors=%d, avg_latency=%v%s%s",
		uptimeStr,
		total, pending, ready, completed,
		atomic.LoadInt64(&r.requestCount),
		atomic.LoadInt64(&r.errorCount),
		avgLatency,
		memInfo,
		gcStats,
	)

	if r.logToStdout {
		logger.Info(reportMsg)
	} else {
		logger.Debug(reportMsg)
	}

	logger.Debugf("Status distribution: pending=%d ready=%d completed=%d | Counts map: %v",
		pending, ready, completed, counts)
}

func (r *Reporter) reportOnceImmediate() string {
	counts := r.taskQueue.CountByStatus()
	total := r.taskQueue.TotalCount()
	pending := r.taskQueue.PendingCount()
	ready := r.taskQueue.ReadyCount()
	completed := r.taskQueue.CompletedCount()

	avgLatency := r.getAverageLatency()

	return fmt.Sprintf("Tasks: total=%d pending=%d ready=%d completed=%d | avg_latency=%v | counts_map=%v",
		total, pending, ready, completed, avgLatency, counts)
}

func (r *Reporter) getAverageLatency() time.Duration {
	sum := atomic.LoadInt64(&r.latencySum)
	count := atomic.LoadInt64(&r.latencyCount)
	if count == 0 {
		return 0
	}
	return time.Duration(sum / count)
}

func (r *Reporter) RecordLatency(d time.Duration) {
	atomic.AddInt64(&r.latencySum, int64(d))
	atomic.AddInt64(&r.latencyCount, 1)
}

func (r *Reporter) RecordRequest() {
	atomic.AddInt64(&r.requestCount, 1)
}

func (r *Reporter) RecordError() {
	atomic.AddInt64(&r.errorCount, 1)
}

func (r *Reporter) Status() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)

	return map[string]interface{}{
		"running":         true,
		"interval":        r.interval.String(),
		"report_count":    r.reportCount,
		"last_report":     r.lastReport.Format(time.RFC3339),
		"uptime":          time.Since(r.serverStarted).String(),
		"total_requests":  atomic.LoadInt64(&r.requestCount),
		"total_errors":    atomic.LoadInt64(&r.errorCount),
		"avg_latency_ms":  float64(r.getAverageLatency().Microseconds()) / 1000.0,
		"goroutines":      runtime.NumGoroutine(),
		"memory_alloc_kb": memStats.Alloc / 1024,
	}
}

func (r *Reporter) ReportWithContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.reportOnce()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", m, s)
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", h, m)
}
