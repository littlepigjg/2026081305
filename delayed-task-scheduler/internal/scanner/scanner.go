package scanner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"delayed-task-scheduler/internal/model"
	"delayed-task-scheduler/internal/queue"
	"delayed-task-scheduler/pkg/logger"
)

type Scanner struct {
	interval       time.Duration
	batchSize      int
	enableParallel bool
	maxGoroutines  int
	taskQueue      *queue.TaskQueue
	stopCh         chan struct{}
	wg             sync.WaitGroup
	lastScanTime   time.Time
	scanCount      int64
	mu             sync.RWMutex
}

func NewScanner(
	interval time.Duration,
	batchSize int,
	enableParallel bool,
	maxGoroutines int,
	taskQueue *queue.TaskQueue,
) *Scanner {
	return &Scanner{
		interval:       interval,
		batchSize:      batchSize,
		enableParallel: enableParallel,
		maxGoroutines:  maxGoroutines,
		taskQueue:      taskQueue,
		stopCh:         make(chan struct{}),
	}
}

func (s *Scanner) Start() {
	s.wg.Add(1)
	go s.scanLoop()
	logger.Infof("Scanner started: interval=%v, batch_size=%d, parallel=%v",
		s.interval, s.batchSize, s.enableParallel)
}

func (s *Scanner) Stop() {
	close(s.stopCh)
	s.wg.Wait()
	logger.Info("Scanner stopped")
}

func (s *Scanner) scanLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			logger.Info("Scanner scan loop exiting")
			return
		case <-ticker.C:
			s.executeScan()
		}
	}
}

func (s *Scanner) executeScan() {
	s.mu.Lock()
	s.lastScanTime = time.Now()
	s.scanCount++
	currentScan := s.scanCount
	s.mu.Unlock()

	pendingTasks, err := s.taskQueue.GetPendingTasks()
	if err != nil {
		logger.Errorf("Failed to get pending tasks: %v", err)
		return
	}

	if len(pendingTasks) == 0 {
		logger.Debugf("Scan #%d: No pending tasks to check", currentScan)
		return
	}

	now := time.Now()
	readyCount := 0
	stillPending := 0

	for _, t := range pendingTasks {
		if now.After(t.ReadyAt) || now.Equal(t.ReadyAt) {
			if err := s.taskQueue.MarkReady(t.ID); err != nil {
				logger.Errorf("Failed to mark task %s as ready: %v", t.ID, err)
			} else {
				readyCount++
			}
		} else {
			stillPending++
		}
	}

	logger.Infof("Scan #%d: %d pending tasks scanned, %d marked ready, %d still pending",
		currentScan, len(pendingTasks), readyCount, stillPending)

	if readyCount > 0 {
		s.cleanupCompletedTasks()
	}
}

func (s *Scanner) executeScanParallel() {
	pendingTasks, err := s.taskQueue.GetPendingTasks()
	if err != nil {
		logger.Errorf("Failed to get pending tasks: %v", err)
		return
	}

	if len(pendingTasks) == 0 {
		return
	}

	tasksCh := make(chan []*model.Task, s.maxGoroutines)
	resultCh := make(chan int, s.maxGoroutines)

	var wg sync.WaitGroup
	for g := 0; g < s.maxGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range tasksCh {
				ready := 0
				now := time.Now()
				for _, t := range batch {
					if now.After(t.ReadyAt) || now.Equal(t.ReadyAt) {
						if err := s.taskQueue.MarkReady(t.ID); err == nil {
							ready++
						}
					}
				}
				resultCh <- ready
			}
		}()
	}

	batchSize := s.batchSize
	for i := 0; i < len(pendingTasks); i += batchSize {
		end := i + batchSize
		if end > len(pendingTasks) {
			end = len(pendingTasks)
		}
		tasksCh <- pendingTasks[i:end]
	}
	close(tasksCh)

	wg.Wait()
	close(resultCh)

	totalReady := 0
	for r := range resultCh {
		totalReady += r
	}

	logger.Infof("Parallel scan: %d tasks processed, %d marked ready", len(pendingTasks), totalReady)
}

func (s *Scanner) cleanupCompletedTasks() {
	s.taskQueue.RemoveCompleted()
}

func (s *Scanner) ScanOnce() {
	s.executeScan()
}

func (s *Scanner) ScanWithContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.executeScan()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("scan context cancelled: %v", ctx.Err())
	}
}

func (s *Scanner) LastScanTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastScanTime
}

func (s *Scanner) ScanCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.scanCount
}

func (s *Scanner) Interval() time.Duration {
	return s.interval
}

func (s *Scanner) SetInterval(d time.Duration) {
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	s.interval = d
	logger.Infof("Scan interval updated to %v", d)
}

func (s *Scanner) Status() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]interface{}{
		"running":        true,
		"interval":       s.interval.String(),
		"last_scan_time": s.lastScanTime.Format(time.RFC3339),
		"scan_count":     s.scanCount,
		"batch_size":     s.batchSize,
		"parallel":       s.enableParallel,
		"max_goroutines": s.maxGoroutines,
	}
}
