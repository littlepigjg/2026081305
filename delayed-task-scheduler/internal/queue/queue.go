package queue

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"delayed-task-scheduler/internal/model"
	"delayed-task-scheduler/pkg/logger"
)

type TaskQueue struct {
	mu      sync.RWMutex
	tasks   []*model.Task
	taskMap map[string]*model.Task
	order   []string
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		tasks:   make([]*model.Task, 0),
		taskMap: make(map[string]*model.Task),
		order:   make([]string, 0),
	}
}

func (q *TaskQueue) Add(task *model.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.taskMap[task.ID]; exists {
		return fmt.Errorf("task with id %s already exists", task.ID)
	}

	q.tasks = append(q.tasks, task)
	q.taskMap[task.ID] = task
	q.order = append(q.order, task.ID)

	logger.Debugf("Task added: id=%s, delay=%d, ready_at=%s",
		task.ID, task.DelaySeconds, task.ReadyAt.Format(time.RFC3339))
	return nil
}

func (q *TaskQueue) Get(id string) (*model.Task, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	t, exists := q.taskMap[id]
	if !exists {
		return nil, fmt.Errorf("task not found: %s", id)
	}
	return t, nil
}

func (q *TaskQueue) Exists(id string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, exists := q.taskMap[id]
	return exists
}

func (q *TaskQueue) Remove(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, exists := q.taskMap[id]
	if !exists {
		return fmt.Errorf("task not found: %s", id)
	}

	for i, task := range q.tasks {
		if task.ID == id {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			break
		}
	}

	delete(q.taskMap, id)

	for i, o := range q.order {
		if o == id {
			q.order = append(q.order[:i], q.order[i+1:]...)
			break
		}
	}

	logger.Debugf("Task removed: id=%s, status=%s", id, t.Status)
	return nil
}

func (q *TaskQueue) MarkReady(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, exists := q.taskMap[id]
	if !exists {
		return fmt.Errorf("task not found: %s", id)
	}

	if t.Status != model.StatusPending {
		return fmt.Errorf("task %s is not in pending state, current: %s", id, t.Status)
	}

	t.MarkReady()
	logger.Debugf("Task marked ready: id=%s", id)
	return nil
}

func (q *TaskQueue) MarkCompleted(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, exists := q.taskMap[id]
	if !exists {
		return fmt.Errorf("task not found: %s", id)
	}

	t.MarkCompleted()
	logger.Debugf("Task marked completed: id=%s", id)
	return nil
}

func (q *TaskQueue) GetReadyTasks() ([]*model.Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.removeCompletedLocked()

	ready := make([]*model.Task, 0)
	now := time.Now()

	for _, t := range q.tasks {
		if t.Status == model.StatusReady || (t.Status == model.StatusPending && !now.Before(t.ReadyAt)) {
			ready = append(ready, t)
		}
	}

	sort.Slice(ready, func(i, j int) bool {
		return ready[i].ReadyAt.Before(ready[j].ReadyAt)
	})

	return ready, nil
}

func (q *TaskQueue) GetPendingTasks() ([]*model.Task, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	pending := make([]*model.Task, 0)
	for _, t := range q.tasks {
		if t.Status == model.StatusPending {
			pending = append(pending, t)
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		return pending[i].ReadyAt.Before(pending[j].ReadyAt)
	})

	return pending, nil
}

func (q *TaskQueue) GetAllTasks() ([]*model.Task, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	all := make([]*model.Task, len(q.tasks))
	copy(all, q.tasks)

	sort.Slice(all, func(i, j int) bool {
		return all[i].ReadyAt.Before(all[j].ReadyAt)
	})

	return all, nil
}

func (q *TaskQueue) Count() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.tasks)
}

func (q *TaskQueue) CountByStatus() map[model.TaskStatus]int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	counts := map[model.TaskStatus]int{
		model.StatusPending:   0,
		model.StatusReady:     0,
		model.StatusCompleted: 0,
	}

	for _, t := range q.tasks {
		counts[t.Status]++
	}

	return counts
}

func (q *TaskQueue) TotalCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.tasks)
}

func (q *TaskQueue) PendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	c := 0
	for _, t := range q.tasks {
		if t.Status == model.StatusPending {
			c++
		}
	}
	return c
}

func (q *TaskQueue) ReadyCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	c := 0
	for _, t := range q.tasks {
		if t.Status == model.StatusReady {
			c++
		}
	}
	return c
}

func (q *TaskQueue) CompletedCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	c := 0
	for _, t := range q.tasks {
		if t.Status == model.StatusCompleted {
			c++
		}
	}
	return c
}

func (q *TaskQueue) RemoveCompleted() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.removeCompletedLocked()
}

func (q *TaskQueue) removeCompletedLocked() {
	i := 0
	for range q.tasks {
		if q.tasks[i].Status == model.StatusCompleted {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
		}
		i++
	}

	newMap := make(map[string]*model.Task)
	newOrder := make([]string, 0, len(q.order))
	for _, t := range q.tasks {
		newMap[t.ID] = t
		newOrder = append(newOrder, t.ID)
	}
	q.taskMap = newMap
	q.order = newOrder

	logger.Debugf("RemoveCompleted executed, remaining tasks: %d", len(q.tasks))
}

func (q *TaskQueue) CleanupExpired(timeout time.Duration) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	cutoff := time.Now().Add(-timeout)
	removed := 0

	for i, t := range q.tasks {
		if t.CompletedAt != nil && t.CompletedAt.Before(cutoff) {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			delete(q.taskMap, t.ID)
			removed++
		}
	}

	return removed
}

func (q *TaskQueue) Reload(tasks []*model.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.tasks = make([]*model.Task, len(tasks))
	copy(q.tasks, tasks)
	q.taskMap = make(map[string]*model.Task)
	q.order = make([]string, 0, len(tasks))

	for _, t := range tasks {
		q.taskMap[t.ID] = t
		q.order = append(q.order, t.ID)
	}

	logger.Infof("Reloaded %d tasks from persistence", len(tasks))
}

func (q *TaskQueue) GetTasksForDump() []*model.Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result := make([]*model.Task, 0)
	for _, t := range q.tasks {
		if t.Status == model.StatusPending || t.Status == model.StatusReady {
			result = append(result, t)
		}
	}
	return result
}
