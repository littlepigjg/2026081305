package model

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusReady     TaskStatus = "ready"
	StatusCompleted TaskStatus = "completed"
)

type Task struct {
	ID            string     `json:"id"`
	Payload       string     `json:"payload"`
	Status        TaskStatus `json:"status"`
	DelaySeconds  int        `json:"delay_seconds"`
	CreatedAt     time.Time  `json:"created_at"`
	ReadyAt       time.Time  `json:"ready_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	RetryCount    int        `json:"retry_count"`
	MaxRetry      int        `json:"max_retry"`
	Priority      int        `json:"priority"`
	Tag           string     `json:"tag,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	AssignedWorker string   `json:"assigned_worker,omitempty"`
	TimeoutSeconds int       `json:"timeout_seconds"`
}

type CreateTaskRequest struct {
	ID           string `json:"id"`
	Payload      string `json:"payload"`
	DelaySeconds int    `json:"delay_seconds"`
	Priority     int    `json:"priority,omitempty"`
	Tag          string `json:"tag,omitempty"`
	MaxRetry     int    `json:"max_retry,omitempty"`
	TimeoutSeconds int  `json:"timeout_seconds,omitempty"`
}

type CreateTaskResponse struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	ReadyAt  string `json:"ready_at"`
	Message  string `json:"message,omitempty"`
}

type TaskStatusResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	ReadyAt   string `json:"ready_at"`
}

type ReadyTaskResponse struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
	ReadyAt string `json:"ready_at"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Uptime    string `json:"uptime"`
	Version   string `json:"version"`
	Tasks     TaskSummary `json:"tasks"`
}

type TaskSummary struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Ready     int `json:"ready"`
	Completed int `json:"completed"`
}

type StatsResponse struct {
	TotalTasks     int     `json:"total_tasks"`
	PendingCount   int     `json:"pending_count"`
	ReadyCount     int     `json:"ready_count"`
	CompletedCount int     `json:"completed_count"`
	AvgLatency     float64 `json:"avg_latency_ms"`
	Uptime         string  `json:"uptime"`
	ScanInterval   string  `json:"scan_interval"`
	LastScanTime   string  `json:"last_scan_time"`
}

func NewTask(req CreateTaskRequest) (*Task, error) {
	if req.ID == "" {
		return nil, fmt.Errorf("task id is required")
	}
	if len(req.ID) > 256 {
		return nil, fmt.Errorf("task id too long: %d characters", len(req.ID))
	}
	if req.DelaySeconds < 0 {
		return nil, fmt.Errorf("delay_seconds must be non-negative: %d", req.DelaySeconds)
	}
	if req.DelaySeconds > 86400*30 {
		return nil, fmt.Errorf("delay_seconds too large: %d", req.DelaySeconds)
	}
	if req.Payload == "" {
		return nil, fmt.Errorf("payload is required")
	}

	now := time.Now()
	readyAt := now.Add(time.Duration(req.DelaySeconds) * time.Second)

	maxRetry := req.MaxRetry
	if maxRetry <= 0 {
		maxRetry = 0
	}

	timeout := req.TimeoutSeconds
	if timeout <= 0 {
		timeout = 30
	}

	priority := req.Priority
	if priority < 0 {
		priority = 0
	}

	return &Task{
		ID:             req.ID,
		Payload:        req.Payload,
		Status:         StatusPending,
		DelaySeconds:   req.DelaySeconds,
		CreatedAt:      now,
		ReadyAt:        readyAt,
		RetryCount:     0,
		MaxRetry:       maxRetry,
		Priority:       priority,
		Tag:            req.Tag,
		Metadata:       make(map[string]string),
		TimeoutSeconds: timeout,
	}, nil
}

func (t *Task) IsReady() bool {
	return time.Now().After(t.ReadyAt)
}

func (t *Task) IsExpired() bool {
	if t.CompletedAt == nil {
		return false
	}
	expireAt := t.CompletedAt.Add(time.Duration(t.TimeoutSeconds) * time.Second)
	return time.Now().After(expireAt)
}

func (t *Task) MarkReady() {
	t.Status = StatusReady
}

func (t *Task) MarkCompleted() {
	now := time.Now()
	t.CompletedAt = &now
	t.Status = StatusCompleted
}

func (t *Task) IncrRetry() {
	t.RetryCount++
}

func (t *Task) HasRetries() bool {
	return t.RetryCount < t.MaxRetry
}

func (t *Task) ToResponse() ReadyTaskResponse {
	return ReadyTaskResponse{
		ID:      t.ID,
		Payload: t.Payload,
		ReadyAt: t.ReadyAt.Format(time.RFC3339),
	}
}

func (t *Task) ToStatusResponse() TaskStatusResponse {
	return TaskStatusResponse{
		ID:        t.ID,
		Status:    string(t.Status),
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
		ReadyAt:   t.ReadyAt.Format(time.RFC3339),
	}
}

func (t *Task) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func UnmarshalTask(data []byte) (*Task, error) {
	t := &Task{}
	err := json.Unmarshal(data, t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func MakeErrorResponse(err error, code int) ErrorResponse {
	return ErrorResponse{
		Error: err.Error(),
		Code:  code,
	}
}

func MakeErrorResponseMsg(msg string, code int) ErrorResponse {
	return ErrorResponse{
		Error: msg,
		Code:  code,
	}
}
