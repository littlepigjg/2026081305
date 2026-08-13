package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"delayed-task-scheduler/internal/model"
	"delayed-task-scheduler/internal/queue"
	"delayed-task-scheduler/internal/scanner"
	"delayed-task-scheduler/internal/stats"
	"delayed-task-scheduler/pkg/logger"
)

type Handler struct {
	taskQueue *queue.TaskQueue
	scanner   *scanner.Scanner
	stats     *stats.Reporter
	startTime time.Time
	version   string
}

func NewHandler(
	taskQueue *queue.TaskQueue,
	scanner *scanner.Scanner,
	stats *stats.Reporter,
) *Handler {
	return &Handler{
		taskQueue: taskQueue,
		scanner:   scanner,
		stats:     stats,
		startTime: time.Now(),
		version:   "1.0.0",
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/task", h.withMiddleware(h.handleTask))
	mux.HandleFunc("/api/v1/task/", h.withMiddleware(h.handleTaskByID))
	mux.HandleFunc("/api/v1/task/ready", h.withMiddleware(h.handleReadyTasks))
	mux.HandleFunc("/health", h.withMiddleware(h.handleHealth))
	mux.HandleFunc("/api/v1/stats", h.withMiddleware(h.handleStats))
}

func (h *Handler) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return h.loggingMiddleware(h.corsMiddleware(h.recoveryMiddleware(next)))
}

func (h *Handler) loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.stats.RecordRequest()

		lrw := &logResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next(lrw, r)

		duration := time.Since(start)
		h.stats.RecordLatency(duration)

		logger.Infof("method=%s path=%s status=%d latency=%v remote=%s",
			r.Method, r.URL.Path, lrw.statusCode, duration, r.RemoteAddr)
	}
}

func (h *Handler) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func (h *Handler) recoveryMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Errorf("PANIC recovered: %v", err)
				h.stats.RecordError()
				writeJSON(w, http.StatusInternalServerError, model.MakeErrorResponseMsg(
					fmt.Sprintf("internal server error: %v", err), http.StatusInternalServerError))
			}
		}()
		next(w, r)
	}
}

func (h *Handler) handleTask(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createTask(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, model.MakeErrorResponseMsg(
			"method not allowed", http.StatusMethodNotAllowed))
	}
}

func (h *Handler) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/task/")
	pathParts := strings.SplitN(path, "/", 2)
	taskID := pathParts[0]

	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, model.MakeErrorResponseMsg(
			"task id is required", http.StatusBadRequest))
		return
	}

	if len(pathParts) > 1 && pathParts[1] == "status" {
		if r.Method == http.MethodGet {
			h.getTaskStatus(w, r, taskID)
			return
		}
		writeJSON(w, http.StatusMethodNotAllowed, model.MakeErrorResponseMsg(
			"method not allowed", http.StatusMethodNotAllowed))
		return
	}

	switch r.Method {
	case http.MethodDelete:
		h.deleteTask(w, r, taskID)
	case http.MethodGet:
		h.getTask(w, r, taskID)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, model.MakeErrorResponseMsg(
			"method not allowed", http.StatusMethodNotAllowed))
	}
}

func (h *Handler) handleReadyTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, model.MakeErrorResponseMsg(
			"method not allowed", http.StatusMethodNotAllowed))
		return
	}

	tasks, err := h.taskQueue.GetReadyTasks()
	if err != nil {
		logger.Errorf("Failed to get ready tasks: %v", err)
		h.stats.RecordError()
		writeJSON(w, http.StatusInternalServerError, model.MakeErrorResponseMsg(
			"failed to get ready tasks", http.StatusInternalServerError))
		return
	}

	response := make([]model.ReadyTaskResponse, 0)
	for _, t := range tasks {
		response = append(response, t.ToResponse())
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, model.MakeErrorResponseMsg(
			"method not allowed", http.StatusMethodNotAllowed))
		return
	}

	counts := h.taskQueue.CountByStatus()
	uptime := time.Since(h.startTime)

	resp := model.HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().Format(time.RFC3339),
		Uptime:    formatUptime(uptime),
		Version:   h.version,
		Tasks: model.TaskSummary{
			Total:     h.taskQueue.TotalCount(),
			Pending:   counts[model.StatusPending],
			Ready:     counts[model.StatusReady],
			Completed: counts[model.StatusCompleted],
		},
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, model.MakeErrorResponseMsg(
			"method not allowed", http.StatusMethodNotAllowed))
		return
	}

	taskStats := h.stats.Status()
	counts := h.taskQueue.CountByStatus()

	resp := map[string]interface{}{
		"tasks": map[string]interface{}{
			"total":     h.taskQueue.TotalCount(),
			"pending":   counts[model.StatusPending],
			"ready":     counts[model.StatusReady],
			"completed": counts[model.StatusCompleted],
		},
		"scanner": h.scanner.Status(),
		"runtime": taskStats,
		"uptime":  time.Since(h.startTime).String(),
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	var req model.CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.MakeErrorResponseMsg(
			fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest))
		return
	}

	if req.ID == "" {
		writeJSON(w, http.StatusBadRequest, model.MakeErrorResponseMsg(
			"task id is required", http.StatusBadRequest))
		return
	}

	if h.taskQueue.Exists(req.ID) {
		writeJSON(w, http.StatusConflict, model.MakeErrorResponseMsg(
			fmt.Sprintf("task with id %s already exists", req.ID), http.StatusConflict))
		return
	}

	task, err := model.NewTask(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, model.MakeErrorResponseMsg(
			err.Error(), http.StatusBadRequest))
		return
	}

	if err := h.taskQueue.Add(task); err != nil {
		logger.Errorf("Failed to add task: %v", err)
		h.stats.RecordError()
		writeJSON(w, http.StatusInternalServerError, model.MakeErrorResponseMsg(
			"failed to create task", http.StatusInternalServerError))
		return
	}

	resp := model.CreateTaskResponse{
		ID:      task.ID,
		Status:  string(task.Status),
		ReadyAt: task.ReadyAt.Format(time.RFC3339),
	}

	logger.Infof("Task created: id=%s, delay=%d, ready_at=%s", task.ID, task.DelaySeconds, resp.ReadyAt)
	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request, taskID string) {
	task, err := h.taskQueue.Get(taskID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, model.MakeErrorResponseMsg(
			fmt.Sprintf("task not found: %s", taskID), http.StatusNotFound))
		return
	}

	writeJSON(w, http.StatusOK, task.ToStatusResponse())
}

func (h *Handler) getTaskStatus(w http.ResponseWriter, r *http.Request, taskID string) {
	task, err := h.taskQueue.Get(taskID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, model.MakeErrorResponseMsg(
			fmt.Sprintf("task not found: %s", taskID), http.StatusNotFound))
		return
	}

	writeJSON(w, http.StatusOK, task.ToStatusResponse())
}

func (h *Handler) deleteTask(w http.ResponseWriter, r *http.Request, taskID string) {
	if !h.taskQueue.Exists(taskID) {
		writeJSON(w, http.StatusNotFound, model.MakeErrorResponseMsg(
			fmt.Sprintf("task not found: %s", taskID), http.StatusNotFound))
		return
	}

	if err := h.taskQueue.MarkCompleted(taskID); err != nil {
		logger.Errorf("Failed to mark task completed: %v", err)
		h.stats.RecordError()
		writeJSON(w, http.StatusInternalServerError, model.MakeErrorResponseMsg(
			"failed to mark task completed", http.StatusInternalServerError))
		return
	}

	logger.Infof("Task completed: id=%s", taskID)
	writeJSON(w, http.StatusNoContent, nil)
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}

func formatUptime(d time.Duration) string {
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

type logResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *logResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}
