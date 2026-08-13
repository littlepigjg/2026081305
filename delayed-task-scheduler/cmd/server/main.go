package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"delayed-task-scheduler/internal/config"
	"delayed-task-scheduler/internal/handler"
	"delayed-task-scheduler/internal/persistence"
	"delayed-task-scheduler/internal/queue"
	"delayed-task-scheduler/internal/scanner"
	"delayed-task-scheduler/internal/stats"
	"delayed-task-scheduler/pkg/logger"
)

var (
	version   = "1.0.0"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	logger.Infof("Starting Delayed Task Scheduler v%s (commit=%s, built=%s)", version, gitCommit, buildTime)

	configPath := flag.String("config", "", "Path to configuration file")
	envMode := flag.Bool("env", false, "Load configuration from environment variables")
	flag.Parse()

	var cfg *config.Config
	var err error

	if *configPath != "" {
		cfg, err = config.LoadFromFile(*configPath)
		if err != nil {
			logger.Fatalf("Failed to load config from file: %v", err)
		}
		logger.Infof("Configuration loaded from file: %s", *configPath)
	} else if *envMode {
		cfg, err = config.LoadFromEnv()
		if err != nil {
			logger.Fatalf("Failed to load config from environment: %v", err)
		}
		logger.Info("Configuration loaded from environment variables")
	} else {
		cfg = config.Default()
		logger.Info("Using default configuration")
	}

	if err := cfg.Validate(); err != nil {
		logger.Fatalf("Invalid configuration: %v", err)
	}

	logger.Infof("Server will listen on %s", cfg.Address())

	taskQueue := queue.NewTaskQueue()

	persister := persistence.NewPersister(
		cfg.Persistence.FilePath,
		cfg.Persistence.AtomicWrite,
		cfg.Persistence.Compress,
		cfg.Persistence.MaxBackupFiles,
	)

	if cfg.Persistence.Enabled && persister.Exists() {
		tasks, err := persister.Load()
		if err != nil {
			logger.Errorf("Failed to load persisted tasks: %v", err)
		} else if len(tasks) > 0 {
			taskQueue.Reload(tasks)
			logger.Infof("Restored %d tasks from persistence", len(tasks))
		}
	}

	taskScanner := scanner.NewScanner(
		cfg.Scanner.Interval,
		cfg.Scanner.BatchSize,
		cfg.Scanner.EnableParallel,
		cfg.Scanner.MaxGoroutines,
		taskQueue,
	)

	statsReporter := stats.NewReporter(
		cfg.Stats.Interval,
		cfg.Stats.EnableGCStats,
		cfg.Stats.EnableMemStats,
		cfg.Stats.LogToStdout,
		taskQueue,
	)

	h := handler.NewHandler(taskQueue, taskScanner, statsReporter)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	server := &http.Server{
		Addr:           cfg.Address(),
		Handler:        mux,
		ReadTimeout:    cfg.Server.ReadTimeout,
		WriteTimeout:   cfg.Server.WriteTimeout,
		MaxHeaderBytes: cfg.Server.MaxHeaderBytes,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Infof("HTTP server starting on %s", cfg.Address())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("HTTP server failed: %v", err)
		}
	}()

	taskScanner.Start()
	statsReporter.Start()

	go func() {
		time.Sleep(100 * time.Millisecond)
		logger.Infof("Startup complete. Server ready on %s", cfg.Address())
	}()

	sig := <-stop
	logger.Infof("Received signal %s, initiating graceful shutdown...", sig)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	logger.Info("Stopping scanner...")
	taskScanner.Stop()

	logger.Info("Stopping stats reporter...")
	statsReporter.Stop()

	logger.Info("Shutting down HTTP server...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorf("HTTP server shutdown error: %v", err)
	}

	if cfg.Persistence.Enabled {
		logger.Info("Persisting pending and ready tasks...")
		tasksToDump := taskQueue.GetTasksForDump()
		if err := persister.Save(tasksToDump); err != nil {
			logger.Errorf("Failed to persist tasks: %v", err)
		} else {
			logger.Infof("Successfully persisted %d tasks to %s", len(tasksToDump), cfg.Persistence.FilePath)
		}
	}

	totalRemaining := taskQueue.TotalCount()
	logger.Infof("Graceful shutdown complete. Remaining in-memory tasks: %d", totalRemaining)
	logger.Info("Server stopped successfully")

	fmt.Fprintf(os.Stdout, "Server shutdown complete\n")
}
