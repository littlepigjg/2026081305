package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server           ServerConfig           `json:"server"`
	Scanner          ScannerConfig          `json:"scanner"`
	Stats            StatsConfig            `json:"stats"`
	Persistence      PersistenceConfig      `json:"persistence"`
	Logging          LoggingConfig          `json:"logging"`
	Worker           WorkerConfig           `json:"worker"`
}

type ServerConfig struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	ReadTimeout     time.Duration `json:"read_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	MaxHeaderBytes  int           `json:"max_header_bytes"`
	EnableCORS      bool          `json:"enable_cors"`
	TrustedOrigins  []string      `json:"trusted_origins"`
}

type ScannerConfig struct {
	Interval       time.Duration `json:"interval"`
	BatchSize      int           `json:"batch_size"`
	EnableParallel bool          `json:"enable_parallel"`
	MaxGoroutines  int           `json:"max_goroutines"`
}

type StatsConfig struct {
	Interval      time.Duration `json:"interval"`
	EnableGCStats bool          `json:"enable_gc_stats"`
	EnableMemStats bool         `json:"enable_mem_stats"`
	LogToStdout   bool          `json:"log_to_stdout"`
}

type PersistenceConfig struct {
	Enabled       bool   `json:"enabled"`
	FilePath      string `json:"file_path"`
	AtomicWrite   bool   `json:"atomic_write"`
	Compress      bool   `json:"compress"`
	MaxBackupFiles int   `json:"max_backup_files"`
}

type LoggingConfig struct {
	Level      string `json:"level"`
	Format     string `json:"format"`
	Output     string `json:"output"`
	ShowCaller bool   `json:"show_caller"`
}

type WorkerConfig struct {
	MaxTasks     int           `json:"max_tasks"`
	TaskTimeout  time.Duration `json:"task_timeout"`
	RetryDelay   time.Duration `json:"retry_delay"`
	MaxRetries   int           `json:"max_retries"`
}

func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    30 * time.Second,
			ShutdownTimeout: 5 * time.Second,
			MaxHeaderBytes:  1 << 20,
			EnableCORS:      false,
			TrustedOrigins:  []string{"*"},
		},
		Scanner: ScannerConfig{
			Interval:       2 * time.Second,
			BatchSize:      100,
			EnableParallel: false,
			MaxGoroutines:  4,
		},
		Stats: StatsConfig{
			Interval:       30 * time.Second,
			EnableGCStats:  true,
			EnableMemStats: true,
			LogToStdout:    true,
		},
		Persistence: PersistenceConfig{
			Enabled:        true,
			FilePath:       "tasks_dump.json",
			AtomicWrite:    true,
			Compress:       false,
			MaxBackupFiles: 3,
		},
		Logging: LoggingConfig{
			Level:      "info",
			Format:     "text",
			Output:     "stdout",
			ShowCaller: true,
		},
		Worker: WorkerConfig{
			MaxTasks:    1000,
			TaskTimeout: 30 * time.Second,
			RetryDelay:  5 * time.Second,
			MaxRetries:  3,
		},
	}
}

func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	cfg := Default()
	err = json.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.Validate()
	return cfg, nil
}

func LoadFromEnv() (*Config, error) {
	cfg := Default()

	if v := os.Getenv("SCHEDULER_HOST"); v != "" {
		cfg.Server.Host = v
	}
	if v := os.Getenv("SCHEDULER_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 && p < 65536 {
			cfg.Server.Port = p
		}
	}
	if v := os.Getenv("SCHEDULER_SCAN_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Scanner.Interval = d
		}
	}
	if v := os.Getenv("SCHEDULER_STATS_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Stats.Interval = d
		}
	}
	if v := os.Getenv("SCHEDULER_DUMP_FILE"); v != "" {
		cfg.Persistence.FilePath = v
	}
	if v := os.Getenv("SCHEDULER_LOG_LEVEL"); v != "" {
		cfg.Logging.Level = strings.ToLower(v)
	}
	if v := os.Getenv("SCHEDULER_SHUTDOWN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Server.ShutdownTimeout = d
		}
	}

	cfg.Validate()
	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Server.Port)
	}
	if c.Scanner.Interval < 100*time.Millisecond {
		return fmt.Errorf("scan interval too short: %v", c.Scanner.Interval)
	}
	if c.Server.ShutdownTimeout < 1*time.Second {
		return fmt.Errorf("shutdown timeout too short: %v", c.Server.ShutdownTimeout)
	}
	if c.Scanner.BatchSize <= 0 {
		c.Scanner.BatchSize = 100
	}
	if c.Worker.MaxTasks <= 0 {
		c.Worker.MaxTasks = 1000
	}
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[strings.ToLower(c.Logging.Level)] {
		c.Logging.Level = "info"
	}
	return nil
}

func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}
