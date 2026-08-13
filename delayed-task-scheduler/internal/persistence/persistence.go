package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"delayed-task-scheduler/internal/model"
	"delayed-task-scheduler/pkg/logger"
)

type Persister struct {
	filePath    string
	atomicWrite bool
	compress    bool
	maxBackups  int
}

type dumpFile struct {
	Version   string            `json:"version"`
	Timestamp time.Time         `json:"timestamp"`
	Tasks     []*model.Task     `json:"tasks"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

func NewPersister(filePath string, atomicWrite bool, compress bool, maxBackups int) *Persister {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		logger.Warnf("Failed to create persistence directory: %v", err)
	}
	return &Persister{
		filePath:    filePath,
		atomicWrite: atomicWrite,
		compress:    compress,
		maxBackups:  maxBackups,
	}
}

func (p *Persister) Save(tasks []*model.Task) error {
	if len(tasks) == 0 {
		logger.Infof("No tasks to persist, removing dump file if exists")
		if err := os.Remove(p.filePath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}

	dump := dumpFile{
		Version:   "1.0",
		Timestamp: time.Now(),
		Tasks:     tasks,
		Metadata: map[string]string{
			"total_tasks": fmt.Sprintf("%d", len(tasks)),
			"created_at":  time.Now().Format(time.RFC3339),
		},
	}

	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal dump: %w", err)
	}

	if p.atomicWrite {
		return p.atomicSave(data)
	}

	return p.directSave(data)
}

func (p *Persister) atomicSave(data []byte) error {
	dir := filepath.Dir(p.filePath)
	base := filepath.Base(p.filePath)
	tmpFile := filepath.Join(dir, base+".tmp")

	err := os.WriteFile(tmpFile, data, 0644)
	if err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	err = os.Rename(tmpFile, p.filePath)
	if err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rename temp file: %w", err)
	}

	logger.Infof("Atomically saved %d tasks to %s", len(data), p.filePath)
	return nil
}

func (p *Persister) directSave(data []byte) error {
	err := os.WriteFile(p.filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	logger.Infof("Directly saved tasks to %s", p.filePath)
	return nil
}

func (p *Persister) Load() ([]*model.Task, error) {
	data, err := os.ReadFile(p.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Infof("No dump file found at %s, starting fresh", p.filePath)
			return nil, nil
		}
		return nil, fmt.Errorf("read dump file: %w", err)
	}

	if len(data) == 0 {
		logger.Infof("Dump file is empty, starting fresh")
		return nil, nil
	}

	dump := dumpFile{}
	err = json.Unmarshal(data, &dump)
	if err != nil {
		logger.Errorf("Failed to parse dump: %v", err)
		return p.tryLoadLegacy(data)
	}

	logger.Infof("Loaded %d tasks from dump file (version=%s)", len(dump.Tasks), dump.Version)
	return dump.Tasks, nil
}

func (p *Persister) tryLoadLegacy(data []byte) ([]*model.Task, error) {
	var tasks []*model.Task
	err := json.Unmarshal(data, &tasks)
	if err != nil {
		return nil, fmt.Errorf("parse legacy dump: %w", err)
	}
	logger.Infof("Loaded %d tasks from legacy format", len(tasks))
	return tasks, nil
}

func (p *Persister) Backup() error {
	if p.maxBackups <= 0 {
		return nil
	}

	dir := filepath.Dir(p.filePath)
	base := filepath.Base(p.filePath)
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(dir, fmt.Sprintf("%s.%s.bak", base, timestamp))

	data, err := os.ReadFile(p.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	err = os.WriteFile(backupFile, data, 0644)
	if err != nil {
		return fmt.Errorf("write backup: %w", err)
	}

	logger.Infof("Backup created: %s", backupFile)
	p.cleanupOldBackups(dir, base)
	return nil
}

func (p *Persister) cleanupOldBackups(dir, base string) {
	pattern := fmt.Sprintf("%s.*.bak", base)
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return
	}

	if len(matches) <= p.maxBackups {
		return
	}

	sortStrings(matches)
	for i := 0; i < len(matches)-p.maxBackups; i++ {
		if err := os.Remove(matches[i]); err != nil {
			logger.Warnf("Failed to remove old backup %s: %v", matches[i], err)
		}
	}
}

func sortStrings(s []string) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func (p *Persister) Exists() bool {
	_, err := os.Stat(p.filePath)
	return err == nil
}

func (p *Persister) Remove() error {
	err := os.Remove(p.filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (p *Persister) FilePath() string {
	return p.filePath
}
