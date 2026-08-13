package logger

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

var levelNames = map[Level]string{
	DebugLevel: "DEBUG",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
	FatalLevel: "FATAL",
}

type Logger struct {
	mu       sync.RWMutex
	output   io.Writer
	level    Level
	prefix   string
	showCall bool
}

var (
	defaultLogger *Logger
	once          sync.Once
)

func init() {
	defaultLogger = NewLogger(os.Stdout, InfoLevel, "", true)
}

func NewLogger(output io.Writer, level Level, prefix string, showCaller bool) *Logger {
	return &Logger{
		output:   output,
		level:    level,
		prefix:   prefix,
		showCall: showCaller,
	}
}

func Default() *Logger {
	once.Do(func() {
		if defaultLogger == nil {
			defaultLogger = NewLogger(os.Stdout, InfoLevel, "", true)
		}
	})
	return defaultLogger
}

func SetDefault(l *Logger) {
	defaultLogger = l
}

func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *Logger) GetLevel() Level {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

func (l *Logger) WithPrefix(prefix string) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	p := l.prefix
	if p != "" && prefix != "" {
		p = p + ":" + prefix
	} else if prefix != "" {
		p = prefix
	}
	return &Logger{
		output:   l.output,
		level:    l.level,
		prefix:   p,
		showCall: l.showCall,
	}
}

func (l *Logger) log(level Level, format string, args ...interface{}) {
	l.mu.RLock()
	if level < l.level {
		l.mu.RUnlock()
		return
	}
	output := l.output
	prefix := l.prefix
	showCall := l.showCall
	l.mu.RUnlock()

	var caller string
	if showCall {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			parts := strings.Split(file, "/")
			if len(parts) > 2 {
				file = strings.Join(parts[len(parts)-2:], "/")
			}
			caller = fmt.Sprintf(" %s:%d", file, line)
		}
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	levelName := levelNames[level]
	msg := fmt.Sprintf(format, args...)

	var buf strings.Builder
	buf.WriteString(timestamp)
	buf.WriteString(" [")
	buf.WriteString(levelName)
	buf.WriteString("]")
	if caller != "" {
		buf.WriteString(caller)
	}
	if prefix != "" {
		buf.WriteString(" [")
		buf.WriteString(prefix)
		buf.WriteString("]")
	}
	buf.WriteString(" ")
	buf.WriteString(msg)
	buf.WriteString("\n")

	l.mu.Lock()
	defer l.mu.Unlock()
	output.Write([]byte(buf.String()))
}

func (l *Logger) Debug(args ...interface{})                 { l.log(DebugLevel, "%s", fmt.Sprint(args...)) }
func (l *Logger) Debugf(format string, args ...interface{}) { l.log(DebugLevel, format, args...) }
func (l *Logger) Info(args ...interface{})                  { l.log(InfoLevel, "%s", fmt.Sprint(args...)) }
func (l *Logger) Infof(format string, args ...interface{})  { l.log(InfoLevel, format, args...) }
func (l *Logger) Warn(args ...interface{})                  { l.log(WarnLevel, "%s", fmt.Sprint(args...)) }
func (l *Logger) Warnf(format string, args ...interface{})   { l.log(WarnLevel, format, args...) }
func (l *Logger) Error(args ...interface{})                 { l.log(ErrorLevel, "%s", fmt.Sprint(args...)) }
func (l *Logger) Errorf(format string, args ...interface{}) { l.log(ErrorLevel, format, args...) }
func (l *Logger) Fatal(args ...interface{})                 { l.log(FatalLevel, "%s", fmt.Sprint(args...)); os.Exit(1) }
func (l *Logger) Fatalf(format string, args ...interface{}) { l.log(FatalLevel, format, args...); os.Exit(1) }

func Debug(args ...interface{})                 { Default().Debug(args...) }
func Debugf(format string, args ...interface{}) { Default().Debugf(format, args...) }
func Info(args ...interface{})                  { Default().Info(args...) }
func Infof(format string, args ...interface{})  { Default().Infof(format, args...) }
func Warn(args ...interface{})                  { Default().Warn(args...) }
func Warnf(format string, args ...interface{})   { Default().Warnf(format, args...) }
func Error(args ...interface{})                 { Default().Error(args...) }
func Errorf(format string, args ...interface{}) { Default().Errorf(format, args...) }
func Fatal(args ...interface{})                 { Default().Fatal(args...) }
func Fatalf(format string, args ...interface{}) { Default().Fatalf(format, args...) }
