package logger

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Logger writes structured key=value log lines to a configurable writer.
// Pass io.Discard in tests and benchmarks to suppress all output.
type Logger struct {
	out io.Writer
}

// New returns a Logger that writes to out.
// If out is nil, os.Stdout is used.
func New(out io.Writer) *Logger {
	if out == nil {
		out = os.Stdout
	}
	return &Logger{out: out}
}

// Info logs an informational message with optional key=value pairs.
// Pairs must be supplied as alternating key, value arguments.
func (l *Logger) Info(msg string, kvs ...any) {
	l.write("INFO", msg, kvs...)
}

// Error logs an error message with optional key=value pairs.
func (l *Logger) Error(msg string, kvs ...any) {
	l.write("ERROR", msg, kvs...)
}

func (l *Logger) write(level, msg string, kvs ...any) {
	line := fmt.Sprintf("time=%s level=%s msg=%q",
		time.Now().UTC().Format(time.RFC3339), level, msg)
	for i := 0; i+1 < len(kvs); i += 2 {
		line += fmt.Sprintf(" %v=%v", kvs[i], kvs[i+1])
	}
	fmt.Fprintln(l.out, line)
}
