package logger

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Logger writes structured key=value log lines to a configurable writer.
// When constructed with io.Discard, all formatting is skipped entirely —
// no allocations on the hot path.
type Logger struct {
	out     io.Writer
	enabled bool
}

// New returns a Logger that writes to out.
// If out is nil, os.Stdout is used.
func New(out io.Writer) *Logger {
	if out == nil {
		out = os.Stdout
	}
	return &Logger{
		out:     out,
		enabled: out != io.Discard,
	}
}

// Info logs an informational message with optional key=value pairs.
func (l *Logger) Info(msg string, kvs ...any) {
	if !l.enabled {
		return
	}
	l.write("INFO", msg, kvs...)
}

// Error logs an error message with optional key=value pairs.
func (l *Logger) Error(msg string, kvs ...any) {
	if !l.enabled {
		return
	}
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
