package broker

import (
	"context"
	"errors"
	"fmt"
	"go-message-queue/internal/logger"
	"go-message-queue/pkg/queue"
	"io"
	"sync"
)

const (
	defaultBufferSize = 256
	defaultMaxRetries = 3
)

// Config controls MemoryBroker behaviour.
// All fields are optional — zero values apply sensible defaults.
type Config struct {
	// BufferSize is the channel capacity for each topic.
	// Controls how many messages can queue up before Publish blocks.
	// Default: 256.
	BufferSize int

	// MaxRetries is the maximum number of delivery attempts per message
	// before it is moved to the dead letter queue.
	// Default: 3.
	MaxRetries int

	// LogOutput is where structured logs are written.
	// Pass io.Discard to silence all output (tests, benchmarks).
	// Default: os.Stdout.
	LogOutput io.Writer
}

func (c Config) withDefaults() Config {
	if c.BufferSize <= 0 {
		c.BufferSize = defaultBufferSize
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}
	return c
}

// MemoryBroker is the in-memory implementation of queue.Broker.
//
// Topics are created lazily on first Publish or Subscribe.
// Each topic gets exactly one dispatch goroutine that lives until Shutdown.
// All exported methods are safe for concurrent use.
type MemoryBroker struct {
	cfg    Config
	log    *logger.Logger
	topics map[string]*topicState
	mu     sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
}

// NewMemoryBroker constructs and returns a ready-to-use MemoryBroker.
func NewMemoryBroker(cfg Config) *MemoryBroker {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryBroker{
		cfg:    cfg,
		log:    logger.New(cfg.LogOutput),
		topics: make(map[string]*topicState),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Publish sends payload to the named topic.
// The topic is created automatically on first use.
// Blocks when the topic buffer is full. Returns an error if the broker
// is shut down or ctx is cancelled before the message is accepted.
func (b *MemoryBroker) Publish(ctx context.Context, topic string, payload []byte) error {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()

	if closed {
		return errors.New("broker: publish on closed broker")
	}

	t := b.getOrCreateTopic(topic)

	if err := t.enqueue(ctx, payload); err != nil {
		return fmt.Errorf("broker: publish to %q: %w", topic, err)
	}

	b.log.Info("published", "topic", topic, "payload_bytes", len(payload))
	return nil
}

// Subscribe registers handler as a consumer of the named topic.
// The topic is created automatically on first use.
// Every registered handler receives every message (fan-out).
// Returns an error if the broker is shut down.
func (b *MemoryBroker) Subscribe(topic string, handler queue.Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New("broker: subscribe on closed broker")
	}

	t := b.getOrCreateTopicLocked(topic)
	t.addHandler(handler)

	b.log.Info("subscribed", "topic", topic)
	return nil
}

// Shutdown gracefully stops the broker.
//
// It marks the broker closed, cancels the internal context, closes every
// topic channel, and waits for all dispatch goroutines to drain and exit.
// The provided ctx sets a deadline on how long to wait for drain.
func (b *MemoryBroker) Shutdown(ctx context.Context) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return nil
	}

	b.closed = true

	// Collect topics while holding the lock, then release before closing
	// channels — closing a channel while holding a lock that other goroutines
	// also acquire on the read path can cause unnecessary contention.
	topics := make([]*topicState, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}

	b.mu.Unlock()

	// Cancel the broker context — signals handlers to stop on context-aware work.
	b.cancel()

	// Close every topic channel. The dispatch goroutine for each topic will
	// drain remaining messages then exit the range loop naturally.
	for _, t := range topics {
		close(t.ch)
	}

	// Wait for all dispatch goroutines to finish, but respect the caller's deadline.
	drained := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		b.log.Info("broker shutdown complete")
		return nil
	case <-ctx.Done():
		b.log.Error("broker shutdown timed out", "err", ctx.Err())
		return ctx.Err()
	}
}

// Stats returns a point-in-time snapshot of metrics for every known topic.
// Safe to call at any time, including after Shutdown.
func (b *MemoryBroker) Stats() []TopicStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]TopicStats, 0, len(b.topics))
	for _, t := range b.topics {
		out = append(out, t.stats())
	}
	return out
}

// getOrCreateTopic is safe for concurrent callers with no lock held.
// Uses a double-checked locking pattern to avoid write-locking on every Publish.
func (b *MemoryBroker) getOrCreateTopic(name string) *topicState {
	b.mu.RLock()
	t, ok := b.topics[name]
	b.mu.RUnlock()
	if ok {
		return t
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	return b.getOrCreateTopicLocked(name)
}

// getOrCreateTopicLocked must be called with b.mu held for writing.
func (b *MemoryBroker) getOrCreateTopicLocked(name string) *topicState {
	if t, ok := b.topics[name]; ok {
		return t
	}
	t := newTopicState(name, b.cfg.BufferSize, b.cfg.MaxRetries)
	b.topics[name] = t
	t.dispatch(b.ctx, &b.wg)
	b.log.Info("topic created", "topic", name,
		"buffer_size", b.cfg.BufferSize,
		"max_retries", b.cfg.MaxRetries)
	return t
}
