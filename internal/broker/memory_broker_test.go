package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-message-queue/pkg/queue"
)

// silentConfig returns a Config with logging suppressed.
// Use this in every test — we never want log noise in test output.
func silentConfig() Config {
	return Config{
		BufferSize: 16,
		MaxRetries: 3,
		LogOutput:  io.Discard,
	}
}

// waitFor polls condition every 5ms until it returns true or the deadline passes.
// Preferred over time.Sleep — tests finish faster on fast machines.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within", timeout)
}

// -----------------------------------------------------------------------------
// Happy path
// -----------------------------------------------------------------------------

func TestPublishSubscribe(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())
	defer b.Shutdown(context.Background()) //nolint:errcheck

	var received atomic.Int64

	err := b.Subscribe("test", func(ctx context.Context, msg queue.Message) error {
		if msg.Topic != "test" {
			t.Errorf("unexpected topic: got %q want %q", msg.Topic, "test")
		}
		if string(msg.Payload) != "hello" {
			t.Errorf("unexpected payload: got %q want %q", string(msg.Payload), "hello")
		}
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), "test", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitFor(t, time.Second, func() bool { return received.Load() == 1 })
}

// -----------------------------------------------------------------------------
// Fan-out
// -----------------------------------------------------------------------------

func TestFanOut(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())
	defer b.Shutdown(context.Background()) //nolint:errcheck

	const numHandlers = 5
	var counts [numHandlers]atomic.Int64

	for i := range numHandlers {
		idx := i
		if err := b.Subscribe("fan", func(ctx context.Context, msg queue.Message) error {
			counts[idx].Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
	}

	const numMessages = 10
	for range numMessages {
		if err := b.Publish(context.Background(), "fan", []byte("msg")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Every handler must receive every message.
	waitFor(t, 2*time.Second, func() bool {
		for i := range numHandlers {
			if counts[i].Load() != numMessages {
				return false
			}
		}
		return true
	})

	for i := range numHandlers {
		if got := counts[i].Load(); got != numMessages {
			t.Errorf("handler %d: got %d messages, want %d", i, got, numMessages)
		}
	}
}

// -----------------------------------------------------------------------------
// Retry
// -----------------------------------------------------------------------------

func TestRetryOnNack(t *testing.T) {
	t.Parallel()

	cfg := silentConfig()
	cfg.MaxRetries = 5
	b := NewMemoryBroker(cfg)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	var attempts atomic.Int64

	if err := b.Subscribe("retry", func(ctx context.Context, msg queue.Message) error {
		attempts.Add(1)
		if msg.Attempts < 4 {
			return errors.New("not ready")
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), "retry", []byte("work")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Should succeed on attempt 4 (attempts 0,1,2,3 fail, 4 succeeds = 5 calls).
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 5 })

	if got := attempts.Load(); got != 5 {
		t.Errorf("attempts: got %d, want 5", got)
	}
}

// -----------------------------------------------------------------------------
// Dead Letter Queue
// -----------------------------------------------------------------------------

func TestDeadLetter(t *testing.T) {
	t.Parallel()

	cfg := silentConfig()
	cfg.MaxRetries = 3
	b := NewMemoryBroker(cfg)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	var attempts atomic.Int64

	if err := b.Subscribe("dlq", func(ctx context.Context, msg queue.Message) error {
		attempts.Add(1)
		return errors.New("always fails")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), "dlq", []byte("doomed")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// MaxRetries=3: attempts 0,1,2 → exhausted → DLQ. Handler called 3 times.
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 3 })

	// Give the broker a moment to record DLQ stats.
	time.Sleep(20 * time.Millisecond)

	stats := b.Stats()
	if len(stats) == 0 {
		t.Fatal("expected stats for topic dlq")
	}

	s := stats[0]
	if s.DeadLettered != 1 {
		t.Errorf("dead_lettered: got %d, want 1", s.DeadLettered)
	}
	if s.DLQDepth != 1 {
		t.Errorf("dlq_depth: got %d, want 1", s.DLQDepth)
	}
	if s.Consumed != 0 {
		t.Errorf("consumed: got %d, want 0", s.Consumed)
	}
}

// -----------------------------------------------------------------------------
// Shutdown behaviour
// -----------------------------------------------------------------------------

func TestPublishAfterShutdown(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())

	shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := b.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	err := b.Publish(context.Background(), "test", []byte("late"))
	if err == nil {
		t.Fatal("expected error publishing to closed broker, got nil")
	}
}

func TestSubscribeAfterShutdown(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())

	shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := b.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	err := b.Subscribe("test", func(ctx context.Context, msg queue.Message) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error subscribing to closed broker, got nil")
	}
}

func TestShutdownDrainsInFlight(t *testing.T) {
	t.Parallel()

	cfg := silentConfig()
	cfg.BufferSize = 64
	b := NewMemoryBroker(cfg)

	const numMessages = 50
	var delivered atomic.Int64

	if err := b.Subscribe("drain", func(ctx context.Context, msg queue.Message) error {
		// Simulate brief processing time.
		time.Sleep(2 * time.Millisecond)
		delivered.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for i := range numMessages {
		payload := []byte(fmt.Sprintf("msg-%d", i))
		if err := b.Publish(context.Background(), "drain", payload); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := b.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if got := delivered.Load(); got != numMessages {
		t.Errorf("delivered: got %d, want %d", got, numMessages)
	}
}

func TestShutdownIdempotent(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())

	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := b.Shutdown(ctx); err != nil {
			cancel()
			t.Fatalf("Shutdown: %v", err)
		}
		cancel()
	}
}

// -----------------------------------------------------------------------------
// Topic isolation
// -----------------------------------------------------------------------------

func TestMultipleTopics(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())
	defer b.Shutdown(context.Background()) //nolint:errcheck

	var ordersCount, paymentsCount atomic.Int64

	if err := b.Subscribe("orders", func(ctx context.Context, msg queue.Message) error {
		ordersCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe orders: %v", err)
	}

	if err := b.Subscribe("payments", func(ctx context.Context, msg queue.Message) error {
		paymentsCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe payments: %v", err)
	}

	for range 5 {
		if err := b.Publish(context.Background(), "orders", []byte("order")); err != nil {
			t.Fatalf("Publish orders: %v", err)
		}
	}
	for range 3 {
		if err := b.Publish(context.Background(), "payments", []byte("payment")); err != nil {
			t.Fatalf("Publish payments: %v", err)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		return ordersCount.Load() == 5 && paymentsCount.Load() == 3
	})

	// Cross-contamination check — orders handler must not have seen payment messages.
	if got := ordersCount.Load(); got != 5 {
		t.Errorf("orders: got %d, want 5", got)
	}
	if got := paymentsCount.Load(); got != 3 {
		t.Errorf("payments: got %d, want 3", got)
	}
}

// -----------------------------------------------------------------------------
// Backpressure / context cancellation
// -----------------------------------------------------------------------------

func TestContextCancellationOnFullBuffer(t *testing.T) {
	t.Parallel()

	cfg := silentConfig()
	cfg.BufferSize = 2
	b := NewMemoryBroker(cfg)
	defer b.Shutdown(context.Background()) //nolint:errcheck

	// Subscribe a handler that blocks forever — nothing ever drains the channel.
	// Without this, the dispatch goroutine empties the buffer before we fill it.
	block := make(chan struct{})
	if err := b.Subscribe("full", func(ctx context.Context, msg queue.Message) error {
		<-block
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Fill the buffer to capacity.
	if err := b.Publish(context.Background(), "full", []byte("a")); err != nil {
		t.Fatalf("Publish a: %v", err)
	}
	if err := b.Publish(context.Background(), "full", []byte("b")); err != nil {
		t.Fatalf("Publish b: %v", err)
	}

	// Wait until the dispatcher has picked up the first message and is blocked
	// inside the handler — at that point exactly 1 slot remains, so we publish
	// one more to fill it completely.
	time.Sleep(20 * time.Millisecond)
	if err := b.Publish(context.Background(), "full", []byte("c")); err != nil {
		t.Fatalf("Publish c: %v", err)
	}

	// Now the buffer is full and the dispatcher is stuck. This publish must block
	// and return DeadlineExceeded when the context expires.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := b.Publish(ctx, "full", []byte("d"))
	if err == nil {
		t.Fatal("expected error on full buffer with cancelled context, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}

	// Unblock the handler so Shutdown can drain cleanly.
	close(block)
}

// -----------------------------------------------------------------------------
// Stats
// -----------------------------------------------------------------------------

func TestStats(t *testing.T) {
	t.Parallel()

	b := NewMemoryBroker(silentConfig())
	defer b.Shutdown(context.Background()) //nolint:errcheck

	var wg sync.WaitGroup
	wg.Add(1)

	if err := b.Subscribe("stats", func(ctx context.Context, msg queue.Message) error {
		defer wg.Done()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Publish(context.Background(), "stats", []byte("measure")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	wg.Wait()
	time.Sleep(10 * time.Millisecond) // let atomic writes settle

	stats := b.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 topic stat, got %d", len(stats))
	}

	s := stats[0]
	if s.Published != 1 {
		t.Errorf("published: got %d, want 1", s.Published)
	}
	if s.Consumed != 1 {
		t.Errorf("consumed: got %d, want 1", s.Consumed)
	}
	if s.DeadLettered != 0 {
		t.Errorf("dead_lettered: got %d, want 0", s.DeadLettered)
	}
}
