package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"go-message-queue/internal/broker"
	"go-message-queue/pkg/queue"
)

// waitFor polls condition every 5ms until met or timeout.
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

// TestFullFlow exercises the complete lifecycle:
// create broker → subscribe → publish → retry → DLQ → stats → shutdown.
func TestFullFlow(t *testing.T) {
	b := broker.NewMemoryBroker(broker.Config{
		BufferSize: 32,
		MaxRetries: 3,
		LogOutput:  io.Discard,
	})

	var goodCount atomic.Int64
	var badAttempts atomic.Int64

	// Handler 1: always succeeds.
	if err := b.Subscribe("events", func(ctx context.Context, msg queue.Message) error {
		goodCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe good: %v", err)
	}

	// Handler 2: always fails — exhausts retries → DLQ.
	if err := b.Subscribe("events", func(ctx context.Context, msg queue.Message) error {
		badAttempts.Add(1)
		return errors.New("permanent failure")
	}); err != nil {
		t.Fatalf("Subscribe bad: %v", err)
	}

	const numMessages = 10
	ctx := context.Background()

	for i := range numMessages {
		payload := []byte(fmt.Sprintf(`{"seq":%d}`, i))
		if err := b.Publish(ctx, "events", payload); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Good handler: 10 messages × 1 attempt = 10
	// Bad handler:  10 messages × 3 attempts (MaxRetries) = 30
	waitFor(t, 5*time.Second, func() bool {
		return goodCount.Load() == numMessages && badAttempts.Load() == int64(numMessages*3)
	})

	time.Sleep(20 * time.Millisecond) // let DLQ writes settle

	stats := b.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(stats))
	}

	s := stats[0]

	if s.Published != numMessages {
		t.Errorf("published: got %d, want %d", s.Published, numMessages)
	}
	if s.Consumed != numMessages {
		t.Errorf("consumed: got %d, want %d", s.Consumed, numMessages)
	}
	if s.DeadLettered != numMessages {
		t.Errorf("dead_lettered: got %d, want %d", s.DeadLettered, numMessages)
	}
	if s.DLQDepth != numMessages {
		t.Errorf("dlq_depth: got %d, want %d", s.DLQDepth, numMessages)
	}
	if s.Retried != int64(numMessages*2) {
		// Each of 10 messages retried twice before the 3rd attempt is counted as dead-lettered.
		t.Errorf("retried: got %d, want %d", s.Retried, numMessages*2)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := b.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestConcurrentProducers verifies the broker handles concurrent publishers safely.
func TestConcurrentProducers(t *testing.T) {
	b := broker.NewMemoryBroker(broker.Config{
		BufferSize: 512,
		MaxRetries: 1,
		LogOutput:  io.Discard,
	})

	var received atomic.Int64

	if err := b.Subscribe("concurrent", func(ctx context.Context, msg queue.Message) error {
		received.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const (
		numProducers = 10
		msgsEach     = 20
	)

	var wg = make(chan struct{}, numProducers)
	ctx := context.Background()

	for range numProducers {
		go func() {
			for range msgsEach {
				if err := b.Publish(ctx, "concurrent", []byte("x")); err != nil {
					t.Errorf("Publish: %v", err)
				}
			}
			wg <- struct{}{}
		}()
	}

	for range numProducers {
		<-wg
	}

	waitFor(t, 5*time.Second, func() bool {
		return received.Load() == int64(numProducers*msgsEach)
	})

	if got := received.Load(); got != int64(numProducers*msgsEach) {
		t.Errorf("received: got %d, want %d", got, numProducers*msgsEach)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := b.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
