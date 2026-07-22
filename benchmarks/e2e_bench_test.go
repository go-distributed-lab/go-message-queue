package benchmarks

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go-message-queue/pkg/queue"
)

// BenchmarkEndToEnd measures the full round-trip latency of a single message:
// Publish → channel enqueue → dispatch goroutine → handler.
// Uses b.N=1 per iteration to isolate per-message cost.
func BenchmarkEndToEnd(b *testing.B) {
	br := newBenchBroker(256)
	defer br.Shutdown(context.Background()) //nolint:errcheck

	delivered := make(chan struct{}, 1)

	if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
		delivered <- struct{}{}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, 256)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if err := br.Publish(ctx, "bench", payload); err != nil {
			b.Fatal(err)
		}
		select {
		case <-delivered:
		case <-time.After(5 * time.Second):
			b.Fatal("timeout waiting for delivery")
		}
	}
}

// BenchmarkRetryOverhead measures the cost of the retry path
// versus the happy path — quantifies how much retries hurt throughput.
func BenchmarkRetryOverhead(b *testing.B) {
	b.Run("happy_path", func(b *testing.B) {
		br := newBenchBroker(b.N + 64)
		defer br.Shutdown(context.Background()) //nolint:errcheck

		var consumed atomic.Int64
		done := make(chan struct{})
		target := int64(b.N)

		if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
			if consumed.Add(1) == target {
				close(done)
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}

		payload := make([]byte, 256)
		ctx := context.Background()
		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			if err := br.Publish(ctx, "bench", payload); err != nil {
				b.Fatal(err)
			}
		}

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			b.Fatal("timeout")
		}
	})

	b.Run("one_retry", func(b *testing.B) {
		br := newBenchBroker(b.N + 64)
		defer br.Shutdown(context.Background()) //nolint:errcheck

		var consumed atomic.Int64
		done := make(chan struct{})
		target := int64(b.N)

		if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
			// Fail once, succeed on retry.
			if msg.Attempts == 0 {
				return errBenchRetry
			}
			if consumed.Add(1) == target {
				close(done)
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}

		payload := make([]byte, 256)
		ctx := context.Background()
		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			if err := br.Publish(ctx, "bench", payload); err != nil {
				b.Fatal(err)
			}
		}

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			b.Fatal("timeout")
		}
	})

	b.Run("always_dlq", func(b *testing.B) {
		br := newBenchBroker(b.N + 64)
		defer br.Shutdown(context.Background()) //nolint:errcheck

		var deadLettered atomic.Int64
		done := make(chan struct{})
		target := int64(b.N)

		if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
			// Always fail — every message goes to DLQ after maxRetries.
			if msg.Attempts >= 2 {
				if deadLettered.Add(1) == target {
					close(done)
				}
			}
			return errBenchRetry
		}); err != nil {
			b.Fatal(err)
		}

		payload := make([]byte, 256)
		ctx := context.Background()
		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			if err := br.Publish(ctx, "bench", payload); err != nil {
				b.Fatal(err)
			}
		}

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			b.Fatal("timeout")
		}
	})
}

// BenchmarkScalability measures broker throughput as producer count scales.
// Answers: does throughput grow linearly with more producers?
func BenchmarkScalability(b *testing.B) {
	for _, numProducers := range []int{1, 2, 4, 8, 12} {
		b.Run(
			fmt.Sprintf("producers_%02d", numProducers),
			func(b *testing.B) {
				br := newBenchBroker(b.N + 64)
				defer br.Shutdown(context.Background()) //nolint:errcheck

				var consumed atomic.Int64
				done := make(chan struct{})
				target := int64(b.N)

				if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
					if consumed.Add(1) == target {
						close(done)
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}

				payload := make([]byte, 256)
				ctx := context.Background()

				b.ResetTimer()
				b.ReportAllocs()

				b.SetParallelism(numProducers)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := br.Publish(ctx, "bench", payload); err != nil {
							b.Errorf("publish: %v", err)
						}
					}
				})

				select {
				case <-done:
				case <-time.After(30 * time.Second):
					b.Fatalf("timeout: consumed %d/%d", consumed.Load(), target)
				}
			},
		)
	}
}

// sentinel error used across retry benchmarks.
var errBenchRetry = &benchError{"bench retry"}

type benchError struct{ msg string }

func (e *benchError) Error() string { return e.msg }
