package benchmarks

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go-message-queue/pkg/queue"
)

// BenchmarkConsume measures end-to-end message throughput:
// publish N messages, wait for all N to be delivered to the handler.
// This is the truest measure of broker throughput under real load.
func BenchmarkConsume(b *testing.B) {
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
		b.Fatalf("timeout: only consumed %d/%d messages", consumed.Load(), target)
	}
}

// BenchmarkConsumeParallel measures consume throughput with multiple
// concurrent publishers feeding a single topic.
func BenchmarkConsumeParallel(b *testing.B) {
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
		b.Fatalf("timeout: only consumed %d/%d", consumed.Load(), target)
	}
}

// BenchmarkFanOut measures the cost of fan-out as subscriber count grows.
// Each subscriber receives every message — total deliveries = N * subscribers.
func BenchmarkFanOut(b *testing.B) {
	for _, numSubs := range []int{1, 2, 4, 8, 16} {
		b.Run(
			fmt.Sprintf("%02d_subscribers", numSubs),
			func(b *testing.B) {
				br := newBenchBroker(b.N + 64)
				defer br.Shutdown(context.Background()) //nolint:errcheck

				var consumed atomic.Int64
				done := make(chan struct{})
				target := int64(b.N * numSubs)

				for range numSubs {
					if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
						if consumed.Add(1) == target {
							close(done)
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
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
					b.Fatalf("timeout: consumed %d/%d", consumed.Load(), target)
				}
			},
		)
	}
}
