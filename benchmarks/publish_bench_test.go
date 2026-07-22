package benchmarks

import (
	"context"
	"io"
	"testing"

	"go-message-queue/internal/broker"
	"go-message-queue/pkg/queue"
)

// payload sizes we benchmark across — small, medium, large.
var payloads = map[string][]byte{
	"small_64B":  make([]byte, 64),
	"medium_1KB": make([]byte, 1024),
	"large_64KB": make([]byte, 65536),
}

func newBenchBroker(bufferSize int) *broker.MemoryBroker {
	return broker.NewMemoryBroker(broker.Config{
		BufferSize: bufferSize,
		MaxRetries: 3,
		LogOutput:  io.Discard,
	})
}

// BenchmarkPublish measures raw publish throughput into a buffered topic
// with no consumer — pure enqueue cost.
func BenchmarkPublish(b *testing.B) {
	for name, payload := range payloads {
		b.Run(name, func(b *testing.B) {
			br := newBenchBroker(b.N + 64)
			defer br.Shutdown(context.Background()) //nolint:errcheck

			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()

			for i := range b.N {
				if err := br.Publish(ctx, "bench", payload); err != nil {
					b.Fatalf("iteration %d: %v", i, err)
				}
			}
		})
	}
}

// BenchmarkPublishParallel measures publish throughput under concurrent producers.
// RunParallel spins up GOMAXPROCS goroutines each calling Publish in a tight loop.
func BenchmarkPublishParallel(b *testing.B) {
	for name, payload := range payloads {
		b.Run(name, func(b *testing.B) {
			br := newBenchBroker(b.N + 64)
			defer br.Shutdown(context.Background()) //nolint:errcheck

			// Subscribe a no-op handler so the channel never fills.
			if err := br.Subscribe("bench", func(ctx context.Context, msg queue.Message) error {
				return nil
			}); err != nil {
				b.Fatal(err)
			}

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
		})
	}
}

// BenchmarkPublishMultiTopic measures publish cost spread across N topics.
func BenchmarkPublishMultiTopic(b *testing.B) {
	topics := []string{"orders", "payments", "inventory", "shipping", "notifications"}

	br := newBenchBroker(b.N + 64)
	defer br.Shutdown(context.Background()) //nolint:errcheck

	// Subscribe no-op handlers so channels never fill.
	for _, t := range topics {
		topic := t
		if err := br.Subscribe(topic, func(ctx context.Context, msg queue.Message) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}

	payload := make([]byte, 256)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		topic := topics[i%len(topics)]
		if err := br.Publish(ctx, topic, payload); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}
