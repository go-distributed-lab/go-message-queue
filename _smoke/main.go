package main

import (
	"context"
	"fmt"
	"go-message-queue/internal/broker"
	"go-message-queue/pkg/queue"
	"io"
	"log"
	"time"
)

func main() {
	b := broker.NewMemoryBroker(broker.Config{
		BufferSize: 8,
		MaxRetries: 3,
		LogOutput:  io.Discard, // silence broker logs for clean output
	})

	// --- happy path ---
	if err := b.Subscribe("orders", func(ctx context.Context, msg queue.Message) error {
		fmt.Printf("[ACK]  id=%s payload=%s attempts=%d\n", msg.ID, msg.Payload, msg.Attempts)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// --- retry + DLQ path ---
	if err := b.Subscribe("orders", func(ctx context.Context, msg queue.Message) error {
		if msg.Attempts < 2 {
			fmt.Printf("[NACK] id=%s attempts=%d (will retry)\n", msg.ID, msg.Attempts)
			return fmt.Errorf("not ready")
		}
		fmt.Printf("[ACK]  id=%s attempts=%d (recovered)\n", msg.ID, msg.Attempts)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if err := b.Publish(ctx, "orders", []byte(`{"id":1,"item":"widget"}`)); err != nil {
		log.Fatal(err)
	}
	if err := b.Publish(ctx, "orders", []byte(`{"id":2,"item":"gadget"}`)); err != nil {
		log.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	stats := b.Stats()
	for _, s := range stats {
		fmt.Printf("\n[STATS] topic=%s published=%d consumed=%d retried=%d dead_lettered=%d queue_depth=%d dlq_depth=%d\n",
			s.Topic, s.Published, s.Consumed, s.Retried, s.DeadLettered, s.QueueDepth, s.DLQDepth)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := b.Shutdown(shutCtx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nDay 2 smoke test OK")
}
