package broker

import (
	"context"
	"fmt"
	"go-message-queue/pkg/queue"
	"sync"
	"sync/atomic"
	"time"
)

// topicState owns everything for a single named topic:
// the message channel, the dead letter queue, all registered handlers,
// and the atomic metric counters.
type topicState struct {
	name       string
	ch         chan queue.Message
	maxRetries int

	dlq   []queue.Message
	dlqMu sync.Mutex

	handlers   []queue.Handler
	handlersMu sync.RWMutex

	// Atomic counters — updated on the hot path, never under a lock.
	published    atomic.Int64
	consumed     atomic.Int64
	retried      atomic.Int64
	deadLettered atomic.Int64
}

func newTopicState(name string, bufferSize, maxRetries int) *topicState {
	return &topicState{
		name:       name,
		ch:         make(chan queue.Message, bufferSize),
		maxRetries: maxRetries,
	}
}

// addHandler appends a new subscriber. Called before dispatch starts,
// or safely at any time under handlersMu.
func (t *topicState) addHandler(h queue.Handler) {
	t.handlersMu.Lock()
	defer t.handlersMu.Unlock()
	t.handlers = append(t.handlers, h)
}

// enqueue builds a Message from payload and sends it to ch.
// Blocks when the buffer is full (backpressure). Respects ctx cancellation.
func (t *topicState) enqueue(ctx context.Context, payload []byte) error {
	msg := queue.Message{
		ID:        fmt.Sprintf("%s-%d", t.name, time.Now().UnixNano()),
		Topic:     t.name,
		Payload:   payload,
		Attempts:  0,
		Timestamp: time.Now(),
	}
	select {
	case t.ch <- msg:
		t.published.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dispatch starts the single goroutine that reads from ch and fans the
// message out to every registered handler. It exits cleanly when ch is
// closed (Shutdown path). wg is decremented on exit so the broker can
// wait for full drain.
func (t *topicState) dispatch(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range t.ch {
			// Snapshot handlers under read lock — new subscribers added
			// after this point will not receive this particular message.
			t.handlersMu.RLock()
			handlers := make([]queue.Handler, len(t.handlers))
			copy(handlers, t.handlers)
			t.handlersMu.RUnlock()

			for _, h := range handlers {
				t.deliver(ctx, msg, h)
			}
		}
	}()
}

// deliver calls h with msg, retrying on error until either the handler
// succeeds or Attempts reaches maxRetries, at which point the message
// is moved to the DLQ.
func (t *topicState) deliver(ctx context.Context, msg queue.Message, h queue.Handler) {
	for {
		if err := h(ctx, msg); err == nil {
			t.consumed.Add(1)
			return
		}

		msg.Attempts++

		if msg.Attempts >= t.maxRetries {
			t.deadLettered.Add(1)
			t.dlqMu.Lock()
			t.dlq = append(t.dlq, msg)
			t.dlqMu.Unlock()
			return
		}

		t.retried.Add(1)
	}
}

// stats returns a consistent point-in-time snapshot of this topic's metrics.
// Atomic loads are not coordinated with each other — this is intentional.
// The values are always directionally correct for observability purposes.
func (t *topicState) stats() TopicStats {
	t.dlqMu.Lock()
	dlqDepth := int64(len(t.dlq))
	t.dlqMu.Unlock()

	return TopicStats{
		Topic:        t.name,
		QueueDepth:   int64(len(t.ch)),
		Published:    t.published.Load(),
		Consumed:     t.consumed.Load(),
		Retried:      t.retried.Load(),
		DeadLettered: t.deadLettered.Load(),
		DLQDepth:     dlqDepth,
	}
}

// TopicStats is an immutable snapshot of a topic's metrics at a point in time.
type TopicStats struct {
	Topic        string
	QueueDepth   int64
	Published    int64
	Consumed     int64
	Retried      int64
	DeadLettered int64
	DLQDepth     int64
}
