package queue

import "context"

// Handler is the function signature all consumers must implement.
//
// Returning nil signals success — the message is acknowledged (ACK).
// Returning a non-nil error signals failure — the message is rejected (NACK)
// and will be retried by the broker up to its configured maximum.
type Handler func(ctx context.Context, msg Message) error

// Broker is the central interface for the message queue system.
// Producers call Publish. Consumers call Subscribe.
// The broker owns routing, buffering, retry, and DLQ logic.
type Broker interface {
	// Publish sends payload to the named topic.
	//
	// If no topic exists it is created automatically.
	// Blocks when the topic buffer is full (backpressure).
	// Returns an error if the broker is shut down or ctx is cancelled.
	Publish(ctx context.Context, topic string, payload []byte) error

	// Subscribe registers handler as a consumer of the named topic.
	//
	// Each message is delivered to every registered handler (fan-out).
	// Handlers are invoked in dedicated goroutines — never on the caller.
	// Returns an error if the broker is shut down.
	Subscribe(topic string, handler Handler) error

	// Shutdown gracefully stops the broker.
	//
	// It closes all topic channels and waits for every in-flight message
	// to finish delivery before returning. The provided ctx sets a deadline
	// on how long to wait. After Shutdown returns, Publish and Subscribe
	// will return errors.
	Shutdown(ctx context.Context) error
}
