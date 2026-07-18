# go-message-queue — Architecture

## Overview

`go-message-queue` is a pure in-memory message queue written in stdlib Go.
It follows the same principles as the rest of the go-distributed-lab series:
zero external dependencies, interface-driven design, and channel-close-driven
shutdown.

## Core Concepts

| Concept      | Description                                                  |
|--------------|--------------------------------------------------------------|
| **Message**  | Immutable unit of data: ID, Topic, Payload, Attempts, Time   |
| **Handler**  | Consumer callback: `func(ctx, msg) error`                    |
| **Broker**   | Central router — owns topics, delivery, retry, DLQ           |
| **Topic**    | Named queue: buffered channel + subscriber list + DLQ        |
| **DLQ**      | Dead Letter Queue — messages that exhausted all retries      |

## Data Flow

```
Producer
   │
   │  Publish(ctx, topic, payload)
   ▼
┌──────────────────────────────────────────────┐
│                 MemoryBroker                 │
│                                              │
│  topics: map[string]*topicState              │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │            topicState                  │  │
│  │                                        │  │
│  │  ch      chan Message  ← buffered      │  │
│  │  dlq     []Message     ← overflow     │  │
│  │  handlers []Handler    ← fan-out      │  │
│  │  metrics  atomic.*     ← counters     │  │
│  └──────────────┬─────────────────────────┘  │
└─────────────────┼────────────────────────────┘
                  │
                  │  dispatch goroutine (per topic)
                  │  reads ch until closed
                  ▼
          ┌───────────────┐
          │   Handler(s)  │  ← fan-out: every subscriber gets every message
          └───────┬───────┘
                  │
          ┌───────┴────────┐
          │                │
         nil             error
          │                │
        ACK           Attempts++
       (done)              │
                    ┌──────┴──────┐
                    │             │
               attempts       attempts
               < maxRetries   ≥ maxRetries
                    │             │
                  retry        → DLQ
```

## Retry & DLQ

- Every handler return is evaluated immediately after delivery.
- On `error`: `msg.Attempts` is incremented and delivery is retried in-place.
- When `Attempts >= MaxRetries`: the message is appended to the topic's DLQ
  and the handler is not called again for that message.
- The DLQ is inspectable via `MemoryBroker.Stats()`.

## Shutdown Sequence

1. Caller invokes `Broker.Shutdown(ctx)`.
2. Broker sets `closed = true` under write lock — rejects new Publish/Subscribe.
3. All topic channels are closed (signals dispatch goroutines to drain and exit).
4. Broker waits on `sync.WaitGroup` for all dispatch goroutines to finish.
5. Returns nil on clean drain, `ctx.Err()` if the deadline is exceeded.

```
Shutdown(ctx)
   │
   ├─ closed = true
   ├─ close(topic.ch) for each topic
   ├─ wg.Wait()  ◄── dispatch goroutines drain, then exit range loop
   └─ return
```

## Concurrency Model

| Component         | Synchronisation          |
|-------------------|--------------------------|
| topics map        | `sync.RWMutex`           |
| handler slice     | `sync.RWMutex`           |
| DLQ slice         | `sync.Mutex`             |
| metrics counters  | `sync/atomic`            |
| shutdown flag     | write lock on broker `mu`|

## Metrics (per topic)

| Metric         | Type            | Description                         |
|----------------|-----------------|-------------------------------------|
| `published`    | `atomic.Int64`  | Messages accepted by the broker     |
| `consumed`     | `atomic.Int64`  | Messages successfully ACKed         |
| `retried`      | `atomic.Int64`  | Delivery attempts after first try   |
| `deadLettered` | `atomic.Int64`  | Messages moved to DLQ               |
| `queueDepth`   | `len(ch)`       | Current messages waiting in buffer  |
| `dlqDepth`     | `len(dlq)`      | Current messages in DLQ             |

## Package Layout

```
pkg/queue/
    message.go      — Message struct, Handler type
    broker.go       — Broker interface

internal/broker/
    memory_broker.go — MemoryBroker (implements Broker)
    topic.go         — topicState, dispatch loop, retry, DLQ, metrics

internal/logger/
    logger.go        — structured key=value logger (io.Writer injection)

cmd/server/
    main.go          — HTTP API: /publish /consume /ack /metrics /health
```

## HTTP API (Day 4)

| Method | Path       | Description                            |
|--------|------------|----------------------------------------|
| POST   | /publish   | Publish a message to a topic           |
| GET    | /consume   | Long-poll: consume one message         |
| POST   | /ack       | Acknowledge a pending message          |
| GET    | /metrics   | Per-topic stats snapshot               |
| GET    | /health    | Returns `{"status":"ok"}`              |

## Design Decisions

**Why fan-out and not consumer groups on Day 1?**  
Fan-out (every subscriber receives every message) is the simpler, more
predictable default. Consumer groups (competing consumers — each message
delivered to exactly one) will be added as an opt-in mode in a later day.

**Why block on full buffer instead of dropping?**  
Dropping silently is dangerous in a queue — callers lose data without knowing.
Blocking gives the producer honest backpressure. A non-blocking `TryPublish`
that returns an error is planned for Day 4.

**Why retry in-place (no re-queue)?**  
Re-queuing to the tail of the channel introduces ordering problems for other
consumers. In-place retry keeps delivery deterministic and avoids starvation
of other messages. A configurable backoff is planned for a later day.