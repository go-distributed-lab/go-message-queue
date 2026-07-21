package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-message-queue/internal/broker"
	"go-message-queue/pkg/queue"
	"net/http"
	"sync"
	"time"
)

// consumerQueue is a pre-subscribed channel for a single topic.
// The broker pushes every message here; HTTP consumers pull from it.
type consumerQueue struct {
	ch chan queue.Message
}

// server owns the broker, the HTTP mux, the consumer queues,
// and the pending ack map.
type server struct {
	broker    *broker.MemoryBroker
	mux       *http.ServeMux
	consumers map[string]*consumerQueue
	consMu    sync.RWMutex
	pending   map[string]queue.Message
	pendMu    sync.Mutex
}

func newServer(b *broker.MemoryBroker) *server {
	s := &server{
		broker:    b,
		mux:       http.NewServeMux(),
		consumers: make(map[string]*consumerQueue),
		pending:   make(map[string]queue.Message),
	}
	s.routes()
	return s
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *server) routes() {
	s.mux.HandleFunc("POST /publish", s.handlePublish)
	s.mux.HandleFunc("GET /consume", s.handleConsume)
	s.mux.HandleFunc("POST /ack", s.handleAck)
	s.mux.HandleFunc("POST /nack", s.handleNack)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /health", s.handleHealth)
}

// getOrCreateConsumer returns the consumerQueue for the given topic,
// creating and subscribing it to the broker on first use.
// This is called when a client first hits /consume?topic=X —
// from that point on the broker pushes every message on that topic
// into the queue's channel, ready for any future /consume call.
func (s *server) getOrCreateConsumer(topic string) (*consumerQueue, error) {
	s.consMu.RLock()
	cq, ok := s.consumers[topic]
	s.consMu.RUnlock()
	if ok {
		return cq, nil
	}

	s.consMu.Lock()
	defer s.consMu.Unlock()

	// Double-checked — another goroutine may have created it.
	if cq, ok = s.consumers[topic]; ok {
		return cq, nil
	}

	cq = &consumerQueue{ch: make(chan queue.Message, 256)}

	if err := s.broker.Subscribe(topic, func(ctx context.Context, msg queue.Message) error {
		select {
		case cq.ch <- msg:
			return nil
		default:
			// Consumer queue full — apply backpressure by returning an error.
			// The broker will retry the message per its MaxRetries config.
			return errors.New("consumer queue full")
		}
	}); err != nil {
		return nil, err
	}

	s.consumers[topic] = cq
	return cq, nil
}

// -----------------------------------------------------------------------------
// Request / response types
// -----------------------------------------------------------------------------

type publishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

type publishResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type consumeResponse struct {
	ID       string `json:"id"`
	Topic    string `json:"topic"`
	Payload  string `json:"payload"`
	Attempts int    `json:"attempts"`
}

type ackRequest struct {
	ID string `json:"id"`
}

type metricsResponse struct {
	Topics []topicMetrics `json:"topics"`
}

type topicMetrics struct {
	Topic        string `json:"topic"`
	QueueDepth   int64  `json:"queue_depth"`
	Published    int64  `json:"published"`
	Consumed     int64  `json:"consumed"`
	Retried      int64  `json:"retried"`
	DeadLettered int64  `json:"dead_lettered"`
	DLQDepth     int64  `json:"dlq_depth"`
}

type healthResponse struct {
	Status string `json:"status"`
}

// -----------------------------------------------------------------------------
// POST /publish
// -----------------------------------------------------------------------------

func (s *server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Topic == "" {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "topic is required"})
		return
	}
	if req.Payload == "" {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "payload is required"})
		return
	}

	if err := s.broker.Publish(r.Context(), req.Topic, []byte(req.Payload)); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, publishResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, publishResponse{OK: true})
}

// -----------------------------------------------------------------------------
// GET /consume?topic=orders&timeout=5s
// -----------------------------------------------------------------------------
//
// Long-polls until a message is available or timeout expires.
// Default timeout: 10s. Max: 30s.
// On first call for a topic, registers a permanent consumer with the broker.
// All subsequent publishes to that topic are buffered and available here.

func (s *server) handleConsume(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "topic query param is required"})
		return
	}

	timeoutStr := r.URL.Query().Get("timeout")
	timeout := 10 * time.Second
	if timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			if d > 30*time.Second {
				d = 30 * time.Second
			}
			timeout = d
		}
	}

	cq, err := s.getOrCreateConsumer(topic)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, publishResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	select {
	case msg := <-cq.ch:
		s.pendMu.Lock()
		s.pending[msg.ID] = msg
		s.pendMu.Unlock()

		writeJSON(w, http.StatusOK, consumeResponse{
			ID:       msg.ID,
			Topic:    msg.Topic,
			Payload:  string(msg.Payload),
			Attempts: msg.Attempts,
		})

	case <-ctx.Done():
		w.WriteHeader(http.StatusNoContent)
	}
}

// -----------------------------------------------------------------------------
// POST /ack
// -----------------------------------------------------------------------------

func (s *server) handleAck(w http.ResponseWriter, r *http.Request) {
	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.ID == "" {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "id is required"})
		return
	}

	s.pendMu.Lock()
	_, ok := s.pending[req.ID]
	if ok {
		delete(s.pending, req.ID)
	}
	s.pendMu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, publishResponse{Error: fmt.Sprintf("message %q not in pending", req.ID)})
		return
	}

	writeJSON(w, http.StatusOK, publishResponse{OK: true})
}

// -----------------------------------------------------------------------------
// POST /nack
// -----------------------------------------------------------------------------

func (s *server) handleNack(w http.ResponseWriter, r *http.Request) {
	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.ID == "" {
		writeJSON(w, http.StatusBadRequest, publishResponse{Error: "id is required"})
		return
	}

	s.pendMu.Lock()
	msg, ok := s.pending[req.ID]
	if ok {
		delete(s.pending, req.ID)
	}
	s.pendMu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, publishResponse{Error: fmt.Sprintf("message %q not in pending", req.ID)})
		return
	}

	msg.Attempts++
	if err := s.broker.Publish(r.Context(), msg.Topic, msg.Payload); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, publishResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, publishResponse{OK: true})
}

// -----------------------------------------------------------------------------
// GET /metrics
// -----------------------------------------------------------------------------

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.Stats()
	resp := metricsResponse{
		Topics: make([]topicMetrics, 0, len(stats)),
	}
	for _, st := range stats {
		resp.Topics = append(resp.Topics, topicMetrics{
			Topic:        st.Topic,
			QueueDepth:   st.QueueDepth,
			Published:    st.Published,
			Consumed:     st.Consumed,
			Retried:      st.Retried,
			DeadLettered: st.DeadLettered,
			DLQDepth:     st.DLQDepth,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// -----------------------------------------------------------------------------
// GET /health
// -----------------------------------------------------------------------------

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
