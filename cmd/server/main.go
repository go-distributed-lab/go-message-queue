package main

import (
	"context"
	"errors"
	"go-message-queue/internal/broker"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	b := broker.NewMemoryBroker(broker.Config{
		BufferSize: 256,
		MaxRetries: 3,
	})

	srv := newServer(b)

	httpServer := &http.Server{
		Addr:         ":8080",
		Handler:      srv,
		ReadTimeout:  35 * time.Second, // must exceed max long-poll timeout (30s)
		WriteTimeout: 35 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start serving in a goroutine so main can listen for signals.
	go func() {
		log.Printf("listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Block until SIGINT or SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutdown signal received")

	// Give HTTP connections 5s to finish, then shut the broker down.
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()

	if err := httpServer.Shutdown(httpCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	brokerCtx, brokerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer brokerCancel()

	if err := b.Shutdown(brokerCtx); err != nil {
		log.Printf("broker shutdown error: %v", err)
	}

	log.Println("shutdown complete")
}
