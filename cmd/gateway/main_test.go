package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type listenerFunc func(context.Context) error

func (f listenerFunc) Start(ctx context.Context) error { return f(ctx) }

func TestServeListenersJoinsShutdownErrors(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var (
		bindErr = errors.New("listener bind failed")
		stopErr = errors.New("listener shutdown failed")
	)
	joined := make(chan struct{})
	listeners := []listener{
		listenerFunc(func(context.Context) error { return bindErr }),
		listenerFunc(func(ctx context.Context) error {
			<-ctx.Done()
			close(joined)
			return stopErr
		}),
		listenerFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}),
	}
	finished := make(chan error, 1)
	go func() { finished <- serveListeners(ctx, stop, listeners) }()
	select {
	case err := <-finished:
		if !errors.Is(err, bindErr) || !errors.Is(err, stopErr) {
			t.Fatalf("listener errors were not joined: %v", err)
		}
		select {
		case <-joined:
		default:
			t.Fatal("returned before the other listener finished")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener failure did not stop and join its siblings")
	}
}

func TestServeListenersRejectsEmptySet(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	if err := serveListeners(ctx, stop, nil); err == nil {
		t.Fatal("missing listeners must fail before startup")
	}
}
