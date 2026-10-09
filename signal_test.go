package events_test

import (
	"context"
	"os"
	"testing"

	"go.osspkg.com/events"
)

func TestOnStopSignalCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	events.OnStopSignal(ctx, func() {
		called = true
	})

	if called {
		t.Fatal("OnStopSignal called callback after context cancellation")
	}
}

func TestOnHubSignalCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	events.OnHubSignal(ctx, func() {
		called = true
	})

	if called {
		t.Fatal("OnHubSignal called callback after context cancellation")
	}
}

func TestOnCustomSignalCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	events.OnCustomSignal(ctx, func() {
		called = true
	}, os.Interrupt)

	if called {
		t.Fatal("OnCustomSignal called callback after context cancellation")
	}
}

func TestOnCustomSignalWithoutSignals(t *testing.T) {
	called := false

	events.OnCustomSignal(context.Background(), func() {
		called = true
	})

	if called {
		t.Fatal("OnCustomSignal called callback without a signal")
	}
}
