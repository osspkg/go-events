package bus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.osspkg.com/events/bus"
)

type testEvent struct {
	id int
}

func newTestBus(t *testing.T, capacity int) *bus.Bus[testEvent] {
	t.Helper()
	b, err := bus.New[testEvent](capacity)
	if err != nil {
		t.Fatalf("New(%d): %v", capacity, err)
	}
	return b
}

func closeTestBus(t *testing.T, b *bus.Bus[testEvent]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for value")
		var zero T
		return zero
	}
}

func TestNewRejectsInvalidCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		if _, err := bus.New[testEvent](capacity); !errors.Is(err, bus.ErrInvalidQueueCapacity) {
			t.Errorf("New(%d) error = %v, want ErrInvalidQueueCapacity", capacity, err)
		}
	}
}

func TestSubscribeRequiresHandlers(t *testing.T) {
	b := newTestBus(t, 1)
	t.Cleanup(func() { closeTestBus(t, b) })

	if _, err := b.Subscribe(nil, func(error) {}); !errors.Is(err, bus.ErrNilHandler) {
		t.Errorf("Subscribe(nil, onError) error = %v, want ErrNilHandler", err)
	}
	if _, err := b.Subscribe(func(context.Context, testEvent) error { return nil }, nil); !errors.Is(err, bus.ErrNilErrorHandler) {
		t.Errorf("Subscribe(handler, nil) error = %v, want ErrNilErrorHandler", err)
	}
}

func TestPublishWithoutSubscribers(t *testing.T) {
	b := newTestBus(t, 1)
	t.Cleanup(func() { closeTestBus(t, b) })

	if err := b.Publish(context.Background(), testEvent{id: 1}); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
}

func TestPublishFansOutInFIFOOrder(t *testing.T) {
	b := newTestBus(t, 3)
	t.Cleanup(func() { closeTestBus(t, b) })

	first := make(chan int, 3)
	second := make(chan int, 3)
	for _, output := range []chan int{first, second} {
		out := output
		if _, err := b.Subscribe(func(_ context.Context, event testEvent) error {
			out <- event.id
			return nil
		}, func(error) {}); err != nil {
			t.Fatal(err)
		}
	}

	for id := 1; id <= 3; id++ {
		if err := b.Publish(context.Background(), testEvent{id: id}); err != nil {
			t.Fatalf("Publish(%d): %v", id, err)
		}
	}

	for id := 1; id <= 3; id++ {
		if got := receive(t, first); got != id {
			t.Errorf("first subscriber event = %d, want %d", got, id)
		}
		if got := receive(t, second); got != id {
			t.Errorf("second subscriber event = %d, want %d", got, id)
		}
	}
}

func TestPublishBackpressureAndPartialDeliveryOnCancellation(t *testing.T) {
	b := newTestBus(t, 1)
	t.Cleanup(func() { closeTestBus(t, b) })

	fastEvents := make(chan int, 4)
	if _, err := b.Subscribe(func(_ context.Context, event testEvent) error {
		fastEvents <- event.id
		return nil
	}, func(error) {}); err != nil {
		t.Fatal(err)
	}

	slowStarted := make(chan struct{})
	slowRelease := make(chan struct{})
	slowEvents := make(chan int, 4)
	if _, err := b.Subscribe(func(_ context.Context, event testEvent) error {
		if event.id == 1 {
			close(slowStarted)
			<-slowRelease
		}
		slowEvents <- event.id
		return nil
	}, func(error) {}); err != nil {
		t.Fatal(err)
	}

	if err := b.Publish(context.Background(), testEvent{id: 1}); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, fastEvents); got != 1 {
		t.Fatalf("fast subscriber event = %d, want 1", got)
	}
	receive(t, slowStarted)

	if err := b.Publish(context.Background(), testEvent{id: 2}); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, fastEvents); got != 2 {
		t.Fatalf("fast subscriber event = %d, want 2", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	publishResult := make(chan error, 1)
	go func() {
		publishResult <- b.Publish(ctx, testEvent{id: 3})
	}()
	if got := receive(t, fastEvents); got != 3 {
		t.Fatalf("fast subscriber event = %d, want partial delivery of 3", got)
	}
	cancel()
	if err := receive(t, publishResult); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context.Canceled", err)
	}

	close(slowRelease)
	if got := receive(t, slowEvents); got != 1 {
		t.Fatalf("slow subscriber event = %d, want 1", got)
	}
	if got := receive(t, slowEvents); got != 2 {
		t.Fatalf("slow subscriber event = %d, want 2", got)
	}
}

func TestUnsubscribeDrainsQueue(t *testing.T) {
	b := newTestBus(t, 2)
	t.Cleanup(func() { closeTestBus(t, b) })

	started := make(chan struct{})
	release := make(chan struct{})
	events := make(chan int, 3)
	sub, err := b.Subscribe(func(_ context.Context, event testEvent) error {
		if event.id == 1 {
			close(started)
			<-release
		}
		events <- event.id
		return nil
	}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}

	for id := 1; id <= 3; id++ {
		if err := b.Publish(context.Background(), testEvent{id: id}); err != nil {
			t.Fatal(err)
		}
		if id == 1 {
			receive(t, started)
		}
	}

	unsubscribed := make(chan error, 1)
	go func() { unsubscribed <- sub.Unsubscribe(context.Background()) }()
	close(release)
	if err := receive(t, unsubscribed); err != nil {
		t.Fatalf("Unsubscribe(): %v", err)
	}
	for id := 1; id <= 3; id++ {
		if got := receive(t, events); got != id {
			t.Errorf("handled event = %d, want %d", got, id)
		}
	}
	if err := sub.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("second Unsubscribe(): %v", err)
	}
}

func TestUnsubscribeTimeoutLeavesDrainRunning(t *testing.T) {
	b := newTestBus(t, 1)
	t.Cleanup(func() { closeTestBus(t, b) })

	started := make(chan struct{})
	release := make(chan struct{})
	sub, err := b.Subscribe(func(_ context.Context, _ testEvent) error {
		close(started)
		<-release
		return nil
	}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), testEvent{id: 1}); err != nil {
		t.Fatal(err)
	}
	receive(t, started)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sub.Unsubscribe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Unsubscribe() error = %v, want context.Canceled", err)
	}
	close(release)
	if err := sub.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("Unsubscribe() after drain: %v", err)
	}
}

func TestCancelDropsQueuedEventsAndCancelsHandlerContext(t *testing.T) {
	b := newTestBus(t, 1)
	t.Cleanup(func() { closeTestBus(t, b) })

	started := make(chan struct{})
	canceled := make(chan struct{})
	handled := make(chan int, 2)
	sub, err := b.Subscribe(func(ctx context.Context, event testEvent) error {
		handled <- event.id
		if event.id == 1 {
			close(started)
			<-ctx.Done()
			close(canceled)
		}
		return nil
	}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Publish(context.Background(), testEvent{id: 1}); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	if err := b.Publish(context.Background(), testEvent{id: 2}); err != nil {
		t.Fatal(err)
	}
	sub.Cancel()
	receive(t, canceled)
	if err := sub.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("Unsubscribe() after Cancel(): %v", err)
	}
	if got := receive(t, handled); got != 1 {
		t.Fatalf("handled event = %d, want only event 1", got)
	}
	select {
	case got := <-handled:
		t.Fatalf("handled queued event %d after Cancel()", got)
	default:
	}
}

func TestHandlerErrorsAndPanicsAreReportedAndWorkerContinues(t *testing.T) {
	b := newTestBus(t, 3)
	t.Cleanup(func() { closeTestBus(t, b) })

	handled := make(chan int, 3)
	reported := make(chan error, 2)
	if _, err := b.Subscribe(func(_ context.Context, event testEvent) error {
		handled <- event.id
		switch event.id {
		case 1:
			return errors.New("handler error")
		case 2:
			panic("handler panic")
		default:
			return nil
		}
	}, func(err error) { reported <- err }); err != nil {
		t.Fatal(err)
	}

	for id := 1; id <= 3; id++ {
		if err := b.Publish(context.Background(), testEvent{id: id}); err != nil {
			t.Fatal(err)
		}
	}
	for id := 1; id <= 3; id++ {
		if got := receive(t, handled); got != id {
			t.Errorf("handled event = %d, want %d", got, id)
		}
	}
	firstError := receive(t, reported)
	secondError := receive(t, reported)
	if firstError.Error() != "handler error" {
		t.Errorf("first reported error = %q, want handler error", firstError)
	}
	if !strings.Contains(secondError.Error(), "handler panic") || !strings.Contains(secondError.Error(), "goroutine") {
		t.Errorf("reported panic error lacks panic value or stack: %v", secondError)
	}
}

func TestCloseDrainsAndIsIdempotent(t *testing.T) {
	b := newTestBus(t, 1)

	started := make(chan struct{})
	release := make(chan struct{})
	handled := make(chan int, 2)
	if _, err := b.Subscribe(func(_ context.Context, event testEvent) error {
		handled <- event.id
		if event.id == 1 {
			close(started)
			<-release
		}
		return nil
	}, func(error) {}); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), testEvent{id: 1}); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	if err := b.Publish(context.Background(), testEvent{id: 2}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want context.Canceled", err)
	}
	if err := b.Publish(context.Background(), testEvent{id: 3}); !errors.Is(err, bus.ErrClosed) {
		t.Fatalf("Publish() after Close error = %v, want ErrClosed", err)
	}
	if _, err := b.Subscribe(func(context.Context, testEvent) error { return nil }, func(error) {}); !errors.Is(err, bus.ErrClosed) {
		t.Fatalf("Subscribe() after Close error = %v, want ErrClosed", err)
	}

	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close() after drain: %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("second Close(): %v", err)
	}
	for id := 1; id <= 2; id++ {
		if got := receive(t, handled); got != id {
			t.Errorf("handled event = %d, want %d", got, id)
		}
	}
}
