// Package bus provides an asynchronous, in-process event bus.
package bus

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

var (
	// ErrClosed is returned when an operation requires an open bus.
	ErrClosed = errors.New("bus: closed")
	// ErrInvalidQueueCapacity is returned when New receives a non-positive capacity.
	ErrInvalidQueueCapacity = errors.New("bus: queue capacity must be positive")
	// ErrNilHandler is returned when Subscribe receives a nil handler.
	ErrNilHandler = errors.New("bus: handler must not be nil")
	// ErrNilErrorHandler is returned when Subscribe receives a nil error handler.
	ErrNilErrorHandler = errors.New("bus: error handler must not be nil")
)

// Handler processes an event in the context owned by its subscription.
type Handler[T any] func(context.Context, T) error

// ErrorHandler receives errors returned by a Handler and panics recovered from it.
// Error handlers run on the subscription's worker and must not panic.
type ErrorHandler func(error)

type busState uint8

const (
	busOpen busState = iota
	busClosing
	busClosed
)

// Bus asynchronously publishes values of one event type to its subscribers.
// Create a Bus with New. The zero value is not ready for use.
type Bus[T any] struct {
	mu sync.Mutex

	state         busState
	queueCapacity int
	subscribers   []*subscription[T]

	publishers     int
	publishersDone chan struct{}
	closeDone      chan struct{}
}

// New creates a Bus whose per-subscriber queues have queueCapacity slots.
func New[T any](queueCapacity int) (*Bus[T], error) {
	if queueCapacity <= 0 {
		return nil, ErrInvalidQueueCapacity
	}

	publishersDone := make(chan struct{})
	closeDone := make(chan struct{})
	close(publishersDone)

	return &Bus[T]{
		queueCapacity:  queueCapacity,
		publishersDone: publishersDone,
		closeDone:      closeDone,
	}, nil
}

// Subscribe registers handler and returns a handle for stopping the subscription.
// handler and onError are required. Events are processed sequentially per
// subscription. A handler panic is recovered and reported through onError.
func (b *Bus[T]) Subscribe(handler Handler[T], onError ErrorHandler) (*Subscription[T], error) {
	if b.queueCapacity <= 0 {
		return nil, ErrInvalidQueueCapacity
	}
	if handler == nil {
		return nil, ErrNilHandler
	}
	if onError == nil {
		return nil, ErrNilErrorHandler
	}

	ctx, cancel := context.WithCancel(context.Background())
	sub := &subscription[T]{
		handler: handler,
		onError: onError,
		ctx:     ctx,
		cancel:  cancel,
		queue:   make([]T, b.queueCapacity),
		wake:    make(chan struct{}, 1),
		space:   make(chan struct{}, 1),
		done:    make(chan struct{}),
	}

	b.mu.Lock()
	if b.state != busOpen {
		b.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	sub.bus = b
	b.subscribers = append(b.subscribers, sub)
	b.mu.Unlock()

	go sub.run()
	return &Subscription[T]{sub: sub}, nil
}

// Publish enqueues event for each active subscriber. It waits for queue space
// or ctx cancellation. If ctx is canceled or a subscriber is unsubscribed during
// fan-out, earlier subscribers may already have received the event. With no
// subscribers, Publish succeeds. A handler should avoid publishing synchronously
// to a full queue on its own bus, as that can block its worker.
func (b *Bus[T]) Publish(ctx context.Context, event T) error {
	if b.queueCapacity <= 0 {
		return ErrInvalidQueueCapacity
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	if b.state != busOpen {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.publishers == 0 {
		b.publishersDone = make(chan struct{})
	}
	b.publishers++
	subscribers := append([]*subscription[T](nil), b.subscribers...)
	b.mu.Unlock()
	defer b.finishPublish()

	for _, sub := range subscribers {
		if err := sub.enqueue(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bus[T]) finishPublish() {
	b.mu.Lock()
	b.publishers--
	if b.publishers == 0 {
		close(b.publishersDone)
	}
	b.mu.Unlock()
}

// Close rejects new subscriptions and publications, then drains active queues.
// If ctx is canceled before draining finishes, Close returns ctx.Err; draining
// continues in the background and a later Close call can wait for completion.
// Do not call Close synchronously from one of this Bus's handlers or error
// handlers; Close waits for those workers to finish.
func (b *Bus[T]) Close(ctx context.Context) error {
	if b.queueCapacity <= 0 {
		return ErrInvalidQueueCapacity
	}
	b.mu.Lock()
	if b.state == busOpen {
		b.state = busClosing
		go b.shutdown()
	}
	done := b.closeDone
	b.mu.Unlock()

	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bus[T]) shutdown() {
	b.mu.Lock()
	publishersDone := b.publishersDone
	b.mu.Unlock()
	<-publishersDone

	b.mu.Lock()
	subscribers := append([]*subscription[T](nil), b.subscribers...)
	b.mu.Unlock()

	for _, sub := range subscribers {
		sub.stopAccepting()
	}
	for _, sub := range subscribers {
		<-sub.done
	}

	b.mu.Lock()
	b.state = busClosed
	close(b.closeDone)
	b.mu.Unlock()
}

func (b *Bus[T]) remove(sub *subscription[T]) {
	b.mu.Lock()
	for i, current := range b.subscribers {
		if current == sub {
			copy(b.subscribers[i:], b.subscribers[i+1:])
			b.subscribers[len(b.subscribers)-1] = nil
			b.subscribers = b.subscribers[:len(b.subscribers)-1]
			break
		}
	}
	b.mu.Unlock()
}

// Subscription controls one subscriber's queue and worker.
type Subscription[T any] struct {
	sub *subscription[T]
}

// Unsubscribe stops accepting events and waits for queued events to be handled.
// If ctx expires, draining continues in the background. Unsubscribe is safe to
// call more than once. Do not wait for a subscription to unsubscribe from its own
// handler or error handler.
func (s *Subscription[T]) Unsubscribe(ctx context.Context) error {
	s.sub.stopAccepting()
	return wait(ctx, s.sub.done)
}

// Cancel immediately cancels the handler context and discards queued events.
// An event already dequeued may still enter its handler; handlers must observe
// their context to stop promptly. Cancel does not wait for a running handler.
func (s *Subscription[T]) Cancel() {
	s.sub.cancelNow()
}

type subscriptionState uint8

const (
	subscriptionActive subscriptionState = iota
	subscriptionDraining
	subscriptionCanceled
)

type subscription[T any] struct {
	bus     *Bus[T]
	handler Handler[T]
	onError ErrorHandler

	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	state subscriptionState
	queue []T
	head  int
	count int
	wake  chan struct{}
	space chan struct{}
	done  chan struct{}
}

func (s *subscription[T]) enqueue(ctx context.Context, event T) error {
	for {
		s.mu.Lock()
		if s.state != subscriptionActive {
			s.mu.Unlock()
			return nil
		}
		if s.count < len(s.queue) {
			tail := (s.head + s.count) % len(s.queue)
			s.queue[tail] = event
			s.count++
			s.mu.Unlock()
			notify(s.wake)
			return nil
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.space:
		}
	}
}

func (s *subscription[T]) run() {
	defer func() {
		s.cancel()
		close(s.done)
		s.bus.remove(s)
	}()

	for {
		s.mu.Lock()
		if s.state == subscriptionCanceled {
			s.clearQueueLocked()
			s.mu.Unlock()
			return
		}
		if s.count > 0 {
			event := s.popLocked()
			s.mu.Unlock()
			notify(s.space)

			if err := s.handle(event); err != nil {
				s.onError(err)
			}
			continue
		}
		if s.state == subscriptionDraining {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		<-s.wake
	}
}

func (s *subscription[T]) handle(event T) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("bus: handler panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return s.handler(s.ctx, event)
}

func (s *subscription[T]) popLocked() T {
	event := s.queue[s.head]
	var zero T
	s.queue[s.head] = zero
	s.head = (s.head + 1) % len(s.queue)
	s.count--
	if s.count == 0 {
		s.head = 0
	}
	return event
}

func (s *subscription[T]) clearQueueLocked() {
	var zero T
	for s.count > 0 {
		s.queue[s.head] = zero
		s.head = (s.head + 1) % len(s.queue)
		s.count--
	}
	s.head = 0
}

func (s *subscription[T]) stopAccepting() {
	s.mu.Lock()
	if s.state == subscriptionActive {
		s.state = subscriptionDraining
		s.mu.Unlock()
		notify(s.wake)
		notify(s.space)
		return
	}
	s.mu.Unlock()
}

func (s *subscription[T]) cancelNow() {
	s.mu.Lock()
	if s.state == subscriptionCanceled {
		s.mu.Unlock()
		return
	}
	s.state = subscriptionCanceled
	s.clearQueueLocked()
	s.mu.Unlock()

	s.cancel()
	notify(s.wake)
	notify(s.space)
}

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
