# events

`go.osspkg.com/events` is a Go library for waiting on operating-system signals
and publishing typed events inside one process.

- Signal helpers live in the root `events` package.
- The asynchronous, typed event bus lives in `go.osspkg.com/events/bus`.

## Requirements and installation

The module requires Go 1.26 or newer.

```sh
go get go.osspkg.com/events
```

## Operating-system signals

The root package provides blocking signal helpers. When a matching signal
arrives, a helper stops its signal subscription and invokes the callback once.
Canceling the context returns without calling the callback; `OnCustomSignal` with
no signals returns immediately.

```go
package main

import (
	"context"
	"log"

	"go.osspkg.com/events"
)

func main() {
	events.OnStopSignal(context.Background(), func() {
		log.Println("shutting down")
	})
}
```

| Function | Waits for | Cancellation and callback behavior |
| --- | --- | --- |
| `OnStopSignal(ctx, callFunc)` | Interrupt or termination signal | Context cancellation returns without calling `callFunc`; a signal calls it once. |
| `OnHubSignal(ctx, callFunc)` | `SIGHUP`, commonly used to reload configuration | Context cancellation returns without calling `callFunc`; a signal calls it once. |
| `OnCustomSignal(ctx, callFunc, signals...)` | One supplied signal | Context cancellation returns without calling `callFunc`; an empty signal list returns immediately without calling it. |

`SIGKILL` cannot be caught, so it cannot trigger cleanup code. These helpers
handle one signal; use a cancelable context when another part of the program must
stop the wait.

## Asynchronous event bus

`bus.Bus[T]` publishes one event type to any number of subscribers. Each
subscriber has a bounded FIFO queue and one worker, so its handler processes
events sequentially. Different subscribers run independently.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"go.osspkg.com/events/bus"
)

type Message struct {
	Text string
}

func main() {
	ctx := context.Background()
	events, err := bus.New[Message](16)
	if err != nil {
		log.Fatal(err)
	}

	sub, err := events.Subscribe(func(_ context.Context, message Message) error {
		fmt.Println(message.Text)
		return nil
	}, func(err error) {
		log.Printf("event handler: %v", err)
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := events.Publish(ctx, Message{Text: "hello"}); err != nil {
		log.Fatal(err)
	}
	if err := sub.Unsubscribe(ctx); err != nil {
		log.Fatal(err)
	}
	if err := events.Close(ctx); err != nil {
		log.Fatal(err)
	}
}
```

### Bus API

The generic event types live in `go.osspkg.com/events/bus`:

| Type | Purpose |
| --- | --- |
| `bus.Bus[T]` | Publishes values of event type `T` to subscribers. Create it with `bus.New`. |
| `bus.Handler[T]` | Processes one event with the subscription context and returns an optional error. |
| `bus.ErrorHandler` | Receives handler errors and recovered panics on that subscription's worker. |
| `bus.Subscription[T]` | Controls one subscriber's worker and queue. |

| API | Behavior |
| --- | --- |
| `bus.New[T](queueCapacity)` | Creates a bus with that many queue slots per subscriber. Capacity must be positive. |
| `(*Bus[T]).Subscribe(handler, onError)` | Registers a handler and required error callback; returns a subscription handle. Nil callbacks are rejected. |
| `(*Bus[T]).Publish(ctx, event)` | Enqueues for active subscribers. Waits for queue space or context cancellation. With no subscribers, it succeeds. |
| `(*Subscription[T]).Unsubscribe(ctx)` | Stops delivery and drains accepted events; on timeout, draining continues in the background. Idempotent. |
| `(*Subscription[T]).Cancel()` | Cancels the handler context and discards queued events. It does not wait for a running handler. |
| `(*Bus[T]).Close(ctx)` | Rejects new work and drains workers; on timeout, shutdown continues and a later call can wait for completion. |

Errors exported by the package:

- `bus.ErrInvalidQueueCapacity`: `New` received a non-positive capacity or a zero-value `Bus` was used.
- `bus.ErrNilHandler` and `bus.ErrNilErrorHandler`: `Subscribe` received a nil callback.
- `bus.ErrClosed`: subscription or publication was attempted after shutdown began.

`Publish` returns the context error if canceled while waiting for queue capacity.
If cancellation or an unsubscribe happens during fan-out, subscribers already
reached may process the event while later ones do not. Concurrent publishers have
no global ordering guarantee; each subscriber handles accepted events in FIFO
order.

A handler should not synchronously wait for `Unsubscribe` or `Close` on its own
worker. Publishing synchronously to a full queue on the same bus from a handler
can also block that worker. `Cancel` may not stop an event already dequeued for a
handler; handlers should observe their context. Handler errors and recovered
panics are sent to the subscription's error callback. Recovered panic errors
include a stack trace; an error callback must not panic. If an error callback is
shared by multiple subscriptions, it must be concurrency-safe.

Event values are shallow-copied. Do not mutate referenced data while subscribers
may still be processing it.

## Development

Run from the repository root:

```sh
go test ./...
go test -race ./...
go vet ./...
```

`make ci` runs the repository's CI sequence: install and setup `goppy`, license
checks, lint, tests, and build. See [Makefile](Makefile) and [CI workflow](.github/workflows/ci.yml)
for the configured targets.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
