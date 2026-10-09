# Typed asynchronous bus

Import the separate package:

```go
import "go.osspkg.com/events/bus"
```

A `Bus[T]` delivers values of one type inside the current process. Each subscriber has a bounded FIFO queue and one worker, so its handler runs sequentially. Ordering between subscribers and concurrent `Publish` calls is not guaranteed. Values are shallow-copied; do not mutate referenced data while handlers may use it.

## Basic use

```go
b, err := bus.New[string](16) // capacity per subscriber
if err != nil {
    return err
}

sub, err := b.Subscribe(
    func(ctx context.Context, event string) error {
        fmt.Println(event)
        return nil
    },
    func(err error) { log.Printf("event handler: %v", err) },
)
if err != nil {
    return err
}

if err := b.Publish(ctx, "started"); err != nil {
    return err
}
if err := sub.Unsubscribe(ctx); err != nil {
    return err
}
return b.Close(ctx)
```

`New` requires a positive queue capacity. Both the handler and error handler are required. `Publish` succeeds when there are no subscribers. With subscribers, it enqueues for each active subscriber and waits for queue capacity or `ctx` cancellation. Cancellation during fan-out can leave earlier subscribers with the event and later subscribers without it; the returned error reports the interrupted publication.

## Lifecycle

- `Subscription.Unsubscribe(ctx)` stops accepting new events and drains accepted events. A deadline error stops waiting; draining continues.
- `Subscription.Cancel()` cancels the handler context and discards queued events immediately. A dequeued or running handler may still run; handlers should observe their context. `Cancel` does not wait.
- `Bus.Close(ctx)` rejects new publishes and subscriptions, then drains existing subscriptions. A deadline error stops waiting; shutdown continues. A later `Close` can wait for completion.

Handler errors and recovered handler panics are passed to the required error handler, after which the worker continues. Panic reports include a stack trace. The error handler itself must not panic. Do not synchronously wait for `Unsubscribe` or `Close` from that subscription's own handler/error handler. Avoid publishing synchronously from a handler into a full queue on the same bus, which can block that worker.

See the complete example in [examples/bus.go](../examples/bus.go), the implementation in [`bus/bus.go`](../../../bus/bus.go), and the public package docs at [pkg.go.dev](https://pkg.go.dev/go.osspkg.com/events/bus).
