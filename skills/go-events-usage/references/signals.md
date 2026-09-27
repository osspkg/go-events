# OS signal callbacks

The root package is imported as `events`:

```go
import "go.osspkg.com/events"
```

## API

- `events.OnStopSignal(ctx, callback)` waits for an interrupt, `SIGINT`, or `SIGTERM`.
- `events.OnHubSignal(ctx, callback)` waits for `SIGHUP`.
- `events.OnCustomSignal(ctx, callback, signals...)` waits for one of the supplied signals.

These functions are one-shot and return after a signal or context cancellation. If `ctx` is canceled, they return without invoking the callback. An empty `signals` slice passed to `OnCustomSignal` returns immediately. The operating system does not allow handling `SIGKILL`.

The callback takes no context. Use the surrounding context to bound or stop the wait; if shutdown work itself needs cancellation, capture or pass an appropriate context in the callback closure.

## Example

```go
func waitForShutdown(ctx context.Context, shutdown func()) {
    events.OnStopSignal(ctx, shutdown)
}
```

See the complete example in [examples/signals.go](../examples/signals.go). The repository implementation is in [`signal.go`](../../../signal.go); package documentation is at [pkg.go.dev](https://pkg.go.dev/go.osspkg.com/events).
