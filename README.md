# events

`go.osspkg.com/events` provides blocking helpers that call a function when the
process receives an operating-system signal.

## Usage

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

`OnStopSignal` waits for an interrupt or termination signal. `OnHubSignal`
waits for `SIGHUP`, which is commonly used to reload configuration.
`OnCustomSignal` waits for one of the supplied signals. Each function returns
without calling the callback if its context is canceled. If no signals are
supplied, `OnCustomSignal` returns without calling the callback. Pass a
cancelable context to stop waiting from another part of the program.

These helpers handle one signal and invoke the callback once. `SIGKILL` cannot
be caught by a process, so it cannot trigger cleanup code.

## API

| Function | Waits for | Behavior |
| --- | --- | --- |
| `OnStopSignal(ctx, callFunc)` | Interrupt or termination signal | Calls `callFunc` once, or returns when `ctx` is canceled. |
| `OnHubSignal(ctx, callFunc)` | `SIGHUP` | Calls `callFunc` once, or returns when `ctx` is canceled. |
| `OnCustomSignal(ctx, callFunc, signals...)` | One supplied signal | Calls `callFunc` once, or returns when `ctx` is canceled; with no signals, returns without calling it. |
