// Package examples contains usage examples for go.osspkg.com/events.
package examples

import (
	"context"

	"go.osspkg.com/events"
)

// WaitForStop calls shutdown after an interrupt or termination signal.
// It returns without calling shutdown if ctx is canceled first.
func WaitForStop(ctx context.Context, shutdown func()) {
	events.OnStopSignal(ctx, shutdown)
}
