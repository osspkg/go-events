package examples

import (
	"context"
	"fmt"
	"log"

	"go.osspkg.com/events/bus"
)

// PublishOne creates a bus, handles one string event, and drains the worker.
func PublishOne(ctx context.Context, value string) error {
	eventBus, err := bus.New[string](8)
	if err != nil {
		return err
	}

	_, err = eventBus.Subscribe(
		func(_ context.Context, event string) error {
			fmt.Println(event)
			return nil
		},
		func(err error) {
			log.Printf("event handler: %v", err)
		},
	)
	if err != nil {
		return err
	}

	if err := eventBus.Publish(ctx, value); err != nil {
		return err
	}
	return eventBus.Close(ctx)
}
