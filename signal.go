/*
 *  Copyright (c) 2024-2026 Mikhail Knyazhev <markus621@yandex.ru>. All rights reserved.
 *  Use of this source code is governed by a BSD 3-Clause license that can be found in the LICENSE file.
 */

package events

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// OnStopSignal waits for an interrupt or termination signal, then calls callFunc.
// It returns without calling callFunc if ctx is canceled. It cannot handle
// signals such as SIGKILL that the operating system does not allow a process to
// catch.
func OnStopSignal(ctx context.Context, callFunc func()) {
	waitForSignal(ctx, callFunc, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
}

// OnHubSignal waits for SIGHUP, then calls callFunc. It returns without calling
// callFunc if ctx is canceled.
func OnHubSignal(ctx context.Context, callFunc func()) {
	waitForSignal(ctx, callFunc, syscall.SIGHUP)
}

// OnCustomSignal waits for one of sig, then calls callFunc. It returns without
// calling callFunc if ctx is canceled or sig is empty.
func OnCustomSignal(ctx context.Context, callFunc func(), sig ...os.Signal) {
	if len(sig) == 0 {
		return
	}

	waitForSignal(ctx, callFunc, sig...)
}

func waitForSignal(ctx context.Context, callFunc func(), sig ...os.Signal) {
	quit := make(chan os.Signal, len(sig))
	signal.Notify(quit, sig...)

	select {
	case <-ctx.Done():
	case <-quit:
	}

	signal.Stop(quit)
	if ctx.Err() != nil {
		return
	}

	callFunc()
}
