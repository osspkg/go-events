---
name: go-events-usage
description: Use and integrate the go.osspkg.com/events Go library, including OS signal callbacks and its generic asynchronous event bus.
---

# Go Events Usage

Use this skill when implementing or explaining integrations with `go.osspkg.com/events`.

Choose the relevant API and read its reference before changing code:

- For process shutdown or OS signals, read [references/signals.md](references/signals.md).
- For in-process typed event delivery, read [references/bus.md](references/bus.md).

The root package provides one-shot signal waits. The separate `bus` package provides asynchronous, bounded per-subscriber queues. Keep those roles distinct. The library does not persist events or deliver them across processes.

Use and adapt the code examples in [examples/signals.go](examples/signals.go) and [examples/bus.go](examples/bus.go). Check the repository [README](../../README.md) and API source when exact behavior or current signatures matter.
