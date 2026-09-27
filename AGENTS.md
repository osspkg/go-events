# Agent instructions

## Repository map

- `signal.go` and `signal_test.go` contain the root package's operating-system signal helpers.
- `bus/` contains the typed, asynchronous in-process event bus and its tests.
- The module targets Go 1.26. The current implementation uses only the standard library.

Keep the signal helpers and generic event bus as separate APIs. Public API or behavior changes must be reflected in `README.md` and covered by tests in the corresponding package.

## Development and validation

Run commands from the repository root.

- `go test ./...` runs all package tests.
- `go test -race ./...` checks concurrent code for data races.
- `go vet ./...` runs Go's static checks.
- `gofmt -w <changed Go files>` formats Go source; `git diff --check` checks whitespace.
- `make tests`, `make lint`, `make build`, and `make ci` delegate to `goppy` as defined in `Makefile`.

CI runs `make ci` on Go 1.26. That target includes `make install`, which installs `goppy@latest` and runs `goppy setup-lib`, then license, lint, tests, and build targets. These steps may change local files or tooling; inspect `git status` and the diff before and after running them. The linter configuration excludes test files (`run.tests: false`), so run Go tests separately when validating behavior.

## API and concurrency contracts

- Put `context.Context` first in public operations that wait or can be canceled. Document what cancellation does and whether callbacks still run.
- Keep bus queues bounded. A subscriber has one worker and processes its queue sequentially; do not imply global ordering across subscribers or concurrent publishers.
- Preserve the documented backpressure, partial fan-out, unsubscribe/drain, cancel, close, and handler-error behavior in `bus/`.
- Event values are shallow-copied. Callers must not mutate referenced data while subscribers may handle it.
- Add regression tests for behavior changes. Keep tests in the package's `*_test.go` file and avoid tests that depend on real process signals unless isolated and deterministic.

## Project memory

Use the Chroma collection `chat_go_events_memory` for durable project context.

- Before a non-trivial task, ensure the collection exists: list collections and create this exact collection if missing, then query it for relevant context.
- After a non-trivial task, query for related memories before adding or updating one. Store only durable decisions, constraints, or lessons, one fact per document; do not store secrets or transient output.
- If Chroma is unavailable, continue from repository evidence. Do not invent memory or let a memory failure block ordinary work.
