# AGENTS.md

- Before you finish, `gofmt -l .` must print nothing and `go vet ./...`,
  `go test -race ./...`, `GOOS=js GOARCH=wasm go vet ./...` and the js/wasm
  tests (`GOOS=js GOARCH=wasm go test ./...` with
  `$(go env GOROOT)/lib/wasm` on `PATH`) must pass, and the fuzz target
  must survive a short run. Give every test run a `-timeout`.
- Standard library only, also for tests; `deps_test.go` checks it.
- Never hold a lock of this package while calling user code (task
  functions, Closers, error handlers, `OnDrop`) or while blocking.
- `Close` never blocks and runs no user code. `Go`, `Add` and `Child` are
  linearised with `Close`: accepted means joined or closed, refused means
  `ErrClosed` and, for `Add`, the resource closed at once.
- Start no goroutine beyond those listed in the package documentation;
  document any new one and how it ends.
- Queue `Push`, `Pop` and `TryPop` stay allocation-free; there is no
  unbounded mode.
- Tests must not leak goroutines or timers, and stress tests are bounded
  in time.
- The README, docs and commits name no other project and quote no benchmark
  numbers.
- Commit subjects are short, English and imperative.
- Never commit host names, IP addresses or credentials.
- Loops that retry non-blocking calls (tests included) must yield, e.g. with
  `runtime.Gosched()`: js/wasm has no preemption, so a spinning goroutine
  starves every other one, and `go test -timeout` cannot fire.
