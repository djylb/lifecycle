// Package lifecycletest helps test code built on lifecycle: it checks that a
// scope tree is empty, finds leaked goroutines from runtime stack dumps, and
// runs the standard start, close and verify cycle. Where the runtime has the
// goroutineleak profile, leak reports add it: it marks the goroutines blocked
// forever on something no running goroutine can reach.
package lifecycletest

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djylb/lifecycle"
)

// LeakTimeout is how long CheckGoroutines, VerifyMain and RunCycle wait for
// goroutines to end before reporting them as leaked.
const LeakTimeout = 5 * time.Second

// Goroutine is one goroutine from a runtime stack dump.
type Goroutine struct {
	ID    uint64
	State string // e.g. "chan receive" or "select, 2 minutes"
	Stack string // the full dump of the goroutine, header included
}

func (g Goroutine) String() string { return g.Stack }

// alwaysIgnored match goroutines of the runtime and the testing package.
var alwaysIgnored = []string{
	"testing.(*M).",
	"testing.tRunner(",
	"testing.runTests",
	"testing.(*T).Run(",
	"testing.(*T).Parallel(",
	"os/signal.signal_recv",
	"os/signal.loop",
	"runtime.ensureSigM",
	"runtime.handleEvent", // js/wasm event handler
	"runtime/trace.Start",
	"runtime.ReadTrace",
}

// Goroutines returns every goroutine except the caller's.
func Goroutines() []Goroutine {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var gs []Goroutine
	for i, block := range bytes.Split(buf, []byte("\n\n")) {
		if i == 0 { // runtime.Stack dumps the caller first
			continue
		}
		if g, ok := parse(string(block)); ok {
			gs = append(gs, g)
		}
	}
	return gs
}

func parse(block string) (Goroutine, bool) {
	header, _, _ := strings.Cut(block, "\n")
	rest, ok := strings.CutPrefix(header, "goroutine ")
	if !ok {
		return Goroutine{}, false
	}
	idText, state, ok := strings.Cut(rest, " ")
	if !ok {
		return Goroutine{}, false
	}
	id, err := strconv.ParseUint(idText, 10, 64)
	if err != nil {
		return Goroutine{}, false
	}
	state = strings.TrimSuffix(strings.TrimPrefix(state, "["), "]:")
	return Goroutine{ID: id, State: state, Stack: block}, true
}

// Leaks returns the goroutines that are not in baseline and match none of
// ignore (substrings of the stack dump) or of the runtime's and the testing
// package's own goroutines. It retries until none are left or timeout has
// passed. A nil baseline allows no goroutine.
func Leaks(baseline []Goroutine, timeout time.Duration, ignore ...string) []Goroutine {
	known := make(map[uint64]bool, len(baseline))
	for _, g := range baseline {
		known[g.ID] = true
	}
	deadline := time.Now().Add(timeout)
	delay := time.Millisecond
	for {
		var leaked []Goroutine
		for _, g := range Goroutines() {
			if !known[g.ID] && !ignored(g.Stack, ignore) {
				leaked = append(leaked, g)
			}
		}
		if len(leaked) == 0 || !time.Now().Before(deadline) {
			return leaked
		}
		time.Sleep(delay)
		delay = min(2*delay, 100*time.Millisecond)
	}
}

func ignored(stack string, ignore []string) bool {
	match := func(s string) bool { return strings.Contains(stack, s) }
	return slices.ContainsFunc(alwaysIgnored, match) || slices.ContainsFunc(ignore, match)
}

func format(leaked []Goroutine) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(leaked)))
	b.WriteString(" leaked goroutine(s):")
	for _, g := range leaked {
		b.WriteString("\n\n")
		b.WriteString(g.Stack)
	}
	if p := leakProfile(); p != "" {
		b.WriteString("\n\n")
		b.WriteString(p)
	}
	return b.String()
}

// leakProfile returns the goroutineleak profile when it lists goroutines,
// else "". Taking it runs a garbage collection. Runtimes without the profile
// return "".
func leakProfile() string {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return ""
	}
	var b bytes.Buffer
	if err := p.WriteTo(&b, 1); err != nil || strings.HasPrefix(b.String(), "goroutineleak profile: total 0\n") {
		return ""
	}
	return b.String()
}

// CheckGoroutines records the goroutines running now and, when the test
// ends, fails it if new goroutines are still running after LeakTimeout.
// ignore lists substrings of stack dumps to tolerate. It must not be used
// in tests that run in parallel with others, whose goroutines it would
// report.
func CheckGoroutines(tb testing.TB, ignore ...string) {
	tb.Helper()
	baseline := Goroutines()
	tb.Cleanup(func() {
		if leaked := Leaks(baseline, LeakTimeout, ignore...); len(leaked) > 0 {
			tb.Error(format(leaked))
		}
	})
}

// VerifyMain runs the tests and exits. If they pass but goroutines started
// during the run are still running after LeakTimeout, it prints them and
// exits with status 1. Call it from TestMain:
//
//	func TestMain(m *testing.M) { lifecycletest.VerifyMain(m) }
func VerifyMain(m *testing.M, ignore ...string) {
	baseline := Goroutines()
	code := m.Run()
	if code == 0 {
		if leaked := Leaks(baseline, LeakTimeout, ignore...); len(leaked) > 0 {
			code = 1
			if _, err := os.Stderr.WriteString(format(leaked) + "\n"); err != nil {
				code = 2 // the leaks could not even be reported
			}
		}
	}
	os.Exit(code)
}

// AssertEmpty fails the test if s or a descendant still has running tasks,
// registered resources or unfinished child scopes.
func AssertEmpty(tb testing.TB, s *lifecycle.Scope) {
	tb.Helper()
	if st := s.Stats(); st.Tasks != 0 || st.Resources != 0 || st.Scopes != 0 {
		tb.Errorf("scope not empty: %d task(s), %d resource(s), %d child scope(s)", st.Tasks, st.Resources, st.Scopes)
	}
}

// RunCycle runs the standard lifecycle test: it creates a root scope with
// the CancelOnError policy, calls start to fill it, closes it, waits up to
// timeout for it to finish, asserts that it is empty, and checks that no
// goroutine started during the cycle is left. It fails the test if start
// fails, the scope does not finish in time, or something is left, and
// returns the scope's result for the caller to check. Like
// CheckGoroutines, it must not run in parallel with other tests.
func RunCycle(tb testing.TB, timeout time.Duration, start func(s *lifecycle.Scope) error) error {
	tb.Helper()
	baseline := Goroutines()
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	if err := start(s); err != nil {
		tb.Errorf("start: %v", err)
	}
	s.Close(nil)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := s.Wait(ctx)
	if errors.Is(err, lifecycle.ErrNotFinished) {
		st := s.Stats()
		tb.Fatalf("scope did not finish within %v: %d task(s), %d child scope(s) left", timeout, st.Tasks, st.Scopes)
	}
	AssertEmpty(tb, s)
	if leaked := Leaks(baseline, timeout); len(leaked) > 0 {
		tb.Error(format(leaked))
	}
	return err
}
