// This file is self-contained so standard-library-only test suites can
// generate their local copy without introducing a module dependency.

package lifecycletest

import (
	"bytes"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// LeakTimeout is how long CheckGoroutines and VerifyMain wait for
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
var alwaysIgnored = []string{ //nolint:gochecknoglobals // read-only table
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
	_, frames, _ := strings.Cut(stack, "\n")
	top, rest, _ := strings.Cut(frames, "\n")
	if strings.HasPrefix(top, "runtime.gopark(") {
		// Some runtimes include the scheduler frame above the parked function.
		_, rest, _ = strings.Cut(rest, "\n")
		top, _, _ = strings.Cut(rest, "\n")
	}
	return slices.ContainsFunc(alwaysIgnored, func(s string) bool { return strings.HasPrefix(top, s) }) || slices.ContainsFunc(ignore, match)
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
