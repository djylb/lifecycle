package lifecycle_test

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestStandardLibraryOnly checks that every package, tests included,
// depends on the standard library and this module alone.
func TestStandardLibraryOnly(t *testing.T) {
	if runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("cannot run the go command")
	}
	goCmd, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command")
	}
	mod, err := exec.CommandContext(t.Context(), goCmd, "list", "-f", "{{.Module.Path}}").Output()
	if err != nil {
		t.Fatal(err)
	}
	module := strings.TrimSpace(string(mod))
	out, err := exec.CommandContext(t.Context(), goCmd, "list", "-deps", "-test", "-f", "{{if not .Standard}}{{if .Module}}{{.Module.Path}}{{else}}unknown{{end}}{{end}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		path := strings.TrimSpace(line)
		if path != "" && path != module {
			t.Errorf("dependency outside the standard library: %s", path)
		}
	}
}
