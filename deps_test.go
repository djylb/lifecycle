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
	const module = "github.com/djylb/lifecycle"
	out, err := exec.Command(goCmd, "list", "-deps", "-test", "-f", "{{.ImportPath}} {{.Standard}}", module+"/...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		path, std, _ := strings.Cut(strings.TrimSpace(line), " ")
		if std != "true" && path != module && !strings.HasPrefix(path, module+"/") && !strings.HasPrefix(path, module+".") && !strings.HasPrefix(path, module+"_") {
			t.Errorf("dependency outside the standard library: %s", path)
		}
	}
}
