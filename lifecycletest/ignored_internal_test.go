package lifecycletest

import "testing"

func TestIgnoreOnlyTopFrame(t *testing.T) {
	stack := "goroutine 1 [chan receive]:\nworker()\n file.go:1\ntesting.tRunner()"
	if ignored(stack, nil) || !ignored(stack, []string{"testing.tRunner"}) {
		t.Fatal(stack)
	}
}
