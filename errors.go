package lifecycle

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	// ErrClosed is returned by Go, Add and Child on a closed scope, and is
	// the cause recorded by Close(nil).
	ErrClosed = errors.New("lifecycle: scope closed")

	// ErrNotFinished is wrapped by the error Wait returns when its context
	// ends before the scope has finished.
	ErrNotFinished = errors.New("lifecycle: scope not finished")

	// ErrGoexit is reported for a task that called runtime.Goexit, for
	// example through testing.T.FailNow.
	ErrGoexit = errors.New("lifecycle: task called runtime.Goexit")

	errNilCloser = errors.New("lifecycle: nil io.Closer")
)

// TaskError is the error of a named task or child scope.
type TaskError struct {
	Name string // name given to Go or Child
	Err  error  // error returned by the task, *PanicError, or a child's result
}

func (e *TaskError) Error() string {
	return "lifecycle: " + strconv.Quote(e.Name) + ": " + e.Err.Error()
}

func (e *TaskError) Unwrap() error { return e.Err }

// PanicError is a panic recovered from a task or a Closer.
type PanicError struct {
	Value any    // value passed to panic
	Stack []byte // stack of the panicking goroutine, from runtime/debug.Stack
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v\n\n%s", e.Value, e.Stack)
}

// Unwrap returns the panic value if it is an error.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
