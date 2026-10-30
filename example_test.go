package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/djylb/lifecycle"
)

type resource string

func (r resource) Close() error { fmt.Println("close", r); return nil }

func ExampleScope() {
	s := lifecycle.New(context.Background(), lifecycle.CancelOnError())
	if _, err := s.Add(resource("database")); err != nil {
		panic(err) // only fails on a closed scope
	}
	if _, err := s.Add(resource("listener")); err != nil {
		panic(err)
	}
	// Go fails only on a closed scope, so a new scope needs no check.
	_ = s.Go("worker", func(ctx context.Context) error {
		<-ctx.Done() // runs until the scope closes
		return nil
	})
	_ = s.Go("job", func(context.Context) error {
		return errors.New("job failed") // closes the scope
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fmt.Println(s.Wait(ctx))
	// Output:
	// close listener
	// close database
	// lifecycle: "job": job failed
}

func ExampleScope_Child() {
	root := lifecycle.New(context.Background(), lifecycle.Isolate(func(err error) {
		fmt.Println("reported:", err)
	}))
	conn, _ := root.Child("conn-1", lifecycle.CancelOnError().Propagate())
	if _, err := conn.Add(resource("conn-1 socket")); err != nil {
		panic(err)
	}
	_ = conn.Go("read", func(ctx context.Context) error { return io.ErrUnexpectedEOF })

	<-conn.Finished() // the connection failed alone; root stays open

	root.Close(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fmt.Println(root.Wait(ctx))
	// Output:
	// close conn-1 socket
	// reported: lifecycle: "conn-1": lifecycle: "read": unexpected EOF
	// <nil>
}

func ExampleRun() {
	var mu sync.Mutex
	total := 0
	err := lifecycle.Run(context.Background(), func(s *lifecycle.Scope) error {
		for _, n := range []int{1, 2, 3} {
			if err := s.Go(fmt.Sprint("add-", n), func(context.Context) error {
				mu.Lock()
				total += n
				mu.Unlock()
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	fmt.Println(total, err)
	// Output: 6 <nil>
}

func ExampleQueue() {
	q, err := lifecycle.NewQueue(lifecycle.QueueConfig[string]{
		Capacity: 2,
		Full:     lifecycle.DropOldest,
		OnDrop:   func(v string) { fmt.Println("dropped", v) },
	})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	for _, v := range []string{"a", "b", "c"} {
		if err := q.Push(ctx, v); err != nil {
			panic(err) // DropOldest never refuses an open queue
		}
	}
	_ = q.Close() // a Queue's Close always returns nil
	for {
		v, err := q.Pop(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println("popped", v)
	}
	// Output:
	// dropped a
	// popped b
	// popped c
	// lifecycle: queue closed
}
