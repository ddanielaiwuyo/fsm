package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"time"
)

func main() {
	cluster, err := parseConfig("")
	if err != nil {
		fmt.Println(err)
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	go func() {
		for t := range ticker.C {
			_ = t
			fmt.Printf("\n\nGOROUTINES::::: %d\n\n", runtime.NumGoroutine())
		}
	}()

	go func() {
		if err := http.ListenAndServe("localhost:6061", nil); err != nil {
			fmt.Println("could not start ptrace server::", err)
		}
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cluster.Start(ctx)
}
