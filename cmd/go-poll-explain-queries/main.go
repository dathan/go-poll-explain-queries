package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/dathan/go-poll-explain-queries/pkg/db_health"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Set up a channel to listen for the interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Listen for the interrupt signal, and when it's received, cancel the context
		<-c
		cancel()
	}()

	go db_health.PollProcessList(ctx, &wg)

	// Block the main thread until an interrupt signal is received
	wg.Wait()
	// Both goroutines have finished, now we can exit
	fmt.Println("Both goroutines have shut down, exiting...")
	os.Exit(0)
}
