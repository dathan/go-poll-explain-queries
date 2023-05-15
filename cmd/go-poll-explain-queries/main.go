package main

import (
	"context"
	"flag"
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

	// take in inputs
	kill := flag.Bool("kill", false, "kill any slow query that by passes the slowis threshold")
	slowis := flag.Int("slowis", 10, "slowis the threshold in seconds that a query needs to take for it to be considered slow")
	lockThreshold := flag.Int("lock_threshold", 7, "lock_threshold is the number of processes that are slow before we issue a query to determine what is locking") // set to 0 to disable

	flag.Parse()

	poll := db_health.NewHealth(ctx, &wg, *slowis, *lockThreshold, *kill)
	go poll.PollProcessList()

	// Block the main thread until an interrupt signal is received
	wg.Wait()
	// Both goroutines have finished, now we can exit
	fmt.Println("Both goroutines have shut down, exiting...")
	os.Exit(0)
}
