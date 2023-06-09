package main

import (
	"context"
	"flag"
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
	go func() {
		// Listen for the interrupt signal, and when it's received, cancel the context
		<-c
		cancel()
	}()

	// take in inputs
	kill := flag.Bool("kill", false, "kill any slow query that bypasses the slowis threshold")
	slowis := flag.Int("slowis", 600, "slowis the threshold in seconds that a query needs to take for it to be considered slow")
	rowThreshold := flag.Int("row_threshold", 1000, "row_threshold is the number of rows in ready to rollback state for a long running transaction")
	batchMode := flag.Bool("batch", true, "Only run this application once, do not act a daemon")
	verbose := flag.Bool("verbose", false, "Show some verbose stats")
	flag.Parse()

	poll := db_health.NewHealth(ctx, &wg, *slowis, *rowThreshold, *kill, *batchMode, *verbose)
	wg.Add(1)
	go poll.PollProcessAndLongRunningTrx()
	// add the other type of queries below in a go routine with context
	// wg.Add(1)
	// go poll.PollProcessList()

	// Block the main thread until an interrupt signal is received
	wg.Wait()
	// goroutines have finished, now we can exit
	os.Exit(0)
}
