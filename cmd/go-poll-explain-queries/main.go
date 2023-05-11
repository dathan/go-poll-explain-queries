package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/dathan/go-poll-explain-queries/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
)

type Process struct {
	Id      int
	User    sql.NullString
	Host    sql.NullString
	Db      sql.NullString
	Command sql.NullString
	Time    int
	State   sql.NullString
	Info    sql.NullString
}

type Explain struct {
	Id           sql.NullInt64
	SelectType   sql.NullString
	Table        sql.NullString
	Partitions   sql.NullString
	Type         sql.NullString
	PossibleKeys sql.NullString
	Key          sql.NullString
	KeyLen       sql.NullString
	Ref          sql.NullString
	Rows         sql.NullInt64
	Filtered     sql.NullFloat64
	Extra        sql.NullString
}

type QueryResult struct {
	ProcessListId    int
	Name             sql.NullString
	Type             sql.NullString
	ProcessListState sql.NullString
	ProcessListInfo  sql.NullString
	ProcessListTime  int
	Engine           sql.NullString
	EngineLockId     sql.NullString
	ObjectSchema     sql.NullString
	ObjectName       sql.NullString
	LockMode         sql.NullString
	LockStatus       sql.NullString
	WaitingThreads   int
}

func main() {
	// Set up a channel to listen for the interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	var wg sync.WaitGroup
	wg.Add(2) // We have 2 goroutines

	dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_HOST"), os.Getenv("MYSQL_DATABASE"))

	fmt.Printf("Connecting ... to %s\n", dsn)

	// Connect to the MySQL server
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c:
				fmt.Println("Quitting... processlist")
				return
			default:
				// Poll the processlist
				rows, err := db.Query("SHOW FULL PROCESSLIST")
				if err != nil {
					panic(err)
				}

				for rows.Next() {
					var process Process
					err := rows.Scan(&process.Id, &process.User, &process.Host, &process.Db, &process.Command, &process.Time, &process.State, &process.Info)

					if err != nil {
						panic(err)
					}

					// If the query has been running for more than 2 seconds, run EXPLAIN
					if process.Time > 2 && process.Info.Valid {
						utils.PrettyPrint(process)
						query := fmt.Sprintf("EXPLAIN %s", process.Info.String)
						explain, err := db.Query(query)
						if err != nil {
							panic(err)
						}
						for explain.Next() {
							// Print the output of the EXPLAIN query
							var output Explain
							err := explain.Scan(&output.Id, &output.SelectType, &output.Table, &output.Partitions, &output.Type, &output.PossibleKeys, &output.Key, &output.KeyLen, &output.Ref, &output.Rows, &output.Filtered, &output.Extra)
							if err != nil {
								panic(err)
							}
							utils.PrettyPrint(output)
						}
					}
				}

				time.Sleep(1 * time.Second)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c:
				fmt.Println("Quitting... lock detector")
				return
			default:
				// Construct and execute the query
				rows, err := db.Query(`
				SELECT   
					th.PROCESSLIST_ID,   
					th.NAME,   
					th.TYPE,   
					th.PROCESSLIST_STATE,   
					th.PROCESSLIST_INFO,   
					th.PROCESSLIST_TIME,   
					dl.OBJECT_SCHEMA,   
					dl.OBJECT_NAME,   
					dl.LOCK_MODE,   
					dl.LOCK_STATUS,   
					COUNT(dlw.REQUESTING_ENGINE_LOCK_ID) AS WAITING_THREADS 
				FROM   
					performance_schema.threads AS th 
				JOIN   
					performance_schema.data_locks AS dl ON   th.THREAD_ID = dl.THREAD_ID 
				LEFT JOIN   
					performance_schema.data_lock_waits AS dlw ON   dl.ENGINE_LOCK_ID = dlw.BLOCKING_ENGINE_LOCK_ID 
				WHERE   
					dl.LOCK_STATUS = 'GRANTED' AND th.PROCESSLIST_INFO IS NOT NULL
				GROUP BY   
					dl.ENGINE_LOCK_ID
			`)
				if err != nil {
					panic(err)
				}

				// Iterate over the rows
				for rows.Next() {
					var result QueryResult
					err := rows.Scan(
						&result.ProcessListId,
						&result.Name,
						&result.Type,
						&result.ProcessListState,
						&result.ProcessListInfo,
						&result.ProcessListTime,
						&result.ObjectSchema,
						&result.ObjectName,
						&result.LockMode,
						&result.LockStatus,
						&result.WaitingThreads,
					)
					if err != nil {
						panic(err)
					}

					// Print the result if the conditions are met
					if result.ProcessListTime >= 3 || result.WaitingThreads >= 2 {
						utils.PrettyPrint(result)
					}
				}

				// Sleep for 10 seconds before the next iteration
				time.Sleep(10 * time.Second)
			}
		}
	}()

	// Block the main thread until an interrupt signal is received
	<-c
	// We received an interrupt signal, now wait for both goroutines to finish
	wg.Wait()
	// Both goroutines have finished, now we can exit
	fmt.Println("Both goroutines have shut down, exiting...")
	os.Exit(0)
}
