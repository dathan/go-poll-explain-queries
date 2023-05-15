package db_health

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/dathan/go-poll-explain-queries/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
)

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

func detectLocksDo(db *sql.DB) {

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

}

// detect locks and print them out
func detectLocks(ctx context.Context, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Quitting... lock detector")
			return
		default:
			// Construct and execute the query
			detectLocksDo(db)
			// Sleep for 10 seconds before the next iteration
			time.Sleep(10 * time.Second)
		}
	}
}
