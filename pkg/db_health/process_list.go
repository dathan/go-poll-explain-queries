package db_health

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dathan/go-poll-explain-queries/pkg/db_conn"
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

// poll the processList and explain bad queries
func PollProcessList(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	db := db_conn.GetDB()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Quitting... processlist")
			return
		default:
			// side effect and look at locks when a threshold is met each loop it resets
			var hitCounter = 0
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
					hitCounter++
					if strings.Contains(process.Info.String, "SHOW ") {
						continue
					}
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

			if hitCounter > 7 {
				wg.Add(1)
				go detectLocksDo(db)
			}

			time.Sleep(1 * time.Second)
		}
	}
}
