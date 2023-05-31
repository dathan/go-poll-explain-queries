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

type LongRunningTrx struct {
	Process
	Trx_id               int
	Trx_started          sql.NullString
	Trx_mysql_thread_id  int
	Trx_rows_modified    int
	Trx_duration_seconds int
}

type Config struct {
	ctx           context.Context
	wg            *sync.WaitGroup
	SlowThreshold int
	RowsThreshold int
	KillSlow      bool
	Batch         bool
}

func NewHealth(ctx context.Context, wg *sync.WaitGroup, slow int, rows int, kill bool, batchMode bool) *Config {
	return &Config{
		ctx, wg, slow, rows, kill, batchMode,
	}
}

func (c *Config) PollProcessAndLongRunningTrx() {
	defer c.wg.Done()
	db := db_conn.GetDB()
	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("Quitting... processlist")
			return
		default:
			// side effect and look at locks when a threshold is met each loop it resets
			var hitCounter = 0
			// Poll the processlist
			query := fmt.Sprintf("SELECT proc.*, trx.trx_id, trx.trx_started, trx.trx_mysql_thread_id, trx.trx_rows_modified, TIMESTAMPDIFF(SECOND, trx.trx_started, NOW()) AS trx_duration_seconds FROM  information_schema.innodb_trx AS trx JOIN information_schema.processlist AS proc ON     trx.trx_mysql_thread_id = proc.ID WHERE trx.trx_started  > NOW() - INTERVAL %d SECOND AND trx.trx_rows_modified > %d ORDER BY  trx.trx_started ASC, trx.trx_rows_modified DESC LIMIT %d", c.SlowThreshold, c.RowsThreshold, 100)
			rows, err := db.Query(query)
			if err != nil {
				panic(err)
			}

			for rows.Next() {
				var process LongRunningTrx
				err := rows.Scan(&process.Id, &process.User, &process.Host, &process.Db, &process.Command, &process.Time, &process.State, &process.Info, &process.Trx_id, &process.Trx_started, &process.Trx_mysql_thread_id, &process.Trx_rows_modified, &process.Trx_duration_seconds)

				if err != nil {
					panic(err)
				}

				// If the query has been running for more than 2 seconds, run EXPLAIN
				if process.Trx_duration_seconds > c.SlowThreshold && process.Info.Valid {
					hitCounter++
					if strings.Contains(process.Info.String, "SHOW ") {
						continue
					}

					utils.PrettyPrint(process)
					if process.Info.Valid {
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

					if c.KillSlow {
						_, err = db.Exec(fmt.Sprintf("KILL %s", process.Id))
						if err != nil {
							panic(err)
						}
						utils.PrettyPrint(process)
					}
				}
			}
			/*
				if hitCounter > c.DetermineLocksAt {
					c.wg.Add(1)
					go detectLocksDo(c.ctx, c.wg, db)
				}
			*/

			if c.Batch {
				fmt.Printf("Exiting...")
				return
			}

			time.Sleep(1 * time.Second)
		}
	}
}

// poll the processList and explain bad queries
func (c *Config) PollProcessList() {
	defer c.wg.Done()
	db := db_conn.GetDB()
	for {
		select {
		case <-c.ctx.Done():
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
				if process.Time > c.SlowThreshold && process.Info.Valid {
					hitCounter++
					if strings.Contains(process.Info.String, "SHOW ") {
						continue
					}

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
					}

					if c.KillSlow {
						_, err = db.Exec(fmt.Sprintf("KILL %s", process.Id))
						if err != nil {
							panic(err)
						}
						utils.PrettyPrint(process)
					}
				}
			}
			/*
				if hitCounter > c.DetermineLocksAt {
					c.wg.Add(1)
					go detectLocksDo(c.ctx, c.wg, db)
				}
			*/

			if c.Batch {
				fmt.Printf("Exiting...")
				return
			}

			time.Sleep(1 * time.Second)
		}
	}
}
