package db_conn

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db   *sql.DB
	once sync.Once
)

// this is an ondemand singleton
func setupDB() error {
	var err error
	once.Do(func() {
		var (
			DBUser     = os.Getenv("MYSQL_USERNAME")
			DBPassword = os.Getenv("MYSQL_PASSWORD")
			DBName     = os.Getenv("MYSQL_DATABASE")
			DBHost     = os.Getenv("MYSQL_HOST")
		)

		dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", DBUser, DBPassword, DBHost, DBName)

		// Connect to the MySQL server
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return
		}

		err = db.Ping()
		if err != nil {
			db = nil
		}

	})

	return err
}

func GetDB() *sql.DB {
	err := setupDB()
	if err != nil {
		panic(err)
	}
	return db
}
