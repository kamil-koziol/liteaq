//go:build mattn

package liteaq_test

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func openDB(path string) (*sql.DB, error) {
	return sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_journal_mode=WAL")
}
