# liteaq

A dead-simple, minimal, durable async job queue for Go, backed entirely by SQLite. It requires only **one database table** and has exactly **0** dependencies.

It was inspired by the great [Asynq](https://github.com/hibiken/asynq) Redis-backed task queue.

## Features

- **Transactional enqueueing** - enqueue inside your app transaction.
- **Lives inside your SQLite database**
- **Driver agnostic**: bring any `database/sql` SQLite driver (tested with [modernc.org/sqlite](https://modernc.org/sqlite) and [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)).
- **At-least-once delivery**: failed jobs are retried up to `max_retries`.


## Quickstart

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
	// _ "modernc.org/sqlite" // or any other driver

	"github.com/kamil-koziol/liteaq"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Enable WAL mode and a busy timeout.
	// WAL lets readers and writers work concurrently (e.g. the client enqueueing while the server processes jobs). 
    // SQLite allows only one writer at a time, so the busy timeout makes a second writer wait up to 5s instead of
	// failing immediately with `database is locked`.
	db, err := sql.Open("sqlite3", "file:jobs.db?_busy_timeout=5000&_journal_mode=WAL")
	// modernc.org/sqlite:
	// db, err := sql.Open("sqlite", "file:jobs.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set up the `liteaq_jobs` table (safe to call on every start)
	if err := liteaq.Setup(ctx, db); err != nil {
		log.Fatal(err)
	}

	// Register job handlers
	mux := liteaq.NewServeMux()
	mux.HandleFunc("email:welcome", func(ctx context.Context, job *liteaq.JobDetails) error {
		log.Printf("sending welcome email: %s", job.Payload)
		return nil
	})

	server := liteaq.NewServer(db)

	// Enqueue a job (it is persisted in the database, so it is picked up
	// as soon as a server is running)
	client := liteaq.NewClient(db)
	job := liteaq.NewJob("email:welcome", []byte("Hello world!"))
	if _, err := client.Enqueue(ctx, job); err != nil {
		log.Fatal(err)
	}

	// Run the server (blocks until ctx is cancelled)
	server.Run(ctx, mux)
}
```
