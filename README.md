# liteaq

A dead-simple, minimal, durable async job queue for Go, backed entirely by SQLite. It requires only **one database table** and has exactly **0** dependencies.

It was inspired by the great [Asynq](https://github.com/hibiken/asynq) Redis-backed task queue.

## Features

- **Transactional enqueueing** - enqueue inside your app transaction.
- **Lives inside your SQLite database**
- **Driver agnostic**: bring any `database/sql` SQLite driver (tested with [modernc.org/sqlite](https://modernc.org/sqlite) and [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)).
- **At-least-once delivery**: failed tasks are retried up to `max_retries`.
