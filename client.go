package liteaq

import (
	"context"
	"database/sql"
)

// Client is responsible for putting tasks into the SQLite table.
type Client struct {
	db      *sql.DB
	querier *Querier
}

func NewClient(db *sql.DB) *Client {
	return &Client{
		db:      db,
		querier: NewQuerier(),
	}
}

func (c *Client) EnqueueTx(ctx context.Context, tx *sql.Tx, task *Job) (*JobDetails, error) {
	return c.querier.Enqueue(ctx, tx, task)
}

func (c *Client) Enqueue(ctx context.Context, task *Job) (*JobDetails, error) {
	return c.querier.Enqueue(ctx, c.db, task)
}
