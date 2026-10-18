package liteaq

import (
	"context"
	"database/sql"
)

// Client is responsible for putting jobs into the SQLite table.
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

func (c *Client) EnqueueTx(ctx context.Context, tx *sql.Tx, job *Job) (*JobDetails, error) {
	return c.querier.Enqueue(ctx, tx, job)
}

func (c *Client) Enqueue(ctx context.Context, job *Job) (*JobDetails, error) {
	return c.querier.Enqueue(ctx, c.db, job)
}
