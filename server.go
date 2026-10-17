package liteaq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Server is responsible for pulling tasks out of SQLite and running them.
type Server struct {
	db      *sql.DB
	querier *Querier
}

func NewServer(db *sql.DB) *Server {
	return &Server{
		db:      db,
		querier: NewQuerier(),
	}
}

func (s *Server) executeTask(ctx context.Context, mux *ServeMux, task *JobDetails) error {
	fn, exists := mux.getHandler(task.Type)
	if !exists {
		return fmt.Errorf("no handler for task type %q", task.Type)
	}

	return fn(ctx, task)
}

func (s *Server) Run(ctx context.Context, mux *ServeMux) {
	interval := 200 * time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		task, err := s.querier.Dequeue(ctx, s.db)
		if err != nil {
			timer.Reset(interval)
			continue
		}

		if task == nil {
			timer.Reset(interval)
			continue
		}

		timer.Reset(0)

		wg.Add(1)
		go func(task *JobDetails) {
			defer wg.Done()

			taskCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			log := slog.With("task_id", task.ID)

			if err := s.executeTask(taskCtx, mux, task); err != nil {
				log.ErrorContext(taskCtx, "failed to execute task", "err", err)
				return
			}

			// Allow completion to finish even if the server context is canceled.
			completeCtx, completeCancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				5*time.Second,
			)
			defer completeCancel()

			if _, err := s.querier.Complete(completeCtx, s.db, task.ID); err != nil {
				log.ErrorContext(completeCtx, "failed to complete the task", "err", err)
			}
		}(task)
	}
}
