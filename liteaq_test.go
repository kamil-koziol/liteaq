package liteaq_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kamil-koziol/liteaq"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")

	conn, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
		os.RemoveAll(dbPath)
	})

	err = liteaq.Setup(t.Context(), conn)
	if err != nil {
		t.Fatalf("unable to setup the database: %v", err)
	}

	return conn
}

func startServer(t *testing.T, server *liteaq.Server, mux *liteaq.ServeMux) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		defer close(done)
		server.Run(ctx, mux)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func TestAll(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	client := liteaq.NewClient(db)
	server := liteaq.NewServer(db)

	mux := liteaq.NewServeMux()

	recvChan := make(chan *liteaq.JobDetails)
	mux.HandleFunc("email:welcome", func(ctx context.Context, task *liteaq.JobDetails) error {
		recvChan <- task
		return nil
	})

	startServer(t, server, mux)

	task := liteaq.NewJob("email:welcome", []byte("Hello world!"))
	_, err := client.Enqueue(t.Context(), task)
	if err != nil {
		t.Fatalf("unable to enqueue task: %v", err)
	}

	receivedTask := <-recvChan
	if receivedTask.Type != task.Type {
		t.Errorf("inavlid task type, expected: %s, got: %s", task.Type, receivedTask.Type)
	}

	if !bytes.Equal(receivedTask.Payload, task.Payload) {
		t.Errorf("inavlid task type, expected: %s, got: %s", task.Payload, receivedTask.Payload)
	}
}

func TestEmptyQueueDoesNotCrash(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	server := liteaq.NewServer(db)
	mux := liteaq.NewServeMux()

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	go server.Run(ctx, mux)
}

func TestMultipleTaskTypes(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	client := liteaq.NewClient(db)
	server := liteaq.NewServer(db)
	mux := liteaq.NewServeMux()

	emailCh := make(chan string, 1)
	logCh := make(chan string, 1)

	mux.HandleFunc("email", func(ctx context.Context, task *liteaq.JobDetails) error {
		emailCh <- "email:" + string(task.Payload)
		return nil
	})

	mux.HandleFunc("log", func(ctx context.Context, task *liteaq.JobDetails) error {
		logCh <- "log:" + string(task.Payload)
		return nil
	})

	startServer(t, server, mux)

	client.Enqueue(t.Context(), liteaq.NewJob("email", []byte("A")))
	client.Enqueue(t.Context(), liteaq.NewJob("log", []byte("B")))

	if got := <-emailCh; got != "email:A" {
		t.Fatalf("email handler failed: got %s", got)
	}
	if got := <-logCh; got != "log:B" {
		t.Fatalf("log handler failed: got %s", got)
	}

}

func TestConcurrentEnqueue(t *testing.T) {
	t.Parallel()

	const n = 50

	db := setupTestDB(t)

	client := liteaq.NewClient(db)
	server := liteaq.NewServer(db)
	mux := liteaq.NewServeMux()

	recv := make(chan string, n)

	mux.HandleFunc("task", func(ctx context.Context, task *liteaq.JobDetails) error {
		recv <- string(task.Payload)
		return nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go server.Run(ctx, mux)

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Go(func() {
			_, err := client.Enqueue(ctx, liteaq.NewJob("task", fmt.Appendf(nil, "%d", i)))
			if err != nil {
				errs <- fmt.Errorf("enqueue %d: %w", i, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	seen := make(map[string]bool, n)
	for len(seen) < n {
		select {
		case p := <-recv:
			if seen[p] {
				t.Fatalf("task %s delivered twice", p)
			}
			seen[p] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("stalled: only got %d/%d tasks", len(seen), n)
		}
	}
}
