package master

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
)

func TestCoordinatorReassignsExpiredTask(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(input, []byte("alpha beta alpha\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := NewCoordinator(Config{
		StorageRoot: filepath.Join(dir, "data"),
		TaskLease:   time.Millisecond,
	}, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	submitted, err := c.SubmitJob(context.Background(), &rpc.SubmitJobRequest{InputPath: input, NumReducers: 1, ChunkSizeBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.RequestTask(context.Background(), &rpc.RequestTaskRequest{WorkerID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if first.TaskType != rpc.TaskTypeMap {
		t.Fatalf("expected map task, got %s", first.TaskType)
	}
	time.Sleep(2 * time.Millisecond)
	second, err := c.RequestTask(context.Background(), &rpc.RequestTaskRequest{WorkerID: "worker-b"})
	if err != nil {
		t.Fatal(err)
	}
	if second.TaskID != first.TaskID {
		t.Fatalf("expected expired task %q to be reassigned, got %q for job %s", first.TaskID, second.TaskID, submitted.JobID)
	}
}
