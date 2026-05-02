package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/master"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
	workerpkg "github.com/tauqxxr7/mini-mapreduce-engine/internal/worker"
	"google.golang.org/grpc"
)

func TestInProcessClusterWordCount(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(input, []byte("go go map reduce\ngo systems\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	coord := master.NewCoordinator(master.Config{
		StorageRoot: filepath.Join(dir, "data"),
		TaskLease:   time.Second,
	}, logger)
	client := localClient{server: coord}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		w := workerpkg.New(client, workerpkg.Config{
			ID:           fmt.Sprintf("worker-test-%d", i),
			StorageRoot:  filepath.Join(dir, "data"),
			PollInterval: 10 * time.Millisecond,
		}, logger)
		go func() { _ = w.Run(ctx) }()
	}

	outputDir := filepath.Join(dir, "out")
	submitted, err := coord.SubmitJob(ctx, &rpc.SubmitJobRequest{
		InputPath:      input,
		OutputPath:     outputDir,
		NumReducers:    2,
		ChunkSizeBytes: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForCompletion(t, coord, submitted.JobID)
	counts := readCounts(t, outputDir)
	assertCount(t, counts, "go", 3)
	assertCount(t, counts, "map", 1)
	assertCount(t, counts, "reduce", 1)
	assertCount(t, counts, "systems", 1)
}

func TestExampleInputMatchesExpectedOutput(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	coord, cancel := startInProcessCluster(t, filepath.Join(dir, "data"))
	defer cancel()

	submitted, err := coord.SubmitJob(context.Background(), &rpc.SubmitJobRequest{
		InputPath:      filepath.FromSlash("../examples/large.txt"),
		OutputPath:     outputDir,
		NumReducers:    3,
		ChunkSizeBytes: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForCompletion(t, coord, submitted.JobID)

	actual := readSortedOutput(t, outputDir)
	expectedData, err := os.ReadFile(filepath.FromSlash("../examples/expected-output.txt"))
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.Split(strings.TrimSpace(string(expectedData)), "\n")
	if len(actual) != len(expected) {
		t.Fatalf("output line count = %d, want %d\nactual=%v\nexpected=%v", len(actual), len(expected), actual, expected)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("output line %d = %q, want %q", i, actual[i], expected[i])
		}
	}
}

func startInProcessCluster(t *testing.T, storageRoot string) (*master.Coordinator, context.CancelFunc) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	coord := master.NewCoordinator(master.Config{
		StorageRoot: storageRoot,
		TaskLease:   time.Second,
	}, logger)
	client := localClient{server: coord}
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 3; i++ {
		w := workerpkg.New(client, workerpkg.Config{
			ID:           fmt.Sprintf("example-worker-%d", i),
			StorageRoot:  storageRoot,
			PollInterval: 10 * time.Millisecond,
		}, logger)
		go func() { _ = w.Run(ctx) }()
	}
	return coord, cancel
}

func waitForCompletion(t *testing.T, coord *master.Coordinator, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := coord.GetJobStatus(context.Background(), &rpc.GetJobStatusRequest{JobID: jobID})
		if err != nil {
			t.Fatal(err)
		}
		if status.Status == rpc.JobStatusCompleted {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("job did not complete")
}

type localClient struct {
	server *master.Coordinator
}

func (c localClient) SubmitJob(ctx context.Context, r *rpc.SubmitJobRequest, _ ...grpc.CallOption) (*rpc.SubmitJobResponse, error) {
	return c.server.SubmitJob(ctx, r)
}

func (c localClient) RegisterWorker(ctx context.Context, r *rpc.RegisterWorkerRequest, _ ...grpc.CallOption) (*rpc.RegisterWorkerResponse, error) {
	return c.server.RegisterWorker(ctx, r)
}

func (c localClient) RequestTask(ctx context.Context, r *rpc.RequestTaskRequest, _ ...grpc.CallOption) (*rpc.RequestTaskResponse, error) {
	return c.server.RequestTask(ctx, r)
}

func (c localClient) SubmitTaskResult(ctx context.Context, r *rpc.SubmitTaskResultRequest, _ ...grpc.CallOption) (*rpc.SubmitTaskResultResponse, error) {
	return c.server.SubmitTaskResult(ctx, r)
}

func (c localClient) Heartbeat(ctx context.Context, r *rpc.HeartbeatRequest, _ ...grpc.CallOption) (*rpc.HeartbeatResponse, error) {
	return c.server.Heartbeat(ctx, r)
}

func (c localClient) GetJobStatus(ctx context.Context, r *rpc.GetJobStatusRequest, _ ...grpc.CallOption) (*rpc.GetJobStatusResponse, error) {
	return c.server.GetJobStatus(ctx, r)
}

func readCounts(t *testing.T, outputDir string) map[string]int {
	t.Helper()
	lines := readSortedOutput(t, outputDir)
	counts := make(map[string]int)
	for _, line := range lines {
		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			t.Fatalf("invalid output line %q", line)
		}
		count, err := strconv.Atoi(fields[1])
		if err != nil {
			t.Fatal(err)
		}
		counts[fields[0]] = count
	}
	return counts
}

func readSortedOutput(t *testing.T, outputDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "part-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(outputDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	return lines
}

func assertCount(t *testing.T, counts map[string]int, key string, want int) {
	t.Helper()
	if got := counts[key]; got != want {
		t.Fatalf("count[%s] = %d, want %d", key, got, want)
	}
}
