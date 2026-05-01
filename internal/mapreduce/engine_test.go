package mapreduce

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/storage"
)

func TestWordCountPipeline(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(input, []byte("Hello world\nhello systems\nworld\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := storage.NewLocalStore(filepath.Join(dir, "data"))
	engine := NewEngine(store, WordCountMap, WordCountReduce)

	intermediate, err := engine.RunMap("job-test", 0, input, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(intermediate) != 2 {
		t.Fatalf("expected 2 intermediate partitions, got %d", len(intermediate))
	}

	output, err := engine.RunReduce("job-test", 0, []string{intermediate[0]}, filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionIsStable(t *testing.T) {
	first := Partition("mapreduce", 5)
	for i := 0; i < 10; i++ {
		if got := Partition("mapreduce", 5); got != first {
			t.Fatalf("partition changed: got %d want %d", got, first)
		}
	}
}
