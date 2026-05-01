package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSplitInputPreservesLines(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(input, []byte("alpha beta\ngamma delta\nomega\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewLocalStore(filepath.Join(dir, "data"))
	chunks, err := store.SplitInput("job-test", input, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for _, chunk := range chunks {
		data, err := os.ReadFile(chunk)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 || data[len(data)-1] != '\n' {
			t.Fatalf("chunk %s does not end on a line boundary", chunk)
		}
	}
}
