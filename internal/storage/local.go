package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type LocalStore struct {
	root string
}

func NewLocalStore(root string) *LocalStore {
	return &LocalStore{root: root}
}

func (s *LocalStore) Root() string {
	return s.root
}

func (s *LocalStore) SplitInput(jobID, inputPath string, chunkSizeBytes int64) ([]string, error) {
	if chunkSizeBytes <= 0 {
		chunkSizeBytes = 512 * 1024
	}
	file, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	chunksDir := filepath.Join(s.root, jobID, "chunks")
	if err := os.MkdirAll(chunksDir, 0o755); err != nil {
		return nil, err
	}

	var paths []string
	var builder strings.Builder
	var chunkID int
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text() + "\n"
		if builder.Len() > 0 && int64(builder.Len()+len(line)) > chunkSizeBytes {
			path, err := writeChunk(chunksDir, chunkID, builder.String())
			if err != nil {
				return nil, err
			}
			paths = append(paths, path)
			chunkID++
			builder.Reset()
		}
		builder.WriteString(line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if builder.Len() > 0 || len(paths) == 0 {
		path, err := writeChunk(chunksDir, chunkID, builder.String())
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func writeChunk(dir string, id int, contents string) (string, error) {
	path := filepath.Join(dir, fmt.Sprintf("chunk-%05d.txt", id))
	return path, os.WriteFile(path, []byte(contents), 0o644)
}

func (s *LocalStore) WriteIntermediate(jobID string, mapID, reduceID int, pairs []KeyValue) (string, error) {
	dir := filepath.Join(s.root, jobID, "intermediate")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})
	path := filepath.Join(dir, fmt.Sprintf("map-%05d-reduce-%05d.jsonl", mapID, reduceID))
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, pair := range pairs {
		if err := encoder.Encode(pair); err != nil {
			return "", err
		}
	}
	return path, nil
}

func (s *LocalStore) ReadAndGroupIntermediate(paths []string) (map[string][]string, error) {
	grouped := make(map[string][]string)
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(file)
		for {
			var pair KeyValue
			if err := decoder.Decode(&pair); err != nil {
				if err == io.EOF {
					break
				}
				_ = file.Close()
				return nil, err
			}
			grouped[pair.Key] = append(grouped[pair.Key], pair.Value)
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	}
	return grouped, nil
}

func (s *LocalStore) WriteReduceOutput(_ string, reduceID int, outputDir string, pairs []KeyValue) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", err
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})
	path := filepath.Join(outputDir, fmt.Sprintf("part-%05d.txt", reduceID))
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, pair := range pairs {
		if _, err := fmt.Fprintf(writer, "%s\t%s\n", pair.Key, pair.Value); err != nil {
			return "", err
		}
	}
	return path, writer.Flush()
}
