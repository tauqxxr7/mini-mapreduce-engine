package mapreduce

import (
	"os"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/storage"
)

type Engine struct {
	store *storage.LocalStore
	mapFn MapFunc
	redFn ReduceFunc
}

func NewEngine(store *storage.LocalStore, mapFn MapFunc, reduceFn ReduceFunc) *Engine {
	return &Engine{store: store, mapFn: mapFn, redFn: reduceFn}
}

func (e *Engine) RunMap(jobID string, mapID int, inputPath string, reducers int) ([]string, error) {
	content, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	partitions := make([][]KeyValue, reducers)
	for _, pair := range e.mapFn(inputPath, string(content)) {
		bucket := Partition(pair.Key, reducers)
		partitions[bucket] = append(partitions[bucket], pair)
	}
	paths := make([]string, 0, reducers)
	for reduceID, pairs := range partitions {
		path, err := e.store.WriteIntermediate(jobID, mapID, reduceID, pairs)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func (e *Engine) RunReduce(jobID string, reduceID int, inputPaths []string, outputDir string) (string, error) {
	grouped, err := e.store.ReadAndGroupIntermediate(inputPaths)
	if err != nil {
		return "", err
	}
	results := make([]KeyValue, 0, len(grouped))
	for key, values := range grouped {
		results = append(results, KeyValue{Key: key, Value: e.redFn(key, values)})
	}
	return e.store.WriteReduceOutput(jobID, reduceID, outputDir, results)
}
