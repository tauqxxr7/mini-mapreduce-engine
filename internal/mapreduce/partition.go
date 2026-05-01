package mapreduce

import "hash/fnv"

func Partition(key string, reducers int) int {
	if reducers <= 0 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(reducers))
}
