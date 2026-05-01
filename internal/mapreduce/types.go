package mapreduce

import (
	"strings"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/storage"
)

type KeyValue = storage.KeyValue

type MapFunc func(filename string, contents string) []KeyValue
type ReduceFunc func(key string, values []string) string

func WordCountMap(_ string, contents string) []KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '\'')
	})
	pairs := make([]KeyValue, 0, len(words))
	for _, word := range words {
		word = strings.ToLower(strings.Trim(word, "'"))
		if word != "" {
			pairs = append(pairs, KeyValue{Key: word, Value: "1"})
		}
	}
	return pairs
}

func WordCountReduce(_ string, values []string) string {
	return strconvItoa(len(values))
}

func strconvItoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}
