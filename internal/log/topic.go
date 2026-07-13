package log

import (
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
)

type Topic struct {
	partitions []*Log
}

func NewTopic(baseDir string, fsyncN int) (*Topic, error) {
	const N = 3
	parts := make([]*Log, N)

	for i := 0; i < N; i++ {
		folder := filepath.Join(baseDir, strconv.Itoa(i))
		err := os.MkdirAll(folder, 0755)
		if err != nil {
			return nil, err
		}

		nl, err := NewLog(filepath.Join(folder, "log"), fsyncN)
		if err != nil {
			return nil, err
		}

		parts[i] = nl
	}

	return &Topic{partitions: parts}, nil
}

func (t *Topic) partitionFor(key []byte) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % len(t.partitions)
}

func (t *Topic) Append(key []byte, payload []byte) (int, int64, error) {
	p := t.partitionFor(key)
	offset, err := t.partitions[p].Append(payload)
	if err != nil {
		return 0, 0, err
	}

	return p, offset, nil
}

func (t *Topic) Read(partition int, offset int64) ([]byte, error) {
	if partition < 0 || partition >= len(t.partitions) {
		return nil, errors.New("invalid partition")
	}

	payload, err := t.partitions[partition].Read(offset)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
