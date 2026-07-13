package log

import (
	"encoding/binary"
	"sync"
)

type offsetKey struct {
	group     string
	partition int
}

type OffsetStore struct {
	log *Log
	mu  sync.Mutex
	m   map[offsetKey]int64
}

func NewOffsetStore(path string) (*OffsetStore, error) {
	l, err := NewLog(path, 1)
	if err != nil {
		return nil, err
	}

	m := make(map[offsetKey]int64)

	var offset int64 = 0
	for {
		payload, err := l.Read(offset)
		if err != nil {
			break
		}

		groupLen := binary.BigEndian.Uint32(payload[0:4])
		group := string(payload[4 : 4+groupLen])
		partition := int(payload[4+groupLen])
		committedOffset := int64(binary.BigEndian.Uint64(payload[4+groupLen+1:]))

		m[offsetKey{group: group, partition: partition}] = committedOffset

		offset += 4 + int64(len(payload))
	}

	return &OffsetStore{log: l, m: m}, nil
}

func (s *OffsetStore) GetCommitted(group string, partition int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.m[offsetKey{group: group, partition: partition}]
}

func (s *OffsetStore) Commit(group string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(len(group)))
	payload = append(payload, group...)
	payload = append(payload, byte(partition))

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	payload = append(payload, buf...)

	_, err := s.log.Append(payload)
	if err != nil {
		return err
	}

	s.m[offsetKey{group: group, partition: partition}] = offset
	return nil
}
