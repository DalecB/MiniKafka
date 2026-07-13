package log

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type Log struct {
	logFile    *os.File
	mu         sync.Mutex
	fsyncN     int
	writeCount int
}

func NewLog(path string, fsyncN int) (*Log, error) {
	f, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &Log{logFile: f, fsyncN: fsyncN}, nil
}

func (l *Log) Append(payload []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, err := l.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(payload)))

	record := append(buf, payload...)
	_, err = l.logFile.Write(record)
	if err != nil {
		return 0, err
	}

	l.writeCount++
	if l.fsyncN > 0 && l.writeCount%l.fsyncN == 0 {
		err = l.logFile.Sync()
		if err != nil {
			return 0, err
		}
	}

	return offset, nil
}

func (l *Log) Read(offset int64) ([]byte, error) {
	head := make([]byte, 4)

	_, err := l.logFile.ReadAt(head, offset)
	if err != nil {
		return nil, err
	}

	N := binary.BigEndian.Uint32(head)
	payload := make([]byte, N)

	_, err = l.logFile.ReadAt(payload, offset+4)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
