package log

import (
	"path/filepath"
	"testing"
)

func TestOffsetCommitRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets")
	s, err := NewOffsetStore(path)
	if err != nil {
		t.Fatalf("failed to make new offset store: %v", err)
	}

	// 커밋 몇 개
	s.Commit("analytics", 0, 100)
	s.Commit("payment", 0, 50)
	s.Commit("analytics", 0, 150) // ← analytics/0 재커밋 (최신)

	// 재시작 시뮬: 같은 path로 새 store
	s2, err := NewOffsetStore(path)
	if err != nil {
		t.Fatalf("faild to replay offset store: %v", err)
	}

	// 검증
	offset := s2.GetCommitted("analytics", 0) // 는 150 이어야 (last wins)
	if offset != 150 {
		t.Fatalf("data mismatch")
	}
	offset = s2.GetCommitted("payment", 0) // 는 50
	if offset != 50 {
		t.Fatalf("data mismatch")
	}
	offset = s2.GetCommitted("unknown", 9) // 는 0  (커밋 없음)
	if offset != 0 {
		t.Fatalf("data mismatch")
	}
}
