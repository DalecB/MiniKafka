package log

import "testing"

func TestPartitioning(t *testing.T) {
	top, err := NewTopic(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to generate new topic: %v", err)
	}

	// ① 같은 키 → 같은 파티션
	key := []byte("user-1")
	p1, off1, _ := top.Append(key, []byte("first"))
	p2, off2, _ := top.Append(key, []byte("second"))
	if p1 != p2 {
		t.Fatalf("partitioning failed on same user p1 != p2")
	}

	// ② 키 단위 순서 — 같은 파티션에서 읽어서 확인
	got1, _ := top.Read(p1, off1) // "first"여야
	got2, _ := top.Read(p2, off2) // "second"여야
	if string(got1) != "first" || string(got2) != "second" {
		t.Fatalf("read data mismatch")
	}
}
