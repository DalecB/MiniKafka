package log

import (
	"path/filepath"
	"sync"
	"testing"
)

// 함수명이 Test로 시작하고 *testing.T를 받으면 Go가 테스트로 인식한다.
// 실행: go test ./internal/log
func TestAppendRead(t *testing.T) {
	// t.TempDir(): 이 테스트 전용 임시 디렉토리를 만들어 반환. 테스트 끝나면 자동 삭제.
	// filepath.Join: 경로 조각을 OS 규칙에 맞게 이어붙임(맥은 "/", 윈도우는 "\").
	path := filepath.Join(t.TempDir(), "test.log")

	// 같은 package log 안이라 NewLog를 바로 부를 수 있다(임포트 불필요).
	l, err := NewLog(path, 0)
	if err != nil {
		// t.Fatalf: 실패를 기록하고 이 테스트를 즉시 중단. %v는 값을 기본 포맷으로 출력.
		t.Fatalf("NewLog 실패: %v", err)
	}

	// 넣을 메시지들. "카프카"는 UTF-8로 9바이트라, 우리 로그가 문자가 아니라
	// "바이트 길이" 기준으로 동작하는지도 같이 검증된다.
	msgs := []string{"hello", "world", "카프카"}

	// --- 1) 여러 건 Append하고, 각 offset을 모아둔다 ---
	// make([]int64, N): 길이 N짜리 int64 슬라이스를 0으로 채워 생성.
	offsets := make([]int64, len(msgs))
	// range: 인덱스 i와 값 m을 함께 순회(JS의 forEach((m, i)=>...)와 순서 반대).
	for i, m := range msgs {
		off, err := l.Append([]byte(m)) // string -> []byte 변환
		if err != nil {
			t.Fatalf("Append(%q) 실패: %v", m, err) // %q는 따옴표 붙여 출력
		}
		offsets[i] = off
	}

	// --- 2) 각 offset을 Read해서 넣은 것과 바이트가 같은지 확인 ---
	for i, off := range offsets {
		got, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read(%d) 실패: %v", off, err)
		}
		if string(got) != msgs[i] {
			t.Fatalf("offset %d: got %q, want %q", off, got, msgs[i])
		}
	}

	// --- 3) 재시작 복구 시뮬: 같은 파일을 '새' Log로 다시 연다 ---
	// 프로세스를 죽였다 켠 것과 동일한 상황(메모리 상태 없이 파일만 가지고 시작).
	l2, err := NewLog(path, 0)
	if err != nil {
		t.Fatalf("재오픈 실패: %v", err)
	}
	got, err := l2.Read(offsets[0]) // 이전에 넣은 첫 메시지가 여전히 살아있나?
	if err != nil {
		t.Fatalf("재시작 후 Read 실패: %v", err)
	}
	if string(got) != msgs[0] {
		t.Fatalf("재시작 후: got %q, want %q", got, msgs[0])
	}
}

func TestConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	l, err := NewLog(path, 0)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}

	const N = 100
	offsets := make([]int64, N)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off, err := l.Append([]byte("msg"))
			if err != nil {
				t.Errorf("Append failed: %v", err)
				return
			}
			offsets[i] = off
		}()
	}
	wg.Wait()

	seen := make(map[int64]bool)
	for _, off := range offsets {
		if seen[off] == true {
			t.Fatalf("Duplicated offset: %d", off)
		} else {
			seen[off] = true
		}
	}
}
