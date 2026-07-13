package main

import (
	"flag"
	"fmt"
	"minikafka/internal/client"
	"sort"
	"time"
)

func main() {
	events := flag.Int("events", 30000, "produce 건수")
	keys := flag.Int("keys", 100, "키 종류 수")
	flag.Parse()

	c, err := client.NewClient("localhost:9092")
	if err != nil {
		fmt.Println("Client connection failed: ", err)
		return
	}

	start := time.Now() // 측정 시작

	latencies := make([]time.Duration, *events)
	counts := map[int]int{} // 파티션별 카운트
	for i := 0; i < *events; i++ {
		key := []byte(fmt.Sprintf("user-%d", i%*keys)) // 100가지 키 순환

		t0 := time.Now()
		partition, _, err := c.Produce(key, []byte("event"))
		latencies[i] = time.Since(t0)

		if err != nil {
			fmt.Println("failed to produce event: ", err)
		}
		counts[partition]++ // 어느 파티션에 갔나 집계
	}

	elapsed := time.Since(start) // 측정 종료

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	fmt.Println("분포:", counts) // 예: map[0:1002 1:998 2:1000]
	fmt.Printf("%d건 | %.0f건/초 | p50=%v p99=%v\n",
		*events, float64(*events)/elapsed.Seconds(), p50, p99)
}
