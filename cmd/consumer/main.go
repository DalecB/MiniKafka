package main

import (
	"flag"
	"fmt"
	"minikafka/internal/client"
	"strconv"
	"strings"
)

func main() {
	partsStr := flag.String("partitions", "0", "담당 파티션 (쉼표 구분)")
	group := flag.String("group", "demo", "컨슈머 그룹")
	flag.Parse()

	c, err := client.NewClient("localhost:9092")
	if err != nil {
		fmt.Println("Client connection failed: ", err)
		return
	}

	for _, p := range strings.Split(*partsStr, ",") { // "0","1"
		partition, _ := strconv.Atoi(p) // string → int
		// 이 partition 소비

		offset, _ := c.GetCommitted(*group, partition) // 재개 지점 (없으면 0)
		for {
			payload, err := c.Fetch(partition, offset)
			if err != nil {
				break // "fetch failed" = 끝 도달 (더 없음)
			}
			fmt.Printf("[P%d @%d] %s\n", partition, offset, string(payload)) // 처리
			offset += 4 + int64(len(payload))                                // ★ 다음 레코드로 전진
			// N건마다 Commit (카운터 두고)
		}
		c.Commit(*group, partition, offset) // 마지막 커밋 (커넥션 살아있음)
	}

}
