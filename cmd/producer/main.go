package main

import (
	"flag"
	"fmt"
	"minikafka/internal/client"
)

func main() {
	key := flag.String("key", "user-1", "파티션 라우팅 키")
	payload := flag.String("payload", "hello", "메시지 내용")
	flag.Parse()

	c, err := client.NewClient("localhost:9092")
	if err != nil {
		fmt.Println("client connection failed: ", err)
		return
	}

	partition, offset, err := c.Produce([]byte(*key), []byte(*payload))
	if err != nil {
		fmt.Println("failed to produce: ", err)
		return
	}

	fmt.Printf("produced → partition %d, offset %d\n", partition, offset)
}
