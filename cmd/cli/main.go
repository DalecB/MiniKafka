package main

import (
	"fmt"
	"minikafka/internal/client"
)

func main() {
	c, err := client.NewClient("localhost:9092")
	if err != nil {
		fmt.Println("Client connection failed: ", err)
		return
	}

	key := []byte("user-1")

	partition, off, err := c.Produce(key, []byte("hello world from client!"))
	if err != nil {
		fmt.Println("Failed to produce log: ", err)
		return
	}

	got, err := c.Fetch(partition, off)
	if err != nil {
		fmt.Println("Failed to fetch log: ", err)
		return
	}

	fmt.Println("Got the log: ", string(got))
}
