package main

import (
	"fmt"
	"os"

	"github.com/logi-camp/go-flink-client"
)

func main() {
	c, err := api.New(os.Getenv("FLINK_API"))
	if err != nil {
		panic(err)
	}

	// plan test
	resp, err := c.PlanJar("8c0c2226-b532-4d9b-b698-8aa649694bb9_test.jar")
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
}
