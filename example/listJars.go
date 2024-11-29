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

	// upload test
	resp, err := c.Jars()
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
}
