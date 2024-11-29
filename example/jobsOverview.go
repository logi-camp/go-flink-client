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

	// jobs overview test
	jobs, err := c.JobsOverview()
	if err != nil {
		panic(err)
	}
	fmt.Println(jobs)
}
