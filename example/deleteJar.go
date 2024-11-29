package main

import (
	"os"

	"github.com/logi-camp/go-flink-client"
)

func main() {
	c, err := api.New(os.Getenv("FLINK_API"))
	if err != nil {
		panic(err)
	}

	// delete test
	err = c.DeleteJar("efb8367e-aa0d-4ceb-957f-0e8f46ed4b10_test.jar")
	if err != nil {
		panic(err)
	}
}
