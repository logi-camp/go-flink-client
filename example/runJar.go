package main

import (
	"fmt"
	"os"

	api "github.com/logi-camp/go-flink-client"
)

func main() {
	c, err := api.New(os.Getenv("FLINK_API"))
	if err != nil {
		panic(err)
	}

	opts := api.RunOpts{
		JarID: "8c0c2226-b532-4d9b-b698-8aa649694bb9_test.jar",
	}
	// run test
	resp, err := c.RunJar(opts)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
}
