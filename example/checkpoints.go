package main

import (
	"fmt"
	"github.com/logi-camp/go-flink-client"
	"os"
)

func main() {
	c, err := api.New(os.Getenv("FLINK_API"))
	if err != nil {
		panic(err)
	}

	// Checkpoints test
	v, err := c.Checkpoints("2bd452ba193d1575a4acc9ed09f896ea")
	if err != nil {
		panic(err)
	}
	fmt.Println(v)
}
