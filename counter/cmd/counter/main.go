package main

import (
	"log"
	"os"

	"github.com/panagiotisptr/dist-sys-challenge/counter/counter"
)

func main() {
	n := counter.NewCounterNode()

	if err := n.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
