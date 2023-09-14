package main

import (
	"log"
	"os"

	distLog "github.com/panagiotisptr/dist-sys-challenge/log/log"
)

func main() {
	n := distLog.NewLogNode()

	if err := n.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
