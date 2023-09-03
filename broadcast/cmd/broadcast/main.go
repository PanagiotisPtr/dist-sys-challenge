package main

import (
	"log"
	"os"

	"github.com/panagiotisptr/dist-sys-challenge/broadcast/broadcast"
)

func main() {
	n := broadcast.NewBroadcastNode()

	if err := n.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
