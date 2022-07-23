package main

import (
	"math/rand"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/server"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	server.Start()
}
