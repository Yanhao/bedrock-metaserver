package main

import (
	"fmt"

	"sr.ht/moyanhao/bedrock-metaserver/server"
)

func main() {
	fmt.Print("Hello metaserver!\n\n")
	server.Start()
}
