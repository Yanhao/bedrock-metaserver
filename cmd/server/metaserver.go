package main

import (
	"fmt"

	"sr.ht/moyanhao/bedrock-metaserver/server"
)

func main() {
	fmt.Print("metaserver start...\n\n")

	server.Start()

	fmt.Print("metaserver stop here\n\n")
}
