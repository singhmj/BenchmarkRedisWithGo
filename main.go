package main

import (
	"fmt"

	"./common"
)

func main() {
	_, err := common.ConnectToSingleNode("tcp", "127.0.0.1", 1)
	if err != nil {
		fmt.Printf("Error: %v", err)
	}
}
