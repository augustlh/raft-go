package main

import (
//	"raft-go/HaskellRipoff/Data"
//	"raft-go/HaskellRipoff/Base"
	"raft-go/raft"
	"os"
	"fmt"
	"strconv"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Expected ID arg")
		return
	}

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("ID must be a non-negative integer")
		return
	}

	nodes, err := raft.LoadServers("./servers.txt")
	if err != nil { panic(err.Error()) }

	if id < 0 || id >= len(nodes) {
		fmt.Printf("ID out of bounds for config file, must be in range [0;%d]", len(nodes)-1)
		return
	}


	nodes2 := []raft.NodeInfo{
		raft.NodeInfo{Id: 0, ConnectionAddr: "localhost:5000"},
		raft.NodeInfo{Id: 1, ConnectionAddr: "localhost:5001"},
		raft.NodeInfo{Id: 2, ConnectionAddr: "localhost:5002"},
	};


	state := raft.NodeState {}
	state.Initialize(raft.NodeId(id), nodes2)

	state.Loop()
}

