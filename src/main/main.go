package main

import (
	"raft"
)

func main() {
	raftNode := &raft.Raft{}
	raft.Run(raftNode)
}
