package main

import (
	"flag"
	"kvraft"
	"raft"
	"strconv"
	"time"
)

func main() {
	var portStart int
	var nodeNum int
	var curNodeIndex int

	flag.IntVar(&curNodeIndex, "I", 0, "当前节点编号")
	flag.IntVar(&nodeNum, "N", 5, "总共节点数量")
	flag.IntVar(&portStart, "P", 8000, "起始节点的port")
	flag.Parse()

	peers := make([]string, nodeNum)
	for i := 0; i < nodeNum; i++ {
		port := strconv.Itoa(portStart + i)
		peers[i] = "127.0.0.1:" + port
	}
	var raftNode *raft.Raft
	time.Sleep(2 * time.Millisecond)

	machine := &kvraft.KvStateMachine{}
	machine.Content = make(map[string]string, 0)
	raftNode = raft.Make(peers, curNodeIndex, machine)
	raftNode.Run()
}
