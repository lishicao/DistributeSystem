package raft

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type RaftRpc struct {
	raft *Raft
}

type Params struct {
	A int
	B int
}

func (raftRpc *RaftRpc) Calc(params Params, result *int) error {
	*result = params.A + params.B
	print(*result)
	return nil
}

// 附加日志RPC
func (raftRpc *RaftRpc) AppendEntries(request AppendEntriesRequest, response *AppendEntriesResponse) error {
	raftRpc.raft.dealAppendEntries(request, response)
	return nil
}

// 请求选举RPC
func (raftRpc *RaftRpc) RequestVote(request VoteRequest, response *VoteResponse) error {
	raftRpc.raft.dealRequestVote(request, response)
	return nil
}

func (raft *Raft) runServer() {
	raftRpc := new(RaftRpc)
	raftRpc.raft = raft
	err := rpc.Register(raftRpc)
	if err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp", raft.peers[raft.curNodeIndex])
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
}
