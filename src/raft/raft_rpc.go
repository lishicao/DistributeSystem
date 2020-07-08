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

func (raft *Raft) runServer() {
	raftRpc := new(RaftRpc)
	err := rpc.Register(raftRpc)
	if err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
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
