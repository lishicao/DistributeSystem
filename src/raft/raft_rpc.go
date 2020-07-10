package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sync"
	"syscall"
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
	return nil
}

// 请求选举RPC
func (raftRpc *RaftRpc) RequestVote(request VoteRequest, response *VoteResponse) error {
	return nil
}

func (raft *Raft) runServer(wg *sync.WaitGroup, signalChan chan os.Signal) {
	(*wg).Add(1)
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
		select {
		case msg := <-signalChan:
			fmt.Println(msg)
			if msg == syscall.SIGINT || msg == syscall.SIGTERM {
				(*wg).Done()
				fmt.Println("exit run server")
				goto exitLoop
			}
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
exitLoop:
	fmt.Println("runServer exit")

}
