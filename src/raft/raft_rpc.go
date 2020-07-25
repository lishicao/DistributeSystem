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

type GetRequest struct {
	key string
}

type GetResponse struct {
	ErrorCode      int
	Result         string
	RedirectAdress string
}

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
	ErrorCode      int
	RedirectAdress string
}

// 对客户端提供的Get接口
func (raftRpc *RaftRpc) Get(getRequest GetRequest, getResponse *GetResponse) error {
	if raftRpc.raft.status != Leader {
		getResponse.ErrorCode = -1 // 查询失败，需要重定向
		getResponse.Result = ""
		getResponse.RedirectAdress = raftRpc.raft.peers[raftRpc.raft.leaderIndex]
	}
	succ := raftRpc.raft.stateMachine.Get(getRequest.key, &getResponse.Result)
	if succ {
		getResponse.ErrorCode = 0
	} else {
		getResponse.ErrorCode = -2 // 查询失败，没有这个key
	}
	return nil
}

// 对客户端提供的Put接口
func (raftRpc *RaftRpc) Put(putRequest PutRequest, putResponse *PutResponse) error {
	if raftRpc.raft.status != Leader {
		putResponse.ErrorCode = -1 // 插入失败，需要重定向
		putResponse.RedirectAdress = raftRpc.raft.peers[raftRpc.raft.leaderIndex]
	}
	succ := raftRpc.raft.dealPutRequest(putRequest)
	if succ {
		putResponse.ErrorCode = 0
	} else {
		putResponse.ErrorCode = -3 //插入失败
	}
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
