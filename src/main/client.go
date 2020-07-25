package main

import (
	"log"
	"net/rpc/jsonrpc"
	"raft"
)

func Call(args raft.PutRequest, reply *raft.PutResponse) error {
	conn, err := jsonrpc.Dial("tcp", "127.0.0.1:8003")
	if err != nil {
		log.Fatalln("dailing error: ", err)
		return err
	}

	defer func() {
		err = conn.Close()
		print(err)
	}()

	// 调用远程的Calc的Compute方法
	err = conn.Call("RaftRpc.Put", args, &reply)
	return err
}

func main() {
	request := raft.PutRequest{"aaa", "bbb"}
	response := raft.PutResponse{}
	err := Call(request, &response)
	if err != nil {
		print(err)
	}

	print(response.ErrorCode)
}
