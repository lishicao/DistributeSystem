package main

import (
	"log"
	"net/rpc/jsonrpc"
	"raft"
)

func Call(args raft.Params, reply *int) error {
	conn, err := jsonrpc.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		log.Fatalln("dailing error: ", err)
		return err
	}

	defer func() {
		err = conn.Close()
		print(err)
	}()

	// 调用远程的Calc的Compute方法
	err = conn.Call("RaftRpc.Calc", args, &reply)
	return err
}

func main() {
	params := raft.Params{123, 2}
	var replay int
	err := Call(params, &replay)
	if err != nil {
		print(err)
	}

	print(replay)
}
