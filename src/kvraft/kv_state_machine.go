package kvraft

import "raft"

// kv状态机，是raft中状态机接口的实现
type KvStateMachine struct {
	content map[string]string
}

// 应用日志到状态机
func (stateMachine *KvStateMachine) apply(entry raft.LogEntry) bool {
	return false
}

// 获取状态机中某一个key的值
func (stateMachine *KvStateMachine) get(entry *raft.LogEntry) string {
	return (*stateMachine).content[(*entry).Log]
}
