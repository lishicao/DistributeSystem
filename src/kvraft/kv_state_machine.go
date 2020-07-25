package kvraft

// kv状态机，是raft中状态机接口的实现
type KvStateMachine struct {
	Content map[string]string
}

// 应用日志到状态机
func (stateMachine *KvStateMachine) Apply(key string, value string) bool {
	stateMachine.Content[key] = value
	return true
}

// 获取状态机中某一个key的值
func (stateMachine *KvStateMachine) Get(key string, value *string) bool {
	var ok bool
	*value, ok = stateMachine.Content[key]
	return ok
}
