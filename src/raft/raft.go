package raft

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// 状态机接口
type StateMachine interface {
	// 应用到状态机
	apply(entry LogEntry) bool
	// 从状态机获取信息
	get(entry LogEntry) string
}

// 日志条目
type LogEntry struct {
	Term  int
	Index int
	Log   string
}

//投票请求
type VoteRequest struct {
	Me           int
	ElectionTerm int
	LogIndex     int
	LogTerm      int
}

//投票rpc返回
type VoteResponse struct {
	IsAgree     bool
	CurrentTerm int
}

//日志复制请求
type AppendEntriesRequest struct {
	Me           int
	Term         int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

//回复日志更新请求
type AppendEntriesResponse struct {
	Term        int
	Successed   bool
	LastApplied int
}

type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state
	//peers           []*labrpc.ClientEnd // rpc节点
	//persister       *Persister          // Object to hold this peer's persisted state
	me   int        // 自己服务编号
	logs []LogEntry // 日志存储
	//logSnapshot     LogSnapshot         //日志快照
	commitIndex     int           //当前日志提交处
	lastApplied     int           //当前状态机执行处
	status          int           //节点状态
	currentTerm     int           //当前任期
	heartbeatTimers []*time.Timer //心跳定时器
	eletionTimer    *time.Timer   //竞选超时定时器
	randtime        *rand.Rand    //随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex  []int //记录每个fallow的同步日志状态
	matchIndex []int //记录每个fallow日志最大索引，0递增
	//applyCh        chan ApplyMsg //状态机apply
	isKilled bool //节点退出
	//lastLogs       AppendEntries //最后更新日志
	EnableDebugLog bool //打印调试日志开关
	LastGetLock    string
}

//func Make(peers []string, me int) *Raft {
//	raft := &Raft{}
//	return raft
//}

//事件循环
func (raft *Raft) runLoop(wg *sync.WaitGroup, signalChan chan os.Signal) {
	(*wg).Add(1)
	for {
		select {
		case msg := <-signalChan:
			fmt.Println(msg)
			if msg == syscall.SIGINT || msg == syscall.SIGTERM {
				(*wg).Done()
				fmt.Println("exit run loop")
				goto exitLoop
			}
		default:
			//fmt.Println("no message received")
		}

	}
exitLoop:
	fmt.Println("runLoop exit")
}

func Run(raft *Raft) {
	var wg sync.WaitGroup
	signalChan := make(chan os.Signal, 2)
	go (*raft).runServer(&wg, signalChan)
	go (*raft).runLoop(&wg, signalChan)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	wg.Wait()
}
