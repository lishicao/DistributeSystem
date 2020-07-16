package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc/jsonrpc"
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

//节点状态
const Fallower, Leader, Candidate int = 1, 2, 3

//心跳周期
const HeartbeatDuration = time.Duration(time.Millisecond * 600)

//竞选周期
const CandidateDuration = HeartbeatDuration * 2

// 日志条目
type LogEntry struct {
	Term  int
	Index int
	Log   string
}

//投票请求
type VoteRequest struct {
	RequestNode  int
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

//设置当前节点状态
func (raft *Raft) setStatus(status int) {
	raft.lock("Raft.setStatus")
	defer raft.unlock("Raft.setStatus")
	//设置节点状态，变换为fallow时候重置选举定时器（避免竞争）
	if (raft.status != Fallower) && (status == Fallower) {
		raft.resetCandidateTimer()
	}

	//节点变为leader，则初始化fallow日志状态
	if raft.status != Leader && status == Leader {
		index := len(raft.logs)
		for i := 0; i < len(raft.peers); i++ {
			raft.nextIndex[i] = index + 1
			raft.matchIndex[i] = 0
		}
	}
	raft.status = status
}

func (raft *Raft) lock(info string) {
	raft.mu.Lock()
	raft.LastGetLock = info
}

func (raft *Raft) unlock(info string) {
	raft.LastGetLock = ""
	raft.mu.Unlock()
}

//重置竞选周期定时
func (raft *Raft) resetCandidateTimer() {
	randCnt := raft.randtime.Intn(250)
	duration := time.Duration(randCnt)*time.Millisecond + CandidateDuration
	raft.eletionTimer.Reset(duration)
}

type Raft struct {
	mu    sync.Mutex // Lock to protect shared access to this peer's state
	peers []string   // rpc节点，记录每个节点的ip:port
	//persister       *Persister          // Object to hold this peer's persisted state
	curNodeIndex int        // 自己服务编号
	logs         []LogEntry // 日志存储
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
	isKilled       bool                 //节点退出
	lastLogs       AppendEntriesRequest //最后更新日志
	EnableDebugLog bool                 //打印调试日志开关
	LastGetLock    string
}

func Make(peers []string, curNodeIndex int) *Raft {
	raft := &Raft{}
	raft.isKilled = false
	raft.peers = peers
	raft.curNodeIndex = curNodeIndex
	raft.currentTerm = 0
	raft.commitIndex = 0
	raft.lastApplied = 0
	raft.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(raft.curNodeIndex)))
	raft.heartbeatTimers = make([]*time.Timer, len(raft.peers))
	raft.eletionTimer = time.NewTimer(CandidateDuration)
	raft.nextIndex = make([]int, len(raft.peers))
	raft.matchIndex = make([]int, len(raft.peers))
	raft.setStatus(Fallower)
	raft.EnableDebugLog = false

	raft.lastLogs = AppendEntriesRequest{
		Me:   -1,
		Term: -1,
	}

	//日志同步协程
	for i := 0; i < len(raft.peers); i++ {
		raft.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go raft.replicateLogLoop(i)
	}
	go raft.electionLoop()
	return raft
}

// 发送投票请求
func (raft *Raft) sendRequestVote(index int, request VoteRequest, response *VoteResponse) error {
	conn, err := jsonrpc.Dial("tcp", raft.peers[index])
	if err != nil {
		log.Fatalln("dailing error: ", err)
		return err
	}
	defer func() {
		err = conn.Close()
		print(err)
	}()
	err = conn.Call("RaftRpc.RequestVote", request, response)
	return err
}

// 处理其他节点发过来的投票请求
func (raft *Raft) dealRequestVote(request VoteRequest, response *VoteResponse) {
	response.IsAgree = true
	response.CurrentTerm = raft.currentTerm
	//竞选任期小于自身任期，则反对票
	if (*response).CurrentTerm >= request.ElectionTerm {
		log.Println(raft.curNodeIndex, "refuse", request.RequestNode, "because of term")
		response.IsAgree = false
		return
	}
	raft.setStatus(Fallower)
	raft.currentTerm = request.ElectionTerm
	log.Println((*raft).curNodeIndex, "agree", request.RequestNode)
	raft.resetCandidateTimer()
}

// 发起投票
func (raft *Raft) Vote() {
	raft.currentTerm += 1
	log.Println("start vote :", raft.curNodeIndex, "term :", raft.currentTerm)
	currentTerm := raft.currentTerm
	req := VoteRequest{
		RequestNode:  raft.curNodeIndex,
		ElectionTerm: currentTerm,
		LogTerm:      0,
		LogIndex:     0,
	}
	var wait sync.WaitGroup
	nodeCount := len(raft.peers)

	wait.Add(nodeCount)

	agreeVote := 0
	term := currentTerm
	for i := 0; i < nodeCount; i++ {
		//并行调用投票rpc，避免单点阻塞
		go func(index int) {
			defer wait.Done()
			resp := VoteResponse{false, -1}
			if index == raft.curNodeIndex {
				agreeVote++
				return
			}
			err := raft.sendRequestVote(index, req, &resp)
			if err != nil {
				return
			}
			if resp.IsAgree {
				agreeVote++
				return
			}
			if resp.CurrentTerm > term {
				term = resp.CurrentTerm
			}

		}(i)
	}

	wait.Wait()

	//如果存在系统任期更大，则更新任期并转为fallow
	if term > currentTerm {
		log.Println(raft.curNodeIndex, "become fallower :", currentTerm)
		raft.currentTerm = term
		raft.status = Fallower
	} else if agreeVote*2 > nodeCount { //获得多数赞同则变成leader
		log.Println(raft.curNodeIndex, "become leader :", currentTerm)
		raft.status = Leader
	}
}

// 选举
func (raft *Raft) electionLoop() {
	//选举超时定时器
	raft.resetCandidateTimer()
	defer raft.eletionTimer.Stop()

	for !raft.isKilled {
		<-raft.eletionTimer.C
		if raft.isKilled {
			break
		}
		if raft.status == Candidate {
			raft.resetCandidateTimer()
			raft.Vote()
		} else if raft.status == Fallower {
			raft.resetCandidateTimer()
			raft.setStatus(Candidate)
			raft.Vote()
		}
	}
}

// 日志复制
func (raft *Raft) replicateLogLoop(peer int) {
	defer func() {
		raft.heartbeatTimers[peer].Stop()
	}()
	for !raft.isKilled {
		<-raft.heartbeatTimers[peer].C
		if raft.isKilled {
			break
		}
	}
}

//事件循环
func (raft *Raft) runLoop(signalChan chan os.Signal, endChan chan string) {
	fmt.Println("start run loop")
	for {
		time.Sleep(500000) // 10毫秒
		select {
		case msg := <-signalChan:
			fmt.Println(msg)
			if msg == syscall.SIGINT || msg == syscall.SIGTERM || msg == syscall.SIGKILL {
				fmt.Println("exit run loop")
				endChan <- "yes"
				raft.isKilled = true
				return
			}
		default:
		}
	}
}

func (raft *Raft) Run() {
	signalChan := make(chan os.Signal, 2)
	endChan := make(chan string)
	go (*raft).electionLoop()
	go (*raft).runLoop(signalChan, endChan)
	go (*raft).runServer()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-endChan
}
