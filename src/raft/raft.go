package raft

import (
	"fmt"
	"kvraft"
	"log"
	"math"
	"math/rand"
	"net/rpc/jsonrpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// 状态机接口
type StateMachine interface {
	// 应用到状态机
	apply(key string, value string) bool
	// 从状态机获取信息
	get(key string, value *string) bool
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
	CandidateId  int
	ElectionTerm int
	LastLogIndex int
	LastLogTerm  int
}

//投票rpc返回
type VoteResponse struct {
	IsAgree     bool
	CurrentTerm int
}

//日志复制请求
type AppendEntriesRequest struct {
	LeaderId     int        //领导人的 Id，以便于跟随者重定向请求
	Term         int        //领导人的任期号
	PrevLogTerm  int        //prevLogIndex 条目的任期号
	PrevLogIndex int        //新的日志条目紧随之前的索引值
	Entries      []LogEntry //准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        //领导人已经提交的日志的索引值
}

//回复日志更新请求
type AppendEntriesResponse struct {
	Term      int  //当前的任期号，用于领导人去更新自己
	Successed bool //跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	//LastApplied int			//已应用的索引
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
			raft.nextIndex[i] = index
			raft.matchIndex[i] = -1
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
	votedFor     int        // 在当前获得选票的候选人的Id
	leaderIndex  int        // leader的服务编号
	logs         []LogEntry // 日志存储 索引从0开始
	//logSnapshot     LogSnapshot         //日志快照
	commitIndex    int         //已知的最大的已经被提交的日志条目的索引值，初始化为-1
	lastApplied    int         //当前状态机执行处，初始化为-1
	status         int         //节点状态
	currentTerm    int         //服务器最后一次知道的任期号（初始化为 0，持续递增）
	heartbeatTimer *time.Timer //心跳定时器
	eletionTimer   *time.Timer //竞选超时定时器
	randtime       *rand.Rand  //随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex  []int //记录每个fallow的同步日志状态
	matchIndex []int //记录每个fallow日志最大索引，0递增
	//applyCh        chan ApplyMsg //状态机apply
	stateMachine   *kvraft.KvStateMachine // 状态机
	isKilled       bool                   //节点退出
	lastLogs       AppendEntriesRequest   //最后更新日志
	EnableDebugLog bool                   //打印调试日志开关
	LastGetLock    string
}

func Make(peers []string, curNodeIndex int, statusMachine *kvraft.KvStateMachine) *Raft {
	raft := &Raft{}
	raft.isKilled = false
	raft.peers = peers
	raft.curNodeIndex = curNodeIndex
	raft.currentTerm = 0
	raft.commitIndex = -1
	raft.lastApplied = -1
	raft.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(raft.curNodeIndex)))
	raft.heartbeatTimer = time.NewTimer(HeartbeatDuration)
	raft.eletionTimer = time.NewTimer(CandidateDuration)
	raft.nextIndex = make([]int, len(raft.peers))
	raft.matchIndex = make([]int, len(raft.peers))
	raft.setStatus(Fallower)
	raft.EnableDebugLog = false
	raft.stateMachine = statusMachine
	raft.logs = make([]LogEntry, 0)

	raft.lastLogs = AppendEntriesRequest{
		LeaderId: -1,
		Term:     -1,
	}

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

// 发送附加日志请求
func (raft *Raft) sendRequestAppendEntries(index int, request AppendEntriesRequest, response *AppendEntriesResponse) error {
	conn, err := jsonrpc.Dial("tcp", raft.peers[index])
	if err != nil {
		log.Fatalln("dailing error: ", err)
		return err
	}
	defer func() {
		err = conn.Close()
		print(err)
	}()
	err = conn.Call("RaftRpc.AppendEntries", request, response)
	return err
}

// 处理leader发送过来的日志追加请求（或者心跳）
func (raft *Raft) dealAppendEntries(request AppendEntriesRequest, response *AppendEntriesResponse) {
	response.Term = raft.currentTerm

	// 1 如果 term < currentTerm 就返回 false
	if request.Term < raft.currentTerm {
		response.Successed = false
		response.Term = raft.currentTerm
		return
	}

	// 重置选举时间
	raft.resetCandidateTimer()

	// 2 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
	if request.PrevLogIndex >= 0 && raft.logs[request.PrevLogIndex].Term != request.PrevLogTerm {
		response.Successed = false
		response.Term = raft.currentTerm
		return
	}

	raft.setStatus(Fallower)

	// 3 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
	for index := 0; index < len(request.Entries); index++ {
		entry := request.Entries[index]
		if entry.Index >= len(raft.logs) {
			break
		}

		if raft.logs[entry.Index].Term != entry.Term {
			// 冲突，删除这一条和之后所有的。
			raft.logs = raft.logs[0:entry.Index]
			break
		}
	}

	// 4 附加日志中尚未存在的任何新条目  TODO:并发控制
	for index := 0; index < len(request.Entries); index++ {
		entry := request.Entries[index]
		if entry.Index >= len(raft.logs) {
			raft.logs = append(raft.logs, entry)
			raft.logs[entry.Index] = entry
			log.Println("node: ", raft.curNodeIndex, " leaderId: ", request.LeaderId, "recive log: ", entry.Log)
		}
	}

	// 5 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
	if request.LeaderCommit > raft.commitIndex {
		if len(request.Entries) > 0 {
			raft.commitIndex = int(math.Min(float64(request.LeaderCommit), float64(request.Entries[0].Index)))
		} else {
			raft.commitIndex = request.LeaderCommit
		}

	}

	response.Successed = true
	response.Term = raft.currentTerm

	return
}

// 处理其他节点发过来的投票请求
func (raft *Raft) dealRequestVote(request VoteRequest, response *VoteResponse) {
	response.IsAgree = true
	response.CurrentTerm = raft.currentTerm
	//竞选任期小于自身任期，则反对票
	if (*response).CurrentTerm >= request.ElectionTerm {
		log.Println(raft.curNodeIndex, "refuse", request.CandidateId, "because of term")
		response.IsAgree = false
		return
	}
	raft.setStatus(Fallower)
	raft.currentTerm = request.ElectionTerm
	log.Println(raft.curNodeIndex, "agree", request.CandidateId)
	raft.resetCandidateTimer()
}

// 处理客户端的put请求
func (raft *Raft) dealPutRequest(request PutRequest) bool {
	logEntry := LogEntry{}
	logEntry.Log = request.Key + "\t" + request.Value
	logEntry.Term = raft.currentTerm
	logEntry.Index = len(raft.logs)

	succssNodeCount := 0
	wg := sync.WaitGroup{}
	wg.Add(len(raft.peers))

	for i := 0; i < len(raft.peers); i++ {
		if i == raft.curNodeIndex {
			succssNodeCount += 1
			wg.Done()
			continue
		}
		go func(peer int) {
			request := AppendEntriesRequest{}
			request.LeaderCommit = raft.commitIndex
			request.Term = raft.currentTerm
			request.LeaderId = raft.curNodeIndex
			if raft.matchIndex[peer] < 0 || len(raft.logs) == 0 {
				request.PrevLogTerm = -1
				request.PrevLogIndex = -1
			} else {
				request.PrevLogTerm = raft.logs[raft.matchIndex[peer]].Term
				request.PrevLogIndex = raft.logs[raft.matchIndex[peer]].Index
			}

			if raft.nextIndex[peer] > len(raft.logs) {
				request.Entries = raft.logs[len(raft.logs):]
			} else {
				request.Entries = raft.logs[raft.nextIndex[peer]:]
			}
			request.Entries = append(request.Entries, logEntry)

			response := AppendEntriesResponse{}
			err := raft.sendRequestAppendEntries(peer, request, &response)
			if response.Term > raft.currentTerm {
				raft.setStatus(Fallower)
			}
			if response.Successed == false {
				raft.nextIndex[peer]--
			} else {
				raft.matchIndex[peer] = raft.nextIndex[peer]
				raft.nextIndex[peer]++
			}

			wg.Done()
			if err == nil && response.Successed == true {
				succssNodeCount += 1
			}
		}(i)
	}
	wg.Wait()

	if succssNodeCount > (len(raft.peers) / 2) {
		// 复制到过半节点，主节点应用这个日志
		raft.logs = append(raft.logs, logEntry)
		raft.commitIndex += 1
		return true
	}
	// 未复制到过半节点，失败
	return false
}

// 发起投票
func (raft *Raft) Vote() {
	raft.currentTerm += 1
	log.Println("start vote :", raft.curNodeIndex, "term :", raft.currentTerm)
	currentTerm := raft.currentTerm
	req := VoteRequest{
		CandidateId:  raft.curNodeIndex,
		ElectionTerm: currentTerm,
		LastLogTerm:  0,
		LastLogIndex: 0,
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
		raft.setStatus(Fallower)
	} else if agreeVote*2 > nodeCount { //获得多数赞同则变成leader
		log.Println(raft.curNodeIndex, "become leader :", currentTerm)
		raft.setStatus(Leader)
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

// 心跳循环
func (raft *Raft) heartbeatLoop() {
	defer func() {
		raft.heartbeatTimer.Stop()
	}()
	for !raft.isKilled {
		<-raft.heartbeatTimer.C
		raft.heartbeatTimer.Reset(HeartbeatDuration)
		if raft.status == Leader {
			for peer := 0; peer < len(raft.peers); peer++ {
				if peer == raft.curNodeIndex {
					continue
				}
				go func(peer int, raft *Raft) {
					request := AppendEntriesRequest{}
					request.LeaderCommit = raft.commitIndex
					request.Term = raft.currentTerm
					request.LeaderId = raft.curNodeIndex
					if raft.matchIndex[peer] < 0 || len(raft.logs) == 0 {
						request.PrevLogTerm = -1
						request.PrevLogIndex = -1
					} else {
						log.Println("node: ", peer, "  len log:", raft.matchIndex[peer])
						log.Println("log number: ", raft.logs[raft.matchIndex[peer]].Term)
						request.PrevLogTerm = raft.logs[raft.matchIndex[peer]].Term
						request.PrevLogIndex = raft.logs[raft.matchIndex[peer]].Index
					}

					if raft.nextIndex[peer] > len(raft.logs) {
						request.Entries = raft.logs[len(raft.logs):]
					} else {
						request.Entries = raft.logs[raft.nextIndex[peer]:]
					}

					response := AppendEntriesResponse{Term: 0}
					err := raft.sendRequestAppendEntries(peer, request, &response)

					if response.Term > raft.currentTerm {
						raft.setStatus(Fallower)
						return
					}

					if response.Successed == false {
						raft.nextIndex[peer]--
						return
					}

					// 更新相应跟随者的 nextIndex 和 matchIndex
					raft.matchIndex[peer] = len(raft.logs) - 1
					raft.nextIndex[peer] = len(raft.logs)

					if err != nil {
						return
					}
				}(peer, raft)
			}
		}
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
		if raft.lastApplied < raft.commitIndex && raft.commitIndex >= 0 {
			entry := raft.logs[raft.lastApplied+1]
			string_slice := strings.Split(entry.Log, "\t")
			if len(string_slice) == 2 {
				key := string_slice[0]
				value := string_slice[1]
				raft.stateMachine.Apply(key, value)
				log.Println("apply to state machine key: ", key, " value: ", value)
			}
			raft.lastApplied++
		}
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
	go raft.electionLoop()
	go raft.heartbeatLoop()
	go raft.runLoop(signalChan, endChan)
	go raft.runServer()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-endChan
}
