// Raft一致性实现

package raft

import (
	"github.com/invxp/raft/proto/message"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
	"time"
)

const MethodForwardMessage = "/raft.message.Message/ForwardMessage"

// CommitEntry 数据提交的通道,每次数据有变动时CommitChannel会触发结果
type CommitEntry struct {
	// Command 客户端提交的命令
	Command string

	// Index 提交的索引
	Index int32

	// Term 提交的生命周期
	Term int32
}

// FSM 状态机
type FSM int32

const (
	Follower FSM = iota
	Candidate
	Leader
	Shutdown
)

func (s FSM) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		panic("unreachable")
	}
}

// raft 结构体
type raft struct {
	// mu 锁
	mu sync.RWMutex

	// currentAddress 对应rpc的地址
	currentAddress string

	// nodeIDs 集群内其他节点地址
	nodeIDs map[string]*struct{}

	// server RPC服务器
	server *Server

	// storage 用于持久化存储
	storage Storage

	// commitChan 数据回调,用于监听数据变更
	commitChan chan<- CommitEntry

	// innerCommitReadyChan 内部回调,准备成功后发往commitChan
	innerCommitReadyChan chan struct{}

	// triggerChan 内部回调状态机
	triggerChan chan struct{}

	// raft 持久化数据(可以存起来)
	currentTerm int32
	votedFor    string
	log         []*message.AppendEntriesRequest_LogEntry

	// raft 非持久化数据(会经常变动)
	commitIndex          int32
	lastApplied          int32
	currentState         FSM
	currentElectionTimer time.Time

	// raft 日志索引游标
	nextIndex  map[string]int32
	matchIndex map[string]int32

	// 选举最短超时时间
	electionTimeoutMinMs int
	// 选举最长超时时间
	electionTimeoutMaxMs int
	// Leader心跳间隔
	heartbeatMs time.Duration
	// rpc请求超时时间
	rpcMsgTimeoutMs time.Duration
	// 是否打印日志
	showLog bool
	// 是否自动转发请求
	autoRedirectMessage bool
}

// create 创建一个新的Raft对象
// address为RPCServer的地址
// server为内部的RPCServer
// storage用于内部持久化KV存储
// commitChan每次有数据变更时会回调该函数让业务感知
// nodeIDs其他节点的地址列表
func create(address string, server *Server, storage Storage, commitChan chan<- CommitEntry, rpcMsgTimeoutMs, heartbeatMs time.Duration, electionTimeoutMinMs, electionTimeoutMaxMs int, showLog, autoRedirectMessage bool, nodeIDs map[string]*grpc.ClientConn) *raft {
	rand.Seed(time.Now().UnixNano())

	cm := &raft{}
	cm.currentAddress = address
	cm.nodeIDs = make(map[string]*struct{})
	for k := range nodeIDs {
		cm.nodeIDs[k] = &struct{}{}
	}
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.innerCommitReadyChan = make(chan struct{}, 65535)
	cm.triggerChan = make(chan struct{}, 1)
	cm.currentState = Follower
	cm.votedFor = ""
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[string]int32)
	cm.matchIndex = make(map[string]int32)

	cm.rpcMsgTimeoutMs = rpcMsgTimeoutMs
	cm.showLog = showLog
	cm.electionTimeoutMinMs = electionTimeoutMinMs
	cm.electionTimeoutMaxMs = electionTimeoutMaxMs
	cm.heartbeatMs = heartbeatMs
	cm.autoRedirectMessage = autoRedirectMessage

	cm.loadFromStorage()

	return cm
}

// run 开始运行选举
func (r *raft) run() {
	go r.loopCommits()

	r.mu.Lock()
	r.currentElectionTimer = time.Now()
	r.mu.Unlock()

	go r.runElectionTimer()
}

// status 获取当前节点状态
func (r *raft) status() (id string, term int32, isLeader bool, leaderID string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var leader string

	switch r.currentState {
	case Leader:
		leader = r.currentAddress
	case Candidate:
		leader = ""
	default:
		leader = r.votedFor
	}

	return r.currentAddress, r.currentTerm, r.currentState == Leader, leader
}

// submitData 提交一条日志(接口)
func (r *raft) submitData(command string) bool {
	return r.submit(command, r.autoRedirectMessage)
}

// 如果当前节点状态是Leader,则直接提交变更
// 如果当前节点状态是Follower,则转发至Leader提交变更
// 如果转发失败或提交的数据点有误则返回提交失败
func (r *raft) submit(command string, redirect bool) bool {
	if r.state() == Leader {
		r.mu.Lock()
		r.log = append(r.log, &message.AppendEntriesRequest_LogEntry{Command: command, Term: r.currentTerm})
		r.mu.Unlock()
		r.saveToStorage()
		r.triggerChan <- struct{}{}
		return true
	} else if r.state() == Follower && redirect {
		return r.callForwardMessage(r.voteFor(), r.address(), r.term(), command, r.rpcMsgTimeoutMs)
	}

	return false
}

// callForwardMessage RPC调用
func (r *raft) callForwardMessage(nodeID, callerID string, currentTerm int32, command string, timeoutMs time.Duration) bool {
	if nodeID == "" {
		return false
	}

	reply := &message.ForwardMessageReply{}

	r.debugLog("forward message: %s -> %s, %d", callerID, nodeID, currentTerm)

	if err := r.server.call(nodeID, MethodForwardMessage, &message.ForwardMessageRequest{Term: currentTerm, NodeID: callerID, Command: command}, reply, timeoutMs); err == nil {
		if reply.SubmitGranted {
			r.mu.Lock()
			r.currentTerm = reply.Term
			r.mu.Unlock()
		}
	} else {
		if err != ErrorInvalidClient {
			r.debugLog("forward message to: %s error: %v", nodeID, err)
		}
	}

	return reply.SubmitGranted
}

// stop 停止服务,一般用于测试用例(优雅的关闭)
func (r *raft) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentState == Shutdown {
		return
	}

	r.debugLog("raft shutdown")

	r.currentState = Shutdown
	close(r.innerCommitReadyChan)
}

// loopCommits 循环接收服务器提交的数据
func (r *raft) loopCommits() {
	for range r.innerCommitReadyChan {
		var entries []*message.AppendEntriesRequest_LogEntry
		r.mu.Lock()
		prevTerm := r.currentTerm
		prevLastApplied := r.lastApplied
		if r.commitIndex > r.lastApplied {
			entries = r.log[r.lastApplied+1 : r.commitIndex+1]
			r.lastApplied = r.commitIndex
		}
		r.mu.Unlock()
		//将内部提交的数据通知给外部
		for i, entry := range entries {
			r.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   prevLastApplied + int32(i) + 1,
				Term:    prevTerm,
			}
		}
	}
}
