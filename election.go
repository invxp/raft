// Raft一致性实现
// 选举
package raft

import (
	"github.com/invxp/raft/proto/message"
	"runtime"
	"sync/atomic"
	"time"
)

const MethodRequestVote = "/raft.message.Message/RequestVote"

// runElectionTimer 进行选举
func (r *raft) runElectionTimer() {
	electionTimeout := r.electionTimeout()
	lastTerm := r.term()

	r.debugLog("new election: %d, %v", lastTerm, r.state())

	for {
		runtime.Gosched()

		currentState := r.state()

		//如果上次选举的Term和本次不一致退出选举
		//且当前的状态必须为Candidate或Follower
		if (lastTerm != r.term()) || (currentState != Candidate && currentState != Follower) {
			return
		}

		//如果到了时间则进行选举
		if elapsed := time.Since(r.electionTimer()); elapsed >= electionTimeout {
			r.startElection()
			return
		}
	}
}

// startElection 选举核心逻辑
func (r *raft) startElection() {
	r.mu.Lock()
	defer r.mu.Unlock()

	//如果没有节点, 则放弃这次选举(不自增Term)
	//可以考虑pre-vote(暂不实现)
	if len(r.nodeIDs) == 0 {
		time.Sleep(time.Second)
		go r.runElectionTimer()
		return
	}

	r.roleTime = time.Now()
	r.currentState = Candidate
	prevTerm := atomic.AddInt32(&r.currentTerm, 1)
	r.currentElectionTimer = time.Now()
	r.votedFor = r.currentAddress

	var votesReceived int32 = 1
	var votes int32 = 0
	for nodeID := range r.nodeIDs {
		go func(nodeID string, votes int32) {
			reply := r.callLeaderElection(prevTerm, nodeID, r.rpcMsgTimeoutMs)
			if reply == nil {
				return
			}

			//如果不是Candidate则退出
			if r.state() != Candidate {
				return
			}

			//如果其他节点的Term大于当前的Term或者其他节点拒绝了本次投票则将自己变成Follower
			if reply.Term > prevTerm || !reply.VoteGranted {
				r.becomeFollower(reply.Term, nodeID)
			} else if reply.Term == prevTerm {
				//如果超过一半以上授权则将自己置为Leader
				if reply.VoteGranted {
					if atomic.AddInt32(&votesReceived, 1) >= int32(len(r.nodes())/2)+1 {
						r.becomeLeader()
					}
				}
			}
		}(nodeID, votes)
	}

	go r.runElectionTimer()
}

// callLeaderElection RPC调用
func (r *raft) callLeaderElection(currentTerm int32, nodeID string, timeoutMs time.Duration) *message.RequestVoteReply {
	prevLastLogIndex, prevLastLogTerm := r.lastLogIndexAndTerm()

	request := &message.RequestVoteRequest{
		Term:         currentTerm,
		CandidateId:  r.address(),
		LastLogIndex: prevLastLogIndex,
		LastLogTerm:  prevLastLogTerm,
	}

	reply := &message.RequestVoteReply{}
	if err := r.server.call(nodeID, MethodRequestVote, request, reply, timeoutMs); err != nil {
		if err != ErrorInvalidClient {
			r.debugLog("leader election to: %s message error: %v", nodeID, err)
		}
		return nil
	}

	return reply
}

// becomeFollower 变更为Follower状态
func (r *raft) becomeFollower(term int32, leaderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.roleTime = time.Now()
	r.currentState = Follower
	r.currentTerm = term
	r.votedFor = leaderID
	r.currentElectionTimer = time.Now()

	r.debugLog("become follower: %d", term)

	go r.runElectionTimer()
}

// becomeLeader 变更为Leader状态
func (r *raft) becomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.roleTime = time.Now()
	r.currentState = Leader

	for peerId := range r.nodeIDs {
		r.nextIndex[peerId] = int32(len(r.log))
		r.matchIndex[peerId] = -1
	}

	r.debugLog("become leader: %d", r.currentTerm)

	go r.startAppendEntry()
}
