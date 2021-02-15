// Raft一致性实现
// RPC实现
package raft

import (
	"github.com/invxp/raft/proto/message"
	"time"
)

// RequestVote 投票
func (r *raft) RequestVote(request *message.RequestVoteRequest) (*message.RequestVoteReply, error) {
	if r.state() == Shutdown {
		return nil, ErrorServerAlreadyShutdown
	}

	go r.addNode(request.CandidateId)

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()

	//如果请求者term > 当前的term
	//并且请求者的日志term > 当前的term
	//并且请求者的日志term = 当前的term并且日志index >= 当前的日志index
	//才将自己变成Follower(防止断网的节点不停增加term)
	if (request.LastLogTerm > lastLogTerm || request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex) && request.Term > r.term() {
		r.becomeFollower(request.Term, request.CandidateId)
	}

	reply := &message.RequestVoteReply{}
	if r.term() == request.Term &&
		(r.voteFor() == "" || r.voteFor() == request.CandidateId) &&
		(request.LastLogTerm > lastLogTerm ||
			(request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		r.mu.Lock()
		r.votedFor = request.CandidateId
		r.currentElectionTimer = time.Now()
		r.mu.Unlock()
	}
	reply.Term = r.term()
	r.saveToStorage()
	return reply, nil
}

// AppendEntries 日志复制
func (r *raft) AppendEntries(request *message.AppendEntriesRequest) (*message.AppendEntriesReply, error) {
	if r.state() == Shutdown {
		return nil, ErrorServerAlreadyShutdown
	}

	go r.addNode(request.LeaderId)

	if request.Term > r.term() {
		r.becomeFollower(request.Term, request.LeaderId)
	}

	reply := &message.AppendEntriesReply{}
	reply.Success = false
	if request.Term == r.term() {
		if r.state() != Follower {
			r.becomeFollower(request.Term, request.LeaderId)
		}
		r.mu.Lock()
		r.currentElectionTimer = time.Now()

		//Server没有日志或者小于当前日志的游标
		//并且Term和当前日志的Term相等
		if request.PrevLogIndex == -1 ||
			(request.PrevLogIndex < int32(len(r.log)) && request.PrevLogTerm == r.log[request.PrevLogIndex].Term) {
			reply.Success = true

			//日志游标+1开始匹配
			logInsertIndex := request.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				//找到日志的位置
				if logInsertIndex >= int32(len(r.log)) || newEntriesIndex >= len(request.Entries) {
					break
				}
				if r.log[logInsertIndex].Term != request.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			//调整日志
			if newEntriesIndex < len(request.Entries) {
				r.log = append(r.log[:logInsertIndex], request.Entries[newEntriesIndex:]...)
				r.debugLog("append entry log current: %v", r.log)
			}

			//如果请求者的日志更新,则更新index(min(request, local))
			if request.LeaderCommit > r.commitIndex {
				r.commitIndex = min(request.LeaderCommit, int32(len(r.log))-1)
				r.innerCommitReadyChan <- struct{}{}
			}
		} else {
			// 处理冲突的日志
			if request.PrevLogIndex >= int32(len(r.log)) {
				reply.ConflictIndex = int32(len(r.log))
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = r.log[request.PrevLogIndex].Term
				var i int32
				for i = request.PrevLogIndex - 1; i >= 0; i-- {
					if r.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
		r.mu.Unlock()
	}

	reply.Term = r.term()
	r.saveToStorage()
	return reply, nil
}

// ForwardMessage 数据转发
func (r *raft) ForwardMessage(request *message.ForwardMessageRequest) (*message.ForwardMessageReply, error) {
	if r.state() == Shutdown {
		return nil, ErrorServerAlreadyShutdown
	}

	go r.addNode(request.NodeID)

	reply := &message.ForwardMessageReply{}
	reply.SubmitGranted = false
	reply.Term = r.term()

	//如果请求的term比现在大直接退出
	if request.Term > r.term() {
		return reply, nil
	}

	//是当前的term并且自己是leader则提交
	if request.Term == r.term() && r.state() == Leader {
		reply.SubmitGranted = true
		r.debugLog("forward message granted: %v", request)
		r.mu.Lock()
		r.log = append(r.log, &message.AppendEntriesRequest_LogEntry{Command: request.Command, Term: r.currentTerm})
		r.mu.Unlock()
		r.saveToStorage()
		r.triggerChan <- struct{}{}
	}

	return reply, nil
}

// Replica 扩缩容集群
func (r *raft) Replica(_ *message.ReplicaRequest) (*message.ReplicaReply, error) {
	if r.state() == Shutdown {
		return nil, ErrorServerAlreadyShutdown
	}

	//TODO
	//动态扩缩容

	return &message.ReplicaReply{Success: true}, nil
}
