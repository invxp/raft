// Raft一致性实现
// 日志复制
package raft

import (
	"raft/proto/message"
	"time"
)

const MethodAppendEntries = "/raft.message.Message/AppendEntries"

// startAppendEntry 循环发送日志(只有Leader能干)
func (r *raft) startAppendEntry() {
	r.debugLog("start append entry: %d", r.term())

	r.appendEntry(r.rpcMsgTimeoutMs)

	t := time.NewTicker(r.heartbeatMs * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case _, ok := <-r.triggerChan:
			if !ok {
				return
			}
		}

		if r.state() != Leader {
			return
		}

		r.appendEntry(r.rpcMsgTimeoutMs)
	}
}

// appendEntry 向所有节点发送日志,并调整日志游标与状态
func (r *raft) appendEntry(timeoutMs time.Duration) {
	prevCurrentTerm := r.term()

	for nodeID := range r.nodes() {
		go func(nodeID string) {
			reply, ni, entries := r.callAppendEntry(nodeID, prevCurrentTerm, timeoutMs)
			if reply == nil {
				return
			}

			//如果其他节点的Term大于当前的Term则变成Follower退出
			if reply.Term > prevCurrentTerm {
				r.becomeFollower(reply.Term, nodeID)
				return
			}

			//如果不是Leader或者Term不相等则退出
			if r.state() != Leader || prevCurrentTerm != reply.Term {
				return
			}

			if reply.Success {
				//更新日志
				r.notifyLogCommits(nodeID, ni, entries)
			} else {
				//调整日志游标
				r.fixConflictLogs(nodeID, reply.ConflictTerm, reply.ConflictIndex)
			}
		}(nodeID)
	}
}

// callAppendEntry RPC调用
func (r *raft) callAppendEntry(nodeID string, currentTerm int32, timeoutMs time.Duration) (*message.AppendEntriesReply, int32, []*message.AppendEntriesRequest_LogEntry) {
	r.mu.Lock()
	ni := r.nextIndex[nodeID]
	prevLogIndex := ni - 1
	prevLogTerm := int32(-1)
	if prevLogIndex >= 0 {
		prevLogTerm = r.log[prevLogIndex].Term
	}
	entries := r.log[ni:]

	args := &message.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     r.currentAddress,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex,
		Entries:      entries,
	}

	r.mu.Unlock()

	reply := &message.AppendEntriesReply{}
	if err := r.server.call(nodeID, MethodAppendEntries, args, reply, timeoutMs); err != nil {
		if err != ErrorInvalidClient {
			r.debugLog("append entry to: %s, error: %v", nodeID, err)
		}
		return nil, -1, nil
	}

	return reply, ni, entries
}

// notifyLogCommits 更新日志,如果有变动则通知
func (r *raft) notifyLogCommits(nodeID string, ni int32, entries []*message.AppendEntriesRequest_LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextIndex[nodeID] = ni + int32(len(entries))
	r.matchIndex[nodeID] = r.nextIndex[nodeID] - 1
	savedCommitIndex := r.commitIndex
	for i := r.commitIndex + 1; i < int32(len(r.log)); i++ {
		if r.log[i].Term == r.currentTerm {
			matchCount := 1
			for nodeID := range r.nodeIDs {
				if r.matchIndex[nodeID] >= i {
					matchCount++
				}
			}
			//多数认为提交成功,则认为提交成功
			if matchCount >= (len(r.nodeIDs)/2)+1 {
				r.commitIndex = i
			}
		}
	}

	//如果提交成功则通知
	if r.commitIndex != savedCommitIndex {
		r.innerCommitReadyChan <- struct{}{}
		r.triggerChan <- struct{}{}
	}
}

// fixConflictLogs 调整日志冲突,找到下次发送日志的坐标
func (r *raft) fixConflictLogs(nodeID string, conflictTerm, conflictIndex int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if conflictTerm >= 0 {
		lastIndexOfTerm := int32(-1)
		for i := int32(len(r.log)) - 1; i >= 0; i-- {
			if r.log[i].Term == conflictTerm {
				lastIndexOfTerm = i
				break
			}
		}
		if lastIndexOfTerm >= 0 {
			r.nextIndex[nodeID] = lastIndexOfTerm + 1
		} else {
			r.nextIndex[nodeID] = conflictIndex
		}
	} else {
		r.nextIndex[nodeID] = conflictIndex
	}
}
