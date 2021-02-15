package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func (r *raft) debugLog(format string, args ...interface{}) {
	if !r.showLog {
		return
	}
	format = fmt.Sprintf("[%s] ", r.currentAddress) + format
	log.Printf(format, args...)
}

func (r *raft) electionTimeout() time.Duration {
	return time.Duration(r.electionTimeoutMinMs+rand.Intn(r.electionTimeoutMaxMs-r.electionTimeoutMinMs)) * time.Millisecond
}

func (r *raft) term() int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

func (r *raft) state() FSM {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentState
}

func (r *raft) voteFor() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.votedFor
}

func (r *raft) address() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentAddress
}

func (r *raft) electionTimer() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentElectionTimer
}

func (r *raft) nodes() map[string]*struct{} {
	m := make(map[string]*struct{})

	r.mu.RLock()
	defer r.mu.RUnlock()

	for k, v := range r.nodeIDs {
		m[k] = v
	}

	return m
}

func (r *raft) lastLogIndexAndTerm() (int32, int32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.log) > 0 {
		lastIndex := len(r.log) - 1
		return int32(lastIndex), r.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (r *raft) addNode(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodeIDs[address]; !ok {
		if _, e := r.server.connect(address, false); e == nil {
			r.nodeIDs[address] = &struct{}{}
			go r.saveNodes()
		}
	}
	return
}
