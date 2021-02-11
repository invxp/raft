// Raft一致性实现
// 测试用例
package raft

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// BenchTest 测试用例
type BenchTest struct {
	mu sync.Mutex

	cluster map[string]*Server

	storage map[string]*MapStorage

	commitChan map[string]chan CommitEntry

	commits map[string][]CommitEntry

	connected map[string]bool

	alive map[string]bool

	t *testing.T
}

// NewBench 新建测试用例(初始化)
func NewBench(t *testing.T, address ...string) *BenchTest {
	rand.Seed(time.Now().UnixNano())
	_ = os.Remove("vote")
	_ = os.Remove("term")
	_ = os.Remove("log")
	ns := make(map[string]*Server, len(address))
	connected := make(map[string]bool, len(address))
	alive := make(map[string]bool, len(address))
	commitChan := make(map[string]chan CommitEntry, len(address))
	commits := make(map[string][]CommitEntry, len(address))
	storage := make(map[string]*MapStorage, len(address))

	config := &Config{150, 300, 50, 3000, true, false}

	for i, addr := range address {
		peerIds := make(map[string]*struct{}, 0)
		for p := 0; p < len(address); p++ {
			if p == i {
				continue
			}
			peerIds[address[p]] = &struct{}{}
		}
		storage[addr] = NewMapStorage()
		commitChan[addr] = make(chan CommitEntry)
		ns[addr] = NewServer(addr, commitChan[addr], config, address...)
		ns[addr].listenAndServer()
		alive[addr] = true
	}

	for i := 0; i < len(address); i++ {
		ns[address[i]].connectToNodes()
		ns[address[i]].raft.run()
		connected[address[i]] = true
	}

	h := &BenchTest{
		cluster:    ns,
		storage:    storage,
		commitChan: commitChan,
		commits:    commits,
		connected:  connected,
		alive:      alive,
		t:          t,
	}

	for i := 0; i < len(address); i++ {
		go h.collectCommits(address[i])
	}

	return h
}

// Shutdown 停止服务
func (h *BenchTest) Shutdown() {
	for k := range h.cluster {
		h.cluster[k].disconnectAll()
		h.connected[k] = false
	}

	for k := range h.cluster {
		if h.alive[k] {
			h.alive[k] = false
			h.cluster[k].Shutdown()
		}
	}

	for k := range h.cluster {
		close(h.commitChan[k])
	}

	_ = os.Remove("vote")
	_ = os.Remove("term")
	_ = os.Remove("log")
}

// DisconnectPeer 断开本地服务并且断开其他节点的该服务
func (h *BenchTest) DisconnectPeer(id string) {
	h.cluster[id].disconnectAll()
	for k := range h.cluster {
		if k != id {
			_ = h.cluster[k].disconnectNode(id)
		}
	}
	h.connected[id] = false
}

// ReconnectPeer 重新连接
func (h *BenchTest) ReconnectPeer(id string) {
	for j := range h.cluster {
		if j != id && h.alive[j] {
			if err := h.cluster[id].connectToNode(j, h.cluster[j].listenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].connectToNode(id, h.cluster[id].listenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// CrashPeer 测试崩一个节点
func (h *BenchTest) CrashPeer(id string) {
	h.DisconnectPeer(id)
	h.alive[id] = false
	h.cluster[id].Shutdown()

	h.mu.Lock()
	h.commits[id] = h.commits[id][:0]
	h.mu.Unlock()
}

// RestartPeer 重启一个节点
func (h *BenchTest) RestartPeer(id string) {
	if h.alive[id] {
		h.t.Fatalf("currentAddress=%s is alive in RestartPeer", id)
	}

	var nodesIds []string
	for p := range h.cluster {
		nodesIds = append(nodesIds, p)
	}

	//config := Config{150, 300, 50, 3000, true, false}
	//h.cluster[id] = NewServer(id, h.storage[id], h.commitChan[id], config, nodesIds...)
	h.cluster[id].listenAndServer()
	h.ReconnectPeer(id)
	h.cluster[id].raft.run()
	h.alive[id] = true
	sleepMs(20)
}

// CheckSingleLeader 检查是否只有一个Leader
func (h *BenchTest) CheckSingleLeader() (string, int32) {
	for r := 0; r < 8; r++ {
		leaderId := ""
		leaderTerm := int32(-1)
		for i := range h.cluster {
			if h.connected[i] {
				term, isLeader, _ := h.cluster[i].raft.status()
				if isLeader {
					if leaderId == "" {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %s and %s think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId != "" {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return "", -1
}

// CheckNoLeader 检查是否没有Leader
func (h *BenchTest) CheckNoLeader() {
	for i := range h.cluster {
		if h.connected[i] {
			_, isLeader, _ := h.cluster[i].raft.status()
			if isLeader {
				h.t.Fatalf("server %s leader; want none", i)
			}
		}
	}
}

// CheckCommitted 检查某条日志是否全部被提交
func (h *BenchTest) CheckCommitted(cmd string) (nc int, index int32) {
	h.mu.Lock()
	defer h.mu.Unlock()

	commitsLen := -1
	for i := range h.cluster {
		if h.connected[i] {
			if commitsLen >= 0 {
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%s] = %v, commitsLen = %d", i, h.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}

	for c := 0; c < commitsLen; c++ {
		cmdAtC := ""
		for i := range h.cluster {
			if h.connected[i] {
				cmdOfN := h.commits[i][c].Command
				if cmdAtC != "" {
					if cmdOfN != cmdAtC {
						h.t.Errorf("got %s, want %s at h.commits[%s][%d]", cmdOfN, cmdAtC, i, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			index := int32(-1)
			nc := 0
			for i := range h.cluster {
				if h.connected[i] {
					if index >= 0 && h.commits[i][c].Index != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%s][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						index = h.commits[i][c].Index
					}
					nc++
				}
			}
			return nc, index
		}
	}

	h.t.Errorf("cmd=%s not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN 检查是否存在N个节点的提交记录
func (h *BenchTest) CheckCommittedN(cmd string, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// CheckNotCommitted 检查某条日志是否未提交
func (h *BenchTest) CheckNotCommitted(cmd string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.cluster {
		if h.connected[i] {
			for c := 0; c < len(h.commits[i]); c++ {
				gotCmd := h.commits[i][c].Command
				if gotCmd == cmd {
					h.t.Errorf("found %s at commits[%s][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

// SubmitToServer 测试向服务器提交日志
func (h *BenchTest) SubmitToServer(serverId string, cmd string) bool {
	return h.cluster[serverId].raft.submitData(cmd)
}

// SubmitToServerNotForward 测试向服务器提交日志(禁用转发)
func (h *BenchTest) SubmitToServerNotForward(serverId string, cmd string) bool {
	return h.cluster[serverId].raft.submit(cmd, false)
}

// Submit 测试向服务器提交日志(不转发,并且直接找到Leader)
func (h *BenchTest) Submit(cmd string) (string, bool) {
	leader, _ := h.CheckSingleLeader()
	if leader == "" {
		return "", false
	}
	return leader, h.cluster[leader].raft.submitData(cmd)
}

// collectCommits 收集日志
func (h *BenchTest) collectCommits(i string) {
	for c := range h.commitChan[i] {
		h.mu.Lock()
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}
