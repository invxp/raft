// Raft一致性实现
// 测试用例
package raft

import (
	"github.com/fortytw2/leaktest"
	"testing"
	"time"
)

func TestElectionBasic(t *testing.T) {
	h := NewBench(t, ":1111", ":2222")
	defer h.Shutdown()
}

func TestElectionLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(350)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Errorf("want create leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	otherId := ""
	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		otherId = k
		break
	}
	h.DisconnectPeer(otherId)

	// No quorum.
	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect one other server; now we'll have quorum.
	h.ReconnectPeer(otherId)
	h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	sleepMs(100)
	//	Disconnect all servers from the start. There will be no leader.
	for k := range h.cluster {
		h.DisconnectPeer(k)
	}
	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect all servers. A leader will be found.
	for k := range h.cluster {
		h.ReconnectPeer(k)
	}
	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()
	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)

	sleepMs(350)
	newLeaderId, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderId)
	sleepMs(150)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader currentAddress got %s; want %s", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(150)
	newLeaderId, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderId)
	sleepMs(150)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader currentAddress got %s; want %s", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	otherId := ""

	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		otherId = k
		break
	}
	h.DisconnectPeer(otherId)
	time.Sleep(650 * time.Millisecond)
	h.ReconnectPeer(otherId)
	sleepMs(150)

	// We can't have an assertion on the create leader currentAddress here because it depends
	// on the relative election timeouts. We can assert that the term changed,
	// however, which implies that re-election has occurred.
	_, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
	}
}

func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderId, _ := h.CheckSingleLeader()

		otherId := ""
		h.DisconnectPeer(leaderId)
		for k := range h.cluster {
			if k == leaderId {
				continue
			}
			otherId = k
			break
		}
		h.DisconnectPeer(otherId)
		sleepMs(310)
		h.CheckNoLeader()

		// Reconnect both.
		h.ReconnectPeer(otherId)
		h.ReconnectPeer(leaderId)

		// Give it time to settle
		sleepMs(150)
	}
}

func TestCommitOneCommand(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	leader, _ := h.Submit("42")

	if leader == "" {
		t.Errorf("no leader")
	}

	sleepMs(1000)

	h.CheckCommittedN("42", 5)
}

func TestSubmitNonLeaderFails(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	sid := ""
	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		sid = k
		break
	}

	isLeader := h.SubmitToServerNotForward(sid, "42")
	if isLeader {
		t.Errorf("want currentAddress=%s !leader, but it is", sid)
	}
	sleepMs(10)
}

func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	values := []string{"42", "55", "81"}
	for _, v := range values {
		isLeader := h.SubmitToServer(origLeaderId, v)
		if !isLeader {
			t.Errorf("want currentAddress=%s leader, but it's not", origLeaderId)
		}
		sleepMs(100)
	}

	sleepMs(250)
	nc, i1 := h.CheckCommitted("42")
	_, i2 := h.CheckCommitted("55")
	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}

	_, i3 := h.CheckCommitted("81")
	if i2 >= i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}

func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	// submitData a couple of values to a fully connected cluster.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")

	sleepMs(250)
	h.CheckCommittedN("6", 3)

	dPeerId := ""
	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		dPeerId = k
		break
	}
	h.DisconnectPeer(dPeerId)
	sleepMs(250)

	// submitData a create command; it will be committed but only to two servers.
	h.SubmitToServer(origLeaderId, "7")
	sleepMs(250)
	h.CheckCommittedN("7", 2)

	// Now reconnect dPeerId and wait a bit; it should find the create command too.
	h.ReconnectPeer(dPeerId)
	sleepMs(250)
	h.CheckSingleLeader()

	sleepMs(150)
	h.CheckCommittedN("7", 3)
}

func TestNoCommitWithNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	// submitData a couple of values to a fully connected cluster.
	origLeaderId, origTerm := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")

	sleepMs(250)
	h.CheckCommittedN("6", 3)

	// Disconnect both followers.
	dPeer1 := ""
	dPeer2 := ""

	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		if dPeer1 == "" {
			dPeer1 = k
		} else if dPeer2 == "" {
			dPeer2 = k
		}
	}

	h.DisconnectPeer(dPeer1)
	h.DisconnectPeer(dPeer2)
	sleepMs(250)

	h.SubmitToServer(origLeaderId, "8")
	sleepMs(250)
	h.CheckNotCommitted("8")

	// Reconnect both other servers, we'll have quorum now.
	h.ReconnectPeer(dPeer1)
	h.ReconnectPeer(dPeer2)
	sleepMs(600)

	// 8 is still not committed because the term has changed.
	h.CheckNotCommitted("8")

	// A create leader will be elected. It could be a different leader, even though
	// the original's log is longer, because the two reconnected peers can elect
	// each other.
	newLeaderId, againTerm := h.CheckSingleLeader()
	if origTerm == againTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", origTerm)
	}

	// But create values will be committed for sure...
	h.SubmitToServer(newLeaderId, "9")
	h.SubmitToServer(newLeaderId, "10")
	h.SubmitToServer(newLeaderId, "11")
	sleepMs(350)

	for _, v := range []string{"9", "10", "11"} {
		h.CheckCommittedN(v, 3)
	}
}

func TestDisconnectLeaderBriefly(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	// submitData a couple of values to a fully connected cluster.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")
	sleepMs(250)
	h.CheckCommittedN("6", 3)

	// Disconnect leader for a short time (less than election timeout in peers).
	h.DisconnectPeer(origLeaderId)
	sleepMs(90)
	h.ReconnectPeer(origLeaderId)
	sleepMs(200)

	h.SubmitToServer(origLeaderId, "7")
	sleepMs(250)
	h.CheckCommittedN("7", 3)
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333", ":4444", ":5555")
	defer h.Shutdown()

	// submitData a couple of values to a fully connected cluster.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")

	sleepMs(250)
	h.CheckCommittedN("6", 5)

	// Leader disconnected...
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)

	// submitData 7 to original leader, even though it's disconnected.
	h.SubmitToServer(origLeaderId, "7")

	sleepMs(250)
	h.CheckNotCommitted("7")

	newLeaderId, _ := h.CheckSingleLeader()

	// submitData 8 to create leader.
	h.SubmitToServer(newLeaderId, "8")
	sleepMs(250)
	h.CheckCommittedN("8", 4)

	// Reconnect old leader and let it settle. The old leader shouldn't be the one
	// winning.
	h.ReconnectPeer(origLeaderId)
	sleepMs(600)

	finalLeaderId, _ := h.CheckSingleLeader()
	if finalLeaderId == origLeaderId {
		t.Errorf("got finalLeaderId==origLeaderId==%s, want them different", finalLeaderId)
	}

	// submitData 9 and check it's fully committed.
	h.SubmitToServer(newLeaderId, "9")
	sleepMs(250)
	h.CheckCommittedN("9", 5)
	h.CheckCommittedN("8", 5)

	// But 7 is not committed...
	h.CheckNotCommitted("7")
}

func TestCrashFollower(t *testing.T) {
	// Basic test to verify that crashing a peer doesn't blow up.
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")

	sleepMs(350)
	h.CheckCommittedN("5", 3)

	pid := ""
	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		pid = k
		break
	}

	h.CrashPeer(pid)

	sleepMs(350)
	h.CheckCommittedN("5", 2)
}

func TestCrashThenRestartFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1111111*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")
	h.SubmitToServer(origLeaderId, "7")

	val := []string{"5", "6", "7"}

	sleepMs(350)
	for _, v := range val {
		h.CheckCommittedN(v, 3)
	}

	pid := ""
	for k := range h.cluster {
		if k == origLeaderId {
			continue
		}
		pid = k
		break
	}

	h.CrashPeer(pid)
	sleepMs(300)
	for _, v := range val {
		h.CheckCommittedN(v, 2)
	}

	// Restart the crashed follower and give it some time to come up-to-date.
	h.RestartPeer(pid)
	sleepMs(650)
	for _, v := range val {
		h.CheckCommittedN(v, 3)
	}
}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")
	h.SubmitToServer(origLeaderId, "7")

	val := []string{"5", "6", "7"}

	sleepMs(350)
	for _, v := range val {
		h.CheckCommittedN(v, 3)
	}

	h.CrashPeer(origLeaderId)
	sleepMs(350)
	for _, v := range val {
		h.CheckCommittedN(v, 2)
	}

	h.RestartPeer(origLeaderId)
	sleepMs(550)
	for _, v := range val {
		h.CheckCommittedN(v, 3)
	}
}

func TestCrashThenRestartAll(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")
	h.SubmitToServer(origLeaderId, "7")

	val := []string{"5", "6", "7"}

	sleepMs(350)
	for _, v := range val {
		h.CheckCommittedN(v, 3)
	}

	for k := range h.cluster {
		h.CrashPeer(k)
	}

	sleepMs(350)

	for k := range h.cluster {
		h.RestartPeer(k)
	}

	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, "8")
	sleepMs(250)

	val = []string{"5", "6", "7", "8"}
	for _, v := range val {
		h.CheckCommittedN(v, 3)
	}
}

func TestReplaceMultipleLogEntries(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	// submitData a couple of values to a fully connected cluster.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, "5")
	h.SubmitToServer(origLeaderId, "6")

	sleepMs(250)
	h.CheckCommittedN("6", 3)

	// Leader disconnected...
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)

	// submitData a few entries to the original leader; it's disconnected, so they
	// won't be replicated.
	h.SubmitToServer(origLeaderId, "21")
	sleepMs(5)
	h.SubmitToServer(origLeaderId, "22")
	sleepMs(5)
	h.SubmitToServer(origLeaderId, "23")
	sleepMs(5)
	h.SubmitToServer(origLeaderId, "24")
	sleepMs(5)

	newLeaderId, _ := h.CheckSingleLeader()

	// submitData entries to create leader -- these will be replicated.
	h.SubmitToServer(newLeaderId, "8")
	sleepMs(5)
	h.SubmitToServer(newLeaderId, "9")
	sleepMs(5)
	h.SubmitToServer(newLeaderId, "10")
	sleepMs(250)
	h.CheckNotCommitted("21")
	h.CheckCommittedN("10", 2)

	// Crash/restart create leader to reset its nextIndex, to ensure that the create
	// leader of the cluster (could be the third server after elections) tries
	// to replace the original's servers not replicated entries from the very end.
	h.CrashPeer(newLeaderId)
	sleepMs(60)
	h.RestartPeer(newLeaderId)

	sleepMs(100)
	finalLeaderId, _ := h.CheckSingleLeader()
	h.ReconnectPeer(origLeaderId)
	sleepMs(400)

	// submitData another entry; this is because leaders won't commit entries from
	// previous terms (paper 5.4.2) so the 8,9,10 may not be committed everywhere
	// after the restart before a create command comes it.
	h.SubmitToServer(finalLeaderId, "11")
	sleepMs(250)

	// At this point, 11 and 10 should be replicated everywhere; 21 won't be.
	h.CheckNotCommitted("21")
	h.CheckCommittedN("11", 3)
	h.CheckCommittedN("10", 3)
}

/*
func TestCrashAfterSubmit(t *testing.T) {
	h := NewBench(t, ":1111", ":2222", ":3333")
	defer h.Shutdown()

	// Wait for a leader to emerge, and submit a command - then immediately
	// crash; the leader should have no time to send an updated LeaderCommit
	// to followers. It doesn't have time to get back AE responses either, so
	// the leader itself won't send it on the commit channel.
	origLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(origLeaderId, "5")
	sleepMs(1)
	h.CrashPeer(origLeaderId)

	// Make sure 5 is not committed when a create leader is elected. Leaders won't
	// commit commands from previous terms.
	sleepMs(10)
	h.CheckSingleLeader()
	sleepMs(300)
	h.CheckNotCommitted("5")

	// The old leader restarts. After a while, 5 is still not committed.
	h.RestartPeer(origLeaderId)
	sleepMs(300)
	newLeaderId, _ := h.CheckSingleLeader()
	h.CheckNotCommitted("5")

	// When we submit a create command, it will be submitted, and so will 5, because
	// it appears in everyone's logs.
	h.SubmitToServer(newLeaderId, "6")
	sleepMs(300)
	h.CheckCommittedN("5", 3)
	h.CheckCommittedN("6", 3)
}
*/
