package raftnode_test

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/raftnode"
)

// startTestNode brings up one real raft.Raft node (real TCP transport on
// localhost, real file snapshot store) -- not a mock.
func startTestNode(t *testing.T, id, addr string, peers []raft.Server, peerIDs []string) (*raft.Raft, *fsm.FSM) {
	t.Helper()
	f := fsm.New(peerIDs)
	r, err := raftnode.Start(f, raftnode.Config{
		NodeID:   id,
		BindAddr: addr,
		DataDir:  t.TempDir(),
		Peers:    peers,
	})
	if err != nil {
		t.Fatalf("starting raft node %s: %v", id, err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	return r, f
}

func waitForLeader(t *testing.T, nodes []*raft.Raft, timeout time.Duration) *raft.Raft {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != nil && n.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func applyCreateTopic(t *testing.T, leader *raft.Raft, name string, numPartitions uint32) fsm.TopicInfo {
	t.Helper()
	cmd := fsm.Command{
		Type:        fsm.CommandCreateTopic,
		CreateTopic: &fsm.CreateTopicCommand{Name: name, NumPartitions: numPartitions},
	}
	data, err := cmd.Encode()
	if err != nil {
		t.Fatal(err)
	}
	future := leader.Apply(data, 3*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	switch v := future.Response().(type) {
	case error:
		t.Fatalf("fsm rejected command: %v", v)
		return fsm.TopicInfo{}
	case fsm.TopicInfo:
		return v
	default:
		t.Fatalf("unexpected FSM response type %T", v)
		return fsm.TopicInfo{}
	}
}

func waitForReplication(t *testing.T, f *fsm.FSM, topicName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, top := range f.ListTopics() {
			if top.Name == topicName {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %q never replicated to this node", topicName)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// This is the MVP proof promised for millrace-cluster: a real 3-node Raft
// group elects a leader, replicates a committed command to every node,
// and -- after the leader process is killed -- elects a new leader among
// the survivors and keeps accepting writes. No mocks: this is the actual
// hashicorp/raft library over real TCP transports on localhost.
func TestThreeNodeClusterElectsLeaderReplicatesAndSurvivesLeaderFailure(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	addrs := map[string]string{
		"n1": "127.0.0.1:19101",
		"n2": "127.0.0.1:19102",
		"n3": "127.0.0.1:19103",
	}

	servers := make([]raft.Server, 0, len(ids))
	for _, id := range ids {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(addrs[id])})
	}

	nodes := make(map[string]*raft.Raft, len(ids))
	fsms := make(map[string]*fsm.FSM, len(ids))
	for _, id := range ids {
		r, f := startTestNode(t, id, addrs[id], servers, ids)
		nodes[id] = r
		fsms[id] = f
	}

	all := []*raft.Raft{nodes["n1"], nodes["n2"], nodes["n3"]}
	leader := waitForLeader(t, all, 10*time.Second)

	topic := applyCreateTopic(t, leader, "orders", 3)
	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topic.Partitions))
	}
	for id, f := range fsms {
		func(id string, f *fsm.FSM) {
			waitForReplication(t, f, "orders", 3*time.Second)
			_ = id
		}(id, f)
	}

	var leaderID string
	for id, r := range nodes {
		if r == leader {
			leaderID = id
		}
	}
	t.Logf("killing leader %s", leaderID)
	if err := leader.Shutdown().Error(); err != nil {
		t.Fatalf("shutting down leader: %v", err)
	}

	var remaining []*raft.Raft
	for id, r := range nodes {
		if id != leaderID {
			remaining = append(remaining, r)
		}
	}

	newLeader := waitForLeader(t, remaining, 10*time.Second)
	if newLeader == leader {
		t.Fatal("expected a different node to become leader after failover")
	}

	applyCreateTopic(t, newLeader, "payments", 2)

	for id, f := range fsms {
		if id == leaderID {
			continue
		}
		waitForReplication(t, f, "payments", 3*time.Second)
	}
}
