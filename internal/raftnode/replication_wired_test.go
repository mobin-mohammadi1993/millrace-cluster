package raftnode_test

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/broker"
	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/raftnode"
)

// rawRoundTrip is a minimal, test-only copy of the wire framing millrace-core
// speaks (see millrace-core/src/protocol.rs) -- just enough to Produce/Fetch
// directly against a broker and prove real replication. internal/broker
// deliberately doesn't expose Produce/Fetch: it only needs to mirror topics.
func rawRoundTrip(t *testing.T, addr string, body []byte) []byte {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(frame, uint32(len(body)))
	copy(frame[4:], body)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		t.Fatal(err)
	}
	resp := make([]byte, binary.LittleEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func rawProduce(t *testing.T, addr, topic string, partition uint32, payload []byte) {
	t.Helper()
	body := []byte{2, 0, 0} // op 2 = Produce
	binary.LittleEndian.PutUint16(body[1:], uint16(len(topic)))
	body = append(body, topic...)
	body = binary.LittleEndian.AppendUint32(body, partition)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(payload)))
	body = append(body, payload...)
	resp := rawRoundTrip(t, addr, body)
	if resp[0] == 1 {
		n := binary.LittleEndian.Uint16(resp[1:3])
		t.Fatalf("produce to %s: %s", addr, resp[3:3+n])
	}
}

// rawEndOffset returns partition 0's end offset via DescribeTopic (op 4);
// this test never uses more than one partition per topic.
func rawEndOffset(t *testing.T, addr, topic string) uint64 {
	t.Helper()
	body := []byte{4, 0, 0} // op 4 = DescribeTopic
	binary.LittleEndian.PutUint16(body[1:], uint16(len(topic)))
	body = append(body, topic...)
	resp := rawRoundTrip(t, addr, body)
	if resp[0] == 1 {
		n := binary.LittleEndian.Uint16(resp[1:3])
		t.Fatalf("describe %s on %s: %s", topic, addr, resp[3:3+n])
	}
	return binary.LittleEndian.Uint64(resp[5:13]) // tag(1) + count(4) + offset[0](8)
}

func applyCreateTopicReplicated(t *testing.T, leader *raft.Raft, name string, numPartitions, replicationFactor uint32) fsm.TopicInfo {
	t.Helper()
	cmd := fsm.Command{
		Type: fsm.CommandCreateTopic,
		CreateTopic: &fsm.CreateTopicCommand{
			Name: name, NumPartitions: numPartitions, ReplicationFactor: replicationFactor,
		},
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
	case fsm.TopicInfo:
		return v
	default:
		t.Fatalf("unexpected FSM response type %T", v)
	}
	return fsm.TopicInfo{}
}

func containsID(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// The cluster-level replication proof: a topic created with
// replication_factor=2 lands on two real brokers via Mirror (Leader on one,
// Follower on the other), real record data flows leader -> follower over
// TCP (not just FSM metadata), and a manual promote -- the real broker RPC
// plus the FSM's PromotePartition command -- makes the follower an
// independent, durable leader whose new writes work and whose FSM entry no
// longer lists the old leader as a replica.
func TestReplicatedTopicMirrorsRealDataAndSupportsManualPromote(t *testing.T) {
	ids := []string{"n1", "n2"}
	raftAddrs := map[string]string{"n1": "127.0.0.1:19301", "n2": "127.0.0.1:19302"}
	var servers []raft.Server
	for _, id := range ids {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(raftAddrs[id])})
	}

	// Every FSM must be seeded with every node's *real* broker address (not
	// nodesMap's blanks): a follower's Mirror needs to resolve its leader's
	// broker address via FSM.NodeBroker to build the right leader_addr.
	brokers := map[string]string{}
	for _, id := range ids {
		brokers[id] = startBroker(t)
	}

	fsms := map[string]*fsm.FSM{}
	var nodes []*raft.Raft
	for _, id := range ids {
		f := fsm.New(brokers)
		f.OnTopicCreated = broker.Mirror(brokers[id], id, f)
		fsms[id] = f
		r, err := raftnode.Start(f, raftnode.Config{
			NodeID: id, BindAddr: raftAddrs[id], DataDir: t.TempDir(), Peers: servers,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Shutdown().Error() })
		nodes = append(nodes, r)
	}

	raftLeader := waitForLeader(t, nodes, 10*time.Second)
	topic := applyCreateTopicReplicated(t, raftLeader, "orders", 1, 2)
	if len(topic.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(topic.Partitions))
	}
	p := topic.Partitions[0]
	if len(p.Replicas) != 1 {
		t.Fatalf("expected 1 replica, got %v", p.Replicas)
	}
	leaderID, followerID := p.NodeID, p.Replicas[0]
	leaderAddr, followerAddr := brokers[leaderID], brokers[followerID]

	// Mirror ran async off OnTopicCreated on both nodes: wait for each
	// broker to actually have the topic before producing anything.
	for id, addr := range map[string]string{leaderID: leaderAddr, followerID: followerAddr} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if n, err := broker.DescribeTopic(addr, "orders"); err == nil && n == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic never mirrored onto %s's broker", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	rawProduce(t, leaderAddr, "orders", 0, []byte("hello"))
	rawProduce(t, leaderAddr, "orders", 0, []byte("world"))

	deadline := time.Now().Add(5 * time.Second)
	for {
		if rawEndOffset(t, followerAddr, "orders") == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never replicated the leader's records")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The FSM must reject promoting a node that isn't actually a replica of
	// the partition -- the guard that stops an operator (or a bug in the
	// HTTP handler) from fabricating a bogus leader.
	badCmd := fsm.Command{Type: fsm.CommandPromotePartition, PromotePartition: &fsm.PromotePartitionCommand{
		Topic: "orders", Partition: 0, NodeID: "not-a-replica",
	}}
	badData, err := badCmd.Encode()
	if err != nil {
		t.Fatal(err)
	}
	badFuture := raftLeader.Apply(badData, 3*time.Second)
	if err := badFuture.Error(); err != nil {
		t.Fatalf("raft apply itself failed: %v", err)
	}
	if _, ok := badFuture.Response().(error); !ok {
		t.Fatalf("promoting a non-replica node should have been rejected by the FSM, got %v", badFuture.Response())
	}

	// Manual promote: the cluster's HTTP handler does this in two steps
	// (real broker RPC, then FSM metadata) -- exercised directly here since
	// this test doesn't otherwise need an HTTP server.
	if err := broker.PromoteToLeader(followerAddr, "orders", 0); err != nil {
		t.Fatalf("promoting follower's broker: %v", err)
	}
	promoteCmd := fsm.Command{Type: fsm.CommandPromotePartition, PromotePartition: &fsm.PromotePartitionCommand{
		Topic: "orders", Partition: 0, NodeID: followerID,
	}}
	data, err := promoteCmd.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := raftLeader.Apply(data, 3*time.Second).Error(); err != nil {
		t.Fatalf("applying promote: %v", err)
	}

	// The metadata change must independently reach every node, not just the
	// one that applied it.
	for _, id := range ids {
		deadline = time.Now().Add(5 * time.Second)
		for {
			top, ok := fsms[id].Topic("orders")
			if ok && top.Partitions[0].NodeID == followerID {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s's FSM never saw the promoted leader", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// The promoted follower must now accept writes directly.
	rawProduce(t, followerAddr, "orders", 0, []byte("promoted"))
	if got := rawEndOffset(t, followerAddr, "orders"); got != 3 {
		t.Fatalf("promoted leader end offset = %d, want 3", got)
	}

	// The FSM must have dropped the old leader from the replica set (this
	// checks the FSM's bookkeeping; it doesn't fence the old leader's
	// broker live, since automatic failover/fencing is out of scope -- see
	// README).
	top, _ := fsms[leaderID].Topic("orders")
	if containsID(top.Partitions[0].Replicas, leaderID) {
		t.Fatalf("old leader %s should have been dropped from the replica set, got %v", leaderID, top.Partitions[0].Replicas)
	}
}
