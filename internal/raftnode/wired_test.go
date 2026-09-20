package raftnode_test

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/broker"
	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/raftnode"
)

var boundAddr = regexp.MustCompile(`listening on (127\.0\.0\.1:\d+)`)

// startBroker launches the real compiled millrace-core on an OS-assigned
// port and returns its address.
func startBroker(t *testing.T) string {
	t.Helper()
	bin, _ := filepath.Abs(filepath.Join("..", "..", "..", "millrace-core", "target", "release", "millrace-core.exe"))
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("millrace-core not built (%s): run `cargo build --release` there", bin)
	}
	// 1 MiB segments: each partition preallocates a whole segment, and the
	// 64 MiB default would need ~600 MB of disk for 3 brokers x 3 partitions.
	cmd := exec.Command(bin, "--port", "0", "--data-dir", t.TempDir(), "--segment-bytes", "1048576")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if m := boundAddr.FindStringSubmatch(sc.Text()); m != nil {
			return m[1]
		}
	}
	t.Fatal("broker never reported its address")
	return ""
}

// The cluster <-> core wiring: a CreateTopic committed through Raft must show
// up on every node's own real broker, with the same partition count.
func TestCommittedTopicIsCreatedOnEveryNodesBroker(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	raftAddrs := map[string]string{"n1": "127.0.0.1:19201", "n2": "127.0.0.1:19202", "n3": "127.0.0.1:19203"}
	var servers []raft.Server
	for _, id := range ids {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(raftAddrs[id])})
	}

	brokers := map[string]string{}
	var nodes []*raft.Raft
	for _, id := range ids {
		brokers[id] = startBroker(t)
		f := fsm.New(ids)
		f.OnTopicCreated = broker.Mirror(brokers[id], id)
		r, err := raftnode.Start(f, raftnode.Config{
			NodeID: id, BindAddr: raftAddrs[id], DataDir: t.TempDir(), Peers: servers,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Shutdown().Error() })
		nodes = append(nodes, r)
	}

	applyCreateTopic(t, waitForLeader(t, nodes, 10*time.Second), "orders", 3)

	for id, addr := range brokers {
		deadline := time.Now().Add(5 * time.Second)
		for {
			n, err := broker.DescribeTopic(addr, "orders")
			if err == nil {
				if n != 3 {
					t.Fatalf("node %s broker has %d partitions, want 3", id, n)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic never appeared on node %s's broker (%s): %v", id, addr, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
