package raftnode_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/control"
	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/groups"
	"millrace-cluster/internal/raftnode"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// clusterNode is everything one node needs for the HTTP surface (no gRPC --
// this test never needs it) so it can join, be described, and serve
// create-topic like a real deployment.
type clusterNode struct {
	id     string
	raft   *raft.Raft
	fsm    *fsm.FSM
	server *httptest.Server // wraps control.Server's real HTTPHandler
}

func startClusterNode(t *testing.T, id, raftAddr string, seed map[string]string, peers []raft.Server) *clusterNode {
	t.Helper()
	f := fsm.New(seed)
	r, err := raftnode.Start(f, raftnode.Config{NodeID: id, BindAddr: raftAddr, DataDir: t.TempDir(), Peers: peers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	srv := &control.Server{Raft: r, FSM: f, Groups: groups.New(time.Minute)}
	ts := httptest.NewServer(srv.HTTPHandler())
	t.Cleanup(ts.Close)
	return &clusterNode{id: id, raft: r, fsm: f, server: ts}
}

// postJSON tries each address in turn (a non-leader answers 409) and returns
// the first 2xx response's decoded body, matching how every real client in
// this project (millrace-cli, millrace-sdk) talks to the cluster.
func postJSON(addrs []string, path string, body any) (map[string]any, error) {
	data, _ := json.Marshal(body)
	var lastErr error
	for _, addr := range addrs {
		resp, err := http.Post(addr+path, "application/json", bytes.NewReader(data))
		if err != nil {
			lastErr = err
			continue
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			return out, nil
		}
		lastErr = fmt.Errorf("%s: HTTP %d: %v", addr, resp.StatusCode, out["error"])
	}
	return nil, lastErr
}

func retryPost(t *testing.T, addrs []string, path string, body any) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, err := postJSON(addrs, path, body)
		if err == nil {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("POST %s never succeeded: %v", path, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func getNodes(t *testing.T, addr string) map[string]any {
	t.Helper()
	resp, err := http.Get(addr + "/cluster/nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func nodeIDsOf(t *testing.T, addr string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, n := range getNodes(t, addr)["nodes"].([]any) {
		out[n.(map[string]any)["node_id"].(string)] = true
	}
	return out
}

func partitionNodes(topic map[string]any) map[string]bool {
	out := map[string]bool{}
	for _, p := range topic["partitions"].([]any) {
		out[p.(map[string]any)["node_id"].(string)] = true
	}
	return out
}

// The core proof: a node started fresh, with no restart of anyone else, can
// join a running cluster and immediately start participating -- future
// topics place partitions on it -- and an explicit leave removes it the
// same way, both changes replicating to every surviving node independently.
func TestNodeJoinsAndLeavesRunningClusterDynamically(t *testing.T) {
	n1Raft, n2Raft := freeAddr(t), freeAddr(t)
	servers := []raft.Server{
		{ID: "n1", Address: raft.ServerAddress(n1Raft)},
		{ID: "n2", Address: raft.ServerAddress(n2Raft)},
	}
	seed := map[string]string{"n1": "", "n2": ""}
	n1 := startClusterNode(t, "n1", n1Raft, seed, servers)
	n2 := startClusterNode(t, "n2", n2Raft, seed, servers)
	addrs := []string{n1.server.URL, n2.server.URL}

	// A 2-partition topic created now must land only on n1/n2.
	orders := retryPost(t, addrs, "/topics", map[string]any{"name": "orders", "partitions": 2})
	if got := partitionNodes(orders); len(got) == 0 || got["n3"] {
		t.Fatalf("orders placed on: %v", got)
	}

	// n3 joins: seeds itself from a real GET (proving discovery works, not
	// just AddNode), starts blank (no bootstrap), then asks to be added.
	seedFromCluster := getNodes(t, n1.server.URL)
	n3Seed := map[string]string{}
	for _, n := range seedFromCluster["nodes"].([]any) {
		m := n.(map[string]any)
		n3Seed[m["node_id"].(string)] = m["broker_addr"].(string)
	}
	if len(n3Seed) != 2 {
		t.Fatalf("n3's discovered seed: %v", n3Seed)
	}
	n3Raft := freeAddr(t)
	n3 := startClusterNode(t, "n3", n3Raft, n3Seed, nil) // nil peers: join mode, no bootstrap
	addrs = append(addrs, n3.server.URL)

	retryPost(t, addrs[:2], "/cluster/join", map[string]any{"node_id": "n3", "raft_addr": n3Raft, "broker_addr": ""})

	// Replication reaches every node independently -- n1 and n2 must also
	// pick up n3's AddNode, not just n3 catching up on the pre-join log.
	for _, n := range []*clusterNode{n1, n2, n3} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			ids := nodeIDsOf(t, n.server.URL)
			if len(ids) == 3 && ids["n1"] && ids["n2"] && ids["n3"] {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s's view of membership: %v", n.id, ids)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// A topic created *after* the join must place partitions on all three --
	// the behavioural proof that n3 is a real participant, not just listed.
	jobs := retryPost(t, addrs, "/topics", map[string]any{"name": "jobs", "partitions": 3})
	if got := partitionNodes(jobs); len(got) != 3 {
		t.Fatalf("jobs placed on: %v, want all of n1,n2,n3", got)
	}

	// n2 leaves. Existing topics are untouched (no rebalancing -- documented),
	// but membership converges to {n1,n3} on every survivor independently,
	// and the *next* topic avoids n2.
	retryPost(t, addrs, "/cluster/leave", map[string]any{"node_id": "n2"})
	survivors := []*clusterNode{n1, n3}
	for _, n := range survivors {
		deadline := time.Now().Add(5 * time.Second)
		for {
			ids := nodeIDsOf(t, n.server.URL)
			if len(ids) == 2 && ids["n1"] && ids["n3"] && !ids["n2"] {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s's view after n2 left: %v", n.id, ids)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	audit := retryPost(t, []string{n1.server.URL, n3.server.URL}, "/topics", map[string]any{"name": "audit", "partitions": 2})
	if got := partitionNodes(audit); got["n2"] || len(got) == 0 {
		t.Fatalf("audit placed on: %v, want only n1/n3", got)
	}
}
