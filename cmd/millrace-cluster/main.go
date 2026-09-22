// Command millrace-cluster runs one node of a Raft cluster with a gRPC and
// HTTP control plane for cluster-aware topic creation. A node either
// bootstraps a brand-new cluster (--peers) or joins a running one (--join).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	"millrace-cluster/internal/broker"
	"millrace-cluster/internal/control"
	pb "millrace-cluster/internal/controlpb"
	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/groups"
	"millrace-cluster/internal/raftnode"
)

func main() {
	nodeID := flag.String("node-id", "", "unique raft node id (required)")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "raft transport bind address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:8000", "gRPC control-plane listen address")
	dataDir := flag.String("data-dir", "data", "raft snapshot data directory")
	peersFlag := flag.String("peers", "", "comma-separated id=raft_addr list for the full static cluster "+
		"(same on every node), e.g. n1=127.0.0.1:7000,n2=127.0.0.1:7001,n3=127.0.0.1:7002. "+
		"Bootstraps a new cluster; required unless --join is given.")
	brokersFlag := flag.String("brokers", "", "optional id=millrace_core_addr list, same on every node "+
		"(bootstrap mode only): committed topics are created on this node's own broker, and /route answers "+
		"with these addresses")
	joinFlag := flag.String("join", "", "comma-separated --http-addr(s) of an already-running cluster to join "+
		"dynamically, instead of bootstrapping a new one with --peers/--brokers")
	brokerAddr := flag.String("broker-addr", "", "this node's own millrace-core address (--join mode only; "+
		"bootstrap mode uses --brokers instead)")
	httpAddr := flag.String("http-addr", "", "optional listen address for the JSON routing/topics/cluster API "+
		"(required to use --join, since joining and leaving both go through it)")
	groupTimeout := flag.Duration("group-session-timeout", 10*time.Second, "a consumer-group member that hasn't heartbeated for this long is evicted")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("--node-id is required")
	}
	if (*joinFlag == "") == (*peersFlag == "") {
		log.Fatal("exactly one of --peers (bootstrap) or --join (join an existing cluster) is required")
	}

	var f *fsm.FSM
	var servers []raft.Server // left empty in --join mode: raftnode.Start then skips bootstrapping

	if *joinFlag != "" {
		nodes, err := fetchClusterNodes(*joinFlag)
		if err != nil {
			log.Fatalf("fetching current cluster membership via --join: %v", err)
		}
		f = fsm.New(nodes) // seeds this node with what the existing cluster already knows
	} else {
		peerIDs, srv, err := parsePeers(*peersFlag)
		if err != nil {
			log.Fatalf("parsing --peers: %v", err)
		}
		servers = srv
		nodes := map[string]string{}
		for _, id := range peerIDs {
			nodes[id] = ""
		}
		if *brokersFlag != "" {
			for _, p := range strings.Split(*brokersFlag, ",") {
				kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
				if len(kv) != 2 {
					log.Fatalf("invalid --brokers entry %q, want id=addr", p)
				}
				nodes[kv[0]] = kv[1]
			}
		}
		f = fsm.New(nodes)
	}

	ownBroker := *brokerAddr
	if *joinFlag == "" {
		ownBroker, _ = f.NodeBroker(*nodeID)
	}
	if ownBroker != "" {
		f.OnTopicCreated = broker.Mirror(ownBroker, *nodeID, f)
	}

	r, err := raftnode.Start(f, raftnode.Config{
		NodeID:   *nodeID,
		BindAddr: *raftAddr,
		DataDir:  *dataDir,
		Peers:    servers,
	})
	if err != nil {
		log.Fatalf("starting raft: %v", err)
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listening on %s: %v", *grpcAddr, err)
	}
	grpcServer := grpc.NewServer()
	srv := &control.Server{Raft: r, FSM: f, Groups: groups.New(*groupTimeout)}
	pb.RegisterControlServer(grpcServer, srv)
	if *httpAddr != "" {
		go func() { log.Fatalf("http serve: %v", http.ListenAndServe(*httpAddr, srv.HTTPHandler())) }()
	}

	if *joinFlag != "" {
		if *httpAddr == "" {
			log.Fatal("--join requires --http-addr (the cluster reaches this node's join/leave/route API through it)")
		}
		if err := joinCluster(*joinFlag, *nodeID, *raftAddr, ownBroker); err != nil {
			log.Fatalf("joining cluster via --join: %v", err)
		}
		log.Printf("millrace-cluster node %q joined the cluster at %s", *nodeID, *joinFlag)
	}

	log.Printf("millrace-cluster node %q ready: raft=%s grpc=%s", *nodeID, *raftAddr, *grpcAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc serve: %v", err)
	}
}

func parsePeers(s string) ([]string, []raft.Server, error) {
	parts := strings.Split(s, ",")
	ids := make([]string, 0, len(parts))
	servers := make([]raft.Server, 0, len(parts))
	for _, p := range parts {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			return nil, nil, fmt.Errorf("invalid peer entry %q, want id=addr", p)
		}
		ids = append(ids, kv[0])
		servers = append(servers, raft.Server{ID: raft.ServerID(kv[0]), Address: raft.ServerAddress(kv[1])})
	}
	sort.Strings(ids)
	sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
	return ids, servers, nil
}

// httpAddrs splits a comma-separated --join/--cluster-style list.
func httpAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// joinHTTPClient bounds every join/discovery request. The default
// http.Client has no timeout at all, so a target that accepts the TCP
// connection but is slow to answer (e.g. its own request is waiting on a
// Raft quorum) would otherwise block the caller indefinitely instead of
// letting the retry loop below move on to the next address.
var joinHTTPClient = &http.Client{Timeout: 5 * time.Second}

// fetchClusterNodes asks the first reachable address for the cluster's
// current membership (any node answers, no leader requirement) -- how a new
// node learns about members whose registration predates its own join, since
// only membership *changes* go through the replicated log, not the original
// bootstrap set.
func fetchClusterNodes(addrs string) (map[string]string, error) {
	var lastErr error
	for _, addr := range httpAddrs(addrs) {
		resp, err := joinHTTPClient.Get("http://" + addr + "/cluster/nodes")
		if err != nil {
			lastErr = err
			continue
		}
		var body struct {
			Nodes []struct {
				NodeID     string `json:"node_id"`
				BrokerAddr string `json:"broker_addr"`
			} `json:"nodes"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		nodes := make(map[string]string, len(body.Nodes))
		for _, n := range body.Nodes {
			nodes[n.NodeID] = n.BrokerAddr
		}
		return nodes, nil
	}
	return nil, fmt.Errorf("no address in %q answered: %w", addrs, lastErr)
}

// joinCluster asks to be added as a voter, trying each address in turn (a
// follower answers 409) and retrying the whole list for up to 10s, since the
// cluster may not have settled on a leader yet.
func joinCluster(addrs, nodeID, raftAddr, brokerAddr string) error {
	body, _ := json.Marshal(map[string]string{"node_id": nodeID, "raft_addr": raftAddr, "broker_addr": brokerAddr})

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		for _, addr := range httpAddrs(addrs) {
			resp, err := joinHTTPClient.Post("http://"+addr+"/cluster/join", "application/json", bytes.NewReader(body))
			if err != nil {
				lastErr = err
				continue
			}
			var respBody struct {
				Error string `json:"error"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&respBody)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("%s: HTTP %d: %s", addr, resp.StatusCode, respBody.Error)
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(200 * time.Millisecond)
	}
}
