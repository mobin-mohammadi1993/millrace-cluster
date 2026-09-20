// Command millrace-cluster runs one node of a static-membership Raft
// cluster with a gRPC control plane for cluster-aware topic creation.
package main

import (
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
		"(same on every node), e.g. n1=127.0.0.1:7000,n2=127.0.0.1:7001,n3=127.0.0.1:7002 (required)")
	brokersFlag := flag.String("brokers", "", "optional id=millrace-core_addr list, same on every node: "+
		"committed topics are created on this node's own broker, and /route answers with these addresses")
	httpAddr := flag.String("http-addr", "", "optional listen address for the JSON routing/topics API")
	groupTimeout := flag.Duration("group-session-timeout", 10*time.Second, "a consumer-group member that hasn't heartbeated for this long is evicted")
	flag.Parse()

	if *nodeID == "" || *peersFlag == "" {
		log.Fatal("--node-id and --peers are required")
	}

	peerIDs, servers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("parsing --peers: %v", err)
	}

	brokers := map[string]string{}
	if *brokersFlag != "" {
		for _, p := range strings.Split(*brokersFlag, ",") {
			kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
			if len(kv) != 2 {
				log.Fatalf("invalid --brokers entry %q, want id=addr", p)
			}
			brokers[kv[0]] = kv[1]
		}
	}

	f := fsm.New(peerIDs)
	if own, ok := brokers[*nodeID]; ok {
		f.OnTopicCreated = broker.Mirror(own, *nodeID)
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
	srv := &control.Server{Raft: r, FSM: f, Brokers: brokers, Groups: groups.New(*groupTimeout)}
	pb.RegisterControlServer(grpcServer, srv)
	if *httpAddr != "" {
		go func() { log.Fatalf("http serve: %v", http.ListenAndServe(*httpAddr, srv.HTTPHandler())) }()
	}

	log.Printf("millrace-cluster node %q ready: raft=%s grpc=%s peers=%v", *nodeID, *raftAddr, *grpcAddr, peerIDs)
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
