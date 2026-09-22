// Package raftnode wires up one hashicorp/raft node: TCP transport, file
// snapshot store, and either a static bootstrap configuration or -- if
// Config.Peers is left empty -- a blank start for a node that will join an
// existing cluster.
package raftnode

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

type Config struct {
	NodeID   string
	BindAddr string
	DataDir  string
	// Peers is the full, static cluster configuration -- identical on every
	// node -- used only to bootstrap a brand-new cluster. Leave it empty for
	// a node joining an existing cluster (cmd/millrace-cluster --join): it
	// starts blank and waits for the leader to add it via raft.AddVoter,
	// which replicates the real configuration to it automatically.
	Peers []raft.Server
}

func Start(fsm raft.FSM, cfg Config) (*raft.Raft, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	// Tuned down from raft's production defaults (seconds) so tests and
	// local demos see leader election in well under a second.
	raftCfg.HeartbeatTimeout = 200 * time.Millisecond
	raftCfg.ElectionTimeout = 200 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 100 * time.Millisecond
	raftCfg.CommitTimeout = 50 * time.Millisecond

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving raft bind addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating raft transport: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot store: %w", err)
	}

	// In-memory log/stable stores: a restarted process starts with a
	// clean slate rather than replaying its own prior log. Acceptable for
	// this MVP (a fresh process still catches up via replication from the
	// live quorum); disk-backed stores are a documented fast-follow.
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("creating raft node: %w", err)
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshots)
	if err != nil {
		return nil, err
	}
	if !hasState && len(cfg.Peers) > 0 {
		future := r.BootstrapCluster(raft.Configuration{Servers: cfg.Peers})
		if err := future.Error(); err != nil {
			return nil, fmt.Errorf("bootstrapping cluster: %w", err)
		}
	}

	return r, nil
}
