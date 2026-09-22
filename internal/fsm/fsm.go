// Package fsm is the cluster's replicated state machine: the thing every
// Raft log entry, applied in the same order on every node, deterministically
// updates. It owns topic-to-partition-to-node assignment.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

type PartitionAssignment struct {
	Partition uint32 `json:"partition"`
	NodeID    string `json:"node_id"`
}

type TopicInfo struct {
	Name       string                `json:"name"`
	Partitions []PartitionAssignment `json:"partitions"`
}

// FSM assigns partitions round-robin over its live node set. That set is
// itself replicated cluster state (AddNode/RemoveNode), not a fixed
// construction-time list, so a node added or removed after bootstrap
// affects placement for every *topic created after* the change -- existing
// topics' assignments never move (see the "no rebalancing" limitation).
type FSM struct {
	mu     sync.RWMutex
	nodes  map[string]string // node id -> millrace-core address ("" if none)
	topics map[string]TopicInfo

	// OnTopicCreated, if set before the node starts, is called (in its own
	// goroutine) each time a CreateTopic is applied on this node. Not called
	// for topics restored from a snapshot.
	OnTopicCreated func(TopicInfo)
}

// New seeds the initial membership. Every node must be started with the same
// seed at first bootstrap; a node joining afterwards should instead fetch the
// live set from an existing member (see cmd/millrace-cluster --join) since
// the original bootstrap membership is never itself replicated through the
// Raft log -- only changes made via AddNode/RemoveNode are.
func New(nodes map[string]string) *FSM {
	seed := make(map[string]string, len(nodes))
	for id, addr := range nodes {
		seed[id] = addr
	}
	return &FSM{nodes: seed, topics: make(map[string]TopicInfo)}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("invalid command: %w", err)
	}
	switch cmd.Type {
	case CommandCreateTopic:
		return f.applyCreateTopic(cmd.CreateTopic)
	case CommandAddNode:
		f.mu.Lock()
		f.nodes[cmd.AddNode.NodeID] = cmd.AddNode.BrokerAddr
		f.mu.Unlock()
		return nil
	case CommandRemoveNode:
		f.mu.Lock()
		delete(f.nodes, cmd.RemoveNode.NodeID)
		f.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("unknown command type %q", cmd.Type)
	}
}

func (f *FSM) applyCreateTopic(c *CreateTopicCommand) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.topics[c.Name]; exists {
		return fmt.Errorf("topic %q already exists", c.Name)
	}
	if c.NumPartitions == 0 {
		return fmt.Errorf("num_partitions must be >= 1")
	}
	if len(f.nodes) == 0 {
		return fmt.Errorf("no nodes to place partitions on")
	}

	nodeIDs := make([]string, 0, len(f.nodes))
	for id := range f.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	partitions := make([]PartitionAssignment, c.NumPartitions)
	for i := uint32(0); i < c.NumPartitions; i++ {
		node := nodeIDs[int(i)%len(nodeIDs)]
		partitions[i] = PartitionAssignment{Partition: i, NodeID: node}
	}
	topic := TopicInfo{Name: c.Name, Partitions: partitions}
	f.topics[c.Name] = topic
	if f.OnTopicCreated != nil {
		go f.OnTopicCreated(topic)
	}
	return topic
}

// Nodes is a direct (non-Raft-log) read of this node's current membership:
// node id -> millrace-core address. Safe on a follower, which may lag.
func (f *FSM) Nodes() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.nodes))
	for id, addr := range f.nodes {
		out[id] = addr
	}
	return out
}

// NodeBroker returns one node's millrace-core address, if it's a member.
func (f *FSM) NodeBroker(id string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	addr, ok := f.nodes[id]
	return addr, ok
}

func (f *FSM) Topic(name string) (TopicInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.topics[name]
	return t, ok
}

// ListTopics is a direct (non-Raft-log) read of this node's current state.
// Safe to call on a follower, which may briefly lag the leader -- same
// caveat as any Raft read that isn't routed through the log.
func (f *FSM) ListTopics() []TopicInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]TopicInfo, 0, len(f.topics))
	for _, t := range f.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

type snapshotState struct {
	Topics map[string]TopicInfo `json:"topics"`
	Nodes  map[string]string    `json:"nodes"`
}

type fsmSnapshot struct {
	state snapshotState
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state := snapshotState{
		Topics: make(map[string]TopicInfo, len(f.topics)),
		Nodes:  make(map[string]string, len(f.nodes)),
	}
	for k, v := range f.topics {
		state.Topics[k] = v
	}
	for k, v := range f.nodes {
		state.Nodes[k] = v
	}
	return &fsmSnapshot{state: state}, nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state snapshotState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics = state.Topics
	f.nodes = state.Nodes
	return nil
}
