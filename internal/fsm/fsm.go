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

// FSM assigns partitions round-robin over nodeIDs, which every node in the
// cluster was started with identically (see cmd/millrace-cluster's static
// --peers flag) -- so placement is deterministic across replicas without
// node membership itself needing to go through the replicated log.
type FSM struct {
	mu      sync.RWMutex
	nodeIDs []string
	topics  map[string]TopicInfo

	// OnTopicCreated, if set before the node starts, is called (in its own
	// goroutine) each time a CreateTopic is applied on this node. Not called
	// for topics restored from a snapshot.
	OnTopicCreated func(TopicInfo)
}

func New(nodeIDs []string) *FSM {
	ids := make([]string, len(nodeIDs))
	copy(ids, nodeIDs)
	sort.Strings(ids)
	return &FSM{nodeIDs: ids, topics: make(map[string]TopicInfo)}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("invalid command: %w", err)
	}
	switch cmd.Type {
	case CommandCreateTopic:
		return f.applyCreateTopic(cmd.CreateTopic)
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

	partitions := make([]PartitionAssignment, c.NumPartitions)
	for i := uint32(0); i < c.NumPartitions; i++ {
		node := f.nodeIDs[int(i)%len(f.nodeIDs)]
		partitions[i] = PartitionAssignment{Partition: i, NodeID: node}
	}
	topic := TopicInfo{Name: c.Name, Partitions: partitions}
	f.topics[c.Name] = topic
	if f.OnTopicCreated != nil {
		go f.OnTopicCreated(topic)
	}
	return topic
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

type fsmSnapshot struct {
	topics map[string]TopicInfo
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	topicsCopy := make(map[string]TopicInfo, len(f.topics))
	for k, v := range f.topics {
		topicsCopy[k] = v
	}
	return &fsmSnapshot{topics: topicsCopy}, nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.topics)
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
	var topics map[string]TopicInfo
	if err := json.NewDecoder(rc).Decode(&topics); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics = topics
	return nil
}
