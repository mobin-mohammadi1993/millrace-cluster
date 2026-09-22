// Package control implements the gRPC Control service defined in
// proto/control.proto, backed by a raft.Raft node and its FSM.
package control

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/broker"
	pb "millrace-cluster/internal/controlpb"
	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/groups"
)

type Server struct {
	pb.UnimplementedControlServer
	Raft   *raft.Raft
	FSM    *fsm.FSM
	Groups *groups.Coordinator
}

// applyCommand submits cmd through Raft and waits for it to commit, returning
// whatever error the FSM's Apply reported (nil on success). Requires this
// node to be the leader -- callers check that first, since Raft.Apply on a
// follower fails anyway but with a less specific error.
func (s *Server) applyCommand(cmd fsm.Command) error {
	data, err := cmd.Encode()
	if err != nil {
		return err
	}
	future := s.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	if err, ok := future.Response().(error); ok {
		return err
	}
	return nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if s.Raft.State() != raft.Leader {
		return &pb.CreateTopicResponse{
			Ok:    false,
			Error: fmt.Sprintf("not leader; current leader raft addr = %s", s.Raft.Leader()),
		}, nil
	}

	cmd := fsm.Command{
		Type: fsm.CommandCreateTopic,
		CreateTopic: &fsm.CreateTopicCommand{
			Name:              req.Name,
			NumPartitions:     req.NumPartitions,
			ReplicationFactor: req.ReplicationFactor,
		},
	}
	data, err := cmd.Encode()
	if err != nil {
		return nil, err
	}

	future := s.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return &pb.CreateTopicResponse{Ok: false, Error: err.Error()}, nil
	}

	switch v := future.Response().(type) {
	case error:
		return &pb.CreateTopicResponse{Ok: false, Error: v.Error()}, nil
	case fsm.TopicInfo:
		return &pb.CreateTopicResponse{Ok: true, Topic: toProtoTopic(v, s.FSM.Nodes())}, nil
	default:
		return nil, fmt.Errorf("unexpected FSM response type %T", v)
	}
}

// PromotePartition is the shared failover primitive: promote nodeID's real
// broker to leader for topic/partition, apply the matching FSM metadata
// update, then best-effort fence the old leader AND re-point every other
// surviving replica at the new leader (both via DemoteToFollower). The
// other-replicas repoint matters as much as fencing the old leader: without
// it, a partition with replication_factor >= 3 leaves its other followers
// forever replicating from the (possibly dead) old leader instead of the
// new one, which also means the new leader's own ack-quorum can never be
// satisfied by them. Used by both the manual POST /partitions/promote
// handler (http.go) and the automatic failure detector (failover.go) --
// same guarantees either way, the only difference is who decided to call it
// and why. Requires this node to be the Raft leader (checked by the caller,
// same as every other write path in this package).
func (s *Server) PromotePartition(topic string, partition uint32, nodeID string) (fenced bool, err error) {
	t, ok := s.FSM.Topic(topic)
	if !ok || int(partition) >= len(t.Partitions) {
		return false, fmt.Errorf("unknown topic or partition")
	}
	p := t.Partitions[partition]
	if p.NodeID == nodeID {
		return true, nil // already the leader: nothing to fence
	}
	addr, ok := s.FSM.NodeBroker(nodeID)
	if !ok || addr == "" {
		return false, fmt.Errorf("unknown node or no broker configured for it")
	}
	newGen := p.Generation + 1
	totalReplicas := uint32(1 + len(p.Replicas))

	// Promote the real broker first: if this fails, the cluster's metadata
	// is left untouched, so retrying (or trying another node) is safe.
	if err := broker.PromoteToLeader(addr, topic, partition, newGen, totalReplicas); err != nil {
		return false, err
	}
	cmd := fsm.Command{Type: fsm.CommandPromotePartition, PromotePartition: &fsm.PromotePartitionCommand{
		Topic: topic, Partition: partition, NodeID: nodeID, Generation: newGen,
	}}
	if err := s.applyCommand(cmd); err != nil {
		return false, err
	}

	// Fence the old leader, best-effort: if it's unreachable (the common
	// case for a real failover -- that's usually *why* this is being
	// called), there's nothing more to do and no real split-brain risk
	// since a broker that's down accepts no writes from anyone.
	fenced = false
	if oldAddr, ok := s.FSM.NodeBroker(p.NodeID); ok && oldAddr != "" {
		fenced = broker.DemoteToFollower(oldAddr, topic, partition, addr, newGen) == nil
	}

	// Re-point every other surviving replica at the new leader, best-effort
	// (a replica that's also down just stays unreplicated until it comes
	// back and gets mirrored again on the next topic creation -- there's no
	// retry loop for this today, a real limitation, not a hidden one).
	for _, r := range p.Replicas {
		if r == nodeID || r == p.NodeID {
			continue // the new leader itself, or the old leader (handled above)
		}
		if rAddr, ok := s.FSM.NodeBroker(r); ok && rAddr != "" {
			_ = broker.DemoteToFollower(rAddr, topic, partition, addr, newGen)
		}
	}
	return fenced, nil
}

func (s *Server) ClusterState(ctx context.Context, req *pb.ClusterStateRequest) (*pb.ClusterStateResponse, error) {
	_, leaderID := s.Raft.LeaderWithID()
	resp := &pb.ClusterStateResponse{LeaderId: string(leaderID)}
	brokers := s.FSM.Nodes()
	for _, t := range s.FSM.ListTopics() {
		resp.Topics = append(resp.Topics, toProtoTopic(t, brokers))
	}
	return resp, nil
}

func toProtoTopic(t fsm.TopicInfo, brokers map[string]string) *pb.TopicInfo {
	pt := &pb.TopicInfo{Name: t.Name}
	for _, p := range t.Partitions {
		pt.Partitions = append(pt.Partitions, &pb.PartitionAssignment{
			Partition:  p.Partition,
			NodeId:     p.NodeID,
			BrokerAddr: brokers[p.NodeID],
			ReplicaIds: p.Replicas,
		})
	}
	return pt
}
