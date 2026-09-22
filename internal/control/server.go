// Package control implements the gRPC Control service defined in
// proto/control.proto, backed by a raft.Raft node and its FSM.
package control

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"

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
		Type:        fsm.CommandCreateTopic,
		CreateTopic: &fsm.CreateTopicCommand{Name: req.Name, NumPartitions: req.NumPartitions},
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
		})
	}
	return pt
}
