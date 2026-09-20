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
)

type Server struct {
	pb.UnimplementedControlServer
	Raft *raft.Raft
	FSM  *fsm.FSM
	// Brokers maps node ID -> that node's millrace-core address (static,
	// identical on every node), used to answer routing lookups.
	Brokers map[string]string
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
		return &pb.CreateTopicResponse{Ok: true, Topic: toProtoTopic(v)}, nil
	default:
		return nil, fmt.Errorf("unexpected FSM response type %T", v)
	}
}

func (s *Server) ClusterState(ctx context.Context, req *pb.ClusterStateRequest) (*pb.ClusterStateResponse, error) {
	_, leaderID := s.Raft.LeaderWithID()
	resp := &pb.ClusterStateResponse{LeaderId: string(leaderID)}
	for _, t := range s.FSM.ListTopics() {
		resp.Topics = append(resp.Topics, toProtoTopic(t))
	}
	return resp, nil
}

func toProtoTopic(t fsm.TopicInfo) *pb.TopicInfo {
	pt := &pb.TopicInfo{Name: t.Name}
	for _, p := range t.Partitions {
		pt.Partitions = append(pt.Partitions, &pb.PartitionAssignment{
			Partition: p.Partition,
			NodeId:    p.NodeID,
		})
	}
	return pt
}
