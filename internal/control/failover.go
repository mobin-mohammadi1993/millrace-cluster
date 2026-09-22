package control

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/broker"
)

// FailoverConfig tunes the automatic failure detector started by
// Server.RunFailureDetector.
type FailoverConfig struct {
	CheckInterval    time.Duration // how often to health-check every partition's current leader
	FailureThreshold int           // consecutive failed checks before promoting a replacement
}

func DefaultFailoverConfig() FailoverConfig {
	return FailoverConfig{CheckInterval: 2 * time.Second, FailureThreshold: 3}
}

// RunFailureDetector health-checks every partition's current leader broker
// and, after FailureThreshold consecutive failures, automatically promotes
// the first replica whose own broker answers a health check -- the
// automatic half of failover (see PromotePartition for the manual one,
// which this reuses, so both paths carry the exact same fencing
// guarantees). Only acts while this node is the Raft leader (checked every
// tick, since only the leader can Apply a promote). Blocks until stop is
// closed (or forever if stop is nil, matching this process's real
// lifetime); run it in its own goroutine.
//
// Failure counts are soft, in-memory state on whichever node happens to be
// leader right now -- the same "soft state on the Raft leader" trade-off
// this project already makes for consumer-group membership (see the groups
// package): a Raft leader change resets detection to zero, so a partition
// whose broker leader just died right as the cluster's own leader changed
// takes one full detection window longer to fail over. Documented, not
// hidden. Health checks for every partition run sequentially each tick
// (ponytail: fine at this project's scale; parallelize if partition count
// or CheckInterval ever makes that the bottleneck).
func (s *Server) RunFailureDetector(stop <-chan struct{}, cfg FailoverConfig) {
	failures := map[string]int{} // "topic/partition" -> consecutive failed checks

	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		if s.Raft.State() != raft.Leader {
			continue // only the Raft leader can Apply a promote; nothing to do yet
		}
		s.checkAndFailover(failures, cfg.FailureThreshold)
	}
}

func (s *Server) checkAndFailover(failures map[string]int, threshold int) {
	brokers := s.FSM.Nodes()
	for _, t := range s.FSM.ListTopics() {
		for _, p := range t.Partitions {
			key := fmt.Sprintf("%s/%d", t.Name, p.Partition)
			if healthy(brokers[p.NodeID], t.Name) {
				failures[key] = 0
				continue
			}
			failures[key]++
			if failures[key] < threshold {
				continue
			}

			candidate := ""
			for _, r := range p.Replicas {
				if healthy(brokers[r], t.Name) {
					candidate = r
					break
				}
			}
			if candidate == "" {
				continue // no healthy replica to promote to; try again next tick
			}
			fenced, err := s.PromotePartition(t.Name, p.Partition, candidate)
			if err != nil {
				log.Printf("auto-failover: promoting %s to leader of %s failed: %v", candidate, key, err)
				continue
			}
			log.Printf("auto-failover: promoted %s to leader of %s (fenced old leader: %v)", candidate, key, fenced)
			failures[key] = 0
		}
	}
}

func healthy(addr, topic string) bool {
	if addr == "" {
		return false
	}
	_, err := broker.DescribeTopic(addr, topic)
	return err == nil
}
