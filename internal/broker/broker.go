// Package broker is the smallest possible client for millrace-core's wire
// protocol (see millrace-core/README.md): just enough to mirror cluster
// topics onto a local broker and to check that it worked.
package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"time"

	"millrace-cluster/internal/fsm"
)

var ErrExists = errors.New("topic already exists")

func roundTrip(addr string, body []byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	frame := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(frame, uint32(len(body)))
	copy(frame[4:], body)
	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	resp := make([]byte, binary.LittleEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, err
	}
	if resp[0] == 1 { // Error: u16 len, message
		n := int(binary.LittleEndian.Uint16(resp[1:3]))
		return nil, errors.New(string(resp[3 : 3+n]))
	}
	return resp, nil
}

func nameBody(op byte, name string) []byte {
	body := []byte{op, 0, 0}
	binary.LittleEndian.PutUint16(body[1:], uint16(len(name)))
	return append(body, name...)
}

// CreateTopic makes the broker serve every partition of the topic; use
// CreateTopicOwned to restrict it. Returns ErrExists if the topic exists.
func CreateTopic(addr, name string, partitions uint32) error {
	body := nameBody(1, name)
	body = binary.LittleEndian.AppendUint32(body, partitions)
	return createErr(roundTrip(addr, body))
}

// CreateTopicOwned makes the broker serve, and accept writes for, only the
// listed partitions (op 5).
func CreateTopicOwned(addr, name string, partitions uint32, owned []uint32) error {
	body := nameBody(5, name)
	body = binary.LittleEndian.AppendUint32(body, partitions)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(owned)))
	for _, p := range owned {
		body = binary.LittleEndian.AppendUint32(body, p)
	}
	return createErr(roundTrip(addr, body))
}

// PartitionRole is one partition's role to hand a broker via
// CreateTopicReplica: Leader accepts client writes directly; a Follower
// (Leader == false) replicates from LeaderAddr instead. Epoch fences stale
// role changes on the broker (see millrace-core's Topic::promote/demote);
// Replicas (Leader only) is the partition's total replica count, which the
// broker uses to compute its ack-quorum majority.
type PartitionRole struct {
	Partition  uint32
	Leader     bool
	LeaderAddr string // only used when Leader is false
	Epoch      uint64
	Replicas   uint32 // only used when Leader is true
}

// CreateTopicReplica makes the broker serve exactly the listed partitions,
// each with an explicit role (op 9) -- the replication-aware sibling of
// CreateTopicOwned, where every owned partition is implicitly a leader.
func CreateTopicReplica(addr, name string, partitions uint32, roles []PartitionRole) error {
	body := nameBody(9, name)
	body = binary.LittleEndian.AppendUint32(body, partitions)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(roles)))
	for _, r := range roles {
		body = binary.LittleEndian.AppendUint32(body, r.Partition)
		if r.Leader {
			body = append(body, 0)
			body = binary.LittleEndian.AppendUint64(body, r.Epoch)
			body = binary.LittleEndian.AppendUint32(body, r.Replicas)
		} else {
			body = append(body, 1)
			body = binary.LittleEndian.AppendUint16(body, uint16(len(r.LeaderAddr)))
			body = append(body, r.LeaderAddr...)
			body = binary.LittleEndian.AppendUint64(body, r.Epoch)
		}
	}
	return createErr(roundTrip(addr, body))
}

// PromoteToLeader turns a Follower partition on this broker into a Leader at
// epoch (op 10) -- the manual failover primitive; a no-op if already Leader
// at exactly this epoch (retry-safe), rejected if epoch isn't strictly
// newer than the partition's current epoch (stale). replicas is the
// partition's total replica count, for this new leader's own quorum math.
func PromoteToLeader(addr, topic string, partition uint32, epoch uint64, replicas uint32) error {
	body := nameBody(10, topic)
	body = binary.LittleEndian.AppendUint32(body, partition)
	body = binary.LittleEndian.AppendUint64(body, epoch)
	body = binary.LittleEndian.AppendUint32(body, replicas)
	_, err := roundTrip(addr, body)
	return err
}

// DemoteToFollower turns a partition into a Follower of leaderAddr at epoch
// (op 11) -- the fencing half of a promote: called on the *old* leader right
// after promoting a replica, so it stops accepting writes instead of
// risking split-brain. Rejected if epoch isn't strictly newer than the
// partition's current epoch; a no-op if already a Follower of exactly this
// leader at exactly this epoch (retry-safe).
func DemoteToFollower(addr, topic string, partition uint32, leaderAddr string, epoch uint64) error {
	body := nameBody(11, topic)
	body = binary.LittleEndian.AppendUint32(body, partition)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(leaderAddr)))
	body = append(body, leaderAddr...)
	body = binary.LittleEndian.AppendUint64(body, epoch)
	_, err := roundTrip(addr, body)
	return err
}

// CommitOffset records a consumer group's committed offset on the broker that
// owns the partition (the broker rejects an offset past the end of the log).
func CommitOffset(addr, topic string, partition uint32, group string, offset uint64) error {
	body := nameBody(6, topic)
	body = binary.LittleEndian.AppendUint32(body, partition)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(group)))
	body = append(body, group...)
	body = binary.LittleEndian.AppendUint64(body, offset)
	_, err := roundTrip(addr, body)
	return err
}

func createErr(_ []byte, err error) error {
	if err != nil && err.Error() == ErrExists.Error() {
		return ErrExists
	}
	return err
}

// DescribeTopic returns the broker's partition count for a topic (an error if unknown).
func DescribeTopic(addr, name string) (int, error) {
	resp, err := roundTrip(addr, nameBody(4, name))
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(resp[1:5])), nil
}

// Mirror returns an fsm.FSM.OnTopicCreated hook that creates each committed
// topic on the broker at addr: as Leader for partitions the FSM assigned
// nodeID as leader, as Follower (replicating from the leader's own broker)
// for partitions where nodeID is a replica, and absent from every other
// partition. Runs async so a dead broker can't stall Raft.
func Mirror(addr, nodeID string, f *fsm.FSM) func(fsm.TopicInfo) {
	return func(t fsm.TopicInfo) {
		var roles []PartitionRole
		for _, p := range t.Partitions {
			switch {
			case p.NodeID == nodeID:
				roles = append(roles, PartitionRole{
					Partition: p.Partition, Leader: true,
					Epoch: p.Generation, Replicas: uint32(1 + len(p.Replicas)),
				})
			case isReplica(p.Replicas, nodeID):
				leaderAddr, _ := f.NodeBroker(p.NodeID)
				roles = append(roles, PartitionRole{
					Partition: p.Partition, LeaderAddr: leaderAddr, Epoch: p.Generation,
				})
			}
		}
		err := CreateTopicReplica(addr, t.Name, uint32(len(t.Partitions)), roles)
		if err != nil && !errors.Is(err, ErrExists) {
			log.Printf("broker %s: mirroring topic %q failed (not retried): %v", addr, t.Name, err)
		}
	}
}

func isReplica(replicas []string, nodeID string) bool {
	for _, r := range replicas {
		if r == nodeID {
			return true
		}
	}
	return false
}
