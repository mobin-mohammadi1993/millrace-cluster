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

// CreateTopic returns ErrExists if the broker already has the topic.
func CreateTopic(addr, name string, partitions uint32) error {
	body := nameBody(1, name)
	body = binary.LittleEndian.AppendUint32(body, partitions)
	_, err := roundTrip(addr, body)
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
// topic on the broker at addr. Runs async so a dead broker can't stall Raft.
func Mirror(addr string) func(fsm.TopicInfo) {
	return func(t fsm.TopicInfo) {
		err := CreateTopic(addr, t.Name, uint32(len(t.Partitions)))
		if err != nil && !errors.Is(err, ErrExists) {
			log.Printf("broker %s: mirroring topic %q failed (not retried): %v", addr, t.Name, err)
		}
	}
}
