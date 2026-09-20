// Package groups is the cluster's consumer-group coordinator: membership and
// partition assignment. It is soft, in-memory state on whichever node is the
// Raft leader (heartbeats through the Raft log would swamp it); durable
// progress lives in the brokers' committed offsets. After a leader change the
// new leader knows no members, so they get ErrUnknownMember, re-join, and
// resume from their commits.
//
// ponytail: groups are never deleted once created (a leak only if group names
// are unbounded). Commit fencing (Authorize) applies only to commits routed
// through the coordinator; brokers know nothing about membership, so a client
// that commits to a broker directly is not fenced.
package groups

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrUnknownMember = errors.New("unknown member: rejoin")
	ErrNotAssigned   = errors.New("fenced: partition is not assigned to this member")
)

// State is what a member is told: who it is, the group's current generation
// (bumped on every membership change), and the partitions it should consume.
type State struct {
	Member     string   `json:"member"`
	Generation int      `json:"generation"`
	Partitions []uint32 `json:"partitions"`
}

type group struct {
	generation int
	seen       map[string]time.Time // member -> last join/heartbeat
}

type Coordinator struct {
	mu      sync.Mutex
	timeout time.Duration
	now     func() time.Time
	nextID  int
	groups  map[string]*group // key: topic + "\x00" + group name
}

func New(sessionTimeout time.Duration) *Coordinator {
	return &Coordinator{timeout: sessionTimeout, now: time.Now, groups: map[string]*group{}}
}

// locked: returns the group with expired members already evicted.
func (c *Coordinator) group(topic, name string) *group {
	key := topic + "\x00" + name
	g := c.groups[key]
	if g == nil {
		g = &group{seen: map[string]time.Time{}}
		c.groups[key] = g
	}
	for m, t := range g.seen {
		if c.now().Sub(t) > c.timeout {
			delete(g.seen, m)
			g.generation++
		}
	}
	return g
}

// Partitions p of the topic go round-robin over the sorted members.
func assignment(g *group, member string, partitions int) []uint32 {
	members := make([]string, 0, len(g.seen))
	for m := range g.seen {
		members = append(members, m)
	}
	sort.Strings(members)
	mine := []uint32{}
	for i, m := range members {
		if m != member {
			continue
		}
		for p := i; p < partitions; p += len(members) {
			mine = append(mine, uint32(p))
		}
	}
	return mine
}

type MemberInfo struct {
	Member     string   `json:"member"`
	Partitions []uint32 `json:"partitions"`
}

type GroupInfo struct {
	Group      string       `json:"group"`
	Generation int          `json:"generation"`
	Members    []MemberInfo `json:"members"`
}

// Describe lists the topic's groups that currently have live members (expired
// ones are evicted first), sorted by group name, members sorted by id.
func (c *Coordinator) Describe(topic string, partitions int) []GroupInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []GroupInfo{}
	for key := range c.groups {
		name, ok := strings.CutPrefix(key, topic+"\x00")
		if !ok {
			continue
		}
		g := c.group(topic, name)
		if len(g.seen) == 0 {
			continue
		}
		info := GroupInfo{Group: name, Generation: g.generation, Members: []MemberInfo{}}
		for m := range g.seen {
			info.Members = append(info.Members, MemberInfo{m, assignment(g, m, partitions)})
		}
		sort.Slice(info.Members, func(i, j int) bool { return info.Members[i].Member < info.Members[j].Member })
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out
}

// Join adds the member (an empty id gets a new one; an unknown id is accepted
// as-is, which is how a member re-joins under its old identity after a leader
// change) or just refreshes it if already present.
func (c *Coordinator) Join(topic, name, member string, partitions int) State {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.group(topic, name)
	if member == "" {
		c.nextID++
		member = fmt.Sprintf("m-%d", c.nextID)
	}
	if _, ok := g.seen[member]; !ok {
		g.generation++
	}
	g.seen[member] = c.now()
	return State{member, g.generation, assignment(g, member, partitions)}
}

func (c *Coordinator) Heartbeat(topic, name, member string, partitions int) (State, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.group(topic, name)
	if _, ok := g.seen[member]; !ok {
		return State{}, ErrUnknownMember
	}
	g.seen[member] = c.now()
	return State{member, g.generation, assignment(g, member, partitions)}, nil
}

// Authorize is the fence for commits: only a live member that is *currently*
// assigned the partition may commit it. An evicted member is unknown; a member
// whose partition was rebalanced away gets ErrNotAssigned even if it hasn't
// noticed yet. (It does not refresh the session -- only heartbeats do.)
func (c *Coordinator) Authorize(topic, name, member string, partition uint32, partitions int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.group(topic, name)
	if _, ok := g.seen[member]; !ok {
		return ErrUnknownMember
	}
	for _, p := range assignment(g, member, partitions) {
		if p == partition {
			return nil
		}
	}
	return ErrNotAssigned
}

func (c *Coordinator) Leave(topic, name, member string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.group(topic, name)
	if _, ok := g.seen[member]; ok {
		delete(g.seen, member)
		g.generation++
	}
}
