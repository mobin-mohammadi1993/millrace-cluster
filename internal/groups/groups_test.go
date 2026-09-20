package groups

import (
	"reflect"
	"testing"
	"time"
)

func fakeClock(c *Coordinator) *time.Time {
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	return &now
}

func TestMembershipAssignmentAndEviction(t *testing.T) {
	c := New(10 * time.Second)
	now := fakeClock(c)
	const parts = 5

	a := c.Join("t", "g", "", parts)
	if a.Member == "" || a.Generation != 1 || !reflect.DeepEqual(a.Partitions, []uint32{0, 1, 2, 3, 4}) {
		t.Fatalf("first member should own everything: %+v", a)
	}

	// Refreshing an existing member changes nothing.
	if again := c.Join("t", "g", a.Member, parts); again.Generation != 1 {
		t.Fatalf("re-join bumped the generation: %+v", again)
	}

	b := c.Join("t", "g", "", parts)
	if b.Generation != 2 || a.Member == b.Member {
		t.Fatalf("second member: %+v", b)
	}
	ha, _ := c.Heartbeat("t", "g", a.Member, parts)
	hb, _ := c.Heartbeat("t", "g", b.Member, parts)
	// Round-robin over sorted ids: m-1 gets 0,2,4 and m-2 gets 1,3.
	if !reflect.DeepEqual(ha.Partitions, []uint32{0, 2, 4}) || !reflect.DeepEqual(hb.Partitions, []uint32{1, 3}) || ha.Generation != 2 {
		t.Fatalf("split wrong: a=%+v b=%+v", ha, hb)
	}

	// Groups are per (topic, name).
	if o := c.Join("t", "other", "", parts); o.Generation != 1 || len(o.Partitions) != parts {
		t.Fatalf("groups leaked into each other: %+v", o)
	}
	if o := c.Join("t2", "g", "", parts); o.Generation != 1 {
		t.Fatalf("topics leaked into each other: %+v", o)
	}

	// b stops heartbeating; a keeps going. After the timeout b is evicted,
	// the generation bumps, and a owns everything again.
	*now = now.Add(6 * time.Second)
	c.Heartbeat("t", "g", a.Member, parts)
	*now = now.Add(6 * time.Second)
	ha, err := c.Heartbeat("t", "g", a.Member, parts)
	if err != nil || ha.Generation != 3 || len(ha.Partitions) != parts {
		t.Fatalf("after eviction: %+v err=%v", ha, err)
	}
	if _, err := c.Heartbeat("t", "g", b.Member, parts); err != ErrUnknownMember {
		t.Fatalf("evicted member should be unknown, got %v", err)
	}

	// An evicted member re-joins under its old id; an explicit leave bumps too.
	if r := c.Join("t", "g", b.Member, parts); r.Member != b.Member || r.Generation != 4 {
		t.Fatalf("re-join after eviction: %+v", r)
	}
	c.Leave("t", "g", b.Member)
	c.Leave("t", "g", b.Member) // leaving twice is harmless
	if ha, _ = c.Heartbeat("t", "g", a.Member, parts); ha.Generation != 5 || len(ha.Partitions) != parts {
		t.Fatalf("after leave: %+v", ha)
	}
}

func TestMoreMembersThanPartitions(t *testing.T) {
	c := New(time.Minute)
	fakeClock(c)
	var owned int
	var last State
	for i := 0; i < 4; i++ {
		last = c.Join("t", "g", "", 2)
	}
	for _, m := range []string{"m-1", "m-2", "m-3", "m-4"} {
		st, _ := c.Heartbeat("t", "g", m, 2)
		owned += len(st.Partitions)
	}
	if owned != 2 || len(last.Partitions) != 0 {
		t.Fatalf("2 partitions across 4 members: owned=%d, last member got %v", owned, last.Partitions)
	}
}
