package groups

import (
	"reflect"
	"sort"
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
	// Sticky: a already had all 5, so it keeps its lowest 3 (its fair share)
	// and only the 2 freed from it move to b -- not a full round-robin reshuffle.
	if !reflect.DeepEqual(ha.Partitions, []uint32{0, 1, 2}) || !reflect.DeepEqual(hb.Partitions, []uint32{3, 4}) || ha.Generation != 2 {
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

func TestDescribeListsLiveGroupsOfOneTopic(t *testing.T) {
	c := New(10 * time.Second)
	now := fakeClock(c)

	if got := c.Describe("t", 3); len(got) != 0 {
		t.Fatalf("nothing joined yet, got %+v", got)
	}
	c.Join("t", "workers", "bob", 3)
	c.Join("t", "workers", "amy", 3)
	c.Join("t", "audit", "solo", 3)
	c.Join("other", "workers", "x", 3) // a different topic never shows up

	// bob joined first (got everything), then amy: sticky keeps bob's lowest
	// partition and moves the rest amy's way to reach a 2/1 fair split.
	want := []GroupInfo{
		{"audit", 1, []MemberInfo{{"solo", []uint32{0, 1, 2}}}},
		{"workers", 2, []MemberInfo{{"amy", []uint32{1, 2}}, {"bob", []uint32{0}}}},
	}
	if got := c.Describe("t", 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}

	// "audit" goes silent past the timeout and is evicted, so it drops out
	// entirely; a group whose last member left does too.
	*now = now.Add(6 * time.Second)
	c.Heartbeat("t", "workers", "amy", 3)
	c.Heartbeat("t", "workers", "bob", 3)
	*now = now.Add(6 * time.Second)
	c.Heartbeat("t", "workers", "amy", 3)
	c.Heartbeat("t", "workers", "bob", 3)
	c.Leave("other", "workers", "x")
	got := c.Describe("t", 3)
	if len(got) != 1 || got[0].Group != "workers" || len(got[0].Members) != 2 {
		t.Fatalf("after eviction: %+v", got)
	}
	if got := c.Describe("other", 3); len(got) != 0 {
		t.Fatalf("empty group should be omitted: %+v", got)
	}
}

func TestAuthorizeFencesCommits(t *testing.T) {
	c := New(10 * time.Second)
	now := fakeClock(c)
	const parts = 4

	a := c.Join("t", "g", "", parts) // alone: owns 0..3
	for p := uint32(0); p < parts; p++ {
		if err := c.Authorize("t", "g", a.Member, p, parts); err != nil {
			t.Fatalf("sole owner refused on partition %d: %v", p, err)
		}
	}
	if err := c.Authorize("t", "g", "stranger", 0, parts); err != ErrUnknownMember {
		t.Fatalf("non-member: %v", err)
	}

	// B joins: sticky keeps A's lowest 2 (0,1) and B gets the 2 freed from A
	// (2,3). A hasn't heartbeated, but the coordinator already refuses A's
	// commits for the partitions it lost.
	b := c.Join("t", "g", "", parts)
	for p, want := range map[uint32]error{0: nil, 1: nil, 2: ErrNotAssigned, 3: ErrNotAssigned} {
		if err := c.Authorize("t", "g", a.Member, p, parts); err != want {
			t.Errorf("A on partition %d: got %v, want %v", p, err, want)
		}
	}
	if err := c.Authorize("t", "g", b.Member, 2, parts); err != nil {
		t.Errorf("B on its own partition: %v", err)
	}

	// B goes silent and is evicted (A keeps heartbeating): B is now a stranger,
	// and A owns everything again.
	*now = now.Add(6 * time.Second)
	c.Heartbeat("t", "g", a.Member, parts)
	*now = now.Add(6 * time.Second)
	c.Heartbeat("t", "g", a.Member, parts)
	if err := c.Authorize("t", "g", b.Member, 1, parts); err != ErrUnknownMember {
		t.Errorf("evicted member: %v", err)
	}
	if err := c.Authorize("t", "g", a.Member, 1, parts); err != nil {
		t.Errorf("A after taking over: %v", err)
	}

	// Authorize alone does not keep a member alive.
	*now = now.Add(11 * time.Second)
	if err := c.Authorize("t", "g", a.Member, 0, parts); err != ErrUnknownMember {
		t.Errorf("silent member should time out even if it only commits: %v", err)
	}
}

// The actual point of stickiness: when a member leaves, only *its* partitions
// get redistributed. Members who kept heartbeating never lose a partition
// they already had, even though a from-scratch round-robin recompute would
// have reshuffled everything.
func TestAssignmentIsStickyAcrossMembershipChanges(t *testing.T) {
	c := New(time.Minute)
	fakeClock(c)
	const parts = 6

	a := c.Join("t", "g", "", parts) // alone: {0,1,2,3,4,5}
	b := c.Join("t", "g", "", parts) // a->{0,1,2}, b->{3,4,5}
	ha, _ := c.Heartbeat("t", "g", a.Member, parts)
	hb, _ := c.Heartbeat("t", "g", b.Member, parts)
	if !reflect.DeepEqual(ha.Partitions, []uint32{0, 1, 2}) || !reflect.DeepEqual(hb.Partitions, []uint32{3, 4, 5}) {
		t.Fatalf("setup: a=%v b=%v", ha.Partitions, hb.Partitions)
	}
	cc := c.Join("t", "g", "", parts) // third member takes 1 from each of a and b
	ha, _ = c.Heartbeat("t", "g", a.Member, parts)
	hb, _ = c.Heartbeat("t", "g", b.Member, parts)
	hc, _ := c.Heartbeat("t", "g", cc.Member, parts)
	if !reflect.DeepEqual(ha.Partitions, []uint32{0, 1}) || !reflect.DeepEqual(hb.Partitions, []uint32{3, 4}) || !reflect.DeepEqual(hc.Partitions, []uint32{2, 5}) {
		t.Fatalf("3-way split: a=%v b=%v c=%v", ha.Partitions, hb.Partitions, hc.Partitions)
	}

	// b leaves. a and c must keep exactly what they had (0,1 and 2,5) --
	// only b's old {3,4} may move.
	c.Leave("t", "g", b.Member)
	ha, _ = c.Heartbeat("t", "g", a.Member, parts)
	hc, _ = c.Heartbeat("t", "g", cc.Member, parts)
	if !reflect.DeepEqual(ha.Partitions, []uint32{0, 1, 3}) {
		t.Errorf("a should keep {0,1} and gain one of b's: got %v", ha.Partitions)
	}
	if !reflect.DeepEqual(hc.Partitions, []uint32{2, 4, 5}) {
		t.Errorf("c should keep {2,5} and gain one of b's: got %v", hc.Partitions)
	}

	all := append(append([]uint32{}, ha.Partitions...), hc.Partitions...)
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if !reflect.DeepEqual(all, []uint32{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("partitions lost or duplicated after leave: %v", all)
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
