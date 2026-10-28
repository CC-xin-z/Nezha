package kvstore

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func tailTestKey(prefix string, i int) string {
	return fmt.Sprintf("%s-%08d", prefix, i)
}

func TestTailFilterConservativeUntilMarkedExact(t *testing.T) {
	f := NewTailFilter(256, 10)
	if !f.MightContain("missing") {
		t.Fatal("new filter must be conservative")
	}
	f.MarkExact()
	if f.MightContain("missing") {
		t.Fatal("exact empty filter should reject an absent key")
	}
}

func TestTailFilterHasNoFalseNegatives(t *testing.T) {
	f := NewTailFilter(1024, 10)
	f.MarkExact()
	for i := 0; i < 5000; i++ {
		key := tailTestKey("hit", i)
		f.Add(key)
		if !f.MightContain(key) {
			t.Fatalf("false negative for %q", key)
		}
	}
}

func TestTailFilterDropKeepsWritesAfterBoundary(t *testing.T) {
	f := NewTailFilter(4, 10)
	f.MarkExact()
	for i := 0; i < 8; i++ {
		f.Add(tailTestKey("old", i))
	}
	boundary := f.SealSegment()
	for i := 0; i < 4; i++ {
		f.Add(tailTestKey("new", i))
	}
	f.DropThrough(boundary)
	for i := 0; i < 4; i++ {
		if !f.MightContain(tailTestKey("new", i)) {
			t.Fatalf("post-boundary key %d was dropped", i)
		}
	}
}

func TestTailFilterConcurrentPublishVisibility(t *testing.T) {
	f := NewTailFilter(1024, 10)
	f.MarkExact()
	const n = 10000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = tailTestKey("concurrent", i)
	}
	var published atomic.Int64
	go func() {
		for i := 0; i < n; i++ {
			f.Add(keys[i])
			published.Store(int64(i + 1))
		}
	}()
	for published.Load() < n {
		if count := int(published.Load()); count > 0 && !f.MightContain(keys[count-1]) {
			t.Fatalf("published key %d became invisible", count-1)
		}
	}
}

func TestTailRouteTargetOnlyWhenCandidateSetIsUnique(t *testing.T) {
	kvs := &KVServer{tailFilter: NewTailFilter(0, 0)}
	last := &PartitionSet{}
	next := &PartitionSet{}
	bphs := &bphsStore{}

	if got := kvs.tailRouteTarget(stateSnapshot{firstGC: true}); got != nil {
		t.Fatal("must not route before the first sorted partition set exists")
	}
	if got := kvs.tailRouteTarget(stateSnapshot{lastParts: last}); got != last {
		t.Fatal("steady state must route to the current partition set")
	}
	if got := kvs.tailRouteTarget(stateSnapshot{anotherStart: true, lastParts: last}); got != nil {
		t.Fatal("must not route during a three-way GC transition")
	}
	if got := kvs.tailRouteTarget(stateSnapshot{
		anotherStart: true, anotherEnd: true, anotherParts: next,
	}); got != next {
		t.Fatal("post-absorb state must route to the new partition set")
	}
	if got := kvs.tailRouteTarget(stateSnapshot{lastParts: last, bphs: bphs}); got != nil {
		t.Fatal("tail routing must not preempt the BPHS read path")
	}
}
