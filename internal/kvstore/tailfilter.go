package kvstore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TailFilter is a no-false-negative membership filter for keys that may still
// live in the current value-log tail. False means the key is definitely in the
// sorted partition set; true means the normal two-path lookup must still run.
type TailFilter struct {
	mu sync.RWMutex

	segItems int
	segWords int
	hashes   int

	segs   []*tailSeg
	nextNo int

	// Conservative mode is required after restart or snapshot installation
	// until the tail has been rebuilt.
	conservative bool

	singlePath atomic.Uint64
	bothPaths  atomic.Uint64
}

type tailSeg struct {
	no     int
	bits   []uint64
	items  int
	sealed bool
}

const (
	defaultTailBitsPerKey = 10
	defaultTailSegItems   = 1 << 18
)

func NewTailFilter(segItems, bitsPerKey int) *TailFilter {
	if segItems <= 0 {
		segItems = defaultTailSegItems
	}
	if bitsPerKey <= 0 {
		bitsPerKey = defaultTailBitsPerKey
	}
	words := (segItems*bitsPerKey + 63) / 64
	if words < 1 {
		words = 1
	}
	hashes := int(float64(bitsPerKey)*0.6931471805599453 + 0.5)
	if hashes < 1 {
		hashes = 1
	}
	if hashes > 32 {
		hashes = 32
	}
	f := &TailFilter{
		segItems:     segItems,
		segWords:     words,
		hashes:       hashes,
		conservative: true,
	}
	f.segs = append(f.segs, f.newSegLocked())
	return f
}

func (f *TailFilter) newSegLocked() *tailSeg {
	s := &tailSeg{no: f.nextNo, bits: make([]uint64, f.segWords)}
	f.nextNo++
	return s
}

func (f *TailFilter) Add(key string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	seg := f.segs[len(f.segs)-1]
	if seg.items >= f.segItems {
		seg = f.sealLocked()
	}
	h1 := tailHash(key)
	h2 := (h1 >> 33) | (h1 << 31)
	h2 |= 1
	n := uint64(f.segWords * 64)
	for i := 0; i < f.hashes; i++ {
		p := (h1 + uint64(i)*h2) % n
		seg.bits[p/64] |= 1 << (p % 64)
	}
	seg.items++
}

func (f *TailFilter) MightContain(key string) bool {
	if f == nil {
		return true
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.conservative || len(f.segs) == 0 {
		return true
	}
	h1 := tailHash(key)
	h2 := (h1 >> 33) | (h1 << 31)
	h2 |= 1
	n := uint64(f.segWords * 64)
	for i := len(f.segs) - 1; i >= 0; i-- {
		s := f.segs[i]
		hit := true
		for j := 0; j < f.hashes; j++ {
			p := (h1 + uint64(j)*h2) % n
			if s.bits[p/64]&(1<<(p%64)) == 0 {
				hit = false
				break
			}
		}
		if hit {
			return true
		}
	}
	return false
}

func (f *TailFilter) SealSegment() int {
	if f == nil {
		return -1
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	no := f.segs[len(f.segs)-1].no
	f.sealLocked()
	return no
}

func (f *TailFilter) sealLocked() *tailSeg {
	last := f.segs[len(f.segs)-1]
	last.sealed = true
	s := f.newSegLocked()
	f.segs = append(f.segs, s)
	return s
}

func (f *TailFilter) DropThrough(no int) {
	if f == nil || no < 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	keep := f.segs[:0]
	for _, s := range f.segs {
		if s.no > no {
			keep = append(keep, s)
		}
	}
	f.segs = keep
	if len(f.segs) == 0 {
		f.segs = append(f.segs, f.newSegLocked())
	}
}

func (f *TailFilter) Invalidate() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conservative = true
	f.segs = []*tailSeg{f.newSegLocked()}
}

func (f *TailFilter) MarkExact() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conservative = false
}

func (f *TailFilter) Stats() (segments int, bitBytes int64, items int, conservative bool) {
	if f == nil {
		return 0, 0, 0, false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, s := range f.segs {
		bitBytes += int64(len(s.bits)) * 8
		items += s.items
	}
	return len(f.segs), bitBytes, items, f.conservative
}

func (f *TailFilter) NoteRoute(single bool) {
	if f == nil {
		return
	}
	if single {
		f.singlePath.Add(1)
		return
	}
	f.bothPaths.Add(1)
}

func (f *TailFilter) RouteStats() (singlePath, bothPaths uint64) {
	if f == nil {
		return 0, 0
	}
	return f.singlePath.Load(), f.bothPaths.Load()
}

func (kvs *KVServer) tailRouteShortcut(key string) bool {
	if kvs.tailFilter == nil {
		return false
	}
	single := !kvs.tailFilter.MightContain(key)
	kvs.tailFilter.NoteRoute(single)
	return single
}

// tailRouteTarget returns the only possible sorted partition set when the
// current state has exactly one non-tail candidate. During mid-GC states the
// old implementation of tail routing is unsafe/unhelpful, so it returns nil.
func (kvs *KVServer) tailRouteTarget(st stateSnapshot) *PartitionSet {
	if kvs.tailFilter == nil || st.bphs != nil {
		return nil
	}
	if st.firstGC {
		return nil
	}
	if !st.anotherStart {
		return st.lastParts
	}
	if st.anotherEnd {
		return st.anotherParts
	}
	return nil
}

func (kvs *KVServer) StartTailRouteStatsReporter(interval time.Duration) {
	if kvs.tailFilter == nil || interval <= 0 {
		return
	}
	go func() {
		for range time.Tick(interval) {
			single, both := kvs.tailFilter.RouteStats()
			if single+both == 0 {
				continue
			}
			fmt.Printf("[TAIL-ROUTE] single=%d both=%d single_ratio=%.4f\n",
				single, both, float64(single)/float64(single+both))
		}
	}()
}

func tailHash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
