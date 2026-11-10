package kvstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

const bphsSplitOp = "BPHSSplit"

const bphsSplitPoll = 10 * time.Second
const (
	bphsSplitMaterializeTimeout = 5 * time.Minute
	bphsSplitMaterializePoll    = 100 * time.Millisecond
)

type bphsSplitCommand struct {
	ParentID int             `json:"parent_id"`
	ParentLo string          `json:"parent_lo,omitempty"`
	ParentHi string          `json:"parent_hi,omitempty"`
	SplitKey string          `json:"split_key"`
	Bounds   bphsSplitBounds `json:"bounds"`
}

type bphsSplitCandidate struct {
	ParentID int
	ParentLo string
	ParentHi string
	SplitKey string
	Bounds   bphsSplitBounds
}

type bphsSplitBounds struct {
	LeftLo       string `json:"left_lo"`
	LeftHi       string `json:"left_hi"`
	RightLo      string `json:"right_lo"`
	RightHi      string `json:"right_hi"`
	LeftBytes    int64  `json:"left_bytes"`
	RightBytes   int64  `json:"right_bytes"`
	LeftEntries  int    `json:"left_entries"`
	RightEntries int    `json:"right_entries"`
}

func (kvs *KVServer) bphsSplitLoop(ctx context.Context) {
	if kvs == nil || kvs.raft == nil {
		return
	}
	ticker := time.NewTicker(bphsSplitPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if !kvs.raft.IsLeader() {
				continue
			}
			kvs.mu.Lock()
			if kvs.bphs == nil || kvs.installing || kvs.gcActive || kvs.gcInProgress ||
				!kvs.legacyPartitionsRetired || now.Sub(kvs.lastSplitProposal) < bphsSplitPoll {
				kvs.mu.Unlock()
				continue
			}
			store := kvs.bphs
			kvs.mu.Unlock()

			candidate, ok := store.chooseSplitCandidate(kvs.bphsSplitThresholdBytes())
			if !ok {
				continue
			}
			payload, err := json.Marshal(bphsSplitCommand{
				ParentID: candidate.ParentID,
				ParentLo: candidate.ParentLo,
				ParentHi: candidate.ParentHi,
				SplitKey: candidate.SplitKey,
				Bounds:   candidate.Bounds,
			})
			if err != nil {
				fmt.Printf("[BPHS-SPLIT] encode candidate failed: %v\n", err)
				continue
			}
			op := &raftrpc.DetailCod{
				OpType: bphsSplitOp,
				Key:    raft.InternalOpKey(bphsSplitOp),
				Value:  string(payload),
			}
			_, _, leader := kvs.raft.Start(op)
			kvs.mu.Lock()
			if leader {
				kvs.lastSplitProposal = now
				fmt.Printf("[BPHS-SPLIT] proposed parent=%d key=%q\n", candidate.ParentID, candidate.SplitKey)
			}
			kvs.mu.Unlock()
		}
	}
}

func (kvs *KVServer) bphsSplitThresholdBytes() int64 {
	percent := kvs.bphsSplitThresholdPercent
	if percent <= 0 {
		percent = 150
	}
	return kvs.partitionTargetBytes * int64(percent) / 100
}

func writeBPHSPartitionManifest(dir string, partition int, meta bphsPartitionManifest) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return raft.WriteFileAtomic(bphsPartitionManifestPath(dir, partition), data)
}

// chooseSplitCandidate selects the largest local partition above the
// logical size budget. The exact split key is carried in the Raft command so
// every replica applies the same ownership boundary.
func (s *bphsStore) chooseSplitCandidate(maxLogicalBytes int64) (bphsSplitCandidate, bool) {
	if s == nil {
		return bphsSplitCandidate{}, false
	}
	s.mu.RLock()
	parts := append([]*bphsPartition(nil), s.parts...)
	s.mu.RUnlock()

	var best bphsSplitCandidate
	var bestSize int64
	for _, part := range parts {
		owner, ok := s.currentLayoutView(part.index)
		if !ok {
			continue
		}
		s.snapshotMu.RLock()
		part.mu.RLock()
		eligible := part.baseLocal && part.base != nil &&
			part.vlog.Size() == 0 &&
			part.logicalBytes > maxLogicalBytes
		if !eligible {
			part.mu.RUnlock()
			s.snapshotMu.RUnlock()
			continue
		}
		view := s.partitionReadViewLocked(part, owner.ownershipLo, owner.ownershipHi)
		view.mem = part.mem.Sorted()
		if view.immutable != nil {
			view.mem = mergePartitionMemtableRecords(view.mem, view.immutable.Sorted())
		}
		view.mem = append([]bphsRecord(nil), view.mem...)
		size := part.logicalBytes
		part.mu.RUnlock()
		live, err := s.collectBPHSViewRecords(view)
		s.snapshotMu.RUnlock()
		var splitKey string
		var bounds bphsSplitBounds
		if err == nil && len(live) >= 3 {
			splitKey = live[len(live)/2].Key
			bounds, err = splitBoundsFromRecords(live, splitKey)
		}
		if err != nil || splitKey == "" || size <= bestSize {
			continue
		}
		best = bphsSplitCandidate{
			ParentID: part.index,
			ParentLo: owner.ownershipLo,
			ParentHi: owner.ownershipHi,
			SplitKey: splitKey,
			Bounds:   bounds,
		}
		bestSize = size
	}
	return best, bestSize > 0
}

func (s *bphsStore) collectBPHSViewRecords(view bphsPartitionReadView) ([]bphsRecord, error) {
	latest := make(map[string]bphsRecord)
	put := func(rec bphsRecord) {
		if (view.ownershipLo != "" && rec.Key < view.ownershipLo) ||
			(view.ownershipHi != "" && rec.Key >= view.ownershipHi) {
			return
		}
		old, ok := latest[rec.Key]
		if !ok || rec.Version >= old.Version {
			latest[rec.Key] = rec
		}
	}
	if view.base != nil {
		records, err := readRawSortedFile(view.base.FilePath)
		if err != nil {
			return nil, err
		}
		for _, rec := range records {
			put(rec)
		}
	}
	for _, run := range view.l0 {
		records, err := readBPHSSortedFile(run.FilePath)
		if err != nil {
			return nil, err
		}
		for _, rec := range records {
			put(rec)
		}
	}
	for _, rec := range view.mem {
		put(rec)
	}
	out := make([]bphsRecord, 0, len(latest))
	for _, rec := range latest {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func splitBoundsFromRecords(records []bphsRecord, splitKey string) (bphsSplitBounds, error) {
	var out bphsSplitBounds
	for _, rec := range records {
		size := bphsRecordBytes(rec)
		if rec.Key < splitKey {
			if out.LeftLo == "" {
				out.LeftLo = rec.Key
			}
			out.LeftHi = rec.Key
			out.LeftBytes += size
			out.LeftEntries++
		} else {
			if out.RightLo == "" {
				out.RightLo = rec.Key
			}
			out.RightHi = rec.Key
			out.RightBytes += size
			out.RightEntries++
		}
	}
	if out.LeftEntries == 0 || out.RightEntries == 0 {
		return bphsSplitBounds{}, fmt.Errorf(
			"split key %q does not divide the live records into non-empty ranges",
			splitKey,
		)
	}
	return out, nil
}

// splitPartition performs the logical half of dynamic partitioning. The child
// initially shares the immutable parent Base; each side filters reads by its
// ownership range and can physically compact into an independent Base later.
func (s *bphsStore) splitPartition(parentID int, splitKey string, commandIndex uint32) error {
	return s.splitPartitionCommand(bphsSplitCommand{
		ParentID: parentID,
		SplitKey: splitKey,
	}, commandIndex)
}

func (s *bphsStore) splitPartitionCommand(cmd bphsSplitCommand, commandIndex uint32) error {
	return s.splitPartitionCommandWithKVSLock(cmd, commandIndex, true)
}

func (s *bphsStore) splitPartitionCommandConcurrent(
	cmd bphsSplitCommand,
	commandIndex uint32,
) error {
	return s.splitPartitionCommandWithKVSLock(cmd, commandIndex, false)
}

func (s *bphsStore) splitPartitionCommandWithKVSLock(
	cmd bphsSplitCommand,
	commandIndex uint32,
	kvsLockHeld bool,
) error {
	if s == nil || s.kvs == nil {
		return fmt.Errorf("BPHS split requires a store")
	}
	if !kvsLockHeld {
		s.kvs.mu.Lock()
	}
	legacyRetired := s.kvs.legacyPartitionsRetired
	if !kvsLockHeld {
		s.kvs.mu.Unlock()
	}
	if !legacyRetired {
		if !kvsLockHeld {
			if err := s.ensureAllPartitionsMaterialized(bphsSplitMaterializeTimeout); err != nil {
				return fmt.Errorf("materialize before split: %w", err)
			}
			s.kvs.mu.Lock()
			reclaimed, err := s.kvs.reclaimLegacyPartitionsLocked()
			s.kvs.mu.Unlock()
			if err != nil {
				return err
			}
			if !reclaimed {
				return fmt.Errorf("legacy retirement declined before split")
			}
		} else {
			if err := s.ensureAllPartitionsMaterialized(bphsSplitMaterializeTimeout); err != nil {
				return fmt.Errorf("materialize before split: %w", err)
			}
			reclaimed, err := s.kvs.reclaimLegacyPartitionsLocked()
			if err != nil {
				return err
			}
			if !reclaimed {
				return fmt.Errorf(
					"legacy retirement declined: covered=%v materialized=%v installing=%v gcActive=%v gcInProgress=%v",
					s.CoversLegacy(), s.AllPartitionsMaterialized(),
					s.kvs.installing, s.kvs.gcActive, s.kvs.gcInProgress,
				)
			}
		}
	}
	parentID, splitKey := cmd.ParentID, cmd.SplitKey
	s.mu.RLock()
	if parentID < 0 || parentID >= len(s.parts) {
		s.mu.RUnlock()
		return fmt.Errorf("invalid parent partition id %d", parentID)
	}
	part := s.parts[parentID]
	newID := len(s.parts)
	owner, ownerOK := s.currentLayoutView(parentID)
	s.mu.RUnlock()
	if !ownerOK || owner.part != part {
		fmt.Printf("[BPHS-SPLIT] stale parent=%d: partition is no longer in the current layout\n", parentID)
		return nil
	}
	if (cmd.ParentLo != "" && cmd.ParentLo != owner.ownershipLo) ||
		(cmd.ParentHi != "" && cmd.ParentHi != owner.ownershipHi) {
		fmt.Printf(
			"[BPHS-SPLIT] stale parent=%d range=[%q,%q), command=[%q,%q)\n",
			parentID, owner.ownershipLo, owner.ownershipHi, cmd.ParentLo, cmd.ParentHi,
		)
		return nil
	}
	if splitKey == "" ||
		(owner.ownershipLo != "" && splitKey < owner.ownershipLo) ||
		(owner.ownershipHi != "" && splitKey >= owner.ownershipHi) {
		fmt.Printf("[BPHS-SPLIT] stale parent=%d split_key=%q outside [%q,%q)\n",
			parentID, splitKey, owner.ownershipLo, owner.ownershipHi)
		return nil
	}

	// Pin the immutable runs for the preparation phase. This excludes
	// compaction/install from deleting them, but it does not take part.mu, so
	// GET and SCAN keep reading the old layout while child metadata is prepared.
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()

	part.mu.RLock()
	if !part.baseLocal || part.base == nil {
		part.mu.RUnlock()
		fmt.Printf("[BPHS-SPLIT] deferred parent=%d: no local base\n", parentID)
		return nil
	}
	if part.vlog.Size() != 0 {
		part.mu.RUnlock()
		fmt.Printf("[BPHS-SPLIT] deferred parent=%d: active VLog delta\n", parentID)
		return nil
	}
	parentOwnershipHi := owner.ownershipHi
	view := s.partitionReadViewLocked(part, owner.ownershipLo, parentOwnershipHi)
	active := part.mem.Sorted()
	if view.immutable != nil {
		active = mergePartitionMemtableRecords(active, view.immutable.Sorted())
	}
	view.mem = append([]bphsRecord(nil), active...)
	parentBase := part.base
	parentL0 := append([]*SortedFileIndex(nil), part.l0...)
	part.mu.RUnlock()

	if s.splitPrepareStart != nil {
		s.splitPrepareStart <- struct{}{}
	}
	if s.splitPrepareRelease != nil {
		<-s.splitPrepareRelease
	}

	bounds := cmd.Bounds
	if bounds.LeftEntries <= 0 || bounds.RightEntries <= 0 {
		live, err := s.collectBPHSViewRecords(view)
		if err != nil {
			return err
		}
		bounds, err = splitBoundsFromRecords(live, splitKey)
		if err != nil {
			return err
		}
	}
	parentMeta, ok, err := loadBPHSPartitionManifest(s.dir, parentID)
	if err != nil {
		return err
	}
	if !ok || parentMeta.BaseFile == "" {
		return fmt.Errorf("partition %d has no durable manifest", parentID)
	}

	childRecords, err := openPartitionRecordLog(filepath.Join(s.dir, fmt.Sprintf("p%06d.records", newID)))
	if err != nil {
		return err
	}
	childVLog, err := openPartitionVLog(filepath.Join(s.dir, fmt.Sprintf("p%06d.vlog", newID)))
	if err != nil {
		childRecords.Close()
		return err
	}
	var childL0Paths []string
	cleanupChild := func() {
		childRecords.Close()
		childVLog.Close()
		os.Remove(childRecords.Path())
		os.Remove(childVLog.Path())
		os.Remove(bphsPartitionManifestPath(s.dir, newID))
		for _, path := range childL0Paths {
			os.Remove(path)
			os.Remove(sparseIndexPath(path))
		}
	}

	var splitMem []bphsRecord
	for _, rec := range view.mem {
		if rec.Kind != bphsInlineRecord {
			cleanupChild()
			return fmt.Errorf("partition %d split view contains BlobRecord for key %q", parentID, rec.Key)
		}
		if rec.Key < splitKey {
			continue
		}
		splitMem = append(splitMem, rec)
		if _, err := childRecords.Append(rec); err != nil {
			cleanupChild()
			return fmt.Errorf("persist child split MemTable key %q: %w", rec.Key, err)
		}
	}
	if err := childRecords.Sync(); err != nil {
		cleanupChild()
		return fmt.Errorf("sync child split MemTable: %w", err)
	}
	childMem := newPartitionMemtable(s.memtableBytes)
	for _, rec := range splitMem {
		childMem.Put(rec)
	}

	parentL0Prefix := fmt.Sprintf("p%06d_l0_", parentID)
	for _, run := range parentL0 {
		sourceName := filepath.Base(run.FilePath)
		if !strings.HasPrefix(sourceName, parentL0Prefix) {
			cleanupChild()
			return fmt.Errorf("partition %d L0 %s has unexpected name", parentID, sourceName)
		}
		childName := fmt.Sprintf("p%06d_l0_%s", newID, strings.TrimPrefix(sourceName, parentL0Prefix))
		childPath := filepath.Join(s.dir, childName)
		if err := os.Link(run.FilePath, childPath); err != nil {
			cleanupChild()
			return fmt.Errorf("link partition %d L0 %s for child %d: %w", parentID, sourceName, newID, err)
		}
		childL0Paths = append(childL0Paths, childPath)
		if sourceIndex, childIndex := sparseIndexPath(run.FilePath), sparseIndexPath(childPath); fileExists(sourceIndex) {
			if err := os.Link(sourceIndex, childIndex); err != nil {
				cleanupChild()
				return fmt.Errorf("link partition %d L0 index %s for child %d: %w", parentID, sourceIndex, newID, err)
			}
		}
	}
	loadedChildL0, err := loadPartitionL0Files(s.kvs, s.dir, newID, 0)
	if err != nil {
		cleanupChild()
		return err
	}
	if len(loadedChildL0) != len(parentL0) {
		cleanupChild()
		return fmt.Errorf("partition %d child L0 count=%d, want %d", parentID, len(loadedChildL0), len(parentL0))
	}
	childL0 := make([]*SortedFileIndex, 0, len(loadedChildL0))
	for _, loaded := range loadedChildL0 {
		childL0 = append(childL0, loaded.file)
	}

	parentMeta.LogicalBytes = bounds.LeftBytes
	parentMeta.Entries = bounds.LeftEntries
	if err := writeBPHSPartitionManifest(s.dir, parentID, parentMeta); err != nil {
		cleanupChild()
		return err
	}
	childMeta := parentMeta
	childMeta.LogicalBytes = bounds.RightBytes
	childMeta.Entries = bounds.RightEntries
	if err := writeBPHSPartitionManifest(s.dir, newID, childMeta); err != nil {
		cleanupChild()
		return err
	}
	childBase, err := loadBPHSBaseFile(s.kvs, s.dir, childMeta, 0)
	if err != nil {
		cleanupChild()
		return err
	}
	childBase.Lo, childBase.Hi = bounds.RightLo, bounds.RightHi
	childBase.OwnershipLo, childBase.OwnershipHi = splitKey, parentOwnershipHi
	childBase.InlineValues = s.inline
	s.retainBase(childBase.FilePath)

	child := &bphsPartition{
		index:        newID,
		base:         childBase,
		baseLocal:    true,
		mem:          childMem,
		records:      childRecords,
		vlog:         childVLog,
		l0:           childL0,
		gen:          parentMeta.Generation,
		logicalBytes: bounds.RightBytes,
		ownershipLo:  splitKey,
		ownershipHi:  parentOwnershipHi,
	}
	child.durableIndex.Store(commandIndex)
	child.pendingDurableVersion.Store(commandIndex)

	var oldSet *PartitionSet
	if kvsLockHeld {
		oldSet = s.kvs.lastPartitions
	} else {
		s.kvs.mu.Lock()
		oldSet = s.kvs.lastPartitions
		s.kvs.mu.Unlock()
	}
	if oldSet == nil {
		if s.releaseBase(childBase.FilePath) {
			childBase.closePool()
		}
		cleanupChild()
		return fmt.Errorf("BPHS split has no routing metadata")
	}
	metas := oldSet.manifest()
	pos := -1
	for i := range oldSet.parts {
		if oldSet.partitionID(i) == parentID {
			pos = i
			break
		}
	}
	if pos < 0 {
		if s.releaseBase(childBase.FilePath) {
			childBase.closePool()
		}
		cleanupChild()
		return fmt.Errorf("parent partition %d is absent from routing metadata", parentID)
	}
	parentRoute := metas[pos]
	parentRoute.Path = filepath.Base(parentBase.FilePath)
	parentRoute.Lo, parentRoute.Hi = bounds.LeftLo, bounds.LeftHi
	parentRoute.Size = parentBase.FileSize
	parentRoute.Entries = bounds.LeftEntries
	parentRoute.BPHSID = parentID
	parentRoute.OwnershipHi = splitKey
	childRoute := parentRoute
	childRoute.Lo, childRoute.Hi = bounds.RightLo, bounds.RightHi
	childRoute.Entries = bounds.RightEntries
	childRoute.BPHSID = newID
	childRoute.OwnershipLo = splitKey
	childRoute.OwnershipHi = parentOwnershipHi
	metas = append(metas, partitionMeta{})
	copy(metas[pos+2:], metas[pos+1:])
	metas[pos] = parentRoute
	metas[pos+1] = childRoute
	routeSet, err := partitionSetFromMetadata(metas)
	if err != nil {
		if s.releaseBase(childBase.FilePath) {
			childBase.closePool()
		}
		cleanupChild()
		return err
	}

	// The expensive preparation above ran without part.mu. This publication is
	// intentionally tiny: install the child slot, change parent ownership and
	// atomically publish one complete layout. In-flight reads that already
	// captured the previous layout remain valid and continue on the full parent.
	if !kvsLockHeld {
		s.kvs.mu.Lock()
		if s.kvs.lastPartitions != oldSet {
			s.kvs.mu.Unlock()
			if s.releaseBase(childBase.FilePath) {
				childBase.closePool()
			}
			cleanupChild()
			return fmt.Errorf("BPHS routing layout changed during split preparation")
		}
	}
	s.mu.Lock()
	if len(s.parts) != newID {
		s.mu.Unlock()
		if !kvsLockHeld {
			s.kvs.mu.Unlock()
		}
		if s.releaseBase(childBase.FilePath) {
			childBase.closePool()
		}
		cleanupChild()
		return fmt.Errorf("partition layout changed concurrently during split")
	}
	part.mu.Lock()
	if part.base != parentBase {
		part.mu.Unlock()
		s.mu.Unlock()
		if !kvsLockHeld {
			s.kvs.mu.Unlock()
		}
		if s.releaseBase(childBase.FilePath) {
			childBase.closePool()
		}
		cleanupChild()
		return fmt.Errorf("partition %d changed during split preparation", parentID)
	}
	part.ownershipHi = splitKey
	part.logicalBytes = bounds.LeftBytes
	s.parts = append(s.parts, child)
	if err := s.publishLayout(routeSet, s.parts); err != nil {
		part.mu.Unlock()
		s.mu.Unlock()
		if !kvsLockHeld {
			s.kvs.mu.Unlock()
		}
		return err
	}
	part.mu.Unlock()
	s.mu.Unlock()

	s.kvs.lastPartitions = routeSet
	if s.kvs.firstPartitions == oldSet {
		s.kvs.firstPartitions = routeSet
	}
	if s.kvs.anotherPartitions == oldSet {
		s.kvs.anotherPartitions = routeSet
	}
	s.kvs.bphsLayoutVersion++
	if err := s.MarkAllDurable(commandIndex); err != nil {
		if !kvsLockHeld {
			s.kvs.mu.Unlock()
		}
		return err
	}
	s.kvs.saveKVState()
	if !kvsLockHeld {
		s.kvs.mu.Unlock()
	}
	fmt.Printf(
		"[BPHS-SPLIT] parent=%d child=%d key=%q parent_bytes=%d child_bytes=%d\n",
		parentID, newID, splitKey, bounds.LeftBytes, bounds.RightBytes,
	)
	return nil
}

// ensureAllPartitionsMaterialized waits until every partition has a complete
// local Base and no compaction is in flight. CompactForRead can legitimately
// schedule another compaction from its completion callback, so a single pass
// followed by an immediate readiness check has a race: a slow follower may
// see compacting=true and reject a Raft split that it must apply.
func (s *bphsStore) ensureAllPartitionsMaterialized(timeout time.Duration) error {
	if s == nil {
		return fmt.Errorf("BPHS store is nil")
	}
	deadline := time.Now().Add(timeout)
	var lastReport time.Time
	for {
		if s.AllPartitionsMaterialized() {
			return nil
		}
		if _, err := s.CompactForRead(); err != nil {
			return err
		}
		if s.AllPartitionsMaterialized() {
			return nil
		}
		now := time.Now()
		if now.After(deadline) {
			return fmt.Errorf("BPHS partitions did not become materialized within %v", timeout)
		}
		if lastReport.IsZero() || now.Sub(lastReport) >= 5*time.Second {
			fmt.Printf("[BPHS-SPLIT] waiting for all partitions to materialize before split\n")
			lastReport = now
		}
		time.Sleep(bphsSplitMaterializePoll)
	}
}

func (kvs *KVServer) applyBPHSSplit(op *raftrpc.DetailCod, index int) error {
	if kvs.bphs == nil {
		return fmt.Errorf("BPHS split received without BPHS enabled")
	}
	var cmd bphsSplitCommand
	if err := json.Unmarshal([]byte(op.Value), &cmd); err != nil {
		return fmt.Errorf("decode BPHS split: %w", err)
	}
	return kvs.bphs.splitPartitionCommand(cmd, uint32(index))
}
