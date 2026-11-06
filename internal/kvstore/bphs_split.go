package kvstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	ParentID int    `json:"parent_id"`
	SplitKey string `json:"split_key"`
}

type bphsSplitCandidate struct {
	ParentID int
	SplitKey string
}

type bphsSplitBounds struct {
	leftLo, leftHi   string
	rightLo, rightHi string
	leftBytes        int64
	rightBytes       int64
	leftEntries      int
	rightEntries     int
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

			candidate, ok := store.chooseSplitCandidate(kvs.partitionTargetBytes * 2)
			if !ok {
				continue
			}
			payload, err := json.Marshal(bphsSplitCommand{
				ParentID: candidate.ParentID,
				SplitKey: candidate.SplitKey,
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

func writeBPHSPartitionManifest(dir string, partition int, meta bphsPartitionManifest) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return raft.WriteFileAtomic(bphsPartitionManifestPath(dir, partition), data)
}

// chooseSplitCandidate selects the largest idle local partition above the
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
		part.mu.RLock()
		eligible := part.baseLocal && part.base != nil && !part.compacting &&
			part.mem.Len() == 0 && len(part.l0) == 0 &&
			part.records.Size() == 0 && part.vlog.Size() == 0 &&
			part.logicalBytes > maxLogicalBytes
		if !eligible {
			part.mu.RUnlock()
			continue
		}
		keys := make([]string, 0, len(part.base.Sparse))
		for _, entry := range part.base.Sparse {
			if part.ownsKey(entry.PaddedKey) {
				keys = append(keys, entry.PaddedKey)
			}
		}
		size := part.logicalBytes
		var splitKey string
		if len(keys) >= 2 {
			splitKey = keys[len(keys)/2]
		} else {
			splitKey, _ = chooseBPHSSplitKey(part.base.FilePath, part)
		}
		part.mu.RUnlock()
		if splitKey == "" || size <= bestSize {
			continue
		}
		best = bphsSplitCandidate{
			ParentID: part.index,
			SplitKey: splitKey,
		}
		bestSize = size
	}
	return best, bestSize > 0
}

func chooseBPHSSplitKey(path string, part *bphsPartition) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	count := 0
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if part.ownsKey(entry.Key) {
			count++
		}
	}
	if count < 3 {
		return "", fmt.Errorf("partition %d has too few owned keys to split", part.index)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	reader = bufio.NewReaderSize(file, 1<<20)
	target := count / 2
	seen := 0
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if !part.ownsKey(entry.Key) {
			continue
		}
		if seen == target {
			return entry.Key, nil
		}
		seen++
	}
	return "", fmt.Errorf("failed to choose split key for partition %d", part.index)
}

func scanBPHSSplitBounds(path string, part *bphsPartition, splitKey string) (bphsSplitBounds, error) {
	if splitKey == "" || !part.ownsKey(splitKey) {
		return bphsSplitBounds{}, fmt.Errorf("split key %q is outside partition %d ownership", splitKey, part.index)
	}
	file, err := os.Open(path)
	if err != nil {
		return bphsSplitBounds{}, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	var out bphsSplitBounds
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			return bphsSplitBounds{}, err
		}
		if !part.ownsKey(entry.Key) {
			continue
		}
		size := int64(20 + len(entry.Key) + len(entry.Value))
		if entry.Key < splitKey {
			if out.leftLo == "" {
				out.leftLo = entry.Key
			}
			out.leftHi = entry.Key
			out.leftBytes += size
			out.leftEntries++
		} else {
			if out.rightLo == "" {
				out.rightLo = entry.Key
			}
			out.rightHi = entry.Key
			out.rightBytes += size
			out.rightEntries++
		}
	}
	if out.leftEntries == 0 || out.rightEntries == 0 {
		return bphsSplitBounds{}, fmt.Errorf(
			"split key %q does not divide partition %d into non-empty ranges",
			splitKey, part.index,
		)
	}
	return out, nil
}

// splitPartition performs the logical half of dynamic partitioning. The child
// initially shares the immutable parent Base; each side filters reads by its
// ownership range and can physically compact into an independent Base later.
func (s *bphsStore) splitPartition(parentID int, splitKey string, commandIndex uint32) error {
	if s == nil || s.kvs == nil {
		return fmt.Errorf("BPHS split requires a store")
	}
	if !s.kvs.legacyPartitionsRetired {
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
	s.mu.RLock()
	if parentID < 0 || parentID >= len(s.parts) {
		s.mu.RUnlock()
		return fmt.Errorf("invalid parent partition id %d", parentID)
	}
	part := s.parts[parentID]
	newID := len(s.parts)
	s.mu.RUnlock()

	part.mu.Lock()
	defer part.mu.Unlock()
	if part.compacting {
		return fmt.Errorf("partition %d is compacting", parentID)
	}
	if !part.baseLocal || part.base == nil {
		return fmt.Errorf("partition %d has no local base", parentID)
	}
	if len(part.l0) != 0 || part.vlog.Size() != 0 {
		return fmt.Errorf("partition %d has an active L0/VLog delta unsupported by hot-memory split", parentID)
	}
	parentOwnershipHi := part.ownershipHi
	bounds, err := scanBPHSSplitBounds(part.base.FilePath, part, splitKey)
	if err != nil {
		return err
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
	cleanupChild := func() {
		childRecords.Close()
		childVLog.Close()
		os.Remove(childRecords.Path())
		os.Remove(childVLog.Path())
		os.Remove(bphsPartitionManifestPath(s.dir, newID))
	}

	var splitMem []bphsRecord
	for _, rec := range part.mem.Sorted() {
		if rec.Kind != bphsInlineRecord {
			cleanupChild()
			return fmt.Errorf("partition %d active MemTable contains BlobRecord for key %q", parentID, rec.Key)
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

	parentMeta.LogicalBytes = bounds.leftBytes
	if err := writeBPHSPartitionManifest(s.dir, parentID, parentMeta); err != nil {
		cleanupChild()
		return err
	}
	childMeta := parentMeta
	childMeta.LogicalBytes = bounds.rightBytes
	childMeta.Entries = bounds.rightEntries
	if err := writeBPHSPartitionManifest(s.dir, newID, childMeta); err != nil {
		cleanupChild()
		return err
	}
	childBase, err := loadBPHSBaseFile(s.kvs, s.dir, childMeta, 0)
	if err != nil {
		cleanupChild()
		return err
	}
	childBase.Lo, childBase.Hi = bounds.rightLo, bounds.rightHi
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
		gen:          parentMeta.Generation,
		logicalBytes: bounds.rightBytes,
		ownershipLo:  splitKey,
		ownershipHi:  parentOwnershipHi,
	}
	child.durableIndex.Store(commandIndex)
	child.pendingDurableVersion.Store(commandIndex)

	// Publish the new storage slot before the routing pointer so a reader that
	// observes the new ownership range can always resolve its partition ID.
	s.mu.Lock()
	if len(s.parts) != newID {
		s.mu.Unlock()
		s.releaseBase(childBase.FilePath)
		removeBPHSSortedFile(childBase)
		cleanupChild()
		return fmt.Errorf("partition layout changed concurrently during split")
	}
	s.parts = append(s.parts, child)
	s.mu.Unlock()

	part.base.Lo, part.base.Hi = bounds.leftLo, bounds.leftHi
	part.base.OwnershipHi = splitKey
	part.ownershipHi = splitKey
	part.logicalBytes = bounds.leftBytes

	oldSet := s.kvs.lastPartitions
	metas := oldSet.manifest()
	pos := -1
	for i := range oldSet.parts {
		if oldSet.partitionID(i) == parentID {
			pos = i
			break
		}
	}
	if pos < 0 {
		return fmt.Errorf("parent partition %d is absent from routing metadata", parentID)
	}
	parentRoute := metas[pos]
	parentRoute.Path = filepath.Base(part.base.FilePath)
	parentRoute.Lo, parentRoute.Hi = bounds.leftLo, bounds.leftHi
	parentRoute.Size = part.base.FileSize
	parentRoute.Entries = bounds.leftEntries
	parentRoute.BPHSID = parentID
	parentRoute.OwnershipHi = splitKey
	childRoute := parentRoute
	childRoute.Lo, childRoute.Hi = bounds.rightLo, bounds.rightHi
	childRoute.Entries = bounds.rightEntries
	childRoute.BPHSID = newID
	childRoute.OwnershipLo = splitKey
	childRoute.OwnershipHi = parentOwnershipHi
	metas = append(metas, partitionMeta{})
	copy(metas[pos+2:], metas[pos+1:])
	metas[pos] = parentRoute
	metas[pos+1] = childRoute
	routeSet, err := partitionSetFromMetadata(metas)
	if err != nil {
		return err
	}

	s.kvs.lastPartitions = routeSet
	if s.kvs.firstPartitions == oldSet {
		s.kvs.firstPartitions = routeSet
	}
	if s.kvs.anotherPartitions == oldSet {
		s.kvs.anotherPartitions = routeSet
	}
	s.kvs.bphsLayoutVersion++
	if err := s.MarkAllDurable(commandIndex); err != nil {
		return err
	}
	s.kvs.saveKVState()
	s.UseOwnershipMetadata(routeSet)
	fmt.Printf(
		"[BPHS-SPLIT] parent=%d child=%d key=%q parent_bytes=%d child_bytes=%d\n",
		parentID, newID, splitKey, bounds.leftBytes, bounds.rightBytes,
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
	return kvs.bphs.splitPartition(cmd.ParentID, cmd.SplitKey, uint32(index))
}
