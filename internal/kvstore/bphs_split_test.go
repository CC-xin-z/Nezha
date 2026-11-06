package kvstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func TestBPHSSplitWaitsForConcurrentMaterialization(t *testing.T) {
	dataDir := t.TempDir()
	valuelogDir := filepath.Join(dataDir, "data", "valuelog")
	if err := os.MkdirAll(valuelogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
		currentLog:      filepath.Join(valuelogDir, "RaftState.log"),
		currentDBPath:   filepath.Join(dataDir, "data", "dbfile"),
	}
	records := make([]bphsRecord, 0, 10)
	for i := 1; i <= 10; i++ {
		records = append(records, bphsRecord{
			Key:     fmt.Sprintf("%010d", i),
			Version: uint32(i),
			Kind:    bphsInlineRecord,
			Value:   fmt.Sprintf("value-%02d", i),
		})
	}
	base, err := kvs.flushBPHSBaseFile(
		records,
		filepath.Join(valuelogDir, "RaftState_sorted_1.p0"),
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	baseSet := &PartitionSet{parts: []*SortedFileIndex{base}, ids: []int{0}}
	baseSet.assignOwnershipRanges()
	kvs.lastPartitions = baseSet
	kvs.sortedFilePath = filepath.Join(valuelogDir, "RaftState_sorted_1")

	store, err := openBPHSStore(kvs, baseSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	part := store.parts[0]
	part.mu.Lock()
	part.compacting = true
	part.mu.Unlock()
	go func() {
		time.Sleep(150 * time.Millisecond)
		part.mu.Lock()
		part.compacting = false
		part.mu.Unlock()
	}()

	if err := store.ensureAllPartitionsMaterialized(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	part.mu.RLock()
	defer part.mu.RUnlock()
	if !part.baseLocal || part.base == nil || part.compacting {
		t.Fatalf("partition not materialized: local=%v base=%v compacting=%v",
			part.baseLocal, part.base != nil, part.compacting)
	}
}

func TestBPHSSplitWithActiveMemTable(t *testing.T) {
	dataDir := t.TempDir()
	valuelogDir := filepath.Join(dataDir, "data", "valuelog")
	if err := os.MkdirAll(valuelogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
		currentLog:      filepath.Join(valuelogDir, "RaftState.log"),
		currentDBPath:   filepath.Join(dataDir, "data", "dbfile"),
	}
	records := make([]bphsRecord, 0, 10)
	for i := 1; i <= 10; i++ {
		records = append(records, bphsRecord{
			Key:     fmt.Sprintf("%010d", i),
			Version: uint32(i),
			Kind:    bphsInlineRecord,
			Value:   fmt.Sprintf("value-%02d", i),
		})
	}
	base, err := kvs.flushBPHSBaseFile(
		records,
		filepath.Join(valuelogDir, "RaftState_sorted_1.p0"),
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	baseSet := &PartitionSet{parts: []*SortedFileIndex{base}, ids: []int{0}}
	baseSet.assignOwnershipRanges()
	kvs.lastPartitions = baseSet
	kvs.sortedFilePath = filepath.Join(valuelogDir, "RaftState_sorted_1")

	store, err := openBPHSStore(kvs, baseSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CompactForRead(); err != nil {
		t.Fatal(err)
	}
	store.markLegacyCovered()
	kvs.bphs = store

	// This update remains in the hot partition's MemTable when the split is
	// committed. The child must still see it after taking over the right range.
	if _, err := store.Put("0000000009", "active-value", 11); err != nil {
		t.Fatal(err)
	}
	kvs.legacyPartitionsRetired = true

	payload, err := json.Marshal(bphsSplitCommand{
		ParentID: 0,
		SplitKey: "0000000006",
	})
	if err != nil {
		t.Fatal(err)
	}
	kvs.mu.Lock()
	splitErr := kvs.applyBPHSSplit(&raftrpc.DetailCod{
		OpType: bphsSplitOp,
		Value:  string(payload),
	}, 12)
	kvs.mu.Unlock()
	if splitErr != nil {
		t.Fatalf("split with active MemTable failed: %v", splitErr)
	}

	left, err := store.route("0000000002")
	if err != nil {
		t.Fatal(err)
	}
	right, err := store.route("0000000009")
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("split did not move the right range to the child")
	}
	if value, found, err := store.Get("0000000009"); err != nil || !found || value != "active-value" {
		t.Fatalf("pre-split active value after split = %q,%v,%v", value, found, err)
	}

	if part, err := store.Put("0000000002", "left-after-split", 13); err != nil || part != 0 {
		t.Fatalf("post-split left PUT routed to %d, err=%v", part, err)
	}
	if part, err := store.Put("0000000006", "right-after-split", 14); err != nil || part != 1 {
		t.Fatalf("post-split right PUT routed to %d, err=%v", part, err)
	}
	if got, err := store.Scan("0000000001", "0000000010"); err != nil {
		t.Fatal(err)
	} else {
		if got["0000000002"] != "left-after-split" {
			t.Fatalf("left post-split value = %q", got["0000000002"])
		}
		if got["0000000006"] != "right-after-split" {
			t.Fatalf("right post-split value = %q", got["0000000006"])
		}
		if got["0000000009"] != "active-value" {
			t.Fatalf("split-MemTable overlay value = %q", got["0000000009"])
		}
		if len(got) != 10 {
			t.Fatalf("post-split SCAN returned %d rows, want 10", len(got))
		}
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	base.closePool()

	persisted, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(persisted.Partitions) != 2 {
		t.Fatalf("persisted hot split state: ok=%v partitions=%d", ok, len(persisted.Partitions))
	}
	reopenSet, err := partitionSetFromMetadata(persisted.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := openBPHSStore(kvs, reopenSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if value, found, err := reopened.Get("0000000009"); err != nil || !found || value != "active-value" {
		t.Fatalf("recovered split-MemTable value = %q,%v,%v", value, found, err)
	}
	if value, found, err := reopened.Get("0000000006"); err != nil || !found || value != "right-after-split" {
		t.Fatalf("recovered post-split right value = %q,%v,%v", value, found, err)
	}
	if value, found, err := reopened.Get("0000000002"); err != nil || !found || value != "left-after-split" {
		t.Fatalf("recovered post-split left value = %q,%v,%v", value, found, err)
	}
}

func TestBPHSSplitWithActiveL0(t *testing.T) {
	dataDir := t.TempDir()
	valuelogDir := filepath.Join(dataDir, "data", "valuelog")
	if err := os.MkdirAll(valuelogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
		currentLog:      filepath.Join(valuelogDir, "RaftState.log"),
		currentDBPath:   filepath.Join(dataDir, "data", "dbfile"),
	}
	records := make([]bphsRecord, 0, 10)
	for i := 1; i <= 10; i++ {
		records = append(records, bphsRecord{
			Key:     fmt.Sprintf("%010d", i),
			Version: uint32(i),
			Kind:    bphsInlineRecord,
			Value:   fmt.Sprintf("value-%02d", i),
		})
	}
	base, err := kvs.flushBPHSBaseFile(
		records,
		filepath.Join(valuelogDir, "RaftState_sorted_1.p0"),
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	baseSet := &PartitionSet{parts: []*SortedFileIndex{base}, ids: []int{0}}
	baseSet.assignOwnershipRanges()
	kvs.lastPartitions = baseSet
	kvs.sortedFilePath = filepath.Join(valuelogDir, "RaftState_sorted_1")

	store, err := openBPHSStore(kvs, baseSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompactForRead(); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.markLegacyCovered()
	kvs.bphs = store

	if _, err := store.Put("0000000009", "l0-value", 11); err != nil {
		store.Close()
		t.Fatal(err)
	}
	parent := store.parts[0]
	parent.mu.Lock()
	if err := store.flushPartitionLocked(parent); err != nil {
		parent.mu.Unlock()
		store.Close()
		t.Fatal(err)
	}
	parent.mu.Unlock()
	if len(parent.l0) == 0 {
		store.Close()
		t.Fatal("test setup did not create an active L0")
	}
	if _, err := store.Put("0000000010", "mem-value", 12); err != nil {
		store.Close()
		t.Fatal(err)
	}
	kvs.legacyPartitionsRetired = true

	payload, err := json.Marshal(bphsSplitCommand{ParentID: 0, SplitKey: "0000000006"})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	kvs.mu.Lock()
	splitErr := kvs.applyBPHSSplit(&raftrpc.DetailCod{OpType: bphsSplitOp, Value: string(payload)}, 13)
	kvs.mu.Unlock()
	if splitErr != nil {
		store.Close()
		t.Fatalf("split with active L0 failed: %v", splitErr)
	}
	if value, found, err := store.Get("0000000009"); err != nil || !found || value != "l0-value" {
		store.Close()
		t.Fatalf("active L0 value after split = %q,%v,%v", value, found, err)
	}
	if value, found, err := store.Get("0000000010"); err != nil || !found || value != "mem-value" {
		store.Close()
		t.Fatalf("active MemTable value after split = %q,%v,%v", value, found, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	base.closePool()

	persisted, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(persisted.Partitions) != 2 {
		t.Fatalf("persisted active-L0 split state: ok=%v partitions=%d", ok, len(persisted.Partitions))
	}
	reopenSet, err := partitionSetFromMetadata(persisted.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := openBPHSStore(kvs, reopenSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, found, err := reopened.Get("0000000009"); err != nil || !found || value != "l0-value" {
		t.Fatalf("recovered active L0 value = %q,%v,%v", value, found, err)
	}
	if value, found, err := reopened.Get("0000000010"); err != nil || !found || value != "mem-value" {
		t.Fatalf("recovered active MemTable value = %q,%v,%v", value, found, err)
	}
}

func TestBPHSChooseSplitCandidateIncludesActiveNewKeys(t *testing.T) {
	dataDir := t.TempDir()
	valuelogDir := filepath.Join(dataDir, "data", "valuelog")
	if err := os.MkdirAll(valuelogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
		currentLog:      filepath.Join(valuelogDir, "RaftState.log"),
		currentDBPath:   filepath.Join(dataDir, "data", "dbfile"),
	}
	records := make([]bphsRecord, 0, 10)
	for i := 1; i <= 10; i++ {
		records = append(records, bphsRecord{
			Key:     fmt.Sprintf("%010d", i),
			Version: uint32(i),
			Kind:    bphsInlineRecord,
			Value:   fmt.Sprintf("value-%02d", i),
		})
	}
	base, err := kvs.flushBPHSBaseFile(
		records,
		filepath.Join(valuelogDir, "RaftState_sorted_1.p0"),
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	baseSet := &PartitionSet{parts: []*SortedFileIndex{base}, ids: []int{0}}
	baseSet.assignOwnershipRanges()
	kvs.lastPartitions = baseSet
	kvs.sortedFilePath = filepath.Join(valuelogDir, "RaftState_sorted_1")

	store, err := openBPHSStore(kvs, baseSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompactForRead(); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.markLegacyCovered()
	kvs.bphs = store
	for i := 11; i <= 20; i++ {
		if _, err := store.Put(fmt.Sprintf("%010d", i), fmt.Sprintf("new-%02d", i), uint32(i)); err != nil {
			store.Close()
			t.Fatal(err)
		}
	}
	part := store.parts[0]
	part.mu.Lock()
	part.logicalBytes = 512 << 20
	part.mu.Unlock()
	kvs.legacyPartitionsRetired = true

	candidate, ok := store.chooseSplitCandidate(256 << 20)
	if !ok {
		store.Close()
		t.Fatal("active growing partition was not selected for split")
	}
	if candidate.SplitKey < "0000000011" || candidate.SplitKey > "0000000019" {
		store.Close()
		t.Fatalf("split key %q does not account for active new keys", candidate.SplitKey)
	}

	payload, err := json.Marshal(bphsSplitCommand{
		ParentID: candidate.ParentID,
		SplitKey: candidate.SplitKey,
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	kvs.mu.Lock()
	splitErr := kvs.applyBPHSSplit(&raftrpc.DetailCod{OpType: bphsSplitOp, Value: string(payload)}, 21)
	kvs.mu.Unlock()
	if splitErr != nil {
		store.Close()
		t.Fatal(splitErr)
	}
	if value, found, err := store.Get("0000000020"); err != nil || !found || value != "new-20" {
		store.Close()
		t.Fatalf("active new key after split = %q,%v,%v", value, found, err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	base.closePool()
	persisted, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(persisted.Partitions) != 2 {
		t.Fatalf("persisted active-new-key split: ok=%v partitions=%d", ok, len(persisted.Partitions))
	}
	reopenSet, err := partitionSetFromMetadata(persisted.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := openBPHSStore(kvs, reopenSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, found, err := reopened.Get("0000000020"); err != nil || !found || value != "new-20" {
		t.Fatalf("recovered active new key = %q,%v,%v", value, found, err)
	}
}

func TestBPHSLogicalSplitRoutesAndRecovers(t *testing.T) {
	dataDir := t.TempDir()
	valuelogDir := filepath.Join(dataDir, "data", "valuelog")
	if err := os.MkdirAll(valuelogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
		currentLog:      filepath.Join(valuelogDir, "RaftState.log"),
		currentDBPath:   filepath.Join(dataDir, "data", "dbfile"),
	}
	basePath := filepath.Join(valuelogDir, "RaftState_sorted_1.p0")
	records := make([]bphsRecord, 0, 10)
	for i := 1; i <= 10; i++ {
		if i == 6 {
			continue
		}
		records = append(records, bphsRecord{
			Key:     fmt.Sprintf("%010d", i),
			Version: uint32(i),
			Kind:    bphsInlineRecord,
			Value:   fmt.Sprintf("value-%02d", i),
		})
	}
	globalBase, err := kvs.flushBPHSBaseFile(records, basePath, 0)
	if err != nil {
		t.Fatal(err)
	}
	baseSet := &PartitionSet{parts: []*SortedFileIndex{globalBase}, ids: []int{0}}
	baseSet.assignOwnershipRanges()
	kvs.lastPartitions = baseSet
	kvs.sortedFilePath = filepath.Join(valuelogDir, "RaftState_sorted_1")

	store, err := openBPHSStore(kvs, baseSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put("0000000005", "value-updated", 11); err != nil {
		t.Fatal(err)
	}
	store.markLegacyCovered()
	kvs.bphs = store
	payload, err := json.Marshal(bphsSplitCommand{
		ParentID: 0,
		SplitKey: "0000000006",
	})
	if err != nil {
		t.Fatal(err)
	}
	kvs.mu.Lock()
	splitErr := kvs.applyBPHSSplit(&raftrpc.DetailCod{
		OpType: bphsSplitOp,
		Value:  string(payload),
	}, 12)
	kvs.mu.Unlock()
	if splitErr != nil {
		t.Fatal(splitErr)
	}
	if !kvs.legacyPartitionsRetired {
		t.Fatal("split did not prepare and reclaim the legacy partition set")
	}
	if len(store.parts) != 2 || kvs.lastPartitions.Len() != 2 {
		t.Fatalf("split left store=%d route=%d partitions", len(store.parts), kvs.lastPartitions.Len())
	}

	left, err := store.route("0000000001")
	if err != nil {
		t.Fatal(err)
	}
	right, err := store.route("0000000010")
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("keys on opposite sides routed to the same partition")
	}
	if value, found, err := store.Get("0000000005"); err != nil || !found || value != "value-updated" {
		t.Fatalf("GET after split = %q,%v,%v", value, found, err)
	}
	if childID, err := store.Put("0000000006", "gap-value", 13); err != nil || childID != 1 {
		t.Fatalf("PUT in split gap routed to %d, err=%v", childID, err)
	}
	if got, err := store.Scan("0000000001", "0000000010"); err != nil || len(got) != 10 {
		t.Fatalf("SCAN after split returned %d rows, err=%v", len(got), err)
	}
	meta := kvs.lastPartitions.manifest()
	if meta[0].OwnershipHi != "0000000006" || meta[1].OwnershipLo != "0000000006" {
		t.Fatalf("split boundaries not persisted: left.hi=%q right.lo=%q",
			meta[0].OwnershipHi, meta[1].OwnershipLo)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	globalBase.closePool()
	persisted, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(persisted.Partitions) != 2 {
		t.Fatalf("persisted split state: ok=%v partitions=%d", ok, len(persisted.Partitions))
	}
	reopenSet, err := partitionSetFromMetadata(persisted.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	if reopenSet.parts[0].OwnershipHi != "0000000006" ||
		reopenSet.parts[1].OwnershipLo != "0000000006" {
		t.Fatalf("split boundaries not restored: left.hi=%q right.lo=%q",
			reopenSet.parts[0].OwnershipHi, reopenSet.parts[1].OwnershipLo)
	}
	reopened, err := openBPHSStore(kvs, reopenSet, filepath.Join(dataDir, "data", "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, found, err := reopened.Get("0000000010"); err != nil || !found || value != "value-10" {
		t.Fatalf("GET after split recovery = %q,%v,%v", value, found, err)
	}
	if value, found, err := reopened.Get("0000000006"); err != nil || !found || value != "gap-value" {
		t.Fatalf("GET split-gap key after recovery = %q,%v,%v", value, found, err)
	}
}
