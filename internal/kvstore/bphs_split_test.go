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
