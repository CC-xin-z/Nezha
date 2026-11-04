package kvstore

import (
	"os"
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func TestBPHSMetadataOnlyRecoversFromLocalBase(t *testing.T) {
	kvs := &KVServer{
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base")
	globalPath := base.parts[0].FilePath
	globalIndex := sparseIndexPath(globalPath)
	metadata, err := base.metadataOnlyClone()
	if err != nil {
		t.Fatal(err)
	}

	bphsDir := t.TempDir()
	store, err := openBPHSStore(kvs, base, bphsDir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put("0000000005", "materialized", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompactForRead(); err != nil {
		t.Fatal(err)
	}
	store.markLegacyCovered()
	if !store.AllPartitionsMaterialized() {
		t.Fatal("partition did not materialize a local BPHS base")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	base.Close()
	if err := os.Remove(globalPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.Remove(globalIndex); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	reopened, err := openBPHSStore(kvs, metadata, bphsDir, 1<<20, 0, 0)
	if err != nil {
		t.Fatalf("metadata-only recovery: %v", err)
	}
	defer reopened.Close()
	value, found, err := reopened.Get("0000000005")
	if err != nil {
		t.Fatal(err)
	}
	if !found || value != "materialized" {
		t.Fatalf("metadata-only recovery returned value=%q found=%v", value, found)
	}
}

func TestReclaimLegacyPartitionsPersistsMetadataBeforeDelete(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
		currentLog:      filepath.Join(dataDir, "data", "valuelog", "RaftState.log"),
		currentDBPath:   filepath.Join(dataDir, "data", "dbfile"),
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base")
	globalPath := base.parts[0].FilePath
	globalIndex := sparseIndexPath(globalPath)

	bphsDir := filepath.Join(dataDir, "data", "bphs")
	store, err := openBPHSStore(kvs, base, bphsDir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put("0000000005", "materialized", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompactForRead(); err != nil {
		t.Fatal(err)
	}
	store.markLegacyCovered()
	if !store.AllPartitionsMaterialized() {
		t.Fatal("partition did not materialize a local BPHS base")
	}

	kvs.mu.Lock()
	kvs.bphs = store
	kvs.bphsDir = bphsDir
	kvs.lastPartitions = base
	kvs.firstPartitions = base
	kvs.sortedFilePath = filepath.Join(dataDir, "data", "valuelog", "RaftState_sorted_1")
	kvs.mu.Unlock()

	pinned, release := kvs.pinPartitions()
	if pinned != base {
		t.Fatal("test did not pin the activation-time partition set")
	}
	removed, err := kvs.maybeReclaimLegacyPartitions()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed=%d while a snapshot still pinned the files", removed)
	}
	if !kvs.legacyPartitionsRetired {
		t.Fatal("legacy retirement flag was not set")
	}
	if got := kvs.lastPartitions.Paths(); len(got) != 0 {
		t.Fatalf("metadata-only set exposes paths: %v", got)
	}
	if _, err := os.Stat(globalPath); err != nil {
		t.Fatalf("pinned legacy partition was removed early: %v", err)
	}
	release()
	if _, err := os.Stat(globalPath); !os.IsNotExist(err) {
		t.Fatalf("legacy partition still exists: %v", err)
	}
	if _, err := os.Stat(globalIndex); !os.IsNotExist(err) {
		t.Fatalf("legacy sparse index still exists: %v", err)
	}

	st, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !st.LegacyPartitionsRetired {
		t.Fatalf("persisted retirement flag missing: ok=%v state=%+v", ok, st)
	}
	if len(st.Partitions) != 1 {
		t.Fatalf("persisted metadata partitions=%d, want 1", len(st.Partitions))
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	base.Close()

	metadata, err := partitionSetFromMetadata(st.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := openBPHSStore(kvs, metadata, bphsDir, 1<<20, 0, 0)
	if err != nil {
		t.Fatalf("recover after legacy retirement: %v", err)
	}
	defer reopened.Close()
	value, found, err := reopened.Get("0000000005")
	if err != nil {
		t.Fatal(err)
	}
	if !found || value != "materialized" {
		t.Fatalf("recovered value=%q found=%v", value, found)
	}
}

func TestReclaimLegacyPartitionsWaitsForLocalBases(t *testing.T) {
	dataDir := t.TempDir()
	kvs := &KVServer{
		dataDir:         dataDir,
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
		numGC:           1,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base")
	globalPath := base.parts[0].FilePath
	store, err := openBPHSStore(kvs, base, filepath.Join(dataDir, "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.markLegacyCovered()

	kvs.mu.Lock()
	kvs.bphs = store
	kvs.bphsDir = store.Dir()
	kvs.lastPartitions = base
	kvs.mu.Unlock()

	removed, err := kvs.maybeReclaimLegacyPartitions()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 || kvs.legacyPartitionsRetired {
		t.Fatalf("reclaimed before local materialization: removed=%d retired=%v",
			removed, kvs.legacyPartitionsRetired)
	}
	if _, err := os.Stat(globalPath); err != nil {
		t.Fatalf("legacy partition disappeared before materialization: %v", err)
	}
	base.Close()
}
