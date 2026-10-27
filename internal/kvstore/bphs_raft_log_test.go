package kvstore

import (
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func TestBPHSRaftLogWALSkipsSharedWALSync(t *testing.T) {
	kvs := &KVServer{
		persister:                 &raft.Persister{},
		inlineThreshold:           512,
		indexBlockBytes:           512,
		bphsSyncConcurrency:       2,
		bphsRecordSyncConcurrency: 2,
		bphsWALBytes:              1 << 20,
		bphsRaftLogWAL:            true,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base")
	defer base.Close()

	store, err := openBPHSStore(
		kvs, base, filepath.Join(t.TempDir(), "bphs"),
		1<<20, 0, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	part, err := store.Put("0000000001", "raft-wal-value", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SyncPartitionsFast(map[int]struct{}{part: struct{}{}}); err != nil {
		t.Fatal(err)
	}
	if store.wal.Size() != 0 {
		t.Fatalf("shared WAL size = %d, want 0 in Raft-WAL mode", store.wal.Size())
	}
}

func TestBPHSRaftLogWALAdvancesDurableIndexAfterL0(t *testing.T) {
	kvs := &KVServer{
		persister:                 &raft.Persister{},
		inlineThreshold:           512,
		indexBlockBytes:           512,
		bphsSyncConcurrency:       1,
		bphsRecordSyncConcurrency: 1,
		bphsWALBytes:              1 << 20,
		bphsRaftLogWAL:            true,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base")
	defer base.Close()

	store, err := openBPHSStore(
		kvs, base, filepath.Join(t.TempDir(), "bphs"),
		1, 0, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.MarkAllDurable(1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put("0000000001", "flush-to-l0", 2); err != nil {
		t.Fatal(err)
	}
	if got := store.DurableIndex(); got < 2 {
		t.Fatalf("durable index = %d, want at least 2 after L0 flush", got)
	}
}
