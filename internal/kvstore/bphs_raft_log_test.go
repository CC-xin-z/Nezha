package kvstore

import (
	"fmt"
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

func TestBPHSRaftLogWALMaterializeAllRecoversUnflushedReplayTail(t *testing.T) {
	kvs := &KVServer{
		persister:                 &raft.Persister{},
		inlineThreshold:           512,
		indexBlockBytes:           128,
		bphsSyncConcurrency:       2,
		bphsRecordSyncConcurrency: 2,
		bphsWALBytes:              1 << 20,
		bphsRaftLogWAL:            true,
	}
	base := newBPHSTestBase(t, kvs, "0000000100", "base")
	defer base.Close()

	dir := filepath.Join(t.TempDir(), "bphs")
	store, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkAllDurable(1); err != nil {
		store.Close()
		t.Fatal(err)
	}

	dirty := make(map[int]struct{})
	for i := 2; i <= 9; i++ {
		part, err := store.Put(fmt.Sprintf("%010d", i), fmt.Sprintf("value-%d", i), uint32(i))
		if err != nil {
			store.Close()
			t.Fatal(err)
		}
		dirty[part] = struct{}{}
	}
	if err := store.SyncPartitionsFast(dirty); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if got := store.DurableIndex(); got != 1 {
		store.Close()
		t.Fatalf("durable index before materialization = %d, want 1", got)
	}

	if err := store.MaterializeAll(); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if got := store.DurableIndex(); got != 9 {
		store.Close()
		t.Fatalf("durable index after materialization = %d, want 9", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 2; i <= 9; i++ {
		want := fmt.Sprintf("value-%d", i)
		got, found, err := reopened.Get(fmt.Sprintf("%010d", i))
		if err != nil || !found || got != want {
			t.Fatalf("key %d after reopen = %q,%v,%v; want %q,true,nil",
				i, got, found, err, want)
		}
	}
}

func TestKVRecoveryGateWaitsForKVApplyNotOnlyRaftDelivery(t *testing.T) {
	kvs := &KVServer{
		recoveryTarget: 5,
		recoveryDone:   make(chan struct{}),
	}
	kvs.noteRecoveryApplied(4)
	select {
	case <-kvs.recoveryDone:
		t.Fatal("recovery gate opened before the KV layer reached the target")
	default:
	}
	kvs.noteRecoveryApplied(5)
	select {
	case <-kvs.recoveryDone:
	default:
		t.Fatal("recovery gate stayed closed after the KV layer reached the target")
	}
	kvs.noteRecoveryApplied(6)
}
