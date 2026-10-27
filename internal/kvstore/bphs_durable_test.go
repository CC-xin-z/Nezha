package kvstore

import "testing"

func TestBPHSDurableIndexUsesMinimumPartitionCoverage(t *testing.T) {
	dir := t.TempDir()
	parts := []*bphsPartition{{}, {}}
	parts[0].durableIndex.Store(10)
	parts[1].durableIndex.Store(20)

	store := &bphsStore{
		dir:         dir,
		durablePath: dir + "/durable_index.json",
		parts:       parts,
	}
	if err := store.refreshDurableIndex(); err != nil {
		t.Fatal(err)
	}
	if got := store.DurableIndex(); got != 10 {
		t.Fatalf("durable index = %d, want 10", got)
	}
	if got, ok, err := loadBPHSDurableIndex(dir); err != nil || !ok || got != 10 {
		t.Fatalf("persisted durable index = %d,%v,%v; want 10,true,nil", got, ok, err)
	}

	parts[0].durableIndex.Store(30)
	if err := store.refreshDurableIndex(); err != nil {
		t.Fatal(err)
	}
	if got := store.DurableIndex(); got != 20 {
		t.Fatalf("durable index = %d, want 20", got)
	}

	parts[1].durableIndex.Store(40)
	if err := store.refreshDurableIndex(); err != nil {
		t.Fatal(err)
	}
	if got := store.DurableIndex(); got != 30 {
		t.Fatalf("durable index = %d, want 30", got)
	}
}
