package kvstore

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func newBPHSTestBase(t *testing.T, kvs *KVServer, key, value string) *PartitionSet {
	t.Helper()
	path := filepath.Join(t.TempDir(), "base.p0")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := bufio.NewWriter(file)
	if err := kvs.WriteEntryToSortedFile(writer, &raft.Entry{
		Index: 1, Key: key, Value: value,
	}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	sparse, size, err := kvs.BuildSparseIndex(path, kvs.indexBlockBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewFileDescriptorPool(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	part := &SortedFileIndex{
		Sparse:       sparse,
		FileSize:     size,
		InlineValues: NewInlineCache(0),
		FilePath:     path,
		Lo:           key,
		Hi:           key,
		pool:         pool,
		Entries:      1,
	}
	ps := &PartitionSet{parts: []*SortedFileIndex{part}}
	ps.assignOwnershipRanges()
	return ps
}

func TestBPHSStorePutGetAndRestart(t *testing.T) {
	kvs := &KVServer{
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 512,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base-old")
	defer base.Close()

	dir := filepath.Join(t.TempDir(), "bphs")
	store, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	dirty := map[int]struct{}{}
	for _, tc := range []struct {
		key, value string
		version    uint32
	}{
		{"0000000001", "small", 2},
		{"0000000002", strings.Repeat("x", 1024), 3},
		{"0000000005", "base-new", 4},
	} {
		idx, err := store.Put(tc.key, tc.value, tc.version)
		if err != nil {
			t.Fatalf("put(%s): %v", tc.key, err)
		}
		dirty[idx] = struct{}{}
	}
	if err := store.SyncPartitions(dirty); err != nil {
		t.Fatal(err)
	}

	for key, want := range map[string]string{
		"0000000001": "small",
		"0000000002": strings.Repeat("x", 1024),
		"0000000005": "base-new",
	} {
		got, found, err := store.Get(key)
		if err != nil {
			t.Fatalf("get(%s): %v", key, err)
		}
		if !found || got != want {
			t.Fatalf("get(%s) = %q,%v; want %q,true", key, got, found, want)
		}
	}
	scan, err := store.Scan("0000000000", "0000000009")
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"0000000001": "small",
		"0000000002": strings.Repeat("x", 1024),
		"0000000005": "base-new",
	} {
		if got := scan[key]; got != want {
			t.Fatalf("scan[%s] = %q, want %q; all=%v", key, got, want, scan)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{
		"0000000001": "small",
		"0000000002": strings.Repeat("x", 1024),
		"0000000005": "base-new",
	} {
		got, found, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("reopened get(%s): %v", key, err)
		}
		if !found || got != want {
			t.Fatalf("reopened get(%s) = %q,%v; want %q,true", key, got, found, want)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 { // one records log + one vlog
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, fmt.Sprint(entry.Name()))
		}
		t.Fatalf("BPHS files = %v, want records+vlog", names)
	}
}

func TestBPHSStoreFlushL0AndRecoverFromNewerRecords(t *testing.T) {
	kvs := &KVServer{
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 128,
	}
	base := newBPHSTestBase(t, kvs, "0000000000", "base")
	defer base.Close()

	dir := filepath.Join(t.TempDir(), "bphs")
	store, err := openBPHSStore(kvs, base, dir, 1, 0, 0) // 每条都会触发一次 flush
	if err != nil {
		t.Fatal(err)
	}
	dirty := map[int]struct{}{}
	for i := 1; i <= 8; i++ {
		idx, err := store.Put(
			fmt.Sprintf("%010d", i),
			fmt.Sprintf("value-%d", i),
			uint32(i),
		)
		if err != nil {
			t.Fatal(err)
		}
		dirty[idx] = struct{}{}
	}
	if err := store.SyncPartitions(dirty); err != nil {
		t.Fatal(err)
	}
	if len(store.parts[0].l0) == 0 {
		t.Fatal("memtable 满后没有生成 L0")
	}
	if len(store.parts[0].l0) >= bphsL0CompactionTrigger {
		t.Fatalf("L0 compaction 未触发：仍有 %d 个 run", len(store.parts[0].l0))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openBPHSStore(kvs, base, dir, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 1; i <= 8; i++ {
		got, found, err := reopened.Get(fmt.Sprintf("%010d", i))
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("value-%d", i)
		if !found || got != want {
			t.Fatalf("get(%d) = %q,%v; want %q,true", i, got, found, want)
		}
	}
}

func TestBPHSFullPartitionCompaction(t *testing.T) {
	kvs := &KVServer{
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 128,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base-old")
	defer base.Close()

	dir := filepath.Join(t.TempDir(), "bphs")
	store, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	dirty := map[int]struct{}{}

	for _, tc := range []struct {
		key, value string
		version    uint32
	}{
		{"0000000001", "small", 2},
		{"0000000005", "l0-old", 3},
	} {
		idx, err := store.Put(tc.key, tc.value, tc.version)
		if err != nil {
			t.Fatal(err)
		}
		dirty[idx] = struct{}{}
	}
	if err := store.SyncPartitions(dirty); err != nil {
		t.Fatal(err)
	}
	part := store.parts[0]
	part.mu.Lock()
	if err := store.flushPartitionLocked(part); err != nil {
		part.mu.Unlock()
		t.Fatal(err)
	}
	if len(part.l0) != 1 {
		part.mu.Unlock()
		t.Fatalf("flush produced %d L0 runs, want 1", len(part.l0))
	}
	staleL0Path := part.l0[0].FilePath
	staleL0Data, err := os.ReadFile(staleL0Path)
	if err != nil {
		part.mu.Unlock()
		t.Fatal(err)
	}
	staleL0Index, err := os.ReadFile(sparseIndexPath(staleL0Path))
	if err != nil {
		part.mu.Unlock()
		t.Fatal(err)
	}
	part.mu.Unlock()

	for _, tc := range []struct {
		key, value string
		version    uint32
	}{
		{"0000000002", strings.Repeat("x", 1024), 4},
		{"0000000005", "base-new", 5},
	} {
		idx, err := store.Put(tc.key, tc.value, tc.version)
		if err != nil {
			t.Fatal(err)
		}
		dirty[idx] = struct{}{}
	}
	if err := store.SyncPartitions(dirty); err != nil {
		t.Fatal(err)
	}

	part.mu.Lock()
	if err := store.compactPartitionLocked(part); err != nil {
		part.mu.Unlock()
		t.Fatal(err)
	}
	part.mu.Unlock()

	if !part.baseLocal || part.gen != 2 {
		t.Fatalf("compaction did not install local base: local=%v gen=%d", part.baseLocal, part.gen)
	}
	if part.records.Size() != 0 || part.vlog.Size() != 0 || len(part.l0) != 0 {
		t.Fatalf("compaction did not reset delta: records=%d vlog=%d l0=%d",
			part.records.Size(), part.vlog.Size(), len(part.l0))
	}
	if _, err := store.SnapshotCut(filepath.Join(t.TempDir(), "too-old"), 4); !errors.Is(err, errSnapshotBusy) {
		t.Fatalf("SnapshotCut below compacted version error = %v, want errSnapshotBusy", err)
	}
	snapshotDir := t.TempDir()
	cut, err := store.SnapshotCut(snapshotDir, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut) != 1 || cut[0].BaseFile == "" || cut[0].BaseIndexFile == "" || cut[0].ManifestFile == "" {
		t.Fatalf("BPHS snapshot cut lacks local base files: %+v", cut)
	}
	for _, f := range []struct {
		name string
		size int64
	}{
		{cut[0].BaseFile, cut[0].BaseBytes},
		{cut[0].BaseIndexFile, cut[0].BaseIndexBytes},
		{cut[0].ManifestFile, cut[0].ManifestBytes},
	} {
		st, err := os.Stat(filepath.Join(snapshotDir, f.name))
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() != f.size {
			t.Fatalf("snapshot %s size = %d, want %d", f.name, st.Size(), f.size)
		}
	}

	for key, want := range map[string]string{
		"0000000001": "small",
		"0000000002": strings.Repeat("x", 1024),
		"0000000005": "base-new",
	} {
		got, found, err := store.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found || got != want {
			t.Fatalf("after compaction get(%s) = %q,%v; want %q,true", key, got, found, want)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟分区 GC 写完 manifest 后、删除旧 L0 前崩溃。旧 L0 中的 key5=v3
	// 必须被 base 中的 v5 压住，不能在重启后重新遮蔽新值。
	if err := os.WriteFile(staleL0Path, staleL0Data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(sparseIndexPath(staleL0Path)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sparseIndexPath(staleL0Path), staleL0Index, 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := len(reopened.parts[0].l0); got != 0 {
		t.Fatalf("recovered %d stale L0 runs, want 0", got)
	}
	for key, want := range map[string]string{
		"0000000001": "small",
		"0000000002": strings.Repeat("x", 1024),
		"0000000005": "base-new",
	} {
		got, found, err := reopened.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found || got != want {
			t.Fatalf("reopened compacted get(%s) = %q,%v; want %q,true", key, got, found, want)
		}
	}
}
