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
	if err := os.WriteFile(filepath.Join(dir, "p000001.records"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "p000001.manifest.json"), []byte("{}"), 0o644); err != nil {
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

func TestBPHSSyncTracksDirtyFiles(t *testing.T) {
	kvs := &KVServer{
		persister:       &raft.Persister{},
		inlineThreshold: 512,
		indexBlockBytes: 128,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base")
	defer base.Close()

	store, err := openBPHSStore(kvs, base, filepath.Join(t.TempDir(), "bphs"), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	partID, err := store.Put("0000000001", "inline", 2)
	if err != nil {
		t.Fatal(err)
	}
	part := store.parts[partID]
	if !part.recordsDirty || part.vlogDirty {
		t.Fatalf("inline dirty flags = records:%v vlog:%v", part.recordsDirty, part.vlogDirty)
	}
	if err := store.SyncPartitions(map[int]struct{}{partID: struct{}{}}); err != nil {
		t.Fatal(err)
	}
	if part.recordsDirty || part.vlogDirty {
		t.Fatalf("dirty flags after inline sync = records:%v vlog:%v", part.recordsDirty, part.vlogDirty)
	}

	partID, err = store.Put("0000000002", strings.Repeat("x", 1024), 3)
	if err != nil {
		t.Fatal(err)
	}
	part = store.parts[partID]
	if !part.recordsDirty || !part.vlogDirty {
		t.Fatalf("blob dirty flags = records:%v vlog:%v", part.recordsDirty, part.vlogDirty)
	}
	if err := store.SyncPartitions(map[int]struct{}{partID: struct{}{}}); err != nil {
		t.Fatal(err)
	}
	if part.recordsDirty || part.vlogDirty {
		t.Fatalf("dirty flags after blob sync = records:%v vlog:%v", part.recordsDirty, part.vlogDirty)
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
	if got := store.parts[0].records.Size(); got != 0 {
		t.Fatalf("L0 durable 后 records 仍保留 %d 字节", got)
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

func TestBPHSSortedFileReadsPreserveVersions(t *testing.T) {
	kvs := &KVServer{indexBlockBytes: 128}

	rawPath := filepath.Join(t.TempDir(), "raw.p0")
	rawFile, err := os.Create(rawPath)
	if err != nil {
		t.Fatal(err)
	}
	rawWriter := bufio.NewWriter(rawFile)
	for _, tc := range []struct {
		key     string
		version uint32
		term    uint32
		voted   uint32
	}{
		{"0000000001", 31, 7, 2},
		{"0000000002", 44, 8, 3},
	} {
		if err := kvs.WriteEntryToSortedFile(rawWriter, &raft.Entry{
			Index:       tc.version,
			CurrentTerm: tc.term,
			VotedFor:    tc.voted,
			Key:         tc.key,
			Value:       "raw-" + tc.key,
		}); err != nil {
			rawFile.Close()
			t.Fatal(err)
		}
	}
	if err := rawWriter.Flush(); err != nil {
		rawFile.Close()
		t.Fatal(err)
	}
	if err := rawFile.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := readRawSortedFile(rawPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2 || raw[0].Version != 31 || raw[1].Version != 44 {
		t.Fatalf("raw versions = %+v, want [31 44]", raw)
	}
	rawReaderFile, err := os.Open(rawPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rawReaderFile.Close()
	first, _, err := ReadEntry(bufio.NewReaderSize(rawReaderFile, 1<<20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.Index != 31 || first.CurrentTerm != 7 || first.VotedFor != 2 {
		t.Fatalf("raw entry metadata = %+v", first)
	}

	mt := newPartitionMemtable(1 << 20)
	mt.Put(bphsRecord{
		Key: "0000000001", Version: 51, Kind: bphsInlineRecord, Value: "inline",
	})
	mt.Put(bphsRecord{
		Key: "0000000002", Version: 62, Kind: bphsBlobRecord,
		Blob: bphsBlobRef{Generation: 3, Offset: 7, Length: 9},
	})
	l0Path := filepath.Join(t.TempDir(), "p0_l0.sst")
	l0, err := kvs.flushPartitionMemtable(mt, l0Path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l0.closePool()
	encoded, err := readBPHSSortedFile(l0Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 2 || encoded[0].Version != 51 || encoded[1].Version != 62 {
		t.Fatalf("BPHS L0 versions = %+v, want [51 62]", encoded)
	}
}

func TestBPHSFullCompactionFromL0PreservesMaxVersion(t *testing.T) {
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
	defer store.Close()

	if _, err := store.Put("0000000001", "inline-v11", 11); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put("0000000002", strings.Repeat("x", 1024), 12); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncPartitions(map[int]struct{}{0: struct{}{}}); err != nil {
		t.Fatal(err)
	}

	part := store.parts[0]
	part.mu.Lock()
	if err := store.flushPartitionLocked(part); err != nil {
		part.mu.Unlock()
		t.Fatal(err)
	}
	if part.mem.Len() != 0 || len(part.l0) != 1 {
		part.mu.Unlock()
		t.Fatalf("before compaction mem=%d l0=%d", part.mem.Len(), len(part.l0))
	}
	if err := store.compactPartitionLocked(part); err != nil {
		part.mu.Unlock()
		t.Fatal(err)
	}
	part.mu.Unlock()

	manifest, ok, err := loadBPHSPartitionManifest(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || manifest.MaxVersion != 12 {
		t.Fatalf("manifest = %+v, ok=%v; want max_version=12", manifest, ok)
	}
	if got, found, err := store.Get("0000000002"); err != nil || !found ||
		got != strings.Repeat("x", 1024) {
		t.Fatalf("get after L0-only compaction = %q,%v,%v", got, found, err)
	}
}

func TestBPHSLegacyTailVersionFallback(t *testing.T) {
	for _, tc := range []struct {
		entryIndex uint32
		cutoff     uint32
		want       uint32
	}{
		{entryIndex: 27, cutoff: 100, want: 27},
		{entryIndex: 0, cutoff: 100, want: 100},
		{entryIndex: 0, cutoff: 0, want: 0},
	} {
		if got := bphsLegacyTailVersion(tc.entryIndex, tc.cutoff); got != tc.want {
			t.Fatalf("legacy version(%d,%d)=%d, want %d",
				tc.entryIndex, tc.cutoff, got, tc.want)
		}
	}
}

func TestBPHSVLogSizeTriggersCompactionBeforeMemtableFull(t *testing.T) {
	kvs := &KVServer{
		persister:                     &raft.Persister{},
		inlineThreshold:               512,
		indexBlockBytes:               128,
		bphsPartitionGCThresholdBytes: 16 << 10,
	}
	base := newBPHSTestBase(t, kvs, "0000000005", "base-old")
	defer base.Close()

	dir := filepath.Join(t.TempDir(), "bphs")
	store, err := openBPHSStore(kvs, base, dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	value := strings.Repeat("x", 4096)
	dirty := map[int]struct{}{}
	for i := 1; i <= 3; i++ {
		part, err := store.Put(fmt.Sprintf("%010d", i), value, uint32(i))
		if err != nil {
			t.Fatal(err)
		}
		dirty[part] = struct{}{}
	}
	if err := store.SyncPartitions(dirty); err != nil {
		t.Fatal(err)
	}

	part := store.parts[0]
	if part.mem.Full() {
		t.Fatal("test setup is invalid: MemTable triggered before VLog threshold")
	}
	if part.vlog.Size() == 0 {
		t.Fatal("4KB values did not enter the partition VLog")
	}
	if part.baseLocal {
		t.Fatal("partition compacted too early")
	}

	if _, err := store.Put("0000000004", value, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncPartitions(map[int]struct{}{0: struct{}{}}); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForCompactions(); err != nil {
		t.Fatal(err)
	}

	if !part.baseLocal {
		t.Fatal("VLog threshold did not trigger partition compaction")
	}
	if part.mem.Len() != 0 || part.vlog.Size() != 0 || part.records.Size() != 0 || len(part.l0) != 0 {
		t.Fatalf("partition delta was not reset: mem=%d vlog=%d records=%d l0=%d",
			part.mem.Len(), part.vlog.Size(), part.records.Size(), len(part.l0))
	}
	manifest, ok, err := loadBPHSPartitionManifest(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || manifest.MaxVersion != 4 || manifest.Generation != 2 {
		t.Fatalf("manifest = %+v, ok=%v; want generation=2 max_version=4", manifest, ok)
	}
	for i := 1; i <= 4; i++ {
		got, found, err := store.Get(fmt.Sprintf("%010d", i))
		if err != nil || !found || got != value {
			t.Fatalf("get(%d) = len=%d found=%v err=%v; want full 4KB value", i, len(got), found, err)
		}
	}
}
