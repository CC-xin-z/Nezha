package kvstore

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func mkIndex(entries []SparseEntry, fileSize int64) *SortedFileIndex {
	return &SortedFileIndex{Sparse: entries, FileSize: fileSize}
}

// 块边界必须正确：最后一块用 FileSize 收尾
func TestBlockRange(t *testing.T) {
	idx := mkIndex([]SparseEntry{
		{PaddedKey: "0000000100", Offset: 0},
		{PaddedKey: "0000000200", Offset: 1000},
		{PaddedKey: "0000000300", Offset: 2000},
	}, 3000)

	cases := []struct {
		key          string
		wantS, wantE int64
		wantOK       bool
		desc         string
	}{
		{"0000000100", 0, 1000, true, "命中首块首key"},
		{"0000000150", 0, 1000, true, "落在首块中间"},
		{"0000000200", 1000, 2000, true, "命中第二块首key"},
		{"0000000250", 1000, 2000, true, "落在第二块中间"},
		{"0000000300", 2000, 3000, true, "命中末块，右边界用 FileSize"},
		{"0000009999", 2000, 3000, true, "大于所有key，仍归入末块"},
		{"0000000050", 0, 0, false, "小于所有key，应 ok=false"},
	}
	for _, c := range cases {
		s, e, ok := idx.blockRange(c.key)
		if ok != c.wantOK || (ok && (s != c.wantS || e != c.wantE)) {
			t.Errorf("%s: blockRange(%s) = %d,%d,%v; want %d,%d,%v",
				c.desc, c.key, s, e, ok, c.wantS, c.wantE, c.wantOK)
		}
	}
}

// 范围查询起点：key 小于全部时应从头开始，而不是放弃
func TestFirstBlockAtOrAfter(t *testing.T) {
	idx := mkIndex([]SparseEntry{
		{PaddedKey: "0000000100", Offset: 0},
		{PaddedKey: "0000000200", Offset: 1000},
	}, 2000)

	if off, ok := idx.firstBlockAtOrAfter("0000000050"); !ok || off != 0 {
		t.Errorf("小于所有key时应从首块起扫: got %d,%v want 0,true", off, ok)
	}
	if off, ok := idx.firstBlockAtOrAfter("0000000250"); !ok || off != 1000 {
		t.Errorf("落在末块: got %d,%v want 1000,true", off, ok)
	}
	if _, ok := mkIndex(nil, 0).firstBlockAtOrAfter("x"); ok {
		t.Error("空索引应返回 ok=false")
	}
}

// 构建器：每块一项，且首条必建索引点
func TestSparseIndexBuilder(t *testing.T) {
	b := NewSparseIndexBuilder(100) // 100 字节一块
	var off int64
	for i := 0; i < 10; i++ { // 10 条 × 30 字节 = 300 字节
		b.Observe(fmt.Sprintf("%010d", i), off, 30)
		off += 30
	}
	got := b.Build().Entries
	// 期望索引点落在累计字节跨过 100 的边界：第0条(0)、第4条(120)、第8条(240)
	want := []SparseEntry{
		{PaddedKey: "0000000000", Offset: 0},
		{PaddedKey: "0000000004", Offset: 120},
		{PaddedKey: "0000000008", Offset: 240},
	}
	if len(got) != len(want) {
		t.Fatalf("索引项数 = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第%d项 = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// 索引必须严格按 key 升序，二分才成立
func TestSparseIndexIsSorted(t *testing.T) {
	b := NewSparseIndexBuilder(64)
	var off int64
	for i := 0; i < 1000; i++ {
		b.Observe(fmt.Sprintf("%010d", i), off, 20)
		off += 20
	}
	got := b.Build().Entries
	if len(got) < 2 {
		t.Fatal("索引项太少")
	}
	for i := 1; i < len(got); i++ {
		if got[i].PaddedKey <= got[i-1].PaddedKey {
			t.Fatalf("第%d项 key 未递增: %q <= %q", i, got[i].PaddedKey, got[i-1].PaddedKey)
		}
		if got[i].Offset <= got[i-1].Offset {
			t.Fatalf("第%d项 offset 未递增: %d <= %d", i, got[i].Offset, got[i-1].Offset)
		}
	}
}

// 内存：稀疏索引项数应远小于 key 数
func TestSparseIndexMemoryReduction(t *testing.T) {
	const keys = 100000
	const entryBytes = 94 // 20B 头 + 10B key + 64B value
	b := NewSparseIndexBuilder(64 * 1024)
	var off int64
	for i := 0; i < keys; i++ {
		b.Observe(fmt.Sprintf("%010d", i), off, entryBytes)
		off += entryBytes
	}
	n := len(b.Build().Entries)
	ratio := float64(keys) / float64(n)
	if ratio < 100 {
		t.Fatalf("压缩比仅 %.0fx（%d key -> %d 索引项），期望 >100x", ratio, keys, n)
	}
	t.Logf("%d 个 key -> %d 个索引项，压缩 %.0fx", keys, n, ratio)
}

func TestBlockHashDisablesWhenUint16OffsetOverflows(t *testing.T) {
	b := NewSparseIndexBuilder(1 << 20)
	b.Observe("0000000001", 0, 65535)
	b.Observe("0000000002", 65535, 100)

	built := b.Build()
	if len(built.Entries) != 1 || len(built.BlockHashes) != 1 {
		t.Fatalf("entries=%d hashes=%d, want 1,1", len(built.Entries), len(built.BlockHashes))
	}
	if built.BlockHashes[0].Mask != 0 {
		t.Fatal("第二个 entry 的偏移超出 uint16，必须禁用该块哈希表并回退顺序扫描")
	}
}

func TestBlockHashIndexFindsEveryKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "RaftState_sorted_test")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	kvs := &KVServer{persister: &raft.Persister{}}
	writer := bufio.NewWriter(file)
	const count = 257
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%010d", i*2)
		entry := &raft.Entry{
			Index: uint32(i + 1),
			Key:   key,
			Value: fmt.Sprintf("value-%06d", i),
		}
		if err := kvs.WriteEntryToSortedFile(writer, entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	built, fileSize, err := kvs.BuildSparseIndex(path, 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(built.BlockHashes) != len(built.Entries) {
		t.Fatalf("BlockHashes=%d, Sparse=%d; 必须一一对应", len(built.BlockHashes), len(built.Entries))
	}
	indexedBlocks := 0
	for _, meta := range built.BlockHashes {
		if meta.Mask != 0 {
			indexedBlocks++
		}
	}
	if indexedBlocks == 0 {
		t.Fatal("没有生成任何块内哈希表")
	}

	index := &SortedFileIndex{
		Sparse:      built.Entries,
		BlockHashes: built.BlockHashes,
		HashSlots:   built.HashSlots,
		FileSize:    fileSize,
		FilePath:    path,
	}
	hashLookupsBefore := avpStats.hashLookups.Load()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%010d", i*2)
		got, err := kvs.lookupInSortedFile(index, key)
		if err != nil {
			t.Fatalf("lookup(%s): %v", key, err)
		}
		want := fmt.Sprintf("value-%06d", i)
		if got.Key != key || got.Value != want {
			t.Fatalf("lookup(%s) = {%q,%q}, want {%q,%q}", key, got.Key, got.Value, key, want)
		}
	}
	if avpStats.hashLookups.Load() <= hashLookupsBefore {
		t.Fatal("存在块内哈希表，但查找没有走哈希路径")
	}

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%010d", i*2+1)
		if _, err := kvs.lookupInSortedFile(index, key); err == nil {
			t.Fatalf("不存在的 key %s 被错误命中", key)
		}
	}
}
