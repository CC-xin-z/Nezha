package kvstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// bphsRecordKind 区分分区 LSM 中自包含的小值和指向分区 VLog 的大值。
type bphsRecordKind byte

const (
	bphsInlineRecord bphsRecordKind = 1
	bphsBlobRecord   bphsRecordKind = 2
)

// bphsBlobRef 是分区 VLog 中的本地位置。Generation 在分区局部 Compaction 时推进，
// Offset 和 Length 都只对本分区、本 generation 有意义。
type bphsBlobRef struct {
	Generation uint64
	Offset     int64
	Length     int64
}

// bphsRecord 是一个逻辑 key 的最新版本。
//
// Version 直接使用 Raft CommandIndex，而不是客户端 Seq：P1 有统一的 Raft 顺序，
// 本地自增序列在 leader 切换后没有可比性。
type bphsRecord struct {
	Key       string
	Version   uint32
	Kind      bphsRecordKind
	Value     string
	Blob      bphsBlobRef
	LogOffset int64 // 仅内存记录使用；SST/L0 中的记录没有外部日志位置
}

// encodeBPHSValue 把记录类型编码进现有 sorted-file 的 Value 字段。L0 因此可以复用
// 当前的 entry 框架、稀疏索引和块哈希；BPHS 读路径再按首字节分流。
func encodeBPHSValue(rec bphsRecord) (string, error) {
	switch rec.Kind {
	case bphsInlineRecord:
		buf := make([]byte, 1+len(rec.Value))
		buf[0] = byte(bphsInlineRecord)
		copy(buf[1:], rec.Value)
		return string(buf), nil
	case bphsBlobRecord:
		buf := make([]byte, 1+8+8+8)
		buf[0] = byte(bphsBlobRecord)
		binary.LittleEndian.PutUint64(buf[1:9], rec.Blob.Generation)
		binary.LittleEndian.PutUint64(buf[9:17], uint64(rec.Blob.Offset))
		binary.LittleEndian.PutUint64(buf[17:25], uint64(rec.Blob.Length))
		return string(buf), nil
	default:
		return "", fmt.Errorf("unknown BPHS record kind %d", rec.Kind)
	}
}

func entryToBPHSRecord(entry *raft.Entry) (bphsRecord, error) {
	if entry == nil {
		return bphsRecord{}, fmt.Errorf("nil entry")
	}
	value := []byte(entry.Value)
	if len(value) < 1 {
		return bphsRecord{}, fmt.Errorf("empty BPHS value for key %q", entry.Key)
	}
	rec := bphsRecord{Key: entry.Key, Version: entry.Index}
	switch bphsRecordKind(value[0]) {
	case bphsInlineRecord:
		rec.Kind = bphsInlineRecord
		rec.Value = string(value[1:])
		return rec, nil
	case bphsBlobRecord:
		if len(value) != 25 {
			return bphsRecord{}, fmt.Errorf("blob record %q has %d bytes, want 25", entry.Key, len(value))
		}
		rec.Kind = bphsBlobRecord
		rec.Blob = bphsBlobRef{
			Generation: binary.LittleEndian.Uint64(value[1:9]),
			Offset:     int64(binary.LittleEndian.Uint64(value[9:17])),
			Length:     int64(binary.LittleEndian.Uint64(value[17:25])),
		}
		return rec, nil
	default:
		return bphsRecord{}, fmt.Errorf("unknown BPHS record kind %d for key %q", value[0], entry.Key)
	}
}

func bphsRecordEntry(rec bphsRecord) (*raft.Entry, error) {
	value, err := encodeBPHSValue(rec)
	if err != nil {
		return nil, err
	}
	return &raft.Entry{Index: rec.Version, Key: rec.Key, Value: value}, nil
}

type partitionMemtableNode struct {
	key         string
	record      bphsRecord
	left, right *partitionMemtableNode
	height      int
}

// partitionMemtable is an in-memory ordered index. The previous map-backed
// implementation had to copy and sort every key whenever a range scan raced
// with writes. The AVL tree makes Get O(log N) and a k-key range O(log N+k),
// while Sorted remains the only full traversal used by flush/compaction.
//
// The memtable itself is not internally locked. Callers serialize mutations
// and snapshots with the owning partition lock.
type partitionMemtable struct {
	root       *partitionMemtableNode
	count      int
	bytes      int64
	limit      int64
	maxVersion uint32

	// Sorted is also used by flush and compaction. It remains cached for those
	// paths, but online range scans do not depend on this cache.
	sortedCache []bphsRecord
	sortedValid bool
}

func newPartitionMemtable(limitBytes int64) *partitionMemtable {
	return &partitionMemtable{
		limit: limitBytes,
	}
}

func partitionMemtableHeight(node *partitionMemtableNode) int {
	if node == nil {
		return 0
	}
	return node.height
}

func updatePartitionMemtableHeight(node *partitionMemtableNode) {
	node.height = 1 + max(partitionMemtableHeight(node.left), partitionMemtableHeight(node.right))
}

func rotatePartitionMemtableRight(node *partitionMemtableNode) *partitionMemtableNode {
	left := node.left
	node.left = left.right
	left.right = node
	updatePartitionMemtableHeight(node)
	updatePartitionMemtableHeight(left)
	return left
}

func rotatePartitionMemtableLeft(node *partitionMemtableNode) *partitionMemtableNode {
	right := node.right
	node.right = right.left
	right.left = node
	updatePartitionMemtableHeight(node)
	updatePartitionMemtableHeight(right)
	return right
}

func balancePartitionMemtable(node *partitionMemtableNode) *partitionMemtableNode {
	if node == nil {
		return nil
	}
	updatePartitionMemtableHeight(node)
	balance := partitionMemtableHeight(node.left) - partitionMemtableHeight(node.right)
	if balance > 1 {
		if partitionMemtableHeight(node.left.left) < partitionMemtableHeight(node.left.right) {
			node.left = rotatePartitionMemtableLeft(node.left)
		}
		return rotatePartitionMemtableRight(node)
	}
	if balance < -1 {
		if partitionMemtableHeight(node.right.right) < partitionMemtableHeight(node.right.left) {
			node.right = rotatePartitionMemtableRight(node.right)
		}
		return rotatePartitionMemtableLeft(node)
	}
	return node
}

func insertPartitionMemtableNode(
	node *partitionMemtableNode,
	key string,
	record bphsRecord,
) (*partitionMemtableNode, bool) {
	if node == nil {
		return &partitionMemtableNode{key: key, record: record, height: 1}, true
	}
	if key < node.key {
		var inserted bool
		node.left, inserted = insertPartitionMemtableNode(node.left, key, record)
		if !inserted {
			return node, false
		}
	} else if key > node.key {
		var inserted bool
		node.right, inserted = insertPartitionMemtableNode(node.right, key, record)
		if !inserted {
			return node, false
		}
	} else {
		return node, false
	}
	return balancePartitionMemtable(node), true
}

func findPartitionMemtableNode(node *partitionMemtableNode, key string) *partitionMemtableNode {
	for node != nil {
		if key < node.key {
			node = node.left
		} else if key > node.key {
			node = node.right
		} else {
			return node
		}
	}
	return nil
}

func minPartitionMemtableNode(node *partitionMemtableNode) *partitionMemtableNode {
	for node != nil && node.left != nil {
		node = node.left
	}
	return node
}

func deletePartitionMemtableNode(
	node *partitionMemtableNode,
	key string,
) (*partitionMemtableNode, bool) {
	if node == nil {
		return nil, false
	}
	var deleted bool
	switch {
	case key < node.key:
		node.left, deleted = deletePartitionMemtableNode(node.left, key)
	case key > node.key:
		node.right, deleted = deletePartitionMemtableNode(node.right, key)
	default:
		deleted = true
		if node.left == nil {
			return node.right, true
		}
		if node.right == nil {
			return node.left, true
		}
		successor := minPartitionMemtableNode(node.right)
		node.key = successor.key
		node.record = successor.record
		node.right, _ = deletePartitionMemtableNode(node.right, successor.key)
	}
	if !deleted {
		return node, false
	}
	return balancePartitionMemtable(node), true
}

func appendPartitionMemtableRange(
	node *partitionMemtableNode,
	startKey, endKey string,
	out *[]bphsRecord,
) {
	if node == nil {
		return
	}
	if startKey < node.key {
		appendPartitionMemtableRange(node.left, startKey, endKey, out)
	}
	if node.key >= startKey && node.key <= endKey {
		*out = append(*out, node.record)
	}
	if endKey > node.key {
		appendPartitionMemtableRange(node.right, startKey, endKey, out)
	}
}

func appendPartitionMemtableAll(node *partitionMemtableNode, out *[]bphsRecord) {
	if node == nil {
		return
	}
	appendPartitionMemtableAll(node.left, out)
	*out = append(*out, node.record)
	appendPartitionMemtableAll(node.right, out)
}

func mergePartitionMemtableRecords(left, right []bphsRecord) []bphsRecord {
	if len(left) == 0 {
		return append([]bphsRecord(nil), right...)
	}
	if len(right) == 0 {
		return append([]bphsRecord(nil), left...)
	}
	out := make([]bphsRecord, 0, len(left)+len(right))
	for i, j := 0, 0; i < len(left) || j < len(right); {
		switch {
		case i >= len(left):
			out = append(out, right[j])
			j++
		case j >= len(right):
			out = append(out, left[i])
			i++
		case left[i].Key < right[j].Key:
			out = append(out, left[i])
			i++
		case left[i].Key > right[j].Key:
			out = append(out, right[j])
			j++
		default:
			if left[i].Version >= right[j].Version {
				out = append(out, left[i])
			} else {
				out = append(out, right[j])
			}
			i++
			j++
		}
	}
	return out
}

func collectPartitionMemtableKeysThroughVersion(
	node *partitionMemtableNode,
	version uint32,
	out *[]string,
) {
	if node == nil {
		return
	}
	collectPartitionMemtableKeysThroughVersion(node.left, version, out)
	if node.record.Version <= version {
		*out = append(*out, node.key)
	}
	collectPartitionMemtableKeysThroughVersion(node.right, version, out)
}

func (mt *partitionMemtable) Len() int {
	if mt == nil {
		return 0
	}
	return mt.count
}

func (mt *partitionMemtable) Bytes() int64 {
	if mt == nil {
		return 0
	}
	return mt.bytes
}

func (mt *partitionMemtable) Full() bool {
	return mt != nil && mt.limit > 0 && mt.bytes >= mt.limit
}

func (mt *partitionMemtable) Put(rec bphsRecord) bool {
	if mt == nil {
		return false
	}
	if existing := findPartitionMemtableNode(mt.root, rec.Key); existing != nil {
		if existing.record.Version > rec.Version {
			return false
		}
		mt.bytes -= bphsRecordBytes(existing.record)
		existing.record = rec
		mt.bytes += bphsRecordBytes(rec)
		if rec.Version > mt.maxVersion {
			mt.maxVersion = rec.Version
		}
		mt.sortedCache = nil
		mt.sortedValid = false
		return true
	}
	var inserted bool
	mt.root, inserted = insertPartitionMemtableNode(mt.root, rec.Key, rec)
	if !inserted {
		return false
	}
	mt.count++
	mt.bytes += bphsRecordBytes(rec)
	if rec.Version > mt.maxVersion {
		mt.maxVersion = rec.Version
	}
	mt.sortedCache = nil
	mt.sortedValid = false
	return true
}

func (mt *partitionMemtable) Get(key string) (bphsRecord, bool) {
	if mt == nil {
		return bphsRecord{}, false
	}
	node := findPartitionMemtableNode(mt.root, key)
	if node == nil {
		return bphsRecord{}, false
	}
	return node.record, true
}

func (mt *partitionMemtable) Sorted() []bphsRecord {
	if mt == nil {
		return nil
	}
	if mt.sortedValid {
		return mt.sortedCache
	}
	out := make([]bphsRecord, 0, mt.count)
	appendPartitionMemtableAll(mt.root, &out)
	mt.sortedCache = out
	mt.sortedValid = true
	return out
}

func (mt *partitionMemtable) MaxVersion() uint32 {
	if mt == nil {
		return 0
	}
	return mt.maxVersion
}

// SortedRange returns a detached, ordered copy of records in the inclusive
// range [startKey, endKey]. The copy can be read after the partition lock is
// released while the active tree continues to accept writes.
func (mt *partitionMemtable) SortedRange(startKey, endKey string) []bphsRecord {
	if mt == nil || startKey > endKey {
		return nil
	}
	out := make([]bphsRecord, 0)
	appendPartitionMemtableRange(mt.root, startKey, endKey, &out)
	if len(out) == 0 {
		return nil
	}
	return out
}

// PruneThroughVersion removes records already covered by a compacted base.
// Newer versions of the same keys remain in the MemTable and continue to
// override the base after compaction.
func (mt *partitionMemtable) PruneThroughVersion(version uint32) int {
	if mt == nil || version == 0 || mt.count == 0 {
		return 0
	}
	var keys []string
	collectPartitionMemtableKeysThroughVersion(mt.root, version, &keys)
	removed := 0
	for _, key := range keys {
		node := findPartitionMemtableNode(mt.root, key)
		if node == nil {
			continue
		}
		mt.bytes -= bphsRecordBytes(node.record)
		var deleted bool
		mt.root, deleted = deletePartitionMemtableNode(mt.root, key)
		if !deleted {
			continue
		}
		mt.count--
		removed++
	}
	if removed > 0 {
		mt.maxVersion = 0
		mt.sortedCache = nil
		mt.sortedValid = false
		var records []bphsRecord
		appendPartitionMemtableAll(mt.root, &records)
		for _, rec := range records {
			if rec.Version > mt.maxVersion {
				mt.maxVersion = rec.Version
			}
		}
	}
	return removed
}

func (mt *partitionMemtable) Reset() {
	if mt == nil {
		return
	}
	mt.root = nil
	mt.count = 0
	mt.bytes = 0
	mt.maxVersion = 0
	mt.sortedCache = nil
	mt.sortedValid = false
}

func bphsRecordBytes(rec bphsRecord) int64 {
	if rec.Kind == bphsBlobRecord {
		return int64(len(rec.Key) + 25)
	}
	return int64(len(rec.Key) + 1 + len(rec.Value))
}

// flushPartitionMemtable 把 MemTable 快照写成现有分区格式的 L0 run。
// 返回的 SortedFileIndex 可以直接复用现有稀疏索引、块哈希、文件描述符池和 SCAN
// 读取逻辑；上层只需要额外解码 BPHS Value 标记。
func (kvs *KVServer) flushPartitionMemtable(mt *partitionMemtable, path string, inlineCacheBytes int64) (*SortedFileIndex, error) {
	records := mt.Sorted()
	if len(records) == 0 {
		return nil, nil
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create BPHS L0 %s: %w", path, err)
	}
	writer := bufio.NewWriter(file)
	sparse := NewSparseIndexBuilder(kvs.indexBlockBytes)

	var offset int64
	lo, hi := records[0].Key, records[0].Key
	for _, rec := range records {
		entry, err := bphsRecordEntry(rec)
		if err != nil {
			file.Close()
			os.Remove(path)
			return nil, err
		}
		if err := kvs.WriteEntryToSortedFile(writer, entry); err != nil {
			file.Close()
			os.Remove(path)
			return nil, fmt.Errorf("write BPHS L0 %s: %w", path, err)
		}
		size := int64(20 + len(entry.Key) + len(entry.Value))
		sparse.Observe(entry.Key, offset, size)
		offset += size
		hi = rec.Key
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("flush BPHS L0 %s: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("fsync BPHS L0 %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("close BPHS L0 %s: %w", path, err)
	}

	sparseEntries := sparse.Build()
	if err := writeSparseIndex(path, sparseEntries, kvs.indexBlockBytes, offset); err != nil {
		return nil, fmt.Errorf("write BPHS L0 index %s: %w", path, err)
	}
	pool, err := NewFileDescriptorPool(path, partitionPoolSize)
	if err != nil {
		os.Remove(path)
		os.Remove(sparseIndexPath(path))
		return nil, fmt.Errorf("file descriptor pool for BPHS L0 %s: %w", path, err)
	}

	return &SortedFileIndex{
		Sparse:       sparseEntries,
		FileSize:     offset,
		InlineValues: NewInlineCache(inlineCacheBytes),
		FilePath:     path,
		Lo:           lo,
		Hi:           hi,
		pool:         pool,
		Entries:      len(records),
		bphsFilter:   newBPHSKeyFilterFromRecords(records),
	}, nil
}
