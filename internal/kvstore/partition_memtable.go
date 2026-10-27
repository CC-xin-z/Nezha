package kvstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

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

// partitionMemtable 是单个 Key Range 分区的版本化写缓冲。
//
// 覆盖写保留最高 Version；重复的旧 Raft entry 在恢复时可以被安全重放。只记录
// key+payload 的逻辑字节数作为 flush 触发依据，不把 Go map 的 allocator 开销当作
// 数据大小。
type partitionMemtable struct {
	records map[string]bphsRecord
	bytes   int64
	limit   int64

	// sortedCache is rebuilt only after a mutation. SCAN used to copy and sort
	// the entire partition map for every range query, even when only a hundred
	// keys were requested. All callers access a memtable under its partition
	// lock, so caching the immutable sorted snapshot here is safe.
	sortedCache []bphsRecord
	sortedValid bool
}

func newPartitionMemtable(limitBytes int64) *partitionMemtable {
	return &partitionMemtable{
		records: make(map[string]bphsRecord),
		limit:   limitBytes,
	}
}

func (mt *partitionMemtable) Len() int {
	if mt == nil {
		return 0
	}
	return len(mt.records)
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
	old, exists := mt.records[rec.Key]
	if exists && old.Version > rec.Version {
		return false
	}
	if exists {
		mt.bytes -= bphsRecordBytes(old)
	}
	mt.records[rec.Key] = rec
	mt.bytes += bphsRecordBytes(rec)
	mt.sortedCache = nil
	mt.sortedValid = false
	return true
}

func (mt *partitionMemtable) Get(key string) (bphsRecord, bool) {
	if mt == nil {
		return bphsRecord{}, false
	}
	rec, ok := mt.records[key]
	return rec, ok
}

func (mt *partitionMemtable) Sorted() []bphsRecord {
	if mt == nil {
		return nil
	}
	if mt.sortedValid {
		return mt.sortedCache
	}
	out := make([]bphsRecord, 0, len(mt.records))
	for _, rec := range mt.records {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	mt.sortedCache = out
	mt.sortedValid = true
	return out
}

func (mt *partitionMemtable) MaxVersion() uint32 {
	if mt == nil {
		return 0
	}
	var maxVersion uint32
	for _, rec := range mt.records {
		if rec.Version > maxVersion {
			maxVersion = rec.Version
		}
	}
	return maxVersion
}

// SortedRange returns only records whose keys fall in the inclusive range
// [startKey, endKey]. The sorted snapshot is immutable while callers hold the
// owning partition lock, so binary-searching and returning a subslice is safe.
func (mt *partitionMemtable) SortedRange(startKey, endKey string) []bphsRecord {
	if mt == nil || startKey > endKey {
		return nil
	}
	records := mt.Sorted()
	lo := sort.Search(len(records), func(i int) bool {
		return records[i].Key >= startKey
	})
	hi := sort.Search(len(records), func(i int) bool {
		return records[i].Key > endKey
	})
	if lo >= hi {
		return nil
	}
	return records[lo:hi]
}

func (mt *partitionMemtable) Reset() {
	if mt == nil {
		return
	}
	mt.records = make(map[string]bphsRecord)
	mt.bytes = 0
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
	}, nil
}
