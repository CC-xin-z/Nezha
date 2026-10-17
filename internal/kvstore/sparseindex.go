package kvstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// 稀疏块索引
//
// GC 产出的 sortedFile 是按 key 升序写入的（RocksDB 迭代器 SeekToFirst/Next 顺序），
// 且 key 定长 padding，因此字典序即为键序。早期实现为每个 key 在内存里保留一条
// key→offset 记录，内存随 key 数线性增长：100GB 的 64B 小值有约 10 亿个 key，
// 光索引就要 58GB。
//
// 外层每约 blockBytes 记录一个块起点，查找时先二分定位到块。块内再维护一张
// open-addressing 哈希表，表项只保存块内 uint16 偏移；key 仍留在 sortedFile 里，
// 哈希命中后回读该偏移处的 key 验证。这样既避免给每个 key 常驻一份 string，
// 也把块内点查从"顺序解析约 8 条"降为"通常比较 1 条"。

// SparseEntry 是稀疏索引的一项：一个数据块的起始偏移及块内首条 entry 的 key。
type SparseEntry struct {
	PaddedKey string // 块内首条 entry 的 key（padding 后，定长）
	Offset    int64  // 块在 sortedFile 中的起始偏移
}

// BlockHashMeta 描述一个块的哈希表在 SortedFileIndex.HashSlots 中的位置。
// Mask==0 表示这个块没有哈希表，读路径回退到顺序扫描。
type BlockHashMeta struct {
	Start uint32 // 哈希表第一个 slot 在 HashSlots 中的下标
	Mask  uint32 // 容量减一；哈希表使用线性探测
}

type blockHashEntry struct {
	paddedKey string
	offset    int64 // 相对块起点的字节偏移
}

type builtSparseIndex struct {
	Entries     []SparseEntry
	BlockHashes []BlockHashMeta
	HashSlots   []uint16
}

// SparseIndexBuilder 在顺序写 sortedFile 的同时构建稀疏索引。
// 调用方每写完一条 entry 就调用一次 Observe，最后用 Build 收尾。
type SparseIndexBuilder struct {
	entries      []SparseEntry
	blockHashes  []BlockHashMeta
	hashSlots    []uint16
	currentBlock []blockHashEntry
	blockBytes   int64
	sinceLast    int64
	started      bool
}

func NewSparseIndexBuilder(blockBytes int64) *SparseIndexBuilder {
	if blockBytes <= 0 {
		blockBytes = defaultIndexBlockBytes
	}
	return &SparseIndexBuilder{blockBytes: blockBytes}
}

// 4KB。块尺寸决定外层稀疏索引的内存与块内哈希表的粒度。早期没有块内索引时，
// 点查延迟随块内条数近似线性增长：
//
//	4KB(~44 条) +15% | 16KB(~174 条) +21% | 64KB(~697 条) +117%
//
// 现在块内改为哈希定位，既保留 4KB 下 SCAN 的优势，也避免 64KB 大块带来的
// 顺序查找放大。哈希表仍按块生成，不随文件总大小线性保存 key。
const defaultIndexBlockBytes = 4 * 1024

// Observe 记录一条刚写入的 entry。paddedKey 必须是写进文件的那个 key（已 padding），
// offset 是它的起始偏移，size 是它占用的字节数。
func (b *SparseIndexBuilder) Observe(paddedKey string, offset int64, size int64) {
	// 第一条永远建立索引点；当前块达到目标大小后，下一条开启新块。
	if !b.started || b.sinceLast >= b.blockBytes {
		b.finishBlock()
		b.entries = append(b.entries, SparseEntry{PaddedKey: paddedKey, Offset: offset})
		b.sinceLast = 0
		b.started = true
	}
	b.currentBlock = append(b.currentBlock, blockHashEntry{
		paddedKey: paddedKey,
		offset:    offset - b.entries[len(b.entries)-1].Offset,
	})
	b.sinceLast += size
}

// finishBlock 为刚结束的数据块生成紧凑哈希表。
//
// slot 中的 0 表示空，n>0 表示块内字节偏移 n-1。块内偏移超过 uint16 可表示
// 的范围时该块不建表，查询会安全回退到顺序扫描。
func (b *SparseIndexBuilder) finishBlock() {
	n := len(b.currentBlock)
	if n == 0 {
		return
	}

	meta := BlockHashMeta{}
	if n >= 2 {
		capacity := 8
		for capacity*7/10 < n {
			capacity <<= 1
		}

		offsetsFit := true
		for _, entry := range b.currentBlock {
			if entry.offset < 0 || entry.offset >= int64(^uint16(0)) {
				offsetsFit = false
				break
			}
		}
		start64 := uint64(len(b.hashSlots))
		end64 := start64 + uint64(capacity)
		if offsetsFit && end64 <= uint64(^uint32(0)) {
			meta = BlockHashMeta{Start: uint32(start64), Mask: uint32(capacity - 1)}
			b.hashSlots = append(b.hashSlots, make([]uint16, capacity)...)
			for _, entry := range b.currentBlock {
				slot := int(meta.Start) + int(blockKeyHash(entry.paddedKey)&meta.Mask)
				for b.hashSlots[slot] != 0 {
					slot = nextBlockHashSlot(slot, int(meta.Start), int(meta.Mask))
				}
				b.hashSlots[slot] = uint16(entry.offset + 1)
			}
		}
	}

	b.blockHashes = append(b.blockHashes, meta)
	b.currentBlock = b.currentBlock[:0]
}

func blockKeyHash(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func nextBlockHashSlot(slot, start, mask int) int {
	return start + ((slot - start + 1) & mask)
}

func (b *SparseIndexBuilder) Build() builtSparseIndex {
	b.finishBlock()
	return builtSparseIndex{
		Entries:     b.entries,
		BlockHashes: b.blockHashes,
		HashSlots:   b.hashSlots,
	}
}

// blockRange 返回可能包含 paddedKey 的块区间 [start, end)。
// paddedKey 小于文件中所有 key 时返回 ok=false。
func (sfi *SortedFileIndex) blockRange(paddedKey string) (start, end int64, ok bool) {
	_, start, end, ok = sfi.lookupBlock(paddedKey)
	return start, end, ok
}

// lookupBlock 返回可能包含 paddedKey 的块及其在 Sparse 中的下标。
func (sfi *SortedFileIndex) lookupBlock(paddedKey string) (block int, start, end int64, ok bool) {
	n := len(sfi.Sparse)
	if n == 0 {
		return 0, 0, 0, false
	}
	// 最后一个 PaddedKey <= paddedKey 的块
	i := sort.Search(n, func(j int) bool { return sfi.Sparse[j].PaddedKey > paddedKey }) - 1
	if i < 0 {
		return 0, 0, 0, false
	}
	start = sfi.Sparse[i].Offset
	end = sfi.FileSize
	if i+1 < n {
		end = sfi.Sparse[i+1].Offset
	}
	return i, start, end, true
}

// firstBlockAtOrAfter 返回第一个可能含有 >= paddedKey 的块的起始偏移。
// 供范围查询定位扫描起点：即使 paddedKey 本身不存在也能给出正确的起点。
func (sfi *SortedFileIndex) firstBlockAtOrAfter(paddedKey string) (int64, bool) {
	if len(sfi.Sparse) == 0 {
		return 0, false
	}
	if start, _, ok := sfi.blockRange(paddedKey); ok {
		return start, true
	}
	// paddedKey 比文件中所有 key 都小，从头开始扫
	return sfi.Sparse[0].Offset, true
}

// scanBlock 在 [start, end) 内顺序查找 paddedKey。
// 找到返回其 entry；块内 key 已超过目标说明不存在，返回 raft.ErrNoKey。
func (kvs *KVServer) scanBlock(index *SortedFileIndex, paddedKey string, start, end int64) (*raft.Entry, error) {
	file, err := os.Open(index.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	// 缓冲按块大小开。原来固定 64KB，而一次块查找只读 4KB —— 每次查找白分配 64KB，
	// 数量级上比解析本身还贵。上限兜底，防异常索引把分配放大。
	blockLen := end - start
	if blockLen <= 0 {
		return nil, errors.New(raft.ErrNoKey)
	}
	bufSize := int(blockLen)
	if bufSize > 1<<20 {
		bufSize = 1 << 20
	}
	reader := bufio.NewReaderSize(io.LimitReader(file, blockLen), bufSize)
	// 解析条数就是 AVP 每次命中所省下的工作量，逐条计数后统一上报
	scanned := 0
	for {
		// 先只读头与 key：块内平均 8 条里只有 1 条是目标，其余 7 条的 value
		// 不读也不拷贝（见 readEntryKeyOnly）。
		key, valueSize, err := readEntryKeyOnly(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			avpRecordScan(scanned, blockLen)
			return nil, err
		}
		scanned++
		if key == paddedKey {
			value, err := readValueBytes(reader, valueSize)
			if err != nil {
				avpRecordScan(scanned, blockLen)
				return nil, err
			}
			avpRecordScan(scanned, blockLen)
			return &raft.Entry{Key: key, Value: value}, nil
		}
		if key > paddedKey { // 已越过目标，块内有序，后面不可能再有
			break
		}
		// 不是目标：把它的 value 跳过，不拷贝
		avpRecordSkip(valueSize)
		if _, err := reader.Discard(int(valueSize)); err != nil {
			avpRecordScan(scanned, blockLen)
			return nil, err
		}
	}
	avpRecordScan(scanned, blockLen)
	return nil, errors.New(raft.ErrNoKey)
}

// lookupBlockHash 通过块内哈希表定位 entry。
//
// 哈希表只保存块内偏移，不保存 key。发生哈希碰撞时用线性探测继续找；每个候选
// 都会回读 sortedFile 里的 key 做最终确认，因此碰撞不会产生错误结果。
func (kvs *KVServer) lookupBlockHash(index *SortedFileIndex, paddedKey string, meta BlockHashMeta, start, end int64) (*raft.Entry, error) {
	blockLen := end - start
	const maxHashBlockBytes = 1 << 20
	if blockLen <= 0 || blockLen > maxHashBlockBytes {
		avpRecordHashFallback()
		return kvs.scanBlock(index, paddedKey, start, end)
	}

	file, err := os.Open(index.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	block := make([]byte, int(blockLen))
	n, err := file.ReadAt(block, start)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != len(block) {
		return nil, io.ErrUnexpectedEOF
	}

	capacity := int(meta.Mask) + 1
	slot := int(meta.Start) + int(blockKeyHash(paddedKey)&meta.Mask)
	probes := 0
	for probes < capacity {
		stored := index.HashSlots[slot]
		if stored == 0 {
			avpRecordScan(probes, blockLen)
			avpRecordHashLookup(probes)
			return nil, errors.New(raft.ErrNoKey)
		}

		probes++
		key, valueStart, valueEnd, err := entryAtBlockOffset(block, int(stored)-1)
		if err != nil {
			avpRecordScan(probes, blockLen)
			avpRecordHashLookup(probes)
			return nil, err
		}
		if equalStringBytes(paddedKey, key) {
			avpRecordScan(probes, blockLen)
			avpRecordHashLookup(probes)
			return &raft.Entry{Key: paddedKey, Value: string(block[valueStart:valueEnd])}, nil
		}
		slot = nextBlockHashSlot(slot, int(meta.Start), int(meta.Mask))
	}

	avpRecordScan(probes, blockLen)
	avpRecordHashLookup(probes)
	return nil, errors.New(raft.ErrNoKey)
}

func entryAtBlockOffset(block []byte, offset int) (key []byte, valueStart, valueEnd int, err error) {
	if offset < 0 || offset > len(block)-20 {
		return nil, 0, 0, errors.New("block entry offset out of range")
	}
	keySize := int(binary.LittleEndian.Uint32(block[offset+12 : offset+16]))
	valueSize := int(binary.LittleEndian.Uint32(block[offset+16 : offset+20]))
	keyStart := offset + 20
	valueStart = keyStart + keySize
	valueEnd = valueStart + valueSize
	if keyStart < offset || valueStart < keyStart || valueEnd < valueStart || valueEnd > len(block) {
		return nil, 0, 0, errors.New("block entry extends past block boundary")
	}
	return block[keyStart:valueStart], valueStart, valueEnd, nil
}

func equalStringBytes(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := range b {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

// lookupInSortedFile 通过稀疏索引和块内哈希索引查找 key 对应的 entry。
func (kvs *KVServer) lookupInSortedFile(index *SortedFileIndex, key string) (*raft.Entry, error) {
	if index == nil {
		return nil, errors.New("invalid index: index is nil")
	}
	paddedKey := kvs.persister.PadKey(key)
	block, start, end, ok := index.lookupBlock(paddedKey)
	if !ok {
		return nil, errors.New(raft.ErrNoKey)
	}
	if block < len(index.BlockHashes) {
		meta := index.BlockHashes[block]
		if meta.Mask != 0 {
			return kvs.lookupBlockHash(index, paddedKey, meta, start, end)
		}
	}
	return kvs.scanBlock(index, paddedKey, start, end)
}

// BuildSparseIndex 扫描整个 sortedFile 重建稀疏索引（进程重启或索引重建时使用）。
func (kvs *KVServer) BuildSparseIndex(filePath string, blockBytes int64) (builtSparseIndex, int64, error) {
	if blockBytes <= 0 {
		blockBytes = defaultIndexBlockBytes
	}
	file, err := os.Open(filePath)
	if err != nil {
		return builtSparseIndex{}, 0, fmt.Errorf("failed to open sorted file: %v", err)
	}
	defer file.Close()

	builder := NewSparseIndexBuilder(blockBytes)
	reader := bufio.NewReaderSize(file, 1<<20)
	var offset int64
	for {
		entry, size, err := ReadEntry(reader, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return builtSparseIndex{}, 0, err
		}
		builder.Observe(entry.Key, offset, size)
		offset += size
	}
	return builder.Build(), offset, nil
}

type SortedFileIndex struct {
	// Sparse 是按 key 升序的稀疏块索引，每约 indexBlockBytes 一项。
	// 查找时二分定位到块，再通过对应的块内哈希表定位 entry。
	Sparse      []SparseEntry
	BlockHashes []BlockHashMeta // 与 Sparse 一一对应
	HashSlots   []uint16        // 所有块共享的扁平槽数组；0=空，n=块内偏移 n-1
	FileSize    int64           // sortedFile 总长度，用于界定最后一块的右边界
	// InlineValues 是小值的有界缓存，纯读加速层，命中则免去一次文件 seek。
	// 它可以为 nil、可以随时淘汰任何条目，都不影响正确性——value 始终在 sortedFile 里。
	InlineValues *InlineCache
	FilePath     string
}
