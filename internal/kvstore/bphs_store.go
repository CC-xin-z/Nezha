package kvstore

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const bphsL0CompactionTrigger = 4

// bphsPartition 是固定 Key Range 上的本地写入层。
//
// base 是全局分区 GC 产出的不可变有序文件；memtable 和 records 保存 BPHS 启用后的
// 新版本。MemTable 达到预算后刷成 L0，l0 数量达到阈值时只在本分区内合并；
// records 仍是持久化 WAL，重启时只重放比最新 L0 更新的版本。
type bphsPartition struct {
	mu      sync.RWMutex
	index   int
	base    *SortedFileIndex
	mem     *partitionMemtable
	records *partitionRecordLog
	vlog    *partitionVLog
	l0      []*SortedFileIndex // 从旧到新
	gen     uint64
}

// bphsStore 是 BPHS 的节点本地状态。base 可以在全局 GC 完成后整体切换，但静态原型
// 要求分区数量和 Ownership Range 保持不变；动态 Split 不在这一版范围内。
type bphsStore struct {
	mu               sync.RWMutex
	kvs              *KVServer
	base             *PartitionSet
	parts            []*bphsPartition
	dir              string
	memtableBytes    int64
	inlineCacheBytes int64
}

func openBPHSStore(kvs *KVServer, base *PartitionSet, dir string, memtableBytes, inlineCacheBytes int64, maxVersion uint32) (*bphsStore, error) {
	if kvs == nil || base == nil || base.Len() == 0 {
		return nil, fmt.Errorf("BPHS requires a non-empty partition set")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &bphsStore{
		kvs:              kvs,
		base:             base,
		dir:              dir,
		memtableBytes:    memtableBytes,
		inlineCacheBytes: inlineCacheBytes,
	}
	for i, basePart := range base.parts {
		records, err := openPartitionRecordLog(filepath.Join(dir, fmt.Sprintf("p%06d.records", i)))
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("open partition %d record log: %w", i, err)
		}
		vlog, err := openPartitionVLog(filepath.Join(dir, fmt.Sprintf("p%06d.vlog", i)))
		if err != nil {
			records.Close()
			s.Close()
			return nil, fmt.Errorf("open partition %d vlog: %w", i, err)
		}
		part := &bphsPartition{
			index:   i,
			base:    basePart,
			mem:     newPartitionMemtable(memtableBytes),
			records: records,
			vlog:    vlog,
			gen:     1,
		}
		l0, maxL0Version, err := loadPartitionL0Files(kvs, dir, i, inlineCacheBytes)
		if err != nil {
			records.Close()
			vlog.Close()
			s.Close()
			return nil, fmt.Errorf("load partition %d L0: %w", i, err)
		}
		part.l0 = l0
		replayed, err := records.ScanAll()
		if err != nil {
			records.Close()
			vlog.Close()
			s.Close()
			return nil, fmt.Errorf("replay partition %d records: %w", i, err)
		}
		for _, rec := range replayed {
			if rec.Version <= maxL0Version {
				continue
			}
			if maxVersion > 0 && rec.Version > maxVersion {
				continue
			}
			part.mem.Put(rec)
		}
		s.parts = append(s.parts, part)
	}
	return s, nil
}

func (s *bphsStore) SetBase(base *PartitionSet) error {
	if s == nil {
		return nil
	}
	if base == nil || base.Len() != len(s.parts) {
		return fmt.Errorf("BPHS base partition count changed: have %d, want %d", base.Len(), len(s.parts))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.base = base
	for i, part := range s.parts {
		part.mu.Lock()
		part.base = base.parts[i]
		part.mu.Unlock()
	}
	return nil
}

func (s *bphsStore) route(paddedKey string) (*bphsPartition, error) {
	if s == nil {
		return nil, fmt.Errorf("BPHS store is nil")
	}
	s.mu.RLock()
	base := s.base
	s.mu.RUnlock()
	_, idx, ok := base.findOwned(paddedKey)
	if !ok || idx < 0 || idx >= len(s.parts) {
		return nil, fmt.Errorf("no BPHS partition owns key %q", paddedKey)
	}
	return s.parts[idx], nil
}

func (s *bphsStore) Dir() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dir
}

// Put 把一条已提交的 Raft PUT 写入目标分区。返回分区 id，调用方应把它加入 dirty
// 集合并在这批 applied index 落库之前调用 SyncPartitions。
func (s *bphsStore) Put(key, value string, version uint32) (int, error) {
	paddedKey := s.kvs.persister.PadKey(key)
	part, err := s.route(paddedKey)
	if err != nil {
		return -1, err
	}

	part.mu.Lock()
	defer part.mu.Unlock()

	rec := bphsRecord{Key: paddedKey, Version: version}
	if s.kvs.shouldInline(len(value)) {
		rec.Kind = bphsInlineRecord
		rec.Value = value
	} else {
		ref, err := part.vlog.Append([]byte(value))
		if err != nil {
			return -1, fmt.Errorf("append partition %d vlog: %w", part.index, err)
		}
		ref.Generation = part.gen
		rec.Kind = bphsBlobRecord
		rec.Blob = ref
	}
	offset, err := part.records.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("append partition %d record: %w", part.index, err)
	}
	rec.LogOffset = offset
	part.mem.Put(rec)
	if part.mem.Full() {
		if err := s.flushPartitionLocked(part); err != nil {
			return -1, err
		}
	}
	return part.index, nil
}

func (s *bphsStore) flushPartitionLocked(part *bphsPartition) error {
	if part == nil || part.mem.Len() == 0 {
		return nil
	}
	name := fmt.Sprintf("p%06d_l0_%020d.sst", part.index, time.Now().UnixNano())
	path := filepath.Join(s.dir, name)
	l0, err := s.kvs.flushPartitionMemtable(part.mem, path, s.inlineCacheBytes)
	if err != nil {
		return fmt.Errorf("flush partition %d memtable: %w", part.index, err)
	}
	if l0 == nil {
		part.mem.Reset()
		return nil
	}
	part.l0 = append(part.l0, l0)
	part.mem.Reset()
	if len(part.l0) >= bphsL0CompactionTrigger {
		if err := s.compactL0Locked(part); err != nil {
			return fmt.Errorf("compact partition %d L0: %w", part.index, err)
		}
	}
	return nil
}

func (s *bphsStore) compactL0Locked(part *bphsPartition) error {
	if part == nil || len(part.l0) < 2 {
		return nil
	}
	merged := newPartitionMemtable(0)
	for _, run := range part.l0 {
		records, err := readBPHSSortedFile(run.FilePath)
		if err != nil {
			return err
		}
		for _, rec := range records {
			merged.Put(rec)
		}
	}
	for _, rec := range part.mem.Sorted() {
		merged.Put(rec)
	}

	name := fmt.Sprintf("p%06d_l0_%020d.sst", part.index, time.Now().UnixNano())
	path := filepath.Join(s.dir, name)
	l0, err := s.kvs.flushPartitionMemtable(merged, path, s.inlineCacheBytes)
	if err != nil {
		return err
	}
	if l0 == nil {
		return nil
	}

	old := part.l0
	part.l0 = []*SortedFileIndex{l0}
	part.mem.Reset()
	for _, run := range old {
		run.closePool()
		for _, p := range []string{run.FilePath, sparseIndexPath(run.FilePath)} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func readBPHSSortedFile(path string) ([]bphsRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	var records []bphsRecord
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		rec, err := entryToBPHSRecord(entry)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
}

func loadPartitionL0Files(kvs *KVServer, dir string, partition int, inlineCacheBytes int64) ([]*SortedFileIndex, uint32, error) {
	pattern := filepath.Join(dir, fmt.Sprintf("p%06d_l0_*.sst", partition))
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, 0, err
	}
	var out []*SortedFileIndex
	var maxVersion uint32
	for _, path := range paths {
		st, err := os.Stat(path)
		if err != nil {
			return nil, 0, err
		}
		sparse, size, _, err := kvs.loadOrRebuildSparseIndex(path, st.Size())
		if err != nil {
			return nil, 0, err
		}
		if len(sparse) == 0 {
			return nil, 0, fmt.Errorf("BPHS L0 %s has no sparse index entries", path)
		}

		file, err := os.Open(path)
		if err != nil {
			return nil, 0, err
		}
		reader := bufio.NewReaderSize(file, 1<<20)
		count := 0
		for {
			entry, _, err := ReadEntry(reader, 0)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return nil, 0, err
			}
			count++
			if entry.Index > maxVersion {
				maxVersion = entry.Index
			}
		}
		file.Close()

		hi, err := lastKeyFrom(path, sparse[len(sparse)-1].Offset, size)
		if err != nil {
			return nil, 0, err
		}
		pool, err := NewFileDescriptorPool(path, partitionPoolSize)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, &SortedFileIndex{
			Sparse:       sparse,
			FileSize:     size,
			InlineValues: NewInlineCache(inlineCacheBytes),
			FilePath:     path,
			Lo:           sparse[0].PaddedKey,
			Hi:           hi,
			pool:         pool,
			Entries:      count,
		})
	}
	return out, maxVersion, nil
}

func (s *bphsStore) Get(key string) (value string, found bool, err error) {
	paddedKey := s.kvs.persister.PadKey(key)
	part, err := s.route(paddedKey)
	if err != nil {
		return "", false, err
	}

	part.mu.RLock()
	defer part.mu.RUnlock()
	if rec, ok := part.mem.Get(paddedKey); ok {
		switch rec.Kind {
		case bphsInlineRecord:
			return rec.Value, true, nil
		case bphsBlobRecord:
			if rec.Blob.Generation != part.gen {
				return "", false, fmt.Errorf("partition %d blob generation %d, current %d",
					part.index, rec.Blob.Generation, part.gen)
			}
			raw, err := part.vlog.Read(rec.Blob)
			if err != nil {
				return "", false, err
			}
			return string(raw), true, nil
		}
	}

	// L0 目前不会自动产生；保留从新到旧的查找顺序，供局部 Compaction 接入。
	for i := len(part.l0) - 1; i >= 0; i-- {
		entry, err := s.kvs.lookupInSortedFile(part.l0[i], paddedKey)
		if err != nil {
			if err == ErrKeyAbsent {
				continue
			}
			return "", false, err
		}
		rec, err := entryToBPHSRecord(entry)
		if err != nil {
			return "", false, err
		}
		if rec.Kind == bphsInlineRecord {
			return rec.Value, true, nil
		}
		raw, err := part.vlog.Read(rec.Blob)
		if err != nil {
			return "", false, err
		}
		return string(raw), true, nil
	}

	if part.base != nil {
		entry, err := s.kvs.lookupInSortedFile(part.base, paddedKey)
		if err == nil {
			return entry.Value, true, nil
		}
		if err != ErrKeyAbsent {
			return "", false, err
		}
	}
	return "", false, nil
}

func (s *bphsStore) Scan(startKey, endKey string) (map[string]string, error) {
	paddedStart := s.kvs.persister.PadKey(startKey)
	paddedEnd := s.kvs.persister.PadKey(endKey)
	if paddedStart > paddedEnd {
		return nil, nil
	}

	s.mu.RLock()
	base := s.base
	s.mu.RUnlock()

	result := make(map[string]string)
	for i, basePart := range base.parts {
		if basePart.OwnershipHi != "" && paddedStart >= basePart.OwnershipHi {
			continue
		}
		if basePart.OwnershipLo != "" && paddedEnd < basePart.OwnershipLo {
			continue
		}
		part := s.parts[i]
		part.mu.RLock()

		if part.base != nil {
			if err := s.kvs.scanFromSortedFileInto(result, paddedStart, paddedEnd, part.base); err != nil {
				part.mu.RUnlock()
				return nil, err
			}
		}
		for _, l0 := range part.l0 {
			if err := s.scanBPHSSortedFileInto(result, paddedStart, paddedEnd, part, l0); err != nil {
				part.mu.RUnlock()
				return nil, err
			}
		}
		for _, rec := range part.mem.Sorted() {
			if rec.Key < paddedStart || rec.Key > paddedEnd {
				continue
			}
			value, err := s.recordValue(part, rec)
			if err != nil {
				part.mu.RUnlock()
				return nil, err
			}
			result[s.kvs.persister.UnpadKey(rec.Key)] = value
		}
		part.mu.RUnlock()
	}
	return result, nil
}

func (s *bphsStore) scanBPHSSortedFileInto(result map[string]string, startKey, endKey string, part *bphsPartition, index *SortedFileIndex) error {
	offset, ok := index.firstBlockAtOrAfter(startKey)
	if !ok {
		return nil
	}
	if index.pool == nil {
		return fmt.Errorf("BPHS L0 %s has no descriptor pool", index.FilePath)
	}
	file, err := index.pool.Get()
	if err != nil {
		return err
	}
	defer index.pool.Put(file)
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(file, 1<<20)
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Key > endKey {
			return nil
		}
		if entry.Key < startKey {
			continue
		}
		rec, err := entryToBPHSRecord(entry)
		if err != nil {
			return err
		}
		value, err := s.recordValue(part, rec)
		if err != nil {
			return err
		}
		result[s.kvs.persister.UnpadKey(rec.Key)] = value
	}
}

func (s *bphsStore) recordValue(part *bphsPartition, rec bphsRecord) (string, error) {
	if rec.Kind == bphsInlineRecord {
		return rec.Value, nil
	}
	if rec.Blob.Generation != part.gen {
		return "", fmt.Errorf("partition %d blob generation %d, current %d",
			part.index, rec.Blob.Generation, part.gen)
	}
	raw, err := part.vlog.Read(rec.Blob)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (s *bphsStore) SyncPartitions(indices map[int]struct{}) error {
	if s == nil || len(indices) == 0 {
		return nil
	}
	for idx := range indices {
		if idx < 0 || idx >= len(s.parts) {
			return fmt.Errorf("invalid BPHS partition id %d", idx)
		}
		part := s.parts[idx]
		part.mu.Lock()
		if err := part.vlog.Sync(); err != nil {
			part.mu.Unlock()
			return fmt.Errorf("sync partition %d vlog: %w", idx, err)
		}
		if err := part.records.Sync(); err != nil {
			part.mu.Unlock()
			return fmt.Errorf("sync partition %d records: %w", idx, err)
		}
		part.mu.Unlock()
	}
	return nil
}

func (s *bphsStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for _, part := range s.parts {
		part.mu.Lock()
		if err := part.records.Close(); err != nil && first == nil {
			first = err
		}
		if err := part.vlog.Close(); err != nil && first == nil {
			first = err
		}
		for _, l0 := range part.l0 {
			l0.closePool()
		}
		part.mu.Unlock()
	}
	s.parts = nil
	return first
}

// enableOrUpdateBPHS 在首轮分区 GC 完成后打开本地 BPHS 状态；后续全局 GC
// 仍复用同一组固定分区时，只更新 base 指针。调用方持有 kvs.mu。
func (kvs *KVServer) enableOrUpdateBPHS(base *PartitionSet) error {
	if !kvs.bphsEnabled || base == nil || base.Len() == 0 {
		return nil
	}
	if kvs.bphs != nil {
		return kvs.bphs.SetBase(base)
	}
	dir := kvs.bphsDir
	if dir == "" {
		dir = filepath.Join(kvs.dataDir, "data", "bphs")
	}
	store, err := openBPHSStore(kvs, base, dir, kvs.bphsMemtableBytes, kvs.inlineCacheBytes, 0)
	if err != nil {
		return err
	}
	kvs.bphs = store
	kvs.bphsDir = dir
	return nil
}

type bphsSnapshotPartition struct {
	Index        int    `json:"index"`
	Generation   uint64 `json:"generation"`
	RecordsFile  string `json:"records_file"`
	RecordsBytes int64  `json:"records_bytes"`
	VLogFile     string `json:"vlog_file"`
	VLogBytes    int64  `json:"vlog_bytes"`
}

// SnapshotCut 返回一份可随 Raft 快照发送的分区本地文件切点。记录日志包含所有
// 已写入 BPHS 的版本；接收端按快照 AppliedIndex 过滤更高版本。
func (s *bphsStore) SnapshotCut() ([]bphsSnapshotPartition, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	parts := append([]*bphsPartition(nil), s.parts...)
	s.mu.RUnlock()
	out := make([]bphsSnapshotPartition, 0, len(parts))
	for _, part := range parts {
		part.mu.Lock()
		meta := bphsSnapshotPartition{
			Index:       part.index,
			Generation:  part.gen,
			RecordsFile: filepath.Base(part.records.Path()),
			VLogFile:    filepath.Base(part.vlog.Path()),
		}
		if err := part.vlog.Sync(); err != nil {
			part.mu.Unlock()
			return nil, err
		}
		if err := part.records.Sync(); err != nil {
			part.mu.Unlock()
			return nil, err
		}
		meta.RecordsBytes = part.records.Size()
		meta.VLogBytes = part.vlog.Size()
		part.mu.Unlock()
		out = append(out, meta)
	}
	return out, nil
}
