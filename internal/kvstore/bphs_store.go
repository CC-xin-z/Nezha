package kvstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"github.com/linxGnu/grocksdb"
)

const (
	bphsL0CompactionTrigger = 4
	// Avoid rewriting a growing base after every L0 merge. Keep several
	// immutable runs during writes and collapse them in a dedicated read-ready
	// pass once the workload becomes quiescent.
	bphsBaseCompactionL0Rounds          = 8
	bphsDefaultPartitionSyncConcurrency = 8
	bphsDefaultRecordSyncConcurrency    = 8
)

type bphsPartitionManifest struct {
	Generation   uint64 `json:"generation"`
	BaseFile     string `json:"base_file,omitempty"`
	BaseSize     int64  `json:"base_size,omitempty"`
	LogicalBytes int64  `json:"logical_bytes,omitempty"`
	MaxVersion   uint32 `json:"max_version"`
	Lo           string `json:"lo,omitempty"`
	Hi           string `json:"hi,omitempty"`
	Entries      int    `json:"entries"`
}

func bphsPartitionManifestPath(dir string, partition int) string {
	return filepath.Join(dir, fmt.Sprintf("p%06d.manifest.json", partition))
}

func loadBPHSPartitionManifest(dir string, partition int) (bphsPartitionManifest, bool, error) {
	var m bphsPartitionManifest
	data, err := os.ReadFile(bphsPartitionManifestPath(dir, partition))
	if os.IsNotExist(err) {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, false, err
	}
	return m, true, nil
}

// bphsPartition 是固定 Key Range 上的本地写入层。
//
// base 是全局分区 GC 产出的不可变有序文件；memtable 和 records 保存 BPHS 启用后的
// 新版本。MemTable 达到预算后刷成 L0，l0 数量达到阈值时只在本分区内合并；
// records 仍是持久化 WAL，重启时只重放比最新 L0 更新的版本。
type bphsPartition struct {
	mu           sync.RWMutex
	index        int
	base         *SortedFileIndex
	baseLocal    bool
	mem          *partitionMemtable
	records      *partitionRecordLog
	vlog         *partitionVLog
	l0           []*SortedFileIndex // 从旧到新
	gen          uint64
	logicalBytes int64
	ownershipLo  string
	ownershipHi  string
	recordsDirty bool
	vlogDirty    bool
	compacting   bool
	l0Merges     int
	durableIndex atomic.Uint32
	// pendingDurableVersion is advanced after a record (and, when needed, its
	// VLog value) is fsynced. A background sync therefore advances the BPHS
	// durable frontier without waiting for a MemTable flush.
	pendingDurableVersion atomic.Uint32
}

func (p *bphsPartition) ownsKey(key string) bool {
	if p == nil {
		return false
	}
	return (p.ownershipLo == "" || key >= p.ownershipLo) &&
		(p.ownershipHi == "" || key < p.ownershipHi)
}

// bphsStore 是 BPHS 的节点本地状态。base 可以在全局 GC 完成后整体切换，但静态原型
// 要求分区数量和 Ownership Range 保持不变；动态 Split 不在这一版范围内。
type bphsStore struct {
	mu                        sync.RWMutex
	kvs                       *KVServer
	base                      *PartitionSet
	basePtr                   atomic.Pointer[PartitionSet]
	parts                     []*bphsPartition
	dir                       string
	memtableBytes             int64
	syncConcurrency           int
	wal                       *bphsSharedWAL
	walPending                []bphsSharedWALRecord
	walMaxBytes               int64
	durablePath               string
	durableMu                 sync.Mutex
	durableIndex              uint32
	raftLogWAL                bool
	recordSyncConcurrency     int
	recordSyncMu              sync.Mutex
	recordSyncPending         map[int]uint32
	recordSyncSignal          chan struct{}
	recordSyncStop            chan struct{}
	recordSyncWG              sync.WaitGroup
	durableErrMu              sync.Mutex
	durableErr                error
	inline                    *InlineCache
	partitionGCThresholdBytes int64
	legacyCovered             bool
	baseRefMu                 sync.Mutex
	baseRefs                  map[string]int

	compactCh   chan int
	compactStop chan struct{}
	compactWG   sync.WaitGroup
	compactJobs sync.WaitGroup
	closeOnce   sync.Once
	errMu       sync.Mutex
	compactErr  error
	compactFail atomic.Bool

	// Test-only barriers for proving that L0 merge construction runs outside
	// part.mu. Production stores leave both nil.
	l0MergeBuildStart   chan struct{}
	l0MergeBuildRelease chan struct{}
}

func openBPHSStore(kvs *KVServer, base *PartitionSet, dir string, memtableBytes, inlineCacheBytes int64, maxVersion uint32) (*bphsStore, error) {
	if kvs == nil || base == nil || base.Len() == 0 {
		return nil, fmt.Errorf("BPHS requires a non-empty partition set")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var inline *InlineCache
	for _, basePart := range base.parts {
		if basePart.InlineValues != nil {
			inline = basePart.InlineValues
			break
		}
	}
	if inline == nil && inlineCacheBytes > 0 {
		inline = NewInlineCache(inlineCacheBytes)
	}
	syncConcurrency := kvs.bphsSyncConcurrency
	if syncConcurrency <= 0 {
		syncConcurrency = bphsDefaultPartitionSyncConcurrency
	}
	walMaxBytes := kvs.bphsWALBytes
	if walMaxBytes <= 0 {
		walMaxBytes = 256 << 20
	}
	recordSyncConcurrency := kvs.bphsRecordSyncConcurrency
	if recordSyncConcurrency <= 0 {
		recordSyncConcurrency = bphsDefaultRecordSyncConcurrency
	}
	wal, err := openBPHSSharedWAL(filepath.Join(dir, bphsSharedWALName))
	if err != nil {
		return nil, fmt.Errorf("open BPHS shared WAL: %w", err)
	}
	s := &bphsStore{
		kvs:                       kvs,
		base:                      base,
		dir:                       dir,
		memtableBytes:             memtableBytes,
		syncConcurrency:           syncConcurrency,
		wal:                       wal,
		walMaxBytes:               walMaxBytes,
		durablePath:               filepath.Join(dir, "durable_index.json"),
		raftLogWAL:                kvs.bphsRaftLogWAL,
		recordSyncConcurrency:     recordSyncConcurrency,
		recordSyncPending:         make(map[int]uint32),
		recordSyncSignal:          make(chan struct{}, 1),
		recordSyncStop:            make(chan struct{}),
		inline:                    inline,
		partitionGCThresholdBytes: kvs.bphsPartitionGCThresholdBytes,
		compactCh:                 make(chan int, base.Len()),
		compactStop:               make(chan struct{}),
	}
	s.basePtr.Store(base)
	s.parts = make([]*bphsPartition, base.Len())
	durableState, hasDurable, err := readBPHSDurableState(dir)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("read BPHS durable index: %w", err)
	}
	if hasDurable && len(durableState.Partitions) != base.Len() {
		wal.Close()
		return nil, fmt.Errorf("BPHS durable index has %d partitions, want %d",
			len(durableState.Partitions), base.Len())
	}
	s.compactWG.Add(1)
	go s.compactLoop()
	coveredVersions := make([]uint32, len(base.parts))
	for pos, basePart := range base.parts {
		partitionID := base.partitionID(pos)
		if partitionID < 0 || partitionID >= len(s.parts) || s.parts[partitionID] != nil {
			s.Close()
			return nil, fmt.Errorf("invalid or duplicate BPHS partition id %d at sorted position %d", partitionID, pos)
		}
		meta, hasMeta, err := loadBPHSPartitionManifest(dir, partitionID)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("load partition %d BPHS manifest: %w", partitionID, err)
		}
		localBase := basePart
		localBaseLoaded := false
		maxCoveredVersion := uint32(0)
		gen := uint64(1)
		logicalBytes := basePart.FileSize
		if hasMeta {
			gen = meta.Generation
			maxCoveredVersion = meta.MaxVersion
			if meta.LogicalBytes > 0 {
				logicalBytes = meta.LogicalBytes
			}
			if meta.BaseFile != "" {
				localBase, err = loadBPHSBaseFile(kvs, dir, meta, inlineCacheBytes)
				if err != nil {
					s.Close()
					return nil, fmt.Errorf("load partition %d BPHS base: %w", partitionID, err)
				}
				localBase.InlineValues = inline
				localBaseLoaded = true
			}
		}
		if base.metadataOnly && !localBaseLoaded {
			s.Close()
			return nil, fmt.Errorf(
				"partition %d uses metadata-only recovery but has no local BPHS base",
				partitionID,
			)
		}
		records, err := openPartitionRecordLog(filepath.Join(dir, fmt.Sprintf("p%06d.records", partitionID)))
		if err != nil {
			if localBaseLoaded {
				localBase.closePool()
			}
			s.Close()
			return nil, fmt.Errorf("open partition %d record log: %w", partitionID, err)
		}
		vlog, err := openPartitionVLog(filepath.Join(dir, fmt.Sprintf("p%06d.vlog", partitionID)))
		if err != nil {
			records.Close()
			if localBaseLoaded {
				localBase.closePool()
			}
			s.Close()
			return nil, fmt.Errorf("open partition %d vlog: %w", partitionID, err)
		}
		part := &bphsPartition{
			index:        partitionID,
			base:         localBase,
			baseLocal:    localBaseLoaded,
			mem:          newPartitionMemtable(memtableBytes),
			records:      records,
			vlog:         vlog,
			gen:          gen,
			logicalBytes: logicalBytes,
			ownershipLo:  basePart.OwnershipLo,
			ownershipHi:  basePart.OwnershipHi,
		}
		l0, err := loadPartitionL0Files(kvs, dir, partitionID, inlineCacheBytes)
		if err != nil {
			records.Close()
			vlog.Close()
			if localBaseLoaded {
				localBase.closePool()
			}
			s.Close()
			return nil, fmt.Errorf("load partition %d L0: %w", partitionID, err)
		}
		for _, loaded := range l0 {
			if localBaseLoaded && loaded.maxVersion <= maxCoveredVersion {
				loaded.file.closePool()
				for _, p := range []string{loaded.file.FilePath, sparseIndexPath(loaded.file.FilePath)} {
					if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
						fmt.Printf("[RECOVER] 删除已被 BPHS base 覆盖的 L0 %s 失败: %v\n", p, err)
					}
				}
				continue
			}
			loaded.file.InlineValues = inline
			part.l0 = append(part.l0, loaded.file)
			if loaded.maxVersion > maxCoveredVersion {
				maxCoveredVersion = loaded.maxVersion
			}
		}
		replayed, err := records.ScanAll()
		if err != nil {
			if s.wal.Size() == 0 {
				records.Close()
				vlog.Close()
				if localBaseLoaded {
					localBase.closePool()
				}
				s.Close()
				return nil, fmt.Errorf("replay partition %d records: %w", partitionID, err)
			}
			// records are synced asynchronously in the fast path. A crash can
			// leave a torn tail there while the shared WAL holds the complete
			// durable batch; replay every complete prefix record and let the WAL
			// cover the rest.
			fmt.Printf("[RECOVER] partition %d record tail ignored; shared WAL will replay it: %v\n", partitionID, err)
		}
		for _, rec := range replayed {
			if rec.Version <= maxCoveredVersion {
				continue
			}
			if maxVersion > 0 && rec.Version > maxVersion {
				continue
			}
			part.mem.Put(rec)
		}
		durableVersion := maxCoveredVersion
		if hasDurable {
			durableVersion = durableState.Partitions[partitionID]
		}
		part.durableIndex.Store(durableVersion)
		coveredVersions[partitionID] = maxCoveredVersion
		s.parts[partitionID] = part
	}
	for i, part := range s.parts {
		if part == nil {
			s.Close()
			return nil, fmt.Errorf("missing BPHS partition %d after recovery", i)
		}
	}
	s.baseRefMu.Lock()
	s.baseRefs = make(map[string]int)
	for _, part := range s.parts {
		if part.baseLocal && part.base != nil {
			s.baseRefs[part.base.FilePath]++
		}
	}
	s.baseRefMu.Unlock()
	if hasDurable {
		s.durableIndex = durableState.DurableIndex
	}
	if !hasDurable && maxVersion > 0 {
		state := bphsDurableState{
			Partitions:   make([]uint32, len(s.parts)),
			DurableIndex: maxVersion,
		}
		for i, part := range s.parts {
			part.durableIndex.Store(maxVersion)
			state.Partitions[i] = maxVersion
		}
		path := s.durablePath
		if path == "" {
			path = filepath.Join(s.dir, bphsDurableIndexName)
		}
		if err := writeBPHSDurableState(path, state); err != nil {
			s.Close()
			return nil, fmt.Errorf("write snapshot BPHS durable index: %w", err)
		}
		s.durableIndex = maxVersion
	}
	walRecords, err := s.wal.ScanAll()
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("replay BPHS shared WAL: %w", err)
	}
	for _, item := range walRecords {
		if item.Partition < 0 || item.Partition >= len(s.parts) {
			s.Close()
			return nil, fmt.Errorf("BPHS shared WAL references invalid partition %d", item.Partition)
		}
		rec := item.Record
		if rec.Version <= coveredVersions[item.Partition] {
			continue
		}
		if maxVersion > 0 && rec.Version > maxVersion {
			continue
		}
		s.parts[item.Partition].mem.Put(rec)
	}
	s.recordSyncWG.Add(1)
	go s.recordSyncLoop()
	if err := cleanupUnreferencedBPHSFiles(s); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

var bphsPartitionFileName = regexp.MustCompile(`^p[0-9]{6}(\.(records|vlog)|_(l0|base)_[^/]+\.sst)$`)
var bphsPartitionManifestName = regexp.MustCompile(`^p([0-9]{6})\.manifest\.json$`)

func cleanupUnreferencedBPHSFiles(s *bphsStore) error {
	keep := make(map[string]struct{})
	for _, part := range s.parts {
		keep[filepath.Base(part.records.Path())] = struct{}{}
		keep[filepath.Base(part.vlog.Path())] = struct{}{}
		if part.baseLocal && part.base != nil {
			keep[filepath.Base(part.base.FilePath)] = struct{}{}
		}
		for _, l0 := range part.l0 {
			keep[filepath.Base(l0.FilePath)] = struct{}{}
		}
	}

	removeIfUnreferenced := func(path string) error {
		name := filepath.Base(path)
		if !bphsPartitionFileName.MatchString(name) {
			return nil
		}
		if _, ok := keep[name]; ok {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Remove(sparseIndexPath(path)); err != nil && !os.IsNotExist(err) {
			return err
		}
		fmt.Printf("[RECOVER] removed stale BPHS file %s\n", path)
		return nil
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if match := bphsPartitionManifestName.FindStringSubmatch(name); match != nil {
			idx, err := strconv.Atoi(match[1])
			if err != nil {
				return err
			}
			if idx < len(s.parts) {
				continue
			}
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
				return err
			}
			fmt.Printf("[RECOVER] removed stale BPHS manifest %s\n", name)
			continue
		}
		if !strings.HasSuffix(name, ".sst") &&
			!strings.HasSuffix(name, ".records") &&
			!strings.HasSuffix(name, ".vlog") {
			continue
		}
		if err := removeIfUnreferenced(filepath.Join(s.dir, name)); err != nil {
			return fmt.Errorf("cleanup BPHS file %s: %w", name, err)
		}
	}
	return nil
}

func (s *bphsStore) compactLoop() {
	defer s.compactWG.Done()
	for {
		select {
		case <-s.compactStop:
			return
		case idx := <-s.compactCh:
			part := s.parts[idx]
			part.mu.RLock()
			l0Only := part.vlog.Size() == 0 && len(part.l0) >= bphsL0CompactionTrigger
			part.mu.RUnlock()
			t0 := time.Now()
			var err error
			if l0Only {
				err = s.compactL0Snapshot(part)
				recordBPHSL0Merge(time.Since(t0))
			} else {
				err = s.runPartitionCompaction(part)
				recordBPHSBaseCompaction(time.Since(t0))
			}
			part.mu.RLock()
			generation := part.gen
			part.mu.RUnlock()
			if l0Only {
				fmt.Printf("[BPHS-L0-ASYNC] partition=%d elapsed=%v generation=%d err=%v\n",
					idx, time.Since(t0), generation, err)
			} else {
				fmt.Printf("[BPHS-COMPACTION] partition=%d elapsed=%v generation=%d err=%v\n",
					idx, time.Since(t0), generation, err)
			}
			if err != nil {
				s.errMu.Lock()
				if s.compactErr == nil {
					s.compactErr = err
					s.compactFail.Store(true)
				}
				s.errMu.Unlock()
				fmt.Printf("BPHS background compaction partition %d failed: %v\n", idx, err)
			}
			s.compactJobs.Done()
		}
	}
}

// runPartitionCompaction clears part.compacting on every exit path. Inline
// partitions use a snapshot built outside part.mu; VLog partitions retain the
// locked path because compaction currently resets the VLog generation.
func (s *bphsStore) runPartitionCompaction(part *bphsPartition) error {
	part.mu.Lock()
	if !part.compacting {
		part.mu.Unlock()
		return nil
	}
	useLockedPath := part.vlog.Size() > 0
	part.mu.Unlock()

	if useLockedPath {
		part.mu.Lock()
		var err error
		if part.compacting {
			err = s.compactPartitionLocked(part)
		}
		part.compacting = false
		if err == nil && s.shouldCompactPartitionLocked(part) {
			s.scheduleCompactionLocked(part)
		}
		part.mu.Unlock()
		return err
	}
	return s.compactPartitionSnapshot(part)
}

func (s *bphsStore) scheduleCompactionLocked(part *bphsPartition) {
	if s == nil || part == nil || part.compacting {
		return
	}
	select {
	case <-s.compactStop:
		return
	default:
	}
	part.compacting = true
	s.compactJobs.Add(1)
	select {
	case s.compactCh <- part.index:
	default:
		part.compacting = false
		s.compactJobs.Done()
	}
}

func (s *bphsStore) compactionError() error {
	if s == nil {
		return nil
	}
	if !s.compactFail.Load() {
		return nil
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.compactErr
}

// WaitForCompactions is used by tests and maintenance tools that need a stable
// on-disk view. Normal request handling never waits for background compaction.
func (s *bphsStore) WaitForCompactions() error {
	if s == nil {
		return nil
	}
	s.compactJobs.Wait()
	return s.compactionError()
}

// CompactForRead collapses the current local MemTable and L0 runs into the
// partition base after writes have gone idle. Inline partitions build the new
// base from a snapshot without holding part.mu for the file rewrite.
func (s *bphsStore) CompactForRead() (int, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.RLock()
	parts := append([]*bphsPartition(nil), s.parts...)
	s.mu.RUnlock()

	compacted := 0
	for _, part := range parts {
		part.mu.Lock()
		if part.mem.Len() > 0 {
			if err := s.flushPartitionLocked(part); err != nil {
				part.mu.Unlock()
				return compacted, err
			}
		}
		// A flush may have scheduled the normal background compaction. Let
		// that worker finish instead of compacting the same partition twice.
		if part.compacting ||
			(part.baseLocal && len(part.l0) == 0 && part.mem.Len() == 0) {
			part.mu.Unlock()
			continue
		}
		if part.vlog.Size() == 0 {
			part.compacting = true
			part.mu.Unlock()
			if err := s.compactPartitionSnapshot(part); err != nil {
				return compacted, err
			}
			compacted++
			continue
		}
		if err := s.compactPartitionLocked(part); err != nil {
			part.mu.Unlock()
			return compacted, err
		}
		compacted++
		part.mu.Unlock()
	}
	if err := s.WaitForCompactions(); err != nil {
		return compacted, err
	}
	return compacted, s.compactionError()
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
	for _, part := range s.parts {
		part.mu.Lock()
	}
	defer func() {
		for _, part := range s.parts {
			part.mu.Unlock()
		}
	}()
	for i, part := range s.parts {
		if part.baseLocal {
			return fmt.Errorf("BPHS partition %d has a compacted local base; global base replacement is not supported", i)
		}
	}
	s.base = base
	s.basePtr.Store(base)
	for pos := range base.parts {
		s.parts[base.partitionID(pos)].base = base.parts[pos]
	}
	return nil
}

// UseOwnershipMetadata replaces the activation-time partition object with a
// metadata-only routing skeleton after every local BPHS base is durable.
func (s *bphsStore) UseOwnershipMetadata(base *PartitionSet) {
	if s == nil || base == nil {
		return
	}
	s.mu.Lock()
	s.base = base
	s.basePtr.Store(base)
	s.mu.Unlock()
}

func (s *bphsStore) retainBase(path string) {
	if s == nil || path == "" {
		return
	}
	s.baseRefMu.Lock()
	if s.baseRefs == nil {
		s.baseRefs = make(map[string]int)
	}
	s.baseRefs[path]++
	s.baseRefMu.Unlock()
}

func (s *bphsStore) releaseBase(path string) bool {
	if s == nil || path == "" {
		return false
	}
	s.baseRefMu.Lock()
	defer s.baseRefMu.Unlock()
	if s.baseRefs == nil {
		return true
	}
	if s.baseRefs[path] > 1 {
		s.baseRefs[path]--
		return false
	}
	delete(s.baseRefs, path)
	return true
}

// AllPartitionsMaterialized reports whether every BPHS partition has a local
// base and no compaction is in flight. Only then can the global partition files
// from activation be retired without making recovery depend on them.
func (s *bphsStore) AllPartitionsMaterialized() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	parts := append([]*bphsPartition(nil), s.parts...)
	s.mu.RUnlock()
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		part.mu.RLock()
		ok := part.baseLocal && part.base != nil && !part.compacting
		path, size := "", int64(0)
		if part.base != nil {
			path = part.base.FilePath
			size = part.base.FileSize
		}
		index := part.index
		part.mu.RUnlock()
		if !ok {
			return false
		}
		if st, err := os.Stat(path); err != nil || st.IsDir() || st.Size() != size {
			return false
		}
		meta, hasMeta, err := loadBPHSPartitionManifest(s.dir, index)
		if err != nil || !hasMeta || meta.BaseFile == "" ||
			meta.BaseSize != size || filepath.Base(path) != meta.BaseFile {
			return false
		}
	}
	return true
}

func (s *bphsStore) route(paddedKey string) (*bphsPartition, error) {
	if s == nil {
		return nil, fmt.Errorf("BPHS store is nil")
	}
	base := s.basePtr.Load()
	_, idx, ok := base.findOwned(paddedKey)
	if !ok || idx < 0 || idx >= len(s.parts) {
		return nil, fmt.Errorf("no BPHS partition owns key %q", paddedKey)
	}
	return s.parts[base.partitionID(idx)], nil
}

func (s *bphsStore) Dir() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dir
}

// CoversLegacy reports whether the activation-time tail was fully imported
// into BPHS. From that point on, BPHS is the authoritative data path and SCAN
// no longer needs to merge the frozen global partition set on every query.
func (s *bphsStore) CoversLegacy() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.legacyCovered
}

func (s *bphsStore) markLegacyCovered() {
	s.mu.Lock()
	s.legacyCovered = true
	s.mu.Unlock()
}

// Put 把一条已提交的 Raft PUT 写入目标分区。返回分区 id，调用方应把它加入 dirty
// 集合并在这批 applied index 落库之前调用 SyncPartitions。
func (s *bphsStore) Put(key, value string, version uint32) (int, error) {
	if err := s.compactionError(); err != nil {
		return -1, fmt.Errorf("background partition compaction: %w", err)
	}
	part, err := s.route(key)
	if err != nil {
		return -1, err
	}

	lockStart := time.Now()
	part.mu.Lock()
	defer part.mu.Unlock()
	lockWait := time.Since(lockStart)

	rec := bphsRecord{Key: key, Version: version}
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
		part.vlogDirty = true
	}
	offset, err := part.records.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("append partition %d record: %w", part.index, err)
	}
	for {
		old := part.pendingDurableVersion.Load()
		if version <= old || part.pendingDurableVersion.CompareAndSwap(old, version) {
			break
		}
	}
	part.recordsDirty = true
	rec.LogOffset = offset
	if !s.raftLogWAL {
		s.walPending = append(s.walPending, bphsSharedWALRecord{
			Partition: part.index,
			Record:    rec,
		})
	}
	part.mem.Put(rec)
	part.logicalBytes += bphsRecordBytes(rec)
	if part.mem.Full() {
		if err := s.flushPartitionLocked(part); err != nil {
			return -1, err
		}
	} else if s.shouldCompactPartitionLocked(part) {
		s.scheduleCompactionLocked(part)
	}
	recordBPHSPut(len(value), lockWait)
	return part.index, nil
}

func (s *bphsStore) flushPartitionLocked(part *bphsPartition) error {
	if part == nil || part.mem.Len() == 0 {
		return nil
	}
	name := fmt.Sprintf("p%06d_l0_%020d.sst", part.index, time.Now().UnixNano())
	path := filepath.Join(s.dir, name)
	flushedVersion := part.mem.MaxVersion()
	memBytes := part.mem.Bytes()
	flushStart := time.Now()
	l0, err := s.kvs.flushPartitionMemtable(part.mem, path, 0)
	recordBPHSMemtableFlush(time.Since(flushStart), memBytes)
	if err != nil {
		return fmt.Errorf("flush partition %d memtable: %w", part.index, err)
	}
	if l0 == nil {
		part.mem.Reset()
		return nil
	}
	l0.InlineValues = s.inline
	part.l0 = append(part.l0, l0)
	part.mem.Reset()
	if err := s.advancePartitionDurable(part, flushedVersion); err != nil {
		return fmt.Errorf("advance partition %d durable index: %w", part.index, err)
	}
	// The L0 file was fsynced by flushPartitionMemtable and contains the latest
	// value for every key in this memtable. Older records are no longer needed
	// for recovery, so do not let this per-partition WAL grow forever.
	if err := part.records.Reset(); err != nil {
		return fmt.Errorf("reset partition %d record log after L0: %w", part.index, err)
	}
	part.recordsDirty = false
	if s.shouldMergeBPHSL0Locked(part) {
		// L0 merge is snapshot-based and runs on the compaction worker. The PUT
		// path only freezes the prefix, so hot-partition writes do not wait for
		// the merge I/O.
		s.scheduleCompactionLocked(part)
	}
	if s.shouldCompactPartitionLocked(part) {
		s.scheduleCompactionLocked(part)
	}
	return nil
}

func (s *bphsStore) shouldMergeBPHSL0Locked(part *bphsPartition) bool {
	return s != nil && part != nil && !part.compacting &&
		part.vlog.Size() == 0 && len(part.l0) >= bphsL0CompactionTrigger
}

func (s *bphsStore) shouldCompactPartitionLocked(part *bphsPartition) bool {
	if part == nil {
		return false
	}
	// Inline records are bounded by partition-local L0 compaction. Merge one
	// completed L0 run into the local Base so quiescent reads see a single run,
	// without rewriting the Base after every small memtable flush.
	// VLog growth still triggers earlier because it is reclaimable Blob space.
	if part.vlog.Size() == 0 {
		return part.l0Merges >= bphsBaseCompactionL0Rounds
	}
	delta := part.vlog.Size()
	threshold := s.partitionGCThresholdBytes
	if threshold <= 0 {
		threshold = int64(16 << 20)
	}
	if part.base != nil {
		if want := part.base.FileSize / 4; want > threshold {
			threshold = want
		}
	}
	return delta >= threshold
}

type bphsCompactionSnapshot struct {
	base       *SortedFileIndex
	l0         []*SortedFileIndex
	mem        []bphsRecord
	generation uint64
}

func (s *bphsStore) captureCompactionSnapshot(
	part *bphsPartition,
) (bphsCompactionSnapshot, bool) {
	part.mu.Lock()
	defer part.mu.Unlock()
	if !part.compacting {
		return bphsCompactionSnapshot{}, false
	}
	snapshot := bphsCompactionSnapshot{
		base:       part.base,
		l0:         append([]*SortedFileIndex(nil), part.l0...),
		generation: part.gen,
	}
	if len(part.mem.records) > 0 {
		snapshot.mem = append([]bphsRecord(nil), part.mem.Sorted()...)
	}
	return snapshot, true
}

func sameBHPSL0Prefix(current, prefix []*SortedFileIndex) bool {
	if len(current) < len(prefix) {
		return false
	}
	for i := range prefix {
		if current[i] != prefix[i] {
			return false
		}
	}
	return true
}

func removeBPHSSortedFile(index *SortedFileIndex) {
	if index == nil {
		return
	}
	index.closePool()
	os.Remove(index.FilePath)
	os.Remove(sparseIndexPath(index.FilePath))
}

func (s *bphsStore) buildCompactionBase(
	part *bphsPartition,
	snapshot bphsCompactionSnapshot,
) (*SortedFileIndex, bphsPartitionManifest, error) {
	name := fmt.Sprintf("p%06d_base_%020d.sst", part.index, time.Now().UnixNano())
	path := filepath.Join(s.dir, name)
	newBase, cutVersion, err := s.writeStreamingCompactionBase(part, snapshot, path)
	if err != nil {
		return nil, bphsPartitionManifest{}, err
	}
	if newBase == nil {
		return nil, bphsPartitionManifest{}, nil
	}
	newBase.InlineValues = s.inline
	meta := bphsPartitionManifest{
		Generation:   snapshot.generation,
		BaseFile:     filepath.Base(path),
		BaseSize:     newBase.FileSize,
		LogicalBytes: newBase.FileSize,
		MaxVersion:   cutVersion,
		Lo:           newBase.Lo,
		Hi:           newBase.Hi,
		Entries:      newBase.Entries,
	}
	return newBase, meta, nil
}

func (s *bphsStore) installCompactionSnapshot(
	part *bphsPartition,
	snapshot bphsCompactionSnapshot,
	newBase *SortedFileIndex,
	meta bphsPartitionManifest,
) error {
	part.mu.Lock()
	defer part.mu.Unlock()
	s.retainBase(newBase.FilePath)

	cleanupNewBase := func() {
		if s.releaseBase(newBase.FilePath) {
			removeBPHSSortedFile(newBase)
		}
	}
	if !part.compacting ||
		part.base != snapshot.base ||
		part.gen != snapshot.generation ||
		!sameBHPSL0Prefix(part.l0, snapshot.l0) {
		cleanupNewBase()
		return fmt.Errorf("BPHS partition %d compaction snapshot changed before install", part.index)
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		cleanupNewBase()
		return err
	}
	if err := raft.WriteFileAtomic(bphsPartitionManifestPath(s.dir, part.index), data); err != nil {
		cleanupNewBase()
		return err
	}

	oldBase, oldLocal := part.base, part.baseLocal
	oldL0 := append([]*SortedFileIndex(nil), snapshot.l0...)
	part.base = newBase
	part.baseLocal = true
	part.logicalBytes = newBase.FileSize
	part.l0 = append([]*SortedFileIndex(nil), part.l0[len(snapshot.l0):]...)
	part.l0Merges = 0
	part.mem.PruneThroughVersion(meta.MaxVersion)

	if err := s.advancePartitionDurable(part, meta.MaxVersion); err != nil {
		return err
	}
	for _, run := range oldL0 {
		removeBPHSSortedFile(run)
	}
	if oldLocal && oldBase != nil && oldBase.FilePath != newBase.FilePath {
		if s.releaseBase(oldBase.FilePath) {
			removeBPHSSortedFile(oldBase)
		}
	}
	return nil
}

// compactPartitionSnapshot materializes an immutable Base/L0/MemTable view
// without holding part.mu during the expensive merge. New writes continue in
// the MemTable or produce newer L0 runs; only the frozen prefix is installed
// into the new Base at commit time.
func (s *bphsStore) compactPartitionSnapshot(part *bphsPartition) (err error) {
	defer func() {
		part.mu.Lock()
		part.compacting = false
		if err == nil && s.shouldCompactPartitionLocked(part) {
			s.scheduleCompactionLocked(part)
		}
		part.mu.Unlock()
	}()

	snapshot, ok := s.captureCompactionSnapshot(part)
	if !ok {
		return nil
	}
	if part.baseLocal && len(snapshot.l0) == 0 && len(snapshot.mem) == 0 {
		return nil
	}
	newBase, meta, err := s.buildCompactionBase(part, snapshot)
	if err != nil || newBase == nil {
		return err
	}
	return s.installCompactionSnapshot(part, snapshot, newBase, meta)
}

func (s *bphsStore) compactPartitionLocked(part *bphsPartition) error {
	if part == nil || (len(part.l0) == 0 && part.mem.Len() == 0) {
		return nil
	}
	if part.vlogDirty {
		if err := part.vlog.Sync(); err != nil {
			return err
		}
		part.vlogDirty = false
	}
	if part.recordsDirty {
		if err := part.records.Sync(); err != nil {
			return err
		}
		part.recordsDirty = false
	}

	merged := make(map[string]bphsRecord)
	putLatest := func(rec bphsRecord) {
		old, ok := merged[rec.Key]
		if !ok || rec.Version >= old.Version {
			merged[rec.Key] = rec
		}
	}

	if part.base != nil {
		records, err := readRawSortedFile(part.base.FilePath)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if part.ownsKey(rec.Key) {
				putLatest(rec)
			}
		}
	}
	for _, run := range part.l0 {
		records, err := readBPHSSortedFile(run.FilePath)
		if err != nil {
			return err
		}
		for _, rec := range records {
			value, err := s.recordValue(part, rec)
			if err != nil {
				return err
			}
			putLatest(bphsRecord{
				Key: rec.Key, Version: rec.Version, Kind: bphsInlineRecord, Value: value,
			})
		}
	}
	for _, rec := range part.mem.Sorted() {
		value, err := s.recordValue(part, rec)
		if err != nil {
			return err
		}
		putLatest(bphsRecord{
			Key: rec.Key, Version: rec.Version, Kind: bphsInlineRecord, Value: value,
		})
	}

	records := make([]bphsRecord, 0, len(merged))
	var cutVersion uint32
	for _, rec := range merged {
		if rec.Version > cutVersion {
			cutVersion = rec.Version
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })

	name := fmt.Sprintf("p%06d_base_%020d.sst", part.index, time.Now().UnixNano())
	path := filepath.Join(s.dir, name)
	newBase, err := s.kvs.flushBPHSBaseFile(records, path, 0)
	if err != nil {
		return err
	}
	if newBase == nil {
		return nil
	}
	newBase.InlineValues = s.inline
	s.retainBase(newBase.FilePath)

	nextGen := part.gen + 1
	meta := bphsPartitionManifest{
		Generation:   nextGen,
		BaseFile:     filepath.Base(path),
		BaseSize:     newBase.FileSize,
		LogicalBytes: newBase.FileSize,
		MaxVersion:   cutVersion,
		Lo:           newBase.Lo,
		Hi:           newBase.Hi,
		Entries:      newBase.Entries,
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		if s.releaseBase(newBase.FilePath) {
			removeBPHSSortedFile(newBase)
		}
		return err
	}
	if err := raft.WriteFileAtomic(bphsPartitionManifestPath(s.dir, part.index), data); err != nil {
		if s.releaseBase(newBase.FilePath) {
			removeBPHSSortedFile(newBase)
		}
		return err
	}

	oldBase, oldLocal := part.base, part.baseLocal
	oldL0 := part.l0
	part.base = newBase
	part.baseLocal = true
	part.gen = nextGen
	part.logicalBytes = newBase.FileSize
	part.l0 = nil
	part.l0Merges = 0
	part.mem.Reset()
	if err := s.advancePartitionDurable(part, cutVersion); err != nil {
		return err
	}

	for _, run := range oldL0 {
		run.closePool()
		os.Remove(run.FilePath)
		os.Remove(sparseIndexPath(run.FilePath))
	}

	if err := part.records.Close(); err != nil {
		return err
	}
	recordPath := part.records.Path()
	if err := os.Remove(recordPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	part.records, err = openPartitionRecordLog(recordPath)
	if err != nil {
		return err
	}
	part.recordsDirty = false

	if err := part.vlog.Close(); err != nil {
		return err
	}
	vlogPath := part.vlog.Path()
	if err := os.Remove(vlogPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	part.vlog, err = openPartitionVLog(vlogPath)
	if err != nil {
		return err
	}
	part.vlogDirty = false

	if oldLocal && oldBase != nil && oldBase.FilePath != newBase.FilePath {
		if s.releaseBase(oldBase.FilePath) {
			removeBPHSSortedFile(oldBase)
		}
	}
	return nil
}

// compactL0Snapshot merges a frozen immutable L0 prefix outside part.mu.
// Writes may append newer L0 runs while the merge runs; installation only
// replaces the captured prefix and preserves the suffix.
func (s *bphsStore) compactL0Snapshot(part *bphsPartition) (err error) {
	defer func() {
		part.mu.Lock()
		if part.compacting {
			part.compacting = false
			if err == nil && (s.shouldMergeBPHSL0Locked(part) ||
				s.shouldCompactPartitionLocked(part)) {
				s.scheduleCompactionLocked(part)
			}
		}
		part.mu.Unlock()
	}()

	part.mu.Lock()
	if !part.compacting {
		part.mu.Unlock()
		return nil
	}
	mergeCount := bphsL0CompactionTrigger
	if mergeCount > len(part.l0) {
		mergeCount = len(part.l0)
	}
	if mergeCount < 2 {
		part.mu.Unlock()
		return nil
	}
	old := append([]*SortedFileIndex(nil), part.l0[:mergeCount]...)
	generation := part.gen
	part.mu.Unlock()

	if s.l0MergeBuildStart != nil {
		s.l0MergeBuildStart <- struct{}{}
	}
	if s.l0MergeBuildRelease != nil {
		<-s.l0MergeBuildRelease
	}

	merged := newPartitionMemtable(0)
	for _, run := range old {
		records, err := readBPHSSortedFile(run.FilePath)
		if err != nil {
			return err
		}
		for _, rec := range records {
			merged.Put(rec)
		}
	}

	name := fmt.Sprintf("p%06d_l0_%020d.sst", part.index, time.Now().UnixNano())
	path := filepath.Join(s.dir, name)
	l0, err := s.kvs.flushPartitionMemtable(merged, path, 0)
	if err != nil {
		return err
	}
	if l0 == nil {
		return nil
	}
	l0.InlineValues = s.inline

	part.mu.Lock()
	if !part.compacting || part.gen != generation || !sameBHPSL0Prefix(part.l0, old) {
		part.mu.Unlock()
		l0.closePool()
		removeBPHSSortedFile(l0)
		return fmt.Errorf("partition %d L0 snapshot changed before install", part.index)
	}
	part.l0 = append([]*SortedFileIndex{l0}, part.l0[len(old):]...)
	part.l0Merges++
	remaining := len(part.l0)
	mergeRounds := part.l0Merges
	part.mu.Unlock()

	fmt.Printf("[BPHS-L0] partition=%d merged=%d remaining=%d l0_merge_rounds=%d\n",
		part.index, mergeCount, remaining, mergeRounds)
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

func readRawSortedFile(path string) ([]bphsRecord, error) {
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
		records = append(records, bphsRecord{
			Key: entry.Key, Version: entry.Index, Kind: bphsInlineRecord, Value: entry.Value,
		})
	}
}

func (kvs *KVServer) flushBPHSBaseFile(records []bphsRecord, path string, inlineCacheBytes int64) (*SortedFileIndex, error) {
	if len(records) == 0 {
		return nil, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	sparse := NewSparseIndexBuilder(kvs.indexBlockBytes)
	var offset int64
	lo, hi := records[0].Key, records[0].Key
	for _, rec := range records {
		entry := &raft.Entry{Index: rec.Version, Key: rec.Key, Value: rec.Value}
		if err := kvs.WriteEntryToSortedFile(writer, entry); err != nil {
			file.Close()
			os.Remove(path)
			return nil, err
		}
		size := int64(20 + len(rec.Key) + len(rec.Value))
		sparse.Observe(rec.Key, offset, size)
		offset += size
		hi = rec.Key
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}
	if err := file.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	sparseEntries := sparse.Build()
	if err := writeSparseIndex(path, sparseEntries, kvs.indexBlockBytes, offset); err != nil {
		os.Remove(path)
		os.Remove(sparseIndexPath(path))
		return nil, err
	}
	pool, err := NewFileDescriptorPool(path, partitionPoolSize)
	if err != nil {
		os.Remove(path)
		os.Remove(sparseIndexPath(path))
		return nil, err
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

type bphsLoadedL0 struct {
	file       *SortedFileIndex
	maxVersion uint32
}

func loadPartitionL0Files(kvs *KVServer, dir string, partition int, inlineCacheBytes int64) ([]bphsLoadedL0, error) {
	pattern := filepath.Join(dir, fmt.Sprintf("p%06d_l0_*.sst", partition))
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	var out []bphsLoadedL0
	for _, path := range paths {
		st, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		sparse, size, _, err := kvs.loadOrRebuildSparseIndex(path, st.Size())
		if err != nil {
			return nil, err
		}
		if len(sparse) == 0 {
			return nil, fmt.Errorf("BPHS L0 %s has no sparse index entries", path)
		}

		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReaderSize(file, 1<<20)
		count := 0
		var maxVersion uint32
		keys := make([]string, 0, 4096)
		for {
			entry, _, err := ReadEntry(reader, 0)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return nil, err
			}
			count++
			keys = append(keys, entry.Key)
			if entry.Index > maxVersion {
				maxVersion = entry.Index
			}
		}
		file.Close()

		hi, err := lastKeyFrom(path, sparse[len(sparse)-1].Offset, size)
		if err != nil {
			return nil, err
		}
		pool, err := NewFileDescriptorPool(path, partitionPoolSize)
		if err != nil {
			return nil, err
		}
		out = append(out, bphsLoadedL0{
			file: &SortedFileIndex{
				Sparse:       sparse,
				FileSize:     size,
				InlineValues: NewInlineCache(inlineCacheBytes),
				FilePath:     path,
				Lo:           sparse[0].PaddedKey,
				Hi:           hi,
				pool:         pool,
				Entries:      count,
				bphsFilter:   newBPHSKeyFilter(keys),
			},
			maxVersion: maxVersion,
		})
	}
	return out, nil
}

func loadBPHSBaseFile(kvs *KVServer, dir string, meta bphsPartitionManifest, inlineCacheBytes int64) (*SortedFileIndex, error) {
	path := filepath.Join(dir, filepath.Base(meta.BaseFile))
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if st.Size() != meta.BaseSize {
		return nil, fmt.Errorf("BPHS base %s size %d, manifest says %d", path, st.Size(), meta.BaseSize)
	}
	sparse, size, _, err := kvs.loadOrRebuildSparseIndex(path, meta.BaseSize)
	if err != nil {
		return nil, err
	}
	if len(sparse) == 0 {
		return nil, fmt.Errorf("BPHS base %s has no sparse index entries", path)
	}
	if meta.Lo != "" && sparse[0].PaddedKey != meta.Lo {
		return nil, fmt.Errorf("BPHS base %s starts at %q, manifest says %q", path, sparse[0].PaddedKey, meta.Lo)
	}
	hi, err := lastKeyFrom(path, sparse[len(sparse)-1].Offset, size)
	if err != nil {
		return nil, err
	}
	if meta.Hi != "" && hi != meta.Hi {
		return nil, fmt.Errorf("BPHS base %s ends at %q, manifest says %q", path, hi, meta.Hi)
	}
	pool, err := NewFileDescriptorPool(path, partitionPoolSize)
	if err != nil {
		return nil, err
	}
	return &SortedFileIndex{
		Sparse:       sparse,
		FileSize:     size,
		InlineValues: NewInlineCache(inlineCacheBytes),
		FilePath:     path,
		Lo:           sparse[0].PaddedKey,
		Hi:           hi,
		pool:         pool,
		Entries:      meta.Entries,
	}, nil
}

func (s *bphsStore) Get(key string) (value string, found bool, err error) {
	if err := s.compactionError(); err != nil {
		return "", false, fmt.Errorf("background partition compaction: %w", err)
	}
	part, err := s.route(key)
	if err != nil {
		return "", false, err
	}

	part.mu.RLock()
	defer part.mu.RUnlock()
	if rec, ok := part.mem.Get(key); ok {
		switch rec.Kind {
		case bphsInlineRecord:
			recordBPHSRead(bphsReadMem)
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
			recordBPHSRead(bphsReadMem)
			return string(raw), true, nil
		}
	}

	// L0 按从新到旧查找；后台分区 Compaction 合入 base 前，这里必须继续
	// 覆盖所有仍在文件中的版本。
	for i := len(part.l0) - 1; i >= 0; i-- {
		if part.l0[i].bphsFilter != nil && !part.l0[i].bphsFilter.MayContain(key) {
			recordBPHSBloomProbe(true)
			continue
		}
		recordBPHSBloomProbe(false)
		entry, err := s.kvs.lookupInSortedFile(part.l0[i], key)
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
			recordBPHSRead(bphsReadL0)
			return rec.Value, true, nil
		}
		raw, err := part.vlog.Read(rec.Blob)
		if err != nil {
			return "", false, err
		}
		recordBPHSRead(bphsReadL0)
		return string(raw), true, nil
	}

	if part.base != nil {
		entry, err := s.kvs.lookupInSortedFile(part.base, key)
		if err == nil {
			recordBPHSRead(bphsReadBase)
			return entry.Value, true, nil
		}
		if err != ErrKeyAbsent {
			return "", false, err
		}
	}
	recordBPHSRead(bphsReadMiss)
	return "", false, nil
}

func (s *bphsStore) Scan(startKey, endKey string) (map[string]string, error) {
	if startKey > endKey {
		return nil, nil
	}
	if err := s.compactionError(); err != nil {
		return nil, fmt.Errorf("background partition compaction: %w", err)
	}

	base := s.basePtr.Load()

	result := make(map[string]string)
	first, last := base.ownedOverlapBounds(startKey, endKey)
	l0Runs := 0
	defer func() {
		recordBPHSScan(max(last-first, 0), l0Runs)
	}()
	for i := first; i < last; i++ {
		part := s.parts[base.partitionID(i)]
		part.mu.RLock()

		partStart, partEnd := startKey, endKey
		if part.ownershipLo != "" && partStart < part.ownershipLo {
			partStart = part.ownershipLo
		}
		if part.ownershipHi != "" && partEnd > part.ownershipHi {
			partEnd = part.ownershipHi
		}
		if partStart > partEnd {
			part.mu.RUnlock()
			continue
		}

		if len(part.l0) == 0 {
			// The ready/local steady state has one run. Keep the original
			// direct streaming path and avoid merge bookkeeping.
			if part.base != nil {
				if err := s.kvs.scanFromSortedFileInto(result, partStart, partEnd, part.base); err != nil {
					part.mu.RUnlock()
					return nil, err
				}
			}
		} else if err := s.scanBPHSPartitionMergedInto(result, partStart, partEnd, part); err != nil {
			part.mu.RUnlock()
			return nil, err
		}
		l0Runs += len(part.l0)
		for _, rec := range part.mem.SortedRange(partStart, partEnd) {
			value, err := s.recordValue(part, rec)
			if err != nil {
				part.mu.RUnlock()
				return nil, err
			}
			result[rec.Key] = value
		}
		part.mu.RUnlock()
	}
	return result, nil
}

type bphsScanRun struct {
	part         *bphsPartition
	file         *os.File
	reader       *bufio.Reader
	pool         *FileDescriptorPool
	decodeRecord bool
	priority     int
	endKey       string
	key          string
	value        string
	valid        bool
}

func (s *bphsStore) openBPHSScanRun(
	part *bphsPartition, index *SortedFileIndex, startKey, endKey string,
	decodeRecord bool, priority int,
) (*bphsScanRun, error) {
	if index == nil {
		return nil, nil
	}
	startOffset, endOffset, ok := index.boundedScanRange(startKey, endKey)
	if !ok {
		return nil, nil
	}
	if index.pool == nil {
		return nil, fmt.Errorf("BPHS scan file %s has no descriptor pool", index.FilePath)
	}
	file, err := index.pool.Get()
	if err != nil {
		return nil, err
	}
	length := endOffset - startOffset
	run := &bphsScanRun{
		part:         part,
		file:         file,
		reader:       bufio.NewReaderSize(io.NewSectionReader(file, startOffset, length), boundedScanBufferBytes(length)),
		pool:         index.pool,
		decodeRecord: decodeRecord,
		priority:     priority,
		endKey:       endKey,
	}
	if err := s.advanceBPHSScanRun(run, startKey); err != nil {
		run.close()
		return nil, err
	}
	return run, nil
}

func (s *bphsStore) advanceBPHSScanRun(run *bphsScanRun, startKey string) error {
	for {
		entry, _, err := ReadEntry(run.reader, 0)
		if err == io.EOF {
			run.valid = false
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Key > run.endKey {
			run.valid = false
			return nil
		}
		if entry.Key < startKey {
			continue
		}
		if run.decodeRecord {
			rec, err := entryToBPHSRecord(entry)
			if err != nil {
				return err
			}
			run.value, err = s.recordValue(run.part, rec)
			if err != nil {
				return err
			}
			run.key = rec.Key
		} else {
			run.key, run.value = entry.Key, entry.Value
		}
		run.valid = true
		return nil
	}
}

func (run *bphsScanRun) close() {
	if run == nil || run.file == nil {
		return
	}
	run.pool.Put(run.file)
	run.file = nil
}

// scanBPHSPartitionMergedInto merges immutable Base and L0 runs in key order.
// Every key is emitted once with the newest L0 version taking precedence over
// the Base, avoiding the old base-first/L0-overwrite double map insertion.
func (s *bphsStore) scanBPHSPartitionMergedInto(
	result map[string]string, startKey, endKey string, part *bphsPartition,
) error {
	runs := make([]*bphsScanRun, 0, len(part.l0)+1)
	closeRuns := func() {
		for _, run := range runs {
			run.close()
		}
	}
	defer closeRuns()

	if part.base != nil {
		run, err := s.openBPHSScanRun(part, part.base, startKey, endKey, false, 0)
		if err != nil {
			return err
		}
		if run != nil {
			runs = append(runs, run)
		}
	}
	for i, l0 := range part.l0 {
		run, err := s.openBPHSScanRun(part, l0, startKey, endKey, true, i+1)
		if err != nil {
			return err
		}
		if run != nil {
			runs = append(runs, run)
		}
	}

	for {
		minKey := ""
		var newest *bphsScanRun
		for _, run := range runs {
			if !run.valid {
				continue
			}
			if newest == nil || run.key < minKey {
				minKey = run.key
				newest = run
				continue
			}
			if run.key == minKey && run.priority > newest.priority {
				newest = run
			}
		}
		if newest == nil {
			return nil
		}
		result[minKey] = newest.value
		for _, run := range runs {
			if run.valid && run.key == minKey {
				if err := s.advanceBPHSScanRun(run, startKey); err != nil {
					return err
				}
			}
		}
	}
}

// scanBPHSSortedFileInto remains the single-run helper for callers that already
// know there is no L0 overlay, including narrow debugging paths and tests.
func (s *bphsStore) scanBPHSSortedFileInto(result map[string]string, startKey, endKey string, part *bphsPartition, index *SortedFileIndex) error {
	if index == nil {
		return fmt.Errorf("BPHS L0 index is nil")
	}
	// L0 files are immutable and carry their exact key bounds. Avoid opening a
	// descriptor or parsing any rows when the requested range cannot overlap.
	if (index.Hi != "" && startKey > index.Hi) || (index.Lo != "" && endKey < index.Lo) {
		return nil
	}
	startOffset, endOffset, ok := index.boundedScanRange(startKey, endKey)
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
	length := endOffset - startOffset
	reader := bufio.NewReaderSize(
		io.NewSectionReader(file, startOffset, length),
		boundedScanBufferBytes(length),
	)
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
		result[rec.Key] = value
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

func (s *bphsStore) durableSyncError() error {
	s.durableErrMu.Lock()
	defer s.durableErrMu.Unlock()
	return s.durableErr
}

func (s *bphsStore) setDurableSyncError(err error) {
	if err == nil {
		return
	}
	s.durableErrMu.Lock()
	if s.durableErr == nil {
		s.durableErr = err
	}
	s.durableErrMu.Unlock()
}

func (s *bphsStore) appendPendingSharedWAL() error {
	if len(s.walPending) == 0 {
		return nil
	}
	batch := s.walPending
	written, err := s.wal.AppendBatch(batch)
	if err != nil {
		return fmt.Errorf("append BPHS shared WAL: %w", err)
	}
	s.walPending = nil
	syncStart := time.Now()
	err = s.wal.Sync()
	recordBPHSSharedWAL(written, time.Since(syncStart))
	if err != nil {
		return fmt.Errorf("sync BPHS shared WAL: %w", err)
	}
	return nil
}

func (s *bphsStore) syncRecordPartitions(indices map[int]uint32, concurrency int) error {
	if len(indices) == 0 {
		return nil
	}
	for idx := range indices {
		if idx < 0 || idx >= len(s.parts) {
			return fmt.Errorf("invalid BPHS partition id %d", idx)
		}
	}
	if concurrency <= 0 {
		concurrency = bphsDefaultRecordSyncConcurrency
	}
	sem := make(chan struct{}, concurrency)
	results := make(chan error, len(indices))
	var wg sync.WaitGroup
	for idx := range indices {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			part := s.parts[idx]
			lockStart := time.Now()
			part.mu.Lock()
			defer part.mu.Unlock()
			recordBPHSPartitionLockWait(time.Since(lockStart))
			if part.vlogDirty {
				syncStart := time.Now()
				err := part.vlog.Sync()
				recordBPHSVLogSync(time.Since(syncStart))
				if err != nil {
					results <- fmt.Errorf("sync partition %d vlog: %w", idx, err)
					return
				}
				part.vlogDirty = false
			}
			if part.recordsDirty {
				syncStart := time.Now()
				err := part.records.Sync()
				recordBPHSRecordSync(time.Since(syncStart))
				if err != nil {
					results <- fmt.Errorf("sync partition %d records: %w", idx, err)
					return
				}
				part.recordsDirty = false
			}
			if version := indices[idx]; version > 0 {
				if err := s.advancePartitionDurable(part, version); err != nil {
					results <- fmt.Errorf("advance partition %d durable index: %w", idx, err)
					return
				}
			}
		}(idx)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *bphsStore) recordSyncLoop() {
	defer s.recordSyncWG.Done()
	for {
		select {
		case <-s.recordSyncStop:
			return
		case <-s.recordSyncSignal:
			for {
				indices := s.takePendingRecordSync()
				if len(indices) == 0 {
					break
				}
				recordBPHSRecordSyncBatch(len(indices))
				if err := s.syncRecordPartitions(indices, s.recordSyncConcurrency); err != nil {
					s.setDurableSyncError(err)
				}
			}
		}
	}
}

func (s *bphsStore) takePendingRecordSync() map[int]uint32 {
	s.recordSyncMu.Lock()
	defer s.recordSyncMu.Unlock()
	if len(s.recordSyncPending) == 0 {
		return nil
	}
	out := s.recordSyncPending
	s.recordSyncPending = make(map[int]uint32)
	return out
}

func (s *bphsStore) queueRecordSync(indices map[int]struct{}) {
	if len(indices) == 0 {
		return
	}
	s.recordSyncMu.Lock()
	deduped := 0
	for idx := range indices {
		version := s.parts[idx].pendingDurableVersion.Load()
		if version == 0 {
			deduped++
			continue
		}
		if old, exists := s.recordSyncPending[idx]; exists && old >= version {
			deduped++
			continue
		}
		s.recordSyncPending[idx] = version
	}
	s.recordSyncMu.Unlock()
	recordBPHSRecordSyncQueued(len(indices), deduped)
	select {
	case s.recordSyncSignal <- struct{}{}:
	default:
	}
}

func (s *bphsStore) checkpointSharedWAL() error {
	if s.walMaxBytes <= 0 || s.wal.Size() < s.walMaxBytes {
		return nil
	}
	allVersions := make(map[int]uint32, len(s.parts))
	for i := range s.parts {
		allVersions[i] = s.parts[i].pendingDurableVersion.Load()
	}
	if err := s.syncRecordPartitions(allVersions, s.syncConcurrency); err != nil {
		return err
	}
	if err := s.wal.Reset(); err != nil {
		return fmt.Errorf("reset BPHS shared WAL: %w", err)
	}
	recordBPHSSharedWALReset()
	return nil
}

// SyncPartitionsFast is the apply-loop path: one shared WAL fsync makes the
// whole batch durable, while per-partition record logs are synced in a
// background worker. The existing synchronous SyncPartitions remains the
// compatibility path used by recovery/import and tests.
func (s *bphsStore) SyncPartitionsFast(indices map[int]struct{}) error {
	if s == nil || len(indices) == 0 {
		return nil
	}
	if err := s.compactionError(); err != nil {
		return fmt.Errorf("background partition compaction: %w", err)
	}
	if err := s.durableSyncError(); err != nil {
		return fmt.Errorf("background BPHS record sync: %w", err)
	}
	for idx := range indices {
		if idx < 0 || idx >= len(s.parts) {
			return fmt.Errorf("invalid BPHS partition id %d", idx)
		}
	}
	syncConcurrency := s.syncConcurrency
	if syncConcurrency <= 0 {
		syncConcurrency = bphsDefaultPartitionSyncConcurrency
	}
	syncStart := time.Now()
	defer func() {
		recordBPHSSyncBatch(len(indices), syncConcurrency, time.Since(syncStart))
	}()
	if s.raftLogWAL {
		s.queueRecordSync(indices)
		return nil
	}

	// Blob refs in the shared WAL are usable only after their partition VLogs
	// are durable. Inline-only workloads skip this loop entirely.
	type syncResult struct {
		idx int
		err error
	}
	results := make(chan syncResult, len(indices))
	sem := make(chan struct{}, syncConcurrency)
	var wg sync.WaitGroup
	for idx := range indices {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			part := s.parts[idx]
			lockStart := time.Now()
			part.mu.Lock()
			defer part.mu.Unlock()
			recordBPHSPartitionLockWait(time.Since(lockStart))
			if !part.vlogDirty {
				return
			}
			syncStart := time.Now()
			err := part.vlog.Sync()
			recordBPHSVLogSync(time.Since(syncStart))
			if err != nil {
				results <- syncResult{idx: idx, err: fmt.Errorf("sync partition %d vlog: %w", idx, err)}
				return
			}
			part.vlogDirty = false
		}(idx)
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			return result.err
		}
	}

	if err := s.appendPendingSharedWAL(); err != nil {
		return err
	}
	s.queueRecordSync(indices)
	return s.checkpointSharedWAL()
}

func (s *bphsStore) SyncPartitions(indices map[int]struct{}) error {
	if s == nil || len(indices) == 0 {
		return nil
	}
	if err := s.compactionError(); err != nil {
		return fmt.Errorf("background partition compaction: %w", err)
	}
	for idx := range indices {
		if idx < 0 || idx >= len(s.parts) {
			return fmt.Errorf("invalid BPHS partition id %d", idx)
		}
	}
	syncConcurrency := s.syncConcurrency
	if syncConcurrency <= 0 {
		syncConcurrency = bphsDefaultPartitionSyncConcurrency
	}
	syncStart := time.Now()
	defer func() {
		recordBPHSSyncBatch(len(indices), syncConcurrency, time.Since(syncStart))
	}()

	type syncResult struct {
		idx int
		err error
	}
	results := make(chan syncResult, len(indices))
	sem := make(chan struct{}, syncConcurrency)
	var wg sync.WaitGroup
	for idx := range indices {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			part := s.parts[idx]
			lockStart := time.Now()
			part.mu.Lock()
			defer part.mu.Unlock()
			recordBPHSPartitionLockWait(time.Since(lockStart))
			if part.vlogDirty {
				syncStart := time.Now()
				if err := part.vlog.Sync(); err != nil {
					recordBPHSVLogSync(time.Since(syncStart))
					results <- syncResult{idx: idx, err: fmt.Errorf("sync partition %d vlog: %w", idx, err)}
					return
				}
				recordBPHSVLogSync(time.Since(syncStart))
				part.vlogDirty = false
			}
			if part.recordsDirty {
				syncStart := time.Now()
				if err := part.records.Sync(); err != nil {
					recordBPHSRecordSync(time.Since(syncStart))
					results <- syncResult{idx: idx, err: fmt.Errorf("sync partition %d records: %w", idx, err)}
					return
				}
				recordBPHSRecordSync(time.Since(syncStart))
				part.recordsDirty = false
			}
			if version := part.pendingDurableVersion.Load(); version > 0 {
				if err := s.advancePartitionDurable(part, version); err != nil {
					results <- syncResult{idx: idx, err: fmt.Errorf("advance partition %d durable index: %w", idx, err)}
					return
				}
			}
			results <- syncResult{idx: idx}
		}(idx)
	}
	wg.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			return result.err
		}
	}
	if err := s.appendPendingSharedWAL(); err != nil {
		return err
	}
	return s.checkpointSharedWAL()
}

// drainLegacyTailIntoBPHS 把首次启用前落在 currentDB/currentLog 的尾部记录导入
// BPHS 覆盖层。首轮 GC 的切换与搬迁之间允许客户端继续写；如果不导入，base 中的
// 旧值会在 BPHS.Get 中先被找到，遮蔽 currentDB 中更新的版本。
func (s *bphsStore) drainLegacyTailIntoBPHS() error {
	file, err := os.Open(s.kvs.currentLog)
	if err != nil {
		return err
	}
	defer file.Close()

	// Inline rows in the legacy store contain only key+value, so
	// entryFromRecord cannot recover their original Raft index. They are all
	// known to be covered by the current applied index, which is a safe cutoff:
	// any post-BPHS write will carry a higher index and still win.
	legacyCutoff := uint32(s.kvs.lastAppliedIndex)

	it := s.kvs.persister.GetDb().NewIterator(grocksdb.NewDefaultReadOptions())
	defer it.Close()
	dirty := make(map[int]struct{})
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key, value := it.Key(), it.Value()
		if raft.IsMetaKey(key.Data()) {
			key.Free()
			value.Free()
			continue
		}
		entry, err := s.kvs.entryFromRecord(string(key.Data()), value.Data(), file)
		key.Free()
		value.Free()
		if err != nil {
			return fmt.Errorf("migrate legacy tail into BPHS: %w", err)
		}
		version := bphsLegacyTailVersion(entry.Index, legacyCutoff)
		part, err := s.Put(entry.Key, entry.Value, version)
		if err != nil {
			return err
		}
		dirty[part] = struct{}{}
	}
	return s.SyncPartitions(dirty)
}

func bphsLegacyTailVersion(entryIndex, appliedCutoff uint32) uint32 {
	if entryIndex != 0 {
		return entryIndex
	}
	return appliedCutoff
}

func (s *bphsStore) Close() error {
	if s == nil {
		return nil
	}
	var first error
	s.closeOnce.Do(func() {
		// Let already queued compactions finish so they cannot race the file
		// close below. New compactions are expected to stop before Close.
		s.compactJobs.Wait()
		close(s.compactStop)
		s.compactWG.Wait()
		close(s.recordSyncStop)
		s.recordSyncWG.Wait()

		s.mu.Lock()
		defer s.mu.Unlock()
		for _, part := range s.parts {
			if part == nil {
				continue
			}
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
			if part.baseLocal && part.base != nil {
				part.base.closePool()
			}
			part.mu.Unlock()
		}
		if err := s.wal.Close(); first == nil {
			first = err
		}
		s.parts = nil
	})
	if first == nil {
		first = s.compactionError()
	}
	if first == nil {
		first = s.durableSyncError()
	}
	return first
}

// enableOrUpdateBPHS 在首轮分区 GC 完成后打开本地 BPHS 状态。BPHS 激活后
// gcLoop 会暂停全局 absorb GC，以保持 Ownership Range 与本地 base 的版本语义稳定。
// 调用方持有 kvs.mu。
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
	if kvs.bphsDir == "" {
		if err := store.drainLegacyTailIntoBPHS(); err != nil {
			store.Close()
			return err
		}
		if err := store.MarkAllDurable(uint32(kvs.lastAppliedIndex)); err != nil {
			store.Close()
			return fmt.Errorf("mark initial BPHS durable index: %w", err)
		}
	}
	store.markLegacyCovered()
	kvs.bphs = store
	kvs.bphsDir = dir
	return nil
}

type bphsSnapshotPartition struct {
	Index          int    `json:"index"`
	Generation     uint64 `json:"generation"`
	MaxVersion     uint32 `json:"max_version"`
	RecordsFile    string `json:"records_file"`
	RecordsBytes   int64  `json:"records_bytes"`
	VLogFile       string `json:"vlog_file"`
	VLogBytes      int64  `json:"vlog_bytes"`
	BaseFile       string `json:"base_file,omitempty"`
	BaseBytes      int64  `json:"base_bytes,omitempty"`
	BaseIndexFile  string `json:"base_index_file,omitempty"`
	BaseIndexBytes int64  `json:"base_index_bytes,omitempty"`
	ManifestFile   string `json:"manifest_file,omitempty"`
	ManifestBytes  int64  `json:"manifest_bytes,omitempty"`
}

// SnapshotCut 返回一份可随 Raft 快照发送的分区本地文件切点。记录日志包含所有
// 已写入 BPHS 的版本；接收端按快照 AppliedIndex 过滤更高版本。base 已经把版本
// 物化进文件，无法在接收端过滤，所以 base 领先 appliedLimit 时必须让快照稍后重试。
func (s *bphsStore) SnapshotCut(workDir string, appliedLimit int) ([]bphsSnapshotPartition, error) {
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
		if manifest, ok, err := loadBPHSPartitionManifest(s.dir, part.index); err != nil {
			part.mu.Unlock()
			return nil, err
		} else if ok {
			if appliedLimit >= 0 && uint64(manifest.MaxVersion) > uint64(appliedLimit) {
				part.mu.Unlock()
				return nil, fmt.Errorf("%w: partition %d local base covers version %d, snapshot applied only %d",
					errSnapshotBusy, part.index, manifest.MaxVersion, appliedLimit)
			}
			meta.MaxVersion = manifest.MaxVersion
			manifestPath := bphsPartitionManifestPath(s.dir, part.index)
			manifestData, err := os.ReadFile(manifestPath)
			if err != nil {
				part.mu.Unlock()
				return nil, err
			}
			meta.ManifestFile = filepath.Base(manifestPath)
			meta.ManifestBytes = int64(len(manifestData))
		}
		if part.baseLocal && part.base != nil {
			meta.BaseFile = filepath.Base(part.base.FilePath)
			meta.BaseBytes = part.base.FileSize
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
		if meta.RecordsBytes > 0 {
			if err := copyFilePrefix(part.records.Path(), filepath.Join(workDir, meta.RecordsFile), meta.RecordsBytes); err != nil {
				part.mu.Unlock()
				return nil, err
			}
		}
		if meta.VLogBytes > 0 {
			if err := copyFilePrefix(part.vlog.Path(), filepath.Join(workDir, meta.VLogFile), meta.VLogBytes); err != nil {
				part.mu.Unlock()
				return nil, err
			}
		}
		if meta.BaseFile != "" {
			baseCopy := filepath.Join(workDir, meta.BaseFile)
			if !fileExists(baseCopy) {
				if err := copyFilePrefix(part.base.FilePath, baseCopy, meta.BaseBytes); err != nil {
					part.mu.Unlock()
					return nil, err
				}
			}
			if idx := sparseIndexPath(part.base.FilePath); fileExists(idx) {
				st, err := os.Stat(idx)
				if err != nil {
					part.mu.Unlock()
					return nil, err
				}
				meta.BaseIndexFile = filepath.Base(idx)
				meta.BaseIndexBytes = st.Size()
				indexCopy := filepath.Join(workDir, meta.BaseIndexFile)
				if !fileExists(indexCopy) {
					if err := copyFilePrefix(idx, indexCopy, meta.BaseIndexBytes); err != nil {
						part.mu.Unlock()
						return nil, err
					}
				}
			}
		}
		if meta.ManifestFile != "" && meta.ManifestBytes > 0 {
			if err := copyFilePrefix(filepath.Join(s.dir, meta.ManifestFile),
				filepath.Join(workDir, meta.ManifestFile), meta.ManifestBytes); err != nil {
				part.mu.Unlock()
				return nil, err
			}
		}
		part.mu.Unlock()
		out = append(out, meta)
	}
	return out, nil
}

func copyFilePrefix(src, dst string, size int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(out, in, size); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	return out.Close()
}
