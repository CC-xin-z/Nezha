package kvstore

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// bphsMergeSource exposes one sorted run to the compaction heap. The heap keeps
// only the current record of each source, so compaction memory is bounded by
// the number of input files rather than the number of keys in the partition.
type bphsMergeSource struct {
	current bphsRecord
	valid   bool
	order   int
	advance func() error
	close   func()
}

type bphsMergeHeap []*bphsMergeSource

func (h bphsMergeHeap) Len() int {
	return len(h)
}

func (h bphsMergeHeap) Less(i, j int) bool {
	left, right := h[i].current, h[j].current
	if left.Key != right.Key {
		return left.Key < right.Key
	}
	if left.Version != right.Version {
		return left.Version > right.Version
	}
	return h[i].order > h[j].order
}

func (h bphsMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *bphsMergeHeap) Push(value any) {
	*h = append(*h, value.(*bphsMergeSource))
}

func (h *bphsMergeHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*h = old[:last]
	return value
}

func openBPHSFileMergeSource(path string, encoded bool, order int) (*bphsMergeSource, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	source := &bphsMergeSource{
		order: order,
		close: func() {
			_ = file.Close()
		},
	}
	reader := bufio.NewReaderSize(file, 1<<20)
	source.advance = func() error {
		entry, _, err := ReadEntry(reader, 0)
		if err == io.EOF {
			source.valid = false
			return nil
		}
		if err != nil {
			return err
		}
		var record bphsRecord
		if encoded {
			record, err = entryToBPHSRecord(entry)
			if err != nil {
				return err
			}
		} else {
			record = bphsRecord{
				Key:     entry.Key,
				Version: entry.Index,
				Kind:    bphsInlineRecord,
				Value:   entry.Value,
			}
		}
		source.current = record
		source.valid = true
		return nil
	}
	if err := source.advance(); err != nil {
		source.close()
		return nil, err
	}
	if !source.valid {
		source.close()
		return nil, nil
	}
	return source, nil
}

func newBPHSMemMergeSource(records []bphsRecord, order int) *bphsMergeSource {
	if len(records) == 0 {
		return nil
	}
	next := 0
	source := &bphsMergeSource{order: order, close: func() {}}
	source.advance = func() error {
		if next >= len(records) {
			source.valid = false
			return nil
		}
		source.current = records[next]
		source.valid = true
		next++
		return nil
	}
	_ = source.advance()
	return source
}

func closeBPHSMergeSources(sources []*bphsMergeSource) {
	for _, source := range sources {
		if source != nil && source.close != nil {
			source.close()
			source.close = nil
		}
	}
}

// writeStreamingCompactionBase merges Base, frozen L0 runs and the snapshotted
// MemTable directly into a new sorted Base file.
func (s *bphsStore) writeStreamingCompactionBase(
	part *bphsPartition,
	snapshot bphsCompactionSnapshot,
	path string,
) (*SortedFileIndex, uint32, error) {
	var sources []*bphsMergeSource
	defer func() {
		closeBPHSMergeSources(sources)
	}()

	order := 0
	if snapshot.base != nil {
		source, err := openBPHSFileMergeSource(snapshot.base.FilePath, false, order)
		if err != nil {
			return nil, 0, err
		}
		if source != nil {
			sources = append(sources, source)
		}
		order++
	}
	for _, run := range snapshot.l0 {
		source, err := openBPHSFileMergeSource(run.FilePath, true, order)
		if err != nil {
			return nil, 0, err
		}
		if source != nil {
			if source.current.Kind != bphsInlineRecord {
				source.close()
				return nil, 0, fmt.Errorf(
					"streaming compaction partition %d contains non-inline L0 record",
					part.index,
				)
			}
			sources = append(sources, source)
		}
		order++
	}
	if source := newBPHSMemMergeSource(snapshot.mem, order); source != nil {
		if source.current.Kind != bphsInlineRecord {
			source.close()
			return nil, 0, fmt.Errorf(
				"streaming compaction partition %d contains non-inline MemTable record",
				part.index,
			)
		}
		sources = append(sources, source)
	}
	if len(sources) == 0 {
		return nil, 0, nil
	}

	merge := bphsMergeHeap(sources)
	heap.Init(&merge)

	file, err := os.Create(path)
	if err != nil {
		return nil, 0, err
	}
	removePartial := func() {
		_ = file.Close()
		_ = os.Remove(path)
		_ = os.Remove(sparseIndexPath(path))
	}
	writer := bufio.NewWriter(file)
	sparse := NewSparseIndexBuilder(s.kvs.indexBlockBytes)
	var offset int64
	var entries int64
	var maxVersion uint32
	var lo, hi string

	for merge.Len() > 0 {
		winner := merge[0].current
		key := winner.Key
		for merge.Len() > 0 && merge[0].current.Key == key {
			source := heap.Pop(&merge).(*bphsMergeSource)
			if err := source.advance(); err != nil {
				removePartial()
				return nil, 0, err
			}
			if source.valid {
				heap.Push(&merge, source)
			} else {
				source.close()
				source.close = nil
			}
		}
		if winner.Kind != bphsInlineRecord {
			removePartial()
			return nil, 0, fmt.Errorf(
				"streaming compaction partition %d produced non-inline record",
				part.index,
			)
		}
		if entries == 0 {
			lo = winner.Key
		}
		hi = winner.Key
		if winner.Version > maxVersion {
			maxVersion = winner.Version
		}
		if err := s.kvs.WriteEntryToSortedFile(writer, &raft.Entry{
			Index: winner.Version,
			Key:   winner.Key,
			Value: winner.Value,
		}); err != nil {
			removePartial()
			return nil, 0, err
		}
		size := int64(20 + len(winner.Key) + len(winner.Value))
		sparse.Observe(winner.Key, offset, size)
		offset += size
		entries++
	}
	if entries == 0 {
		removePartial()
		return nil, 0, nil
	}
	if err := writer.Flush(); err != nil {
		removePartial()
		return nil, 0, err
	}
	if err := file.Sync(); err != nil {
		removePartial()
		return nil, 0, err
	}
	if err := file.Close(); err != nil {
		removePartial()
		return nil, 0, err
	}

	sparseEntries := sparse.Build()
	if err := writeSparseIndex(path, sparseEntries, s.kvs.indexBlockBytes, offset); err != nil {
		removePartial()
		return nil, 0, err
	}
	pool, err := NewFileDescriptorPool(path, partitionPoolSize)
	if err != nil {
		removePartial()
		return nil, 0, err
	}
	return &SortedFileIndex{
		Sparse:       sparseEntries,
		FileSize:     offset,
		InlineValues: NewInlineCache(0),
		FilePath:     path,
		Lo:           lo,
		Hi:           hi,
		pool:         pool,
		Entries:      int(entries),
	}, maxVersion, nil
}
