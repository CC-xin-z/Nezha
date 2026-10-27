package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

const bphsDurableIndexName = "durable_index.json"

type bphsDurableState struct {
	Partitions   []uint32 `json:"partitions"`
	DurableIndex uint32   `json:"durable_index"`
}

func loadBPHSDurableIndex(dir string) (uint32, bool, error) {
	state, ok, err := readBPHSDurableState(dir)
	if err != nil || !ok {
		return 0, ok, err
	}
	return state.DurableIndex, true, nil
}

func readBPHSDurableState(dir string) (bphsDurableState, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, bphsDurableIndexName))
	if errors.Is(err, os.ErrNotExist) {
		return bphsDurableState{}, false, nil
	}
	if err != nil {
		return bphsDurableState{}, false, err
	}
	var state bphsDurableState
	if err := json.Unmarshal(data, &state); err != nil {
		return bphsDurableState{}, false, err
	}
	if len(state.Partitions) == 0 {
		return bphsDurableState{}, false, fmt.Errorf("BPHS durable index has no partitions")
	}
	for _, version := range state.Partitions {
		if version < state.DurableIndex {
			return bphsDurableState{}, false, fmt.Errorf("BPHS durable index %d exceeds partition coverage %d",
				state.DurableIndex, version)
		}
	}
	return state, true, nil
}

func writeBPHSDurableState(path string, state bphsDurableState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return raft.WriteFileAtomic(path, data)
}

func (s *bphsStore) DurableIndex() uint32 {
	if s == nil {
		return 0
	}
	s.durableMu.Lock()
	defer s.durableMu.Unlock()
	return s.durableIndex
}

func (s *bphsStore) MarkAllDurable(index uint32) error {
	if s == nil {
		return nil
	}
	for _, part := range s.parts {
		part.durableIndex.Store(index)
	}
	return s.refreshDurableIndex()
}

func (s *bphsStore) advancePartitionDurable(part *bphsPartition, index uint32) error {
	if s == nil || part == nil || index == 0 {
		return nil
	}
	for {
		old := part.durableIndex.Load()
		if index <= old || part.durableIndex.CompareAndSwap(old, index) {
			break
		}
	}
	return s.refreshDurableIndex()
}

func (s *bphsStore) refreshDurableIndex() error {
	if s == nil || len(s.parts) == 0 {
		return nil
	}
	var minVersion uint32
	for i, part := range s.parts {
		version := part.durableIndex.Load()
		if i == 0 || version < minVersion {
			minVersion = version
		}
	}
	if minVersion == 0 {
		return nil
	}

	s.durableMu.Lock()
	if minVersion > s.durableIndex {
		state := bphsDurableState{
			Partitions:   make([]uint32, len(s.parts)),
			DurableIndex: minVersion,
		}
		for i, part := range s.parts {
			state.Partitions[i] = part.durableIndex.Load()
		}
		path := s.durablePath
		if path == "" {
			path = filepath.Join(s.dir, bphsDurableIndexName)
		}
		if err := writeBPHSDurableState(path, state); err != nil {
			s.durableMu.Unlock()
			return err
		}
		s.durableIndex = minVersion
	}
	durable := s.durableIndex
	s.durableMu.Unlock()

	if durable == 0 {
		return nil
	}
	if s.raftLogWAL && s.kvs != nil && s.kvs.raft != nil {
		s.kvs.raft.SetCompactFloor(int(durable))
		s.kvs.persister.SetApplied(int(durable))
	}
	return nil
}
