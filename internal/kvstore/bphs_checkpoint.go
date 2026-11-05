package kvstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	bphsCheckpointPoll    = time.Second
	bphsCheckpointMinLog  = 256 << 20
	bphsCheckpointMaxTail = 64 << 20
	bphsIdleCompactPoll   = 5 * time.Second
	bphsIdleCompactQuiet  = 60 * time.Second
)

func (kvs *KVServer) noteClientActivity() {
	if kvs != nil {
		kvs.lastClientActivity.Store(time.Now().UnixNano())
	}
}

// bphsLogCheckpointLoop retires the durable prefix of the active Raft log while
// preserving entries after the partition durable frontier. It can run during
// writes and does not force every MemTable to flush.
func (kvs *KVServer) bphsLogCheckpointLoop(ctx context.Context) {
	if kvs == nil || !kvs.bphsRaftLogWAL || kvs.bphsLogCheckpointBytes <= 0 {
		return
	}
	threshold := kvs.bphsLogCheckpointBytes
	if threshold < bphsCheckpointMinLog {
		threshold = bphsCheckpointMinLog
	}

	ticker := time.NewTicker(bphsCheckpointPoll)
	defer ticker.Stop()
	var nextStatus time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			logSize := kvs.raft.CurrentLogDiskSize()
			if logSize < threshold {
				continue
			}
			kvs.mu.Lock()
			store := kvs.bphs
			applied := kvs.lastAppliedIndex
			kvs.mu.Unlock()
			if store == nil {
				continue
			}
			index := int(store.DurableIndex())
			if index <= 0 || index > applied {
				continue
			}

			rotated, oldLog, activeLog, err := kvs.checkpointBPHSLog(index)
			if err != nil {
				fmt.Printf("[BPHS-CHECKPOINT] failed at index %d: %v\n", index, err)
				continue
			}
			if !rotated {
				if now.After(nextStatus) {
					st := kvs.raft.CheckpointStatus()
					fmt.Printf("[BPHS-CHECKPOINT-WAIT] log=%dMB cut=%d last=%d applied=%d commit=%d written=%d offsets=%d batch=%d snapshot=%v in_flight=%v\n",
						logSize>>20, index, st.LastIndex, st.LastApplied, st.CommitIndex,
						st.LastWrittenIndex, st.PendingOffsets, st.PendingBatch,
						st.SnapshotInFlight, st.BatchInFlight)
					nextStatus = now.Add(30 * time.Second)
				}
				continue
			}
			fmt.Printf("[BPHS-CHECKPOINT] retired %s at base=%d; active log=%s\n",
				filepath.Base(oldLog), index, filepath.Base(activeLog))
		}
	}
}

// bphsIdleCompactionLoop collapses residual per-partition MemTables and L0
// runs after a write burst has gone quiet. This is intentionally separate from
// the checkpoint loop: checkpointing bounds the Raft log, while this loop
// bounds the number of local runs observed by subsequent GET and SCAN calls.
func (kvs *KVServer) bphsIdleCompactionLoop(ctx context.Context) {
	if kvs == nil || !kvs.bphsEnabled {
		return
	}
	ticker := time.NewTicker(bphsIdleCompactPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// Keep the store lifecycle stable while compacting. Taking the
			// read lock before inspecting kvs.bphs prevents a concurrent
			// snapshot/GC replacement from closing the store underneath us.
			kvs.storeRetireMu.RLock()
			kvs.mu.Lock()
			store := kvs.bphs
			active := store != nil &&
				!kvs.installing && !kvs.gcActive && !kvs.gcInProgress &&
				now.Sub(kvs.lastPutTime) >= bphsIdleCompactQuiet &&
				now.Sub(time.Unix(0, kvs.lastClientActivity.Load())) >= bphsIdleCompactQuiet
			kvs.mu.Unlock()
			if !active {
				kvs.storeRetireMu.RUnlock()
				continue
			}

			start := time.Now()
			compacted, err := store.CompactForRead()
			kvs.storeRetireMu.RUnlock()
			if err != nil {
				fmt.Printf("[BPHS-IDLE-COMPACT] failed: %v\n", err)
				continue
			}
			if compacted > 0 {
				fmt.Printf("[BPHS-IDLE-COMPACT] partitions=%d elapsed=%v\n",
					compacted, time.Since(start))
			}
			if _, err := kvs.maybeReclaimLegacyPartitions(); err != nil {
				fmt.Printf("[BPHS-LEGACY] reclaim failed: %v\n", err)
			}
			if removed := kvs.reapPartitions(); removed > 0 {
				fmt.Printf("[BPHS-LEGACY] removed %d retired partition files\n", removed)
			}
		}
	}
}

func (kvs *KVServer) checkpointBPHSLog(expected int) (bool, string, string, error) {
	// Keep store/GC replacement out while the Raft log is rotated, but do not
	// exclude normal readers during the tail copy.
	kvs.storeRetireMu.RLock()
	kvs.mu.Lock()
	if kvs.bphs == nil || !kvs.bphsRaftLogWAL || kvs.installing || kvs.gcActive || kvs.gcInProgress {
		kvs.mu.Unlock()
		kvs.storeRetireMu.RUnlock()
		return false, "", "", nil
	}
	if expected <= 0 || expected > kvs.lastAppliedIndex ||
		uint32(expected) > kvs.bphs.DurableIndex() {
		kvs.mu.Unlock()
		kvs.storeRetireMu.RUnlock()
		return false, "", "", nil
	}
	currentLog := kvs.currentLog
	kvs.mu.Unlock()

	newLog := filepath.Join(filepath.Dir(currentLog),
		fmt.Sprintf("RaftState.checkpoint-%d-%d.log", expected, time.Now().UnixNano()))
	oldLog, rotated, err := kvs.raft.RotateLogAtCut(expected, newLog, bphsCheckpointMaxTail)
	if err != nil || !rotated {
		kvs.storeRetireMu.RUnlock()
		return rotated, "", "", err
	}
	kvs.mu.Lock()
	kvs.currentLog = newLog
	kvs.saveKVState()
	kvs.mu.Unlock()
	kvs.storeRetireMu.RUnlock()

	if oldLog != "" {
		// The state now points at the replacement log. Wait for older readers
		// that may still use the retired file, then unlink it.
		kvs.storeRetireMu.Lock()
		if err := os.Remove(oldLog); err != nil && !os.IsNotExist(err) {
			kvs.storeRetireMu.Unlock()
			return true, oldLog, newLog, fmt.Errorf("remove retired Raft log %s: %w", oldLog, err)
		}
		kvs.storeRetireMu.Unlock()
	}
	return true, oldLog, newLog, nil
}
