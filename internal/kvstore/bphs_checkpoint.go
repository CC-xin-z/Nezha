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
	// A read workload commonly starts shortly after the write phase. Waiting for
	// both reads and writes to go idle keeps every residual L0 visible to the
	// entire read phase. Once writes have been quiet briefly, maintain the local
	// bases in the background while reads continue.
	bphsReadCompactWriteQuiet = 10 * time.Second
	bphsReadCompactActiveFor  = 2 * time.Second
	bphsReadCompactMinLoad    = 64
)

func (kvs *KVServer) noteClientActivity() {
	if kvs != nil {
		kvs.lastClientActivity.Store(time.Now().UnixNano())
	}
}

func (kvs *KVServer) noteReadActivity(weight uint64) {
	if kvs != nil {
		if weight == 0 {
			weight = 1
		}
		now := time.Now()
		kvs.lastClientActivity.Store(now.UnixNano())
		kvs.lastReadActivity.Store(now.UnixNano())
		kvs.postWriteReadRequests.Add(weight)
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
// runs. A fully idle store gets a complete pass; when writes stop but reads are
// active, it performs one read-priority pass per write generation. This is
// intentionally separate from checkpointing, which bounds the Raft log.
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
			mode, putAt := kvs.bphsMaintenanceModeLocked(now)
			kvs.mu.Unlock()
			if mode == bphsMaintenanceNone || store == nil {
				kvs.storeRetireMu.RUnlock()
				continue
			}

			start := time.Now()
			compacted, err := store.CompactForRead()
			kvs.storeRetireMu.RUnlock()
			if err != nil {
				fmt.Printf("[BPHS-%s-COMPACT] failed: %v\n", mode.logName(), err)
				continue
			}
			if compacted > 0 {
				fmt.Printf("[BPHS-%s-COMPACT] partitions=%d elapsed=%v\n",
					mode.logName(), compacted, time.Since(start))
			}
			if mode == bphsMaintenanceRead {
				kvs.mu.Lock()
				if kvs.lastPutTime.Equal(putAt) {
					kvs.lastReadMaintainedPut = putAt
				}
				kvs.mu.Unlock()
			}
			if _, err := kvs.maybeReclaimLegacyPartitions(); err != nil {
				fmt.Printf("[BPHS-LEGACY] reclaim failed: %v\n", err)
			}
		}
	}
}

type bphsMaintenanceMode uint8

const (
	bphsMaintenanceNone bphsMaintenanceMode = iota
	bphsMaintenanceRead
	bphsMaintenanceIdle
)

func (m bphsMaintenanceMode) logName() string {
	if m == bphsMaintenanceRead {
		return "READ"
	}
	return "IDLE"
}

// bphsMaintenanceModeLocked selects the maintenance priority. The caller holds
// kvs.mu and storeRetireMu; lastReadActivity is atomic because read handlers do
// not take kvs.mu merely to publish activity.
func (kvs *KVServer) bphsMaintenanceModeLocked(now time.Time) (bphsMaintenanceMode, time.Time) {
	if kvs == nil || kvs.bphs == nil || kvs.installing || kvs.gcActive || kvs.gcInProgress {
		return bphsMaintenanceNone, time.Time{}
	}
	putAt := kvs.lastPutTime
	clientAt := time.Unix(0, kvs.lastClientActivity.Load())
	if now.Sub(putAt) >= bphsIdleCompactQuiet && now.Sub(clientAt) >= bphsIdleCompactQuiet {
		return bphsMaintenanceIdle, putAt
	}
	readAt := time.Unix(0, kvs.lastReadActivity.Load())
	if readAt.UnixNano() == 0 ||
		now.Sub(putAt) < bphsReadCompactWriteQuiet ||
		now.Sub(readAt) > bphsReadCompactActiveFor ||
		kvs.postWriteReadRequests.Load() < bphsReadCompactMinLoad ||
		!kvs.lastReadMaintainedPut.Before(putAt) {
		return bphsMaintenanceNone, time.Time{}
	}
	return bphsMaintenanceRead, putAt
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
