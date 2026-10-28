package kvstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"
)

const (
	bphsCheckpointPoll   = 5 * time.Second
	bphsCheckpointIdle   = 30 * time.Second
	bphsCheckpointMinLog = 256 << 20
)

func (kvs *KVServer) noteClientActivity() {
	if kvs != nil {
		kvs.lastClientActivity.Store(time.Now().UnixNano())
	}
}

// bphsLogCheckpointLoop retires an obsolete Raft log file after BPHS has made
// every applied entry durable. It waits for a quiet period so log rewriting and
// Go heap scavenging do not overlap a latency-sensitive read phase. The Raft
// log remains the WAL while writes are active; materialization happens only
// after the Raft log is idle, immediately before the old file is retired.
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
	var idleSince time.Time
	var nextStatus time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			logSize := kvs.raft.CurrentLogDiskSize()
			if logSize < threshold {
				idleSince = time.Time{}
				continue
			}
			if kvs.raft.IsLeader() {
				lastActivity := time.Unix(0, kvs.lastClientActivity.Load())
				if now.Sub(lastActivity) < bphsCheckpointIdle {
					idleSince = time.Time{}
					continue
				}
			}
			index, _, ok := kvs.raft.CheckpointCandidate()
			if !ok {
				idleSince = time.Time{}
				if now.After(nextStatus) {
					st := kvs.raft.CheckpointStatus()
					fmt.Printf("[BPHS-CHECKPOINT-WAIT] log=%dMB last=%d applied=%d commit=%d written=%d offsets=%d batch=%d snapshot=%v in_flight=%v\n",
						logSize>>20, st.LastIndex, st.LastApplied, st.CommitIndex,
						st.LastWrittenIndex, st.PendingOffsets, st.PendingBatch,
						st.SnapshotInFlight, st.BatchInFlight)
					nextStatus = now.Add(30 * time.Second)
				}
				continue
			}
			kvs.mu.Lock()
			applied := kvs.lastAppliedIndex
			kvs.mu.Unlock()
			if applied != index {
				idleSince = time.Time{}
				continue
			}
			if idleSince.IsZero() {
				idleSince = now
				continue
			}
			if now.Sub(idleSince) < bphsCheckpointIdle {
				continue
			}

			rotated, oldLog, activeLog, err := kvs.checkpointBPHSLog(index)
			if err != nil {
				fmt.Printf("[BPHS-CHECKPOINT] failed at index %d: %v\n", index, err)
				idleSince = time.Time{}
				continue
			}
			if !rotated {
				idleSince = time.Time{}
				continue
			}
			fmt.Printf("[BPHS-CHECKPOINT] retired %s at base=%d; active log=%s\n",
				filepath.Base(oldLog), index, filepath.Base(activeLog))
			debug.FreeOSMemory()
			idleSince = time.Time{}
		}
	}
}

func (kvs *KVServer) checkpointBPHSLog(expected int) (bool, string, string, error) {
	// Excluding readers prevents the old log file from being unlinked under an
	// in-flight lookup. Do not take stateMu here: applyBatch owns kvs.mu and
	// then takes stateMu.RLock, so stateMu -> kvs.mu would deadlock.
	kvs.storeRetireMu.Lock()
	defer kvs.storeRetireMu.Unlock()
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	if kvs.bphs == nil || !kvs.bphsRaftLogWAL || kvs.installing || kvs.gcActive || kvs.gcInProgress {
		return false, "", "", nil
	}
	index, _, ok := kvs.raft.CheckpointCandidate()
	if !ok || index != kvs.lastAppliedIndex {
		return false, "", "", nil
	}

	// Flush any residual partition MemTables while apply is blocked, then mark
	// every partition durable through the exact Raft index being retired.
	if err := kvs.bphs.MaterializeAll(); err != nil {
		return false, "", "", fmt.Errorf("materialize BPHS: %w", err)
	}
	if err := kvs.bphs.MarkAllDurable(uint32(index)); err != nil {
		return false, "", "", fmt.Errorf("advance BPHS durable index: %w", err)
	}

	newLog := filepath.Join(filepath.Dir(kvs.currentLog),
		fmt.Sprintf("RaftState.checkpoint-%d-%d.log", index, time.Now().UnixNano()))
	oldLog, rotated, err := kvs.raft.RotateEmptyLogIfIdle(index, newLog)
	if err != nil || !rotated {
		return rotated, "", "", err
	}
	kvs.currentLog = newLog
	kvs.saveKVState()
	if oldLog != "" {
		if err := os.Remove(oldLog); err != nil && !os.IsNotExist(err) {
			return true, oldLog, newLog, fmt.Errorf("remove retired Raft log %s: %w", oldLog, err)
		}
	}
	return true, oldLog, newLog, nil
}
