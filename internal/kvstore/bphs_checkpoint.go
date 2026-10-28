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
// Go heap scavenging do not overlap a latency-sensitive read phase.
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
			maxLagEntries := int(threshold / 512)
			if maxLagEntries < 100_000 {
				maxLagEntries = 100_000
			}
			if drained, err := kvs.drainBPHSDurable(maxLagEntries); err != nil {
				fmt.Printf("[BPHS-DURABLE] failed to drain durable watermark: %v\n", err)
				idleSince = time.Time{}
				continue
			} else if drained > 0 {
				fmt.Printf("[BPHS-DURABLE] materialized through index %d; active Raft log=%dMB\n",
					drained, logSize>>20)
				idleSince = time.Time{}
				continue
			}
			lastActivity := time.Unix(0, kvs.lastClientActivity.Load())
			if now.Sub(lastActivity) < bphsCheckpointIdle {
				idleSince = time.Time{}
				continue
			}
			index, _, ok := kvs.raft.CheckpointCandidate()
			if !ok {
				idleSince = time.Time{}
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

func (kvs *KVServer) drainBPHSDurable(maxLagEntries int) (int, error) {
	kvs.stateMu.Lock()
	defer kvs.stateMu.Unlock()
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	if kvs.bphs == nil || !kvs.bphsRaftLogWAL || kvs.installing || kvs.gcActive || kvs.gcInProgress {
		return 0, nil
	}
	applied := kvs.lastAppliedIndex
	durable := int(kvs.bphs.DurableIndex())
	if applied <= durable || applied-durable < maxLagEntries {
		return 0, nil
	}
	if err := kvs.bphs.MaterializeAll(); err != nil {
		return 0, err
	}
	if err := kvs.bphs.MarkAllDurable(uint32(applied)); err != nil {
		return 0, err
	}
	return applied, nil
}

func (kvs *KVServer) checkpointBPHSLog(expected int) (bool, string, string, error) {
	// Match captureState/installSnapshot lock order: readers are excluded first,
	// then apply/snapshot state, then the GC state fields.
	kvs.storeRetireMu.Lock()
	defer kvs.storeRetireMu.Unlock()
	kvs.stateMu.Lock()
	defer kvs.stateMu.Unlock()
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
