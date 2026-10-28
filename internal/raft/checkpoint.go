package raft

import (
	"fmt"
	"os"
	"path/filepath"
)

// CheckpointStatus describes why a fully materialized log checkpoint is or is
// not currently possible.
type CheckpointStatus struct {
	LastIndex        int
	LastApplied      int
	CommitIndex      int
	LastWrittenIndex int
	PendingOffsets   int
	PendingBatch     int
	SnapshotInFlight bool
	BatchInFlight    bool
}

// CurrentLogDiskSize returns the size of the active on-disk Raft log.
func (rf *Raft) CurrentLogDiskSize() int64 {
	rf.logMu.Lock()
	path := rf.currentLog
	rf.logMu.Unlock()
	if path == "" {
		return 0
	}
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

// CheckpointCandidate reports the current applied and committed index when the
// Raft log is completely idle. It is intentionally conservative: any pending
// disk batch, apply offset, snapshot, or committed-but-undelivered entry makes
// the candidate unavailable.
func (rf *Raft) CheckpointCandidate() (index int, term int32, ok bool) {
	st := rf.CheckpointStatus()
	if !st.Ready() {
		return 0, 0, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.termAt(st.LastIndex)
	if term < 0 {
		return 0, 0, false
	}
	return st.LastIndex, term, true
}

// CheckpointStatus snapshots the conditions that guard empty-log rotation.
func (rf *Raft) CheckpointStatus() CheckpointStatus {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	st := CheckpointStatus{
		LastIndex:        rf.lastIndex(),
		LastApplied:      rf.lastApplied,
		CommitIndex:      rf.commitIndex,
		LastWrittenIndex: rf.lastWrittenIndex,
		PendingOffsets:   len(rf.Offsets),
		PendingBatch:     len(rf.batchLog),
		SnapshotInFlight: rf.snapshotsInFlightLocked(),
	}
	rf.batchMu.Lock()
	st.BatchInFlight = rf.curBatch != nil && len(rf.curBatch.entries) != 0
	rf.batchMu.Unlock()
	return st
}

func (st CheckpointStatus) Ready() bool {
	return st.LastIndex > 0 &&
		st.LastApplied == st.LastIndex &&
		st.CommitIndex == st.LastIndex &&
		st.LastWrittenIndex == st.LastIndex &&
		st.PendingOffsets == 0 &&
		st.PendingBatch == 0 &&
		!st.SnapshotInFlight &&
		!st.BatchInFlight
}

// RotateEmptyLogIfIdle replaces a fully materialized Raft log with a new empty
// log whose base is expected. The caller must ensure the KV state machine is
// durable through expected before invoking this method. Entry indexes and the
// in-memory log history remain unchanged; only the obsolete on-disk prefix is
// removed.
func (rf *Raft) RotateEmptyLogIfIdle(expected int, newLog string) (oldLog string, rotated bool, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	last := rf.lastIndex()
	if rf.killed() || rf.snapshotsInFlightLocked() ||
		last != expected || rf.lastApplied != expected || rf.commitIndex != expected ||
		rf.lastWrittenIndex != expected ||
		len(rf.Offsets) != 0 || len(rf.offsetVersions) != 0 || len(rf.batchLog) != 0 {
		return "", false, nil
	}
	term := rf.termAt(expected)
	if term < 0 {
		return "", false, fmt.Errorf("checkpoint term for index %d is unavailable", expected)
	}

	rf.batchMu.Lock()
	batchPending := rf.curBatch != nil && len(rf.curBatch.entries) != 0
	rf.batchMu.Unlock()
	if batchPending {
		return "", false, nil
	}

	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	if rf.logWriter != nil {
		if err := rf.logWriter.Flush(); err != nil {
			return "", false, fmt.Errorf("flush old Raft log: %w", err)
		}
	}
	if rf.logFile != nil {
		if err := rf.logFile.Sync(); err != nil {
			return "", false, fmt.Errorf("sync old Raft log: %w", err)
		}
	}

	file, err := os.OpenFile(newLog, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return "", false, fmt.Errorf("create empty Raft checkpoint %s: %w", newLog, err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(newLog)
		return "", false, fmt.Errorf("sync empty Raft checkpoint %s: %w", newLog, err)
	}
	if err := file.Close(); err != nil {
		os.Remove(newLog)
		return "", false, fmt.Errorf("close empty Raft checkpoint %s: %w", newLog, err)
	}
	dir, err := os.Open(filepath.Dir(newLog))
	if err != nil {
		os.Remove(newLog)
		return "", false, err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		os.Remove(newLog)
		return "", false, err
	}
	dir.Close()

	oldLog = rf.currentLog
	rf.currentLog = newLog
	if err := rf.openLogFile(newLog); err != nil {
		rf.currentLog = oldLog
		if reopenErr := rf.openLogFile(oldLog); reopenErr != nil {
			return "", false, fmt.Errorf("open checkpoint log: %v; reopen old log: %w", err, reopenErr)
		}
		os.Remove(newLog)
		return "", false, err
	}

	rf.setLog(nil)
	rf.Offsets = nil
	rf.offsetVersions = nil
	rf.batchLog = nil
	rf.lastIncludedIndex = expected
	rf.lastIncludedTerm = term
	rf.fileBaseIndex = expected
	rf.fileBaseTerm = term
	rf.lastWrittenIndex = expected
	rf.lastWrittenTerm = term
	rf.pendingBaseIndex = expected
	rf.pendingBaseTerm = term
	rf.lastApplied = expected
	rf.commitIndex = expected
	rf.shotOffset = expected
	rf.persistedIndex.Store(int64(expected))
	rf.persistHardState()
	return oldLog, true, nil
}
