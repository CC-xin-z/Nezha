package raft

import (
	"bufio"
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

func writeRaftTailFile(path string, entries []*Entry) ([]int64, int64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, 0, err
	}
	cleanup := func() {
		file.Close()
		os.Remove(path)
	}
	writer := bufio.NewWriterSize(file, 1<<20)
	offsets := make([]int64, len(entries))
	var offset int64
	for i, entry := range entries {
		data := encodeRaftLogEntry(entry)
		if _, err := writer.Write(data); err != nil {
			cleanup()
			return nil, 0, err
		}
		offsets[i] = offset
		offset += int64(len(data))
	}
	if err := writer.Flush(); err != nil {
		cleanup()
		return nil, 0, err
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return nil, 0, err
	}
	if err := file.Close(); err != nil {
		os.Remove(path)
		return nil, 0, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		os.Remove(path)
		return nil, 0, err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		os.Remove(path)
		return nil, 0, err
	}
	dir.Close()
	return offsets, offset, nil
}

// RotateLogAtCut retires the fully durable prefix while preserving every
// in-memory entry after expected in the replacement log. local Raft writes are
// excluded by rf.mu only for the bounded tail-copy window.
func (rf *Raft) RotateLogAtCut(expected int, newLog string, maxTailBytes int64) (
	oldLog string, rotated bool, err error,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.snapshotsInFlightLocked() ||
		expected <= rf.lastIncludedIndex || expected > rf.lastApplied ||
		expected > rf.lastWrittenIndex || rf.lastWrittenIndex != rf.lastIndex() ||
		len(rf.batchLog) != 0 {
		return "", false, nil
	}
	rf.batchMu.Lock()
	batchPending := rf.curBatch != nil && len(rf.curBatch.entries) != 0
	rf.batchMu.Unlock()
	if batchPending {
		return "", false, nil
	}

	term := rf.termAt(expected)
	if term < 0 {
		return "", false, fmt.Errorf("checkpoint term for index %d is unavailable", expected)
	}
	last := rf.lastIndex()
	lastTerm := rf.lastWrittenTerm
	lastApplied := rf.lastApplied
	startPos := rf.index2LogPos(expected + 1)
	if startPos < 0 || startPos > len(rf.log) {
		return "", false, fmt.Errorf("checkpoint cut %d is outside in-memory log", expected)
	}
	tailLog := rf.log[startPos:]
	var tailBytes int64
	for _, entry := range tailLog {
		tailBytes += logEntryBytes(entry)
	}
	if maxTailBytes > 0 && tailBytes > maxTailBytes {
		return "", false, nil
	}
	tailEntries := make([]*Entry, 0, len(tailLog))
	for i, entry := range tailLog {
		command := entry.GetCommand()
		if command == nil {
			return "", false, fmt.Errorf("checkpoint tail contains nil command at index %d", expected+1+i)
		}
		tailEntries = append(tailEntries, &Entry{
			Index:       uint32(expected + 1 + i),
			CurrentTerm: uint32(entry.Term),
			Key:         command.Key,
			Value:       command.Value,
			NoOp:        command.OpType == "TermLog",
		})
	}
	consumed := lastApplied - expected
	if consumed < 0 || consumed > len(tailEntries) {
		return "", false, fmt.Errorf("checkpoint tail offset window invalid: applied=%d cut=%d entries=%d",
			lastApplied, expected, len(tailEntries))
	}

	offsets, _, err := writeRaftTailFile(newLog, tailEntries)
	if err != nil {
		return "", false, err
	}

	rf.logMu.Lock()
	if rf.logWriter != nil {
		if err := rf.logWriter.Flush(); err != nil {
			rf.logMu.Unlock()
			os.Remove(newLog)
			return "", false, err
		}
	}
	if rf.logFile != nil {
		if err := rf.logFile.Sync(); err != nil {
			rf.logMu.Unlock()
			os.Remove(newLog)
			return "", false, err
		}
	}
	oldLog = rf.currentLog
	rf.currentLog = newLog
	if err := rf.openLogFile(newLog); err != nil {
		rf.currentLog = oldLog
		reopenErr := rf.openLogFile(oldLog)
		rf.logMu.Unlock()
		os.Remove(newLog)
		if reopenErr != nil {
			return "", false, fmt.Errorf("open checkpoint log: %v; reopen old log: %w", err, reopenErr)
		}
		return "", false, err
	}
	rf.logMu.Unlock()

	rf.dropLogPrefix(expected - rf.lastIncludedIndex)
	rf.lastIncludedIndex = expected
	rf.lastIncludedTerm = term
	rf.Offsets = append([]int64(nil), offsets[consumed:]...)
	rf.offsetVersions = make([]int32, len(rf.Offsets))
	for i := range rf.offsetVersions {
		rf.offsetVersions[i] = rf.logVersion
	}
	rf.shotOffset = lastApplied
	rf.lastWrittenIndex = last
	rf.lastWrittenTerm = lastTerm
	rf.fileBaseIndex = expected
	rf.fileBaseTerm = term
	rf.pendingBaseIndex = expected
	rf.pendingBaseTerm = term
	rf.persistedIndex.Store(int64(last))
	rf.persistHardState()
	return oldLog, true, nil
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
