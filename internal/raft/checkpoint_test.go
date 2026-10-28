package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRotateEmptyLogIfIdleDurablyAdvancesFileBase(t *testing.T) {
	dir := t.TempDir()
	oldLog := filepath.Join(dir, "RaftState.log")
	newLog := filepath.Join(dir, "RaftState.checkpoint.log")
	stateFile := filepath.Join(dir, "raft_state.json")

	w := newLogWriter(t, oldLog, 0)
	writeSampleLog(t, w)
	w.CloseLogFile()

	rf := &Raft{persister: &Persister{}, stateFile: stateFile}
	last, err := rf.RecoverLog([]LogFile{{Path: oldLog, Version: 0}}, 9)
	if err != nil {
		t.Fatal(err)
	}
	if last != 9 {
		t.Fatalf("last index = %d, want 9", last)
	}
	// Mirror server startup: RecoverLog rebuilds state first, then the KV layer
	// attaches the recovered log file as the active writer.
	rf.SetCurrentLogVersioned(oldLog, 0)

	gotOld, rotated, err := rf.RotateEmptyLogIfIdle(9, newLog)
	if err != nil {
		t.Fatal(err)
	}
	if !rotated || gotOld != oldLog {
		t.Fatalf("rotated=%v old=%q, want true / %q", rotated, gotOld, oldLog)
	}
	st, err := os.Stat(newLog)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 0 {
		t.Fatalf("checkpoint log size = %d, want 0", st.Size())
	}
	if rf.currentLog != newLog || rf.fileBaseIndex != 9 || rf.fileBaseTerm != 2 {
		t.Fatalf("active log/base = %s/(%d,%d), want %s/(9,2)",
			rf.currentLog, rf.fileBaseIndex, rf.fileBaseTerm, newLog)
	}
	if len(rf.log) != 0 || len(rf.Offsets) != 0 || rf.lastApplied != 9 {
		t.Fatalf("checkpoint state: log=%d offsets=%d applied=%d",
			len(rf.log), len(rf.Offsets), rf.lastApplied)
	}

	hs, ok, err := loadHardState(stateFile)
	if err != nil || !ok {
		t.Fatalf("loadHardState: ok=%v err=%v", ok, err)
	}
	if hs.BaseIndex != 9 || hs.BaseTerm != 2 {
		t.Fatalf("persisted checkpoint base = (%d,%d), want (9,2)", hs.BaseIndex, hs.BaseTerm)
	}

	// New writes must continue at checkpoint base + 1 in the replacement file.
	rf.AppendToLogFile([]*Entry{putEntry(10, 2, "10", "v10")})
	rf.CloseLogFile()
	fresh := &Raft{
		persister:         &Persister{},
		stateLoaded:       true,
		fileBaseIndex:     9,
		fileBaseTerm:      2,
		lastIncludedIndex: 9,
		lastIncludedTerm:  2,
	}
	recoveredLast, err := fresh.RecoverLog([]LogFile{{Path: newLog, Version: 0}}, 9)
	if err != nil {
		t.Fatalf("recover replacement checkpoint log: %v", err)
	}
	if recoveredLast != 10 || len(fresh.Offsets) != 1 {
		t.Fatalf("replacement log recovery last/offsets = %d/%d, want 10/1",
			recoveredLast, len(fresh.Offsets))
	}
}

func TestRecoverLogAcceptsRetiredFileAtCheckpointBase(t *testing.T) {
	dir := t.TempDir()
	oldLog := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, oldLog, 0)
	writeSampleLog(t, w)
	w.CloseLogFile()

	rf := &Raft{
		persister:         &Persister{},
		stateLoaded:       true,
		fileBaseIndex:     9,
		fileBaseTerm:      2,
		lastIncludedIndex: 9,
		lastIncludedTerm:  2,
	}
	last, err := rf.RecoverLog([]LogFile{{Path: oldLog, Version: 0}}, 9)
	if err != nil {
		t.Fatalf("retired log should be accepted after checkpoint: %v", err)
	}
	if last != 9 || len(rf.log) != 0 || rf.lastApplied != 9 {
		t.Fatalf("recovered last/log/applied = %d/%d/%d, want 9/0/9",
			last, len(rf.log), rf.lastApplied)
	}
}
