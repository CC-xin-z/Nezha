package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBPHSSharedWALRoundTripAndReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), bphsSharedWALName)
	wal, err := openBPHSSharedWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []bphsSharedWALRecord{
		{
			Partition: 2,
			Record: bphsRecord{
				Key: "0000000002", Version: 11, Kind: bphsInlineRecord, Value: "small",
			},
		},
		{
			Partition: 7,
			Record: bphsRecord{
				Key: "0000000007", Version: 12, Kind: bphsBlobRecord,
				Blob: bphsBlobRef{Generation: 3, Offset: 456, Length: 789},
			},
		},
	}
	if _, err := wal.AppendBatch(want); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	got, err := wal.ScanAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("WAL records = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("WAL record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if err := wal.Reset(); err != nil {
		t.Fatal(err)
	}
	if wal.Size() != 0 {
		t.Fatalf("WAL size after reset = %d, want 0", wal.Size())
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBPHSSharedWALTruncatesTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), bphsSharedWALName)
	wal, err := openBPHSSharedWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.AppendBatch([]bphsSharedWALRecord{{
		Partition: 1,
		Record:    bphsRecord{Key: "k", Version: 3, Kind: bphsInlineRecord, Value: "v"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	wal, err = openBPHSSharedWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	got, err := wal.ScanAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Partition != 1 || got[0].Record.Value != "v" {
		t.Fatalf("records after torn-tail truncation = %+v", got)
	}
}
