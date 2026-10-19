package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPartitionRecordLogRoundTripAndScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p0.records")
	log, err := openPartitionRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []bphsRecord{
		{Key: "0000000001", Version: 1, Kind: bphsInlineRecord, Value: "small"},
		{Key: "0000000002", Version: 2, Kind: bphsBlobRecord,
			Blob: bphsBlobRef{Generation: 3, Offset: 4, Length: 5}},
	}
	offsets := make([]int64, len(want))
	for i, rec := range want {
		offsets[i], err = log.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	for i := range want {
		got, err := log.ReadAt(offsets[i])
		if err != nil {
			t.Fatal(err)
		}
		got.LogOffset = 0
		if got != want[i] {
			t.Fatalf("record %d = %+v, want %+v", i, got, want[i])
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openPartitionRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.ScanAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("scan records = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].Key || got[i].Version != want[i].Version ||
			got[i].Kind != want[i].Kind || got[i].Value != want[i].Value ||
			got[i].Blob != want[i].Blob || got[i].LogOffset != offsets[i] {
			t.Fatalf("scan record %d = %+v, want %+v at offset %d", i, got[i], want[i], offsets[i])
		}
	}
}

func TestPartitionRecordLogDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p0.records")
	log, err := openPartitionRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := log.Append(bphsRecord{
		Key: "0000000001", Version: 1, Kind: bphsInlineRecord, Value: "payload",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("X"), offset+partitionRecordHeaderBytes+1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openPartitionRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.ReadAt(offset); err == nil {
		t.Fatal("corrupted record should fail checksum validation")
	}
}
