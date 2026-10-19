package kvstore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPartitionVLogAppendReadAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p1.vlog")
	vlog, err := openPartitionVLog(path)
	if err != nil {
		t.Fatal(err)
	}

	values := [][]byte{
		bytes.Repeat([]byte("a"), 64),
		bytes.Repeat([]byte("b"), 1<<20),
		[]byte("tail"),
	}
	refs := make([]bphsBlobRef, 0, len(values))
	for _, value := range values {
		ref, err := vlog.Append(value)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	if err := vlog.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := vlog.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openPartitionVLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i, ref := range refs {
		got, err := reopened.Read(ref)
		if err != nil {
			t.Fatalf("read ref %d: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("value %d mismatch: got %dB want %dB", i, len(got), len(values[i]))
		}
	}
}

func TestPartitionVLogDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p1.vlog")
	vlog, err := openPartitionVLog(path)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := vlog.Append([]byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := vlog.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := vlog.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("X"), ref.Offset+partitionVLogHeaderBytes); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openPartitionVLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(ref); err == nil {
		t.Fatal("corrupted payload should fail checksum validation")
	}
}
