package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotWorkDirsAreIndependent(t *testing.T) {
	kvs := &KVServer{dataDir: t.TempDir()}

	first, err := kvs.createSnapshotWorkDir()
	if err != nil {
		t.Fatal(err)
	}
	second, err := kvs.createSnapshotWorkDir()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("两次 Snapshot 复用了同一个目录 %s", first)
	}

	firstSST := filepath.Join(first, snapshotStoreSSTName)
	secondSST := filepath.Join(second, snapshotStoreSSTName)
	if err := os.WriteFile(firstSST, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondSST, []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 模拟第一份快照传输结束后的 release。它只能删除自己的目录，不能把
	// 第二份仍在传输的 store.sst 一起带走。
	if err := os.RemoveAll(first); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(secondSST)
	if err != nil {
		t.Fatalf("第一份 Snapshot release 删除了第二份文件: %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("第二份 Snapshot 内容 = %q, want second", data)
	}
}

func TestClearSnapshotWorkRemovesStaleTrees(t *testing.T) {
	kvs := &KVServer{dataDir: t.TempDir()}
	dir, err := kvs.createSnapshotWorkDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "index"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index", "stale.idx"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	kvs.clearSnapshotWork()
	if _, err := os.Stat(kvs.snapshotWorkPath()); !os.IsNotExist(err) {
		t.Fatalf("启动清理后 Snapshot 根目录仍存在: %v", err)
	}
}

func TestCheckSnapshotFilesSkipsRetiredLegacyPartitions(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "p000000_base.sst")
	manifestPath := filepath.Join(dir, "p000000.manifest.json")
	logPath := filepath.Join(dir, "RaftState.log")
	if err := os.WriteFile(basePath, []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("log"), 0o644); err != nil {
		t.Fatal(err)
	}
	sm := snapshotManifest{
		LegacyPartitionsRetired: true,
		CurrentLog:              filepath.Base(logPath),
		Partitions: []partitionMeta{{
			Path: "RaftState_sorted_1.p0",
			Size: 123,
		}},
		BPHS: []bphsSnapshotPartition{{
			Index:         0,
			BaseFile:      filepath.Base(basePath),
			BaseBytes:     4,
			ManifestFile:  filepath.Base(manifestPath),
			ManifestBytes: 2,
		}},
	}
	byName := map[string]string{
		filepath.Base(basePath):     basePath,
		filepath.Base(manifestPath): manifestPath,
		filepath.Base(logPath):      logPath,
	}
	if err := checkSnapshotFiles(sm, byName); err != nil {
		t.Fatalf("metadata-only snapshot rejected missing legacy partition: %v", err)
	}

	sm.LegacyPartitionsRetired = false
	if err := checkSnapshotFiles(sm, byName); err == nil {
		t.Fatal("non-retired snapshot accepted a missing legacy partition")
	}
}
