package kvstore

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestPartitionMemtableKeepsNewestVersion(t *testing.T) {
	mt := newPartitionMemtable(1 << 20)
	key := "0000000001"

	if !mt.Put(bphsRecord{Key: key, Version: 10, Kind: bphsInlineRecord, Value: "old"}) {
		t.Fatal("首次写入应成功")
	}
	if !mt.Put(bphsRecord{Key: key, Version: 20, Kind: bphsInlineRecord, Value: "new"}) {
		t.Fatal("新版本应替换旧版本")
	}
	if mt.Put(bphsRecord{Key: key, Version: 15, Kind: bphsInlineRecord, Value: "stale"}) {
		t.Fatal("旧版本在恢复重放时不应覆盖新版本")
	}

	got, ok := mt.Get(key)
	if !ok || got.Version != 20 || got.Value != "new" {
		t.Fatalf("memtable = %+v,%v; want version=20 value=new", got, ok)
	}
	if mt.Len() != 1 {
		t.Fatalf("memtable len = %d, want 1", mt.Len())
	}
}

func TestPartitionMemtableSortedCacheInvalidatesOnWrite(t *testing.T) {
	mt := newPartitionMemtable(1 << 20)
	mt.Put(bphsRecord{Key: "0000000002", Version: 1, Kind: bphsInlineRecord, Value: "b"})
	mt.Put(bphsRecord{Key: "0000000001", Version: 2, Kind: bphsInlineRecord, Value: "a"})

	first := mt.Sorted()
	second := mt.Sorted()
	if len(first) != 2 || len(second) != 2 || first[0].Key != "0000000001" {
		t.Fatalf("sorted records = %+v, %+v", first, second)
	}
	if &first[0] != &second[0] {
		t.Fatal("unchanged memtable should reuse its sorted snapshot")
	}

	mt.Put(bphsRecord{Key: "0000000000", Version: 3, Kind: bphsInlineRecord, Value: "z"})
	updated := mt.Sorted()
	if len(updated) != 3 || updated[0].Key != "0000000000" {
		t.Fatalf("sorted cache was not invalidated: %+v", updated)
	}
}

func TestFlushPartitionMemtableRoundTrip(t *testing.T) {
	mt := newPartitionMemtable(1 << 20)
	const count = 257
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%010d", i)
		rec := bphsRecord{
			Key:     key,
			Version: uint32(i + 1),
			Kind:    bphsInlineRecord,
			Value:   fmt.Sprintf("value-%06d", i),
		}
		if i == count-1 {
			rec.Kind = bphsBlobRecord
			rec.Blob = bphsBlobRef{Generation: 7, Offset: 1234, Length: 1 << 20}
		}
		mt.Put(rec)
	}

	path := filepath.Join(t.TempDir(), "p0_l0_1.sst")
	kvs := &KVServer{indexBlockBytes: 512}
	index, err := kvs.flushPartitionMemtable(mt, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if index == nil {
		t.Fatal("flush 结果为空")
	}
	defer index.closePool()

	if index.Lo != "0000000000" || index.Hi != fmt.Sprintf("%010d", count-1) {
		t.Fatalf("L0 range = [%s,%s]", index.Lo, index.Hi)
	}
	if index.Entries != count {
		t.Fatalf("L0 entries = %d, want %d", index.Entries, count)
	}

	for _, i := range []int{0, 1, 100, count - 1} {
		key := fmt.Sprintf("%010d", i)
		entry, err := kvs.lookupInSortedFile(index, key)
		if err != nil {
			t.Fatalf("lookup(%s): %v", key, err)
		}
		rec, err := entryToBPHSRecord(entry)
		if err != nil {
			t.Fatalf("decode(%s): %v", key, err)
		}
		if rec.Version != uint32(i+1) {
			t.Fatalf("decode(%s) version=%d, want %d", key, rec.Version, i+1)
		}
		if i == count-1 {
			if rec.Kind != bphsBlobRecord || rec.Blob != (bphsBlobRef{Generation: 7, Offset: 1234, Length: 1 << 20}) {
				t.Fatalf("blob record = %+v", rec)
			}
		} else if rec.Kind != bphsInlineRecord || rec.Value != fmt.Sprintf("value-%06d", i) {
			t.Fatalf("inline record = %+v", rec)
		}
	}
}

func TestBPHSValueRoundTrip(t *testing.T) {
	inline := bphsRecord{
		Key: "0000000001", Version: 11, Kind: bphsInlineRecord, Value: "small",
	}
	entry, err := bphsRecordEntry(inline)
	if err != nil {
		t.Fatal(err)
	}
	got, err := entryToBPHSRecord(entry)
	if err != nil {
		t.Fatal(err)
	}
	if got != inline {
		t.Fatalf("inline round trip = %+v, want %+v", got, inline)
	}

	blob := bphsRecord{
		Key: "0000000002", Version: 12, Kind: bphsBlobRecord,
		Blob: bphsBlobRef{Generation: 9, Offset: 4567, Length: 8901},
	}
	entry, err = bphsRecordEntry(blob)
	if err != nil {
		t.Fatal(err)
	}
	got, err = entryToBPHSRecord(entry)
	if err != nil {
		t.Fatal(err)
	}
	if got != blob {
		t.Fatalf("blob round trip = %+v, want %+v", got, blob)
	}
}
