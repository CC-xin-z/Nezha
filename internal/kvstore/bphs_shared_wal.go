package kvstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	bphsSharedWALName        = "bphs.wal"
	bphsSharedRecordHeader   = 48
	bphsSharedRecordMagic    = uint16(0x4257) // "BW"
	bphsSharedRecordVersion  = byte(1)
	bphsSharedWALBufferBytes = 256 << 10
)

type bphsSharedWALRecord struct {
	Partition int
	Record    bphsRecord
}

// bphsSharedWAL is the node-wide durability path for one apply batch. It
// contains records from all touched partitions and is fsynced once per batch.
// Per-partition record logs remain the checkpoint/recovery representation and
// are synced asynchronously after the shared WAL has made the batch durable.
type bphsSharedWAL struct {
	file   *os.File
	writer *bufio.Writer
	path   string
	size   int64
}

func openBPHSSharedWAL(path string) (*bphsSharedWAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &bphsSharedWAL{
		file:   file,
		writer: bufio.NewWriterSize(file, bphsSharedWALBufferBytes),
		path:   path,
		size:   st.Size(),
	}, nil
}

func (w *bphsSharedWAL) Path() string {
	if w == nil {
		return ""
	}
	return w.path
}

func (w *bphsSharedWAL) Size() int64 {
	if w == nil {
		return 0
	}
	return w.size
}

func encodeBPHSSharedWALRecord(dst []byte, partition int, rec bphsRecord) ([]byte, error) {
	key := []byte(rec.Key)
	value := []byte(nil)
	switch rec.Kind {
	case bphsInlineRecord:
		value = []byte(rec.Value)
	case bphsBlobRecord:
		if rec.Value != "" {
			return nil, fmt.Errorf("blob record %q unexpectedly embeds a value", rec.Key)
		}
	default:
		return nil, fmt.Errorf("unknown BPHS record kind %d", rec.Kind)
	}
	if partition < 0 || partition > int(^uint32(0)) {
		return nil, fmt.Errorf("invalid BPHS partition %d", partition)
	}
	total := bphsSharedRecordHeader + len(key) + len(value)
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	for i := range dst {
		dst[i] = 0
	}
	binary.LittleEndian.PutUint16(dst[0:2], bphsSharedRecordMagic)
	dst[2] = bphsSharedRecordVersion
	dst[3] = byte(rec.Kind)
	binary.LittleEndian.PutUint32(dst[4:8], uint32(partition))
	binary.LittleEndian.PutUint32(dst[8:12], rec.Version)
	binary.LittleEndian.PutUint32(dst[12:16], uint32(len(key)))
	binary.LittleEndian.PutUint32(dst[16:20], uint32(len(value)))
	binary.LittleEndian.PutUint64(dst[20:28], rec.Blob.Generation)
	binary.LittleEndian.PutUint64(dst[28:36], uint64(rec.Blob.Offset))
	binary.LittleEndian.PutUint64(dst[36:44], uint64(rec.Blob.Length))
	copy(dst[bphsSharedRecordHeader:], key)
	copy(dst[bphsSharedRecordHeader+len(key):], value)
	crc := crc32.NewIEEE()
	_, _ = crc.Write(dst[:44])
	_, _ = crc.Write(key)
	_, _ = crc.Write(value)
	binary.LittleEndian.PutUint32(dst[44:48], crc.Sum32())
	return dst, nil
}

func (w *bphsSharedWAL) AppendBatch(records []bphsSharedWALRecord) (int64, error) {
	if w == nil {
		return 0, fmt.Errorf("nil BPHS shared WAL")
	}
	if len(records) == 0 {
		return 0, nil
	}
	start := w.size
	var scratch []byte
	for _, item := range records {
		encoded, err := encodeBPHSSharedWALRecord(scratch, item.Partition, item.Record)
		if err != nil {
			return 0, err
		}
		scratch = encoded
		if err := writeFull(w.writer, encoded); err != nil {
			return 0, err
		}
		w.size += int64(len(encoded))
	}
	return w.size - start, nil
}

func (w *bphsSharedWAL) Sync() error {
	if w == nil {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *bphsSharedWAL) Reset() error {
	if w == nil {
		return nil
	}
	if err := w.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.writer = bufio.NewWriterSize(file, bphsSharedWALBufferBytes)
	w.size = 0
	return nil
}

func (w *bphsSharedWAL) ScanAll() ([]bphsSharedWALRecord, error) {
	if w == nil {
		return nil, nil
	}
	if err := w.writer.Flush(); err != nil {
		return nil, err
	}
	var out []bphsSharedWALRecord
	var offset int64
	for offset < w.size {
		if w.size-offset < bphsSharedRecordHeader {
			if err := w.truncateTornTail(offset); err != nil {
				return nil, err
			}
			break
		}
		var header [bphsSharedRecordHeader]byte
		if _, err := w.file.ReadAt(header[:], offset); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint16(header[0:2]) != bphsSharedRecordMagic ||
			header[2] != bphsSharedRecordVersion {
			return nil, fmt.Errorf("invalid BPHS shared WAL record at %d", offset)
		}
		kind := bphsRecordKind(header[3])
		partition := int(binary.LittleEndian.Uint32(header[4:8]))
		version := binary.LittleEndian.Uint32(header[8:12])
		keyLen := int64(binary.LittleEndian.Uint32(header[12:16]))
		valueLen := int64(binary.LittleEndian.Uint32(header[16:20]))
		total := int64(bphsSharedRecordHeader) + keyLen + valueLen
		if total < bphsSharedRecordHeader || offset+total > w.size {
			if err := w.truncateTornTail(offset); err != nil {
				return nil, err
			}
			break
		}
		key := make([]byte, int(keyLen))
		value := make([]byte, int(valueLen))
		if keyLen > 0 {
			if _, err := w.file.ReadAt(key, offset+bphsSharedRecordHeader); err != nil {
				return nil, err
			}
		}
		if valueLen > 0 {
			if _, err := w.file.ReadAt(value, offset+bphsSharedRecordHeader+keyLen); err != nil {
				return nil, err
			}
		}
		crc := crc32.NewIEEE()
		_, _ = crc.Write(header[:44])
		_, _ = crc.Write(key)
		_, _ = crc.Write(value)
		if got, want := crc.Sum32(), binary.LittleEndian.Uint32(header[44:48]); got != want {
			if offset+total == w.size {
				if err := w.truncateTornTail(offset); err != nil {
					return nil, err
				}
				break
			}
			return nil, fmt.Errorf("BPHS shared WAL checksum mismatch at %d", offset)
		}
		rec := bphsRecord{
			Key:     string(key),
			Version: version,
			Kind:    kind,
			Blob: bphsBlobRef{
				Generation: binary.LittleEndian.Uint64(header[20:28]),
				Offset:     int64(binary.LittleEndian.Uint64(header[28:36])),
				Length:     int64(binary.LittleEndian.Uint64(header[36:44])),
			},
		}
		switch kind {
		case bphsInlineRecord:
			if rec.Blob != (bphsBlobRef{}) {
				return nil, fmt.Errorf("inline BPHS shared WAL record at %d has blob ref", offset)
			}
			rec.Value = string(value)
		case bphsBlobRecord:
			if valueLen != 0 {
				return nil, fmt.Errorf("blob BPHS shared WAL record at %d embeds a value", offset)
			}
		default:
			return nil, fmt.Errorf("unknown BPHS shared WAL record kind %d at %d", kind, offset)
		}
		out = append(out, bphsSharedWALRecord{Partition: partition, Record: rec})
		offset += total
	}
	return out, nil
}

func (w *bphsSharedWAL) truncateTornTail(offset int64) error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	w.size = offset
	w.writer.Reset(w.file)
	return nil
}

func (w *bphsSharedWAL) Close() error {
	if w == nil {
		return nil
	}
	if w.file == nil {
		return nil
	}
	var first error
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			first = err
		}
		w.writer = nil
	}
	if err := w.file.Close(); err != nil && first == nil {
		first = err
	}
	w.file = nil
	return first
}
