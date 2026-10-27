package kvstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const partitionRecordHeaderBytes = 44

// partitionRecordLog 是单分区未压缩记录日志。小值在这里保存完整 key+value；大值
// 只保存 key 与本地 BlobRef，真实 value 在 partitionVLog。
//
// 它是 BPHS 覆盖写的持久化来源：MemTable 只是内存索引，崩溃后 ScanAll 可以重建。
// 记录一旦合法就只追加；覆盖版本在局部 Compaction 时删除。
type partitionRecordLog struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	path   string
	size   int64
}

func openPartitionRecordLog(path string) (*partitionRecordLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &partitionRecordLog{
		file: file, writer: bufio.NewWriterSize(file, 256<<10), path: path, size: st.Size(),
	}, nil
}

func (l *partitionRecordLog) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

func (l *partitionRecordLog) Size() int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

func (l *partitionRecordLog) Append(rec bphsRecord) (int64, error) {
	if l == nil {
		return 0, fmt.Errorf("nil partition record log")
	}
	value := []byte(nil)
	if rec.Kind == bphsInlineRecord {
		value = []byte(rec.Value)
	}
	key := []byte(rec.Key)

	var header [partitionRecordHeaderBytes]byte
	header[0] = byte(rec.Kind)
	binary.LittleEndian.PutUint32(header[4:8], rec.Version)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(value)))
	binary.LittleEndian.PutUint64(header[16:24], rec.Blob.Generation)
	binary.LittleEndian.PutUint64(header[24:32], uint64(rec.Blob.Offset))
	binary.LittleEndian.PutUint64(header[32:40], uint64(rec.Blob.Length))
	crc := crc32.NewIEEE()
	_, _ = crc.Write(header[:40])
	_, _ = crc.Write(key)
	_, _ = crc.Write(value)
	binary.LittleEndian.PutUint32(header[40:44], crc.Sum32())

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return 0, os.ErrClosed
	}
	offset := l.size
	if err := writeFull(l.writer, header[:]); err != nil {
		return 0, err
	}
	if err := writeFull(l.writer, key); err != nil {
		return 0, err
	}
	if err := writeFull(l.writer, value); err != nil {
		return 0, err
	}
	l.size += int64(len(header) + len(key) + len(value))
	return offset, nil
}

func (l *partitionRecordLog) Sync() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return os.ErrClosed
	}
	if err := l.writer.Flush(); err != nil {
		return err
	}
	return l.file.Sync()
}

// Reset drops records that have already been made durable in an L0 or base.
// The caller must hold the partition lock, ensure the replacement data is
// fsynced, and only then call Reset.
func (l *partitionRecordLog) Reset() error {
	if l == nil {
		return fmt.Errorf("nil partition record log")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.writer = nil
		if err := l.file.Close(); err != nil {
			return err
		}
		l.file = nil
	}
	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.file = file
	l.writer = bufio.NewWriterSize(file, 256<<10)
	l.size = 0
	return nil
}

func (l *partitionRecordLog) ReadAt(offset int64) (bphsRecord, error) {
	if l == nil {
		return bphsRecord{}, fmt.Errorf("nil partition record log")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return bphsRecord{}, os.ErrClosed
	}
	if err := l.writer.Flush(); err != nil {
		return bphsRecord{}, err
	}
	return l.readAtLocked(offset)
}

func (l *partitionRecordLog) readAtLocked(offset int64) (bphsRecord, error) {
	if offset < 0 || offset > l.size-partitionRecordHeaderBytes {
		return bphsRecord{}, fmt.Errorf("invalid partition record offset %d (size %d)", offset, l.size)
	}
	var header [partitionRecordHeaderBytes]byte
	if _, err := l.file.ReadAt(header[:], offset); err != nil {
		return bphsRecord{}, err
	}
	kind := bphsRecordKind(header[0])
	version := binary.LittleEndian.Uint32(header[4:8])
	keyLen := int64(binary.LittleEndian.Uint32(header[8:12]))
	valueLen := int64(binary.LittleEndian.Uint32(header[12:16]))
	total := int64(partitionRecordHeaderBytes) + keyLen + valueLen
	if total < partitionRecordHeaderBytes || offset+total > l.size {
		return bphsRecord{}, fmt.Errorf("partition record at %d extends past log size %d", offset, l.size)
	}

	key := make([]byte, int(keyLen))
	value := make([]byte, int(valueLen))
	if keyLen > 0 {
		if _, err := l.file.ReadAt(key, offset+partitionRecordHeaderBytes); err != nil {
			return bphsRecord{}, err
		}
	}
	if valueLen > 0 {
		if _, err := l.file.ReadAt(value, offset+partitionRecordHeaderBytes+keyLen); err != nil {
			return bphsRecord{}, err
		}
	}
	crc := crc32.NewIEEE()
	_, _ = crc.Write(header[:40])
	_, _ = crc.Write(key)
	_, _ = crc.Write(value)
	if want := binary.LittleEndian.Uint32(header[40:44]); crc.Sum32() != want {
		return bphsRecord{}, fmt.Errorf("partition record checksum at %d mismatch", offset)
	}

	rec := bphsRecord{
		Key:       string(key),
		Version:   version,
		Kind:      kind,
		Value:     string(value),
		LogOffset: offset,
		Blob: bphsBlobRef{
			Generation: binary.LittleEndian.Uint64(header[16:24]),
			Offset:     int64(binary.LittleEndian.Uint64(header[24:32])),
			Length:     int64(binary.LittleEndian.Uint64(header[32:40])),
		},
	}
	switch kind {
	case bphsInlineRecord:
		if rec.Blob != (bphsBlobRef{}) {
			return bphsRecord{}, fmt.Errorf("inline record at %d has blob ref", offset)
		}
	case bphsBlobRecord:
		if valueLen != 0 {
			return bphsRecord{}, fmt.Errorf("blob record at %d embeds %d value bytes", offset, valueLen)
		}
	default:
		return bphsRecord{}, fmt.Errorf("unknown record kind %d at %d", kind, offset)
	}
	return rec, nil
}

func (l *partitionRecordLog) ScanAll() ([]bphsRecord, error) {
	if l == nil {
		return nil, fmt.Errorf("nil partition record log")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil, os.ErrClosed
	}
	if err := l.writer.Flush(); err != nil {
		return nil, err
	}
	var records []bphsRecord
	for offset := int64(0); offset < l.size; {
		rec, err := l.readAtLocked(offset)
		if err != nil {
			return records, err
		}
		records = append(records, rec)
		offset += int64(partitionRecordHeaderBytes + len(rec.Key))
		if rec.Kind == bphsInlineRecord {
			offset += int64(len(rec.Value))
		}
	}
	return records, nil
}

func (l *partitionRecordLog) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	var first error
	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			first = err
		}
		l.writer = nil
	}
	if err := l.file.Close(); err != nil && first == nil {
		first = err
	}
	l.file = nil
	return first
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
