package kvstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const partitionVLogHeaderBytes = 16

// partitionVLog 是一个 Key Range 专属的大值追加文件。
//
// 每次追加先写 [payload_len, payload_crc32, reserved] 的 16B 头，再写原始 value。
// bphsBlobRef.Offset 指向记录头，Length 是 payload 长度；读取时校验长度和 CRC，
// 把截断或错位变成显式错误，而不是静默返回错误 value。
type partitionVLog struct {
	mu   sync.Mutex
	file *os.File
	path string
	size int64
}

func openPartitionVLog(path string) (*partitionVLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &partitionVLog{file: file, path: path, size: st.Size()}, nil
}

func (v *partitionVLog) Path() string {
	if v == nil {
		return ""
	}
	return v.path
}

func (v *partitionVLog) Size() int64 {
	if v == nil {
		return 0
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.size
}

// Append 追加一个 value。调用方在推进 applied index 前必须调用 Sync，或者确保
// Raft 日志能在重启时重放这次 apply。
func (v *partitionVLog) Append(value []byte) (bphsBlobRef, error) {
	if v == nil {
		return bphsBlobRef{}, fmt.Errorf("nil partition vlog")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.file == nil {
		return bphsBlobRef{}, os.ErrClosed
	}

	var header [partitionVLogHeaderBytes]byte
	binary.LittleEndian.PutUint64(header[0:8], uint64(len(value)))
	binary.LittleEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(value))
	offset := v.size
	n, err := v.file.Write(header[:])
	if err != nil || n != len(header) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return bphsBlobRef{}, err
	}
	if len(value) > 0 {
		n, err := v.file.Write(value)
		if err != nil || n != len(value) {
			if err == nil {
				err = io.ErrShortWrite
			}
			return bphsBlobRef{}, err
		}
	}
	v.size += int64(len(header) + len(value))
	return bphsBlobRef{Offset: offset, Length: int64(len(value))}, nil
}

func (v *partitionVLog) Sync() error {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.file == nil {
		return os.ErrClosed
	}
	return v.file.Sync()
}

func (v *partitionVLog) Read(ref bphsBlobRef) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("nil partition vlog")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.file == nil {
		return nil, os.ErrClosed
	}
	if ref.Offset < 0 || ref.Length < 0 ||
		ref.Offset > v.size-partitionVLogHeaderBytes {
		return nil, fmt.Errorf("invalid partition blob ref: %+v", ref)
	}

	var header [partitionVLogHeaderBytes]byte
	if _, err := v.file.ReadAt(header[:], ref.Offset); err != nil {
		return nil, err
	}
	length := int64(binary.LittleEndian.Uint64(header[0:8]))
	wantCRC := binary.LittleEndian.Uint32(header[8:12])
	if length != ref.Length {
		return nil, fmt.Errorf("partition blob length %d, ref says %d", length, ref.Length)
	}
	value := make([]byte, int(length))
	if length > 0 {
		n, err := v.file.ReadAt(value, ref.Offset+partitionVLogHeaderBytes)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if int64(n) != length {
			return nil, io.ErrUnexpectedEOF
		}
	}
	if got := crc32.ChecksumIEEE(value); got != wantCRC {
		return nil, fmt.Errorf("partition blob checksum %08x, want %08x", got, wantCRC)
	}
	return value, nil
}

func (v *partitionVLog) Close() error {
	if v == nil || v.file == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	err := v.file.Close()
	v.file = nil
	return err
}
