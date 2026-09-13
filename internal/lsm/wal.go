package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const walMagic = uint32(0x4c534d57) // "LSMW"

// WAL is an append-only durability log. Each record is:
//
//	crc32 | payloadLen | type | seq | klen | vlen | key | value
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	dir  string
	num  uint64
	sync bool
}

func walPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.log", num))
}

func OpenWAL(dir string, num uint64, sync bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := walPath(dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, dir: dir, num: num, sync: sync}, nil
}

func (w *WAL) Num() uint64 { return w.num }

func (w *WAL) Append(rec Record) error {
	payload := encodeWALPayload(rec)
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(hdr[0:4], crc)

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(hdr); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

func (w *WAL) Remove() error {
	_ = w.Close()
	return os.Remove(walPath(w.dir, w.num))
}

func encodeWALPayload(rec Record) []byte {
	klen := len(rec.Key)
	vlen := len(rec.Value)
	buf := make([]byte, 1+8+4+4+klen+vlen)
	buf[0] = byte(rec.Type)
	binary.LittleEndian.PutUint64(buf[1:9], rec.Seq)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(klen))
	binary.LittleEndian.PutUint32(buf[13:17], uint32(vlen))
	copy(buf[17:17+klen], rec.Key)
	copy(buf[17+klen:], rec.Value)
	return buf
}

func decodeWALPayload(p []byte) (Record, error) {
	if len(p) < 17 {
		return Record{}, io.ErrUnexpectedEOF
	}
	rec := Record{
		Type: RecordType(p[0]),
		Seq:  binary.LittleEndian.Uint64(p[1:9]),
	}
	klen := int(binary.LittleEndian.Uint32(p[9:13]))
	vlen := int(binary.LittleEndian.Uint32(p[13:17]))
	if 17+klen+vlen != len(p) {
		return Record{}, fmt.Errorf("wal: bad payload size")
	}
	rec.Key = cloneBytes(p[17 : 17+klen])
	if vlen > 0 {
		rec.Value = cloneBytes(p[17+klen : 17+klen+vlen])
	}
	return rec, nil
}

// ReplayWAL walks every .log file in dir in file-number order.
func ReplayWAL(dir string, fn func(Record) error) (maxNum uint64, err error) {
	nums, err := listWALNums(dir)
	if err != nil {
		return 0, err
	}
	for _, num := range nums {
		if num > maxNum {
			maxNum = num
		}
		if err := replayOne(walPath(dir, num), fn); err != nil {
			return maxNum, fmt.Errorf("wal %d: %w", num, err)
		}
	}
	return maxNum, nil
}

func replayOne(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				// torn write at the end of the file — ignore
				return nil
			}
			return err
		}
		crcWant := binary.LittleEndian.Uint32(hdr[0:4])
		n := int(binary.LittleEndian.Uint32(hdr[4:8]))
		if n < 17 || n > 1<<26 {
			return fmt.Errorf("wal: implausible record length %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		if crc32.ChecksumIEEE(payload) != crcWant {
			return fmt.Errorf("wal: crc mismatch")
		}
		rec, err := decodeWALPayload(payload)
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

func listWALNums(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var nums []uint64
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// unused but reserved for header validation
var _ = walMagic
