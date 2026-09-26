package lsm

import (
	"encoding/binary"
	"errors"
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

// WAL frame:
//
//	crc32(len || payload) | u32(len) | payload
//	payload = type:u8 | seq:u64 | klen:u32 | vlen:u32 | key | value
//
// The checksum covers the length so a torn length field cannot frame a
// different byte slice that still matches the stored CRC.

const (
	walHeaderLen = 8
	walMinRecord = 17
	walMaxRecord = 1 << 26
)

// WAL is an append-only durability log.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	dir  string
	num  uint64
	sync bool
}

// WALReplay is the result of scanning every log in a directory.
type WALReplay struct {
	MaxNum         uint64
	TornTails      int
	TruncatedBytes int64
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
	// The directory entry must be durable before any fsynced record in this
	// file can be acknowledged. File data sync does not cover the dirent.
	if err := syncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{f: f, dir: dir, num: num, sync: sync}, nil
}

func (w *WAL) Num() uint64 { return w.num }

func (w *WAL) Append(rec Record) error {
	buf := encodeWALFrame(rec)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("wal: closed")
	}
	st, err := w.f.Stat()
	if err != nil {
		return err
	}
	end := st.Size()
	if err := writeFull(w.f, buf); err != nil {
		// Roll the file back so a later successful append cannot land
		// after a torn frame. Replay stops at the first bad frame.
		_ = w.f.Truncate(end)
		_ = w.f.Sync()
		return err
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("wal: closed")
	}
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

func encodeWALFrame(rec Record) []byte {
	payload := encodeWALPayload(rec)
	buf := make([]byte, walHeaderLen+len(payload))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[0:4], checksumWAL(payload))
	copy(buf[8:], payload)
	return buf
}

func checksumWAL(payload []byte) uint32 {
	var lenb [4]byte
	binary.LittleEndian.PutUint32(lenb[:], uint32(len(payload)))
	h := crc32.NewIEEE()
	_, _ = h.Write(lenb[:])
	_, _ = h.Write(payload)
	return h.Sum32()
}

func decodeWALPayload(p []byte) (Record, error) {
	if len(p) < walMinRecord {
		return Record{}, io.ErrUnexpectedEOF
	}
	rec := Record{
		Type: RecordType(p[0]),
		Seq:  binary.LittleEndian.Uint64(p[1:9]),
	}
	if rec.Type != RecordPut && rec.Type != RecordDel {
		return Record{}, fmt.Errorf("wal: bad record type %d", rec.Type)
	}
	klenU := binary.LittleEndian.Uint32(p[9:13])
	vlenU := binary.LittleEndian.Uint32(p[13:17])
	if int64(klenU)+int64(vlenU)+int64(walMinRecord) != int64(len(p)) {
		return Record{}, fmt.Errorf("wal: bad payload size")
	}
	klen := int(klenU)
	vlen := int(vlenU)
	rec.Key = cloneBytes(p[17 : 17+klen])
	if vlen > 0 {
		rec.Value = cloneBytes(p[17+klen : 17+klen+vlen])
	}
	return rec, nil
}

type frameKind int

const (
	frameOK frameKind = iota
	frameTorn
	frameBad
)

// readWALFrame parses one frame at off. frameTorn means the bytes from off
// to EOF cannot be a complete record (short header, short body, or a length
// that runs past EOF). frameBad means a complete slice was read but the CRC
// or payload was invalid; next is the offset just past that slice.
func readWALFrame(f *os.File, off, size int64) (Record, int64, frameKind, error) {
	if size-off < walHeaderLen {
		return Record{}, off, frameTorn, io.ErrUnexpectedEOF
	}
	var hdr [walHeaderLen]byte
	if _, err := f.ReadAt(hdr[:], off); err != nil {
		return Record{}, off, frameTorn, err
	}
	crcWant := binary.LittleEndian.Uint32(hdr[0:4])
	n := int64(binary.LittleEndian.Uint32(hdr[4:8]))
	if n < walMinRecord || n > walMaxRecord || off+walHeaderLen+n > size {
		return Record{}, off, frameTorn, fmt.Errorf("wal: torn record at %d (len %d)", off, n)
	}
	payload := make([]byte, n)
	if _, err := f.ReadAt(payload, off+walHeaderLen); err != nil {
		return Record{}, off, frameTorn, err
	}
	next := off + walHeaderLen + n
	if checksumWAL(payload) != crcWant {
		return Record{}, next, frameBad, fmt.Errorf("wal: crc mismatch at %d", off)
	}
	rec, err := decodeWALPayload(payload)
	if err != nil {
		return Record{}, next, frameBad, err
	}
	return rec, next, frameOK, nil
}

// suffixValid reports whether [off, size) is one or more complete, valid
// frames and nothing else. An empty suffix is not valid: a checksum failure
// on the last frame is a torn tail, not proof of a good record after it.
func suffixValid(f *os.File, off, size int64) bool {
	if off >= size {
		return false
	}
	for off < size {
		_, next, kind, _ := readWALFrame(f, off, size)
		if kind != frameOK {
			return false
		}
		if next <= off {
			return false
		}
		off = next
	}
	return true
}

func truncateWAL(f *os.File, good, size int64) (int64, error) {
	if err := f.Truncate(good); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return size - good, nil
}

// ReplayWAL walks every .log file in dir in file-number order.
// A torn tail (incomplete frame, or a CRC failure that is not followed by
// a valid frame suffix) is truncated to the last good frame and counted.
// A CRC failure followed by valid frames is returned as an error so a
// mid-file checksum break is not silently dropped.
func ReplayWAL(dir string, fn func(Record) error) (WALReplay, error) {
	var out WALReplay
	nums, err := listWALNums(dir)
	if err != nil {
		return out, err
	}
	for _, num := range nums {
		if num > out.MaxNum {
			out.MaxNum = num
		}
		torn, n, err := replayFile(walPath(dir, num), fn)
		if torn {
			out.TornTails++
			out.TruncatedBytes += n
		}
		if err != nil {
			return out, fmt.Errorf("wal %d: %w", num, err)
		}
	}
	return out, nil
}

func replayFile(path string, fn func(Record) error) (torn bool, truncated int64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil
		}
		return false, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return false, 0, err
	}
	size := st.Size()
	var good int64
	for good < size {
		rec, next, kind, ferr := readWALFrame(f, good, size)
		switch kind {
		case frameOK:
			if err := fn(rec); err != nil {
				return false, 0, err
			}
			good = next
		case frameTorn:
			n, err := truncateWAL(f, good, size)
			if err != nil {
				return false, 0, err
			}
			return true, n, nil
		case frameBad:
			if suffixValid(f, next, size) {
				return false, 0, ferr
			}
			n, err := truncateWAL(f, good, size)
			if err != nil {
				return false, 0, err
			}
			return true, n, nil
		default:
			return false, 0, ferr
		}
	}
	return false, 0, nil
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

// cleanupTempFiles removes leftover *.tmp files from a crashed writer.
func cleanupTempFiles(dir string) (maxNum uint64, err error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var tmps []string
	for _, e := range ents {
		name := e.Name()
		if n, ok := leadingFileNum(name); ok && n > maxNum {
			maxNum = n
		}
		if strings.HasSuffix(name, ".tmp") {
			tmps = append(tmps, name)
		}
	}
	for _, name := range tmps {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return maxNum, err
		}
	}
	return maxNum, nil
}

func leadingFileNum(name string) (uint64, bool) {
	i := 0
	for i < len(name) && name[i] >= '0' && name[i] <= '9' {
		i++
	}
	if i == 0 || i >= len(name) || name[i] != '.' {
		return 0, false
	}
	n, err := strconv.ParseUint(name[:i], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func maxDataFileNum(dir string) (uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var maxNum uint64
	for _, e := range ents {
		if n, ok := leadingFileNum(e.Name()); ok && n > maxNum {
			maxNum = n
		}
	}
	return maxNum, nil
}

// unused but reserved for header validation
var _ = walMagic
