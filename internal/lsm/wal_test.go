package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func durableOpts(dir string) Options {
	return Options{
		Dir:             dir,
		SyncWAL:         true,
		MemtableSize:    32 << 20,
		CompactInterval: time.Hour,
	}
}

func putKeys(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k-%03d", i))
		v := []byte(fmt.Sprintf("v-%03d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
}

func expectKeys(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k-%03d", i))
		want := fmt.Sprintf("v-%03d", i)
		got, err := db.Get(k)
		if err != nil || string(got) != want {
			t.Fatalf("get %s: %q %v", k, got, err)
		}
	}
	if _, err := db.Get([]byte("phantom-key")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("phantom key: %v", err)
	}
}

func onlyWAL(t *testing.T, dir string) string {
	t.Helper()
	nums, err := listWALNums(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(nums) != 1 {
		t.Fatalf("wal files: %v", nums)
	}
	return walPath(dir, nums[0])
}

func appendSynced(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFull(f, b); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// corruptTail writes n durable keys, closes, appends junk to the only WAL,
// reopens, and checks the prefix survived and the file was truncated.
func corruptTail(t *testing.T, junk []byte) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(durableOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	putKeys(t, db, n)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := onlyWAL(t, dir)
	good := fileSize(t, path)
	if torn, err := scanWALTorn(path); err != nil || torn {
		t.Fatalf("clean wal looks torn: torn=%v err=%v", torn, err)
	}
	appendSynced(t, path, junk)

	db2, err := Open(durableOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	info := db2.RecoveryInfo()
	if info.TornTails != 1 {
		t.Fatalf("torn tails: %+v", info)
	}
	if info.TruncatedBytes != int64(len(junk)) {
		t.Fatalf("truncated %d want %d", info.TruncatedBytes, len(junk))
	}
	if fileSize(t, path) != good {
		t.Fatalf("size %d want %d", fileSize(t, path), good)
	}
	if torn, err := scanWALTorn(path); err != nil || torn {
		t.Fatalf("after repair torn=%v err=%v", torn, err)
	}
	expectKeys(t, db2, n)

	if err := db2.Put([]byte("after"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3, err := Open(durableOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	expectKeys(t, db3, n)
	got, err := db3.Get([]byte("after"))
	if err != nil || string(got) != "ok" {
		t.Fatalf("after: %q %v", got, err)
	}
	if db3.RecoveryInfo().TornTails != 0 {
		t.Fatalf("second open repaired again: %+v", db3.RecoveryInfo())
	}
}

func TestTornWALTailTruncation(t *testing.T) {
	t.Run("partial header", func(t *testing.T) {
		corruptTail(t, []byte{0x01, 0x02, 0x03})
	})
	t.Run("partial payload", func(t *testing.T) {
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[0:4], 0x11111111)
		binary.LittleEndian.PutUint32(hdr[4:8], 64)
		junk := append(hdr[:], bytes.Repeat([]byte{0xab}, 10)...)
		corruptTail(t, junk)
	})
	t.Run("bad crc", func(t *testing.T) {
		frame := encodeWALFrame(Record{
			Type:  RecordPut,
			Seq:   99,
			Key:   []byte("phantom-key"),
			Value: []byte("should-not-surface"),
		})
		frame[len(frame)-1] ^= 0xff
		corruptTail(t, frame)
	})
	t.Run("implausible length", func(t *testing.T) {
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[0:4], 0)
		binary.LittleEndian.PutUint32(hdr[4:8], uint32(walMaxRecord+1))
		corruptTail(t, hdr[:])
	})
}

func TestWALMidFileCRCFailsOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(durableOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k-000"), []byte("v-000")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k-001"), []byte("v-001")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := onlyWAL(t, dir)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	_, next, kind, err := readWALFrame(f, 0, st.Size())
	if err != nil || kind != frameOK {
		t.Fatalf("first frame: kind=%v err=%v", kind, err)
	}
	if next >= st.Size() {
		t.Fatal("expected two frames")
	}
	// Flip a payload byte of the first frame. Length stays intact so the
	// second frame is still a valid suffix — recovery must not truncate it.
	one := []byte{0}
	if _, err := f.ReadAt(one, 8); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := f.WriteAt(one, 8); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before := fileSize(t, path)
	_, err = Open(durableOpts(dir))
	if err == nil {
		t.Fatal("expected mid-file crc error")
	}
	if fileSize(t, path) != before {
		t.Fatalf("mid-file corruption was truncated: size %d -> %d", before, fileSize(t, path))
	}
}

func TestWALFramesAreContiguous(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(durableOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	putKeys(t, db, 5)
	if err := db.Delete([]byte("k-002")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := onlyWAL(t, dir)
	if torn, err := scanWALTorn(path); err != nil || torn {
		t.Fatalf("torn=%v err=%v", torn, err)
	}
	db2, err := Open(durableOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("k-002")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete: %v", err)
	}
	got, err := db2.Get([]byte("k-004"))
	if err != nil || string(got) != "v-004" {
		t.Fatalf("k-004: %q %v", got, err)
	}
}

// scanWALTorn is an independent framing check used by the crash suite.
// It does not call ReplayWAL. A file is torn when it cannot be consumed as
// complete CRC-valid frames. Mid-file corruption (bad CRC with a valid
// suffix) returns an error instead of a torn tail.
func scanWALTorn(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	off := 0
	for off < len(data) {
		if len(data)-off < walHeaderLen {
			return true, nil
		}
		crcWant := binary.LittleEndian.Uint32(data[off : off+4])
		n := int(binary.LittleEndian.Uint32(data[off+4 : off+8]))
		if n < walMinRecord || n > walMaxRecord || off+walHeaderLen+n > len(data) {
			return true, nil
		}
		payload := data[off+walHeaderLen : off+walHeaderLen+n]
		next := off + walHeaderLen + n
		if walTestChecksum(payload) != crcWant || walTestPayloadOK(payload) != nil {
			if walTestSuffixOK(data[next:]) {
				return false, fmt.Errorf("mid-file wal corruption at %d in %s", off, filepath.Base(path))
			}
			return true, nil
		}
		off = next
	}
	return false, nil
}

func scanDirTornWALs(dir string) (int, error) {
	nums, err := listWALNums(dir)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, num := range nums {
		torn, err := scanWALTorn(walPath(dir, num))
		if err != nil {
			return n, err
		}
		if torn {
			n++
		}
	}
	return n, nil
}

func walTestChecksum(payload []byte) uint32 {
	var lenb [4]byte
	binary.LittleEndian.PutUint32(lenb[:], uint32(len(payload)))
	h := crc32.NewIEEE()
	_, _ = h.Write(lenb[:])
	_, _ = h.Write(payload)
	return h.Sum32()
}

func walTestPayloadOK(p []byte) error {
	if len(p) < walMinRecord {
		return errBadPayload
	}
	typ := p[0]
	if typ != byte(RecordPut) && typ != byte(RecordDel) {
		return errBadPayload
	}
	klen := binary.LittleEndian.Uint32(p[9:13])
	vlen := binary.LittleEndian.Uint32(p[13:17])
	if int64(klen)+int64(vlen)+int64(walMinRecord) != int64(len(p)) {
		return errBadPayload
	}
	return nil
}

var errBadPayload = errors.New("bad payload")

func walTestSuffixOK(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	off := 0
	for off < len(data) {
		if len(data)-off < walHeaderLen {
			return false
		}
		crcWant := binary.LittleEndian.Uint32(data[off : off+4])
		n := int(binary.LittleEndian.Uint32(data[off+4 : off+8]))
		if n < walMinRecord || n > walMaxRecord || off+walHeaderLen+n > len(data) {
			return false
		}
		payload := data[off+walHeaderLen : off+walHeaderLen+n]
		if walTestChecksum(payload) != crcWant || walTestPayloadOK(payload) != nil {
			return false
		}
		off += walHeaderLen + n
	}
	return true
}
