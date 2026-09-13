package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

const (
	sstMagic     = uint64(0x4c534d5353543031) // "LSMSST01"
	sstFooterLen = 48
)

type indexEntry struct {
	key    []byte
	offset int64
}

// TableMeta is persisted in MANIFEST for a live SSTable.
type TableMeta struct {
	FileNum    uint64 `json:"file_num"`
	Level      int    `json:"level"`
	Size       int64  `json:"size"`
	MinKey     []byte `json:"min_key"`
	MaxKey     []byte `json:"max_key"`
	NumEntries uint64 `json:"num_entries"`
}

// SSTable is an immutable sorted file with a restart index and bloom filter.
type SSTable struct {
	meta     TableMeta
	path     string
	f        *os.File
	index    []indexEntry
	bloom    *Bloom
	indexOff int64
	refs     atomic.Int32
}

func sstPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
}

func OpenSSTable(dir string, meta TableMeta) (*SSTable, error) {
	path := sstPath(dir, meta.FileNum)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t := &SSTable{meta: meta, path: path, f: f}
	// Versions retain tables they publish. Open starts at 0 so the first
	// retainAll() brings the live refcount to 1.
	if err := t.loadMeta(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return t, nil
}

func (t *SSTable) retain() { t.refs.Add(1) }
func (t *SSTable) release() {
	if t.refs.Add(-1) == 0 {
		_ = t.Close()
	}
}

func (t *SSTable) Close() error {
	if t.f == nil {
		return nil
	}
	err := t.f.Close()
	t.f = nil
	return err
}

func (t *SSTable) Meta() TableMeta { return t.meta }
func (t *SSTable) Path() string    { return t.path }

func (t *SSTable) Overlaps(min, max []byte) bool {
	if len(t.meta.MinKey) == 0 && len(t.meta.MaxKey) == 0 {
		return true
	}
	if compareBytes(t.meta.MaxKey, min) < 0 {
		return false
	}
	if compareBytes(t.meta.MinKey, max) > 0 {
		return false
	}
	return true
}

func (t *SSTable) ContainsKeyRange(key []byte) bool {
	if len(t.meta.MinKey) > 0 && compareBytes(key, t.meta.MinKey) < 0 {
		return false
	}
	if len(t.meta.MaxKey) > 0 && compareBytes(key, t.meta.MaxKey) > 0 {
		return false
	}
	return true
}

func (t *SSTable) MayContain(key []byte) bool {
	if !t.ContainsKeyRange(key) {
		return false
	}
	return t.bloom.MayContain(key)
}

func (t *SSTable) Get(key []byte) (value []byte, deleted, ok bool, err error) {
	if !t.MayContain(key) {
		return nil, false, false, nil
	}
	off, ok := t.blockOffset(key)
	if !ok {
		return nil, false, false, nil
	}
	it, err := t.iteratorFrom(off)
	if err != nil {
		return nil, false, false, err
	}
	defer it.Close()
	for it.Next() {
		c := compareBytes(it.Key(), key)
		if c == 0 {
			return cloneBytes(it.Value()), it.Deleted(), true, nil
		}
		if c > 0 {
			return nil, false, false, nil
		}
	}
	return nil, false, false, it.Err()
}

func (t *SSTable) blockOffset(key []byte) (int64, bool) {
	if len(t.index) == 0 {
		return 0, false
	}
	// Last restart whose first key is <= key.
	lo, hi := 0, len(t.index)-1
	ans := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if compareBytes(t.index[mid].key, key) <= 0 {
			ans = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if ans < 0 {
		return 0, false
	}
	return t.index[ans].offset, true
}

func (t *SSTable) NewIterator() (*SSTIterator, error) {
	return t.iteratorFrom(0)
}

func (t *SSTable) iteratorFrom(off int64) (*SSTIterator, error) {
	st, err := t.f.Stat()
	if err != nil {
		return nil, err
	}
	dataEnd := t.dataEnd()
	if dataEnd <= 0 {
		dataEnd = st.Size()
	}
	if off > dataEnd {
		off = dataEnd
	}
	return &SSTIterator{r: io.NewSectionReader(t.f, off, dataEnd-off), remain: dataEnd - off}, nil
}

func (t *SSTable) dataEnd() int64 {
	// data occupies [0, first footer-described section). Stored as index offset.
	if len(t.index) == 0 {
		return 0
	}
	return t.indexOff
}

func (t *SSTable) loadMeta() error {
	st, err := t.f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < sstFooterLen {
		return fmt.Errorf("sstable %s: too small", t.path)
	}
	footer := make([]byte, sstFooterLen)
	if _, err := t.f.ReadAt(footer, st.Size()-sstFooterLen); err != nil {
		return err
	}
	magic := binary.LittleEndian.Uint64(footer[40:48])
	if magic != sstMagic {
		return fmt.Errorf("sstable %s: bad magic", t.path)
	}
	t.indexOff = int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int64(binary.LittleEndian.Uint64(footer[8:16]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomLen := int64(binary.LittleEndian.Uint64(footer[24:32]))
	t.meta.NumEntries = binary.LittleEndian.Uint64(footer[32:40])
	t.meta.Size = st.Size()

	idxBuf := make([]byte, indexLen)
	if _, err := t.f.ReadAt(idxBuf, t.indexOff); err != nil {
		return err
	}
	t.index, err = decodeIndex(idxBuf)
	if err != nil {
		return err
	}
	if len(t.index) > 0 {
		t.meta.MinKey = cloneBytes(t.index[0].key)
	}
	bloomBuf := make([]byte, bloomLen)
	if bloomLen > 0 {
		if _, err := t.f.ReadAt(bloomBuf, bloomOff); err != nil {
			return err
		}
		t.bloom = DecodeBloom(bloomBuf)
	} else {
		t.bloom = NewBloom(1, 10)
	}
	return nil
}

type sstWriter struct {
	f          *os.File
	path       string
	tmp        string
	dir        string
	num        uint64
	level      int
	blockSize  int
	bitsPerKey int
	off        int64
	blockStart int64
	nInBlock   int
	index      []indexEntry
	bloom      *Bloom
	count      uint64
	minKey     []byte
	maxKey     []byte
	estKeys    int
}

func newSSTWriter(dir string, num uint64, level int, opts Options, estKeys int) (*sstWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp := sstPath(dir, num) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if estKeys < 8 {
		estKeys = 8
	}
	return &sstWriter{
		f:          f,
		path:       sstPath(dir, num),
		tmp:        tmp,
		dir:        dir,
		num:        num,
		level:      level,
		blockSize:  opts.BlockSize,
		bitsPerKey: opts.BloomBitsPerKey,
		bloom:      NewBloom(estKeys, opts.BloomBitsPerKey),
		estKeys:    estKeys,
	}, nil
}

func (w *sstWriter) Add(key, value []byte, seq uint64, deleted bool) error {
	if w.nInBlock == 0 || (w.off-w.blockStart) >= int64(w.blockSize) {
		w.index = append(w.index, indexEntry{key: cloneBytes(key), offset: w.off})
		w.blockStart = w.off
		w.nInBlock = 0
	}
	var typ RecordType = RecordPut
	if deleted {
		typ = RecordDel
	}
	buf := encodeSSTRecord(typ, seq, key, value)
	n, err := w.f.Write(buf)
	if err != nil {
		return err
	}
	w.off += int64(n)
	w.nInBlock++
	w.count++
	w.bloom.Add(key)
	if w.minKey == nil {
		w.minKey = cloneBytes(key)
	}
	w.maxKey = cloneBytes(key)
	return nil
}

func (w *sstWriter) Finish() (*SSTable, error) {
	if w.count == 0 {
		_ = w.f.Close()
		_ = os.Remove(w.tmp)
		return nil, nil
	}
	indexOff := w.off
	idxBuf := encodeIndex(w.index)
	if _, err := w.f.Write(idxBuf); err != nil {
		return nil, err
	}
	bloomOff := indexOff + int64(len(idxBuf))
	bloomBuf := w.bloom.Encode()
	if _, err := w.f.Write(bloomBuf); err != nil {
		return nil, err
	}
	footer := make([]byte, sstFooterLen)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOff))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(idxBuf)))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOff))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(bloomBuf)))
	binary.LittleEndian.PutUint64(footer[32:40], w.count)
	binary.LittleEndian.PutUint64(footer[40:48], sstMagic)
	if _, err := w.f.Write(footer); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(w.tmp, w.path); err != nil {
		return nil, err
	}

	meta := TableMeta{
		FileNum:    w.num,
		Level:      w.level,
		Size:       w.off + int64(len(idxBuf)+len(bloomBuf)+sstFooterLen),
		MinKey:     w.minKey,
		MaxKey:     w.maxKey,
		NumEntries: w.count,
	}
	return OpenSSTable(w.dir, meta)
}

func (w *sstWriter) Abort() {
	if w.f != nil {
		_ = w.f.Close()
	}
	_ = os.Remove(w.tmp)
}

func encodeSSTRecord(typ RecordType, seq uint64, key, value []byte) []byte {
	klen, vlen := len(key), len(value)
	buf := make([]byte, 1+8+4+4+klen+vlen)
	buf[0] = byte(typ)
	binary.LittleEndian.PutUint64(buf[1:9], seq)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(klen))
	binary.LittleEndian.PutUint32(buf[13:17], uint32(vlen))
	copy(buf[17:], key)
	copy(buf[17+klen:], value)
	return buf
}

func encodeIndex(idx []indexEntry) []byte {
	n := 4
	for _, e := range idx {
		n += 4 + len(e.key) + 8
	}
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(idx)))
	off := 4
	for _, e := range idx {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(e.key)))
		off += 4
		copy(buf[off:], e.key)
		off += len(e.key)
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(e.offset))
		off += 8
	}
	return buf
}

func decodeIndex(buf []byte) ([]indexEntry, error) {
	if len(buf) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(binary.LittleEndian.Uint32(buf[0:4]))
	off := 4
	out := make([]indexEntry, 0, n)
	for i := 0; i < n; i++ {
		if off+4 > len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		klen := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		if off+klen+8 > len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		key := cloneBytes(buf[off : off+klen])
		off += klen
		offset := int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
		out = append(out, indexEntry{key: key, offset: offset})
	}
	return out, nil
}

// SSTIterator scans records from a section of an SSTable.
type SSTIterator struct {
	r      *io.SectionReader
	remain int64
	rec    Record
	valid  bool
	err    error
	hdr    [17]byte
}

func (it *SSTIterator) Next() bool {
	if it.err != nil || it.remain < 17 {
		it.valid = false
		return false
	}
	if _, err := io.ReadFull(it.r, it.hdr[:]); err != nil {
		if err != io.EOF {
			it.err = err
		}
		it.valid = false
		return false
	}
	it.remain -= 17
	typ := RecordType(it.hdr[0])
	seq := binary.LittleEndian.Uint64(it.hdr[1:9])
	klen := int(binary.LittleEndian.Uint32(it.hdr[9:13]))
	vlen := int(binary.LittleEndian.Uint32(it.hdr[13:17]))
	need := int64(klen + vlen)
	if need > it.remain {
		it.err = io.ErrUnexpectedEOF
		it.valid = false
		return false
	}
	kv := make([]byte, need)
	if _, err := io.ReadFull(it.r, kv); err != nil {
		it.err = err
		it.valid = false
		return false
	}
	it.remain -= need
	it.rec = Record{
		Type:  typ,
		Seq:   seq,
		Key:   kv[:klen],
		Value: kv[klen:],
	}
	it.valid = true
	return true
}

func (it *SSTIterator) Valid() bool   { return it.valid }
func (it *SSTIterator) Key() []byte   { return it.rec.Key }
func (it *SSTIterator) Value() []byte { return it.rec.Value }
func (it *SSTIterator) Seq() uint64   { return it.rec.Seq }
func (it *SSTIterator) Deleted() bool { return it.rec.Type.Deleted() }
func (it *SSTIterator) Err() error    { return it.err }
func (it *SSTIterator) Close()        {}
