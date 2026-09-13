package lsm

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound = errors.New("lsm: not found")
	ErrClosed   = errors.New("lsm: closed")
	ErrEmptyKey = errors.New("lsm: empty key")
)

type immTable struct {
	mem     *Memtable
	walNums []uint64
}

// DB is a single-node LSM store: memtable + WAL + SSTables + leveled compaction.
type DB struct {
	opts Options

	mu        sync.RWMutex
	flushCond *sync.Cond
	mem       *Memtable
	imm       []*immTable
	version   *Version
	wal       *WAL
	replayed  []uint64
	closed    bool
	flushErr  error
	obsolete  []*SSTable

	seq      atomic.Uint64
	nextFile atomic.Uint64

	flushCh   chan struct{}
	compactCh chan struct{}
	stopCh    chan struct{}
	wg        sync.WaitGroup
	compactMu sync.Mutex

	puts        atomic.Uint64
	gets        atomic.Uint64
	deletes     atomic.Uint64
	flushes     atomic.Uint64
	compactions atomic.Uint64
}

func Open(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if opts.Dir == "" {
		return nil, fmt.Errorf("lsm: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		opts:      opts,
		mem:       NewMemtable(),
		version:   newVersion(opts.MaxLevels),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	db.flushCond = sync.NewCond(&db.mu)

	if err := db.recover(); err != nil {
		return nil, err
	}

	db.wg.Add(2)
	go db.flushLoop()
	go db.compactLoop()
	return db, nil
}

func (db *DB) recover() error {
	m, err := readManifest(db.opts.Dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		if len(m.Levels) > len(db.version.levels) {
			db.version.levels = make([][]*SSTable, len(m.Levels))
		}
		for level, files := range m.Levels {
			for _, meta := range files {
				t, err := OpenSSTable(db.opts.Dir, meta)
				if err != nil {
					return fmt.Errorf("open sst %d: %w", meta.FileNum, err)
				}
				db.version.levels[level] = append(db.version.levels[level], t)
			}
		}
		db.seq.Store(m.LastSeq)
		db.nextFile.Store(m.NextFileNum)
	}
	if db.nextFile.Load() == 0 {
		db.nextFile.Store(1)
	}
	db.version.retainAll()

	var replayed []uint64
	maxWAL, err := ReplayWAL(db.opts.Dir, func(rec Record) error {
		if rec.Seq > db.seq.Load() {
			db.seq.Store(rec.Seq)
		}
		if rec.Type.Deleted() {
			db.mem.Delete(rec.Key, rec.Seq)
		} else {
			db.mem.Put(rec.Key, rec.Value, rec.Seq)
		}
		return nil
	})
	if err != nil {
		return err
	}
	nums, _ := listWALNums(db.opts.Dir)
	replayed = nums
	db.replayed = replayed
	if maxWAL >= db.nextFile.Load() {
		db.nextFile.Store(maxWAL + 1)
	}

	wal, err := OpenWAL(db.opts.Dir, db.allocFileNum(), db.opts.SyncWAL)
	if err != nil {
		return err
	}
	db.wal = wal
	return nil
}

func (db *DB) allocFileNum() uint64 {
	return db.nextFile.Add(1) - 1
}

func (db *DB) sstDir() string { return db.opts.Dir }

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	close(db.stopCh)
	db.flushCond.Broadcast()
	db.mu.Unlock()

	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	var err error
	if db.wal != nil {
		err = db.wal.Close()
	}
	if db.version != nil {
		_ = writeManifest(db.opts.Dir, db.version.toManifest(db.nextFile.Load(), db.seq.Load()))
		db.version.release()
		db.version = nil
	}
	return err
}

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if err := db.prepareWriteLocked(); err != nil {
		db.mu.Unlock()
		return err
	}
	seq := db.seq.Add(1)
	if err := db.wal.Append(Record{Type: RecordPut, Seq: seq, Key: key, Value: value}); err != nil {
		db.mu.Unlock()
		return err
	}
	db.mem.Put(key, value, seq)
	db.puts.Add(1)
	need := db.mem.ApproximateSize() >= db.opts.MemtableSize
	db.mu.Unlock()
	if need {
		db.maybeRotate()
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if err := db.prepareWriteLocked(); err != nil {
		db.mu.Unlock()
		return err
	}
	seq := db.seq.Add(1)
	if err := db.wal.Append(Record{Type: RecordDel, Seq: seq, Key: key}); err != nil {
		db.mu.Unlock()
		return err
	}
	db.mem.Delete(key, seq)
	db.deletes.Add(1)
	need := db.mem.ApproximateSize() >= db.opts.MemtableSize
	db.mu.Unlock()
	if need {
		db.maybeRotate()
	}
	return nil
}

func (db *DB) prepareWriteLocked() error {
	for db.mem.ApproximateSize() >= db.opts.MemtableSize && len(db.imm) >= 2 {
		if db.closed {
			return ErrClosed
		}
		db.flushCond.Wait()
	}
	return db.flushErr
}

func (db *DB) maybeRotate() {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return
	}
	for db.mem.ApproximateSize() >= db.opts.MemtableSize {
		for len(db.imm) >= 2 {
			if db.closed {
				return
			}
			db.flushCond.Wait()
		}
		if db.mem.ApproximateSize() < db.opts.MemtableSize {
			return
		}
		if err := db.rotateLocked(); err != nil {
			db.flushErr = err
			return
		}
	}
}

func (db *DB) rotateLocked() error {
	if db.mem.Len() == 0 {
		return nil
	}
	db.mem.Freeze()
	walNums := append(append([]uint64{}, db.replayed...), db.wal.Num())
	db.replayed = nil
	db.imm = append(db.imm, &immTable{mem: db.mem, walNums: walNums})
	db.mem = NewMemtable()
	_ = db.wal.Close()
	w, err := OpenWAL(db.opts.Dir, db.allocFileNum(), db.opts.SyncWAL)
	if err != nil {
		return err
	}
	db.wal = w
	db.signal(db.flushCh)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	db.gets.Add(1)

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	mem := db.mem
	imm := append([]*immTable(nil), db.imm...)
	v := db.version
	v.retain()
	db.mu.RUnlock()
	defer v.release()

	if val, del, ok := mem.Get(key); ok {
		if del {
			return nil, ErrNotFound
		}
		return val, nil
	}
	for i := len(imm) - 1; i >= 0; i-- {
		if val, del, ok := imm[i].mem.Get(key); ok {
			if del {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	for i := len(v.levels[0]) - 1; i >= 0; i-- {
		val, del, ok, err := v.levels[0][i].Get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			if del {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}
	for level := 1; level < len(v.levels); level++ {
		t := findInLevel(v.levels[level], key)
		if t == nil {
			continue
		}
		val, del, ok, err := t.Get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			if del {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}
	return nil, ErrNotFound
}

func findInLevel(files []*SSTable, key []byte) *SSTable {
	lo, hi := 0, len(files)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		t := files[mid]
		if compareBytes(key, t.meta.MinKey) < 0 {
			hi = mid - 1
			continue
		}
		if compareBytes(key, t.meta.MaxKey) > 0 {
			lo = mid + 1
			continue
		}
		return t
	}
	return nil
}

// Flush freezes the active memtable and waits until every immutable table is on disk.
func (db *DB) Flush() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.mem.Len() > 0 {
		for len(db.imm) >= 2 {
			db.flushCond.Wait()
		}
		if err := db.rotateLocked(); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	for len(db.imm) > 0 {
		if db.flushErr != nil {
			err := db.flushErr
			db.mu.Unlock()
			return err
		}
		if db.closed {
			db.mu.Unlock()
			return ErrClosed
		}
		db.flushCond.Wait()
	}
	err := db.flushErr
	db.mu.Unlock()
	return err
}

func (db *DB) flushLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.stopCh:
			db.drainFlush()
			return
		case <-db.flushCh:
			db.drainFlush()
		}
	}
}

func (db *DB) drainFlush() {
	for {
		if err := db.flushOne(); err != nil {
			db.mu.Lock()
			db.flushErr = err
			db.flushCond.Broadcast()
			db.mu.Unlock()
			return
		}
		db.mu.RLock()
		left := len(db.imm)
		db.mu.RUnlock()
		if left == 0 {
			return
		}
	}
}

func (db *DB) flushOne() error {
	db.mu.Lock()
	if len(db.imm) == 0 {
		db.mu.Unlock()
		return nil
	}
	imm := db.imm[0]
	db.mu.Unlock()

	var table *SSTable
	if imm.mem.Len() > 0 {
		num := db.allocFileNum()
		w, err := newSSTWriter(db.opts.Dir, num, 0, db.opts, imm.mem.Len())
		if err != nil {
			return err
		}
		it := AdaptMem(imm.mem.NewIterator())
		for it.Valid() {
			if err := w.Add(it.Key(), it.Value(), it.Seq(), it.Deleted()); err != nil {
				w.Abort()
				return err
			}
			it.Next()
		}
		table, err = w.Finish()
		if err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if table != nil {
		next := db.version.snapshot(db.opts.MaxLevels)
		next.levels[0] = append(next.levels[0], table)
		next.retainAll()
		if err := writeManifest(db.opts.Dir, next.toManifest(db.nextFile.Load(), db.seq.Load())); err != nil {
			next.release()
			_ = os.Remove(table.path)
			return err
		}
		old := db.version
		db.version = next
		old.release()
	}
	if len(db.imm) > 0 && db.imm[0] == imm {
		db.imm = db.imm[1:]
	}
	db.flushes.Add(1)
	db.flushCond.Broadcast()
	for _, n := range imm.walNums {
		_ = os.Remove(walPath(db.opts.Dir, n))
	}
	db.signal(db.compactCh)
	return nil
}

func (db *DB) compactLoop() {
	defer db.wg.Done()
	tick := time.NewTicker(db.opts.CompactInterval)
	defer tick.Stop()
	for {
		select {
		case <-db.stopCh:
			return
		case <-db.compactCh:
			_ = db.compactOnce()
		case <-tick.C:
			_ = db.compactOnce()
		}
	}
}

func (db *DB) compactOnce() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil
	}
	v := db.version
	v.retain()
	db.mu.RUnlock()
	job := db.pickCompaction(v)
	v.release()
	if job == nil {
		return nil
	}
	return db.runCompaction(job)
}

func (db *DB) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Stats is a point-in-time snapshot of engine counters and file counts.
type Stats struct {
	Puts          uint64  `json:"puts"`
	Gets          uint64  `json:"gets"`
	Deletes       uint64  `json:"deletes"`
	Flushes       uint64  `json:"flushes"`
	Compactions   uint64  `json:"compactions"`
	MemtableBytes int64   `json:"memtable_bytes"`
	MemtableKeys  int     `json:"memtable_keys"`
	Immutable     int     `json:"immutable_memtables"`
	LastSeq       uint64  `json:"last_seq"`
	FilesPerLevel []int   `json:"files_per_level"`
	BytesPerLevel []int64 `json:"bytes_per_level"`
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	s := Stats{
		Puts:          db.puts.Load(),
		Gets:          db.gets.Load(),
		Deletes:       db.deletes.Load(),
		Flushes:       db.flushes.Load(),
		Compactions:   db.compactions.Load(),
		MemtableBytes: db.mem.ApproximateSize(),
		MemtableKeys:  db.mem.Len(),
		Immutable:     len(db.imm),
		LastSeq:       db.seq.Load(),
	}
	if db.version != nil {
		s.FilesPerLevel = make([]int, len(db.version.levels))
		s.BytesPerLevel = make([]int64, len(db.version.levels))
		for i := range db.version.levels {
			s.FilesPerLevel[i] = db.version.fileCount(i)
			s.BytesPerLevel[i] = db.version.levelSize(i)
		}
	}
	return s
}
