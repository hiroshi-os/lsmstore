package lsm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
)

const manifestName = "MANIFEST"

type manifest struct {
	NextFileNum     uint64        `json:"next_file_num"`
	LastSeq         uint64        `json:"last_seq"`
	Levels          [][]TableMeta `json:"levels"`
	ObsoleteWALNums []uint64      `json:"obsolete_wal_nums,omitempty"`
}

// Version is an immutable snapshot of the SST set. Readers retain a ref so
// compaction can install a new version without deleting files still in use.
type Version struct {
	refs   atomic.Int32
	levels [][]*SSTable
}

func newVersion(maxLevels int) *Version {
	v := &Version{levels: make([][]*SSTable, maxLevels)}
	v.refs.Store(1)
	return v
}

func (v *Version) retain() { v.refs.Add(1) }

func (v *Version) release() {
	if v.refs.Add(-1) == 0 {
		for _, lvl := range v.levels {
			for _, t := range lvl {
				t.release()
			}
		}
	}
}

func (v *Version) snapshot(maxLevels int) *Version {
	out := newVersion(maxLevels)
	if len(v.levels) > maxLevels {
		out.levels = make([][]*SSTable, len(v.levels))
	}
	for i, lvl := range v.levels {
		out.levels[i] = append([]*SSTable(nil), lvl...)
	}
	return out
}

func (v *Version) retainAll() {
	for _, lvl := range v.levels {
		for _, t := range lvl {
			t.retain()
		}
	}
}

func (v *Version) fileCount(level int) int {
	if level < 0 || level >= len(v.levels) {
		return 0
	}
	return len(v.levels[level])
}

func (v *Version) levelSize(level int) int64 {
	var n int64
	for _, t := range v.levels[level] {
		n += t.meta.Size
	}
	return n
}

func (v *Version) overlapping(level int, min, max []byte) []*SSTable {
	var out []*SSTable
	if level < 0 || level >= len(v.levels) {
		return out
	}
	for _, t := range v.levels[level] {
		if t.Overlaps(min, max) {
			out = append(out, t)
		}
	}
	return out
}

func (v *Version) findLevelGE1(key []byte) *SSTable {
	for level := 1; level < len(v.levels); level++ {
		lvl := v.levels[level]
		// Non-overlapping: binary search by max key.
		lo, hi := 0, len(lvl)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			t := lvl[mid]
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
	}
	return nil
}

func (v *Version) toManifest(nextFile, lastSeq uint64, obsoleteWALs []uint64) manifest {
	m := manifest{
		NextFileNum:     nextFile,
		LastSeq:         lastSeq,
		Levels:          make([][]TableMeta, len(v.levels)),
		ObsoleteWALNums: append([]uint64(nil), obsoleteWALs...),
	}
	for i, lvl := range v.levels {
		for _, t := range lvl {
			m.Levels[i] = append(m.Levels[i], t.meta)
		}
	}
	return m
}

func writeManifest(dir string, m manifest) error {
	path := filepath.Join(dir, manifestName)
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	// fsync the temp file before rename so the new directory entry cannot
	// point at a zero-length or partial manifest after a crash.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeFull(f, data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func readManifest(dir string) (manifest, error) {
	path := filepath.Join(dir, manifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, err
	}
	return m, nil
}
