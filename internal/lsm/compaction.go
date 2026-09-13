package lsm

import (
	"fmt"
	"os"
	"sort"
)

type compactJob struct {
	level    int
	inputs   []*SSTable // files from `level`
	overlaps []*SSTable // files from level+1
}

func (db *DB) pickCompaction(v *Version) *compactJob {
	if v.fileCount(0) >= db.opts.L0CompactionTrigger {
		inputs := append([]*SSTable(nil), v.levels[0]...)
		min, max := spanKeys(inputs)
		overlaps := v.overlapping(1, min, max)
		return &compactJob{level: 0, inputs: inputs, overlaps: overlaps}
	}
	for level := 1; level < db.opts.MaxLevels-1; level++ {
		if v.levelSize(level) > db.opts.levelTargetSize(level) {
			// Pick the first file (oldest / lowest file number) and its overlaps.
			if len(v.levels[level]) == 0 {
				continue
			}
			pick := v.levels[level][0]
			overlaps := v.overlapping(level+1, pick.meta.MinKey, pick.meta.MaxKey)
			return &compactJob{level: level, inputs: []*SSTable{pick}, overlaps: overlaps}
		}
	}
	return nil
}

func spanKeys(tables []*SSTable) (min, max []byte) {
	for _, t := range tables {
		if min == nil || compareBytes(t.meta.MinKey, min) < 0 {
			min = t.meta.MinKey
		}
		if max == nil || compareBytes(t.meta.MaxKey, max) > 0 {
			max = t.meta.MaxKey
		}
	}
	return min, max
}

func (db *DB) runCompaction(job *compactJob) error {
	iters := make([]Iterator, 0, len(job.inputs)+len(job.overlaps))
	var opened []*SSTIterator
	defer func() {
		for _, it := range opened {
			it.Close()
		}
	}()
	add := func(t *SSTable) error {
		raw, err := t.NewIterator()
		if err != nil {
			return err
		}
		opened = append(opened, raw)
		iters = append(iters, AdaptSST(raw))
		return nil
	}
	for _, t := range job.inputs {
		if err := add(t); err != nil {
			return err
		}
	}
	for _, t := range job.overlaps {
		if err := add(t); err != nil {
			return err
		}
	}

	outLevel := job.level + 1
	dropTombstones := outLevel >= db.opts.MaxLevels-1
	merge := NewMergeIterator(iters)

	var outputs []*SSTable
	var writer *sstWriter
	est := 0
	for _, t := range append(job.inputs, job.overlaps...) {
		est += int(t.meta.NumEntries)
	}

	finishWriter := func() error {
		if writer == nil {
			return nil
		}
		t, err := writer.Finish()
		writer = nil
		if err != nil {
			return err
		}
		if t != nil {
			outputs = append(outputs, t)
		}
		return nil
	}

	for merge.Valid() {
		if merge.Deleted() && dropTombstones {
			merge.Next()
			continue
		}
		if writer == nil {
			num := db.allocFileNum()
			var err error
			writer, err = newSSTWriter(db.sstDir(), num, outLevel, db.opts, est)
			if err != nil {
				return err
			}
		}
		if err := writer.Add(merge.Key(), merge.Value(), merge.Seq(), merge.Deleted()); err != nil {
			writer.Abort()
			return err
		}
		if writer.off >= db.opts.TargetFileSize {
			if err := finishWriter(); err != nil {
				return err
			}
		}
		merge.Next()
	}
	if err := finishWriter(); err != nil {
		return err
	}

	return db.installCompaction(job, outputs)
}

func (db *DB) installCompaction(job *compactJob, outputs []*SSTable) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	next := db.version.snapshot(db.opts.MaxLevels)
	drop := map[uint64]bool{}
	for _, t := range job.inputs {
		drop[t.meta.FileNum] = true
	}
	for _, t := range job.overlaps {
		drop[t.meta.FileNum] = true
	}

	filter := func(level int) []*SSTable {
		var keep []*SSTable
		for _, t := range next.levels[level] {
			if !drop[t.meta.FileNum] {
				keep = append(keep, t)
			}
		}
		return keep
	}
	next.levels[job.level] = filter(job.level)
	next.levels[job.level+1] = filter(job.level + 1)
	next.levels[job.level+1] = append(next.levels[job.level+1], outputs...)
	sort.Slice(next.levels[job.level+1], func(i, j int) bool {
		return compareBytes(next.levels[job.level+1][i].meta.MinKey, next.levels[job.level+1][j].meta.MinKey) < 0
	})
	next.retainAll()

	if err := writeManifest(db.opts.Dir, next.toManifest(db.nextFile.Load(), db.seq.Load())); err != nil {
		next.release()
		for _, t := range outputs {
			_ = os.Remove(t.path)
		}
		return fmt.Errorf("manifest: %w", err)
	}

	old := db.version
	db.version = next
	old.release()

	db.compactions.Add(1)
	db.obsolete = append(db.obsolete, job.inputs...)
	db.obsolete = append(db.obsolete, job.overlaps...)
	db.reapObsolete()
	return nil
}

func (db *DB) reapObsolete() {
	kept := db.obsolete[:0]
	for _, t := range db.obsolete {
		// Safe to unlink once no version holds the file. Versions retain tables
		// by pointer; after install the old version's release() closes fds when
		// readers drain. We delete the path immediately — open fds still read.
		_ = os.Remove(t.path)
	}
	db.obsolete = kept
}

func (db *DB) CompactAll() error {
	if err := db.Flush(); err != nil {
		return err
	}
	for {
		db.compactMu.Lock()
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			db.compactMu.Unlock()
			return ErrClosed
		}
		v := db.version
		v.retain()
		db.mu.RUnlock()
		job := db.pickCompaction(v)
		if job == nil && v.fileCount(0) > 0 {
			// Tests and operators want L0 drained, even below the trigger.
			inputs := append([]*SSTable(nil), v.levels[0]...)
			min, max := spanKeys(inputs)
			job = &compactJob{level: 0, inputs: inputs, overlaps: v.overlapping(1, min, max)}
		}
		v.release()
		if job == nil {
			db.compactMu.Unlock()
			return nil
		}
		err := db.runCompaction(job)
		db.compactMu.Unlock()
		if err != nil {
			return err
		}
	}
}
