package lsm

import "time"

// Options configures a DB. Zero values are replaced by Defaults().
type Options struct {
	Dir string

	// MemtableSize is the approximate flush threshold in bytes.
	MemtableSize int64

	// L0CompactionTrigger is the number of L0 files that triggers compaction.
	L0CompactionTrigger int

	// BaseLevelSize is the target size of level 1. Higher levels grow by LevelSizeMultiplier.
	BaseLevelSize int64

	// LevelSizeMultiplier is typically 10 (leveled LSM).
	LevelSizeMultiplier int

	// TargetFileSize is the max size of an output SSTable from flush/compaction.
	TargetFileSize int64

	// BloomBitsPerKey controls bloom filter memory / false-positive rate.
	BloomBitsPerKey int

	// BlockRestartInterval is how many keys share one index restart (bytes-based blocks).
	BlockSize int

	// MaxLevels is the number of LSM levels (L0..Lmax-1).
	MaxLevels int

	// SyncWAL fsyncs the WAL after every append. Safer, slower.
	SyncWAL bool

	// CompactInterval is how often the background worker re-checks triggers.
	CompactInterval time.Duration
}

func Defaults() Options {
	return Options{
		MemtableSize:        4 << 20, // 4 MiB
		L0CompactionTrigger: 4,
		BaseLevelSize:       10 << 20, // 10 MiB
		LevelSizeMultiplier: 10,
		TargetFileSize:      2 << 20, // 2 MiB
		BloomBitsPerKey:     10,
		BlockSize:           4096,
		MaxLevels:           7,
		SyncWAL:             true,
		CompactInterval:     50 * time.Millisecond,
	}
}

func (o Options) withDefaults() Options {
	d := Defaults()
	if o.MemtableSize <= 0 {
		o.MemtableSize = d.MemtableSize
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = d.L0CompactionTrigger
	}
	if o.BaseLevelSize <= 0 {
		o.BaseLevelSize = d.BaseLevelSize
	}
	if o.LevelSizeMultiplier <= 0 {
		o.LevelSizeMultiplier = d.LevelSizeMultiplier
	}
	if o.TargetFileSize <= 0 {
		o.TargetFileSize = d.TargetFileSize
	}
	if o.BloomBitsPerKey <= 0 {
		o.BloomBitsPerKey = d.BloomBitsPerKey
	}
	if o.BlockSize <= 0 {
		o.BlockSize = d.BlockSize
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = d.MaxLevels
	}
	if o.CompactInterval <= 0 {
		o.CompactInterval = d.CompactInterval
	}
	return o
}

func (o Options) levelTargetSize(level int) int64 {
	if level <= 0 {
		return 0
	}
	sz := o.BaseLevelSize
	for i := 1; i < level; i++ {
		sz *= int64(o.LevelSizeMultiplier)
	}
	return sz
}
