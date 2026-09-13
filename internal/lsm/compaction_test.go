package lsm

import (
	"fmt"
	"testing"
	"time"
)

func testOpts(dir string) Options {
	return Options{
		Dir:                 dir,
		MemtableSize:        256,
		L0CompactionTrigger: 2,
		BaseLevelSize:       512,
		LevelSizeMultiplier: 10,
		TargetFileSize:      256,
		BloomBitsPerKey:     10,
		BlockSize:           64,
		MaxLevels:           7,
		SyncWAL:             false,
		CompactInterval:     time.Hour, // tests drive compaction explicitly
	}
}

func TestFlushCreatesL0AndReadsBack(t *testing.T) {
	db, err := Open(testOpts(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 40
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k-%03d", i))
		if err := db.Put(k, []byte(fmt.Sprintf("v-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	if st.FilesPerLevel[0] < 1 {
		t.Fatalf("expected L0 files after flush, stats=%+v", st)
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k-%03d", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != fmt.Sprintf("v-%03d", i) {
			t.Fatalf("get %s = %q", k, got)
		}
	}
}

func TestLeveledCompactionMergesL0IntoL1(t *testing.T) {
	db, err := Open(testOpts(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Three flushes of distinct key ranges → at least 3 L0 files, trigger=2.
	for round := 0; round < 3; round++ {
		for i := 0; i < 20; i++ {
			k := []byte(fmt.Sprintf("r%d-%02d", round, i))
			if err := db.Put(k, []byte(fmt.Sprintf("v-%d-%d", round, i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	before := db.Stats()
	if before.FilesPerLevel[0] < 2 {
		t.Fatalf("need overlapping L0 files before compact, got %+v", before)
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	after := db.Stats()
	if after.FilesPerLevel[0] != 0 {
		t.Fatalf("L0 should be empty after CompactAll, stats=%+v", after)
	}
	lower := 0
	for lvl := 1; lvl < len(after.FilesPerLevel); lvl++ {
		lower += after.FilesPerLevel[lvl]
	}
	if lower < 1 {
		t.Fatalf("compacted files should land in L1+, stats=%+v", after)
	}
	if after.Compactions == 0 {
		t.Fatal("expected at least one compaction")
	}
	for round := 0; round < 3; round++ {
		for i := 0; i < 20; i++ {
			k := []byte(fmt.Sprintf("r%d-%02d", round, i))
			got, err := db.Get(k)
			if err != nil {
				t.Fatalf("post-compact get %s: %v", k, err)
			}
			want := fmt.Sprintf("v-%d-%d", round, i)
			if string(got) != want {
				t.Fatalf("post-compact get %s = %q want %q", k, got, want)
			}
		}
	}
}

func TestCompactionDropsTombstonesOnLastLevel(t *testing.T) {
	opts := testOpts(t.TempDir())
	opts.MaxLevels = 2 // L0 + L1 (last)
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("gone"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("stay"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("gone")); err != ErrNotFound {
		t.Fatalf("deleted key should be absent, err=%v", err)
	}
	got, err := db.Get([]byte("stay"))
	if err != nil || string(got) != "2" {
		t.Fatalf("stay: %q %v", got, err)
	}
}

func TestOverwriteWinsAcrossSSTables(t *testing.T) {
	db, err := Open(testOpts(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("k"))
	if err != nil || string(got) != "v2" {
		t.Fatalf("got %q err=%v", got, err)
	}
}
