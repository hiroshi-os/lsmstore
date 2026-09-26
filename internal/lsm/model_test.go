package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Counted by TestModelProperty. Logged once the rapid campaign finishes.
var (
	modelSequences atomic.Int64
	modelActions   atomic.Int64
)

func TestModelProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		modelSequences.Add(1)
		dir, err := os.MkdirTemp("", "lsm-model-")
		if err != nil {
			rt.Fatal(err)
		}
		rt.Cleanup(func() { _ = os.RemoveAll(dir) })

		opts := Options{
			Dir:                 dir,
			MemtableSize:        512,
			L0CompactionTrigger: 2,
			BaseLevelSize:       2048,
			LevelSizeMultiplier: 10,
			TargetFileSize:      1024,
			BloomBitsPerKey:     10,
			BlockSize:           64,
			MaxLevels:           4,
			SyncWAL:             true,
			CompactInterval:     time.Hour,
		}
		db, err := Open(opts)
		if err != nil {
			rt.Fatal(err)
		}
		rt.Cleanup(func() {
			if db != nil {
				_ = db.Close()
			}
		})

		type cell struct {
			v []byte
		}
		model := map[string]*cell{}

		check := func(rt *rapid.T) {
			for i := 0; i < 21; i++ {
				key := []byte(fmt.Sprintf("k%02d", i))
				got, err := db.Get(key)
				c := model[string(key)]
				if c == nil {
					if !errors.Is(err, ErrNotFound) {
						rt.Fatalf("missing %s: got %q err=%v", key, got, err)
					}
					continue
				}
				if err != nil {
					rt.Fatalf("get %s: %v", key, err)
				}
				if !bytes.Equal(got, c.v) {
					rt.Fatalf("get %s: got %q want %q", key, got, c.v)
				}
			}
		}

		rt.Repeat(map[string]func(*rapid.T){
			"": check,
			"put": func(rt *rapid.T) {
				modelActions.Add(1)
				i := rapid.IntRange(0, 20).Draw(rt, "key")
				val := rapid.SliceOfN(rapid.Byte(), 0, 24).Draw(rt, "val")
				key := []byte(fmt.Sprintf("k%02d", i))
				if err := db.Put(key, val); err != nil {
					rt.Fatal(err)
				}
				model[string(key)] = &cell{v: append([]byte(nil), val...)}
			},
			"delete": func(rt *rapid.T) {
				modelActions.Add(1)
				i := rapid.IntRange(0, 20).Draw(rt, "key")
				key := []byte(fmt.Sprintf("k%02d", i))
				if err := db.Delete(key); err != nil {
					rt.Fatal(err)
				}
				delete(model, string(key))
			},
			"get": func(rt *rapid.T) {
				modelActions.Add(1)
				i := rapid.IntRange(0, 20).Draw(rt, "key")
				key := []byte(fmt.Sprintf("k%02d", i))
				got, err := db.Get(key)
				c := model[string(key)]
				if c == nil {
					if !errors.Is(err, ErrNotFound) {
						rt.Fatalf("get %s: got %q err=%v", key, got, err)
					}
					return
				}
				if err != nil || !bytes.Equal(got, c.v) {
					rt.Fatalf("get %s: got %q want %q err=%v", key, got, c.v, err)
				}
			},
			"flush": func(rt *rapid.T) {
				modelActions.Add(1)
				if err := db.Flush(); err != nil {
					rt.Fatal(err)
				}
			},
			"compact": func(rt *rapid.T) {
				modelActions.Add(1)
				if err := db.CompactAll(); err != nil {
					rt.Fatal(err)
				}
			},
			"reopen": func(rt *rapid.T) {
				modelActions.Add(1)
				if err := db.Close(); err != nil {
					rt.Fatal(err)
				}
				db, err = Open(opts)
				if err != nil {
					rt.Fatal(err)
				}
			},
		})
	})
	t.Logf("PROPERTY_RESULT sequences=%d actions=%d", modelSequences.Load(), modelActions.Load())
}
