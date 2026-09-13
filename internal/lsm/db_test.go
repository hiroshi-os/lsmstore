package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestDBPutGetDelete(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), SyncWAL: false, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("hello"))
	if err != nil || string(got) != "world" {
		t.Fatalf("get: %q %v", got, err)
	}
	if err := db.Delete([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	_, err = db.Get([]byte("hello"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if err := db.Put([]byte("hello"), []byte("again")); err != nil {
		t.Fatal(err)
	}
	got, err = db.Get([]byte("hello"))
	if err != nil || string(got) != "again" {
		t.Fatalf("put after delete: %q %v", got, err)
	}
}

func TestWALRecoveryWithoutFlush(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, SyncWAL: true, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("durable"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("nope")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Dir: dir, SyncWAL: true, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	got, err := db2.Get([]byte("durable"))
	if err != nil || string(got) != "yes" {
		t.Fatalf("recovered get: %q %v", got, err)
	}
	if _, err := db2.Get([]byte("nope")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recovered tombstone: %v", err)
	}
}

func TestRecoveryAfterFlush(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		k := []byte(fmt.Sprintf("f-%02d", i))
		if err := db.Put(k, bytes.Repeat([]byte{'x'}, 8)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("tail"), []byte("mem")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 30; i++ {
		k := []byte(fmt.Sprintf("f-%02d", i))
		got, err := db2.Get(k)
		if err != nil || string(got) != "xxxxxxxx" {
			t.Fatalf("sst key %s: %q %v", k, got, err)
		}
	}
	got, err := db2.Get([]byte("tail"))
	if err != nil || string(got) != "mem" {
		t.Fatalf("wal tail: %q %v", got, err)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), SyncWAL: false, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("put: %v", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("get: %v", err)
	}
}
