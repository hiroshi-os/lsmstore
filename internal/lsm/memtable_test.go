package lsm

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMemtablePutGet(t *testing.T) {
	m := NewMemtable()
	m.Put([]byte("a"), []byte("1"), 1)
	m.Put([]byte("b"), []byte("2"), 2)
	val, del, ok := m.Get([]byte("a"))
	if !ok || del || string(val) != "1" {
		t.Fatalf("get a: val=%q del=%v ok=%v", val, del, ok)
	}
	val, del, ok = m.Get([]byte("missing"))
	if ok || del || val != nil {
		t.Fatalf("missing should be absent")
	}
}

func TestMemtableOverwriteAndDelete(t *testing.T) {
	m := NewMemtable()
	m.Put([]byte("k"), []byte("old"), 1)
	m.Put([]byte("k"), []byte("new"), 2)
	val, del, ok := m.Get([]byte("k"))
	if !ok || del || string(val) != "new" {
		t.Fatalf("overwrite failed: %q del=%v ok=%v", val, del, ok)
	}
	m.Delete([]byte("k"), 3)
	val, del, ok = m.Get([]byte("k"))
	if !ok || !del {
		t.Fatalf("delete should leave a tombstone, got val=%q del=%v ok=%v", val, del, ok)
	}
	m.Put([]byte("k"), []byte("reborn"), 4)
	val, del, ok = m.Get([]byte("k"))
	if !ok || del || string(val) != "reborn" {
		t.Fatalf("put after delete failed: %q del=%v ok=%v", val, del, ok)
	}
}

func TestMemtableIgnoresOlderSeq(t *testing.T) {
	m := NewMemtable()
	m.Put([]byte("k"), []byte("new"), 10)
	m.Put([]byte("k"), []byte("old"), 3)
	val, _, _ := m.Get([]byte("k"))
	if string(val) != "new" {
		t.Fatalf("older seq overwrote newer value: %q", val)
	}
}

func TestMemtableSortedIteration(t *testing.T) {
	m := NewMemtable()
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for i, k := range keys {
		m.Put([]byte(k), []byte(fmt.Sprintf("%d", i)), uint64(i+1))
	}
	it := m.NewIterator()
	var got []string
	for it.Next(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"alpha", "bravo", "charlie", "delta"}
	if len(got) != len(want) {
		t.Fatalf("iter len %d want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iter[%d]=%s want %s", i, got[i], want[i])
		}
	}
}

func TestMemtableCallerSlicesNotAliased(t *testing.T) {
	m := NewMemtable()
	k := []byte("key")
	v := []byte("val")
	m.Put(k, v, 1)
	k[0] = 'K'
	v[0] = 'V'
	got, _, ok := m.Get([]byte("key"))
	if !ok || !bytes.Equal(got, []byte("val")) {
		t.Fatalf("memtable aliased caller memory: %q ok=%v", got, ok)
	}
}

func TestMemtableSizeAndLen(t *testing.T) {
	m := NewMemtable()
	if m.Len() != 0 || m.ApproximateSize() != 0 {
		t.Fatal("empty memtable should be zero-sized")
	}
	m.Put([]byte("abc"), []byte("xyz"), 1)
	if m.Len() != 1 || m.ApproximateSize() <= 0 {
		t.Fatalf("len=%d size=%d", m.Len(), m.ApproximateSize())
	}
}
