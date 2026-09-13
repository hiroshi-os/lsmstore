package lsm

import "container/heap"

// Iterator is the common scan interface used by flush and compaction.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Seq() uint64
	Deleted() bool
}

type memIterAdapter struct {
	inner *MemIterator
}

func (a *memIterAdapter) Valid() bool   { return a.inner.Valid() }
func (a *memIterAdapter) Next()         { a.inner.Next() }
func (a *memIterAdapter) Key() []byte   { return a.inner.Key() }
func (a *memIterAdapter) Value() []byte { return a.inner.Value() }
func (a *memIterAdapter) Seq() uint64   { return a.inner.Seq() }
func (a *memIterAdapter) Deleted() bool { return a.inner.Deleted() }

func AdaptMem(it *MemIterator) Iterator {
	it.Next()
	return &memIterAdapter{inner: it}
}

type sstIterAdapter struct {
	inner *SSTIterator
}

func (a *sstIterAdapter) Valid() bool   { return a.inner.Valid() }
func (a *sstIterAdapter) Next()         { a.inner.Next() }
func (a *sstIterAdapter) Key() []byte   { return a.inner.Key() }
func (a *sstIterAdapter) Value() []byte { return a.inner.Value() }
func (a *sstIterAdapter) Seq() uint64   { return a.inner.Seq() }
func (a *sstIterAdapter) Deleted() bool { return a.inner.Deleted() }

func AdaptSST(it *SSTIterator) Iterator {
	it.Next()
	return &sstIterAdapter{inner: it}
}

type heapItem struct {
	it  Iterator
	idx int
}

type iterHeap []*heapItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	return CompareInternal(h[i].it.Key(), h[i].it.Seq(), h[j].it.Key(), h[j].it.Seq()) < 0
}
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any)   { *h = append(*h, x.(*heapItem)) }
func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// MergeIterator is a k-way merge that emits the newest seq per user key.
type MergeIterator struct {
	h       iterHeap
	cur     heapItem
	valid   bool
	key     []byte
	value   []byte
	seq     uint64
	deleted bool
}

func NewMergeIterator(iters []Iterator) *MergeIterator {
	m := &MergeIterator{h: make(iterHeap, 0, len(iters))}
	for i, it := range iters {
		if it != nil && it.Valid() {
			heap.Push(&m.h, &heapItem{it: it, idx: i})
		}
	}
	m.Next()
	return m
}

func (m *MergeIterator) Valid() bool   { return m.valid }
func (m *MergeIterator) Key() []byte   { return m.key }
func (m *MergeIterator) Value() []byte { return m.value }
func (m *MergeIterator) Seq() uint64   { return m.seq }
func (m *MergeIterator) Deleted() bool { return m.deleted }

func (m *MergeIterator) Next() {
	m.valid = false
	if m.h.Len() == 0 {
		return
	}
	item := heap.Pop(&m.h).(*heapItem)
	m.key = cloneBytes(item.it.Key())
	m.value = cloneBytes(item.it.Value())
	m.seq = item.it.Seq()
	m.deleted = item.it.Deleted()
	m.valid = true

	// Advance this iterator; if it still has the same user key, skip older seqs
	// after we have captured the newest (heap orders seq descending).
	item.it.Next()
	if item.it.Valid() {
		heap.Push(&m.h, item)
	}

	for m.h.Len() > 0 {
		top := m.h[0]
		if compareBytes(top.it.Key(), m.key) != 0 {
			break
		}
		item := heap.Pop(&m.h).(*heapItem)
		item.it.Next()
		if item.it.Valid() {
			heap.Push(&m.h, item)
		}
	}
}
