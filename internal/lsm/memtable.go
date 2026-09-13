package lsm

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

const (
	skiplistMaxLevel = 16
	skiplistP        = 4 // 1/4 probability of promoting a level
	entryOverhead    = 48
)

type slNode struct {
	key     []byte
	value   []byte
	seq     uint64
	deleted bool
	next    [skiplistMaxLevel]*slNode
}

// Memtable is a mutex-protected skiplist. Keys are unique; a later Put/Delete
// overwrites the previous entry in place (the stored seq is the latest).
type Memtable struct {
	mu     sync.RWMutex
	head   *slNode
	level  int
	bytes  int64
	count  int
	frozen atomic.Bool
	rnd    *rand.Rand
}

func NewMemtable() *Memtable {
	return &Memtable{
		head:  &slNode{},
		level: 1,
		rnd:   rand.New(rand.NewSource(0x7f4a7c159e3779b9)),
	}
}

func (m *Memtable) Freeze()      { m.frozen.Store(true) }
func (m *Memtable) Frozen() bool { return m.frozen.Load() }

func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bytes
}

func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

func (m *Memtable) Put(key, value []byte, seq uint64) {
	m.upsert(key, value, seq, false)
}

func (m *Memtable) Delete(key []byte, seq uint64) {
	m.upsert(key, nil, seq, true)
}

func (m *Memtable) upsert(key, value []byte, seq uint64, deleted bool) {
	if m.frozen.Load() {
		panic("lsm: write to frozen memtable")
	}
	key = cloneBytes(key)
	value = cloneBytes(value)

	m.mu.Lock()
	defer m.mu.Unlock()

	var preds [skiplistMaxLevel]*slNode
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareBytes(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		preds[i] = x
	}
	next := x.next[0]
	if next != nil && compareBytes(next.key, key) == 0 {
		// In-place update; only keep the newest seq.
		if seq < next.seq {
			return
		}
		old := entrySize(next)
		next.value = value
		next.seq = seq
		next.deleted = deleted
		m.bytes += entrySize(next) - old
		return
	}

	lvl := m.randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			preds[i] = m.head
		}
		m.level = lvl
	}
	n := &slNode{key: key, value: value, seq: seq, deleted: deleted}
	for i := 0; i < lvl; i++ {
		n.next[i] = preds[i].next[i]
		preds[i].next[i] = n
	}
	m.count++
	m.bytes += entrySize(n)
}

func (m *Memtable) Get(key []byte) (value []byte, deleted, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareBytes(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	n := x.next[0]
	if n == nil || compareBytes(n.key, key) != 0 {
		return nil, false, false
	}
	return cloneBytes(n.value), n.deleted, true
}

func (m *Memtable) NewIterator() *MemIterator {
	m.mu.RLock()
	nodes := make([]*slNode, 0, m.count)
	for n := m.head.next[0]; n != nil; n = n.next[0] {
		nodes = append(nodes, n)
	}
	m.mu.RUnlock()
	return &MemIterator{nodes: nodes, idx: -1}
}

func (m *Memtable) randomLevel() int {
	lvl := 1
	for lvl < skiplistMaxLevel && m.rnd.Intn(skiplistP) == 0 {
		lvl++
	}
	return lvl
}

func entrySize(n *slNode) int64 {
	return int64(len(n.key) + len(n.value) + entryOverhead)
}

// MemIterator walks keys in sorted order. It is a snapshot of node pointers
// taken at construction; values are not copied until Key/Value are called.
type MemIterator struct {
	nodes []*slNode
	idx   int
}

func (it *MemIterator) Next() {
	it.idx++
}

func (it *MemIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.nodes)
}

func (it *MemIterator) Key() []byte {
	return cloneBytes(it.nodes[it.idx].key)
}

func (it *MemIterator) Value() []byte {
	return cloneBytes(it.nodes[it.idx].value)
}

func (it *MemIterator) Seq() uint64 { return it.nodes[it.idx].seq }

func (it *MemIterator) Deleted() bool { return it.nodes[it.idx].deleted }
