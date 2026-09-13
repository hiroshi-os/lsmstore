package lsm

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// Bloom is a classic Bloom filter with double hashing.
// It never produces false negatives: if MayContain is false, the key was never added.
type Bloom struct {
	bits []byte
	m    uint32 // bit count
	k    uint32 // hash functions
}

// NewBloom allocates a filter sized for n keys at bitsPerKey.
func NewBloom(n, bitsPerKey int) *Bloom {
	if n < 1 {
		n = 1
	}
	if bitsPerKey < 1 {
		bitsPerKey = 10
	}
	m := uint32(n * bitsPerKey)
	if m < 64 {
		m = 64
	}
	// Round up to a multiple of 8 bits.
	if m%8 != 0 {
		m += 8 - m%8
	}
	k := uint32(math.Round(float64(bitsPerKey) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return &Bloom{
		bits: make([]byte, m/8),
		m:    m,
		k:    k,
	}
}

func (b *Bloom) Add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(b.m)
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *Bloom) MayContain(key []byte) bool {
	if b == nil || len(b.bits) == 0 {
		return true
	}
	h1, h2 := bloomHashes(key)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(b.m)
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the filter: [k:u32][m:u32][bits...]
func (b *Bloom) Encode() []byte {
	out := make([]byte, 8+len(b.bits))
	binary.LittleEndian.PutUint32(out[0:4], b.k)
	binary.LittleEndian.PutUint32(out[4:8], b.m)
	copy(out[8:], b.bits)
	return out
}

func DecodeBloom(data []byte) *Bloom {
	if len(data) < 8 {
		return &Bloom{k: 1, m: 8, bits: []byte{0xff}}
	}
	k := binary.LittleEndian.Uint32(data[0:4])
	m := binary.LittleEndian.Uint32(data[4:8])
	bits := append([]byte(nil), data[8:]...)
	if int(m) > len(bits)*8 {
		m = uint32(len(bits) * 8)
	}
	if k == 0 {
		k = 1
	}
	return &Bloom{bits: bits, m: m, k: k}
}

func bloomHashes(key []byte) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write(key)
	h1 := h.Sum64()
	h.Reset()
	_, _ = h.Write(key)
	_, _ = h.Write([]byte{0x5f})
	h2 := h.Sum64()
	if h2 == 0 {
		h2 = 0x9e3779b97f4a7c15
	}
	return h1, h2
}
