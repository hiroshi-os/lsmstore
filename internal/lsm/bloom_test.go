package lsm

import (
	"fmt"
	"testing"
)

func TestBloomAddContains(t *testing.T) {
	b := NewBloom(1000, 10)
	for i := 0; i < 500; i++ {
		b.Add([]byte(fmt.Sprintf("key-%04d", i)))
	}
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		if !b.MayContain(k) {
			t.Fatalf("false negative for %s", k)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 2000
	b := NewBloom(n, 10)
	for i := 0; i < n; i++ {
		b.Add([]byte(fmt.Sprintf("in-%d", i)))
	}
	fp := 0
	trials := 10000
	for i := 0; i < trials; i++ {
		k := []byte(fmt.Sprintf("out-%d", i))
		if b.MayContain(k) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	// 10 bits/key ≈ 1% theoretical. Allow slack for a small n and hash quality.
	if rate > 0.05 {
		t.Fatalf("false-positive rate too high: %.3f (%d/%d)", rate, fp, trials)
	}
	t.Logf("bloom FPR=%.4f (%d/%d)", rate, fp, trials)
}

func TestBloomEncodeDecode(t *testing.T) {
	b := NewBloom(64, 8)
	b.Add([]byte("alpha"))
	b.Add([]byte("beta"))
	raw := b.Encode()
	got := DecodeBloom(raw)
	if !got.MayContain([]byte("alpha")) || !got.MayContain([]byte("beta")) {
		t.Fatal("decoded bloom lost a key")
	}
	if got.k != b.k || got.m != b.m {
		t.Fatalf("meta mismatch k/m %d/%d vs %d/%d", got.k, got.m, b.k, b.m)
	}
}

func TestBloomNeverFalseNegativeAfterRoundTrip(t *testing.T) {
	b := NewBloom(200, 10)
	keys := make([][]byte, 200)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k-%d", i))
		b.Add(keys[i])
	}
	got := DecodeBloom(b.Encode())
	for _, k := range keys {
		if !got.MayContain(k) {
			t.Fatalf("false negative after encode/decode: %s", k)
		}
	}
}
