package lsm

// Record is a single user mutation stored in the WAL and SSTables.
type Record struct {
	Type  RecordType
	Seq   uint64
	Key   []byte
	Value []byte
}

type RecordType uint8

const (
	RecordPut RecordType = 1
	RecordDel RecordType = 2
)

func (t RecordType) Deleted() bool { return t == RecordDel }

// InternalKey compares two keys for merge order: user-key ascending, seq descending.
func CompareInternal(aKey []byte, aSeq uint64, bKey []byte, bSeq uint64) int {
	c := compareBytes(aKey, bKey)
	if c != 0 {
		return c
	}
	if aSeq > bSeq {
		return -1
	}
	if aSeq < bSeq {
		return 1
	}
	return 0
}

func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
