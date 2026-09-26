# lsmstore design

Single-node LSM MVP. This document is the source of truth for how the pieces fit together and for the write-RPS numbers we actually measured.

## Why LSM

A B-tree updates pages in place and pays random writes plus a WAL. An LSM turns every write into an append:

1. Append a record to the WAL (durability).
2. Insert into an in-memory skiplist (the memtable).
3. When the memtable fills, freeze it and write one sorted SSTable into level 0.
4. Merge SSTables in the background so reads stay bounded.

Deletes are tombstones (`RecordDel`), not in-place erases. A later compaction drops a tombstone once it has been pushed to the last level (nothing older can exist below it).

## Write path

```
Put/Delete
  → allocate seq (monotonic uint64)
  → WAL.Append(type, seq, key, value)   [optional fsync]
  → memtable upsert (key unique; newer seq wins)
  → if memtable ≥ MemtableSize: freeze, rotate WAL, enqueue flush
```

Two immutable memtables may sit behind the active one. Further writers wait on `flushCond` so memory cannot grow without bound.

Flush writes one L0 SSTable from the frozen skiplist (already sorted), then:

- atomically swaps a new `Version` (copy-on-write file set)
- rewrites `MANIFEST` via temp file + rename
- deletes the WAL segments that produced that memtable

Crash windows:

| Failure after | Recovery |
| --- | --- |
| WAL append + memtable, before flush | Replay WAL into a new memtable |
| SST write, before MANIFEST rename | Orphan `.sst` / `.tmp` ignored; WAL still replays |
| MANIFEST rename, before WAL delete | SST loaded; obsolete WALs removed only after that MANIFEST is observed on Open |
| WAL still present after flush | SST loaded and WAL replayed; memtable shadows the SST |

Obsolete WAL segments are **not** unlinked during `flushOne`. Their numbers are recorded in `MANIFEST.obsolete_wal_nums` and removed on the next successful `Open` after those SSTables open. That closes a window where a rolled-back directory entry for MANIFEST plus an already-deleted WAL would lose acknowledged writes (observed under kill on Windows).

A torn WAL record at EOF is truncated to the last complete CRC-valid frame (length+payload checksum). Mid-file corruption with a valid suffix fails open.

## Read path

`Get` snapshots the active memtable, any immutable memtables, and the current `Version` (refcounted so compaction cannot close files still being read), then searches:

1. Active memtable
2. Immutable memtables, newest first
3. All L0 SSTables, newest file first (ranges overlap)
4. Levels 1…N: at most one file per level contains the key (binary search on min/max)

A bloom miss skips the file. An index miss (restart key > search key in the first block, or no restart ≤ key) skips the scan. The first hit — value or tombstone — wins; older versions in lower levels are not consulted.

## Memtable (skiplist)

`internal/lsm/memtable.go` is a mutex-protected skiplist (`p = 1/4`, max 16 levels). Reasons it is a skiplist rather than a red-black tree or a sorted slice:

- inserts stay O(log n) without rotating a large contiguous buffer
- iteration is a level-0 walk, which is exactly the flush order
- an in-place update on the same user key keeps one entry per key (flush does not emit history)

Caller key/value slices are copied on the way in. `Freeze()` forbids further writes so a flush iterator can walk nodes without copying the whole table first (it still snapshots the node pointer list under the lock).

## WAL

File: `NNNNNN.log` in the DB directory.

```
record = crc32(len || payload) || u32(len) || payload
payload = type:u8 || seq:u64 || klen:u32 || vlen:u32 || key || value
```

`SyncWAL=true` (default on the HTTP/CLI servers) calls `fsync` after every append. That is the durable configuration. Opening a new WAL also syncs the parent directory on Unix so the file's directory entry is durable before any acknowledged record can land in it. Benchmarks also report the buffered (`SyncWAL=false`) number so the CPU/skiplist path is visible without being dominated by the disk barrier.

Replay scans `*.log` in file-number order. A torn frame at EOF (short header/body, implausible length past EOF, or a CRC failure with no valid frames after it) is truncated to the last good offset and counted in `RecoveryInfo`. A mid-file CRC failure that is followed by a valid frame suffix fails `Open` instead of silently truncating. Older seqs that lose to a newer in-memory entry are ignored by the skiplist upsert.

## SSTable format

File: `NNNNNN.sst`. Written to `*.tmp` and renamed.

```
[ data records, sorted by user key ]
[ index: count || (klen, key, offset)*     ]   one restart every BlockSize bytes
[ bloom: k:u32 || m:u32 || bitset          ]
[ footer 48 bytes ]
    index_off u64 | index_len u64
    bloom_off u64 | bloom_len u64
    num_entries u64 | magic "LSMSST01"
```

A data record is the same layout as a WAL payload (type, seq, key, value). `Get` binary-searches the restart index for the last restart key ≤ target, then scans that block.

Min/max keys and file numbers live in `MANIFEST` (JSON). Levels ≥ 1 are kept non-overlapping on user-key ranges so a point read touches one file per level.

## Bloom filters

Classic Bloom filter, double hashing (`FNV-64a` and a domain-separated FNV).

- `m = n * bits_per_key` bits (default 10 bits/key)
- `k = round(bits_per_key * ln 2)` ≈ 7 probes

`MayContain == false` is authoritative (no false negatives). A true result only means “maybe”; the block scan is the source of truth. Filters are rebuilt on every flush/compaction from the keys actually written, then stored next to the index so a file can be opened without a sidecar.

Theoretical FPR at 10 bits/key is about 1%. The unit test checks there are **zero** false negatives and that the measured FPR on 10k probes stays under 5% (slack for small `n` and hash quality).

## Leveled compaction

Inspired by LevelDB/RocksDB, not size-tiered.

| Level | Invariant | Trigger |
| --- | --- | --- |
| L0 | Flushed memtables; ranges **may overlap** | `len(L0) ≥ L0CompactionTrigger` (default 4) |
| L1 | Non-overlapping files; target `BaseLevelSize` (10 MiB) | total bytes > target |
| Lk | Non-overlapping; target `BaseLevelSize * 10^(k-1)` | total bytes > target |

When L0 fires, **all** L0 files plus every L1 file whose range overlaps that span are merged. When Lk fires, the oldest file in Lk plus overlapping files in L(k+1) are merged. Output is split on `TargetFileSize` (2 MiB) and installed as the new L(k+1) set for those ranges.

Merge is a k-way heap ordered by `(user_key ASC, seq DESC)`. For each user key only the newest seq is emitted. Tombstones are dropped when the output level is the last configured level (`MaxLevels-1`).

Version install is a pointer-slice copy plus `retainAll` on the new file set. Readers hold a `Version` ref; when the last ref drops, tables that no longer appear in any version close their fds. Obsolete paths are unlinked after install (open fds remain readable).

## Concurrency

- Writers serialize on `DB.mu` for seq + WAL + memtable (correctness first).
- `Get` releases `DB.mu` after snapshotting and does SST I/O lock-free against that `Version`.
- One flush goroutine, one compaction goroutine. Compaction is also serialized by `compactMu` so `CompactAll()` (tests) cannot interleave with the background worker.

A later Raft layer would replace the writer mutex with “raft committed log → Apply”, not change the SST/compaction machinery.

## Defaults

| Option | Default |
| --- | --- |
| MemtableSize | 4 MiB |
| L0CompactionTrigger | 4 files |
| BaseLevelSize | 10 MiB |
| LevelSizeMultiplier | 10 |
| TargetFileSize | 2 MiB |
| BloomBitsPerKey | 10 |
| BlockSize | 4 KiB |
| MaxLevels | 7 |
| SyncWAL | true (servers); benches set this explicitly |

## Measured write RPS

These numbers come from `go run ./cmd/lsmbench` in this repository. They are wall-clock measurements, not synthetic estimates.

### Hardware and date

```
date:        2026-09-13T10:56:13Z / 2026-09-13T10:56:17Z (UTC)
go:          go1.22.2 linux/amd64
os:          Linux 6.12.94+ (KVM guest, hostname cursor)
cpu:         Intel(R) Xeon(R) Processor  family 6 model 207, 4 cores / 4 threads
gomaxprocs:  4
memory:      16 GiB
disk:        VM-backed ext4 (not a local NVMe claim)
```

### Results

Measured with `go run ./cmd/lsmbench` in this repo (wall clock, single process).

| Run | n | value | workers | SyncWAL | write RPS | p50 | p99 | notes |
| --- | ---: | ---: | ---: | --- | ---: | --- | --- | --- |
| buffered | 50,000 | 64 B | 1 | false | **514,249** | 1.323 µs | 3.809 µs | WAL append, no fsync; 1 flush to L0; 95,184 read RPS on 5k gets |
| durable | 20,000 | 64 B | 1 | true | **10,350** | 91.225 µs | 224.402 µs | fsync after every Put; data still in memtable |

Reproduce:

```bash
go run ./cmd/lsmbench -n 50000 -value 64 -workers 1
go run ./cmd/lsmbench -n 20000 -value 64 -workers 1 -sync
```

The durable number is expected to sit near the filesystem’s fsync rate (often a few thousand RPS on a VM disk, much higher on a battery-backed or `ext4` with barriers relaxed). The buffered number is the skiplist + syscall append path.

## What this MVP is not

- No range scans / snapshots / transactions
- No compression, checksums on SST data blocks, or table cache
- No partitioned / hashed memtables
- No Raft, multi-disk, or multi-process locking (`lsmctl` and `lsmstore` must not share a live directory)
- Directory-entry fsync is a no-op on Windows (file data Sync still runs); Unix CI is the durability bar for rename/create
- Group commit / concurrent writers on the WAL are not optimized; writers serialize on `DB.mu`
