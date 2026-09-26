# lsmstore

[![ci](https://github.com/hiroshi-os/lsmstore/actions/workflows/ci.yml/badge.svg)](https://github.com/hiroshi-os/lsmstore/actions/workflows/ci.yml)

A single-node LSM (Log-Structured Merge-tree) storage engine written in Go. No wrappers, no Raft — just the write path that actually matters: a skiplist memtable, a CRC-checked WAL, block-indexed SSTables, bloom filters, and leveled compaction.

```
Put/Delete ──► WAL (fsync optional) ──► skiplist memtable
                                            │ flush
                                            ▼
                                      L0 SSTables  ── leveled compact ──► L1…Ln
Get ◄── memtable, immutable tables, L0 (newest first), then one file per level
         (bloom + restart index skip files and blocks that cannot contain the key)
```

## Build

Requires Go 1.23+.

```bash
make build          # bin/lsmstore  bin/lsmctl  bin/lsmbench
make test
```

## Demo: HTTP

```bash
./bin/lsmstore -addr :8080 -dir ./data
```

Open http://localhost:8080 for a small Put/Get/Delete UI, or:

```bash
curl -X PUT  --data 'world' http://localhost:8080/v1/kv/hello
curl          http://localhost:8080/v1/kv/hello          # world
curl -X DELETE http://localhost:8080/v1/kv/hello
curl          http://localhost:8080/v1/stats
```

## Demo: CLI

`lsmctl` opens the directory directly (do not point it at a dir a server already has open).

```bash
./bin/lsmctl -dir ./data put user:1 alice
./bin/lsmctl -dir ./data get user:1
./bin/lsmctl -dir ./data delete user:1
./bin/lsmctl -dir ./data stats
```

## Docker (optional)

```bash
docker build -t lsmstore .
docker run --rm -p 8080:8080 -v lsmdata:/data lsmstore
```

## Durability: kill -9 crash suite

A child process opens the store with `SyncWAL=true`, writes Put/Delete, and prints an ack line **only after** `Put`/`Delete` returns (which fsyncs the WAL). The parent kills the child (`Process.Kill` / SIGKILL), reopens the directory, and checks:

1. every acknowledged fsynced write is still present (or correctly deleted)
2. no corrupt / phantom record is returned
3. any torn WAL tail is detected via CRC and truncated (`RecoveryInfo`)

Command (this is what produced the numbers below):

```bash
LSMSTORE_CRASH_RUNS=220 go test ./internal/lsm/ -run TestCrashKill9 -count=1 -timeout 60m -v
```

| Metric | Value |
| --- | ---: |
| kill runs | **220** |
| lost acknowledged-synced writes | **0** |
| runs that hit a torn WAL tail | **0** |
| torn tails truncated | **0** |
| acknowledged ops in those runs | **8428** |

Hardware, date, commit SHA, and the full log summary live in [bench/RESULTS.md](bench/RESULTS.md).

## Model-based property test

Random Put/Get/Delete/flush/compact/reopen sequences are checked against a Go `map` using [`pgregory.net/rapid`](https://pkg.go.dev/pgregory.net/rapid):

```bash
go test ./internal/lsm/ -run TestModelProperty -count=1 -timeout 15m -v -args -rapid.checks=200 -rapid.steps=40
```

| Metric | Value |
| --- | ---: |
| sequences (rapid checks) | **200** |
| state-machine actions | **7695** |
| result | PASS |

## Measured write RPS

Numbers are from `cmd/lsmbench` on the hardware recorded in [DESIGN.md](DESIGN.md) / [bench/RESULTS.md](bench/RESULTS.md). They are not estimates.

| Mode | Writes | Value | Workers | Write RPS | Hardware / date |
| --- | ---: | ---: | ---: | ---: | --- |
| WAL buffered (`-sync=false`) | 50,000 | 64 B | 1 | **514,249** | 4× Xeon (KVM), Linux 6.12, 2026-09-13 UTC |
| WAL fsync (`-sync=true`) | 20,000 | 64 B | 1 | **10,350** | same host, 2026-09-13 UTC |

```bash
make bench
# or:
./bin/lsmbench -n 50000 -value 64 -workers 1
./bin/lsmbench -n 20000 -value 64 -workers 1 -sync
```

## Tests / CI

```bash
go test ./...
go test -race ./...
LSMSTORE_CRASH_RUNS=20 go test ./internal/lsm/ -run TestCrashKill9 -count=1 -v
```

GitHub Actions (`.github/workflows/ci.yml`) runs `gofmt`, `go vet`, `go test -race ./...`, and a short crash suite (`LSMSTORE_CRASH_RUNS=20`).

Coverage includes the skiplist memtable, bloom false-negative / encode path, WAL recovery with torn-tail truncation, L0 flush, L0→L1 compaction, model-based properties, and kill-9 crash recovery.

## Honesty — what is NOT guaranteed

- **`SyncWAL=false`**: acknowledged `Put`/`Delete` returns are **not** durable across power loss or kill.
- **Windows directory sync**: `syncDir` is a no-op on Windows; file `Sync()` still runs for WAL/SST/MANIFEST contents. Rename/create durability is claimed for Unix (CI).
- **Group commit / multi-writer throughput**: writers serialize on `DB.mu`; no batched fsync.
- **SST data-block checksums**: SSTables are not CRC'd per block; only the WAL frames carry CRC.
- **Torn-tail frequency under kill**: depends on OS, filesystem, and timing. A measured **0** torn tails on a host is not a claim that tears never happen; unit tests still force truncation paths.
- **Multi-process / multi-host**: one live opener per directory; no file locking, no Raft.
- **Range scans, snapshots, transactions, compression**: not implemented.
- **README write-RPS rows from 2026-09-13**: those are historical measurements on a Linux KVM guest (see DESIGN.md); they were not re-run in the durability PR unless listed again in `bench/RESULTS.md`.

## Layout

| Path | Role |
| --- | --- |
| `internal/lsm` | Engine: memtable, WAL, SSTable, bloom, version, compaction, DB |
| `cmd/lsmstore` | HTTP demo |
| `cmd/lsmctl` | CLI |
| `cmd/lsmbench` | Write RPS harness (real wall-clock) |
| `bench/RESULTS.md` | Recorded crash / property / bench runs |
| `DESIGN.md` | Compaction, bloom, WAL, file format, measurements |
| `.github/workflows/ci.yml` | gofmt, vet, race, short crash test |

Raft attach is intentionally out of scope. The `DB` type is a local engine; a consensus layer could sit in front of `Put`/`Delete` later.
