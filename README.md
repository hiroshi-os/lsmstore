# lsmstore

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

Requires Go 1.22+.

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

## Measured write RPS

Numbers are from `cmd/lsmbench` on the hardware recorded in [DESIGN.md](DESIGN.md). They are not estimates.

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

## Tests

```bash
go test ./internal/lsm/ -count=1
```

Coverage includes the skiplist memtable, bloom false-negative / encode path, WAL recovery, L0 flush, and the L0→L1 compaction merge (newest seq wins, tombstones on the last level).

## Layout

| Path | Role |
| --- | --- |
| `internal/lsm` | Engine: memtable, WAL, SSTable, bloom, version, compaction, DB |
| `cmd/lsmstore` | HTTP demo |
| `cmd/lsmctl` | CLI |
| `cmd/lsmbench` | Write RPS harness (real wall-clock) |
| `DESIGN.md` | Compaction, bloom, WAL, file format, measurements |

Raft attach is intentionally out of scope. The `DB` type is a local engine; a consensus layer could sit in front of `Put`/`Delete` later.
