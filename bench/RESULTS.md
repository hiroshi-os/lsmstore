# Measured results

Every number below comes from a command executed on this machine. No estimates.

## Hardware

```
date:        2026-09-26T14:43:48Z (UTC)
go:          go1.25.5 windows/amd64
os:          Microsoft Windows 11 Home Single Language
cpu:         AMD Ryzen 5 7530U with Radeon Graphics
logical CPUs: 12
memory:      23.3 GiB
disk:        local NTFS (not a claim about enterprise battery-backed controllers)
```

Historical write-RPS rows in the README / DESIGN.md (2026-09-13) were measured on a different host (Linux KVM, 4× Xeon). They are not re-stated here.

## Kill -9 crash suite

```
command:
  LSMSTORE_CRASH_RUNS=220 go test ./internal/lsm/ -run TestCrashKill9 -count=1 -timeout 60m -v

tree:        commit 9f956184591352ea3176a81648137229962f4fce
             (branch cursor/lsm-durability-ci-0424; crash suite was run on this
             tree immediately before the RESULTS.md SHA line was filled in)

result line:
  CRASH_RESULT runs=220 lost_writes=0 torn_tail_runs=0 torn_tails=0 truncated_bytes=0 acked_ops=8428

elapsed:     44.19s (test wall clock)
```

| Metric | Value |
| --- | ---: |
| kill runs | 220 |
| lost acknowledged-synced writes | 0 |
| runs that hit a torn WAL tail | 0 |
| torn tails truncated | 0 |
| truncated bytes | 0 |
| total acknowledged ops observed | 8428 |

Torn-tail count of zero on this Windows/NTFS host is a measurement, not a guarantee tears never occur. Forced torn-tail cases are covered by `TestTornWALTailTruncation` (partial header, partial payload, bad CRC, implausible length).

## Model-based property test

```
command:
  go test ./internal/lsm/ -run TestModelProperty -count=1 -timeout 15m -v -args "-rapid.checks=200" "-rapid.steps=40"

result lines:
  [rapid] OK, passed 200 tests (1m34.9009799s)
  PROPERTY_RESULT sequences=200 actions=7695

elapsed:     94.90s
```

| Metric | Value |
| --- | ---: |
| sequences (rapid checks) | 200 |
| state-machine actions | 7695 |
| result | PASS |

## Bugs found and fixed in this work

1. **WAL deleted before MANIFEST is known-durable (lost acked write under kill)**  
   `flushOne` removed WAL segments immediately after writing the MANIFEST. On Windows, directory-entry durability for the MANIFEST rename is not guaranteed (`syncDir` is a no-op). A kill after WAL unlink + rolled-back MANIFEST lost acknowledged fsynced keys (reproduced: `acked key k-05: lsm: not found` at crash run ~187).  
   **Fix:** record obsolete WAL numbers in `MANIFEST.obsolete_wal_nums` and delete those files only on the next successful `Open` after SSTables from that MANIFEST are opened.

2. **Torn WAL tail was ignored but not truncated; CRC did not cover length**  
   Incomplete or CRC-bad tails at EOF returned success without truncating the file, and CRC covered only the payload.  
   **Fix:** CRC over `len || payload`; truncate + fsync to the last good frame; count in `RecoveryInfo`; mid-file CRC with a valid suffix fails `Open`.

3. **MANIFEST / SST rename durability**  
   Manifest used `WriteFile` + rename without syncing the temp file; SST rename lacked a directory sync on Unix.  
   **Fix:** write + `Sync` temp MANIFEST before rename; `syncDir` after MANIFEST and SST rename on Unix.

## CI

GitHub Actions workflow `.github/workflows/ci.yml`: `gofmt`, `go vet`, `go test -race ./...`, short crash run (`LSMSTORE_CRASH_RUNS=20`).
