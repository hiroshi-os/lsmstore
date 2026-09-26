package lsm

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var crashRuns = flag.Int("crash.runs", 5, "kill -9 iterations for TestCrashKill9")

func TestMain(m *testing.M) {
	if os.Getenv("LSMSTORE_CRASH_CHILD") == "1" {
		if err := crashChildMain(); err != nil {
			fmt.Fprintln(os.Stderr, "crash-child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func crashRunCount() int {
	if v := os.Getenv("LSMSTORE_CRASH_RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n >= 1 {
			return n
		}
	}
	return *crashRuns
}

func crashChildMain() error {
	dir := os.Getenv("LSMSTORE_CRASH_DIR")
	if dir == "" {
		return errors.New("LSMSTORE_CRASH_DIR is empty")
	}
	db, err := Open(Options{
		Dir:                 dir,
		SyncWAL:             true,
		MemtableSize:        2048,
		L0CompactionTrigger: 2,
		BaseLevelSize:       4096,
		LevelSizeMultiplier: 10,
		TargetFileSize:      2048,
		BloomBitsPerKey:     10,
		BlockSize:           128,
		MaxLevels:           4,
		CompactInterval:     5 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer db.Close()
	if err := crashWriteLine("READY\n"); err != nil {
		return err
	}
	const keySpace = 24
	for i := 0; ; i++ {
		id := i + 1
		key := fmt.Sprintf("k-%02d", i%keySpace)
		if i%5 == 4 {
			if err := crashWriteLine(fmt.Sprintf("B %d D %s\n", id, key)); err != nil {
				return err
			}
			if err := db.Delete([]byte(key)); err != nil {
				return err
			}
			if err := crashWriteLine(fmt.Sprintf("A %d\n", id)); err != nil {
				return err
			}
		} else {
			val := fmt.Sprintf("v-%d-%s", i, strings.Repeat("x", 64+i%128))
			if err := crashWriteLine(fmt.Sprintf("B %d P %s %s\n", id, key, val)); err != nil {
				return err
			}
			if err := db.Put([]byte(key), []byte(val)); err != nil {
				return err
			}
			if err := crashWriteLine(fmt.Sprintf("A %d\n", id)); err != nil {
				return err
			}
		}
		if i%20 == 19 {
			if err := db.Flush(); err != nil {
				return err
			}
		}
	}
}

func crashWriteLine(s string) error {
	_, err := os.Stdout.Write([]byte(s))
	return err
}

type crashOp struct {
	id  int
	del bool
	key string
	val string
}

func TestCrashKill9(t *testing.T) {
	runs := crashRunCount()
	if runs < 1 {
		t.Fatal("crash run count must be >= 1")
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var (
		lost           int
		tornRuns       int
		tornTails      int
		ackedOps       int
		truncatedBytes int64
	)
	for i := 0; i < runs; i++ {
		delay := crashDelay(rng)
		acks, torn, trunc, err := crashOnce(t, delay)
		if err != nil {
			t.Fatalf("run %d delay %s: %v", i, delay, err)
		}
		ackedOps += acks
		tornTails += torn
		truncatedBytes += trunc
		if torn > 0 {
			tornRuns++
		}
		t.Logf("run %d acks=%d torn=%d truncated_bytes=%d delay=%s", i, acks, torn, trunc, delay)
	}
	if lost != 0 {
		t.Fatalf("lost acknowledged writes: %d", lost)
	}
	t.Logf("CRASH_RESULT runs=%d lost_writes=%d torn_tail_runs=%d torn_tails=%d truncated_bytes=%d acked_ops=%d",
		runs, lost, tornRuns, tornTails, truncatedBytes, ackedOps)
}

func crashDelay(rng *rand.Rand) time.Duration {
	switch rng.Intn(10) {
	case 0, 1:
		return time.Duration(rng.Intn(5)) * time.Millisecond
	case 2, 3, 4, 5, 6:
		return time.Duration(5+rng.Intn(80)) * time.Millisecond
	default:
		return time.Duration(80+rng.Intn(400)) * time.Millisecond
	}
}

func crashOnce(t *testing.T, delay time.Duration) (acks int, torn int, truncated int64, err error) {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^$", "-test.v=false")
	cmd.Env = append(os.Environ(), "LSMSTORE_CRASH_CHILD=1", "LSMSTORE_CRASH_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, 0, 0, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()

	ready := make(chan struct{})
	var (
		mu    sync.Mutex
		lines []string
		once  sync.Once
	)
	readDone := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if line == "READY" {
				once.Do(func() { close(ready) })
				continue
			}
			mu.Lock()
			lines = append(lines, line)
			mu.Unlock()
		}
		readDone <- sc.Err()
	}()

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	select {
	case <-ready:
	case err := <-waitCh:
		return 0, 0, 0, fmt.Errorf("child exited before READY: %v stderr=%s", err, stderr.String())
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-waitCh
		return 0, 0, 0, fmt.Errorf("timeout waiting for READY stderr=%s", stderr.String())
	}

	time.Sleep(delay)
	if err := cmd.Process.Kill(); err != nil {
		// Already exited is fine.
		select {
		case waitErr := <-waitCh:
			if waitErr != nil && !killedOK(waitErr) {
				<-readDone
				return 0, 0, 0, fmt.Errorf("child failed: %v stderr=%s", waitErr, stderr.String())
			}
		default:
		}
	}
	waitErr := <-waitCh
	if waitErr != nil && !killedOK(waitErr) {
		<-readDone
		return 0, 0, 0, fmt.Errorf("child failed: %v stderr=%s", waitErr, stderr.String())
	}
	if rerr := <-readDone; rerr != nil && !ignorePipeErr(rerr) {
		return 0, 0, 0, rerr
	}

	mu.Lock()
	gotLines := append([]string(nil), lines...)
	mu.Unlock()

	begins, ackIDs, err := parseCrashLines(gotLines)
	if err != nil {
		return 0, 0, 0, err
	}

	tornBefore, err := scanDirTornWALs(dir)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("pre-scan: %w", err)
	}

	db, err := Open(Options{
		Dir:             dir,
		SyncWAL:         true,
		CompactInterval: time.Hour,
	})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("reopen: %w", err)
	}
	defer db.Close()

	info := db.RecoveryInfo()
	if info.TornTails != tornBefore {
		return 0, 0, 0, fmt.Errorf("torn tails scanner=%d recovery=%+v", tornBefore, info)
	}
	tornAfter, err := scanDirTornWALs(dir)
	if err != nil {
		return 0, 0, 0, err
	}
	if tornAfter != 0 {
		return 0, 0, 0, fmt.Errorf("torn tail survived recovery (%d files)", tornAfter)
	}
	if err := verifyCrash(db, begins, ackIDs); err != nil {
		return 0, 0, 0, err
	}
	return len(ackIDs), info.TornTails, info.TruncatedBytes, nil
}

func ignorePipeErr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "file already closed") || strings.Contains(msg, "broken pipe")
}

func killedOK(err error) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok {
		if ws.Signaled() && (ws.Signal() == syscall.SIGKILL || ws.Signal() == syscall.SIGTERM) {
			return true
		}
		if ws.ExitStatus() != 0 {
			// Windows TerminateProcess often surfaces as a non-zero exit.
			return true
		}
	}
	return ee.ExitCode() != 0
}

func parseCrashLines(lines []string) (map[int]crashOp, []int, error) {
	begins := map[int]crashOp{}
	var acks []int
	seenAck := map[int]bool{}
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			if i == len(lines)-1 {
				continue
			}
			return nil, nil, fmt.Errorf("bad crash line %q", line)
		}
		id, err := strconv.Atoi(fields[1])
		if err != nil {
			if i == len(lines)-1 {
				continue
			}
			return nil, nil, fmt.Errorf("bad crash id %q", line)
		}
		switch fields[0] {
		case "B":
			op := crashOp{id: id}
			switch {
			case len(fields) == 4 && fields[2] == "D":
				op.del = true
				op.key = fields[3]
			case len(fields) == 5 && fields[2] == "P":
				op.key = fields[3]
				op.val = fields[4]
			default:
				if i == len(lines)-1 {
					continue
				}
				return nil, nil, fmt.Errorf("bad begin %q", line)
			}
			if _, ok := begins[id]; ok {
				return nil, nil, fmt.Errorf("duplicate begin %d", id)
			}
			begins[id] = op
		case "A":
			if len(fields) != 2 {
				if i == len(lines)-1 {
					continue
				}
				return nil, nil, fmt.Errorf("bad ack %q", line)
			}
			if _, ok := begins[id]; !ok {
				return nil, nil, fmt.Errorf("ack %d without begin", id)
			}
			if seenAck[id] {
				return nil, nil, fmt.Errorf("duplicate ack %d", id)
			}
			seenAck[id] = true
			acks = append(acks, id)
		default:
			if i == len(lines)-1 {
				continue
			}
			return nil, nil, fmt.Errorf("bad crash line %q", line)
		}
	}
	return begins, acks, nil
}

func verifyCrash(db *DB, begins map[int]crashOp, acks []int) error {
	acked := make([]crashOp, 0, len(acks))
	ackedSet := map[int]bool{}
	for _, id := range acks {
		op, ok := begins[id]
		if !ok {
			return fmt.Errorf("missing begin for ack %d", id)
		}
		acked = append(acked, op)
		ackedSet[id] = true
	}
	if err := diffCrash(db, acked); err == nil {
		return nil
	} else if len(begins) == len(acked) {
		return err
	}
	var pending []crashOp
	for id, op := range begins {
		if !ackedSet[id] {
			pending = append(pending, op)
		}
	}
	// One in-flight begin is allowed: Put/Delete may have fsynced and the
	// ack line may not have reached the parent before kill.
	if len(pending) == 1 {
		if err2 := diffCrash(db, append(append([]crashOp{}, acked...), pending[0])); err2 == nil {
			return nil
		}
	}
	if len(pending) > 1 {
		return fmt.Errorf("%d unacked begins; acked=%d first mismatch: %w", len(pending), len(acked), diffCrash(db, acked))
	}
	return diffCrash(db, acked)
}

func diffCrash(db *DB, ops []crashOp) error {
	type cell struct {
		val string
	}
	model := map[string]*cell{}
	seen := map[string]struct{}{}
	for _, op := range ops {
		seen[op.key] = struct{}{}
		if op.del {
			delete(model, op.key)
			continue
		}
		model[op.key] = &cell{val: op.val}
	}
	for k, c := range model {
		got, err := db.Get([]byte(k))
		if err != nil {
			return fmt.Errorf("acked key %s: %w", k, err)
		}
		if string(got) != c.val {
			return fmt.Errorf("acked key %s: got %q want %q", k, got, c.val)
		}
	}
	for k := range seen {
		if _, ok := model[k]; ok {
			continue
		}
		_, err := db.Get([]byte(k))
		if !errors.Is(err, ErrNotFound) {
			if err == nil {
				return fmt.Errorf("deleted key %s still present", k)
			}
			return fmt.Errorf("deleted key %s: %w", k, err)
		}
	}
	if _, err := db.Get([]byte("phantom-key")); !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("phantom key: %v", err)
	}
	return nil
}
