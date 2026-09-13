package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/lsmstore/internal/lsm"
)

func main() {
	dir := flag.String("dir", "", "database directory (default: temp)")
	n := flag.Int("n", 50000, "number of writes")
	valueSize := flag.Int("value", 64, "value size in bytes")
	workers := flag.Int("workers", 1, "concurrent writers")
	syncWAL := flag.Bool("sync", false, "fsync WAL after every write")
	mem := flag.Int64("memtable", 4<<20, "memtable size")
	flag.Parse()

	workDir := *dir
	if workDir == "" {
		tmp, err := os.MkdirTemp("", "lsmbench-")
		if err != nil {
			fatal(err)
		}
		defer os.RemoveAll(tmp)
		workDir = tmp
	}

	db, err := lsm.Open(lsm.Options{
		Dir:          workDir,
		SyncWAL:      *syncWAL,
		MemtableSize: *mem,
	})
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	value := bytesRepeat(*valueSize, 'x')
	lat := make([]time.Duration, *n)
	var next atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= *n {
					return
				}
				key := []byte(fmt.Sprintf("k-%08d", i))
				t0 := time.Now()
				if err := db.Put(key, value); err != nil {
					fatal(err)
				}
				lat[i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	rps := float64(*n) / elapsed.Seconds()

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p50 := lat[len(lat)*50/100]
	p99 := lat[len(lat)*99/100]

	// Point-read a subset after writes (includes memtable + any flushed SSTs).
	reads := *n
	if reads > 5000 {
		reads = 5000
	}
	rstart := time.Now()
	for i := 0; i < reads; i++ {
		key := []byte(fmt.Sprintf("k-%08d", i*(*n/reads)))
		if _, err := db.Get(key); err != nil {
			fatal(err)
		}
	}
	readRPS := float64(reads) / time.Since(rstart).Seconds()

	cpu := cpuModel()
	fmt.Printf("lsmstore write bench\n")
	fmt.Printf("  date        %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Printf("  go          %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Printf("  cpu         %s\n", cpu)
	fmt.Printf("  gomaxprocs  %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("  writes      %d  value=%dB  workers=%d  sync_wal=%v  memtable=%d\n",
		*n, *valueSize, *workers, *syncWAL, *mem)
	fmt.Printf("  elapsed     %s\n", elapsed)
	fmt.Printf("  write_rps   %.0f\n", rps)
	fmt.Printf("  write_p50   %s\n", p50)
	fmt.Printf("  write_p99   %s\n", p99)
	fmt.Printf("  read_rps    %.0f  (%d gets)\n", readRPS, reads)
	st := db.Stats()
	fmt.Printf("  flushes     %d  compactions=%d  files/level=%v\n",
		st.Flushes, st.Compactions, st.FilesPerLevel)
}

func bytesRepeat(n int, c byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return b
}

func cpuModel() string {
	if runtime.GOOS != "linux" {
		return runtime.GOARCH
	}
	out, err := exec.Command("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").Output()
	if err != nil {
		return runtime.GOARCH
	}
	s := strings.TrimSpace(string(out))
	if s == "" {
		return runtime.GOARCH
	}
	return s
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
