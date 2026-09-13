package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/hiroshi-os/lsmstore/internal/lsm"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dir := flag.String("dir", "./data", "database directory")
	syncWAL := flag.Bool("sync", true, "fsync WAL after every write")
	mem := flag.Int64("memtable", 4<<20, "memtable flush size in bytes")
	flag.Parse()

	db, err := lsm.Open(lsm.Options{
		Dir:          *dir,
		SyncWAL:      *syncWAL,
		MemtableSize: *mem,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/v1/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(db.Stats())
	})
	mux.HandleFunc("/v1/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := db.Put([]byte(key), body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			val, err := db.Get([]byte(key))
			if errors.Is(err, lsm.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(val)
		case http.MethodDelete:
			if err := db.Delete([]byte(key)); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("lsmstore listening on %s (dir=%s sync=%v)", *addr, *dir, *syncWAL)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>lsmstore</title>
<style>
  :root { font-family: ui-sans-serif, system-ui, sans-serif; color: #e8edf5; background: #0e141b; }
  body { max-width: 720px; margin: 2.5rem auto; padding: 0 1.25rem; }
  h1 { font-size: 1.4rem; letter-spacing: .02em; }
  p { color: #9aa8b8; line-height: 1.5; }
  form, .card { background: #18222d; border: 1px solid #2a3847; border-radius: 10px; padding: 1rem 1.1rem; margin: 1rem 0; }
  label { display: block; font-size: .8rem; color: #8b9aab; margin: .4rem 0 .2rem; }
  input, textarea { width: 100%; box-sizing: border-box; background: #0e141b; color: #e8edf5; border: 1px solid #314254; border-radius: 6px; padding: .45rem .55rem; }
  button { margin-top: .75rem; margin-right: .4rem; background: #3d8bfd; color: #041018; border: 0; border-radius: 6px; padding: .45rem .8rem; font-weight: 650; cursor: pointer; }
  button.secondary { background: #2a3847; color: #e8edf5; }
  button.danger { background: #e35d6a; color: #14080a; }
  pre { background: #0e141b; border-radius: 8px; padding: .8rem; min-height: 3rem; white-space: pre-wrap; word-break: break-all; }
  code { color: #9ad4ff; }
</style>
</head>
<body>
  <h1>lsmstore</h1>
  <p>Single-node LSM engine. Keys live in a skiplist memtable, a CRC-protected WAL, and leveled SSTables with bloom filters.</p>
  <form id="kv" onsubmit="return false">
    <label>Key</label>
    <input id="key" placeholder="user:42" />
    <label>Value</label>
    <textarea id="value" rows="3" placeholder="payload"></textarea>
    <button onclick="call('PUT')">Put</button>
    <button class="secondary" onclick="call('GET')">Get</button>
    <button class="danger" onclick="call('DELETE')">Delete</button>
    <button class="secondary" onclick="stats()">Stats</button>
  </form>
  <div class="card">
    <label>Result</label>
    <pre id="out">ready</pre>
  </div>
<script>
async function call(method) {
  const key = document.getElementById('key').value;
  const value = document.getElementById('value').value;
  const out = document.getElementById('out');
  if (!key) { out.textContent = 'key required'; return; }
  const opts = { method };
  if (method === 'PUT') opts.body = value;
  const res = await fetch('/v1/kv/' + encodeURIComponent(key), opts);
  const text = await res.text();
  out.textContent = res.status + ' ' + res.statusText + (text ? '\n' + text : '');
}
async function stats() {
  const res = await fetch('/v1/stats');
  document.getElementById('out').textContent = JSON.stringify(await res.json(), null, 2);
}
</script>
</body>
</html>
`
