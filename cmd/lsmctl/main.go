package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/hiroshi-os/lsmstore/internal/lsm"
)

func main() {
	dir := flag.String("dir", "./data", "database directory")
	syncWAL := flag.Bool("sync", true, "fsync WAL after every write")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: lsmctl [-dir DIR] put KEY VALUE | get KEY | delete KEY | stats\n")
		os.Exit(2)
	}

	db, err := lsm.Open(lsm.Options{Dir: *dir, SyncWAL: *syncWAL})
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	switch args[0] {
	case "put":
		if len(args) < 3 {
			fatal(errors.New("put KEY VALUE"))
		}
		if err := db.Put([]byte(args[1]), []byte(args[2])); err != nil {
			fatal(err)
		}
	case "get":
		if len(args) < 2 {
			fatal(errors.New("get KEY"))
		}
		val, err := db.Get([]byte(args[1]))
		if err != nil {
			fatal(err)
		}
		os.Stdout.Write(val)
		if len(val) == 0 || val[len(val)-1] != '\n' {
			fmt.Println()
		}
	case "delete":
		if len(args) < 2 {
			fatal(errors.New("delete KEY"))
		}
		if err := db.Delete([]byte(args[1])); err != nil {
			fatal(err)
		}
	case "stats":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(db.Stats())
	default:
		fatal(fmt.Errorf("unknown command %q", args[0]))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
