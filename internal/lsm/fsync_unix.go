//go:build !windows

package lsm

import (
	"fmt"
	"os"
)

// syncDir persists directory entries (create, rename, unlink) for dir.
// A file fsync does not make the file's name durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	serr := d.Sync()
	cerr := d.Close()
	if serr != nil {
		return fmt.Errorf("sync dir %s: %w", dir, serr)
	}
	if cerr != nil {
		return fmt.Errorf("sync dir %s: %w", dir, cerr)
	}
	return nil
}
