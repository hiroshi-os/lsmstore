//go:build windows

package lsm

// syncDir is a no-op on Windows: opening a directory handle and calling
// FlushFileBuffers is not supported the same way as fsync(dirfd) on Unix.
// File Sync() of WAL/SST data still runs. Directory-entry durability is
// only claimed for Unix (see README honesty section).
func syncDir(dir string) error {
	_ = dir
	return nil
}
