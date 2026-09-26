package lsm

import (
	"io"
	"os"
)

func writeFull(f *os.File, p []byte) error {
	for len(p) > 0 {
		n, err := f.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
