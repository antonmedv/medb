//go:build darwin || linux || freebsd || netbsd || openbsd

package lock

import (
	"os"
	"syscall"
)

func Acquire(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, ErrLocked
	}
	return f, nil
}

func Release(f *os.File) error {
	if err := f.Close(); err != nil {
		return err
	}
	return os.Remove(f.Name())
}
