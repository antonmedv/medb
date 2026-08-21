//go:build !(darwin || linux || freebsd || netbsd || openbsd)

package lock

import (
	"os"
)

func Acquire(path string) (*os.File, error) {
	return nil, ErrNotSupported
}
