//go:build !(darwin || linux || freebsd || netbsd || openbsd)

package lock

import (
	"errors"
	"os"
)

func Acquire(path string) (*os.File, error) {
	return nil, errors.New("medb: file locking is not supported on this platform")
}
