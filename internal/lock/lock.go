package lock

import "errors"

var (
	ErrLocked       = errors.New("medb: database is locked by another process")
	ErrNotSupported = errors.New("medb: file locking is not supported on this platform")
)
