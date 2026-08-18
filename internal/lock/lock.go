package lock

import "errors"

var ErrLocked = errors.New("medb: database is locked by another process")
