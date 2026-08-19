//go:build darwin || linux || freebsd || netbsd || openbsd

package lock_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/antonmedv/medb/internal/lock"
)

func TestAcquire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	f, err := lock.Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file not created: %v", err)
	}
	if _, err := lock.Acquire(path); !errors.Is(err, lock.ErrLocked) {
		t.Fatalf("second Acquire: got %v, want ErrLocked", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file removed on release: %v", err)
	}
	again, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	again.Close()
}

func TestOpenFailureIsNotErrLocked(t *testing.T) {
	_, err := lock.Acquire(filepath.Join(t.TempDir(), "missing", "LOCK"))
	if errors.Is(err, lock.ErrLocked) {
		t.Fatal("open failure reported as ErrLocked")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want ErrNotExist", err)
	}
}
