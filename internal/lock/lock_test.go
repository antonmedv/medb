//go:build darwin || linux || freebsd || netbsd || openbsd

package lock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	f, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file not created: %v", err)
	}
	if _, err := Acquire(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire: got %v, want ErrLocked", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file removed on release: %v", err)
	}
	again, err := Acquire(path)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	again.Close()
}

func TestOpenFailureIsNotErrLocked(t *testing.T) {
	_, err := Acquire(filepath.Join(t.TempDir(), "missing", "LOCK"))
	if errors.Is(err, ErrLocked) {
		t.Fatal("open failure reported as ErrLocked")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want ErrNotExist", err)
	}
}
