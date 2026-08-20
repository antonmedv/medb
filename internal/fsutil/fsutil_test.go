package fsutil_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/antonmedv/medb/internal/fsutil"
)

func read(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestWriteFile(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello")},
		{"binary", []byte{0, 1, 2, 0xff, 0, 0xfe}},
		{"large", bytes.Repeat([]byte("0123456789abcdef"), 1<<16)}, // 1 MiB
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data")
			if err := fsutil.WriteFileAtomic(path, tc.data); err != nil {
				t.Fatal(err)
			}
			if got := read(t, path); !bytes.Equal(got, tc.data) {
				t.Fatalf("read back %d bytes, wrote %d", len(got), len(tc.data))
			}
			if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("tmp file left behind: %v", err)
			}
		})
	}
}

func TestWriteFileOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	if err := fsutil.WriteFileAtomic(path, []byte("old and longer")); err != nil {
		t.Fatal(err)
	}
	if err := fsutil.WriteFileAtomic(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, path); string(got) != "new" {
		t.Fatalf("got %q, want %q", got, "new")
	}
}

// A failed write must leave the previous contents intact: callers recover by
// reading the old file, so a half-written or truncated target is unacceptable.
func TestWriteFileFailureKeepsOldContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	old := []byte("old")
	if err := fsutil.WriteFileAtomic(path, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path+".tmp", 0o700); err != nil { // makes creating the temp file fail
		t.Fatal(err)
	}
	if err := fsutil.WriteFileAtomic(path, []byte("new")); err == nil {
		t.Fatal("want error, got nil")
	}
	if got := read(t, path); !bytes.Equal(got, old) {
		t.Fatalf("target clobbered: got %q, want %q", got, old)
	}
}

func TestWriteFileRenameFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(path, 0o700); err != nil { // makes os.Rename fail
		t.Fatal(err)
	}
	if err := fsutil.WriteFileAtomic(path, []byte("new")); err == nil {
		t.Fatal("want error, got nil")
	}
}

func TestWriteFileMissingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "data")
	err := fsutil.WriteFileAtomic(path, []byte("x"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want ErrNotExist", err)
	}
}

func TestPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	if err := fsutil.WriteFileAtomic(path, []byte("x")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("mode %v, want no group or world access", perm)
	}
}

func TestSyncDir(t *testing.T) {
	if err := fsutil.SyncDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestSyncDirMissing(t *testing.T) {
	err := fsutil.SyncDir(filepath.Join(t.TempDir(), "missing"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want ErrNotExist", err)
	}
}
