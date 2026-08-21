package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

func TestMkdirAllSyncsEveryCreatedDirectoryEntry(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "one", "two", "three")
	var synced []string

	err := mkdirAll(target, 0o700, func(path string) error {
		synced = append(synced, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		root,
		filepath.Join(root, "one"),
		filepath.Join(root, "one", "two"),
	}
	if !slices.Equal(synced, want) {
		t.Fatalf("synced %q, want %q", synced, want)
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Fatalf("target was not created as a directory: %v", err)
	}
}

func TestMkdirAllExistingDirectoryNeedsNoParentSync(t *testing.T) {
	target := t.TempDir()
	called := false
	if err := mkdirAll(target, 0o700, func(string) error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("synced a parent although no directory entry was created")
	}
}

func TestMkdirAllReportsSyncFailure(t *testing.T) {
	boom := errors.New("sync failed")
	err := mkdirAll(filepath.Join(t.TempDir(), "new"), 0o700, func(string) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("MkdirAll = %v, want %v", err, boom)
	}
}

func TestSyncDirIgnoresUnsupportedFilesystems(t *testing.T) {
	for _, unsupported := range []error{
		syscall.EINVAL,
		syscall.ENOTSUP,
		syscall.EOPNOTSUPP,
		syscall.ENOSYS,
	} {
		t.Run(unsupported.Error(), func(t *testing.T) {
			err := syncDir(t.TempDir(), func(*os.File) error { return unsupported })
			if err != nil {
				t.Fatalf("SyncDir = %v, want best-effort success", err)
			}
		})
	}
}

func TestSyncDirReportsRealFailure(t *testing.T) {
	err := syncDir(t.TempDir(), func(*os.File) error { return syscall.EIO })
	if !errors.Is(err, ErrDirSync) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("SyncDir = %v, want ErrDirSync wrapping EIO", err)
	}
}
