package fsutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var ErrDirSync = errors.New("medb: directory sync failed")

func MkdirAll(path string, perm os.FileMode) error {
	return mkdirAll(path, perm, SyncDir)
}

func mkdirAll(path string, perm os.FileMode, syncDir func(string) error) error {
	path = filepath.Clean(path)
	var parents []string
	for dir := path; ; dir = filepath.Dir(dir) {
		_, err := os.Stat(dir)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return err
		}
		parents = append(parents, parent)
	}

	if err := os.MkdirAll(path, perm); err != nil {
		return err
	}
	// Persist the hierarchy from the first existing ancestor downwards. Each
	// directory entry belongs to its parent, hence it is the parent we sync.
	for i := len(parents) - 1; i >= 0; i-- {
		if err := syncDir(parents[i]); err != nil {
			return err
		}
	}
	return nil
}

func WriteFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}

func SyncDir(path string) error {
	return syncDir(path, func(d *os.File) error { return d.Sync() })
}

func syncDir(path string, sync func(*os.File) error) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: open directory %q: %w", ErrDirSync, path, err)
	}
	if err := sync(d); err != nil && !dirSyncUnsupported(err) {
		_ = d.Close()
		return fmt.Errorf("%w: sync directory %q: %w", ErrDirSync, path, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("%w: close directory %q: %w", ErrDirSync, path, err)
	}
	return nil
}

func dirSyncUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.ENOSYS)
}
