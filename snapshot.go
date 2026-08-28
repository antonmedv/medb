package medb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/antonmedv/medb/internal/fsutil"
)

func (db *DB) writeSnapshot(err error) error {
	if err != nil {
		return err
	}
	db.mu.Lock()
	if len(db.dirty) == 0 && db.size.Load() == 0 {
		db.mu.Unlock()
		return nil
	}
	dirty := db.dirty
	db.dirty = map[string]bool{}
	// A dirty name that no longer has a collection was dropped.
	snaps := make(map[string][]byte, len(dirty))
	var dropped []string
	for name := range dirty {
		coll, ok := db.colls[name]
		if !ok {
			dropped = append(dropped, name)
			continue
		}
		b, err := json.Marshal(coll)
		if err != nil {
			db.mu.Unlock()
			return err
		}
		snaps[name] = b
	}
	db.mu.Unlock()

	for name, data := range snaps {
		path := db.collPath(name)
		if err := fsutil.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		if err := fsutil.WriteFileAtomic(path, data); err != nil {
			return err
		}
	}
	for _, name := range dropped {
		if err := db.removeColl(name); err != nil {
			return err
		}
	}
	if err := db.log.Truncate(0); err != nil {
		return err
	}
	if err := db.log.Sync(); err != nil {
		return err
	}
	db.size.Store(0)
	return nil
}

func (db *DB) removeColl(name string) error {
	path := db.collPath(name)
	switch err := os.Remove(path); {
	case errors.Is(err, fs.ErrNotExist):
		// Dropped before its first flush: there is no file, and for a
		// namespaced collection no parent directory to sync either.
		return nil
	case err != nil:
		return err
	}
	return fsutil.SyncDir(filepath.Dir(path))
}

func (db *DB) load() error {
	return filepath.WalkDir(db.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return err
		}
		rel, err := filepath.Rel(db.dir, path)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(filepath.ToSlash(rel), ".json")
		if !validName(name) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		c := map[string]json.RawMessage{}
		if err := json.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("medb: corrupt snapshot %s: %w", path, err)
		}
		db.colls[name] = c
		return nil
	})
}
