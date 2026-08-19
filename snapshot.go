package medb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/antonmedv/medb/internal/fsutil"
	"github.com/antonmedv/medb/internal/wal"
)

func (db *DB) run() {
	defer db.done.Done()
	ticker := time.NewTicker(db.opts.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.flushc:
			db.tryFlush()
		case <-ticker.C:
			db.tryFlush()
		case <-db.stop:
			db.tryFlush()
			return
		}
	}
}

func (db *DB) tryFlush() {
	db.mu.RLock()
	bad := db.failed != nil
	db.mu.RUnlock()
	if bad || db.log.Size() == 0 {
		return
	}
	db.fail(db.log.Exec(db.flush))
}

func (db *DB) flush() error {
	db.mu.Lock()
	if db.failed != nil {
		err := db.failed
		db.mu.Unlock()
		return err
	}
	if err := db.log.Drain(); err != nil {
		db.mu.Unlock()
		return err
	}
	snaps := make(map[string]map[string]json.RawMessage, len(db.dirty))
	for name := range db.dirty {
		if c, ok := db.colls[name]; ok {
			snaps[name] = maps.Clone(c)
		}
	}
	dropped := slices.Sorted(maps.Keys(db.dropped))
	clear(db.dirty)
	clear(db.dropped)
	db.mu.Unlock()

	for _, name := range slices.Sorted(maps.Keys(snaps)) {
		data, err := json.Marshal(snaps[name])
		if err != nil {
			return err
		}
		if err := db.writeSnapshot(name, data); err != nil {
			return err
		}
	}
	for _, name := range dropped {
		if err := db.removeSnapshot(name); err != nil {
			return err
		}
	}
	return db.log.Truncate()
}

func (db *DB) removeSnapshot(name string) error {
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

func (db *DB) writeSnapshot(name string, data []byte) error {
	path := db.collPath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := fsutil.WriteFile(path, data); err != nil {
		return err
	}
	for d := filepath.Dir(path); d != db.dir; {
		parent := filepath.Dir(d)
		if parent == d {
			return fmt.Errorf("medb: snapshot path %s escapes %s", path, db.dir)
		}
		d = parent
		if err := fsutil.SyncDir(d); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) load() error {
	err := filepath.WalkDir(db.dir, func(path string, d fs.DirEntry, err error) error {
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
	if err != nil {
		return err
	}
	return db.replay()
}

func (db *DB) replay() error {
	recs, err := wal.Records(db.walPath())
	if err != nil {
		return err
	}
	for i, payload := range recs {
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("medb: corrupt wal record %d in %s: %w", i+1, db.walPath(), err)
		}
		if err := rec.valid(); err != nil {
			return fmt.Errorf("medb: invalid wal record %d in %s: %w", i+1, db.walPath(), err)
		}
		db.apply(rec)
	}
	if db.log.Size() > 0 {
		return db.log.Exec(db.flush)
	}
	return nil
}
