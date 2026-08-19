package medb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
			db.snapshot()
		case <-ticker.C:
			db.snapshot()
		case <-db.stop:
			db.snapshot()
			return
		}
	}
}

func (db *DB) snapshot() {
	// TODO:
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
		db.apply(rec)
	}
	// TODO: we need to snapshot?
	return nil
}
