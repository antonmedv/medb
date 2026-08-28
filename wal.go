package medb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/antonmedv/medb/internal/fsutil"
)

type commit struct {
	done chan struct{}
	err  error
}

func (c *commit) wait() error {
	<-c.done
	return c.err
}

type file interface {
	Write(p []byte) (int, error)
	Sync() error
	Truncate(size int64) error
	Close() error
}

func openLog(path string) (file, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	// Sync the containing directory so a successful WAL fsync cannot be
	// followed by a power loss that forgets the wal.log directory entry.
	if err := fsutil.SyncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func (db *DB) enqueue(rec []byte) *commit {
	db.logMu.Lock()
	if db.commit == nil {
		db.commit = &commit{done: make(chan struct{})}
	}
	c := db.commit
	db.pending = append(db.pending, rec)
	db.logMu.Unlock()
	select {
	case db.notify <- struct{}{}:
	default:
	}
	return c
}

func (db *DB) writeLog(err error) error {
	db.logMu.Lock()
	batch, commit := db.pending, db.commit
	db.pending, db.spare = db.spare[:0], db.pending
	db.commit = nil
	db.logMu.Unlock()
	if commit == nil {
		return err
	}
	if err == nil && len(batch) > 0 {
		db.buf = db.buf[:0]
		for _, rec := range batch {
			db.buf = append(db.buf, rec...)
			db.buf = append(db.buf, '\n')
		}
		if _, err = db.log.Write(db.buf); err == nil {
			err = db.log.Sync()
		}
		if err == nil {
			db.size.Add(int64(len(db.buf)))
		}
	}
	commit.err = err
	close(commit.done)
	clear(batch)
	return err
}

func (db *DB) replayLog(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	// A record counts only once its terminating newline is on disk, so the
	// bytes after the last newline are a torn tail and get truncated away.
	var size int64
	for n := 1; ; n++ {
		i := bytes.IndexByte(data[size:], '\n')
		if i < 0 {
			break
		}
		var rec record
		if err := json.Unmarshal(data[size:size+int64(i)], &rec); err != nil {
			return fmt.Errorf("medb: corrupt wal record %d in %s: %w", n, path, err)
		}
		if !validName(rec.Coll) {
			return fmt.Errorf("medb: corrupt wal record %d in %s: invalid collection name %q", n, path, rec.Coll)
		}
		if rec.Op == opSet && len(rec.Doc) == 0 {
			return fmt.Errorf("medb: corrupt wal record %d in %s: set record without doc", n, path)
		}
		db.apply(rec)
		size += int64(i) + 1
	}
	if size < int64(len(data)) {
		if err := db.log.Truncate(size); err != nil {
			return fmt.Errorf("medb: truncate torn wal tail in %s: %w", path, err)
		}
		if err := db.log.Sync(); err != nil {
			return fmt.Errorf("medb: sync truncated wal %s: %w", path, err)
		}
	}
	db.size.Store(size)
	return nil
}

const (
	opSet  = "set"
	opDel  = "del"
	opDrop = "drop"
)

type record struct {
	Op   string          `json:"op"`
	Coll string          `json:"coll"`
	ID   string          `json:"id,omitempty"`
	Doc  json.RawMessage `json:"doc,omitempty"`
}

func (db *DB) apply(rec record) {
	switch rec.Op {
	case opSet:
		c := db.colls[rec.Coll]
		if c == nil {
			c = map[string]json.RawMessage{}
			db.colls[rec.Coll] = c
		}
		c[rec.ID] = rec.Doc
		db.dirty[rec.Coll] = true
	case opDel:
		c := db.colls[rec.Coll]
		if _, ok := c[rec.ID]; !ok {
			return
		}
		delete(c, rec.ID)
		db.dirty[rec.Coll] = true
	case opDrop:
		if _, ok := db.colls[rec.Coll]; !ok {
			return
		}
		delete(db.colls, rec.Coll)
		db.dirty[rec.Coll] = true
	}
}
