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

func readLog(path string) ([][]byte, int64, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false, err
	}
	var recs [][]byte
	for offset := 0; offset < len(data); {
		i := bytes.IndexByte(data[offset:], '\n')
		if i < 0 {
			return recs, int64(offset), true, nil
		}
		i += offset
		recs = append(recs, data[offset:i])
		offset = i + 1
	}
	return recs, int64(len(data)), false, nil
}

func (db *DB) replayLog(path string) error {
	recs, validSize, torn, err := readLog(path)
	if err != nil {
		return err
	}
	for i, payload := range recs {
		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("medb: corrupt wal record %d in %s: %w", i+1, path, err)
		}
		if !validName(rec.Coll) {
			return fmt.Errorf("medb: corrupt wal record %d in %s: invalid collection name %q", i+1, path, rec.Coll)
		}
		if rec.Op == opSet && len(rec.Doc) == 0 {
			return fmt.Errorf("medb: corrupt wal record %d in %s: set record without doc", i+1, path)
		}
		db.apply(rec)
	}
	if torn {
		if err := db.log.Truncate(validSize); err != nil {
			return fmt.Errorf("medb: truncate torn wal tail in %s: %w", path, err)
		}
		if err := db.log.Sync(); err != nil {
			return fmt.Errorf("medb: sync truncated wal %s: %w", path, err)
		}
	}
	db.size.Store(validSize)
	return nil
}
