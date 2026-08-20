package medb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
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

func readLog(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var recs [][]byte
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return recs, nil
		}
		recs = append(recs, data[:i])
		data = data[i+1:]
	}
}

func (db *DB) replayLog(path string) error {
	recs, err := readLog(path)
	if err != nil {
		return err
	}
	for i, payload := range recs {
		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("medb: corrupt wal record %d in %s: %w", i+1, path, err)
		}
		db.apply(rec)
	}
	return nil
}
