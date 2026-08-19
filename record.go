package medb

import (
	"encoding/json"
	"fmt"

	"github.com/antonmedv/medb/internal/wal"
)

const (
	opSet  = "set"
	opDel  = "del"
	opDrop = "drop"
)

type walRecord struct {
	Op   string          `json:"op"`
	Coll string          `json:"coll"`
	ID   string          `json:"id,omitempty"`
	Doc  json.RawMessage `json:"doc,omitempty"`
}

func encode(rec walRecord) ([]byte, error) {
	if err := rec.valid(); err != nil {
		return nil, fmt.Errorf("medb: %w", err)
	}
	return json.Marshal(rec)
}

// The log is the one input that reaches the filesystem without going through
// C or Drop, so a replayed name must be checked before it becomes a path.
func (rec walRecord) valid() error {
	if !validName(rec.Coll) {
		return fmt.Errorf("invalid collection name %q", rec.Coll)
	}
	switch rec.Op {
	case opSet:
		if rec.ID == "" {
			return fmt.Errorf("%s record with no id", rec.Op)
		}
		if len(rec.Doc) == 0 {
			return fmt.Errorf("%s record with no document", rec.Op)
		}
	case opDel:
		if rec.ID == "" {
			return fmt.Errorf("%s record with no id", rec.Op)
		}
	case opDrop:
	default:
		return fmt.Errorf("unknown op %q", rec.Op)
	}
	return nil
}

// stage applies rec to memory and queues payload under one db.mu hold, so the
// order of records in the log matches the order of updates to the maps. It
// returns the ticket rather than waiting on it: flush takes db.mu from the
// commit goroutine, so waiting here would deadlock.
func (db *DB) stage(rec walRecord, payload []byte) (wal.Ticket, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.writable(); err != nil {
		return wal.Ticket{}, err
	}
	db.apply(rec)
	return db.log.Enqueue(payload), nil
}

func (db *DB) commitWait(t wal.Ticket) error {
	if err := t.Wait(); err != nil {
		db.fail(err)
		return err
	}
	if db.log.Size() >= db.opts.flushBytes {
		select {
		case db.flushc <- struct{}{}:
		default:
		}
	}
	return nil
}

func (db *DB) apply(rec walRecord) {
	switch rec.Op {
	case opSet:
		c := db.colls[rec.Coll]
		if c == nil {
			c = map[string]json.RawMessage{}
			db.colls[rec.Coll] = c
		}
		c[rec.ID] = rec.Doc
		db.dirty[rec.Coll] = true
		delete(db.dropped, rec.Coll)
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
		delete(db.dirty, rec.Coll)
		db.dropped[rec.Coll] = true
	}
}
