package medb

import (
	"encoding/json"

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

// Replay applies records in log order, so the in-memory update and the
// log enqueue must happen under one db.mu hold to keep both orders identical.
func (db *DB) enqueue(rec walRecord) (wal.Ticket, error) {
	payload, err := json.Marshal(rec)
	if err != nil {
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
		delete(db.colls[rec.Coll], rec.ID)
		db.dirty[rec.Coll] = true
	case opDrop:
		delete(db.colls, rec.Coll)
		delete(db.dirty, rec.Coll)
		db.dropped[rec.Coll] = true
	}
}
