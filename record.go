package medb

import (
	"encoding/json"
)

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
