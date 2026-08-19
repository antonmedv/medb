package medb

import (
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"

	"github.com/antonmedv/medb/internal/wal"
)

var errEmptyID = errors.New("medb: empty document id")

type Collection[T any] struct {
	db   *DB
	name string
}

func C[T any](db *DB, name string) *Collection[T] {
	mustValidName(name)
	return &Collection[T]{db: db, name: name}
}

func (c *Collection[T]) Get(id string) (T, error) {
	var zero T
	c.db.mu.RLock()
	if c.db.closed {
		c.db.mu.RUnlock()
		return zero, ErrClosed
	}
	raw, ok := c.db.colls[c.name][id]
	c.db.mu.RUnlock()
	if !ok {
		return zero, ErrNotFound
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, err
	}
	return v, nil
}

func (c *Collection[T]) Set(id string, doc T) error {
	if id == "" {
		return errEmptyID
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	db := c.db
	if err := db.checkDocSize(raw); err != nil {
		return err
	}
	rec := walRecord{Op: opSet, Coll: c.name, ID: id, Doc: raw}
	payload, err := encode(rec)
	if err != nil {
		return err
	}
	t, err := db.stage(rec, payload)
	if err != nil {
		return err
	}
	return db.commitWait(t)
}

func (c *Collection[T]) Delete(id string) error {
	db := c.db
	rec := walRecord{Op: opDel, Coll: c.name, ID: id}
	payload, err := encode(rec)
	if err != nil {
		return err
	}
	t, err := db.stage(rec, payload)
	if err != nil {
		return err
	}
	return db.commitWait(t)
}

// fn runs while holding the database write lock and must not block or call
// back into the DB.
func (c *Collection[T]) Update(id string, fn func(T) (T, error)) error {
	t, err := c.stageUpdate(id, fn)
	if err != nil {
		return err
	}
	return c.db.commitWait(t)
}

func (c *Collection[T]) stageUpdate(id string, fn func(T) (T, error)) (wal.Ticket, error) {
	db := c.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.writable(); err != nil {
		return wal.Ticket{}, err
	}
	raw, ok := db.colls[c.name][id]
	if !ok {
		return wal.Ticket{}, ErrNotFound
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return wal.Ticket{}, err
	}
	v, err := fn(v)
	if err != nil {
		return wal.Ticket{}, err
	}
	out, err := json.Marshal(v)
	if err != nil {
		return wal.Ticket{}, err
	}
	if err := db.checkDocSize(out); err != nil {
		return wal.Ticket{}, err
	}
	rec := walRecord{Op: opSet, Coll: c.name, ID: id, Doc: out}
	payload, err := encode(rec)
	if err != nil {
		return wal.Ticket{}, err
	}
	db.apply(rec)
	return db.log.Enqueue(payload), nil
}

func (c *Collection[T]) Has(id string) bool {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return false
	}
	_, ok := c.db.colls[c.name][id]
	return ok
}

func (c *Collection[T]) Count() int {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return 0
	}
	return len(c.db.colls[c.name])
}

func (c *Collection[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		c.db.mu.RLock()
		if c.db.closed {
			c.db.mu.RUnlock()
			return
		}
		snap := maps.Clone(c.db.colls[c.name])
		c.db.mu.RUnlock()
		for _, id := range slices.Sorted(maps.Keys(snap)) {
			var v T
			if err := json.Unmarshal(snap[id], &v); err != nil {
				panic(fmt.Sprintf("medb: decode %s/%s: %v", c.name, id, err))
			}
			if !yield(id, v) {
				return
			}
		}
	}
}
