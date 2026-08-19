package medb

import (
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
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
	db.mu.Lock()
	if err := db.writable(); err != nil {
		db.mu.Unlock()
		return err
	}
	t, err := db.enqueue(walRecord{Op: opSet, Coll: c.name, ID: id, Doc: raw})
	db.mu.Unlock()
	if err != nil {
		return err
	}
	return db.commitWait(t)
}

func (c *Collection[T]) Delete(id string) error {
	db := c.db
	db.mu.Lock()
	if err := db.writable(); err != nil {
		db.mu.Unlock()
		return err
	}
	if _, ok := db.colls[c.name][id]; !ok {
		db.mu.Unlock()
		return nil
	}
	t, err := db.enqueue(walRecord{Op: opDel, Coll: c.name, ID: id})
	db.mu.Unlock()
	if err != nil {
		return err
	}
	return db.commitWait(t)
}

// fn runs while holding the database write lock and must not call back into the DB.
func (c *Collection[T]) Update(id string, fn func(T) (T, error)) error {
	db := c.db
	db.mu.Lock()
	if err := db.writable(); err != nil {
		db.mu.Unlock()
		return err
	}
	raw, ok := db.colls[c.name][id]
	if !ok {
		db.mu.Unlock()
		return ErrNotFound
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		db.mu.Unlock()
		return err
	}
	v, err := fn(v)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	out, err := json.Marshal(v)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if err := db.checkDocSize(out); err != nil {
		db.mu.Unlock()
		return err
	}
	t, err := db.enqueue(walRecord{Op: opSet, Coll: c.name, ID: id, Doc: out})
	db.mu.Unlock()
	if err != nil {
		return err
	}
	return db.commitWait(t)
}

func (c *Collection[T]) Has(id string) bool {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	_, ok := c.db.colls[c.name][id]
	return ok
}

func (c *Collection[T]) Count() int {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	return len(c.db.colls[c.name])
}

func (c *Collection[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		c.db.mu.RLock()
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
