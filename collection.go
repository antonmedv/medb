package medb

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
)

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
	c.db.mu.RLock()
	closed := c.db.closed
	c.db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := c.db.checkDocSize(raw); err != nil {
		return err
	}
	rec := record{Op: opSet, Coll: c.name, ID: id, Doc: raw}
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	c.db.mu.Lock()
	c.db.apply(rec)
	commit := c.db.enqueue(buf)
	c.db.mu.Unlock()
	return commit.wait()
}

func (c *Collection[T]) Delete(id string) error {
	c.db.mu.RLock()
	closed := c.db.closed
	c.db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	rec := record{Op: opDel, Coll: c.name, ID: id}
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	c.db.mu.Lock()
	c.db.apply(rec)
	commit := c.db.enqueue(buf)
	c.db.mu.Unlock()
	return commit.wait()
}

func (c *Collection[T]) Update(id string, fn func(T) (T, error)) error {
	c.db.mu.Lock()
	if c.db.closed {
		c.db.mu.Unlock()
		return ErrClosed
	}
	raw, ok := c.db.colls[c.name][id]
	if !ok {
		c.db.mu.Unlock()
		return ErrNotFound
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		c.db.mu.Unlock()
		return err
	}
	v, err := fn(v)
	if err != nil {
		c.db.mu.Unlock()
		return err
	}
	out, err := json.Marshal(v)
	if err != nil {
		c.db.mu.Unlock()
		return err
	}
	if err := c.db.checkDocSize(out); err != nil {
		c.db.mu.Unlock()
		return err
	}

	rec := record{Op: opSet, Coll: c.name, ID: id, Doc: out}
	buf, err := json.Marshal(rec)
	if err != nil {
		c.db.mu.Unlock()
		return err
	}
	c.db.apply(rec)
	commit := c.db.enqueue(buf)
	c.db.mu.Unlock()
	return commit.wait()
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
