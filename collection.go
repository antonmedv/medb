package medb

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
)

// Collection provides typed access to one collection in a [DB]. It encodes
// documents as JSON. Use compatible document types whenever you open the same
// stored collection.
type Collection[T any] struct {
	db   *DB
	name string
}

// C returns a typed handle for name in db. It does not create stored data.
// The first successful [Collection.Set] creates the collection.
//
// A name may contain lowercase ASCII letters, digits, hyphens, underscores,
// and slash-separated path segments. It must not exceed 240 bytes. C panics if
// the name is invalid. Document IDs may contain any string.
//
// Valid names include "users", "audit-log", "user_profiles", "2026/events",
// and "prod/eu/users".
func C[T any](db *DB, name string) *Collection[T] {
	mustValidName(name)
	return &Collection[T]{db: db, name: name}
}

// Get decodes and returns the document with id. It returns [ErrNotFound] if the
// document does not exist and [ErrClosed] if the database has closed. It
// returns a JSON decoding error if the stored document is not compatible with
// T.
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

// Set encodes and stores doc under id. It replaces any document with the same
// id. A successful return means the change is durable. Set returns
// [ErrTooLarge] if the encoded document exceeds the size limit and [ErrClosed]
// if the database has closed. It can also return a JSON encoding error.
func (c *Collection[T]) Set(id string, doc T) error {
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
	if c.db.closed {
		c.db.mu.Unlock()
		return ErrClosed
	}
	c.db.apply(rec)
	commit := c.db.enqueue(buf)
	c.db.mu.Unlock()
	return commit.wait()
}

// Delete removes the document with id. It does nothing if the document does
// not exist. A successful return means the change is durable. Delete returns
// [ErrClosed] if the database has closed.
func (c *Collection[T]) Delete(id string) error {
	rec := record{Op: opDel, Coll: c.name, ID: id}
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	c.db.mu.Lock()
	if c.db.closed {
		c.db.mu.Unlock()
		return ErrClosed
	}
	c.db.apply(rec)
	commit := c.db.enqueue(buf)
	c.db.mu.Unlock()
	return commit.wait()
}

// Update replaces the document with the value returned by fn. It returns
// [ErrNotFound] without calling fn if the document does not exist. If fn or JSON
// encoding returns an error, Update leaves the document unchanged. It also
// returns [ErrTooLarge] for an oversized result and [ErrClosed] after the
// database closes. If fn panics, Update leaves the document unchanged and
// continues the panic.
//
// Update blocks other operations on the database while fn runs. Keep fn short,
// and do not call methods on the same database from fn.
func (c *Collection[T]) Update(id string, fn func(T) (T, error)) error {
	commit, err := func() (*commit, error) {
		c.db.mu.Lock()
		defer c.db.mu.Unlock()

		if c.db.closed {
			return nil, ErrClosed
		}
		raw, ok := c.db.colls[c.name][id]
		if !ok {
			return nil, ErrNotFound
		}
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		v, err := fn(v)
		if err != nil {
			return nil, err
		}
		out, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		if err := c.db.checkDocSize(out); err != nil {
			return nil, err
		}

		rec := record{Op: opSet, Coll: c.name, ID: id, Doc: out}
		buf, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		c.db.apply(rec)
		return c.db.enqueue(buf), nil
	}()
	if err != nil {
		return err
	}
	return commit.wait()
}

// Has reports whether id exists. It reports false after the database closes.
func (c *Collection[T]) Has(id string) bool {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return false
	}
	_, ok := c.db.colls[c.name][id]
	return ok
}

// Count returns the number of documents in the collection. It returns zero
// after the database closes.
func (c *Collection[T]) Count() int {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return 0
	}
	return len(c.db.colls[c.name])
}

// All returns a sequence over the collection. Each iteration takes a snapshot,
// then yields its documents by id in ascending order. Later changes do not
// affect that iteration. The sequence yields no documents after the database
// closes and panics if a stored document cannot decode into T.
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
