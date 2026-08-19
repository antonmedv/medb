package medb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/antonmedv/medb/internal/lock"
	"github.com/antonmedv/medb/internal/wal"
)

const (
	walName  = "wal.log"
	lockName = "LOCK"
)

var (
	ErrNotFound = errors.New("medb: document not found")
	ErrLocked   = lock.ErrLocked
	ErrClosed   = errors.New("medb: database is closed")
	ErrTooLarge = errors.New("medb: document exceeds size limit")
)

type Option func(*options)

type options struct {
	maxDocSize    int
	flushBytes    int64
	flushInterval time.Duration
}

func WithMaxDocSize(n int) Option {
	return func(o *options) { o.maxDocSize = n }
}

func WithFlushBytes(n int64) Option {
	return func(o *options) { o.flushBytes = n }
}

func WithFlushInterval(d time.Duration) Option {
	return func(o *options) { o.flushInterval = d }
}

func (o options) validate() error {
	if o.maxDocSize <= 0 {
		return fmt.Errorf("medb: max document size must be positive, got %d", o.maxDocSize)
	}
	if o.flushBytes <= 0 {
		return fmt.Errorf("medb: flush threshold must be positive, got %d", o.flushBytes)
	}
	if o.flushInterval <= 0 {
		return fmt.Errorf("medb: flush interval must be positive, got %s", o.flushInterval)
	}
	return nil
}

type DB struct {
	dir  string
	opts options

	mu      sync.RWMutex
	colls   map[string]map[string]json.RawMessage
	dirty   map[string]bool
	dropped map[string]bool
	closed  bool
	failed  error

	log    *wal.Log
	flock  *os.File
	flushc chan struct{}
	stop   chan struct{}
	done   sync.WaitGroup
}

func Open(dir string, opts ...Option) (*DB, error) {
	o := options{
		maxDocSize:    16 << 20,
		flushBytes:    64 << 20,
		flushInterval: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	flock, err := lock.Acquire(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	log, err := wal.Open(filepath.Join(dir, walName))
	if err != nil {
		flock.Close()
		return nil, err
	}
	db := &DB{
		dir:     filepath.Clean(dir),
		opts:    o,
		colls:   map[string]map[string]json.RawMessage{},
		dirty:   map[string]bool{},
		dropped: map[string]bool{},
		log:     log,
		flock:   flock,
		flushc:  make(chan struct{}, 1),
		stop:    make(chan struct{}),
	}
	if err := db.load(); err != nil {
		log.Close()
		flock.Close()
		return nil, err
	}
	db.done.Add(1)
	go db.run()
	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.mu.Unlock()

	// The lock must be free here: the final flush runs on the flusher
	// goroutine and takes db.mu, so waiting for it while holding the lock
	// deadlocks.
	close(db.stop)
	db.done.Wait()

	err := db.log.Close()
	if e := db.flock.Close(); err == nil {
		err = e
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.failed != nil {
		err = db.failed
	}
	clear(db.colls)
	clear(db.dirty)
	clear(db.dropped)
	return err
}

func (db *DB) Collections() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil
	}
	return slices.Sorted(maps.Keys(db.colls))
}

func (db *DB) Drop(name string) error {
	mustValidName(name)
	rec := walRecord{Op: opDrop, Coll: name}
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

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func (db *DB) writable() error {
	if db.closed {
		return ErrClosed
	}
	return db.failed
}

func (db *DB) fail(err error) {
	if err == nil {
		return
	}
	db.mu.Lock()
	if db.failed == nil {
		db.failed = err
	}
	db.mu.Unlock()
}

func (db *DB) checkDocSize(raw []byte) error {
	if len(raw) > db.opts.maxDocSize {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, len(raw), db.opts.maxDocSize)
	}
	return nil
}

func (db *DB) collPath(name string) string {
	return filepath.Join(db.dir, filepath.FromSlash(name)+".json")
}

func (db *DB) walPath() string {
	return filepath.Join(db.dir, walName)
}

func validName(name string) bool {
	seg := 0
	for i := range len(name) {
		switch c := name[i]; {
		case c == '/':
			if seg == 0 {
				return false
			}
			seg = 0
		case 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '_', c == '-':
			seg++
		default:
			return false
		}
	}
	return seg > 0
}

func mustValidName(name string) {
	if !validName(name) {
		panic(fmt.Sprintf("medb: invalid collection name %q", name))
	}
}
