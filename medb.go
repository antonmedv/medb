package medb

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonmedv/medb/internal/fsutil"
	"github.com/antonmedv/medb/internal/lock"
)

const (
	walName  = "wal.log"
	lockName = "lock"
)

var (
	ErrNotFound = errors.New("medb: document not found")
	ErrLocked   = lock.ErrLocked
	ErrClosed   = errors.New("medb: database is closed")
	ErrTooLarge = errors.New("medb: document exceeds size limit")
	ErrDirSync  = fsutil.ErrDirSync
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
	flock *os.File
	dir   string
	opts  options

	mu      sync.RWMutex
	colls   map[string]map[string]json.RawMessage
	dirty   map[string]bool
	dropped map[string]bool
	closed  bool

	logMu   sync.Mutex
	log     file
	size    atomic.Int64
	buf     []byte
	pending [][]byte
	spare   [][]byte
	commit  *commit

	notify chan struct{}
	stop   chan struct{}
	done   sync.WaitGroup
	failed error
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
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := fsutil.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	flock, err := lock.Acquire(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, walName)
	log, err := openLog(logPath)
	if err != nil {
		_ = flock.Close()
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = log.Close()
			_ = flock.Close()
		}
	}()
	db := &DB{
		flock:   flock,
		dir:     dir,
		opts:    o,
		colls:   map[string]map[string]json.RawMessage{},
		dirty:   map[string]bool{},
		dropped: map[string]bool{},
		log:     log,
		notify:  make(chan struct{}, 1),
		stop:    make(chan struct{}),
	}
	if err := db.load(); err != nil {
		return nil, err
	}
	if err := db.replayLog(logPath); err != nil {
		return nil, err
	}
	if err := db.writeSnapshot(nil); err != nil {
		return nil, err
	}
	db.done.Add(1)
	go db.run()
	ok = true
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

	close(db.stop)
	db.done.Wait()

	err := db.failed
	if e := db.log.Close(); err == nil {
		err = e
	}
	if e := db.flock.Close(); err == nil {
		err = e
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
	rec := record{Op: opDrop, Coll: name}
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.apply(rec)
	commit := db.enqueue(buf)
	db.mu.Unlock()
	return commit.wait()
}

func (db *DB) run() {
	defer db.done.Done()
	ticker := time.NewTicker(db.opts.flushInterval)
	defer ticker.Stop()
	var err error
	for {
		select {
		case <-db.notify:
			err = db.writeLog(err)
			if db.size.Load() >= db.opts.flushBytes {
				err = db.writeSnapshot(err)
			}
		case <-ticker.C:
			err = db.writeSnapshot(err)
		case <-db.stop:
			err = db.writeLog(err)
			db.failed = db.writeSnapshot(err)
			return
		}
	}
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

// maxNameLen bounds a collection name so that its snapshot path is always
// writable: the name plus the ".json.tmp" suffix of an atomic write has to fit
// in a single filesystem component (255 bytes on ext4, APFS and NTFS).
const maxNameLen = 240

func validName(name string) bool {
	if len(name) > maxNameLen {
		return false
	}
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
