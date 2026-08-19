// Package wal is an append-only log of newline-delimited records, fsynced
// before a write is acknowledged. A payload must not contain '\n' and must
// not be modified until its Commit settles. The first failed write, sync, or
// truncate poisons the log and every later call reports it: a failed fsync
// cannot be retried, because the kernel may have dropped the dirty pages
// while marking them clean.
package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/antonmedv/medb/internal/fsutil"
)

type file interface {
	Write(p []byte) (int, error)
	Sync() error
	Truncate(size int64) error
	Close() error
}

type Log struct {
	f    file
	size atomic.Int64
	buf  []byte

	mu      sync.Mutex
	pending [][]byte
	spare   [][]byte
	commit  *Commit
	failed  error

	notify chan struct{}
	drain  chan chan error
	stop   chan struct{}
	done   sync.WaitGroup
}

type Commit struct {
	done chan struct{}
	err  error
}

func (c *Commit) Wait() error {
	<-c.done
	return c.err
}

func settled(err error) *Commit {
	c := &Commit{done: make(chan struct{}), err: err}
	close(c.done)
	return c
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := fsutil.SyncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return newLog(f, info.Size()), nil
}

func newLog(f file, size int64) *Log {
	l := &Log{
		f:      f,
		notify: make(chan struct{}, 1),
		drain:  make(chan chan error),
		stop:   make(chan struct{}),
	}
	l.size.Store(size)
	l.done.Add(1)
	go l.run()
	return l
}

func (l *Log) Enqueue(payload []byte) *Commit {
	l.mu.Lock()
	if l.failed != nil {
		c := settled(l.failed)
		l.mu.Unlock()
		return c
	}
	if l.commit == nil {
		l.commit = &Commit{done: make(chan struct{})}
	}
	c := l.commit
	l.pending = append(l.pending, payload)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return c
}

func (l *Log) Drain() error {
	errc := make(chan error, 1)
	select {
	case l.drain <- errc:
		return <-errc
	case <-l.stop:
		return os.ErrClosed
	}
}

func (l *Log) Size() int64 {
	return l.size.Load()
}

func (l *Log) Truncate() error {
	l.mu.Lock()
	err := l.failed
	l.mu.Unlock()
	if err != nil {
		return err
	}
	if err := l.f.Truncate(0); err != nil {
		l.fail(err)
		return err
	}
	if err := l.f.Sync(); err != nil {
		l.fail(err)
		return err
	}
	l.size.Store(0)
	return nil
}

func (l *Log) Close() error {
	close(l.stop)
	l.done.Wait()
	err := l.f.Close()
	l.mu.Lock()
	if l.failed != nil {
		err = l.failed
	}
	l.mu.Unlock()
	return err
}

func (l *Log) fail(err error) {
	l.mu.Lock()
	if l.failed == nil {
		l.failed = err
	}
	l.mu.Unlock()
}

func (l *Log) run() {
	defer l.done.Done()
	for {
		select {
		case <-l.notify:
			l.write()
		case errc := <-l.drain:
			l.write()
			l.mu.Lock()
			err := l.failed
			l.mu.Unlock()
			errc <- err
		case <-l.stop:
			l.write()
			return
		}
	}
}

func (l *Log) write() {
	l.mu.Lock()
	batch, commit := l.pending, l.commit
	l.pending, l.spare = l.spare[:0], l.pending
	l.commit = nil
	err := l.failed
	l.mu.Unlock()
	if commit == nil {
		return
	}
	if err == nil {
		l.buf = l.buf[:0]
		for _, payload := range batch {
			l.buf = append(l.buf, payload...)
			l.buf = append(l.buf, '\n')
		}
		if _, err = l.f.Write(l.buf); err == nil {
			err = l.f.Sync()
		}
		if err == nil {
			l.size.Add(int64(len(l.buf)))
		} else {
			l.fail(err)
		}
	}
	commit.err = err
	close(commit.done)
	clear(batch)
}

func Records(path string) ([][]byte, error) {
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
