package wal

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
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
	group   *group
	failed  error

	notify chan struct{}
	exec   chan execReq
	stop   chan struct{}
	done   sync.WaitGroup
}

// Every record in one commit shares a group: closing done broadcasts the
// single write-and-sync result to all of its waiters.
type group struct {
	done chan struct{}
	err  error
}

func settled(err error) *group {
	g := &group{done: make(chan struct{}), err: err}
	close(g.done)
	return g
}

type execReq struct {
	fn  func() error
	err chan error
}

type Ticket struct {
	group *group
}

func (t Ticket) Wait() error {
	<-t.group.done
	return t.group.err
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
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
		exec:   make(chan execReq),
		stop:   make(chan struct{}),
	}
	l.size.Store(size)
	l.done.Add(1)
	go l.run()
	return l
}

func (l *Log) Enqueue(payload []byte) Ticket {
	l.mu.Lock()
	if l.failed != nil {
		err := l.failed
		l.mu.Unlock()
		return Ticket{settled(err)}
	}
	if l.group == nil {
		l.group = &group{done: make(chan struct{})}
	}
	t := Ticket{l.group}
	l.pending = append(l.pending, payload)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return t
}

// Exec runs fn in the commit goroutine after draining pending records,
// so nothing is appended to the log while fn runs.
func (l *Log) Exec(fn func() error) error {
	req := execReq{fn, make(chan error, 1)}
	l.exec <- req
	return <-req.err
}

func (l *Log) Size() int64 {
	return l.size.Load()
}

func (l *Log) Truncate() error {
	if err := l.f.Truncate(0); err != nil {
		return err
	}
	l.size.Store(0)
	return nil
}

func (l *Log) Close() error {
	close(l.stop)
	l.done.Wait()
	return l.f.Close()
}

func (l *Log) run() {
	defer l.done.Done()
	for {
		select {
		case <-l.notify:
			l.commit()
		case req := <-l.exec:
			l.commit()
			// commit records a failure synchronously, so checking here keeps a
			// poisoned log from being snapshotted and truncated.
			l.mu.Lock()
			err := l.failed
			l.mu.Unlock()
			if err != nil {
				req.err <- err
				continue
			}
			req.err <- req.fn()
		case <-l.stop:
			l.commit()
			return
		}
	}
}

func (l *Log) commit() {
	l.mu.Lock()
	batch := l.pending
	g := l.group
	l.pending, l.spare = l.spare[:0], l.pending
	l.group = nil
	err := l.failed
	l.mu.Unlock()
	if g == nil {
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
			l.mu.Lock()
			if l.failed == nil {
				l.failed = err
			}
			l.mu.Unlock()
		}
	}
	g.err = err
	close(g.done)
	clear(batch)
}

// Records returns the complete records in the log. A record interrupted by a
// crash lacks its newline and is discarded, along with anything after it.
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
