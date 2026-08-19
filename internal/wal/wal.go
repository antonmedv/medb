package wal

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
)

type Log struct {
	f    *os.File
	size atomic.Int64
	buf  []byte

	mu      sync.Mutex
	pending []request
	failed  error

	notify chan struct{}
	exec   chan execReq
	stop   chan struct{}
	done   sync.WaitGroup
}

type request struct {
	payload []byte
	done    chan error
}

type execReq struct {
	fn  func() error
	err chan error
}

type Ticket struct {
	done chan error
}

func (t Ticket) Wait() error {
	return <-t.done
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
	l := &Log{
		f:      f,
		notify: make(chan struct{}, 1),
		exec:   make(chan execReq),
		stop:   make(chan struct{}),
	}
	l.size.Store(info.Size())
	l.done.Add(1)
	go l.run()
	return l, nil
}

func (l *Log) Enqueue(payload []byte) Ticket {
	t := Ticket{done: make(chan error, 1)}
	l.mu.Lock()
	if l.failed != nil {
		err := l.failed
		l.mu.Unlock()
		t.done <- err
		return t
	}
	l.pending = append(l.pending, request{payload, t.done})
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
	l.pending = nil
	err := l.failed
	l.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if err == nil {
		l.buf = l.buf[:0]
		for _, r := range batch {
			l.buf = append(l.buf, r.payload...)
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
	for _, r := range batch {
		r.done <- err
	}
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
