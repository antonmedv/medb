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
	commit  *Commit

	notify chan struct{}
	stop   chan struct{}
	done   sync.WaitGroup
}

type Commit struct {
	done chan struct{}
	err  error
}

func (t *Commit) Wait() error {
	<-t.done
	return t.err
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
		stop:   make(chan struct{}),
	}
	l.size.Store(size)
	l.done.Add(1)
	go l.run()
	return l
}

func (l *Log) Enqueue(payload []byte) *Commit {
	l.mu.Lock()
	if l.commit == nil {
		l.commit = &Commit{done: make(chan struct{})}
	}
	l.pending = append(l.pending, payload)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return l.commit
}

func (l *Log) Size() int64 {
	return l.size.Load()
}

func (l *Log) Truncate() error {
	if err := l.f.Truncate(0); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
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
			l.doCommit()
		case <-l.stop:
			l.doCommit()
			return
		}
	}
}

func (l *Log) doCommit() {
	l.mu.Lock()
	batch, commit := l.pending, l.commit
	l.pending, l.spare = l.spare[:0], l.pending
	l.commit = nil
	l.mu.Unlock()
	if commit == nil {
		return
	}
	var err error
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
