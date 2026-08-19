package wal

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type fakeFile struct {
	mu        sync.Mutex
	written   []byte
	syncs     int
	writeErr  error
	syncErr   error
	truncErr  error
	closeErr  error
	closeDone bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	f.written = append(f.written, p...)
	return len(p), nil
}

func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return f.syncErr
}

func (f *fakeFile) Truncate(int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.truncErr != nil {
		return f.truncErr
	}
	f.written = nil
	return nil
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeDone = true
	return f.closeErr
}

func (f *fakeFile) fail(write, sync error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErr, f.syncErr = write, sync
}

func (f *fakeFile) contents() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.written)
}

func (f *fakeFile) syncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncs
}

type gatedFile struct {
	fakeFile
	entered chan struct{}
	release chan struct{}
}

func newGatedFile() *gatedFile {
	return &gatedFile{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *gatedFile) Write(p []byte) (int, error) {
	f.entered <- struct{}{}
	<-f.release
	return f.fakeFile.Write(p)
}

func TestSyncBeforeAck(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if f.syncCount() == 0 {
		t.Fatal("acknowledged before any fsync")
	}
	if got := f.contents(); got != "rec\n" {
		t.Fatalf("file holds %q at ack time, want %q", got, "rec\n")
	}
}

func TestGroupCommitBatches(t *testing.T) {
	f := newGatedFile()
	l := newLog(f, 0)
	defer l.Close()

	c1 := l.Enqueue([]byte("a"))
	<-f.entered // batch {a} is being written
	c2 := l.Enqueue([]byte("b"))
	c3 := l.Enqueue([]byte("c"))
	if c1 == c2 {
		t.Fatal("record enqueued mid-commit joined the batch already on disk")
	}
	if c2 != c3 {
		t.Fatal("records enqueued while a commit is in flight do not share a batch")
	}
	f.release <- struct{}{}
	<-f.entered // batch {b, c}
	f.release <- struct{}{}

	for i, c := range []*Commit{c1, c2, c3} {
		if err := c.Wait(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if got := f.contents(); got != "a\nb\nc\n" {
		t.Fatalf("file holds %q, want records in enqueue order", got)
	}
	if n := f.syncCount(); n != 2 {
		t.Fatalf("%d fsyncs for two batches, want 2", n)
	}
}

func TestSyncFailureIsFailStop(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("first Enqueue: got %v, want %v", err, want)
	}
	for i := range 3 {
		if err := l.Enqueue([]byte("b")).Wait(); !errors.Is(err, want) {
			t.Fatalf("Enqueue %d after failure: got %v, want %v", i, err, want)
		}
	}
	if n := f.syncCount(); n != 1 {
		t.Fatalf("%d syncs attempted, want 1: the log kept writing after a failure", n)
	}
	if got := f.contents(); got != "a\n" {
		t.Fatalf("file holds %q after the failure, want only the first batch", got)
	}
	if l.Size() != 0 {
		t.Fatalf("Size()=%d after a failed commit, want 0", l.Size())
	}
}

func TestWriteFailureIsFailStop(t *testing.T) {
	want := errors.New("write failed")
	f := &fakeFile{writeErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("first Enqueue: got %v, want %v", err, want)
	}
	if err := l.Enqueue([]byte("b")).Wait(); !errors.Is(err, want) {
		t.Fatalf("second Enqueue: got %v, want %v", err, want)
	}
	if got := f.contents(); got != "" {
		t.Fatalf("log holds %q after a failed write", got)
	}
	if n := f.syncCount(); n != 0 {
		t.Fatalf("%d syncs after a failed write, want 0", n)
	}
}

func TestFailureReachesEveryWaiter(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	commits := make([]*Commit, 8)
	for i := range commits {
		commits[i] = l.Enqueue([]byte("rec"))
	}
	for i, c := range commits {
		if err := c.Wait(); !errors.Is(err, want) {
			t.Fatalf("waiter %d: got %v, want %v", i, err, want)
		}
	}
}

func TestQueuedRecordsNeverWrittenAfterFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := newGatedFile()
	f.syncErr = want
	l := newLog(f, 0)
	defer l.Close()

	c1 := l.Enqueue([]byte("a"))
	<-f.entered // batch {a} is being written, its sync will fail
	c2 := l.Enqueue([]byte("b"))
	f.release <- struct{}{}

	if err := c1.Wait(); !errors.Is(err, want) {
		t.Fatalf("failing batch: got %v, want %v", err, want)
	}
	if err := c2.Wait(); !errors.Is(err, want) {
		t.Fatalf("queued batch: got %v, want %v", err, want)
	}
	if got := f.contents(); got != "a\n" {
		t.Fatalf("file holds %q; the queued record reached a failed fd", got)
	}
	if n := f.syncCount(); n != 1 {
		t.Fatalf("%d syncs, want 1: the queued batch was attempted", n)
	}
}

func TestFailureAfterSuccessfulCommits(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("kept")).Wait(); err != nil {
		t.Fatal(err)
	}
	size := l.Size()
	if size != int64(len("kept\n")) {
		t.Fatalf("Size()=%d, want %d", size, len("kept\n"))
	}

	want := errors.New("sync failed")
	f.fail(nil, want)
	if err := l.Enqueue([]byte("lost")).Wait(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() != size {
		t.Fatalf("Size()=%d after a failed commit, want %d unchanged", l.Size(), size)
	}
	if got := f.contents(); got != "kept\nlost\n" {
		t.Fatalf("log holds %q; the failed record reached the file but was not counted", got)
	}
}

func TestDrain(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Drain(); err != nil {
		t.Fatalf("draining an empty log: %v", err)
	}
	l.Enqueue([]byte("late"))
	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	if got := f.contents(); got != "late\n" {
		t.Fatalf("log holds %q, want the record Drain committed", got)
	}
	if l.Size() != int64(len("late\n")) {
		t.Fatalf("size %d, want %d", l.Size(), len("late\n"))
	}
}

func TestDrainReportsFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	l.Enqueue([]byte("doomed"))
	if err := l.Drain(); !errors.Is(err, want) {
		t.Fatalf("Drain returned %v, want %v", err, want)
	}
	if l.Size() != 0 {
		t.Fatalf("size %d after a failed sync, want 0", l.Size())
	}
}

func TestDrainReportsEarlierFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if err := l.Drain(); !errors.Is(err, want) {
		t.Fatalf("Drain returned %v, want %v", err, want)
	}
}

func TestCloseCommitsPending(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)

	c := l.Enqueue([]byte("rec"))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := f.contents(); got != "rec\n" {
		t.Fatalf("log holds %q after Close, want %q", got, "rec\n")
	}
	if !f.closeDone {
		t.Fatal("file not closed")
	}
}

func TestCloseReportsFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)

	l.Enqueue([]byte("rec"))
	if err := l.Close(); !errors.Is(err, want) {
		t.Fatalf("Close returned %v, want %v", err, want)
	}
}

func TestCloseReportsCloseError(t *testing.T) {
	want := errors.New("close failed")
	f := &fakeFile{closeErr: want}
	l := newLog(f, 0)

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); !errors.Is(err, want) {
		t.Fatalf("Close returned %v, want %v", err, want)
	}
}

var errInjected = errors.New("injected sync failure")

type failAfterFile struct {
	fakeFile
	after int
}

func (f *failAfterFile) Sync() error {
	f.fakeFile.Sync()
	if f.syncCount() > f.after {
		return errInjected
	}
	return nil
}

// Sweep the failure across every sync ordinal: whatever batch the failure
// lands on, an acknowledged record is in the log, records appear whole, at
// most once, in per-writer order, and each writer's surviving records are a
// prefix of what it enqueued.
func TestAckedRecordsSurviveInjectedFailure(t *testing.T) {
	const writers, each = 4, 8
	for limit := 1; limit <= 12; limit++ {
		f := &failAfterFile{after: limit}
		l := newLog(f, 0)

		var mu sync.Mutex
		acked := map[string]bool{}
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := range each {
					rec := fmt.Sprintf("w%d-%d", w, i)
					if l.Enqueue([]byte(rec)).Wait() == nil {
						mu.Lock()
						acked[rec] = true
						mu.Unlock()
					}
				}
			}(w)
		}
		wg.Wait()
		l.Close()

		content := f.contents()
		if content != "" && !strings.HasSuffix(content, "\n") {
			t.Fatalf("limit %d: log does not end at a record boundary: %q", limit, content)
		}
		pos := map[string]int{}
		for i, rec := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
			if rec == "" {
				continue
			}
			if _, dup := pos[rec]; dup {
				t.Fatalf("limit %d: record %q appears twice", limit, rec)
			}
			pos[rec] = i
		}
		for rec := range acked {
			if _, ok := pos[rec]; !ok {
				t.Fatalf("limit %d: acked record %q missing from the log", limit, rec)
			}
		}
		for w := range writers {
			prev, missing := -1, false
			for i := range each {
				p, ok := pos[fmt.Sprintf("w%d-%d", w, i)]
				if !ok {
					missing = true
					continue
				}
				if missing {
					t.Fatalf("limit %d: writer %d record %d present after a gap", limit, w, i)
				}
				if p <= prev {
					t.Fatalf("limit %d: writer %d record %d out of order", limit, w, i)
				}
				prev = p
			}
		}
	}
}

// After Drain returns and no writer is active, the goroutine never touches
// the file again: that idleness is what makes an external Reset safe while
// the log is open.
func TestIdleLogTouchesNothing(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	if n := f.syncCount(); n != 0 {
		t.Fatalf("%d syncs on an idle log, want 0", n)
	}
	if got := f.contents(); got != "" {
		t.Fatalf("idle log wrote %q", got)
	}
}

func TestReset(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("old")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Reset(); err != nil {
		t.Fatal(err)
	}
	if l.Size() != 0 {
		t.Fatalf("Size()=%d after Reset, want 0", l.Size())
	}
	if err := l.Enqueue([]byte("new")).Wait(); err != nil {
		t.Fatal(err)
	}
	if got := f.contents(); got != "new\n" {
		t.Fatalf("log holds %q, want only the post-Reset record", got)
	}
}

func TestResetTruncateFailure(t *testing.T) {
	want := errors.New("truncate failed")
	f := &fakeFile{truncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Reset(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() == 0 {
		t.Fatal("Size() reset despite a failed truncate")
	}
	if got := f.contents(); got != "rec\n" {
		t.Fatalf("log holds %q after a failed Reset", got)
	}
}

func TestResetSyncFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	f.fail(nil, want)
	if err := l.Reset(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() == 0 {
		t.Fatal("Size() reset despite a failed sync")
	}
}
