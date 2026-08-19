package wal

import (
	"errors"
	"sync"
	"testing"
)

type fakeFile struct {
	mu        sync.Mutex
	written   []byte
	syncs     int
	truncs    int
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
	f.truncs++
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

func (f *fakeFile) truncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.truncs
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

func TestTruncateFailure(t *testing.T) {
	want := errors.New("truncate failed")
	f := &fakeFile{truncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Truncate(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() == 0 {
		t.Fatal("Size() reset despite a failed truncate")
	}
	if err := l.Enqueue([]byte("more")).Wait(); !errors.Is(err, want) {
		t.Fatalf("Enqueue after a failed truncate: got %v, want %v", err, want)
	}
}

func TestTruncateSyncs(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	before := f.syncCount()
	if err := l.Truncate(); err != nil {
		t.Fatal(err)
	}
	if f.syncCount() == before {
		t.Fatal("Truncate did not sync the new length")
	}
}

func TestTruncateSyncFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	f.fail(nil, want)
	if err := l.Truncate(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() == 0 {
		t.Fatal("Size() reset despite a failed sync")
	}
	if err := l.Enqueue([]byte("more")).Wait(); !errors.Is(err, want) {
		t.Fatalf("Enqueue after a failed truncate sync: got %v, want %v", err, want)
	}
}

func TestTruncateRefusedOnPoisonedLog(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if err := l.Truncate(); !errors.Is(err, want) {
		t.Fatalf("Truncate on a poisoned log: got %v, want %v", err, want)
	}
	if n := f.truncCount(); n != 0 {
		t.Fatalf("ftruncate reached the file %d times on a poisoned log", n)
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
