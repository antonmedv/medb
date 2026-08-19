package wal_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/antonmedv/medb/internal/wal"
)

func open(tb testing.TB, path string) *wal.Log {
	tb.Helper()
	l, err := wal.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	return l
}

func appendRaw(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func records(t *testing.T, path string) [][]byte {
	t.Helper()
	recs, err := wal.Records(path)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func TestEnqueueAndRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	want := []string{"one", "two", "three"}
	for _, s := range want {
		if err := l.Enqueue([]byte(s)).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if l.Size() == 0 {
		t.Fatal("size not tracked")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	recs := records(t, path)
	if len(recs) != len(want) {
		t.Fatalf("got %d records, want %d", len(recs), len(want))
	}
	for i, rec := range recs {
		if string(rec) != want[i] {
			t.Fatalf("record %d is %q, want %q", i, rec, want[i])
		}
	}
}

func TestRecordInFileWhenWaitReturns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	defer l.Close()
	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "rec\n" {
		t.Fatalf("file holds %q after ack, want %q", data, "rec\n")
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	const writers, each = 16, 50
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				if err := l.Enqueue([]byte(fmt.Sprintf("w%d-r%d", w, i))).Wait(); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	recs := records(t, path)
	if len(recs) != writers*each {
		t.Fatalf("got %d records, want %d", len(recs), writers*each)
	}
	seen := make(map[string]bool, len(recs))
	for _, r := range recs {
		if seen[string(r)] {
			t.Fatalf("duplicate record %q", r)
		}
		seen[string(r)] = true
	}
	for w := range writers {
		for i := range each {
			if want := fmt.Sprintf("w%d-r%d", w, i); !seen[want] {
				t.Errorf("missing record %q", want)
			}
		}
	}

	last := make(map[int]int, writers)
	for _, r := range recs {
		var w, i int
		if _, err := fmt.Sscanf(string(r), "w%d-r%d", &w, &i); err != nil {
			t.Fatalf("unparseable record %q: %v", r, err)
		}
		if prev, ok := last[w]; ok && i != prev+1 {
			t.Fatalf("writer %d: record %d follows %d", w, i, prev)
		}
		last[w] = i
	}
}

func TestTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	for range 3 {
		if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, path, "torn garbage")
	if recs := records(t, path); len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
}

func TestTerminatedGarbageIsARecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, path, "{partial\n")
	recs := records(t, path)
	if len(recs) != 2 || string(recs[1]) != "{partial" {
		t.Fatalf("got %q, want the terminated line returned verbatim", recs)
	}
}

func TestTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	if err := l.Truncate(); err != nil {
		t.Fatal(err)
	}
	if l.Size() != 0 {
		t.Fatalf("size %d after truncate", l.Size())
	}
	if err := l.Enqueue([]byte("after")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "after\n" {
		t.Fatalf("log holds %q, want the post-truncate record at offset 0", data)
	}

	l = open(t, path)
	defer l.Close()
	if l.Size() != int64(len("after\n")) {
		t.Fatalf("size %d after reopen, want %d", l.Size(), len("after\n"))
	}
}

func TestDrainCommitsUnwaitedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	defer l.Close()
	l.Enqueue([]byte("a"))
	l.Enqueue([]byte("b"))
	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "a\nb\n" {
		t.Fatalf("file holds %q after Drain, want %q", data, "a\nb\n")
	}
	if l.Size() != int64(len("a\nb\n")) {
		t.Fatalf("size %d after Drain, want %d", l.Size(), len("a\nb\n"))
	}
}

func TestDrainAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Drain(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestSizeMatchesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	defer l.Close()
	for i := range 5 {
		if err := l.Enqueue([]byte(fmt.Sprintf("rec%d", i))).Wait(); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if l.Size() != info.Size() {
			t.Fatalf("after %d records Size()=%d, file is %d bytes", i+1, l.Size(), info.Size())
		}
	}
}

func TestReopenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	if err := l.Enqueue([]byte("first")).Wait(); err != nil {
		t.Fatal(err)
	}
	size := l.Size()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l = open(t, path)
	defer l.Close()
	if l.Size() != size {
		t.Fatalf("reopened Size()=%d, want %d", l.Size(), size)
	}
	if err := l.Enqueue([]byte("second")).Wait(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range records(t, path) {
		got = append(got, string(r))
	}
	if want := []string{"first", "second"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestEveryPrefixRecoversWholeRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	l := open(t, path)
	want := []string{"first", "second", "third", "fourth"}
	for _, s := range want {
		if err := l.Enqueue([]byte(s)).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	trunc := filepath.Join(dir, "truncated.log")
	for n := 0; n <= len(full); n++ {
		if err := os.WriteFile(trunc, full[:n], 0o600); err != nil {
			t.Fatal(err)
		}
		var expect []string
		consumed := 0
		for _, s := range want {
			if consumed+len(s)+1 > n {
				break
			}
			expect = append(expect, s)
			consumed += len(s) + 1
		}
		var got []string
		for _, r := range records(t, trunc) {
			got = append(got, string(r))
		}
		if !slices.Equal(got, expect) {
			t.Fatalf("truncated to %d bytes: got %q, want %q", n, got, expect)
		}
	}
}

func FuzzRecords(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("\n"))
	f.Add([]byte("\n\n"))
	f.Add([]byte("one\ntwo\n"))
	f.Add([]byte("one\npartial"))
	f.Add([]byte("{\"op\":\"set\",\"id\":\"a\"}\n\x00\x00\x00"))
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "wal.log")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		recs, err := wal.Records(path)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := len(recs), bytes.Count(data, []byte{'\n'}); got != want {
			t.Fatalf("got %d records, want %d (one per newline)", got, want)
		}
		var rebuilt []byte
		for _, r := range recs {
			if bytes.IndexByte(r, '\n') >= 0 {
				t.Fatalf("record contains a newline: %q", r)
			}
			rebuilt = append(rebuilt, r...)
			rebuilt = append(rebuilt, '\n')
		}
		if !bytes.HasPrefix(data, rebuilt) {
			t.Fatal("records do not reconstruct a prefix of the file")
		}
	})
}

func TestOpenFailure(t *testing.T) {
	_, err := wal.Open(filepath.Join(t.TempDir(), "missing", "wal.log"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want ErrNotExist", err)
	}
}

func TestRecordsMissingFile(t *testing.T) {
	_, err := wal.Records(filepath.Join(t.TempDir(), "wal.log"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want ErrNotExist", err)
	}
}
