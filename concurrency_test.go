package medb_test

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

// Every writer owns a disjoint id space and runs a deterministic sequence,
// so the final state is exactly predictable while readers hammer the DB.
func TestConcurrentMixedOps(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)

	const writers, docs = 8, 20
	users := medb.C[User](db, "users")

	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				users.Count()
				users.Has("w0-0")
				_, _ = users.Get("w1-1")
				for range users.All() {
				}
				db.Collections()
			}
		}()
	}

	var writersWG sync.WaitGroup
	for w := range writers {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := range docs {
				id := fmt.Sprintf("w%d-%d", w, i)
				if err := users.Set(id, User{Age: i}); err != nil {
					t.Errorf("Set(%s): %v", id, err)
				}
				if err := users.Update(id, func(u User) (User, error) {
					u.Age += 100
					return u, nil
				}); err != nil {
					t.Errorf("Update(%s): %v", id, err)
				}
				if i%3 == 0 {
					if err := users.Delete(id); err != nil {
						t.Errorf("Delete(%s): %v", id, err)
					}
				}
			}
		}()
	}
	writersWG.Wait()
	close(done)
	readers.Wait()

	verify := func(t *testing.T, users *medb.Collection[User]) {
		t.Helper()
		for w := range writers {
			for i := range docs {
				id := fmt.Sprintf("w%d-%d", w, i)
				if i%3 == 0 {
					if users.Has(id) {
						t.Fatalf("%s should be deleted", id)
					}
					continue
				}
				if got := get(t, users, id); got.Age != i+100 {
					t.Fatalf("%s Age = %d, want %d", id, got.Age, i+100)
				}
			}
		}
	}
	verify(t, users)
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)
	verify(t, medb.C[User](db, "users"))
}

// Update must be an atomic read-modify-write: concurrent increments on the
// same document may not lose updates.
func TestConcurrentUpdateCounter(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)

	counters := medb.C[int](db, "counters")
	set(t, counters, "hits", 0)

	const workers, increments = 8, 50
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range increments {
				if err := counters.Update("hits", func(n int) (int, error) {
					return n + 1, nil
				}); err != nil {
					t.Errorf("Update: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	want := workers * increments
	if got := get(t, counters, "hits"); got != want {
		t.Fatalf("counter = %d, want %d (lost updates)", got, want)
	}
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)
	if got := get(t, medb.C[int](db, "counters"), "hits"); got != want {
		t.Fatalf("counter = %d after reopen, want %d", got, want)
	}
}

// All must yield a consistent point-in-time view: sorted ids, and every
// yielded document fully matches what was written for that id (documents
// are write-once here, so any torn read would surface as a wrong value).
func TestAllDuringWrites(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	docs := medb.C[int](db, "docs")
	const total = 300

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range total {
			if err := docs.Set(fmt.Sprintf("d%04d", i), i); err != nil {
				t.Errorf("Set: %v", err)
				return
			}
		}
	}()

	for {
		var ids []string
		for id, v := range docs.All() {
			ids = append(ids, id)
			var want int
			if _, err := fmt.Sscanf(id, "d%d", &want); err != nil || v != want {
				t.Fatalf("torn read: %s = %d", id, v)
			}
		}
		if !slices.IsSorted(ids) {
			t.Fatalf("All out of order: %v", ids)
		}
		select {
		case <-done:
			if len(ids) == total {
				return
			}
		default:
		}
	}
}

// Any Set that returns nil — even one racing Close — must be durable.
func TestCloseDuringWritesDurability(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	docs := medb.C[int](db, "docs")

	const writers = 6
	acked := make([][]string, writers)
	var ackedTotal atomic.Int64
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				err := docs.Set(id, i)
				if err != nil {
					if !errors.Is(err, medb.ErrClosed) {
						t.Errorf("Set(%s): %v", id, err)
					}
					return
				}
				acked[w] = append(acked[w], id)
				ackedTotal.Add(1)
			}
		}()
	}
	// Let some commits land, then pull the rug.
	waitFor(t, 5*time.Second, func() bool { return ackedTotal.Load() > 20 })
	closeDB(t, db)
	wg.Wait()

	db = openDB(t, dir)
	defer closeDB(t, db)
	docs = medb.C[int](db, "docs")
	for w := range writers {
		for i, id := range acked[w] {
			if v, err := docs.Get(id); err != nil || v != i {
				t.Fatalf("acknowledged %s lost across Close (%d, %v)", id, v, err)
			}
		}
	}
}

// Set and Drop racing on the same collection: no hang, no race, and the DB
// is left in a consistent, reopenable state.
func TestConcurrentSetAndDrop(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	users := medb.C[User](db, "users")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 100 {
			if err := users.Set("x", User{Age: i}); err != nil {
				t.Errorf("Set: %v", err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 50 {
			if err := db.Drop("users"); err != nil {
				t.Errorf("Drop: %v", err)
			}
		}
	}()
	wg.Wait()

	// Whatever the interleaving, counts and iteration must agree.
	n := users.Count()
	seen := 0
	for range users.All() {
		seen++
	}
	if n != seen {
		t.Fatalf("Count = %d but All yielded %d", n, seen)
	}
	closeDB(t, db)

	db = openDB(t, dir)
	closeDB(t, db)
}
