// Phase 1 of TESTING.md: happy-path tests for the whole public API.
package medb_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

type User struct {
	Name string
	Age  int
	Tags []string
}

func openDB(t *testing.T, dir string, opts ...medb.Option) *medb.DB {
	t.Helper()
	db, err := medb.Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open(%q): %v", dir, err)
	}
	return db
}

func closeDB(t *testing.T, db *medb.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func set[T any](t *testing.T, c *medb.Collection[T], id string, doc T) {
	t.Helper()
	if err := c.Set(id, doc); err != nil {
		t.Fatalf("Set(%q): %v", id, err)
	}
}

func get[T any](t *testing.T, c *medb.Collection[T], id string) T {
	t.Helper()
	v, err := c.Get(id)
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	return v
}

// waitFor polls cond until it returns true or the deadline expires.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestOpenAndClose(t *testing.T) {
	db := openDB(t, t.TempDir())
	closeDB(t, db)
}

func TestOpenWithOptions(t *testing.T) {
	db := openDB(t, t.TempDir(),
		medb.WithMaxDocSize(1<<20),
		medb.WithFlushBytes(1<<20),
		medb.WithFlushInterval(time.Second),
	)
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	want := User{Name: "Ada", Age: 36}
	set(t, users, "u1", want)
	if got := get(t, users, "u1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSetAndGet(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	want := User{Name: "Ada", Age: 36, Tags: []string{"math", "computing"}}
	set(t, users, "u1", want)
	if got := get(t, users, "u1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSetOverwrite(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada", Age: 36})
	want := User{Name: "Ada Lovelace", Age: 37}
	set(t, users, "u1", want)

	if got := get(t, users, "u1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if n := users.Count(); n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}
}

func TestHas(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	if users.Has("u1") {
		t.Fatal("Has(u1) = true before Set")
	}
	set(t, users, "u1", User{Name: "Ada"})
	if !users.Has("u1") {
		t.Fatal("Has(u1) = false after Set")
	}
}

func TestDelete(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})
	set(t, users, "u2", User{Name: "Grace"})

	if err := users.Delete("u1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if users.Has("u1") {
		t.Fatal("Has(u1) = true after Delete")
	}
	if !users.Has("u2") {
		t.Fatal("Delete(u1) removed u2")
	}
	if n := users.Count(); n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}
}

func TestUpdate(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada", Age: 36})

	err := users.Update("u1", func(u User) (User, error) {
		u.Age++
		return u, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	want := User{Name: "Ada", Age: 37}
	if got := get(t, users, "u1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestCount(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	if n := users.Count(); n != 0 {
		t.Fatalf("Count = %d on empty collection, want 0", n)
	}
	for i := range 3 {
		set(t, users, fmt.Sprintf("u%d", i), User{Age: i})
	}
	if n := users.Count(); n != 3 {
		t.Fatalf("Count = %d, want 3", n)
	}
}

func TestAll(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	docs := map[string]User{
		"b": {Name: "Barbara"},
		"a": {Name: "Ada"},
		"c": {Name: "Christine"},
	}
	for id, u := range docs {
		set(t, users, id, u)
	}

	var ids []string
	for id, u := range users.All() {
		ids = append(ids, id)
		if !reflect.DeepEqual(u, docs[id]) {
			t.Fatalf("All: %q = %+v, want %+v", id, u, docs[id])
		}
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(ids, want) {
		t.Fatalf("All order = %v, want %v", ids, want)
	}
}

func TestAllEarlyBreak(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	for i := range 5 {
		set(t, users, fmt.Sprintf("u%d", i), User{Age: i})
	}
	seen := 0
	for range users.All() {
		seen++
		if seen == 2 {
			break
		}
	}
	if seen != 2 {
		t.Fatalf("iterated %d times, want 2", seen)
	}
}

func TestCollections(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	if got := db.Collections(); len(got) != 0 {
		t.Fatalf("Collections = %v on fresh db, want none", got)
	}

	// C alone does not create a collection; the first write does.
	_ = medb.C[User](db, "phantom")
	set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})
	set(t, medb.C[User](db, "prod/orders"), "o1", User{})
	set(t, medb.C[User](db, "logs"), "l1", User{})

	want := []string{"logs", "prod/orders", "users"}
	if got := db.Collections(); !slices.Equal(got, want) {
		t.Fatalf("Collections = %v, want %v", got, want)
	}
}

func TestNestedCollectionName(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "prod/eu/users")
	want := User{Name: "Ada"}
	set(t, users, "u1", want)
	if got := get(t, users, "u1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDrop(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	orders := medb.C[User](db, "orders")
	set(t, users, "u1", User{Name: "Ada"})
	set(t, orders, "o1", User{})

	if err := db.Drop("users"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if users.Has("u1") {
		t.Fatal("Has(u1) = true after Drop")
	}
	if n := users.Count(); n != 0 {
		t.Fatalf("Count = %d after Drop, want 0", n)
	}
	if got, want := db.Collections(), []string{"orders"}; !slices.Equal(got, want) {
		t.Fatalf("Collections = %v, want %v", got, want)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)

	users := medb.C[User](db, "users")
	orders := medb.C[User](db, "prod/orders")
	ada := User{Name: "Ada", Age: 36, Tags: []string{"math"}}
	grace := User{Name: "Grace", Age: 45}
	set(t, users, "u1", ada)
	set(t, users, "u2", grace)
	set(t, orders, "o1", User{Name: "order"})
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)

	users = medb.C[User](db, "users")
	orders = medb.C[User](db, "prod/orders")
	if got, want := db.Collections(), []string{"prod/orders", "users"}; !slices.Equal(got, want) {
		t.Fatalf("Collections = %v, want %v", got, want)
	}
	if n := users.Count(); n != 2 {
		t.Fatalf("users.Count = %d, want 2", n)
	}
	if got := get(t, users, "u1"); !reflect.DeepEqual(got, ada) {
		t.Fatalf("u1 = %+v, want %+v", got, ada)
	}
	if got := get(t, users, "u2"); !reflect.DeepEqual(got, grace) {
		t.Fatalf("u2 = %+v, want %+v", got, grace)
	}
	if n := orders.Count(); n != 1 {
		t.Fatalf("orders.Count = %d, want 1", n)
	}
}

func TestPersistenceAfterDelete(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)

	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})
	set(t, users, "u2", User{Name: "Grace"})
	if err := users.Delete("u2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)

	users = medb.C[User](db, "users")
	if users.Has("u2") {
		t.Fatal("Has(u2) = true after Delete and reopen")
	}
	if n := users.Count(); n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}
}

func TestPersistenceAfterDrop(t *testing.T) {
	t.Run("snapshotted collection", func(t *testing.T) {
		dir := t.TempDir()
		db := openDB(t, dir)
		set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})
		closeDB(t, db) // snapshot puts users.json on disk

		db = openDB(t, dir)
		if err := db.Drop("users"); err != nil {
			t.Fatalf("Drop: %v", err)
		}
		closeDB(t, db)

		db = openDB(t, dir)
		defer closeDB(t, db)
		if got := db.Collections(); len(got) != 0 {
			t.Fatalf("Collections = %v after Drop and reopen, want none", got)
		}
	})

	t.Run("collection only in wal", func(t *testing.T) {
		dir := t.TempDir()
		db := openDB(t, dir)
		set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})
		if err := db.Drop("users"); err != nil { // dropped before any snapshot
			t.Fatalf("Drop: %v", err)
		}
		closeDB(t, db)

		db = openDB(t, dir)
		defer closeDB(t, db)
		if got := db.Collections(); len(got) != 0 {
			t.Fatalf("Collections = %v after Drop and reopen, want none", got)
		}
	})
}

func TestSnapshotFileFormat(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	want := User{Name: "Ada", Age: 36}
	set(t, medb.C[User](db, "prod/users"), "u1", want)
	closeDB(t, db)

	data, err := os.ReadFile(filepath.Join(dir, "prod", "users.json"))
	if err != nil {
		t.Fatalf("reading snapshot: %v", err)
	}
	var coll map[string]User
	if err := json.Unmarshal(data, &coll); err != nil {
		t.Fatalf("snapshot is not an id→doc JSON object: %v", err)
	}
	if got := coll["u1"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot u1 = %+v, want %+v", got, want)
	}
}

func TestFlushIntervalWritesSnapshot(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, medb.WithFlushInterval(20*time.Millisecond))
	defer closeDB(t, db)

	set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})

	path := filepath.Join(dir, "users.json")
	waitFor(t, 2*time.Second, func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		var coll map[string]User
		return json.Unmarshal(data, &coll) == nil && coll["u1"].Name == "Ada"
	})
}

func TestFlushBytesWritesSnapshot(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir,
		medb.WithFlushBytes(1),
		medb.WithFlushInterval(time.Hour), // only the size trigger can fire
	)
	defer closeDB(t, db)

	set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})

	path := filepath.Join(dir, "users.json")
	waitFor(t, 2*time.Second, func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		var coll map[string]User
		return json.Unmarshal(data, &coll) == nil && coll["u1"].Name == "Ada"
	})
}

func TestNewID(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		id := medb.NewID()
		if len(id) != 32 {
			t.Fatalf("NewID() = %q, want 32 hex chars", id)
		}
		if _, err := hex.DecodeString(id); err != nil {
			t.Fatalf("NewID() = %q is not hex: %v", id, err)
		}
		if seen[id] {
			t.Fatalf("NewID() returned duplicate %q", id)
		}
		seen[id] = true
	}
}

func TestConcurrentSets(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	const writers, docs = 8, 25

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range docs {
				id := fmt.Sprintf("w%d-%d", w, i)
				if err := users.Set(id, User{Name: id, Age: i}); err != nil {
					t.Errorf("Set(%q): %v", id, err)
				}
			}
		}()
	}
	wg.Wait()

	if n := users.Count(); n != writers*docs {
		t.Fatalf("Count = %d, want %d", n, writers*docs)
	}
	for w := range writers {
		id := fmt.Sprintf("w%d-%d", w, docs-1)
		if got := get(t, users, id); got.Name != id {
			t.Fatalf("Get(%q) = %+v", id, got)
		}
	}
}

func TestOpsAfterClose(t *testing.T) {
	db := openDB(t, t.TempDir())
	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})
	closeDB(t, db)

	if err := users.Set("u2", User{Name: "Bob"}); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Set after close: %v, want ErrClosed", err)
	}
	if err := users.Delete("u1"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Delete after close: %v, want ErrClosed", err)
	}
	if err := db.Drop("users"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Drop after close: %v, want ErrClosed", err)
	}
	if err := users.Update("u1", func(v User) (User, error) { return v, nil }); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Update after close: %v, want ErrClosed", err)
	}

	// None of the above may return while still holding the lock.
	done := make(chan struct{})
	go func() {
		users.Get("u1")
		users.Has("u1")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("db wedged: mu still held after ErrClosed was returned")
	}
}

// Writers racing Close must either get ErrClosed or a completed commit —
// never hang waiting for a commit the worker will no longer write.
func TestCloseRace(t *testing.T) {
	for range 20 {
		db := openDB(t, t.TempDir())
		users := medb.C[User](db, "users")

		var wg sync.WaitGroup
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 10 {
					if err := users.Set("u", User{Name: fmt.Sprintf("w%d", w), Age: i}); err != nil {
						if !errors.Is(err, medb.ErrClosed) {
							t.Errorf("Set: %v", err)
						}
						return
					}
				}
			}()
		}
		closeDB(t, db)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("writer hung in commit.wait() across Close")
		}
	}
}
