package medb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

type user struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func open(t *testing.T, dir string, opts ...medb.Option) *medb.DB {
	t.Helper()
	db, err := medb.Open(dir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// crash copies the database directory as it currently sits on disk, so the
// copy can be opened as if the process holding the original had died.
func crash(t *testing.T, dir string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "crashed")
	if err := os.CopyFS(dst, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	return dst
}

func appendToWAL(t *testing.T, dir, s string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0)
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

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func seed(t *testing.T, users *medb.Collection[user], n int) {
	t.Helper()
	for i := range n {
		if err := users.Set(fmt.Sprintf("u%d", i), user{Name: fmt.Sprintf("user-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSetGet(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	users := medb.C[user](db, "users")

	if err := users.Set("ada", user{"Ada", 36}); err != nil {
		t.Fatal(err)
	}
	got, err := users.Get("ada")
	if err != nil {
		t.Fatal(err)
	}
	if got != (user{"Ada", 36}) {
		t.Fatalf("got %+v", got)
	}
	if _, err := users.Get("nope"); !errors.Is(err, medb.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if err := users.Set("", user{}); err == nil {
		t.Fatal("empty id accepted")
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	if err := medb.C[user](db, "users").Set("ada", user{"Ada", 36}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "users.json")); err != nil {
		t.Fatal(err)
	}
	if n := walSize(t, dir); n != 0 {
		t.Fatalf("wal not truncated after Close: %d bytes", n)
	}

	db = open(t, dir)
	defer db.Close()
	got, err := medb.C[user](db, "users").Get("ada")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Ada" {
		t.Fatalf("got %+v", got)
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, medb.WithFlushInterval(time.Hour))
	defer db.Close()
	seed(t, medb.C[user](db, "users"), 3)

	if _, err := os.Stat(filepath.Join(dir, "users.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("snapshot exists before flush; recovery would not exercise the WAL")
	}
	crashed := crash(t, dir)

	db2 := open(t, crashed)
	defer db2.Close()
	if n := medb.C[user](db2, "users").Count(); n != 3 {
		t.Fatalf("recovered %d docs, want 3", n)
	}
	if n := walSize(t, crashed); n != 0 {
		t.Fatalf("wal not truncated after recovery: %d bytes", n)
	}
}

func TestTornTail(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, medb.WithFlushInterval(time.Hour))
	defer db.Close()
	seed(t, medb.C[user](db, "users"), 3)

	crashed := crash(t, dir)
	appendToWAL(t, crashed, "torn garbage tail")

	db2 := open(t, crashed)
	defer db2.Close()
	if n := medb.C[user](db2, "users").Count(); n != 3 {
		t.Fatalf("recovered %d docs, want 3", n)
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, medb.WithFlushInterval(time.Hour))
	defer db.Close()
	seed(t, medb.C[user](db, "users"), 3)

	crashed := crash(t, dir)
	appendToWAL(t, crashed, `{"op":"set","coll":"users","id":"u9","doc":{"name":`+"\n")

	db2 := open(t, crashed)
	defer db2.Close()
	users := medb.C[user](db2, "users")
	if users.Count() != 3 {
		t.Fatalf("recovered %d docs, want 3", users.Count())
	}
	if users.Has("u9") {
		t.Fatal("unparseable record was applied")
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	users := medb.C[user](db, "users")
	users.Set("a", user{Name: "A"})
	users.Set("b", user{Name: "B"})
	if err := users.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := users.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Get("a"); !errors.Is(err, medb.ErrNotFound) {
		t.Fatal("still present")
	}
	db.Close()

	db = open(t, dir)
	defer db.Close()
	users = medb.C[user](db, "users")
	if _, err := users.Get("a"); !errors.Is(err, medb.ErrNotFound) {
		t.Fatal("resurrected after reopen")
	}
	if !users.Has("b") {
		t.Fatal("b lost")
	}
}

func TestDrop(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	medb.C[user](db, "users").Set("a", user{Name: "A"})
	medb.C[user](db, "orders").Set("o", user{Name: "O"})
	if err := db.Drop("users"); err != nil {
		t.Fatal(err)
	}
	if err := db.Drop("missing"); err != nil {
		t.Fatal(err)
	}
	if got := db.Collections(); !slices.Equal(got, []string{"orders"}) {
		t.Fatalf("collections %v", got)
	}
	db.Close()

	if _, err := os.Stat(filepath.Join(dir, "users.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("users.json survived drop")
	}
	db = open(t, dir)
	defer db.Close()
	if got := db.Collections(); !slices.Equal(got, []string{"orders"}) {
		t.Fatalf("collections after reopen %v", got)
	}
}

func TestNamespaces(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	if err := medb.C[user](db, "prod/users").Set("a", user{Name: "A"}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := os.Stat(filepath.Join(dir, "prod", "users.json")); err != nil {
		t.Fatal(err)
	}
	db = open(t, dir)
	defer db.Close()
	if got := db.Collections(); !slices.Equal(got, []string{"prod/users"}) {
		t.Fatalf("collections %v", got)
	}
	if _, err := medb.C[user](db, "prod/users").Get("a"); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidNames(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	for _, name := range []string{"", "Users", "a b", "a//b", "/a", "a/", "a.b", "../etc", "wal.log"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("no panic for %q", name)
				}
			}()
			medb.C[user](db, name)
		}()
	}
}

func TestPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	db := open(t, dir)
	if err := medb.C[user](db, "users").Set("ada", user{"Ada", 36}); err != nil {
		t.Fatal(err)
	}
	if err := medb.C[user](db, "prod/orders").Set("o1", user{"O", 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".", "wal.log", "LOCK", "users.json", "prod", "prod/orders.json"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("%s has mode %v, want no group or world access", name, perm)
		}
	}
}

func TestLocked(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	defer db.Close()
	if _, err := medb.Open(dir); !errors.Is(err, medb.ErrLocked) {
		t.Fatalf("got %v, want ErrLocked", err)
	}
}

func TestConcurrent(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	users := medb.C[user](db, "users")
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 25 {
				id := fmt.Sprintf("u%d-%d", w, i)
				if err := users.Set(id, user{Name: id}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if users.Count() != 200 {
		t.Fatalf("count %d, want 200", users.Count())
	}
	db.Close()

	db = open(t, dir)
	defer db.Close()
	if n := medb.C[user](db, "users").Count(); n != 200 {
		t.Fatalf("count after reopen %d, want 200", n)
	}
}

func TestUpdate(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	users := medb.C[user](db, "users")
	users.Set("a", user{Name: "A", Age: 1})

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := users.Update("a", func(u user) (user, error) {
				u.Age++
				return u, nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got, err := users.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Age != 11 {
		t.Fatalf("age %d, want 11", got.Age)
	}
	if err := users.Update("missing", func(u user) (user, error) { return u, nil }); !errors.Is(err, medb.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestAll(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	users := medb.C[user](db, "users")
	users.Set("b", user{Name: "B"})
	users.Set("a", user{Name: "A"})
	users.Set("c", user{Name: "C"})

	var ids []string
	for id, u := range users.All() {
		ids = append(ids, id)
		if u.Name != strings.ToUpper(id) {
			t.Fatalf("id %s has %+v", id, u)
		}
	}
	if !slices.Equal(ids, []string{"a", "b", "c"}) {
		t.Fatalf("order %v", ids)
	}

	for range users.All() {
		break
	}
}

func TestFlushBytes(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, medb.WithFlushBytes(1))
	defer db.Close()
	if err := medb.C[user](db, "users").Set("a", user{Name: "A"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "snapshot flush", func() bool {
		info, err := os.Stat(filepath.Join(dir, "wal.log"))
		if err != nil {
			return false
		}
		_, serr := os.Stat(filepath.Join(dir, "users.json"))
		return info.Size() == 0 && serr == nil
	})
	crashed := crash(t, dir)

	db2 := open(t, crashed)
	defer db2.Close()
	if _, err := medb.C[user](db2, "users").Get("a"); err != nil {
		t.Fatal(err)
	}
}

func TestFlushInterval(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, medb.WithFlushInterval(50*time.Millisecond))
	defer db.Close()
	if err := medb.C[user](db, "users").Set("a", user{Name: "A"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "interval flush", func() bool {
		info, err := os.Stat(filepath.Join(dir, "wal.log"))
		return err == nil && info.Size() == 0
	})
}

func TestTooLarge(t *testing.T) {
	db := open(t, t.TempDir(), medb.WithMaxDocSize(16))
	defer db.Close()
	err := medb.C[user](db, "users").Set("a", user{Name: strings.Repeat("x", 100)})
	if !errors.Is(err, medb.ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

func TestClosed(t *testing.T) {
	db := open(t, t.TempDir())
	users := medb.C[user](db, "users")
	users.Set("a", user{Name: "A"})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := users.Set("b", user{}); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Set: %v", err)
	}
	if _, err := users.Get("a"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Get: %v", err)
	}
	if err := db.Drop("users"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Drop: %v", err)
	}
	if err := db.Close(); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewID(t *testing.T) {
	a, b := medb.NewID(), medb.NewID()
	if len(a) != 32 || a == b {
		t.Fatalf("ids %q %q", a, b)
	}
}
