package medb_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/antonmedv/medb"
)

func benchDB(b *testing.B) *medb.DB {
	b.Helper()
	db, err := medb.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkSet(b *testing.B) {
	users := medb.C[User](benchDB(b), "users")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := users.Set(fmt.Sprintf("u%d", i), User{Name: "Ada", Age: i}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetParallel(b *testing.B) {
	users := medb.C[User](benchDB(b), "users")
	var n atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := fmt.Sprintf("u%d", n.Add(1))
			if err := users.Set(id, User{Name: "Ada"}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGet(b *testing.B) {
	users := medb.C[User](benchDB(b), "users")
	const n = 1000
	for i := range n {
		if err := users.Set(fmt.Sprintf("u%d", i), User{Name: "Ada", Age: i}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := users.Get(fmt.Sprintf("u%d", i%n)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	users := medb.C[User](benchDB(b), "users")
	if err := users.Set("u", User{}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		err := users.Update("u", func(u User) (User, error) {
			u.Age++
			return u, nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAll(b *testing.B) {
	users := medb.C[User](benchDB(b), "users")
	for i := range 1000 {
		if err := users.Set(fmt.Sprintf("u%d", i), User{Age: i}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		for range users.All() {
		}
	}
}
