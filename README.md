# MeDB
[![GoDoc](https://godoc.org/antonmedv/medb?status.svg)](https://godoc.org/github.com/antonmedv/medb)

I was building a simple Go server and kept users in a map. I wanted to save them to disk, but a full database felt like
too much, so I just wrote the map to a JSON file. It worked, but I wanted the same simplicity with durability. A map in memory, stored as JSON on disk, without losing
records if the process crashes.

That is how this database was born. MeDB is a small embedded in memory database that persists data to JSON files on disk.

1. Writes are durable: acknowledged writes are fsynced.
2. Data is stored as JSON files.
3. Only one process can open the database at a time.
4. Concurrent reads/writes are safe.

The optional [MeDB server](cmd/medb/README.md) exposes the database over HTTP with JSON, token authentication, and roles.

## Usage

```go
package main

import (
	"fmt"

	"github.com/antonmedv/medb"
)

type User struct {
	Name string
	Age  int
}

func main() {
	db, err := medb.Open("data")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()

	users := medb.C[User](db, "users")

	if err := users.Set("ada", User{Name: "Ada", Age: 36}); err != nil {
		panic(err)
	}

	user, err := users.Get("ada")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s is %d years old\n", user.Name, user.Age)
}
```

Go 1.27 and newer also support the equivalent method syntax:

```go
users := db.C[User]("users")
```

The package-level `medb.C` function remains available on every supported Go
version.

## Benchmarks

Rewriting a JSON file on every change, the way the story above starts, costs the whole collection per write.
MeDB appends one log record instead.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="bench/bench-dark.png">
  <img alt="MeDB against a map rewritten to a JSON file on every change" src="bench/bench.png">
</picture>

- MeDB holds ~245 writes/s and 4 ms p50 from 10 to 100k documents. The rewrite falls from 30k writes/s to 43,
  because one change grew from 411 B to 4.5 MB.
- A durable write costs one fsync (~4 ms here), so with a single writer the rewrite is ahead below ~20k
  documents. A map with fsync hits the same floor: that gap is durability, not MeDB.
- With 64 writers MeDB batches them onto one fsync: ~7.8k writes/s at every size, 187x the rewrite at 100k
  documents, ahead past ~440.
- Reads are the trade: 2.6-4.5M/s against 31-100M/s, because `Get` unmarshals the stored JSON.

Measured on an Apple M4 Pro with APFS, where fsync is `F_FULLFSYNC`. Method, full table and harness:
[bench](bench/).

## License

[MIT](LICENSE)
