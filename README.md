# MeDB

[![GoDoc](https://godoc.org/antonmedv/medb?status.svg)](https://godoc.org/github.com/antonmedv/medb)

## Features

- In-memory database with on-disk persistence.
- Simple file layout, such as `prod/users.json`.
- Write-ahead log (WAL) with durable writes.
- Optional [MeDB server](cmd/medb/README.md):
    - Simple HTTP API.
    - Token-based authentication and roles.

## Rationale

I was building a simple Go server and kept users in a map. I wanted to save them to disk, but a full database felt like
too much, so I just wrote the map to a JSON file. It worked, but I wanted the same simplicity with durability. A map in
memory, stored as JSON on disk, without losing records if the process crashes.

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

The package-level `medb.C` function remains available on every supported Go version.

## Benchmarks

Rewriting the entire JSON file on every update gets more expensive as the collection grows. MeDB writes a small log
entry instead.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="bench/bench-dark.png">
  <img alt="MeDB compared with rewriting a JSON file on every update" src="bench/bench.png">
</picture>

- **Single writer:** MeDB stays near 245 writes/s with a 4 ms p50, from 10 to 100k documents. Full-file rewriting drops
  from 30k writes/s to 43 as each update grows from 411 B to 4.5 MB.
- **Durability has a cost:** each committed write requires an `fsync`—about 4 ms on this machine. That is why the
  rewrite is faster below roughly 20k documents when it does not use `fsync`. Add `fsync`, and it reaches the same limit
  as MeDB.
- **Concurrent writes are batched:** with 64 writers, MeDB combines multiple writes into one `fsync` and sustains about
  7.8k writes/s at every collection size. It overtakes the rewrite at roughly 440 documents and is 187× faster at 100k.
- **Reads are the trade-off:** MeDB handles 2.6–4.5M reads/s, compared with 31–100M for an in-memory map, because `Get`
  must unmarshal the stored JSON.

Measured on an Apple M4 Pro with APFS, where `fsync` uses `F_FULLFSYNC`. See the [bench](bench/).

## License

[MIT](LICENSE)
