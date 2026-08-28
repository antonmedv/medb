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

## Usage

Install MeDB:

```sh
go get github.com/antonmedv/medb
```

### Quick start

```go
package main

import (
	"fmt"

	"github.com/antonmedv/medb"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
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

This creates a `users` collection backed by `data/users.json`. Collections are typed views over JSON data, so use a compatible Go type whenever you reopen the same collection.

Collection names may contain nested paths:

```go
users := medb.C[User](db, "prod/eu/users")
```

This collection is stored as `data/prod/eu/users.json`.

### Updating documents

Use `Update` when the new value depends on the current document:

```go
err := users.Update("ada", func(user User) (User, error) {
	user.Age++
	return user, nil
})
if err != nil {
	panic(err)
}
```

`Update` prevents read-modify-write races between goroutines. Keep the callback short and do not call other methods on the same database from inside it.

### IDs, checks, and iteration

```go
id := medb.NewID()

if err := users.Set(id, User{Name: "Grace", Age: 45}); err != nil {
	panic(err)
}

fmt.Println(users.Has(id))
fmt.Println(users.Count())

for id, user := range users.All() {
	fmt.Printf("%s: %s\n", id, user.Name)
}
```

`NewID` returns a random 128-bit hexadecimal identifier. `All` iterates over a stable snapshot of the collection, ordered by document ID.

### Deleting data

Delete one document:

```go
if err := users.Delete("ada"); err != nil {
	panic(err)
}
```

List and remove entire collections:

```go
for _, name := range db.Collections() {
	fmt.Println(name)
}

if err := db.Drop("users"); err != nil {
	panic(err)
}
```

Deleting a missing document or collection is a no-op. Successful `Set`, `Update`, `Delete`, and `Drop` calls are durable when they return.

### Error handling

MeDB exposes sentinel errors that can be checked with `errors.Is`:

```go
user, err := users.Get("missing")

switch {
case errors.Is(err, medb.ErrNotFound):
	fmt.Println("user not found")
case err != nil:
	panic(err)
default:
	fmt.Println(user)
}
```

Other sentinel errors include:

- `ErrLocked` when another process has already opened the database directory.
- `ErrClosed` when an operation is attempted after `Close`.
- `ErrTooLarge` when a document exceeds the configured size limit.
- `ErrDirSync` when a filesystem directory change cannot be made durable.

MeDB is safe to use from multiple goroutines, but a database directory can be opened by only one process at a time.

### Configuration

Database options can control document size and when JSON snapshots are written:

```go
db, err := medb.Open(
	"data",
	medb.WithMaxDocSize(4<<20),          // 4 MiB per document
	medb.WithFlushBytes(16<<20),         // snapshot at 16 MiB of WAL
	medb.WithFlushInterval(time.Second), // snapshot changed collections every second
)
if err != nil {
	panic(err)
}
```

`WithFlushBytes` and `WithFlushInterval` control snapshot timing, not write durability. Every successful change is synced to the write-ahead log before returning.

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
