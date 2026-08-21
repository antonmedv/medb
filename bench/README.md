# bench

Compares MeDB with the naive alternative it replaces: a Go map rewritten to a
JSON file on every change, without fsync.

```
go run . -write-time 2s -out results.json   # measure
python3 plot.py results.json                # render bench.png / bench.svg (+ dark)
```

This module replaces `github.com/antonmedv/medb` with `..`, so it always
measures the working tree, with or without a `go.work`. Plotting needs
matplotlib; only the PNGs are committed.

![MeDB vs. a map rewritten to JSON](bench.png)

## What is measured

Three stores, all holding the same documents:

| store | durability |
|---|---|
| `map+json` | none -- `os.WriteFile` of the whole map, no fsync, truncated in place |
| `map+json+fsync` | fsync per write, still not atomic |
| `medb` | fsync per write, atomic snapshots, torn writes recovered |

Each case seeds a collection of *n* documents (`{"Name":"user000123","Age":123}`),
then measures steady state: writes that replace an existing document, so the
collection never grows while the clock runs. Every case runs in a fresh
directory, one writer first, then 64 concurrent writers, plus a read phase.

`bytes/write` is the volume one change hands to the filesystem. Rewriting a map
costs the whole collection every time. MeDB appends one log record per write and
rewrites the collection once per flush interval (5 s by default), so its
snapshot cost is spread over the writes in that interval:
`record + snapshot / (writes_per_sec x 5 s)`.

## Results

Apple M4 Pro, 14 cores, APFS on internal NVMe, Go 1.27, macOS 25.5. `fsync` on
this machine is `F_FULLFSYNC` and costs ~4 ms.

| docs | store | writes/s | writes/s (64) | p50 | p99 | bytes/write | reads/s |
|---:|---|---:|---:|---:|---:|---:|---:|
| 10 | `map+json` | 30.3k | 27.8k | 0.03 ms | 0.05 ms | 411 B | 100.5M |
| 10 | `map+json+fsync` | 227 | 218 | 4.09 ms | 5.21 ms | 411 B | 87.3M |
| 10 | `medb` | 249 | 7.8k | 4.00 ms | 4.77 ms | 80 B | 4.5M |
| 100 | `map+json` | 19.6k | 19.5k | 0.04 ms | 0.19 ms | 4.2 KB | 72.7M |
| 100 | `map+json+fsync` | 214 | 220 | 4.92 ms | 6.01 ms | 4.2 KB | 80.4M |
| 100 | `medb` | 242 | 7.9k | 4.01 ms | 6.09 ms | 83 B | 4.5M |
| 1k | `map+json` | 5.1k | 4.7k | 0.19 ms | 0.38 ms | 42.9 KB | 85M |
| 1k | `map+json+fsync` | 180 | 184 | 5.64 ms | 6.96 ms | 42.9 KB | 47.7M |
| 1k | `medb` | 246 | 7.7k | 4.01 ms | 5.14 ms | 115 B | 4M |
| 10k | `map+json` | 526 | 516 | 1.84 ms | 2.41 ms | 438.9 KB | 47.6M |
| 10k | `map+json+fsync` | 137 | 126 | 7.06 ms | 8.96 ms | 438.9 KB | 49.7M |
| 10k | `medb` | 245 | 7.9k | 4.01 ms | 5.16 ms | 438 B | 3.7M |
| 100k | `map+json` | 43 | 42 | 22.99 ms | 25.77 ms | 4.5 MB | 31.3M |
| 100k | `map+json+fsync` | 34 | 35 | 28.91 ms | 45.11 ms | 4.5 MB | 30.9M |
| 100k | `medb` | 244 | 7.8k | 4.01 ms | 5.13 ms | 3.8 KB | 2.6M |

- **MeDB is flat in collection size, the JSON rewrite is linear.** MeDB holds
  ~245 writes/s and 4 ms p50 from 10 to 100k documents. `map+json` goes from 30k
  writes/s to 43, because one change writes 4.5 MB instead of 120 B.
- **One writer: the rewrite wins below ~20k documents.** A durable write costs
  one full fsync (~4 ms), which is more than rewriting a small file into the page
  cache. `map+json+fsync` shows the same 4 ms floor, so the gap is durability,
  not MeDB.
- **Concurrent writers: MeDB wins past ~440 documents.** MeDB batches the writes
  waiting on one fsync, so 64 writers get ~7.8k writes/s at every size -- 187x the
  rewrite at 100k documents. The map has to serialize, so it gets nothing from
  concurrency.
- **Write amplification is the whole story: 1194x at 100k documents.**
- **Reads cost more in MeDB**: ~2.6-4.5M/s against 31-100M/s, because `Get`
  unmarshals the stored JSON while the map returns the value it already holds.

## Notes

- Numbers are single-run, +/-10% between runs; fsync cost dominates and is
  filesystem-specific. Rerun locally before quoting.
- 100k documents at 4.5 MB per write is the point of the comparison, not a
  strawman: it is what "just write the map to a JSON file" does at that size.
