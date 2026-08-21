// Command bench compares MeDB with the naive alternative it replaces: a Go map
// rewritten to a JSON file on every change, without fsync.
//
// Each case starts from a collection of the given size, then measures steady
// state: writes that replace an existing document, so the collection size does
// not change while the benchmark runs.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonmedv/medb"
)

const coll = "users"

// medbFlushInterval mirrors the default of medb.WithFlushInterval and is used
// to amortize snapshot bytes over the writes between two snapshots.
const medbFlushInterval = 5 * time.Second

type User struct {
	Name string
	Age  int
}

// record mirrors the MeDB write-ahead log record so the bench can compute the
// exact number of bytes MeDB appends for one write.
type record struct {
	Op   string          `json:"op"`
	Coll string          `json:"coll"`
	ID   string          `json:"id,omitempty"`
	Doc  json.RawMessage `json:"doc,omitempty"`
}

type store interface {
	set(id string, u User) error
	get(id string) (User, bool)
	close() error
}

// mapJSON keeps documents in a map and rewrites the whole map as a JSON file on
// every change. With fsync false an acknowledged write is only in the page
// cache, so a crash can lose it, and a crash mid-write can leave a partial file.
type mapJSON struct {
	path  string
	m     map[string]User
	fsync bool
}

func openMapJSON(path string, fsync bool) (*mapJSON, error) {
	m := map[string]User{}
	switch data, err := os.ReadFile(path); {
	case err == nil:
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	return &mapJSON{path: path, m: m, fsync: fsync}, nil
}

func (s *mapJSON) set(id string, u User) error {
	s.m[id] = u
	data, err := json.Marshal(s.m)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil && s.fsync {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (s *mapJSON) get(id string) (User, bool) {
	u, ok := s.m[id]
	return u, ok
}

func (s *mapJSON) close() error { return nil }

type medbStore struct {
	db *medb.DB
	c  *medb.Collection[User]
}

func openMedb(dir string) (*medbStore, error) {
	db, err := medb.Open(dir)
	if err != nil {
		return nil, err
	}
	return &medbStore{db: db, c: medb.C[User](db, coll)}, nil
}

func (s *medbStore) set(id string, u User) error { return s.c.Set(id, u) }

func (s *medbStore) get(id string) (User, bool) {
	u, err := s.c.Get(id)
	return u, err == nil
}

func (s *medbStore) close() error { return s.db.Close() }

type result struct {
	Store         string  `json:"store"`
	Size          int     `json:"size"`
	Writes        int     `json:"writes"`
	Seconds       float64 `json:"seconds"`
	WritesPerSec  float64 `json:"writes_per_sec"`
	MeanUS        float64 `json:"mean_us"`
	P50US         float64 `json:"p50_us"`
	P99US         float64 `json:"p99_us"`
	BytesPerWrite float64 `json:"bytes_per_write"`
	ReadsPerSec   float64 `json:"reads_per_sec"`
	Writers       int     `json:"writers"`
	ParWritesPS   float64 `json:"par_writes_per_sec"`
	Durable       bool    `json:"durable"`
}

type spec struct {
	name    string
	durable bool
	// lock reports that the store is not safe for concurrent use and needs an
	// external mutex, the way a plain map behind a JSON file does.
	lock bool
	open func(dir string) (store, error)
}

// sink keeps the read loop from being optimized away; a zero sum after the run
// would mean every read missed.
var sink int

func main() {
	var (
		sizesFlag = flag.String("sizes", "10,100,1000,10000,100000", "collection sizes to measure")
		writeTime = flag.Duration("write-time", 1500*time.Millisecond, "time budget per write case")
		readTime  = flag.Duration("read-time", 300*time.Millisecond, "time budget per read case")
		minWrites = flag.Int("min-writes", 20, "minimum writes per case")
		maxWrites = flag.Int("max-writes", 2_000_000, "maximum writes per case")
		writers   = flag.Int("writers", 64, "concurrent writers in the parallel phase")
		out       = flag.String("out", "results.json", "where to write results as JSON")
		workDir   = flag.String("work", "", "directory for benchmark data (default: a temp dir)")
	)
	flag.Parse()

	sizes, err := parseSizes(*sizesFlag)
	if err != nil {
		fatal(err)
	}
	work := *workDir
	if work == "" {
		work, err = os.MkdirTemp("", "medb-bench-")
		if err != nil {
			fatal(err)
		}
		defer os.RemoveAll(work)
	} else if err := os.MkdirAll(work, 0o700); err != nil {
		fatal(err)
	}

	specs := []spec{
		{"map+json", false, true, func(dir string) (store, error) {
			return openMapJSON(filepath.Join(dir, coll+".json"), false)
		}},
		{"map+json+fsync", true, true, func(dir string) (store, error) {
			return openMapJSON(filepath.Join(dir, coll+".json"), true)
		}},
		{"medb", true, false, func(dir string) (store, error) { return openMedb(dir) }},
	}

	var results []result
	for _, size := range sizes {
		ids, seed := makeSeed(size)
		seedJSON, err := json.Marshal(seed)
		if err != nil {
			fatal(err)
		}
		for _, sp := range specs {
			dir := filepath.Join(work, fmt.Sprintf("%s-%d", strings.NewReplacer("+", "-").Replace(sp.name), size))
			if err := os.RemoveAll(dir); err != nil {
				fatal(err)
			}
			if err := os.MkdirAll(dir, 0o700); err != nil {
				fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, coll+".json"), seedJSON, 0o600); err != nil {
				fatal(err)
			}
			r, err := runCase(sp, size, ids, dir, len(seedJSON), *writeTime, *readTime, *minWrites, *maxWrites, *writers)
			if err != nil {
				fatal(err)
			}
			if err := os.RemoveAll(dir); err != nil {
				fatal(err)
			}
			results = append(results, r)
			fmt.Printf("%-16s n=%-7d %9.0f writes/s  %9.0f writes/s x%d  p50 %8.1fus  p99 %9.1fus  %10s/write  %10.0f reads/s\n",
				r.Store, r.Size, r.WritesPerSec, r.ParWritesPS, r.Writers, r.P50US, r.P99US, bytes(r.BytesPerWrite), r.ReadsPerSec)
		}
	}

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*out, append(data, '\n'), 0o600); err != nil {
		fatal(err)
	}
	if sink == 0 {
		fmt.Fprintln(os.Stderr, "bench: read phase found no documents")
	}
	fmt.Printf("\nwrote %s\n", *out)
}

func runCase(sp spec, size int, ids []string, dir string, snapBytes int, writeTime, readTime time.Duration, minWrites, maxWrites, writers int) (result, error) {
	s, err := sp.open(dir)
	if err != nil {
		return result{}, err
	}
	rnd := rand.New(rand.NewSource(1))

	for i := 0; i < 5; i++ {
		if err := s.set(ids[rnd.Intn(len(ids))], User{Name: name(i), Age: i}); err != nil {
			return result{}, err
		}
	}

	lat := make([]float64, 0, 1<<16)
	writes := 0
	start := time.Now()
	for {
		id := ids[rnd.Intn(len(ids))]
		t := time.Now()
		if err := s.set(id, User{Name: name(writes), Age: writes}); err != nil {
			return result{}, err
		}
		d := time.Since(t)
		writes++
		if len(lat) < cap(lat) {
			lat = append(lat, float64(d.Nanoseconds())/1e3)
		}
		if writes >= maxWrites || (writes >= minWrites && time.Since(start) >= writeTime) {
			break
		}
	}
	elapsed := time.Since(start)

	reads := 0
	rstart := time.Now()
	for {
		u, ok := s.get(ids[rnd.Intn(len(ids))])
		if ok {
			sink += u.Age
		}
		reads++
		if reads%64 == 0 && time.Since(rstart) >= readTime {
			break
		}
	}
	rElapsed := time.Since(rstart)

	par, parElapsed, err := runParallel(s, sp, ids, writeTime, writers, maxWrites)
	if err != nil {
		return result{}, err
	}

	if err := s.close(); err != nil {
		return result{}, err
	}

	rate := float64(writes) / elapsed.Seconds()
	sort.Float64s(lat)
	r := result{
		Store:         sp.name,
		Size:          size,
		Writes:        writes,
		Seconds:       elapsed.Seconds(),
		WritesPerSec:  rate,
		MeanUS:        elapsed.Seconds() * 1e6 / float64(writes),
		P50US:         percentile(lat, 0.50),
		P99US:         percentile(lat, 0.99),
		BytesPerWrite: bytesPerWrite(sp.name, snapBytes, walRecordBytes(ids[0], User{Name: name(0), Age: 0}), rate),
		ReadsPerSec:   float64(reads) / rElapsed.Seconds(),
		Writers:       writers,
		ParWritesPS:   float64(par) / parElapsed.Seconds(),
		Durable:       sp.durable,
	}
	return r, nil
}

// runParallel measures writers goroutines writing at the same time, the shape
// of a server handling concurrent requests. MeDB groups the writes waiting on
// one fsync; a map behind a JSON file has to serialize them.
func runParallel(s store, sp spec, ids []string, budget time.Duration, writers, maxWrites int) (int, time.Duration, error) {
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		count atomic.Int64
		errs  = make([]error, writers)
	)
	deadline := time.Now().Add(budget)
	start := time.Now()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w) + 1))
			for i := 0; ; i++ {
				id := ids[rnd.Intn(len(ids))]
				u := User{Name: name(w*1_000 + i), Age: i}
				var err error
				if sp.lock {
					mu.Lock()
					err = s.set(id, u)
					mu.Unlock()
				} else {
					err = s.set(id, u)
				}
				if err != nil {
					errs[w] = err
					return
				}
				n := count.Add(1)
				if n >= int64(maxWrites) || time.Now().After(deadline) {
					return
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, err := range errs {
		if err != nil {
			return 0, 0, err
		}
	}
	return int(count.Load()), elapsed, nil
}

// bytesPerWrite models the bytes one write hands to the filesystem in steady
// state. A map rewritten as JSON writes the whole collection every time. MeDB
// appends one log record per write and rewrites the collection once per flush
// interval, so its snapshot cost is spread over the writes in that interval.
func bytesPerWrite(store string, snapBytes, recBytes int, rate float64) float64 {
	if store != "medb" {
		return float64(snapBytes)
	}
	perInterval := rate * medbFlushInterval.Seconds()
	if perInterval < 1 {
		perInterval = 1
	}
	return float64(recBytes) + float64(snapBytes)/perInterval
}

func walRecordBytes(id string, u User) int {
	doc, err := json.Marshal(u)
	if err != nil {
		fatal(err)
	}
	rec, err := json.Marshal(record{Op: "set", Coll: coll, ID: id, Doc: doc})
	if err != nil {
		fatal(err)
	}
	return len(rec) + 1 // trailing newline
}

func makeSeed(size int) ([]string, map[string]User) {
	ids := make([]string, size)
	m := make(map[string]User, size)
	for i := 0; i < size; i++ {
		ids[i] = fmt.Sprintf("u%07d", i)
		m[ids[i]] = User{Name: name(i), Age: i}
	}
	return ids, m
}

func name(i int) string { return fmt.Sprintf("user%06d", i%1_000_000) }

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}

func parseSizes(s string) ([]int, error) {
	var sizes []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, fmt.Errorf("size must be positive, got %d", n)
		}
		sizes = append(sizes, n)
	}
	return sizes, nil
}

func bytes(n float64) string {
	switch {
	case n >= 1e6:
		return fmt.Sprintf("%.1fMB", n/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fKB", n/1e3)
	default:
		return fmt.Sprintf("%.0fB", n)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}
