package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/antonmedv/medb/internal/wal"
)

func record(size int) []byte {
	return []byte(fmt.Sprintf(`{"op":"set","coll":"users","id":"user:1","doc":{"pad":%q}}`, strings.Repeat("x", size)))
}

func BenchmarkEnqueue(b *testing.B) {
	payload := record(128)
	for _, writers := range []int{1, 2, 4, 8, 16, 64} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			l := open(b, filepath.Join(b.TempDir(), "wal.log"))
			defer l.Close()
			b.ReportAllocs()
			b.ResetTimer()

			var wg sync.WaitGroup
			for w := range writers {
				n := b.N / writers
				if w < b.N%writers {
					n++
				}
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					for range n {
						if err := l.Enqueue(payload).Wait(); err != nil {
							b.Error(err)
						}
					}
				}(n)
			}
			wg.Wait()

			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
		})
	}
}

func BenchmarkEnqueueSize(b *testing.B) {
	for _, size := range []int{128, 4096, 65536} {
		b.Run(fmt.Sprintf("doc=%d", size), func(b *testing.B) {
			l := open(b, filepath.Join(b.TempDir(), "wal.log"))
			defer l.Close()
			payload := record(size)
			b.SetBytes(int64(len(payload) + 1))
			b.ResetTimer()
			for range b.N {
				if err := l.Enqueue(payload).Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRecords(b *testing.B) {
	payload := record(128)
	for _, n := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "wal.log")
			var content []byte
			for range n {
				content = append(content, payload...)
				content = append(content, '\n')
			}
			if err := os.WriteFile(path, content, 0o600); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(content)))
			b.ResetTimer()
			for range b.N {
				recs, err := wal.Records(path)
				if err != nil {
					b.Fatal(err)
				}
				if len(recs) != n {
					b.Fatalf("read %d records, want %d", len(recs), n)
				}
			}
		})
	}
}
