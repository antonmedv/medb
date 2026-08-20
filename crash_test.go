package medb_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/antonmedv/medb"
)

func TestCrashDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness spawns subprocesses; skipped in -short")
	}
	for round, killAfter := range []int{5, 20, 40} {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			crashRound(t, killAfter)
		})
	}
}

func crashRound(t *testing.T, killAfter int) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWriterHelper$")
	cmd.Env = append(os.Environ(), "MEDB_CRASH_HELPER=1", "MEDB_CRASH_DIR="+dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	lastAck := -1
	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		var n int
		if _, err := fmt.Sscanf(scanner.Text(), "ACK %d", &n); err == nil {
			lastAck = n
			if lastAck+1 >= killAfter {
				break
			}
		}
	}
	_ = cmd.Process.Kill()
	_, _ = io.Copy(io.Discard, out)
	_ = cmd.Wait()

	if lastAck < 0 {
		t.Fatalf("helper produced no acks; stderr:\n%s", stderr.String())
	}

	db := openDB(t, dir)
	defer closeDB(t, db)
	docs := medb.C[int](db, "crash")
	for i := 0; i <= lastAck; i++ {
		id := fmt.Sprintf("doc-%06d", i)
		v, err := docs.Get(id)
		if err != nil {
			t.Fatalf("acknowledged %s lost after SIGKILL: %v", id, err)
		}
		if v != i {
			t.Fatalf("%s = %d, want %d", id, v, i)
		}
	}
}

// TestCrashWriterHelper is not a test: it is the crash-victim subprocess for
// TestCrashDurability and does nothing unless spawned by it.
func TestCrashWriterHelper(t *testing.T) {
	if os.Getenv("MEDB_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper for TestCrashDurability")
	}
	db, err := medb.Open(os.Getenv("MEDB_CRASH_DIR"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper open:", err)
		os.Exit(1)
	}
	docs := medb.C[int](db, "crash")
	for i := 0; ; i++ {
		if err := docs.Set(fmt.Sprintf("doc-%06d", i), i); err != nil {
			fmt.Fprintln(os.Stderr, "helper set:", err)
			os.Exit(1)
		}
		fmt.Printf("ACK %d\n", i)
	}
}
