package stampeded

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// database makes a private stampeded schema, and a URL that reaches it through its options.
func database(t *testing.T) (string, func(string) string) {
	t.Helper()
	base := os.Getenv("STAMPEDED_TEST_DATABASE_URL")
	if base == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("STAMPEDED_TEST_DATABASE_URL is not set and CI is")
		}
		t.Skip("STAMPEDED_TEST_DATABASE_URL not set")
	}

	b := make([]byte, 8)
	_, _ = rand.Read(b)
	schema := "go_" + hex.EncodeToString(b)
	psql := func(sql string) string {
		t.Helper()
		cmd := exec.Command("psql", "-XAtq", "-v", "ON_ERROR_STOP=1", base, "-c", "SET search_path TO "+schema, "-c", sql)
		var stderr strings.Builder
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("psql %q: %v\n%s", sql, err, stderr.String())
		}
		return strings.TrimSpace(string(out))
	}

	run := exec.Command("psql", "-XAtq", "-v", "ON_ERROR_STOP=1", base, "-c", "CREATE SCHEMA "+schema)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("create schema: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("psql", "-XAtq", base, "-c", "SET client_min_messages TO WARNING", "-c", "DROP SCHEMA "+schema+" CASCADE").Run()
	})
	// The migrations, in order: Glob sorts.
	migrations, err := filepath.Glob("../crates/stampeded/schema/*.sql")
	if err != nil || len(migrations) == 0 {
		t.Fatalf("no migrations: %v", err)
	}
	args := []string{"-XAtq", "-v", "ON_ERROR_STOP=1", base}
	for _, m := range migrations {
		args = append(args, "-f", m)
	}
	install := exec.Command("psql", args...)
	install.Env = append(os.Environ(), "PGOPTIONS=-c search_path="+schema)
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("install schema: %v\n%s", err, out)
	}

	separator := "?"
	if strings.Contains(base, "?") {
		separator = "&"
	}
	return base + separator + "options=" + url.QueryEscape("-c search_path="+schema), psql
}

func connect(t *testing.T) (*Client, func(string) string) {
	t.Helper()
	dsn, psql := database(t)
	client, err := Connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, psql
}

func pull(t *testing.T, client *Client, opts PullerOptions) *Puller {
	t.Helper()
	puller, err := client.Pull(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(puller.Close)
	return puller
}

func next(t *testing.T, puller *Puller) *Lease {
	t.Helper()
	lease, err := puller.Next(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil {
		t.Fatal("no job came")
	}
	return lease
}

func TestALeaseIsCompletedFailedOrDiscarded(t *testing.T) {
	client, psql := connect(t)

	id, queued, err := client.Enqueue("go", "greet", map[string]any{"name": "gopher"}, &EnqueueOptions{Meta: map[string]any{"go": map[string]any{"version": runtime.Version()}}})
	if err != nil || !queued || len(id) != 36 {
		t.Fatalf("enqueue: %q %v %v", id, queued, err)
	}
	once := &EnqueueOptions{Unique: "only-once"}
	if _, queued, err := client.Enqueue("go", "flaky", nil, once); err != nil || !queued {
		t.Fatalf("enqueue flaky: %v %v", queued, err)
	}
	if again, queued, err := client.Enqueue("go", "flaky", nil, once); err != nil || queued || again != "" {
		t.Fatalf("a second unique job was queued: %q %v %v", again, queued, err)
	}
	if _, _, err := client.Enqueue("go", "doomed", nil, nil); err != nil {
		t.Fatal(err)
	}

	puller := pull(t, client, PullerOptions{Queues: "go", PollInterval: 50 * time.Millisecond})
	if len(puller.WorkerID) != 36 {
		t.Fatalf("worker id %q", puller.WorkerID)
	}

	for range 3 {
		lease := next(t, puller)
		switch lease.Task {
		case "greet":
			var args struct{ Name string }
			if err := json.Unmarshal(lease.Args, &args); err != nil || args.Name != "gopher" || lease.ID != id {
				t.Fatalf("greet: %s %s %v", lease.ID, lease.Args, err)
			}
			if !strings.Contains(string(lease.Meta), runtime.Version()) || !strings.Contains(string(lease.Job), puller.WorkerID) {
				t.Fatalf("meta %s / row %s", lease.Meta, lease.Job)
			}
			if ours, err := lease.Checkpoint(map[string]int{"cursor": 1}); err != nil || !ours {
				t.Fatalf("checkpoint: %v %v", ours, err)
			}
			if err := lease.Complete(); err != nil {
				t.Fatal(err)
			}
			if err := lease.Complete(); !errors.Is(err, ErrSettled) {
				t.Fatalf("a second outcome: %v", err)
			}
		case "flaky":
			if err := lease.Fail("downstream said no", 10*time.Minute); err != nil {
				t.Fatal(err)
			}
		case "doomed":
			if err := lease.Discard("will never work"); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected task %q", lease.Task)
		}
	}
	if err := puller.Stop(time.Second); err != nil {
		t.Fatal(err)
	}

	got := psql("SELECT string_agg(task || ':' || state || ':' || coalesce(failure_kind, '-') || ':' || checkpoint_seq, ' ' ORDER BY task) FROM stampeded_jobs")
	want := "doomed:discarded:unrecoverable:0 flaky:retryable:raised:0 greet:completed:-:1"
	if got != want {
		t.Fatalf("rows\n got %s\nwant %s", got, want)
	}
	if n := psql("SELECT count(*) FROM stampeded_workers"); n != "0" {
		t.Fatalf("%s workers left after stop", n)
	}
}

func TestManyGoroutinesRunEveryJobExactlyOnce(t *testing.T) {
	client, psql := connect(t)
	const jobs = 60
	for i := range jobs {
		if _, _, err := client.Enqueue("go", "count", map[string]int{"i": i}, nil); err != nil {
			t.Fatal(err)
		}
	}

	puller := pull(t, client, PullerOptions{Queues: "go", MaxLeases: 8, BatchSize: 4, PollInterval: 20 * time.Millisecond})
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				lease, err := puller.Next(500 * time.Millisecond)
				if err != nil {
					t.Error(err)
					return
				}
				if lease == nil {
					return
				}
				mu.Lock()
				seen[lease.ID]++
				mu.Unlock()
				if err := lease.Complete(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("ran %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s ran %d times", id, n)
		}
	}
	if n := psql("SELECT count(*) FROM stampeded_jobs WHERE state = 'completed'"); n != "60" {
		t.Fatalf("%s completed", n)
	}
}

func TestALeaseWithoutAnOutcomeComesBackAsAnInterruption(t *testing.T) {
	client, psql := connect(t)
	if _, _, err := client.Enqueue("go", "dropped", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Enqueue("go", "forgotten", nil, nil); err != nil {
		t.Fatal(err)
	}
	puller := pull(t, client, PullerOptions{Queues: "go", MaxLeases: 2, PollInterval: 20 * time.Millisecond})

	first, second := next(t, puller), next(t, puller)
	first.Release()
	// The other is simply forgotten: the garbage collector gives it back.
	second = nil
	_ = second

	deadline := time.Now().Add(10 * time.Second)
	for psql("SELECT count(*) FROM stampeded_jobs WHERE interruptions = 1 AND state <> 'executing'") != "2" {
		if time.Now().After(deadline) {
			t.Fatalf("not both back: %s", psql("SELECT string_agg(task || ':' || state || ':' || interruptions, ' ') FROM stampeded_jobs"))
		}
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFailuresAreErrorsWithTheLibrarysReason(t *testing.T) {
	if _, err := Connect("postgres://nobody@127.0.0.1:1/none"); err == nil {
		t.Fatal("connected to nothing")
	} else if e := new(Error); !errors.As(err, &e) || e.Op != "connect" || e.Message == "" {
		t.Fatalf("connect error: %#v", err)
	}

	client, _ := connect(t)
	if _, _, err := client.Enqueue("go", "bad", []int{1, 2}, nil); err == nil {
		t.Fatal("an args array was accepted")
	}
	if _, err := client.Pull(PullerOptions{Queues: "a,,b"}); err == nil || !strings.Contains(err.Error(), "blank") {
		t.Fatalf("a blank queue name: %v", err)
	}

	client.Close()
	if _, _, err := client.Enqueue("go", "late", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("enqueue after close: %v", err)
	}
}

func TestTheLibraryAnswers(t *testing.T) {
	if Version() == "" {
		t.Fatal("no version")
	}
	if _, err := LogInit("stampeded=loud"); err == nil {
		t.Fatal("a bad log filter was accepted")
	}
}
