// Package stampeded enqueues and runs stampeded jobs from Go over libstampeded_ffi.
// Calls block their thread, so run Puller.Next from as many goroutines as you want jobs in flight.
//
//	CGO_LDFLAGS="-L/path/to/stampeded/target/release -Wl,-rpath,/path/to/stampeded/target/release" go build
package stampeded

/*
#cgo LDFLAGS: -lstampeded_ffi
#include <stdlib.h>
#include "stampeded.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

// Backoff, as a Fail delay, retries the job on its own backoff schedule.
const Backoff time.Duration = -1

// ErrSettled is returned by a second outcome for the same lease.
var ErrSettled = errors.New("stampeded: lease already settled")

// ErrClosed is returned by calls on a closed Client or Puller.
var ErrClosed = errors.New("stampeded: closed")

// Error is a failure the library reported.
type Error struct {
	Op      string
	Message string
}

func (e *Error) Error() string { return "stampeded: " + e.Op + ": " + e.Message }

// Version is the library's version.
func Version() string { return C.GoString(C.stampeded_version()) }

// LogInit sends the library's logs to stderr at filter, e.g. "stampeded=debug" ("" for "warn").
// It reports false when this process already has a logger.
func LogInit(filter string) (bool, error) {
	var spec *C.char
	if filter != "" {
		spec = C.CString(filter)
		defer C.free(unsafe.Pointer(spec))
	}
	s, err := call("log_init", func() C.int32_t { return C.stampeded_log_init(spec) })
	return s == C.STAMPEDED_OK, err
}

// call runs f and reads its error on one locked OS thread: the library's last error is per thread.
func call(op string, f func() C.int32_t) (C.int32_t, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if s := f(); s != C.STAMPEDED_ERROR {
		return s, nil
	}
	return C.STAMPEDED_ERROR, lastError(op)
}

func construct(op string, f func() unsafe.Pointer) (unsafe.Pointer, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if p := f(); p != nil {
		return p, nil
	}
	return nil, lastError(op)
}

func lastError(op string) error {
	message := "unknown error"
	if p := C.stampeded_last_error(); p != nil {
		message = C.GoString(p)
	}
	return &Error{Op: op, Message: message}
}

// cstring is s in C memory, or NULL for "". Free it with C.free.
func cstring(s string) *C.char {
	if s == "" {
		return nil
	}
	return C.CString(s)
}

func millis(d time.Duration) C.uint32_t {
	return C.uint32_t(min(max(d.Milliseconds(), 0), 1<<32-1))
}

// Client is a connection pool to a stampeded database.
type Client struct {
	ptr     unsafe.Pointer // *C.stampeded_client, swapped to nil once freed
	cleanup runtime.Cleanup
}

// Connect to a Postgres URL; its options parameter may set a search_path.
func Connect(url string) (*Client, error) {
	u := C.CString(url)
	defer C.free(unsafe.Pointer(u))
	p, err := construct("connect", func() unsafe.Pointer { return unsafe.Pointer(C.stampeded_connect(u)) })
	if err != nil {
		return nil, err
	}
	c := &Client{ptr: p}
	c.cleanup = runtime.AddCleanup(c, func(p unsafe.Pointer) { C.stampeded_client_free((*C.stampeded_client)(p)) }, p)
	return c, nil
}

// Close frees the client. Pullers started from it keep working until closed.
func (c *Client) Close() {
	if p := atomic.SwapPointer(&c.ptr, nil); p != nil {
		c.cleanup.Stop()
		C.stampeded_client_free((*C.stampeded_client)(p))
	}
}

func (c *Client) handle() *C.stampeded_client {
	return (*C.stampeded_client)(atomic.LoadPointer(&c.ptr))
}

// EnqueueOptions are optional settings for Enqueue.
type EnqueueOptions struct {
	// Meta is runtime context, encoded as a JSON object. nil for {}.
	Meta any
	// Unique refuses a second job with this scope in the queue until the first finishes.
	Unique   string
	Priority int16
	// MaxAttempts 0 is the default, 25.
	MaxAttempts int16
	// ScheduledAt zero is now.
	ScheduledAt time.Time
}

// Enqueue a job; args is encoded as a JSON object of named arguments (nil for {}).
// queued is false, with no id, when a job with the same Unique scope already waits.
func (c *Client) Enqueue(queue, task string, args any, opts *EnqueueOptions) (id string, queued bool, err error) {
	client := c.handle()
	if client == nil {
		return "", false, ErrClosed
	}
	argsJSON, err := encode(args)
	if err != nil {
		return "", false, err
	}

	q, t, a := C.CString(queue), C.CString(task), C.CString(argsJSON)
	defer C.free(unsafe.Pointer(q))
	defer C.free(unsafe.Pointer(t))
	defer C.free(unsafe.Pointer(a))

	var options *C.stampeded_enqueue_options
	if opts != nil {
		options = &C.stampeded_enqueue_options{
			priority:     C.int16_t(opts.Priority),
			max_attempts: C.int16_t(opts.MaxAttempts),
		}
		if opts.Meta != nil {
			meta, err := encode(opts.Meta)
			if err != nil {
				return "", false, err
			}
			options.meta_json = C.CString(meta)
			defer C.free(unsafe.Pointer(options.meta_json))
		}
		options.unique = cstring(opts.Unique)
		defer C.free(unsafe.Pointer(options.unique))
		if !opts.ScheduledAt.IsZero() {
			options.scheduled_at_unix_ms = C.int64_t(opts.ScheduledAt.UnixMilli())
		}
	}

	var out [37]C.char
	s, err := call("enqueue", func() C.int32_t {
		return C.stampeded_enqueue(client, q, t, a, options, &out[0])
	})
	if err != nil || s == C.STAMPEDED_NONE {
		return "", false, err
	}
	return C.GoString(&out[0]), true, nil
}

func encode(v any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	b, err := json.Marshal(v)
	return string(b), err
}

// PullerOptions configure Pull. Zero a field for its default.
type PullerOptions struct {
	// Queues is "mail", "default,mail" or "*"; "" for "default".
	Queues string
	// Except, with "*", lists queues to leave alone, e.g. "dlq".
	Except string
	// MaxLeases is how many jobs may be out at once; 0 for 10.
	MaxLeases uint32
	// BatchSize is how many jobs one round trip claims; 0 for 5.
	BatchSize uint32
	// PollInterval is how often to look for work when there is none; 0 for 1s.
	PollInterval time.Duration
	// HeartbeatInterval keeps the worker alive to the reaper; 0 for 30s.
	HeartbeatInterval time.Duration
}

// Puller is a registered worker whose jobs the caller runs.
type Puller struct {
	// WorkerID is the stampeded_workers row this puller registered as.
	WorkerID string

	ptr     unsafe.Pointer // *C.stampeded_puller, swapped to nil once freed
	cleanup runtime.Cleanup
}

// Pull registers a worker and starts its heartbeat.
func (c *Client) Pull(opts PullerOptions) (*Puller, error) {
	client := c.handle()
	if client == nil {
		return nil, ErrClosed
	}
	options := C.stampeded_puller_options{
		queues:                cstring(opts.Queues),
		except_queues:         cstring(opts.Except),
		max_leases:            C.uint32_t(opts.MaxLeases),
		batch_size:            C.uint32_t(opts.BatchSize),
		poll_interval_ms:      millis(opts.PollInterval),
		heartbeat_interval_ms: millis(opts.HeartbeatInterval),
	}
	defer C.free(unsafe.Pointer(options.queues))
	defer C.free(unsafe.Pointer(options.except_queues))

	p, err := construct("puller_start", func() unsafe.Pointer {
		return unsafe.Pointer(C.stampeded_puller_start(client, &options))
	})
	if err != nil {
		return nil, err
	}
	puller := &Puller{WorkerID: C.GoString(C.stampeded_puller_worker_id((*C.stampeded_puller)(p))), ptr: p}
	puller.cleanup = runtime.AddCleanup(puller, func(p unsafe.Pointer) { C.stampeded_puller_free((*C.stampeded_puller)(p)) }, p)
	return puller, nil
}

// Next waits up to timeout for a job. nil, nil when none came, every lease is
// out, or the puller is stopping.
func (p *Puller) Next(timeout time.Duration) (*Lease, error) {
	puller := p.handle()
	if puller == nil {
		return nil, ErrClosed
	}
	var out *C.stampeded_lease
	s, err := call("puller_next", func() C.int32_t {
		return C.stampeded_puller_next(puller, millis(timeout), &out)
	})
	if err != nil || s == C.STAMPEDED_NONE {
		return nil, err
	}
	return newLease(out), nil
}

// Stop hands out no more jobs, gives unstarted ones back, and waits up to
// drain for leases still out. Close the puller afterwards.
func (p *Puller) Stop(drain time.Duration) error {
	puller := p.handle()
	if puller == nil {
		return ErrClosed
	}
	_, err := call("puller_stop", func() C.int32_t { return C.stampeded_puller_stop(puller, millis(drain)) })
	return err
}

// Close frees the puller. Without Stop first, its worker and any leases
// still out are left to the reaper. No goroutine may be inside Next.
func (p *Puller) Close() {
	if ptr := atomic.SwapPointer(&p.ptr, nil); ptr != nil {
		p.cleanup.Stop()
		C.stampeded_puller_free((*C.stampeded_puller)(ptr))
	}
}

func (p *Puller) handle() *C.stampeded_puller {
	return (*C.stampeded_puller)(atomic.LoadPointer(&p.ptr))
}

// Lease is one claimed job until it reports an outcome. Released or garbage collected without
// one, the job goes back to the queue as an interruption.
type Lease struct {
	ID    string
	Queue string
	Task  string
	// Args is the job's named arguments, a JSON object.
	Args json.RawMessage
	// Meta is runtime context, a JSON object namespaced by runtime.
	Meta json.RawMessage
	// Job is the whole row, every column.
	Job     json.RawMessage
	Attempt int

	ptr     unsafe.Pointer // *C.stampeded_lease, swapped to nil once freed
	cleanup runtime.Cleanup
}

func newLease(p *C.stampeded_lease) *Lease {
	l := &Lease{
		ID:      C.GoString(C.stampeded_lease_id(p)),
		Queue:   C.GoString(C.stampeded_lease_queue(p)),
		Task:    C.GoString(C.stampeded_lease_task(p)),
		Args:    json.RawMessage(C.GoString(C.stampeded_lease_args(p))),
		Meta:    json.RawMessage(C.GoString(C.stampeded_lease_meta(p))),
		Job:     json.RawMessage(C.GoString(C.stampeded_lease_job(p))),
		Attempt: int(C.stampeded_lease_attempt(p)),
		ptr:     unsafe.Pointer(p),
	}
	l.cleanup = runtime.AddCleanup(l, func(p unsafe.Pointer) { C.stampeded_lease_free((*C.stampeded_lease)(p)) }, unsafe.Pointer(p))
	return l
}

// Checkpoint saves resume state, encoded as JSON, for the next attempt.
// False when the job is no longer this worker's: stop working on it.
func (l *Lease) Checkpoint(state any) (bool, error) {
	lease := (*C.stampeded_lease)(atomic.LoadPointer(&l.ptr))
	if lease == nil {
		return false, ErrSettled
	}
	s, err := json.Marshal(state)
	if err != nil {
		return false, err
	}
	text := C.CString(string(s))
	defer C.free(unsafe.Pointer(text))
	status, err := call("lease_checkpoint", func() C.int32_t { return C.stampeded_lease_checkpoint(lease, text) })
	return status == C.STAMPEDED_OK, err
}

// Complete reports success.
func (l *Lease) Complete() error {
	return l.settle("lease_complete", "", func(p *C.stampeded_lease, _ *C.char) C.int32_t {
		return C.stampeded_lease_complete(p)
	})
}

// Fail reports a failure: the job is retried after retryIn, or on its
// backoff schedule with Backoff, until it runs out of attempts.
func (l *Lease) Fail(message string, retryIn time.Duration) error {
	delay := C.int64_t(-1)
	if retryIn >= 0 {
		delay = C.int64_t(retryIn.Milliseconds())
	}
	return l.settle("lease_fail", message, func(p *C.stampeded_lease, m *C.char) C.int32_t {
		return C.stampeded_lease_fail(p, m, delay)
	})
}

// TimedOut reports that the job ran past the caller's limit: retried on its
// backoff schedule, recorded as timed_out.
func (l *Lease) TimedOut(message string) error {
	return l.settle("lease_timed_out", message, func(p *C.stampeded_lease, m *C.char) C.int32_t {
		return C.stampeded_lease_timed_out(p, m)
	})
}

// Discard reports that the job can never succeed: discarded whatever attempts remain.
func (l *Lease) Discard(message string) error {
	return l.settle("lease_discard", message, func(p *C.stampeded_lease, m *C.char) C.int32_t {
		return C.stampeded_lease_discard(p, m)
	})
}

// Release gives the lease back without an outcome: the job returns to the queue as an interruption.
func (l *Lease) Release() {
	if p := atomic.SwapPointer(&l.ptr, nil); p != nil {
		l.cleanup.Stop()
		C.stampeded_lease_free((*C.stampeded_lease)(p))
	}
}

func (l *Lease) settle(op, message string, f func(*C.stampeded_lease, *C.char) C.int32_t) error {
	p := (*C.stampeded_lease)(atomic.SwapPointer(&l.ptr, nil))
	if p == nil {
		return ErrSettled
	}
	l.cleanup.Stop()
	m := C.CString(message)
	defer C.free(unsafe.Pointer(m))
	_, err := call(op, func() C.int32_t { return f(p, m) })
	return err
}
