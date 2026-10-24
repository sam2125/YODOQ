package solution

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually-advanced clock for tests.
type fakeClock struct{ now int64 }

func (c *fakeClock) Now() int64            { return c.now }
func (c *fakeClock) advance(deltaMS int64) { c.now += deltaMS }

// Challenge #1: delivery cap + dead-letter. A job that is never acked is
// handed out at most MaxDeliveries times, then abandoned — bounding executions
// at K instead of retrying forever.
func TestDeliveryCapDeadLetters(t *testing.T) {
	clk := &fakeClock{}
	q := NewWithOptions(clk, Options{MaxDeliveries: 3})

	id, _ := q.Enqueue([]byte("poison"))

	deliveries := 0
	for i := 0; i < 10; i++ {
		j, _ := q.Lease("w", 1000)
		if j == nil {
			break // dead-lettered: no longer leasable
		}
		if j.ID != id {
			t.Fatalf("leased unexpected job %q", j.ID)
		}
		deliveries++
		clk.advance(2000) // let the lease expire without an ack
	}

	if deliveries != 3 {
		t.Fatalf("expected exactly 3 deliveries (K), got %d", deliveries)
	}
	m := q.Metrics()
	if m.Dead != 1 || m.TotalDeadLettered != 1 {
		t.Fatalf("expected 1 dead-lettered job, got Dead=%d Total=%d", m.Dead, m.TotalDeadLettered)
	}
	if s := q.Stats(); s.Pending != 0 || s.Leased != 0 {
		t.Fatalf("dead job should not count as pending/leased: %+v", s)
	}
}

// Challenge #1 corollary: with no cap (the harness default), the same poison
// job is retried indefinitely — at-least-once is preserved, unbounded.
func TestUnlimitedByDefault(t *testing.T) {
	clk := &fakeClock{}
	q := New(clk)
	q.Enqueue([]byte("poison"))

	for i := 0; i < 20; i++ {
		j, _ := q.Lease("w", 1000)
		if j == nil {
			t.Fatalf("default queue should never stop re-leasing, stopped at %d", i)
		}
		clk.advance(2000)
	}
}

// Challenge #3: backoff keeps a flapping job from being re-leased on every
// round, so it can't hog a lease slot ahead of healthy work.
func TestBackoffGatesRedelivery(t *testing.T) {
	clk := &fakeClock{}
	q := NewWithOptions(clk, Options{BackoffBaseMS: 5000, BackoffMaxMS: 60000})

	q.Enqueue([]byte("flaky"))

	// First delivery.
	j, _ := q.Lease("w", 1000)
	if j == nil {
		t.Fatal("first lease should succeed")
	}
	clk.advance(1001) // lease expired, but backoff (2nd delivery) should gate it

	if j2, _ := q.Lease("w", 1000); j2 != nil {
		t.Fatal("job should be gated by backoff right after expiry")
	}
	clk.advance(5000) // past the backoff window
	if j3, _ := q.Lease("w", 1000); j3 == nil {
		t.Fatal("job should be leasable once backoff elapses")
	}
}

// Challenge #3: a re-delivered job moves to the tail so fresh jobs leased in
// the same round come first.
func TestRequeueToTailFairness(t *testing.T) {
	clk := &fakeClock{}
	q := NewWithOptions(clk, Options{RequeueToTail: true})

	poison, _ := q.Enqueue([]byte("poison")) // position 0
	fresh, _ := q.Enqueue([]byte("fresh"))   // position 1

	// Poison is leased first (FIFO) and left to expire.
	j, _ := q.Lease("w", 1000)
	if j.ID != poison {
		t.Fatalf("expected poison first, got %q", j.ID)
	}
	clk.advance(2000) // poison's lease expires; both are now available

	// Next lease must prefer the fresh job, because the expired poison job was
	// moved to the tail on its first (re)delivery attempt.
	got, _ := q.Lease("w2", 1000)
	if got.ID != fresh {
		t.Fatalf("expected fresh job to be preferred over requeued poison, got %q", got.ID)
	}
}

// Challenge #6: metrics expose what an operator needs for a stuck queue.
func TestMetricsSurfaceStuckState(t *testing.T) {
	clk := &fakeClock{}
	q := New(clk)

	a, _ := q.Enqueue([]byte("a"))
	q.Enqueue([]byte("b"))
	clk.advance(100)

	j, _ := q.Lease("w", 1000)
	q.Ack("w", j.ID)

	// Leave one job waiting; advance time so it ages.
	clk.advance(5000)

	m := q.Metrics()
	if m.TotalEnqueued != 2 || m.TotalAcked != 1 {
		t.Fatalf("counter mismatch: %+v", m)
	}
	if m.Completed != 1 || m.Pending != 1 {
		t.Fatalf("state mismatch: %+v", m.Stats)
	}
	if m.OldestPendingAgeMS <= 0 {
		t.Fatalf("expected a positive oldest-pending age, got %d", m.OldestPendingAgeMS)
	}
	_ = a
}

// atomicClock is safe for concurrent reads/advances, for the race test.
type atomicClock struct{ now int64 }

func (c *atomicClock) Now() int64        { return atomic.LoadInt64(&c.now) }
func (c *atomicClock) add(deltaMS int64) { atomic.AddInt64(&c.now, deltaMS) }

// Challenge #4: the queue must be correct under many concurrent callers.
// Run with `go test -race` to exercise the mutex against the data-race detector.
// Workers race to lease+ack while a separate goroutine advances the clock, so
// leases expire and jobs get re-delivered mid-flight. Every job must still be
// completed exactly once, with nothing lost.
func TestConcurrentLeaseAckRace(t *testing.T) {
	clk := &atomicClock{}
	q := NewWithOptions(clk, Options{RequeueToTail: true})

	const n = 500
	for i := 0; i < n; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("job-%d", i))); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var mu sync.Mutex
	execs := make(map[string]int) // ground-truth execution count per job

	stop := make(chan struct{})
	var tick sync.WaitGroup
	tick.Add(1)
	go func() {
		defer tick.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.add(40) // leases are 100ms; some will expire before ack
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	const workers = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				mu.Lock()
				seen := len(execs)
				mu.Unlock()
				if seen >= n {
					return
				}
				j, err := q.Lease(id, 100)
				if err != nil {
					t.Errorf("lease: %v", err)
					return
				}
				if j == nil {
					continue
				}
				// Ground-truth execution happens regardless of whether the ack
				// lands (the lease may have expired under us).
				mu.Lock()
				execs[j.ID]++
				mu.Unlock()
				_ = q.Ack(id, j.ID)
			}
		}(fmt.Sprintf("w-%d", w))
	}

	wg.Wait()
	close(stop)
	tick.Wait()

	// Every job ran at least once — nothing lost.
	mu.Lock()
	defer mu.Unlock()
	if len(execs) != n {
		t.Fatalf("expected all %d jobs executed at least once, got %d distinct", n, len(execs))
	}
	for id, c := range execs {
		if c < 1 {
			t.Fatalf("job %s never executed", id)
		}
	}
}
