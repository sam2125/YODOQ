package main

import (
	"fmt"
	mrand "math/rand"
	"strings"

	"yodoq/solution"
)

// =============================================================================
// Test infrastructure
// =============================================================================

// TestClock implements solution.Clock with manual advancement.
type TestClock struct{ now int64 }

func (c *TestClock) Now() int64           { return c.now }
func (c *TestClock) Advance(deltaMS int64) { c.now += deltaMS }

// ExecutionLedger is the harness's out-of-band ground truth: how many times
// did each job's processing function actually run? This is independent of
// what the queue thinks, and is the basis for grading.
type ExecutionLedger struct{ counts map[string]int }

func NewLedger() *ExecutionLedger { return &ExecutionLedger{counts: map[string]int{}} }
func (l *ExecutionLedger) Record(jobID string) { l.counts[jobID]++ }

func (l *ExecutionLedger) Lost(allIDs []string) int {
	n := 0
	for _, id := range allIDs {
		if l.counts[id] == 0 {
			n++
		}
	}
	return n
}

func (l *ExecutionLedger) Excess(allIDs []string) int {
	n := 0
	for _, id := range allIDs {
		if l.counts[id] > 1 {
			n += l.counts[id] - 1
		}
	}
	return n
}

func (l *ExecutionLedger) Total() int {
	n := 0
	for _, c := range l.counts {
		n += c
	}
	return n
}

// FailureSimulator parameterizes how often workers misbehave.
type FailureSimulator struct {
	CrashRate        float64
	AckDropRate      float64
	ProcessingTimeMS int64
	Rng              *mrand.Rand
}

func (f *FailureSimulator) ShouldCrash() bool   { return f.Rng.Float64() < f.CrashRate }
func (f *FailureSimulator) ShouldDropAck() bool { return f.Rng.Float64() < f.AckDropRate }

// Worker simulates a job consumer that may crash mid-process or have its
// acks lost in transit.
type Worker struct {
	ID       string
	Queue    *solution.JobQueue
	Clock    *TestClock
	Ledger   *ExecutionLedger
	Failures *FailureSimulator
	LeaseMS  int64
}

// Tick attempts one lease+process+ack cycle. Returns true if a job was attempted.
func (w *Worker) Tick() bool {
	job, err := w.Queue.Lease(w.ID, w.LeaseMS)
	if err != nil || job == nil {
		return false
	}

	// Simulate processing time (the harness's clock advances; in real life this
	// would be wall-clock work).
	w.Clock.Advance(w.Failures.ProcessingTimeMS)

	if w.Failures.ShouldCrash() {
		// Worker dies before recording or acking. The lease will eventually
		// expire and the queue should re-lease the job to another worker.
		return true
	}

	// Record successful execution (ground truth — this happens regardless of
	// whether the ack reaches the queue).
	w.Ledger.Record(job.ID)

	if w.Failures.ShouldDropAck() {
		// Ack lost in flight; queue still thinks we're working on it.
		return true
	}

	_ = w.Queue.Ack(w.ID, job.ID)
	return true
}

// runUntilDrained drives all workers in a loop, advancing the clock between
// rounds so expired leases become available again. Bails out when there's
// no progress to make or after a hard iteration cap.
func runUntilDrained(q *solution.JobQueue, clock *TestClock, workers []*Worker) {
	const maxIterations = 200000
	for i := 0; i < maxIterations; i++ {
		anyProgress := false
		for _, w := range workers {
			if w.Tick() {
				anyProgress = true
			}
		}
		// Advance time past the typical lease so expired leases recycle.
		clock.Advance(2000)

		s := q.Stats()
		if s.Pending == 0 && s.Leased == 0 {
			return
		}
		if !anyProgress && s.Pending == 0 {
			return
		}
	}
}

// =============================================================================
// Logger
// =============================================================================

func info(msg string)    { fmt.Printf("[INFO] %s\n", msg) }
func successf(msg string) { fmt.Printf("[SUCCESS] ✓ %s\n", msg) }
func errf(msg string)    { fmt.Printf("[ERROR] ✗ %s\n", msg) }
func warn(msg string)    { fmt.Printf("[WARNING] ⚠ %s\n", msg) }

func section(title string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("📬 YODOQ %s\n", strings.ToUpper(title))
	fmt.Println(strings.Repeat("=", 60))
}

func intro() {
	fmt.Println(`
██╗   ██╗ ██████╗ ██████╗  ██████╗  ██████╗
╚██╗ ██╔╝██╔═══██╗██╔══██╗██╔═══██╗██╔═══██╗
 ╚████╔╝ ██║   ██║██║  ██║██║   ██║██║   ██║
  ╚██╔╝  ██║   ██║██║  ██║██║   ██║██║▄▄ ██║
   ██║   ╚██████╔╝██████╔╝╚██████╔╝╚██████╔╝
   ╚═╝    ╚═════╝ ╚═════╝  ╚═════╝  ╚══▀▀═╝

   Your Own Don't-Drop Queue`)
	fmt.Println("📬 Welcome to YODOQ — where workers crash and acks vanish.")
	fmt.Println("💪 Let's see how reliable your queue really is...")
}

// =============================================================================
// Levels
// =============================================================================

// Level 1: basic correctness, no failures injected.
func level1() {
	section("Level 1: Basic YODOQ Operations")

	clock := &TestClock{}
	q := solution.New(clock)

	info("Testing enqueue + lease + ack...")
	id, err := q.Enqueue([]byte("hello"))
	if err != nil {
		errf(fmt.Sprintf("Enqueue failed: %v", err))
		return
	}
	successf(fmt.Sprintf("Enqueued job %s", id))

	job, err := q.Lease("worker-1", 5000)
	if err != nil || job == nil {
		errf(fmt.Sprintf("Lease failed: %v", err))
		return
	}
	if string(job.Payload) != "hello" {
		errf("Leased job payload mismatch")
		return
	}
	successf("Leased job with correct payload")

	if err := q.Ack("worker-1", job.ID); err != nil {
		errf(fmt.Sprintf("Ack failed: %v", err))
		return
	}
	successf("Acked job successfully")

	if j, _ := q.Lease("worker-1", 5000); j == nil {
		successf("Acked job is not re-leasable")
	} else {
		errf("Acked job should not be re-leasable")
	}

	info("Testing multiple jobs...")
	for i := 0; i < 10; i++ {
		_, _ = q.Enqueue([]byte(fmt.Sprintf("job-%d", i)))
	}
	leased := 0
	for {
		j, _ := q.Lease("worker-1", 5000)
		if j == nil {
			break
		}
		_ = q.Ack("worker-1", j.ID)
		leased++
	}
	if leased == 10 {
		successf(fmt.Sprintf("Leased and acked all %d jobs", leased))
	} else {
		errf(fmt.Sprintf("Only leased %d/10 jobs", leased))
	}

	info("Testing fail re-lease...")
	_, _ = q.Enqueue([]byte("to-fail"))
	j1, _ := q.Lease("worker-1", 5000)
	if j1 == nil {
		errf("Could not lease the job we just enqueued")
		return
	}
	_ = q.Fail("worker-1", j1.ID)
	j2, _ := q.Lease("worker-2", 5000)
	if j2 != nil && j2.ID == j1.ID {
		successf("Failed job was re-leased to a different worker")
	} else {
		errf("Failed job was not re-leased")
	}
	if j2 != nil {
		_ = q.Ack("worker-2", j2.ID)
	}

	info("Testing stale ack rejection (lease expiry)...")
	_, _ = q.Enqueue([]byte("stale"))
	s1, _ := q.Lease("worker-1", 1000)
	if s1 == nil {
		errf("Could not lease job for stale-ack test")
		return
	}
	clock.Advance(2000)
	s2, _ := q.Lease("worker-2", 5000)
	if s2 != nil && s2.ID == s1.ID {
		if err := q.Ack("worker-1", s1.ID); err != nil {
			successf("Stale ack from expired lease holder was rejected")
		} else {
			errf("Stale ack from expired lease holder was accepted (BAD)")
		}
		_ = q.Ack("worker-2", s2.ID)
	} else {
		warn("Lease did not transfer after expiry — Level 2 will surface this")
	}
}

// Level 2: 20% crash rate, no ack drops. Goal: zero lost jobs.
func level2() {
	section("Level 2: YODOQ Worker Crash Recovery")

	clock := &TestClock{}
	q := solution.New(clock)
	ledger := NewLedger()
	fails := &FailureSimulator{
		CrashRate:        0.20,
		AckDropRate:      0.0,
		ProcessingTimeMS: 100,
		Rng:              mrand.New(mrand.NewSource(42)),
	}

	const jobCount = 200
	info(fmt.Sprintf("Enqueuing %d jobs...", jobCount))
	jobIDs := make([]string, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		id, err := q.Enqueue([]byte(fmt.Sprintf("job-%d", i)))
		if err != nil {
			errf(fmt.Sprintf("Enqueue %d failed: %v", i, err))
			return
		}
		jobIDs = append(jobIDs, id)
	}

	workers := make([]*Worker, 5)
	for i := range workers {
		workers[i] = &Worker{
			ID: fmt.Sprintf("worker-%d", i), Queue: q, Clock: clock,
			Ledger: ledger, Failures: fails, LeaseMS: 1000,
		}
	}

	info(fmt.Sprintf("Running 5 workers with %.0f%% crash rate...", fails.CrashRate*100))
	runUntilDrained(q, clock, workers)

	lost := ledger.Lost(jobIDs)
	excess := ledger.Excess(jobIDs)
	total := ledger.Total()

	info("Results:")
	info(fmt.Sprintf("  - Total executions:    %d", total))
	info(fmt.Sprintf("  - Jobs lost (0 runs):  %d / %d", lost, jobCount))
	info(fmt.Sprintf("  - Excess executions:   %d", excess))
	dupRate := 0.0
	if jobCount > 0 {
		dupRate = (float64(total)/float64(jobCount) - 1) * 100
	}
	info(fmt.Sprintf("  - Effective dup rate:  %.1f%%", dupRate))

	if lost == 0 {
		successf("No jobs were lost — every job ran at least once!")
	} else {
		errf(fmt.Sprintf("%d jobs were lost forever (BAD).", lost))
	}
	if float64(excess) <= float64(jobCount)*fails.CrashRate*2 {
		successf("Duplication is bounded reasonably given crash rate.")
	} else {
		warn(fmt.Sprintf("High duplication: %d excess executions.", excess))
	}
}

// Level 3: sweep crash + ack-drop rates from 0.1% to 50%.
func level3() {
	section("Level 3: YODOQ Reliability Stress Sweep")

	rates := []float64{0.001, 0.01, 0.05, 0.10, 0.20, 0.30, 0.50}
	const jobCount = 200

	rateStr := strings.Builder{}
	for i, r := range rates {
		if i > 0 {
			rateStr.WriteString(", ")
		}
		rateStr.WriteString(fmt.Sprintf("%.1f%%", r*100))
	}
	info(fmt.Sprintf("Sweeping crash + ack-drop rates: %s", rateStr.String()))

	for _, rate := range rates {
		fmt.Println()
		info(fmt.Sprintf("Testing rate: crash=%.2f%%, ack-drop=%.2f%%", rate*100, rate*100))

		clock := &TestClock{}
		q := solution.New(clock)
		ledger := NewLedger()
		fails := &FailureSimulator{
			CrashRate:        rate,
			AckDropRate:      rate,
			ProcessingTimeMS: 100,
			Rng:              mrand.New(mrand.NewSource(int64(rate * 10000))),
		}

		jobIDs := make([]string, 0, jobCount)
		for i := 0; i < jobCount; i++ {
			id, err := q.Enqueue([]byte(fmt.Sprintf("job-%d", i)))
			if err != nil {
				continue
			}
			jobIDs = append(jobIDs, id)
		}
		if len(jobIDs) == 0 {
			warn("Enqueue is not implemented — skipping this rate.")
			continue
		}

		workers := make([]*Worker, 5)
		for i := range workers {
			workers[i] = &Worker{
				ID: fmt.Sprintf("worker-%d", i), Queue: q, Clock: clock,
				Ledger: ledger, Failures: fails, LeaseMS: 1000,
			}
		}

		runUntilDrained(q, clock, workers)

		lost := ledger.Lost(jobIDs)
		excess := ledger.Excess(jobIDs)
		total := ledger.Total()
		dup := 0.0
		if jobCount > 0 {
			dup = (float64(total)/float64(jobCount) - 1) * 100
		}

		info("  Results:")
		info(fmt.Sprintf("    - Jobs lost:        %d / %d (%.1f%%)", lost, len(jobIDs), float64(lost)/float64(len(jobIDs))*100))
		info(fmt.Sprintf("    - Excess execs:     %d", excess))
		info(fmt.Sprintf("    - Duplication:      %.1f%%", dup))
		info(fmt.Sprintf("    - Total executions: %d", total))

		if lost > 0 {
			errf(fmt.Sprintf("Jobs lost at %.1f%% — reliability compromised.", rate*100))
		}
	}
}

func main() {
	intro()
	section("Reliability Assessment Protocol")
	level1()
	level2()
	level3()
	section("YODOQ Assessment Complete")
}
