# YODOQ

Welcome to **Y**our **O**wn **D**on't-**D**rop **Q**ueue.

A small Go exercise: build a reliable job queue that doesn't lose work even when workers crash and acks vanish in transit. It doesn't compete with Sidekiq or Temporal — it's your own.

## Getting started

You will implement a single-process, in-memory job queue with the following guarantees:

1. **Basic operations**: enqueue, lease, ack, fail
2. **Crash tolerance**: when a worker leases a job and dies, the job is eventually re-leased to someone else
3. **Bounded duplication**: under increasing crash + dropped-ack rates, the queue never *loses* work and keeps duplicate executions reasonably bounded

YODOQ ships with a test harness that runs three levels of escalating failure injection and grades how reliable your queue actually is.

## Architecture

- **`solution.JobQueue`** (`solution/queue.go`): your implementation lives here
- **`solution.Clock`**: abstraction over `time.Now()` so the harness can fast-forward
- **`Worker`** (`main.go`): virtual job consumer that may crash mid-process or have its acks dropped
- **`FailureSimulator`** (`main.go`): parameterizes crash and ack-drop rates
- **`ExecutionLedger`** (`main.go`): out-of-band ground truth of how many times each job *actually* ran — the harness's source of truth for grading
- **`TestHarness`** (`main.go`): three levels of tests

## Implementation Levels

### Level 1: Basic queue operations

- Implement `Enqueue`, `Lease`, `Ack`, `Fail`, `Stats`
- Acked jobs must not be re-leased
- Failed jobs must be re-leased
- Acks from a worker who no longer holds the lease (e.g. lease expired) must be rejected

### Level 2: Crash recovery

- When a worker leases a job and never acks (because it "crashed"), the queue must re-lease it after the lease expires
- Run with 20% crash rate — every job must run *at least once*
- No jobs lost forever

### Level 3: Reliability under stress

- Sweep crash + dropped-ack rates from 0.1% to 50%
- Goal: zero lost jobs, with duplicate-execution count bounded by the failure rate (not unbounded)

## Prerequisites

- Go 1.22+

## Running the tests

```bash
go run .
```

## Test results — what to look at

- **Jobs lost**: count of jobs whose processing function never completed. **This must be 0**. A queue that loses work is not a queue.
- **Excess executions**: count of duplicate runs (a job that ran 3 times contributes 2 excess). Should grow roughly linearly with failure rate, not exponentially.
- **Duplication rate**: total executions / job count − 1. A reasonable solution stays under ~2× the input failure rate.
- **Effective throughput**: how quickly the queue drains under failure.

## Success criteria

- Pass all Level 1 correctness tests
- Zero jobs lost at Level 2 (20% crash rate)
- Zero jobs lost across all Level 3 failure rates (up to 50%)
- Duplication remains bounded — pathological solutions can run a single job hundreds of times

## Advanced challenges

Once you have a working solution, consider:

- **Exactly-once-ish**: introduce a dedup window so a single job can never run more than `K` times even under arbitrary failures. What's the trade-off in memory?
- **Priorities**: support priority levels without starvation
- **Fairness**: ensure no worker starves and no single job hogs retries
- **Concurrency**: make the queue safe for many parallel callers
- **Persistence**: how would you survive a queue-process restart? What's the minimum on-disk footprint?
- **Observability**: surface the metrics that would actually help an operator debug a stuck queue
