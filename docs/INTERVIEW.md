# TraceLens interview walkthrough

TraceLens is an independent implementation inspired by Google's documented distributed-tracing and sampling challenges. Use the wording “inspired by,” never imply that it is a Google product or was deployed at Google.

## Six-minute walkthrough

### 1. Problem and source — 45 seconds

Google's Dapper paper explains why one request becomes difficult to understand when it crosses many services and machines. Google Cloud Trace documentation distinguishes head sampling, which commits before the outcome is known, from tail sampling, which can inspect a complete trace but requires buffering and trace-ID-aware routing.

I built a local collector around that tradeoff: retain every error and slow trace, retain a deterministic fraction of normal traces, and still keep aggregate latency summaries for traces whose detailed spans are removed.

### 2. Data path — 60 seconds

1. The SDK creates a 128-bit trace ID and 64-bit span IDs, propagates a canonical `traceparent`, and measures duration with a monotonic clock.
2. The HTTP or direct-store intake validates a strict span schema. A canonical payload hash makes redelivery idempotent and detects a changed payload under the same span ID.
3. SQLite stores incomplete traces. Batches run under `BEGIN IMMEDIATE`, so a conflict rolls back every insertion in the batch.
4. The caller finalizes only after it expects the trace to be complete. The analyzer rejects multiple roots, missing parents, cycles, impossible containment, and excessive clock skew.
5. The sampler keeps errors first, then slow traces, then a SHA-256-selected fraction of ordinary traces. A dropped trace loses its span rows but keeps its summary.

### 3. Interesting algorithm — 75 seconds

Naively summing span durations double-counts concurrent work. TraceLens instead collects every clipped span boundary along the root request. For each adjacent time segment, it finds the deepest active span and assigns that wall-time segment to its service. Adjacent segments attributed to the same span are merged into a critical timeline.

The algorithm is deterministic and ensures the total attributed latency equals covered root wall time. Separately, self-time subtracts the union of direct-child intervals from each parent; interval union avoids double-subtracting overlapping children. The largest service attribution becomes the diagnostic bottleneck candidate. It is intentionally described as a heuristic because trace timing alone does not prove causality.

### 4. Correctness and failure handling — 60 seconds

- Duplicate delivery: identical payload is a no-op; changed content under the same ID is a conflict.
- Partial batch: any invalid state or conflict rolls back the whole transaction.
- Out-of-order spans: accepted during collection; graph consistency is enforced at finalization.
- Retry stability: SHA-256 over trace ID gives the same ordinary-sampling decision after a restart.
- Late span: rejected after finalization rather than silently corrupting a completed trace.
- Bad input/resource abuse: strict fields plus body, batch, trace, span, attribute, and worker bounds.
- Instrumentation failure: exporter failure is visible during normal execution but never masks an application exception already in flight.

### 5. Measured demonstration — 60 seconds

The deterministic workload generated 800 traces and 4,800 spans across six services. Sixty traces were anomalous: 50 slow and 20 failed, with 10 overlapping. The tail policy retained all 60 anomalies; a stable 10% head baseline selected 8. The analyzer attributed `inventory` as the bottleneck in all 50 deliberately slow traces. After finalization, 816 of 4,800 span rows remained, an 83.0% reduction, while p50/p95/p99 summaries remained available. Sixty-eight automated tests passed.

These results establish behavior on controlled synthetic data; they are not production throughput claims.

### 6. Tradeoff and next step — 60 seconds

The main tradeoff is that tail sampling saves long-term detail storage but not ingress or temporary-buffer cost. Explicit finalization is deterministic for a lab but unrealistic for a fleet. My next iteration would add per-trace idle deadlines and late-span metrics, consistent-hash routing to collector shards, an OTLP receiver, bounded on-disk queues, and a mergeable percentile sketch. I would retain the immutable span-ID conflict rule and deterministic policy version in every decision summary.

## Code tour

| File | Point to discuss |
| --- | --- |
| `tracelens/model.py` | Strict schema, bounded attributes, canonical IDs, `traceparent` parse/format. |
| `tracelens/store.py` | Idempotency hashes, transaction boundary, stable sampling, summary/full-detail split. |
| `tracelens/analysis.py` | Graph invariants, interval union, boundary sweep, dependency edges. |
| `tracelens/sdk.py` | `contextvars` nesting, monotonic timing, exception-safe exporter behavior. |
| `tracelens/httpserver.py` | Loopback default, bearer gate, constant-time compare, request/worker bounds, safe errors. |
| `tracelens/demo.py` | Seeded disorder, injected latency/failures/duplicates/conflict, head-vs-tail oracle. |
| `tests/` | Model, graph, storage, policy, aggregate, SDK, HTTP, and CLI coverage. |

## Likely questions

### Why not sample at the SDK?

That is cheaper because unsampled spans never travel or buffer, but the decision cannot know whether the request will later fail or become slow. A practical design can combine light head sampling for known-low-value traffic with tail rules for flows where anomaly retention matters.

### How do you know a trace is complete?

This prototype does not infer completeness; the caller explicitly finalizes. At fleet scale I would use an inactivity deadline plus a maximum lifetime, record incomplete decisions, allow a bounded late-span window, and route all spans with the same trace ID to one state owner.

### Does the bottleneck algorithm find the root cause?

No. It finds the service occupying the largest portion of the reconstructed critical timeline. Lock contention, downstream queues, CPU saturation, missing spans, clock errors, or asynchronous work can change causality. I expose it as a diagnosis candidate and preserve the timeline for inspection.

### What happens with concurrent children?

Self-time subtracts their interval union, not their sum. Critical attribution divides root time into boundary segments and assigns each segment once, preventing concurrency from inflating wall-clock latency.

### Why SQLite?

It supplies durable local transactions, crash recovery, constraints, and WAL concurrency without infrastructure or credentials. It also makes the lab reproducible. It is not a claim that one SQLite node is a suitable global tracing backend.

### How would this scale?

Partition on trace ID with consistent hashing; replicate ownership; batch/compress intake; set per-tenant quotas; spill bounded trace state to durable object storage; version sampling policies; emit backpressure and dropped-span metrics; aggregate percentiles with sketches; and expose OTLP plus standard semantic conventions.

### What security work remains?

TLS and mutual authentication, tenant-scoped authorization, secure secret rotation, encryption at rest, audit logs, rate limits per identity, dependency scanning, and hardened deployment isolation. The current token gate is intentionally small and documented as application-level protection.

## Failure scenarios to whiteboard

1. **Collector crashes before finalization:** SQLite retains active spans. On restart, an idle-timeout worker would finalize or expire them; current users can query/purge active state explicitly.
2. **Two collectors receive one trace:** neither sees a complete tree. Route on trace ID or introduce a durable shuffle before tail processors.
3. **A producer reuses a span ID:** canonical hashes differ, so intake returns conflict and rolls back the batch.
4. **A span arrives after the decision:** current state rejects it. A production policy could reopen within a versioned grace window, but that complicates deterministic retention and queries.
5. **Clock skew moves a child outside its parent:** up to 5 ms is clipped; beyond that finalization fails so invalid timing is not presented as trustworthy analysis.
6. **Traffic exceeds buffer capacity:** bounded active-trace/span limits fail closed. A production system should signal backpressure and expose saturation SLOs rather than rely on client timeouts.

## Evidence-based resume bullets

- Built an independent Python distributed-tracing collector inspired by Google Dapper/Cloud Trace, with W3C-style context propagation, transactional idempotent span intake, graph validation, deterministic tail sampling, a bounded authenticated HTTP API, Docker, and CI; passed 68 automated unit/integration tests.
- Designed a reproducible 800-trace/4,800-span fault lab where latency/error-aware sampling retained 60/60 injected anomalous traces versus 8/60 for a deterministic 10% head baseline, correctly attributed all 50 injected slow paths to `inventory`, and reduced retained span rows by 83.0% after decisions.

## Accuracy guardrails

- Say “independent local implementation inspired by Google's published tracing work.”
- Say “synthetic result” and state the workload size with every metric.
- Do not call the custom schema OTLP, the SDK OpenTelemetry, or the heuristic a root-cause proof.
- Do not claim production deployment, external users, Google affiliation, real cost savings, or production-scale throughput.
