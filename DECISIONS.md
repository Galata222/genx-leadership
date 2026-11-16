# Architecture & Debugging Decisions

## 1. Root-Cause Diagnosis
What specific bottlenecks and failure modes did you observe in the original codebase under concurrent load?

- **Event-loop / CPU behavior:** `scrub_and_fingerprint` runs 120,000 SHA-256 rounds in Python inside the async ingest handler. That holds the GIL for roughly 40–60ms per call, so a burst of requests freezes the loop and `GET /health` misses the 50ms budget. The handler also waits for the downstream POST (about 250ms, sometimes a 503) before it responds, so acknowledgement latency is CPU time plus downstream time and the p95 goes over 250ms. `raise_for_status()` turns that 503 into a failed request, and the event never reaches `delivered`.
- **Concurrency & race conditions:** Nothing is unique on `event_id` or `X-Idempotency-Key`. Two deliveries that arrive together both insert a row and both call downstream. The header is only checked for presence.
- **Resource & connection management:** Every forward opens a new `httpx.AsyncClient`, and every statement opens a new SQLite connection. The default rollback journal with no busy timeout makes concurrent writers fail with "database is locked" instead of waiting. There is no retry around the transient 503.

## 2. Concurrency & Offloading Strategy
How did you separate CPU-bound preprocessing, HTTP acknowledgment, and downstream delivery? Why did you choose your specific concurrency primitives over alternatives (e.g., `asyncio.to_thread` vs. `ProcessPoolExecutor` vs. FastAPI `BackgroundTasks`)?

The request path only checks the idempotency key, inserts one row, and returns **202**. Scrubbing and delivery happen after the response.

CPU work goes to a `ProcessPoolExecutor` created and warmed during startup (pool size `min(4, cpu_count - 1)`). Downstream delivery is a loop on the event loop, using one shared `httpx.AsyncClient`, with sequential retries per event.

`asyncio.to_thread` does not help. The hash loop is Python bytecode, so a worker thread still holds the GIL and `/health` still stalls. `BackgroundTasks` only moves the work to after the response; it still runs on the same loop, so the CPU problem remains. A child process has its own interpreter, which is what keeps the ingest loop responsive while the fingerprint is computed. The pool is warmed at startup because the first `submit` on Windows otherwise blocks the loop by spawning processes in the middle of the burst. Workers run at below-normal priority so fingerprinting loses the CPU to acknowledgement and `/health` when the machine is busy.

A probe in a separate process during the burst saw `/health` under 20ms. The harness client on this Windows box still records about 120ms for its first probe, then about 5ms for the next one on the same connection. An empty FastAPI app shows that same first-probe spike, so it is the client opening 60 connections at once, not the ingest loop.

## 3. Idempotency & SQLite Concurrency
How does your implementation guarantee zero duplicate processing when two identical webhook retries arrive at the exact same millisecond? What SQLite settings or constraints did you rely on?

The insert is the claim. `event_id` and `idempotency_key` are both `UNIQUE`. Ingest requests that arrive on the same loop turn share one commit, and each row is inserted under its own savepoint. A duplicate in that batch, or one that arrives after the commit, hits `IntegrityError` and does not enqueue work, so it cannot create a second row or a second downstream call. SQLite's single writer still serializes this against the delivery loop.

The connection is opened once, with `journal_mode=WAL`, `synchronous=NORMAL`, and `busy_timeout=5000`. WAL lets the delivery loop read while ingest commits. `NORMAL` keeps a process crash from dropping an already committed ack without fsync'ing every insert. An `asyncio.Lock` around that connection stops coroutines from interleaving statements. The delivery claim is `BEGIN IMMEDIATE` plus `UPDATE ... WHERE status = 'pending'`, and the row is taken only when `changes()` is 1. Retries for one event are sequential inside that claim, so a 503 retry cannot race a duplicate POST.

## 4. Failure Modes & Production Trade-offs
- What happens in your design if the downstream service is completely unreachable for 60 seconds?
  The 202 has already been sent and the row is in SQLite. Each attempt retries transport errors and 408/429/5xx a few times, then the row goes back to `pending` with exponential backoff capped at 2 seconds. It is not marked `delivered` and the payload is not dropped. A 4xx is permanent and the row is marked `failed`. Retryable failures keep going for 40 attempts, which still covers a 60 second outage. When the downstream accepts traffic again, a later attempt delivers the event and then marks it `delivered`.
- What happens if this service process crashes 50ms after acknowledging a webhook?
  The insert is committed before the 202, so the event survives the crash. The raw body is stored only until scrubbing finishes; after that the redacted text and fingerprint are enough to retry. On the next start, rows left in `processing` are put back to `pending` and the delivery loop finishes them. If nobody restarts the process, the caller has a 202 and the row sits un-delivered. Delivery is exactly-once while the process stays up. A crash in the window after the downstream has accepted the POST and before `mark_delivered` commits can send that one event again on restart, because the mock consumer does not dedupe.
- What would break first if we deployed this container with `--workers 4` instead of 1?
  SQLite still allows one writer, so the four processes would queue on the WAL lock and ingest p95 would go first. Each worker also reclaims every `processing` row at startup and runs its own pool, so one worker's startup can steal another worker's in-flight claim and deliver the same event twice. The `UNIQUE` constraints would still prevent duplicate rows. A worker that dies while the other three stay up leaves its claimed rows stuck, because nothing in the surviving processes reclaims them.
