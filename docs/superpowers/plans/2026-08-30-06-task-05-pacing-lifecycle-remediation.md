# Plan 06 Task 5 Pacing Lifecycle Remediation Implementation Plan

> **For agentic workers:** Implement this remediation inside the existing Plan 06 Task 5 review boundary. Erabi uses implementation-first, verification-after sequencing for this repository. Do not intentionally create failing tests first. Do not use subagents. Do not commit or push Task 5 implementation until user heavy verification and acceptance.

**Goal:** Replace the process-lifetime sticky concurrency ceiling with active immutable configuration registrations, keep safety deadlines effective after registration release, and bound the process-wide pacing-origin registry without weakening Task 5 anti-bypass guarantees.

**Architecture:** `PacingService` continues to own one process-wide per-origin runtime registry. Each relevant execution context registers its immutable concurrency contribution and receives an RAII `PacingRegistration`; the effective same-origin concurrency ceiling is the minimum of currently active registrations only. Request-delay, Retry-After, backoff, permits, and waiters remain origin safety state and may outlive a registration until their own liveness/deadline conditions clear. Origin entries are retired only when the full safe-retirement predicate holds, and registry capacity exhaustion fails closed rather than evicting protected state.

**Tech Stack:** stable Rust, Tokio async synchronization/monotonic time, existing Task 5 `PacingService`, `OriginKey`, manual test clock, `BTreeMap`-based deterministic state, existing Erabi crawler error conventions.

**Spec:** `docs/superpowers/specs/2026-08-30-06-task-05-pacing-lifecycle-design.md`

**Parent Task:** `docs/superpowers/plans/2026-08-29-06-crawl4ai-and-quick-scrape-execution.md` Task 5

## Global Constraints

- Canonical `docs/specs/` remains authoritative on conflict.
- Task 5 remains one shared provider-neutral robots/pacing layer for later Quick Scrape and Production.
- Task 4 `NetworkTargetPolicy` and all accepted Task 1-4 semantics remain frozen.
- Same-origin pacing state remains process-wide so independent runs/services cannot create isolated buckets.
- Historical completed runs must not permanently constrain future unrelated runs.
- The effective concurrency ceiling is the minimum of currently active immutable registrations for one origin.
- Releasing a registration must not erase active permits, waiters, request-delay deadlines, Retry-After deadlines, backoff deadlines, or other established safety timing state.
- Origin retirement is legal only when there are no active registrations, active permits, waiting admissions, or unexpired safety deadlines.
- The process-wide origin registry is finite and named.
- Registry pressure may prune only states satisfying the complete safe-retirement predicate.
- If capacity remains full because entries are active/protected, fail with a typed capacity error.
- Never silently create a private per-run/per-service fallback bucket when process-wide capacity is exhausted.
- The Terra fixes already made during Task 5 review remain frozen: 401/403 synthetic deny cannot be overridden, waiters are registered before state inspection to avoid lost wakes, and cold same-origin robots cache misses are single-flight.
- Robots parsing, cache, Task 4 network-policy reuse, request/Crawl-delay composition, bounded Retry-After, and bounded backoff remain semantically unchanged unless a narrow lifecycle integration requires a non-semantic signature adjustment.
- No migration `0006`.
- No API route or Task 6 Quick Scrape orchestration.
- No Task 7 batch, Task 8 Production orchestration, Task 9 finalization, Plan 07, Plan 08, UI, or CI work.
- No public-network tests.
- Luna/Terra lightweight gate only: fmt, clippy, diff checks. User owns `cargo test`, `cargo check`, and `cargo build`.

---

## Existing Task 5 state that must be preserved

Before modifying code, read the current uncommitted Task 5 implementation in full, especially:

- `crates/erabi-crawler/src/pacing.rs`
- `crates/erabi-crawler/src/robots.rs`
- `crates/erabi-crawler/tests/robots_pacing.rs`

Preserve these reviewed properties:

```text
robots fetch
  -> Task 4 NetworkTargetPolicy
  -> pinned validated addresses
  -> no proxy
  -> no redirects
  -> bounded 512 KiB body
  -> 5 second timeout

robots decision
  -> exact/wildcard User-Agent semantics
  -> longest Allow/Disallow match
  -> Allow wins equal specificity
  -> bounded Crawl-delay
  -> parsed-policy cache
  -> 401/403 synthetic deny is not overrideable

pacing
  -> same-origin process-wide state
  -> different origins isolated
  -> request delay / Crawl-delay / backoff / Retry-After use latest deadline
  -> Retry-After max 5 minutes
  -> backoff max 60 seconds
  -> RAII AdmissionPermit
  -> cancellation/panic-safe release
  -> lost-wake remediation intact
```

Do not rewrite these subsystems merely to implement lifecycle ownership.

---

## Task 5R-A: Introduce active immutable pacing registrations

**Files:**

- Modify: `crates/erabi-crawler/src/pacing.rs`
- Modify: `crates/erabi-crawler/tests/robots_pacing.rs`
- Modify: `crates/erabi-crawler/src/lib.rs` only if a new public Task 5 lifecycle type must be exported

**Consumes:**

- existing `PacingService`;
- existing normalized `OriginKey`;
- existing immutable concurrency/request-delay values already extracted from `CrawlRunSnapshot` / `SnapshotOperationalSettings`;
- existing process-wide origin map;
- existing `AdmissionPermit` accounting and clock abstraction.

**Produces:**

The Task 5 API must expose equivalents of:

```rust
pub struct PacingRegistration { /* opaque RAII owner */ }

impl PacingService {
    pub fn register(
        &self,
        origin: OriginKey,
        concurrency: NonZeroUsize,
    ) -> Result<PacingRegistration, PacingError>;
}
```

If the current concurrency type is already a validated domain/newtype rather than `NonZeroUsize`, use that existing exact type instead. Do not introduce a second concurrency validator.

The registration must expose the safe admission path. Prefer one of these shapes according to the current code:

```rust
impl PacingRegistration {
    pub async fn acquire(
        &self,
        request_delay: Duration,
        robots_delay: Option<Duration>,
        cancellation: &CancellationToken,
    ) -> Result<AdmissionPermit, PacingError>;
}
```

or, if the current service already owns immutable delay inputs:

```rust
impl PacingRegistration {
    pub async fn acquire(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<AdmissionPermit, PacingError>;
}
```

Do not leave a public `PacingService::acquire(origin, concurrency, ...)` path that silently creates an unowned historical ceiling. If compatibility is temporarily required inside Task 5 tests, make the compatibility path private and route it through an explicit short-lived registration.

### Step 1: Replace the historical scalar ceiling with registration contributions

Change per-origin configuration state from a single sticky value equivalent to:

```rust
concurrency_ceiling: usize
```

into active contributions keyed by an internal runtime registration identity, conceptually:

```rust
struct RegistrationId(u64);

struct OriginPacingState {
    registrations: BTreeMap<RegistrationId, usize>,
    // existing active permit / waiter / deadline fields remain
}
```

Use the repository's existing validated concurrency type instead of raw `usize` where already available.

Registration identity requirements:

- one runtime ID per registration instance;
- duplicate concurrency values remain separate owners;
- dropping one registration removes only its own ID;
- no removal by value equality;
- no dependence on map iteration order;
- allocation overflow returns a typed runtime error instead of wrapping/reusing a live ID.

Keep registration IDs private to `erabi-crawler`; they are not durable IDs.

### Step 2: Compute effective concurrency from active registrations only

Add one pure helper on origin state equivalent to:

```rust
fn effective_concurrency(&self) -> Option<usize> {
    self.registrations.values().copied().min()
}
```

Use the existing validated concurrency type if applicable.

Required semantics:

```text
active registrations: [2, 5]
-> effective = 2

release registration 2
active registrations: [5]
-> effective = 5

release registration 5
active registrations: []
-> no historical configuration ceiling
```

The old lowest-ever scalar must be removed; do not leave it as a secondary clamp.

### Step 3: Make registration lifetime RAII

`PacingRegistration` must hold only what is required to remove its own contribution safely on `Drop`, such as:

```rust
struct PacingRegistration {
    origin: OriginKey,
    registration_id: RegistrationId,
    registry: Arc<OriginRegistry>,
    released: bool,
}
```

Exact fields may follow current ownership structure.

`Drop` must synchronously release only the registration contribution under a short lock. It must not perform async cleanup.

Manual release, if exposed internally for tests, must be idempotent and must not cause double-removal during later `Drop`.

Do not remove the whole `OriginState` merely because the registration is dropped.

### Step 4: Keep active permits independent from registration lifetime

A permit acquired through a registration remains accounted as an active origin request until its own `AdmissionPermit` drops.

Required sequence:

```text
registration A active
permit P acquired
registration A drops
permit P still active
origin state remains live
permit P drops
active permit count decrements exactly once
```

Dropping registration A must not decrement active request count.

### Step 5: Preserve safety deadlines after registration release

Do not reset or clear these existing fields when a registration is removed:

- next request/request-delay deadline;
- Retry-After deadline;
- bounded backoff deadline;
- any currently reviewed pacing deadline whose deletion could admit a request earlier.

Regression scenario:

```text
registration A concurrency 1
record Retry-After = 120s
release A after 10s
register B concurrency 8
B effective concurrency = 8
B still waits remaining Retry-After ~= 110s
```

Use manual monotonic test clock; do not sleep real seconds.

### Step 6: Preserve safe multi-run concurrent minimum

Regression scenario:

```text
register A concurrency 2
register B concurrency 5
same origin

only two simultaneous permits may be active

release A
B remains active

now B may reach five simultaneous permits
```

This test must prove both halves. A test that proves only `min(2,5)=2` is insufficient because that was already true in the blocked implementation.

### Step 7: Prove future unrelated run is not sticky-throttled

Add a deterministic regression equivalent to:

```text
register A concurrency 1
acquire/release work as needed
release A

register B concurrency 8

B can admit more than one simultaneous request up to its own current constraint
```

This is the primary regression for the Terra blocker.

---

## Task 5R-B: Track waiters explicitly for retirement safety

**Files:**

- Modify: `crates/erabi-crawler/src/pacing.rs`
- Modify: `crates/erabi-crawler/tests/robots_pacing.rs`

**Consumes:**

- existing lost-wake-safe Notify/cancellation implementation;
- existing per-origin state locks;
- Task 5R-A registration accounting.

**Produces:**

Explicit per-origin waiter liveness, conceptually:

```rust
waiting_admissions: usize
```

with RAII/idempotent accounting around every admission path that can remain pending.

### Step 1: Identify every pending admission phase

Trace the current `acquire` state machine and identify waits for:

- concurrency availability;
- request/Crawl-delay deadline;
- backoff/Retry-After deadline;
- cancellation-aware notification.

Use one logical waiter contribution for one pending acquire operation, not one increment per loop iteration.

### Step 2: Add waiter RAII accounting

Create an internal guard equivalent to:

```rust
struct WaiterGuard { /* origin + state reference */ }
```

The guard increments waiter count before the acquire operation can become pending and decrements exactly once on:

- successful admission;
- cancellation;
- early error;
- future drop/abort;
- panic unwind where Drop executes.

Preserve the Terra lost-wake fix: register/enable the Notify waiter before inspecting the state that decides whether to sleep.

### Step 3: Ensure cancellation does not leave phantom liveness

Add a deterministic regression:

```text
one admission is forced to wait
cancel/drop waiting future
waiting_admissions returns to zero
origin can later become safe-retirable
```

Do not expose waiter counters publicly only for testing. Use existing test-only inspection helpers or behaviorally prove retirement in Task 5R-C.

---

## Task 5R-C: Define the complete safe-retirement predicate

**Files:**

- Modify: `crates/erabi-crawler/src/pacing.rs`
- Modify: `crates/erabi-crawler/tests/robots_pacing.rs`

**Consumes:**

- active registrations from Task 5R-A;
- active permit accounting;
- waiting admissions from Task 5R-B;
- existing monotonic pacing deadlines.

**Produces:**

One internal source of truth equivalent to:

```rust
fn is_safe_to_retire(&self, now: Instant) -> bool
```

### Step 1: Encode every liveness condition in one helper

Return `true` only when all are satisfied:

```text
registrations empty
active permits == 0
waiting admissions == 0
request-delay deadline absent or expired
Retry-After deadline absent or expired
backoff deadline absent or expired
```

Include any other current Task 5 field whose deletion could admit requests earlier or break active accounting.

Do not scatter slightly different retirement checks across insertion, release, and capacity code.

### Step 2: Normalize expired timing state before retirement evaluation

When the existing code keeps expired deadlines as `Some(past_instant)`, retirement may treat them as expired. Prefer a small helper that clears/normalizes expired safety state under the origin lock before returning the retirement decision.

Do not clear unexpired state.

### Step 3: Verify protected safety state prevents retirement

Add manual-clock behavioral regressions for at least:

```text
no registration + no permit + unexpired Retry-After
-> NOT retire

advance past Retry-After
-> retire eligible
```

and:

```text
no registration + unexpired backoff
-> NOT retire

advance past backoff
-> retire eligible
```

Also cover request-delay deadline if it remains stored independently after permit release.

### Step 4: Verify active permit prevents retirement after registration release

Regression:

```text
register
acquire permit
release registration
attempt/prune registry
origin retained
release permit
now retirement may proceed when deadlines/waiters clear
```

---

## Task 5R-D: Bound the process-wide pacing-origin registry

**Files:**

- Modify: `crates/erabi-crawler/src/pacing.rs`
- Modify: `crates/erabi-crawler/tests/robots_pacing.rs`

**Consumes:**

- process-wide origin registry;
- normalized `OriginKey` ordering;
- `is_safe_to_retire(now)` from Task 5R-C;
- manual clock.

**Produces:**

A named finite registry capacity and typed capacity failure.

Use a named constant in `pacing.rs`, for example:

```rust
const MAX_ORIGIN_PACING_STATES: usize = 1_024;
```

Use `1_024` unless an existing canonical/configured crawler origin bound already exists and is clearly more appropriate. Do not expose this as a mutable user setting in Task 5.

Add/extend the existing Task 5 error enum with a bounded typed variant equivalent to:

```rust
PacingError::OriginCapacityExhausted
```

Do not include raw registry contents or URL queries in its Debug/Display output.

### Step 1: Add deterministic safe-pruning before new-origin insertion

When registering a configuration for an origin that is not already present:

1. get monotonic `now`;
2. if registry has room, insert normally;
3. if full, inspect origins in deterministic normalized `OriginKey` order;
4. remove only entries for which the full `is_safe_to_retire(now)` predicate is true;
5. stop pruning once capacity is available;
6. insert the new origin;
7. if no state can be safely pruned, return `OriginCapacityExhausted`.

Do not evict the oldest active entry merely because it is old.

Do not use randomized `HashMap` iteration.

### Step 2: Make capacity failure fail closed

When capacity is exhausted by protected origins, do NOT:

- create an isolated `OriginState` outside the process registry;
- bypass registration and call the current raw admission path;
- drop Retry-After/backoff state;
- silently use a higher concurrency ceiling;
- downgrade into robots unavailable/allowed semantics.

Return the typed pacing error to the caller.

### Step 3: Prove safe retired entries are pruned

Use test-only registry capacity injection if the current architecture already supports injectable process state for tests. If not, factor the registry implementation so tests can instantiate a small-capacity shared registry without mutating the production process-global singleton.

Preferred internal construction:

```rust
let registry = Arc::new(OriginRegistry::with_capacity(2));
let service = PacingService::with_registry_for_test(registry, clock);
```

Keep such constructor `#[cfg(test)]` or crate-private to avoid production callers intentionally constructing isolated bypass registries.

Regression:

```text
capacity = 2
origin A register/release -> safe retired
origin B active
register origin C
-> A pruned, B retained, C inserted
```

### Step 4: Prove protected entries are never evicted

Regression:

```text
capacity = 2
origin A has active registration
origin B has unexpired Retry-After
register origin C
-> typed OriginCapacityExhausted
-> A and B state still intact
```

Then release/expire one protected state and prove C can register after deterministic pruning.

### Step 5: Prove deterministic pruning order

For multiple safe-retirable entries, verify the same normalized `OriginKey` is chosen first regardless of insertion order.

Do not make functional safety depend on which safe entry is removed; this regression protects deterministic diagnostics/tests and future maintainability.

---

## Task 5R-E: Route the safe admission API through registrations

**Files:**

- Modify: `crates/erabi-crawler/src/pacing.rs`
- Modify: `crates/erabi-crawler/tests/robots_pacing.rs`
- Modify: `crates/erabi-crawler/src/lib.rs` if required for exports

**Consumes:**

- `PacingRegistration` from Task 5R-A;
- current immutable Task 5 settings extraction;
- current `AdmissionPermit` behavior;
- existing result/backoff/Retry-After recording methods.

**Produces:**

A public Task 5 boundary that future Task 6/8 callers can use without knowing internal registry state.

### Step 1: Make registration the owner of future admissions

Preferred conceptual usage:

```rust
let registration = pacing.register(origin, snapshot_concurrency)?;

let permit = registration.acquire(
    snapshot_request_delay,
    robots_crawl_delay,
    cancellation,
).await?;
```

If existing code has `PacingConfig` carrying immutable settings, use:

```rust
let registration = pacing.register(origin, pacing_config)?;
let permit = registration.acquire(cancellation).await?;
```

Do not make Task 5 depend on `CrawlRunId`; the caller merely holds the registration for its relevant execution lifetime.

### Step 2: Keep outcome timing updates origin-scoped

Existing APIs that record provider-neutral outcomes such as:

```text
RateLimited { retry_after_ms }
success
transient failure/backoff
```

must continue updating the shared origin safety state, not registration-local private state.

That is what allows Retry-After/backoff to survive registration release.

### Step 3: Prevent obvious bypass APIs

After migration, inspect public exports and visibility.

A future caller should not have a public method that can perform same-origin admission without either:

- an active `PacingRegistration`; or
- another explicitly safe process-wide registration primitive.

Internal helpers may remain private.

Do not remove testability or robots APIs unnecessarily.

---

## Task 5R-F: Preserve Terra Task 5 remediations

**Files:**

- Verify/modify only if necessary: `crates/erabi-crawler/src/robots.rs`
- Verify/modify: `crates/erabi-crawler/src/pacing.rs`
- Verify/modify: `crates/erabi-crawler/tests/robots_pacing.rs`

**Consumes:** existing uncommitted Terra remediation.

**Produces:** unchanged accepted behavior.

### Step 1: Preserve access-denied override restriction

Keep the regression equivalent to:

```text
401/403 synthetic deny-all
+ valid frozen robots override
-> still denied
```

Only ordinary parsed robots `Disallow` may be overridden according to existing Task 5 semantics.

### Step 2: Preserve lost-wake ordering

Every Notify-based wait changed by Terra must still:

1. create/register the waiter;
2. enable/pin it as required by Tokio `Notify` semantics;
3. only then inspect the shared state that decides whether to wait.

Do not regress to check-then-register.

### Step 3: Preserve robots single-flight

Same-origin cold robots cache misses on cloned services must remain coalesced.

Do not combine pacing-origin retirement with the robots cache/single-flight map; they have different ownership and expiry semantics.

---

## Task 5R-G: Deterministic lifecycle verification source

**Files:**

- Modify: `crates/erabi-crawler/tests/robots_pacing.rs`
- Add focused unit tests in `crates/erabi-crawler/src/pacing.rs` only if testing private retirement helpers there is materially clearer

Do not intentionally run tests during implementation; user owns heavy verification.

The final test source must cover all of these cases:

### Registration lifecycle

```text
1. one registration defines concurrency
2. concurrent registrations use minimum active ceiling
3. restrictive registration release widens to remaining registration
4. completed historical registration does not sticky-throttle future run
5. duplicate values have independent registration ownership
6. double/manual release cannot remove another registration
7. registration drop with active permit does not corrupt permit accounting
```

### Safety state lifetime

```text
8. Retry-After survives registration release
9. backoff survives registration release
10. request-delay reservation survives registration release when still unexpired
11. expired safety state becomes retirement-eligible
12. new registration after old completion receives its own concurrency while old safety deadline still gates timing
```

### Waiter/permit liveness

```text
13. active permit prevents retirement
14. waiting admission prevents retirement
15. cancelled waiter releases waiter liveness
16. cancellation does not leak active permit
17. existing lost-wake regression remains
```

### Registry capacity

```text
18. safe-retirable origin is pruned when capacity is needed
19. active registration is never evicted
20. active permit is never evicted
21. waiter is never evicted
22. unexpired Retry-After/backoff/request-delay state is never evicted
23. all-protected capacity returns typed OriginCapacityExhausted
24. no isolated fallback bucket is created after capacity error
25. deterministic pruning does not depend on insertion order
26. registry size never exceeds configured/test capacity
```

### Anti-bypass integration

```text
27. two independently constructed production PacingService values still share one process origin state
28. different origins remain isolated
29. same-origin independent run-style registrations cannot sum concurrency limits
30. release of one run-style registration does not release another
31. Retry-After recorded by one registration constrains another same-origin registration
```

Use unique test origins or isolated test registries so parallel `cargo test` execution cannot interfere through process-global state.

---

## Task 5R-H: Lightweight verification and final self-audit

**Files:** all Task 5 files only.

### Step 1: Run formatting

Run:

```text
cargo fmt --all
cargo fmt --all --check
```

Fix all ordinary formatting findings.

### Step 2: Run relevant Clippy

Run:

```text
cargo clippy -p erabi-crawler --all-targets -- -D warnings
```

Fix all ordinary Clippy findings without weakening semantics.

### Step 3: Run whitespace/diff gate

Run:

```text
git diff --check
git status --short
git diff --stat
```

Remember `git diff --stat` excludes untracked Task 5 source files until staged. Explicitly list them in the return report.

### Step 4: Final architectural self-audit

Confirm all of the following before returning:

```text
- no lowest-ever scalar concurrency ceiling remains
- active registration minimum is the only configuration-derived ceiling
- registration release removes only its own contribution
- permit lifetime remains independent
- waiter lifetime is explicit and cancellation-safe
- Retry-After/backoff/request-delay safety state survives registration release
- full safe-retirement predicate is centralized
- process origin registry has finite named capacity
- only safe-retirable states are pruned
- capacity exhaustion is typed and fail-closed
- no private isolated fallback bucket exists
- default PacingService instances still share process-wide state
- different origins remain isolated
- no Task 4 network safety change
- Terra 401/403 override remediation preserved
- Terra lost-wake remediation preserved
- Terra robots single-flight remediation preserved
- no migration 0006
- no Task 6+ implementation
```

### Step 5: Do not run user-owned heavy commands

Do NOT run:

```text
cargo test
cargo check
cargo build
cargo metadata
```

The user will run:

```text
cargo test -p erabi-crawler
```

---

## Scope boundary after remediation

After this remediation, Task 5 should provide a future-safe runtime contract approximately equivalent to:

```text
immutable execution context
    |
    +-> normalize origin
    |
    +-> register immutable pacing contribution
            |
            +-> PacingRegistration (RAII)
                    |
                    +-> robots decision already resolved
                    |
                    +-> acquire AdmissionPermit
                    |       |
                    |       +-> active permit accounting
                    |       +-> max deadline composition
                    |       +-> cancellation-safe waiting
                    |
                    +-> record provider outcome
                            |
                            +-> shared Retry-After/backoff state

registration ends
    -> configuration contribution released
    -> safety deadlines NOT erased

origin registry entry
    -> retained while config/permit/waiter/deadline is live
    -> safely pruned only after all protection clears
```

Task 6 will later decide exactly how long an execution context holds `PacingRegistration`. Task 5 must not implement that run/job lifecycle now.

---

## Acceptance criteria

The Task 5 blocker is resolved only when:

1. completed historical runs cannot permanently lower an origin's future concurrency;
2. simultaneously active immutable configurations use the minimum active ceiling;
3. releasing the restrictive active registration allows the remaining active ceiling to widen safely;
4. request-delay, Retry-After, and backoff protections survive registration release until expiry;
5. active permits and waiters keep origin state alive;
6. cancelled waiters/permits do not leak liveness;
7. the process-wide origin registry is finite;
8. only fully safe-retirable entries may be pruned;
9. full protected capacity returns a typed fail-closed error;
10. independent Task 5 service instances still share same-origin state;
11. robots/network/override/lost-wake/single-flight semantics reviewed by Terra remain intact;
12. no Task 6+ behavior, persistence, migration, UI, or CI scope is introduced;
13. Luna/Terra lightweight gates pass;
14. user `cargo test -p erabi-crawler` passes after remediation.

## Commit boundary

Do not commit during remediation or review.

After user heavy verification and Task 5 acceptance, create exactly one Task 5 implementation commit containing the original Task 5 implementation plus accepted Terra/remediation changes.

Suggested final Task 5 commit subject remains:

```text
feat(crawler): enforce robots and crawl pacing
```

The design-spec and remediation-plan documentation commits remain separate and must not be amended or squashed.
