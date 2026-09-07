# Plan 06 Task 5 Pacing Lifecycle Design

**Status:** Approved architectural remediation for Plan 06 Task 5  
**Date:** 2026-08-30  
**Applies to:** Task 5 shared per-origin pacing state used later by Quick Scrape, bounded batch, and Production  
**Parent plan:** `docs/superpowers/plans/2026-08-29-06-crawl4ai-and-quick-scrape-execution.md`

## 1. Decision summary

Task 5 will not keep the lowest concurrency value ever observed for an origin for the lifetime of the Erabi process.

Instead, per-origin concurrency is constrained by **active immutable configuration registrations**. Each relevant execution context registers the concurrency ceiling from its already-frozen run configuration and keeps that registration alive only while that execution context remains relevant. The effective concurrency ceiling for an origin is the minimum of the currently active registrations for that origin.

Configuration lifetime and safety-timing lifetime are intentionally different:

- active concurrency registrations disappear when their owner is released;
- request-delay, Retry-After, and backoff deadlines remain effective until their monotonic deadlines expire, even if the run that created them has already finished;
- origin state may be retired only when no active configuration, request, waiter, or unexpired safety deadline remains.

The process-wide origin registry is bounded. When capacity is reached, Erabi first deterministically prunes states that are safe to retire. If capacity is still exhausted because all remaining entries are genuinely active or protected, new registration/admission fails with a typed capacity error. Erabi must never evict active or safety-protected origin state merely to make space.

This decision preserves immutable per-run configuration while preventing old completed runs from permanently throttling unrelated future runs.

## 2. Why this design exists

The initial Task 5 implementation used process-wide origin state so that independent runs cannot bypass same-origin pacing. That property is required for Task 6 Quick Scrape, Task 7 bounded batch, and Task 8 Production.

However, the first implementation also stored a single `concurrency_ceiling` equal to the minimum value ever observed for that origin. Because the process-wide state had no configuration ownership/release lifecycle, this sequence was possible:

```text
Run A: example.com, concurrency = 1
Run A completes
Run B: example.com, concurrency = 8

process-wide effective concurrency remains 1
```

The behavior changes only when the Erabi process restarts, which makes a future run's execution semantics depend on unrelated historical process state rather than its immutable snapshot and currently relevant competing work.

The same process-wide registry also had no retirement boundary, so arbitrary distinct origins could accumulate state for the full process lifetime.

Both issues require one coherent lifecycle model rather than independent patches.

## 3. Alternatives considered

### 3.1 Active configuration registration — selected

Each relevant execution context registers its immutable per-origin pacing configuration and receives an RAII registration/scope.

For one origin:

```text
Run A registration: concurrency 2
Run B registration: concurrency 5

active effective ceiling = min(2, 5) = 2

Run A registration released
active effective ceiling = 5
```

This matches immutable-run semantics: each run's own value remains unchanged, and only currently relevant configurations constrain one another.

### 3.2 Permanent lowest-ever ceiling — rejected

Keeping the minimum ever seen for the entire process is conservative but makes later run settings misleading and process-restart-dependent.

A completed run with concurrency `1` must not silently force all future unrelated runs for that origin to remain at `1` until restart.

### 3.3 Idle-state retirement without configuration ownership — rejected

Retiring origin state merely because no request is currently active cannot determine whether a run is actually finished.

A live run can temporarily be idle between requests. Removing its constraint during that gap would permit another run to widen concurrency while the restrictive run is still relevant.

Therefore traffic idleness is not a substitute for explicit configuration ownership.

## 4. Ownership and dependency direction

### `erabi-crawler`

Owns the provider-neutral runtime pacing lifecycle.

It may define equivalents of:

```text
PacingService
PacingRegistration / PacingScope
AdmissionPermit
OriginKey
OriginState
PacingError::CapacityExhausted
```

Exact Rust names may follow repository conventions, but the semantics in this document are authoritative.

### Task 6 / Task 8 orchestration

Later orchestration owns the lifetime of a pacing registration because it knows when an execution context is still relevant.

Task 5 must not depend on:

- `CrawlRunId`;
- database run lifecycle;
- Quick Scrape-specific types;
- Production-specific types;
- job repository state.

Task 5 exposes a generic RAII registration boundary that later tasks can hold for the appropriate execution lifetime.

### `erabi-domain`

Keeps existing immutable snapshot/configuration values. Task 5 consumes resolved values; it does not mutate them.

### `erabi-db`

No new Task 5 persistence is introduced.

## 5. Configuration registration model

The shared pacing service must support explicit registration of an origin plus immutable pacing configuration.

Conceptually:

```text
PacingService::register(origin, immutable_settings)
    -> PacingRegistration
```

The registration owns one active configuration contribution for that origin.

At minimum the contribution includes the immutable concurrency ceiling required by Task 5 admission semantics. If existing Task 5 code structurally ties other immutable per-run inputs to the registration, they may remain there only when doing so preserves the already-approved Task 5 behavior.

The registration is runtime-only. It is not persisted.

## 6. Effective concurrency semantics

For one origin, if active registrations have concurrency values:

```text
2, 5, 8
```

the effective concurrency ceiling is:

```text
min(2, 5, 8) = 2
```

This minimum applies only while those registrations remain active.

When the registration with `2` is released, the effective ceiling becomes:

```text
min(5, 8) = 5
```

When all registrations are released, there is no active configuration-derived concurrency ceiling for future unrelated work.

The implementation must not preserve a historical minimum after its owning registration has been released.

## 7. Registration identity and deterministic ownership

Each registration must have an internal unique runtime identity sufficient to add and remove exactly one configuration contribution.

Requirements:

- dropping one registration removes only its own contribution;
- duplicate configuration values from different owners remain distinct registrations;
- removal must not depend on value equality alone;
- removal must not depend on map iteration order;
- repeated/drop-like cleanup must not underflow counts or remove another owner's registration;
- registration IDs are runtime implementation details and are not durable product identity.

A monotonically allocated bounded integer or other deterministic runtime token is acceptable if overflow is handled safely.

## 8. RAII registration lifecycle

`PacingRegistration` / `PacingScope` should release its configuration contribution on `Drop` where practical.

This protects against:

- normal completion;
- early return;
- cancellation where the owner is dropped;
- panic unwinding where Rust `Drop` runs.

If cleanup requires async work, the design must not rely on asynchronous `Drop`. Prefer state structures where releasing a registration contribution can be done synchronously under a short lock, with any deferred pruning performed safely later.

Task 5 must not require callers to remember a fragile manual decrement on every error branch.

## 9. Admission permits remain separate from configuration registrations

A configuration registration and a request admission permit represent different lifetimes.

```text
PacingRegistration
    -> execution context remains relevant

AdmissionPermit
    -> one admitted request is currently consuming origin concurrency
```

One registration may acquire many admission permits over its lifetime.

Dropping one `AdmissionPermit` releases only the active request slot. It must not release the run/configuration registration.

Dropping a registration while one of its previously acquired permits still exists must not invalidate permit accounting or allow active requests to exceed safe concurrency unexpectedly. The origin state must remain alive while active permits exist.

## 10. Safety timing state survives registration release

Historical configuration must not be sticky, but active safety deadlines must remain sticky until they expire.

The following state may outlive the registration/run that created it:

- request-delay deadline already reserved by an admitted request;
- Retry-After deadline;
- bounded backoff deadline;
- any other already-approved Task 5 per-origin monotonic safety deadline.

Example:

```text
Run A concurrency = 1
server yields Retry-After = 120s
Run A finishes after 10s

Run B registers concurrency = 8

configuration-derived effective concurrency = 8
Retry-After still blocks admission for the remaining 110s
```

Releasing a run registration must never erase an unexpired Retry-After or backoff deadline.

## 11. Crawl-delay semantics

The already-approved Task 5 robots Crawl-delay behavior remains unchanged.

Task 5 should continue combining applicable request delay, robots Crawl-delay, backoff, and Retry-After through the previously reviewed deadline semantics.

This design does not make robots policy mutable and does not permit a registration release to bypass an already-reserved same-origin admission deadline.

## 12. Shared process-wide origin state remains required

Independent Task 5 service instances must continue to share same-origin runtime state in the process.

This remains a core anti-bypass requirement:

```text
Quick Scrape A
Quick Scrape B
Production C

same origin
-> same process-wide origin pacing state
```

The fix must not solve sticky configuration by returning to per-run or per-service isolated limiter state.

Different origins remain isolated.

## 13. Safe origin retirement predicate

An origin state is safe to retire only when all relevant liveness/protection conditions are clear.

At minimum all of the following must hold:

```text
active configuration registrations == 0
active admission permits            == 0
waiting admissions                  == 0
```

and there is no unexpired safety timing state such as:

```text
request-delay deadline
Retry-After deadline
backoff deadline
```

If the actual Task 5 state contains another field whose removal could let a caller bypass an already-established safety constraint, that field must also participate in the retirement predicate.

The implementation must derive retirement from explicit state, not approximate inactivity.

## 14. Waiting-admission ownership

Waiting admissions are part of origin-state liveness.

An origin must not be retired while callers are waiting for:

- concurrency availability;
- a pacing deadline;
- cancellation-aware wakeup;
- another Task 5 admission condition.

Cancellation of a waiter must release its waiter accounting reliably.

The lost-wake remediation already performed during Terra review remains required and must not regress.

## 15. Process-wide registry bound

The process-wide origin registry must have an explicit named capacity bound.

The exact constant should be chosen in implementation according to current Task 5 scale assumptions and existing configuration conventions, but it must be:

- finite;
- named;
- tested;
- large enough for expected MVP local crawling;
- not silently dependent on a `HashMap` implementation detail.

No migration or durable persistence is introduced.

## 16. Capacity handling

When a registration/admission needs an origin entry and the registry is at capacity:

1. deterministically identify states that satisfy the full safe-retirement predicate;
2. remove/prune only those safe-retirable states;
3. retry insertion;
4. if capacity remains exhausted because all entries are active or safety-protected, return a typed capacity error.

Do not evict:

- an origin with an active registration;
- an origin with an active permit;
- an origin with a waiter;
- an origin with an unexpired Retry-After;
- an origin with unexpired backoff/request-delay safety state.

Failing closed on capacity is preferable to dropping safety state.

## 17. Deterministic pruning

If more than one state is safe to retire, pruning order must be deterministic or semantically irrelevant by construction.

Acceptable approaches include deterministic ordering by normalized `OriginKey` or another stable retirement sequence.

Do not use randomized hash-map iteration order to decide which state disappears.

Pruning must not affect active origin semantics.

## 18. No background cleanup requirement

Task 5 does not require a background janitor task.

Safe pruning may occur opportunistically during registration/admission/cache-style maintenance as long as:

- memory remains bounded by the registry capacity;
- protected states are retained;
- expired safe states can eventually be removed;
- no background lifecycle complexity is introduced merely for cleanup.

This keeps the remediation inside Task 5 scope.

## 19. Interaction with robots cache

The robots cache remains a separate Task 5 structure with its already-reviewed bounded capacity and expiry behavior.

Do not merge robots cache lifecycle into the pacing-origin registry merely because both are keyed by origin.

They store different state and have different safety semantics.

The cold-cache single-flight remediation performed during Terra review remains required.

## 20. Override semantics remain unchanged

The robots override design is not changed by this remediation.

A frozen valid override may only affect an ordinary parsed robots disallow decision according to the reviewed Task 5 semantics.

It does not bypass:

- synthetic access-denied policy such as reviewed 401/403 behavior;
- invalid/unavailable policy handling;
- NetworkTargetPolicy;
- concurrency;
- request delay;
- Crawl-delay;
- backoff;
- Retry-After.

The Terra remediation preventing override of synthetic 401/403 deny-all behavior remains authoritative.

## 21. Lost-wake and single-flight remediations remain frozen

The following Terra Task 5 fixes are accepted prerequisites and must remain intact:

- waiters are registered/enabled before checking shared state so notifications cannot be lost between state inspection and first poll;
- cancellation-aware waits preserve the same no-lost-wake invariant;
- same-origin cold robots-cache misses are coalesced through cancellation/abort-safe single-flight behavior.

The pacing-lifecycle remediation must not undo these fixes.

## 22. Error semantics

Registry capacity exhaustion must be a typed runtime admission/registration failure.

It must not be represented as:

- robots disallow;
- robots unavailable;
- network-target rejection;
- generic provider failure;
- silent fallback to an isolated pacing bucket.

Errors must remain bounded and must not expose URL query values or raw internal map contents.

## 23. Locking and deadlock constraints

The remediation must preserve Task 5 lock discipline.

Do not:

- hold the process-wide registry lock while sleeping;
- hold it during network I/O;
- hold it while waiting for a semaphore/permit;
- introduce lock-order inversion between registry state and per-origin state.

Registration insertion/removal and retirement checks should use short critical sections.

Different origins must remain independently schedulable.

## 24. Task 6 / Task 8 integration contract

Task 5 exposes the lifecycle primitive; Task 6 and Task 8 will later decide exactly where to hold it.

The intended integration is conceptually:

```text
immutable run snapshot
    -> normalized origin
    -> register pacing configuration
    -> hold registration for relevant execution lifetime
    -> acquire/release request admission permits as work proceeds
    -> release registration when the execution context is no longer relevant
```

Task 5 must not implement Quick Scrape or Production lifecycle in this remediation.

## 25. Process restart semantics

All Task 5 pacing state remains runtime-local and is lost on process restart, as already implied by the no-new-persistence Task 5 boundary.

This design minimizes undesirable restart dependence by ensuring configuration contributions are tied to active runtime owners rather than historical requests.

Existing non-durable Retry-After/backoff behavior is unchanged.

## 26. Concurrency race requirements

Registration changes and admission decisions for one origin must be coherent under concurrency.

Required properties:

- two simultaneous registrations both contribute before effective concurrency is calculated for subsequent admissions once registered;
- releasing one registration cannot remove another owner's contribution;
- an admission cannot observe an effective ceiling larger than the active-registration minimum because of a race;
- active permit count never exceeds the effective ceiling enforced at the point of each admission;
- reducing the effective ceiling below the number of already-active permits does not forcibly cancel in-flight requests, but no new permit is admitted until active count falls below the new ceiling;
- increasing the effective ceiling after a restrictive registration is released may admit additional waiters normally.

## 27. Registration release while waiters exist

When a restrictive registration is released while callers are waiting, the state change must wake/re-evaluate relevant waiters so they do not remain asleep under a stale lower ceiling.

The implementation should notify waiters after effective configuration changes where required.

Likewise, adding a stricter registration must affect future admission decisions immediately after the registration is established.

## 28. Test requirements

Add deterministic focused coverage for at least:

### Configuration lifecycle

- one registration concurrency `1` constrains the origin;
- two active registrations `2` and `5` produce effective `2`;
- releasing `2` widens effective ceiling to `5`;
- releasing all registrations does not leave historical minimum sticky;
- duplicate equal-value registrations remain independently owned;
- dropping one equal-value registration leaves the other active;
- stricter registration added while permits are active blocks new admissions without cancelling existing permits;
- releasing a restrictive registration wakes/re-evaluates waiters.

### Safety deadline persistence

- Retry-After remains active after its originating registration is released;
- backoff remains active after originating registration release;
- request-delay deadline remains active after registration release where the existing Task 5 semantics establish such a deadline;
- state retires only after these deadlines expire and all liveness conditions clear.

### Registry retirement/capacity

- inactive safe origin can be retired;
- active registration prevents retirement;
- active permit prevents retirement;
- waiter prevents retirement;
- unexpired Retry-After/backoff/request-delay prevents retirement;
- deterministic pruning occurs when capacity is reached;
- capacity exhaustion with only protected entries returns typed error;
- capacity exhaustion never creates an isolated fallback bucket;
- process-wide map does not grow beyond the accepted bound.

### Existing reviewed behavior remains

- same-origin independent `PacingService` instances share state;
- different origins remain isolated;
- lost-wake regressions remain covered;
- robots single-flight remains covered;
- synthetic 401/403 deny-all cannot be overridden;
- Retry-After/backoff/deadline composition remains unchanged.

Tests must use deterministic/manual monotonic time and no public network.

## 29. Non-goals

This remediation does not add:

- distributed/multi-process pacing;
- durable pacing state;
- migration `0006`;
- database run ownership in `erabi-crawler`;
- Quick Scrape orchestration;
- batch orchestration;
- Production orchestration;
- a background cleanup daemon;
- a generic cache framework;
- proxy architecture;
- changes to Task 4 network-target policy;
- changes to robots parser semantics;
- new robots override semantics.

## 30. Compatibility with accepted Tasks 1–4 and reviewed Task 5 fixes

This design must not change:

- Task 1 `CrawlerAdapter` contract;
- typed provider-neutral Retry-After representation;
- Task 2 Crawl4AI mapping;
- Task 3 execution persistence;
- Task 4 `NetworkTargetPolicy` and pinned-resolution invariant;
- Task 4 Source identity/classification behavior;
- Task 5 robots parsing/evaluation semantics already reviewed clean;
- Task 5 robots cache bound/TTL/key behavior already reviewed clean;
- Task 5 lost-wake remediation;
- Task 5 robots single-flight remediation;
- Task 5 401/403 override remediation.

## 31. Acceptance criteria

The Task 5 lifecycle blocker is resolved when all of the following are true:

1. concurrency constraints are represented by explicit active configuration registrations;
2. effective same-origin concurrency is the minimum of active registrations only;
3. completed/released registrations do not permanently throttle future unrelated runs;
4. independent service/run callers still share process-wide same-origin state;
5. active Retry-After/backoff/request-delay safety deadlines survive registration release until expiry;
6. active permits and waiters keep origin state alive;
7. origin state has an explicit safe-retirement predicate;
8. the process-wide origin registry has a finite tested capacity;
9. safe entries are pruned deterministically before capacity failure;
10. active/protected entries are never evicted merely to free capacity;
11. capacity exhaustion fails with a typed error rather than creating a bypass bucket;
12. registration release cannot remove another registration or leak ownership;
13. registration changes wake/re-evaluate waiters where necessary;
14. no Task 6+ behavior, migration, or durable pacing state is introduced;
15. all previously accepted/reviewed Task 5 robots, network, lost-wake, single-flight, and override semantics remain intact.
