# Plan 06 Task 4 Network Target Safety Implementation Plan

> **For agentic workers:** Implement this plan in the existing Plan 06 Task 4 boundary. Erabi uses implementation-first verification-after sequencing. Do not intentionally create failing tests first. Do not use subagents for this task.

**Goal:** Unblock Plan 06 Task 4 by adding one shared provider-neutral outbound network-target safety layer, then use it for bounded direct-file probing and deterministic Source create/reuse without changing accepted Tasks 1-3.

**Architecture:** `erabi-domain::CanonicalizationPolicy` remains the URL-identity authority. `erabi-crawler` gains a runtime `NetworkTargetPolicy` plus resolver/validated-target boundary that rejects non-public destinations and prevents validate-then-re-resolve DNS rebinding. Task 4 content probing consumes that validated target with redirects disabled, while Source persistence remains in `erabi-db` and never mutates Crawler Seeds.

**Tech Stack:** stable Rust, Tokio, Reqwest/Rustls where already used/appropriate, `url`, Turso, deterministic fake DNS/local HTTP fixtures, existing Erabi domain/repository conventions.

**Spec:** `docs/superpowers/specs/2026-08-29-06-task-04-network-target-safety-design.md`

**Parent Task:** `docs/superpowers/plans/2026-08-29-06-crawl4ai-and-quick-scrape-execution.md` Task 4

## Global Constraints

- Canonical `docs/specs/` remains authoritative on conflict.
- Exactly four CrawlRun types remain `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, `PRODUCTION_RUN`.
- Tasks 1-3 and foundation-hardening commit `663f1688614dc2e95107d47db1e449871995537a` are frozen.
- `CanonicalizationPolicy` keeps URL identity ownership; network policy must not duplicate canonicalization.
- Outbound Erabi network connections are public-unicast-only by default.
- Any prohibited address in a DNS answer rejects the whole target.
- The actual HTTP connection must be constrained to the exact validated resolution set; validate-then-independent-re-resolve is forbidden.
- Task 4 probe redirects are disabled.
- Security-policy rejection never degrades into ordinary `NormalWebCrawl` fallback.
- Probe ambiguity/unavailability normally degrades to `NormalWebCrawl`.
- Confident direct non-HTML targets become `FileAsset` and never enter HTML extraction.
- Source intake never silently creates, updates, or deletes Crawler Seeds.
- No migration `0006` is introduced for Task 4 unless an explicit design blocker proves the accepted Source schema unusable; stop instead of inventing schema.
- No Task 5 robots/pacing, Task 6 Quick Scrape orchestration, Task 7 batch lifecycle, Task 8 production orchestration, Task 9 finalization, Plan 07, Plan 08, UI, or CI scope.
- No public DNS/internet dependency in tests.
- Luna owns implementation plus fmt/clippy/diff checks. The user owns `cargo test`, `cargo check`, and `cargo build`. Terra performs the independent review after user tests pass.

---

## Task 4A: Shared outbound network-target safety foundation

**Files:**

- Create: `crates/erabi-crawler/src/network_policy.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Modify: `crates/erabi-crawler/Cargo.toml` only if the implementation genuinely needs an already-compatible runtime dependency
- Test: add focused unit tests in `network_policy.rs` or a dedicated `crates/erabi-crawler/tests/network_policy.rs` according to current crate convention

**Consumes:**

- `erabi_domain::CanonicalizationPolicy` and/or its canonicalized `url::Url` result;
- existing Task 1 URL-safety semantics;
- Tokio/runtime facilities already accepted in the workspace.

**Produces:**

Provider-neutral equivalents of:

```text
NetworkTargetPolicy
NetworkResolver
ValidatedNetworkTarget / ResolvedNetworkTarget
NetworkTargetError
```

Exact names may follow existing conventions, but later Task 4 components must consume this single policy rather than recreate address checks.

### Step 1: Inspect current HTTP/DNS capabilities before choosing transport binding

Read the exact versions/features of:

- `reqwest` used by the workspace;
- Tokio networking/DNS APIs;
- existing HTTP-client construction in `erabi-crawl4ai` for style only, not ownership;
- current `erabi-crawler` dependencies.

Identify one supported mechanism that can constrain the actual HTTP connection to the already validated socket-address set while retaining the original hostname for HTTP Host/TLS SNI.

Acceptable implementation families include an HTTP-client DNS override/resolver hook or another narrow mechanism that guarantees no unvalidated second resolution.

If the available stack cannot provide that invariant without redesigning accepted Tasks 1-3, STOP and report the concrete API/architecture blocker. Do not implement validate-then-re-resolve.

### Step 2: Implement URL-level outbound validation

The shared policy must reject before DNS/network I/O when the outbound target:

- is not HTTP/HTTPS;
- has no host;
- contains username/password;
- contains a fragment;
- has malformed/ambiguous authority or invalid effective port.

Do not add canonical-query rewriting here. Consume canonical URL identity from the existing canonicalization layer.

### Step 3: Implement pure address classification

Implement deterministic pure helpers for IPv4/IPv6 that permit only clearly public-unicast addresses.

At minimum reject:

```text
IPv4
- unspecified / 0-space
- loopback 127/8
- RFC1918 private ranges
- link-local 169.254/16
- shared/CGNAT 100.64/10
- multicast
- limited broadcast
- non-global special/documentation/benchmarking/reserved ranges

IPv6
- ::
- ::1
- fc00::/7
- fe80::/10
- ff00::/8
- non-global special/documentation ranges
- IPv4-mapped values when mapped IPv4 is prohibited
```

Prefer standard-library classification when it exactly matches the approved public-unicast rule; retain explicit regression tests for every security-significant class so a future library/compiler change cannot silently widen policy.

### Step 4: Implement the resolver abstraction

The resolver contract must:

- accept the host/effective port needed for connection;
- return a bounded deterministic collection of socket addresses;
- reject empty answer sets;
- normalize duplicate addresses deterministically;
- never expose raw resolver/provider diagnostics as a stable public contract.

Provide a production resolver and a deterministic fake resolver for tests.

The fake resolver must support fixtures such as:

```text
public-v4.test -> one permitted public IPv4
public-v6.test -> one permitted public IPv6
private.test -> 127.0.0.1 or RFC1918
mixed.test -> one permitted address plus one prohibited address
empty.test -> no addresses
```

Fixture names are local test identities; they do not require public DNS.

### Step 5: Implement all-address validation and mixed-answer fail-closed behavior

For a resolved hostname:

- validate every returned address;
- if any one address is prohibited, reject the whole target;
- do not select only the safe subset;
- do not rely on resolver return order for allow/reject semantics.

Produce an immutable runtime validated-target value containing only the URL/host/port plus the permitted resolution information needed to make the immediate connection safely.

Do not persist this value in the DB.

### Step 6: Protect against DNS rebinding at connection time

Make the actual outbound HTTP client/request consume the validated resolution rather than independently resolve the hostname again.

A focused regression must prove the connection path uses the validated address mapping. The test can use a fake resolver/local HTTP fixture and a hostname that would be meaningless without the supplied validated mapping.

Do not weaken TLS/Host semantics: the logical hostname remains the hostname for HTTP/TLS; only the transport destination is constrained.

### Step 7: Add deterministic network-policy verification source

Add tests covering at least:

- permitted public IPv4;
- prohibited loopback/private/link-local/CGNAT/multicast/special IPv4;
- permitted public IPv6;
- prohibited IPv6 loopback/ULA/link-local/multicast/special;
- prohibited IPv4-mapped IPv6;
- prohibited literal IP rejected before resolver use;
- empty DNS response;
- mixed answer rejection;
- all-public answer acceptance;
- resolver ordering independence;
- duplicate-address normalization;
- validated-resolution connection binding;
- credential and fragment rejection.

Do not run `cargo test`; test execution belongs to the user.

---

## Task 4B: Deterministic Source repository create/reuse

**Files:**

- Create if no equivalent exists: `crates/erabi-db/src/repositories/source.rs`
- Modify: `crates/erabi-db/src/repositories/mod.rs`
- Test: add Source repository coverage in the most focused existing/new Task 4 DB test file

**Consumes:**

- existing `Source`, `SourceId`, `SourceTargetType`, `SourceStatus`, Collection ownership, and `sources` table;
- existing URL canonicalization/naming behavior.

**Produces:**

A deterministic repository boundary for create/reuse by the existing authoritative Source identity dimensions.

### Step 1: Derive Source identity from current schema/spec, not UUID order

Inspect the Source domain type and `sources` table before coding.

Use the existing authoritative identity dimensions, expected to include the owning collection/context and canonical URL identity. Preserve original URL separately.

Do not create a new Source identity definition only for Task 4.

### Step 2: Implement deterministic lookup/reuse

Repository behavior must be:

```text
no matching Source
    -> create one valid Source

exactly one coherent matching Source
    -> reuse it

multiple/contradictory rows where the domain expects one identity
    -> typed corruption/conflict error
```

Never use UUID/database order as a semantic duplicate resolver.

Do not use `LIMIT 1` to hide forbidden duplicates.

### Step 3: Preserve Source/Seed independence

Source create/reuse code must not write to:

- `seeds`;
- crawler version configuration;
- PageTypes;
- URL matchers;
- transitions;
- RunProfiles.

Add a focused verification fixture that creates or observes an existing Seed and proves Source intake leaves it unchanged.

### Step 4: Fail closed on malformed durable Source state

When reading a Source row, validate domain IDs, status/target type, original URL, and canonical URL according to existing repository conventions.

Persisted corruption must use corruption/integrity semantics, not ordinary caller-invalid-input semantics where the repository contract distinguishes them.

---

## Task 4C: Bounded content probe

**Files:**

- Create: `crates/erabi-crawler/src/content_probe.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Modify: `crates/erabi-crawler/Cargo.toml` only for genuinely required HTTP/runtime dependencies
- Test: `crates/erabi-crawler/tests/source_intake.rs` and/or focused probe tests

**Consumes:**

- Task 4A shared network-target policy;
- existing Source target type/classification terminology.

**Produces:**

A bounded probe result that keeps **security rejection** distinct from **classification result**.

Use equivalents of:

```text
ContentProbe
ContentProbeDecision::FileAsset(...)
ContentProbeDecision::NormalWebCrawl
ContentProbeError::NetworkTargetRejected(...)
```

Exact names may follow established conventions.

### Step 1: Build a probe HTTP client with redirects disabled

The Task 4 probe must not auto-follow redirects.

It must use the validated transport destination from Task 4A.

Set explicit bounded timeout behavior using existing project configuration/default patterns rather than an unexplained magic value. If Task 4 needs a new internal constant, name it and document why it is a fixed safety bound.

### Step 2: Implement HEAD-first probing

For an allowed validated target:

- issue bounded HEAD when appropriate;
- inspect response status and `Content-Type`;
- do not consume an arbitrary response body;
- if metadata confidently identifies a direct file category, return the file decision;
- if metadata clearly represents normal HTML/web content, return normal web crawl;
- if HEAD is unsupported or metadata insufficient, proceed only to the bounded GET fallback allowed below.

### Step 3: Implement tightly bounded prefix GET fallback

GET fallback is for classification evidence only.

Requirements:

- redirects remain disabled;
- use a bounded prefix/range/stream-read strategy supported by the HTTP stack;
- stop reading once the classification prefix bound is reached;
- do not buffer the entire file;
- do not retry indefinitely;
- inspect only limited signature evidence needed for classification.

If the server ignores a Range request, the client still must stop after the local prefix limit rather than buffering the response.

### Step 4: Implement conservative evidence classification

Confident `FileAsset` categories must cover at least:

- PDF;
- CSV;
- JSON;
- archive;
- image;
- office-like documents.

Use deterministic evidence precedence.

Rules:

- extension alone is insufficient;
- a credible file MIME can be strong evidence;
- bounded magic/signature bytes can resolve `application/octet-stream` or missing MIME;
- contradictory signals must not fabricate confidence;
- HTML MIME/body evidence wins against a misleading `.pdf`-style extension;
- confident signature evidence may identify a file despite a misleading path extension;
- ambiguous or contradictory evidence returns `NormalWebCrawl` unless it is a security rejection.

Do not perform extraction or archive inspection beyond bounded outer-file signature classification.

### Step 5: Treat redirects as probe ambiguity

For Task 4 pre-crawl probe `3xx` responses:

- do not follow `Location`;
- do not classify the redirect destination as a file;
- return `NormalWebCrawl`/inconclusive classification according to the probe API.

This behavior does not weaken the future redirect rule: any later redirect-following consumer must validate every hop independently under the shared network policy.

### Step 6: Keep security rejection separate from normal fallback

Examples:

```text
DNS resolves to 127.0.0.1
    -> typed NetworkTargetRejected error

probe timeout on permitted target
    -> NormalWebCrawl

HEAD 405 + bounded GET ambiguous
    -> NormalWebCrawl

missing MIME + PDF signature
    -> FileAsset(PDF)
```

Never turn a prohibited network target into normal crawl fallback.

---

## Task 4D: Source intake orchestration inside the Task 4 boundary

**Files:**

- Create: `crates/erabi-crawler/src/source_intake.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Test: `crates/erabi-crawler/tests/source_intake.rs`
- Modify `erabi-api` only if canonical specs/current code prove an API is required now; otherwise leave API untouched for Task 6

**Consumes:**

- canonicalization;
- Source repository;
- shared network policy;
- bounded content probe.

**Produces:**

A reusable Task 4 service/function that later Task 6/8 can call before crawler execution.

### Step 1: Canonicalize and preserve provenance

Take the raw user/source URL through the accepted canonicalization layer.

Preserve:

- original URL;
- canonical URL;
- canonicalization decisions where the existing Source/API model needs them.

Do not replace original input with provider final URL.

### Step 2: Create/reuse Source without Seed mutation

Use Task 4B repository behavior.

Do not derive Seed writes from Source intake.

### Step 3: Apply network safety before probe I/O

Resolve/validate using Task 4A.

A policy rejection returns a typed source-intake failure.

Do not call the probe or crawler after a prohibited target decision.

### Step 4: Probe and update the Source classification only within existing schema semantics

Use Task 4C result:

```text
confident file
    -> SourceTargetType::FileAsset (or exact existing equivalent)

normal/ambiguous/unavailable probe
    -> normal web target type
```

Do not add physical asset bodies or download state to Source rows.

Do not add migration `0006`.

If updating an existing Source's target classification would violate an accepted immutable/identity rule in the current domain, STOP and report that precise conflict rather than inventing mutation semantics.

### Step 5: Keep future final-response classification possible

The Task 4 API must not imply that `NormalWebCrawl` is permanent proof of HTML.

It means only that the pre-crawl probe did not confidently route directly to `FileAsset`.

Task 6/8 may later observe an authoritative non-HTML final crawler response and route it through the future FileAsset path.

---

## Task 4E: Verification source and lightweight gate

### Verification source requirements

Add deterministic tests covering the full Task 4 contract where applicable:

**Source**

- create;
- deterministic reuse;
- original/canonical preservation;
- collection ownership;
- forbidden duplicate/corrupt durable state;
- Source/Seed independence.

**Network safety**

- prohibited IPv4/IPv6 classes;
- mixed DNS fail-closed;
- literal IP rejection;
- empty DNS result;
- all-public resolution;
- validated-resolution connection binding;
- credentials/fragment rejection.

**Probe/classification**

- PDF;
- CSV;
- JSON;
- archive;
- image;
- office-like;
- misleading extension;
- conflicting MIME/signature;
- `application/octet-stream` plus signature;
- HTML despite file-like extension;
- HEAD unsupported;
- bounded GET fallback;
- timeout/unavailable ambiguity;
- redirects disabled;
- response prefix bound;
- security rejection remains an error rather than fallback.

Use deterministic fake resolver and local HTTP fixture server only.

### Luna-owned lightweight verification

After implementation, Luna runs and fixes ordinary findings until green:

```text
cargo fmt --all
cargo fmt --all --check
cargo clippy -p erabi-crawler -p erabi-db --all-targets -- -D warnings
git diff --check
git status --short
git diff --stat
git diff
```

Include `erabi-domain` or `erabi-api` in Clippy only if Task 4 actually changes those crates.

Luna must not run:

```text
cargo test
cargo check
cargo build
cargo metadata
```

### User-owned heavy verification

After Luna returns a clean lightweight gate, the user runs at minimum:

```text
cargo test -p erabi-crawler
cargo test -p erabi-db
```

Additionally run:

```text
cargo test -p erabi-domain
```

only if domain code changed, and:

```text
cargo test -p erabi-api
```

only if API code changed.

### Review gate

After user heavy tests pass:

- start a NEW Terra session;
- review Task 4 independently against this spec, the parent Task 4 plan, and canonical specs;
- Terra may remediate narrow unambiguous implementation defects;
- if Terra changes behavior/code, the user reruns affected heavy tests;
- only then accept, commit, and push Task 4.

Do not commit/push directly from Luna.

---

## Stop conditions

Luna must stop and report a design blocker rather than guess when any of these occur:

1. the current HTTP stack cannot bind the real connection to the validated DNS resolution without an unvalidated re-resolution;
2. the existing Source schema/domain cannot represent Task 4 create/reuse/classification without migration `0006` or changing accepted identity semantics;
3. implementing the shared policy would require redesigning accepted Task 1/2/3 public or durable contracts;
4. canonical specs conflict with this approved prerequisite;
5. correct behavior would require introducing private-network allowlists/overrides not approved by this design;
6. Task 4 would have to implement robots, pacing, Quick Scrape, production orchestration, or Plan 08 downloader behavior to proceed.

## Completion state

When implementation plus Luna's lightweight gate are clean, return:

```text
TASK 4 IMPLEMENTED AND LIGHTWEIGHT-VERIFIED — AWAITING USER HEAVY TESTS
```

Do not commit. Do not push. Do not begin Task 5.
