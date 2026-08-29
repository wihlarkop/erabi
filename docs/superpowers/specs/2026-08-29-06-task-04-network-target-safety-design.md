# Plan 06 Task 4 Network Target Safety Design

**Status:** Approved architectural prerequisite for Plan 06 Task 4  
**Date:** 2026-08-29  
**Applies to:** Task 4 Source intake/direct-file classification and later outbound HTTP consumers in Plan 06  
**Parent plan:** `docs/superpowers/plans/2026-08-29-06-crawl4ai-and-quick-scrape-execution.md`

## 1. Decision summary

Erabi will own one provider-neutral outbound network-target safety policy in `erabi-crawler`.

The policy answers a different question from URL canonicalization:

- `CanonicalizationPolicy` in `erabi-domain` answers **what durable URL identity means**.
- `NetworkTargetPolicy` in `erabi-crawler` answers **whether Erabi may actually connect to this target now**.

The MVP policy is **public-network only by default**. A target is connectable only when its URL is structurally valid for crawling and every resolved address selected for the connection is permitted public unicast. Unsafe, mixed-safe/unsafe, malformed, or otherwise prohibited resolution fails closed.

Task 4 content probing must use this shared policy. Task 4 probe redirects are disabled. Later consumers that intentionally follow redirects must validate every redirect target from zero and must not inherit trust from the previous hop.

The connection must use the same validated resolution set that passed policy checks. A validate-then-re-resolve sequence is not acceptable because it leaves a DNS-rebinding/TOCTOU gap.

## 2. Why this design exists

Task 4 requires a bounded `HEAD` and/or prefix `GET` probe to classify confident direct non-HTML targets before HTML crawling. The repository already has URL canonicalization and adapter URL validation, but it did not have one authoritative policy for:

- loopback/private/link-local/internal address rejection;
- DNS resolution safety;
- mixed DNS answers;
- DNS rebinding between validation and connection;
- redirect-hop validation.

Implementing these rules only inside `content_probe.rs` would create a security policy that Task 5 robots fetching and later orchestration could accidentally bypass or reimplement differently. Therefore the policy is a shared crawler-runtime foundation, not a probe-local helper.

## 3. Ownership and dependency direction

### `erabi-domain`

Owns URL identity and canonicalization only.

Existing `CanonicalizationPolicy` remains authoritative for:

- supported HTTP/HTTPS URL identity;
- original versus canonical URL preservation;
- host normalization;
- default-port normalization;
- fragment removal in canonical identity;
- query policy and tracking-parameter handling.

It must not gain DNS, socket, or runtime connection behavior.

### `erabi-crawler`

Owns provider-neutral outbound crawl-target safety.

It may define equivalents of:

- `NetworkTargetPolicy`;
- `NetworkResolver`;
- `ResolvedNetworkTarget` or `ValidatedNetworkTarget`;
- typed network-policy rejection/error values;
- address classification helpers.

Exact Rust names may follow repository conventions, but the semantics in this document are authoritative.

### `erabi-crawl4ai`

Remains the concrete Crawl4AI provider adapter. It is not the owner of Erabi network-target policy and must not become the source of truth for SSRF rules.

### `erabi-db`

Persists durable state only. DNS answers and validated socket addresses are runtime safety evidence and are not durable Source identity.

### `erabi-api`

May translate typed policy errors at an API boundary when a later route needs that behavior. It does not own the policy.

## 4. URL-level target requirements

A network target is eligible for resolution only when all of the following hold:

1. scheme is `http` or `https`;
2. a host exists;
3. username is empty;
4. password is absent;
5. the effective port is valid for the parsed URL;
6. the URL used as an outbound target has no fragment;
7. malformed or ambiguous authority is rejected.

Task 4 must reuse existing canonicalization/URL validation rather than create a second canonical URL algorithm.

Original URL and canonical URL remain distinct durable concepts. Network safety must not overwrite either.

## 5. Address policy

### 5.1 Default rule

The default MVP rule is:

> An outbound crawl connection is allowed only to permitted public-unicast addresses.

Anything not clearly permitted fails closed.

### 5.2 IPv4 rejection classes

At minimum reject addresses in these classes/ranges:

- unspecified (`0.0.0.0/8` semantics, including `0.0.0.0`);
- loopback (`127.0.0.0/8`);
- private (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`);
- link-local (`169.254.0.0/16`);
- carrier-grade NAT/shared address space (`100.64.0.0/10`);
- multicast (`224.0.0.0/4`);
- reserved/non-forwardable/special-use ranges that are not public unicast;
- limited broadcast (`255.255.255.255`);
- documentation/benchmarking/special-purpose ranges when the standard library or explicit policy classifies them as non-global.

The implementation may use a standard-library global-address predicate where its semantics match this contract, but tests must still cover the security-significant classes explicitly.

### 5.3 IPv6 rejection classes

At minimum reject:

- unspecified (`::`);
- loopback (`::1`);
- unique-local (`fc00::/7`);
- link-local (`fe80::/10`);
- multicast (`ff00::/8`);
- IPv4-mapped addresses when the mapped IPv4 address is prohibited;
- documentation/special-purpose/non-global addresses;
- any other address that is not clearly permitted public unicast.

### 5.4 Literal IP hosts

Literal IPv4 or IPv6 hosts are validated by the same address policy. A literal prohibited address is rejected before any HTTP request.

## 6. DNS resolution contract

DNS resolution is explicit and bounded through a provider-neutral resolver abstraction.

Conceptually:

```text
NetworkResolver::resolve(host, port)
    -> bounded set/list of SocketAddr values
```

Requirements:

- empty answer is a typed resolution failure;
- the answer set is bounded;
- duplicate socket addresses may be normalized deterministically;
- semantic decisions do not depend on resolver iteration order;
- tests use a deterministic fake resolver and never require public DNS;
- raw DNS diagnostics do not become public/durable error payloads.

### Mixed answers

If one hostname resolves to any prohibited address, reject the entire target for that resolution attempt.

Example:

```text
safe-looking.example
  -> 93.184.216.34
  -> 127.0.0.1
```

Result: reject.

Do not silently keep only the public answer. Mixed answers are fail-closed because choosing one address while ignoring another creates ambiguous security semantics and can interact badly with resolver/client connection behavior.

## 7. DNS rebinding / TOCTOU requirement

This sequence is forbidden:

```text
resolve hostname
validate returned IPs
call an HTTP client that independently resolves the hostname again
```

The HTTP connection must be constrained to the address set that was already validated.

Conceptually:

```text
URL
 -> resolve
 -> validate all addresses
 -> ValidatedNetworkTarget
 -> connect only to validated address(es)
```

The HTTP Host header and TLS server name continue to use the original hostname as required by HTTP/TLS semantics. Transport address selection, however, must not silently substitute a newly resolved address that bypassed validation.

The implementation may use the HTTP client's supported per-request/client DNS override or another narrow mechanism that guarantees this invariant. If the current HTTP stack cannot provide that invariant without a larger redesign, implementation must stop and report the concrete blocker rather than falling back to validate-then-re-resolve.

Validated DNS/socket addresses are runtime-only and must not become Source canonical identity.

## 8. Redirect policy

### 8.1 Task 4 probe

Task 4 content-probe clients use redirects disabled.

A `3xx` response during the pre-crawl probe does not cause Task 4 to chase the redirect. The probe returns an inconclusive/normal-web-crawl decision unless another existing security/error rule requires rejection.

This is deliberate: direct-file probing is an optimization. Failing to classify a redirect as a file is safer than adding redirect complexity to Task 4.

### 8.2 Future consumers

Any later Plan 06 HTTP consumer that intentionally follows redirects must treat every hop as a new untrusted target:

1. parse new `Location`;
2. apply URL-level target rules;
3. resolve it independently;
4. validate all resolved addresses;
5. constrain the next connection to that validated resolution;
6. enforce a finite redirect-hop bound.

Trust never transfers from the previous origin/hop.

## 9. Error semantics

Network security rejection is distinct from ordinary probe uncertainty.

### Security/policy rejection

Examples:

- unsupported or malformed outbound URL;
- credential-bearing target;
- prohibited literal IP;
- DNS answer containing prohibited address;
- address-policy violation;
- unsafe redirect target in a future redirect-following consumer.

These are typed failures/rejections. They must not become `NormalWebCrawl` fallback because doing so would permit another crawler path to contact the same prohibited target.

### Probe uncertainty/unavailability

Examples:

- timeout while probing an otherwise permitted target;
- `HEAD` unsupported;
- missing or ambiguous `Content-Type`;
- bounded prefix insufficient to classify;
- ordinary upstream outage;
- Task 4 redirect response with redirect following disabled.

These normally produce the existing Task 4 conservative fallback:

```text
NormalWebCrawl
```

Security rejection and classification uncertainty must remain separate typed paths.

## 10. Task 4 content-probe use

The content probe receives only a target that has passed the shared network policy for the actual connection attempt.

The probe remains bounded:

- explicit timeout;
- redirects disabled;
- `HEAD` first when appropriate;
- tightly bounded prefix `GET` fallback only when useful;
- never buffer arbitrary full files;
- never execute/open/extract content;
- MIME/signature evidence may be inspected within the bound;
- ambiguous evidence returns normal web crawl;
- confident direct non-HTML evidence may return `FileAsset`.

The probe must not duplicate network-target policy internally.

## 11. Task 5 and later reuse

This design is introduced because Task 4 needs it first, but it is a shared Plan 06 foundation.

Task 5 robots fetching must reuse the same outbound target policy rather than create separate private-network/DNS checks.

Task 6/8 orchestration must not bypass the policy when Erabi itself makes outbound HTTP requests.

This document does not implement Task 5 robots/pacing, Quick Scrape, or production orchestration.

## 12. Crawl4AI boundary

The shared Erabi outbound policy governs HTTP requests made by Erabi itself, including Task 4 probing and later robots requests.

Crawl4AI is a separate process/provider boundary. Task 4 must not attempt to reimplement or redesign Crawl4AI networking in this prerequisite.

The accepted adapter contract continues to reject malformed/credential/fragment URL forms. If later product requirements demand that Erabi guarantee equivalent private-network restrictions inside an external Crawl4AI deployment, that requires an explicit deployment/security contract and is outside this Task 4 prerequisite.

## 13. Determinism and testability

Provide deterministic tests for at least:

- public IPv4 accepted;
- loopback IPv4 rejected;
- private IPv4 rejected;
- link-local IPv4 rejected;
- CGNAT/shared IPv4 rejected;
- multicast/special IPv4 rejected;
- public IPv6 accepted;
- IPv6 loopback rejected;
- IPv6 unique-local rejected;
- IPv6 link-local rejected;
- IPv6 multicast rejected;
- prohibited IPv4-mapped IPv6 rejected;
- literal prohibited IP rejected without network I/O;
- empty DNS answer rejected;
- mixed public/prohibited answer rejected;
- all-public bounded answer accepted;
- resolver answer order does not alter allow/reject semantics;
- connection is constrained to the validated resolution rather than implicitly re-resolved;
- Task 4 probe redirects remain disabled;
- security rejection is distinguishable from normal probe ambiguity.

Use deterministic fake/local resolver and HTTP fixtures. No public DNS or internet dependency.

## 14. Non-goals

This prerequisite does not add:

- user-configurable private-network allowlists;
- an SSRF override toggle;
- arbitrary internal-network crawling;
- proxy support;
- DNS cache persistence;
- durable DNS evidence;
- certificate pinning;
- custom CA management;
- robots policy;
- pacing;
- Quick Scrape orchestration;
- production orchestration;
- full asset downloader/storage lifecycle;
- Plan 07 extraction/dataset work;
- Plan 08 export/backup/retention work.

If private/internal crawling becomes a future product requirement, it needs an explicit security design rather than weakening this default silently.

## 15. Compatibility with accepted Tasks 1-3

Tasks 1-3 remain frozen.

This prerequisite must not change:

- the provider-neutral `CrawlerAdapter` semantics;
- accepted Crawl4AI v0.9.2 mapping;
- execution persistence schema `0005`;
- exactly four CrawlRun types;
- published/draft CrawlerVersion invariants;
- Source/Seed independence;
- provider-secret redaction guarantees.

Small exports or shared helper reuse are acceptable only when they do not alter accepted semantics.

## 16. Acceptance criteria

The prerequisite is complete when:

1. one shared provider-neutral network-target policy exists in `erabi-crawler`;
2. URL and resolved-address checks are explicit and fail closed;
3. mixed DNS answers are rejected;
4. HTTP transport cannot silently re-resolve outside the validated address set;
5. Task 4 probe uses redirects disabled;
6. security rejection is distinct from probe ambiguity fallback;
7. deterministic tests cover the security-significant address classes and rebinding invariant;
8. no Task 5+ behavior or Plan 07/08 scope is implemented;
9. accepted Task 1-3 contracts remain semantically unchanged.
