# Crawler Studio Domain Specification

## 1. Domain center: Crawler

`Crawler` is the primary reusable design object in Erabi.

A Crawler represents the intent to repeatedly acquire and extract a coherent web domain or workflow. It does not represent one execution. Execution belongs to `CrawlRun`.

```text
Crawler
├── identity and metadata
├── Collection (optional)
├── published versions
├── draft version
├── run profiles
└── run history
```

Every major entity uses UUIDv7 generated application-side and represented as canonical UUID strings at API boundaries.

### 1.1 Source boundary

`Source` is a supporting durable identity for a web target or direct-file target encountered through Quick Scrape or retained crawl history. It may retain original/canonical URL, target type, lifecycle state, Collection association where applicable, related Crawl Runs, and artifact references.

A Source is not the reusable crawling design. It MUST NOT replace a Crawler, Seed, Page Type, Dataset, or Crawl Run. Quick Scrape may create or reuse Sources without a Crawler. A Crawler Seed remains explicit versioned configuration and is never silently rewritten merely because Source metadata changes.

## 2. Crawler Version lifecycle

Crawler configuration is versioned.

```text
Crawler
├── Published v1  immutable
├── Published v2  immutable
├── Published v3  active production version
└── Draft v4      editable
```

Rules:

- A published version MUST be immutable.
- Editing an already published crawler creates a new draft version.
- A crawler may have at most one ordinary active draft in MVP.
- A draft MAY be created from any prior published version.
- A normal production run MUST reference an active published version.
- `TEST_RUN` and `DISCOVERY_PREVIEW` MAY reference a draft version.
- Publishing a draft creates a new immutable published version and may make it active.
- Reactivating an older published version is a pointer/lifecycle action; the old version itself is never modified.
- Historical runs always retain the exact Crawler Version they used.

Crawler Version content includes all behavior that changes what URLs are considered, how they are classified, how they are traversed, and how data is extracted.

## 3. Seeds

A Crawler supports **multiple seed URLs**.

Each seed stores at minimum:

- UUIDv7 identity;
- original URL;
- canonical seed URL;
- enabled flag;
- optional label;
- optional seed-specific entry Page Type hint;
- provenance/audit metadata.

A production or test run may target:

- all enabled seeds;
- selected seeds;
- one seed.

Selecting a subset is a run parameter and does not change the Crawler Version.

## 4. Page Types

A Crawler Version contains multiple Page Types. A Page Type represents a structural/semantic class of pages such as:

- Category;
- Listing;
- Product Detail;
- Article;
- Forum Topic;
- Forum Post page;
- Profile;
- Documentation page.

Each Page Type contains:

```text
PageType
├── id
├── name
├── priority
├── URL matchers
├── optional structural hints
├── extraction schema
├── validation rules
├── unique-key contract
├── primary/shared Dataset mapping
└── discovery transitions
```

### 4.1 URL matching

A URL may match zero, one, or multiple Page Types.

Match resolution is lexicographic and fully explainable:

1. higher explicit Page Type priority wins;
2. when priorities tie, compare the best matching URL matcher using the deterministic specificity key below;
3. when the complete specificity key ties, classify as `AMBIGUOUS_PAGE_TYPE`.

For MVP, matcher-kind specificity is ordered from most to least constrained:

1. exact canonical URL;
2. exact host plus path/template constraints;
3. path glob/prefix constraints;
4. regular-expression matcher.

Within the same matcher kind, compare in this order:

1. more literal path segments;
2. more explicit query-key/value constraints;
3. more literal characters;
4. fewer wildcard/capture tokens.

The resulting specificity key is derived only from the validated matcher definition and MUST be persisted or reproducibly recomputed. Runtime matching MUST NOT use map iteration order, creation time, database row order, matcher insertion order, UUID ordering, or another hidden tie-breaker.

Erabi MUST NOT silently choose between Page Types whose complete resolution keys tie. The Test Lab and Discovery Preview show all candidate matches, explicit priority, matcher kind, specificity components, and why a winner was selected or why ambiguity remains.

### 4.2 Unmatched URLs

A canonical URL that matches no Page Type is classified as `UNMATCHED`.

`UNMATCHED` URLs:

- are preserved;
- retain discovery provenance;
- appear in Discovery Preview and Run Inspector;
- are not traversed further by default.

The Studio may offer actions such as create a Page Type from selected URLs, extend an existing matcher, or add an ignore pattern. Those actions still produce an explicit draft configuration change.

## 5. Discovery Transitions

Page Types are connected by explicit directed discovery transitions.

```text
Category ──category links──> Listing
Listing ──product links────> Product Detail
Listing ──next page────────> Listing
Product Detail ──related───> Product Detail
```

A `DiscoveryTransition` contains at minimum:

- UUIDv7 identity;
- source Page Type;
- target Page Type;
- name;
- enabled flag;
- link selector / extraction rule;
- optional URL constraints;
- priority;
- maximum links discovered per source page;
- optional total transition budget;
- depth contribution;
- deduplication behavior;
- test evidence summary.

The discovery path itself becomes provenance for each discovered URL.

## 6. Cycles and guardrails

Cycles are valid and necessary. Examples include pagination and related-content traversal.

Every crawler therefore operates with mandatory guardrails:

### Crawler-wide guardrails

- maximum pages;
- maximum crawl depth;
- maximum run duration;
- maximum downloaded bytes/storage budget;
- domain concurrency/rate limits.

### Page Type guardrails

- optional Page Type page budget;
- extraction/validation health thresholds.

### Transition guardrails

- maximum links discovered per page;
- optional total transition budget.

URL deduplication is mandatory. The run inspector must surface suspicious discovery growth and duplicate prevention counts.

## 7. URL canonicalization

Canonicalization is a required, versioned Crawler concern because URL identity controls deduplication, Page Type classification, run budgets, and missing-record semantics.

Default safe canonicalization pipeline:

```text
original URL
→ parse and validate
→ lowercase scheme/host
→ normalize default port
→ remove fragment
→ normalize path/trailing slash consistently
→ sort query parameters
→ remove known tracking parameters
→ apply crawler custom keep/drop rules
→ canonical URL
```

Default removable tracking parameters may include common `utm_*`, `fbclid`, and `gclid` forms. Erabi MUST NOT broadly drop unknown query parameters because they may alter content identity.

Both original and canonical URLs are retained. Provenance records the original URL; scheduling/deduplication uses canonical identity.

Test Lab and Discovery Preview show canonicalization decisions and deduplication impact.

## 8. Domain Scope Policy

Cross-domain crawling is never implicit.

Default scope:

> **Seed domains only.**

Supported policies:

- seed domains only;
- same registrable domain plus explicitly chosen subdomains;
- explicit allowlist;
- custom allow/block policy.

Discovered out-of-scope URLs are classified as `EXTERNAL`. They are preserved with discovery provenance but are not crawled.

Domain scope belongs to the Crawler Version, not a Run Profile, because changing it changes crawler semantics.

## 9. Run Profiles

A `RunProfile` is reusable operational configuration layered on top of a Crawler Version.

Typical examples:

- Quick Test;
- Normal;
- Deep Crawl.

Run Profiles MAY override only operational settings, such as:

- max pages;
- max depth;
- max duration;
- concurrency;
- request delay;
- timeouts;
- screenshot policy;
- asset download/storage limits.

Run Profiles MUST NOT override semantic crawler structure:

- Page Type matchers;
- discovery transitions;
- extraction schema;
- unique keys;
- canonicalization;
- domain scope;
- validation rules;
- dataset identity mappings.

## 10. Temporary per-run overrides

When starting a run, the user may apply temporary operational overrides on top of a Run Profile.

These values:

- apply to one run only;
- do not mutate the Run Profile;
- do not create a Crawler Version;
- are limited to the same operational fields allowed in a Run Profile;
- are resolved and stored in the immutable Crawl Run configuration snapshot at run creation time.

The UI shows the source of each resolved value: built-in/global, Collection, Crawler default, Run Profile, or per-run override.

## 11. Publish validation gate

Publishing a Crawler draft is a validated domain transition, not a save button.

Blocking structural errors include at minimum:

- no enabled seed;
- invalid Page Type matcher syntax;
- unresolved Page Type ambiguity caused by invalid priorities/matchers known at design time;
- transition referencing missing Page Types;
- invalid extraction schema;
- incompatible Dataset mapping;
- invalid unique-key contract;
- missing mandatory cycle/run guardrails;
- invalid domain-scope configuration;
- invalid canonicalization rules;
- invalid crawl budgets.

Warnings do not block publish. Examples:

- Page Type has never been tested;
- transition has no test evidence;
- selector coverage was low in latest test;
- matcher appears unusually broad;
- Discovery Preview predicts rapid graph growth.

A successful test run is recommended but not mandatory for publish.

Publishing produces an audit event containing version identity, config hash, actor, timestamp, warning summary, and parent/base version.

## 12. Test Evidence

Tests executed in the Test Lab produce durable `TestEvidence` records containing:

- Crawler Version ID;
- test type;
- test input URL(s);
- Page Type evaluated;
- match decision;
- extraction summary;
- discovered URL summary;
- warnings/errors;
- artifact references;
- configuration hash;
- execution timestamp.

Publish UI can use this evidence to show which Page Types and transitions have recently been tested. Test evidence is confidence metadata, not production approval.

## 13. Crawler and generic-framework boundary

Crawler configuration is domain-specific. Erabi will not generalize Page Types, Dataset relationships, or Studio panels into an arbitrary application-schema framework merely because they can be represented generically in code.
