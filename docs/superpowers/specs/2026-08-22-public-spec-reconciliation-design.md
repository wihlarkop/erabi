# Erabi Public Specification Reconciliation Design

**Status:** Proposed correction design
**Date:** 2026-08-22
**Base revision:** `54800bb33754a07afccbd1f369f15f43a2cb3629`

## 1. Purpose

This correction pass reconciles Erabi's current public Crawler Studio specification with older July design documents and implementation plans.

The goal is not to expand MVP scope. The goal is to remove ambiguity and prevent implementation from following stale contracts.

## 2. Authority rule

The current public specification under `docs/specs/` is the product source of truth.

The July documents under `docs/superpowers/specs/2026-07-22-*` are historical design inputs. The July implementation plans under `docs/superpowers/plans/` were derived from those older inputs and MUST NOT be executed unchanged when they conflict with `docs/specs/`.

After this reconciliation:

1. `docs/specs/` remains canonical for product/domain behavior.
2. `docs/roadmap/` remains canonical for MVP versus deferred capability boundaries.
3. July superpowers design documents are marked superseded by the public specification.
4. July implementation plans are marked stale until regenerated from the corrected public specification revision.

## 3. Why regeneration is required

The mismatch is architectural rather than editorial.

The July plan set is primarily Source/Extraction-Schema centric. The current public specification is Crawler/Page-Type/Crawler-Version centric.

Examples include:

- July UI navigation uses `Inbox` and global `Schemas`, and omits `Crawlers`.
- Current public navigation uses `Start`, `Crawlers`, `Collections`, `Runs`, `Datasets`, `Assets`, `Exports`, and `Settings`.
- July extraction plans give `ExtractionSchema` an independent draft/approval lifecycle.
- Current public spec places extraction schema inside Page Types within immutable published Crawler Versions.
- July settings resolution omits Crawler defaults and Run Profiles from its main resolver contract.
- Current public spec includes Crawler operational defaults, Run Profiles, and per-run overrides.
- Current public spec defines four first-class run types: `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, and `PRODUCTION_RUN`; the July plan set does not consistently model these as the canonical execution contract.

Trying to patch each old task in place would preserve stale assumptions and create hidden contradictions. Regenerating the implementation plan after the public spec is corrected is safer and easier to verify.

## 4. Public-spec corrections

### 4.1 Define `Source` without displacing `Crawler`

`Crawler` remains the primary reusable design object.

`Source` is a durable identity for an input web target or direct-file target encountered through Quick Scrape or saved crawl history. A Source stores identity/history metadata such as original URL, canonical URL, type, lifecycle state, Collection association when used, and related runs/artifacts.

A Source is not a replacement for a Crawler, Page Type, Seed, Dataset, or Crawl Run.

Quick Scrape MAY create or reuse Sources without requiring a Crawler. Crawler Seeds remain versioned crawler configuration and MUST NOT be silently rewritten from Source state.

### 4.2 Make Quick Scrape batch semantics explicit

`QUICK_SCRAPE` supports one URL by default and MAY accept a bounded pasted URL batch as a convenience submission.

A batch does not create a fifth `BATCH` run type. Each accepted URL creates an independent `QUICK_SCRAPE` run with its own immutable configuration snapshot, status, artifacts, provenance, review outcome, and cancellation/retry semantics.

The batch envelope preserves input order and per-item validation/outcome. One item failing does not silently fail or roll back unrelated accepted items.

CSV/JSONL upload, sitemap ingestion, and RSS ingestion remain outside the MVP unless separately admitted by the roadmap.

### 4.3 Specify direct-file URL behavior

Before normal HTML crawling, Erabi MAY perform a bounded content-type probe when safe.

A URL confidently identified as a direct PDF, CSV, JSON, archive, image, office document, or other non-HTML file is treated as a file Source/Asset intake path rather than an HTML extraction page.

MVP behavior:

- preserve original/canonical URL and Source metadata;
- show detected content type and safe download action;
- use controlled asset storage rules when downloaded;
- do not run HTML extraction against the file;
- do not parse arbitrary file contents into Dataset records in MVP;
- if the probe is unavailable or ambiguous, continue through normal crawl handling and classify from the final response content type.

### 4.4 Make Page Type matching deterministic

Page Type resolution is lexicographic and fully explainable.

1. Higher explicit Page Type priority wins.
2. When priorities tie, compare the best matching URL matcher by deterministic specificity key.
3. When the specificity key ties, classify the URL as `AMBIGUOUS_PAGE_TYPE`.

For MVP, URL matcher kinds are ordered from most to least constrained:

1. exact canonical URL;
2. exact host plus path/template constraints;
3. path glob/prefix constraints;
4. regular-expression matcher.

Within the same matcher kind, specificity compares in this order:

1. more literal path segments;
2. more explicit query-key/value constraints;
3. more literal characters;
4. fewer wildcard/capture tokens.

All components of the specificity key are derived from the validated matcher definition, persisted or reproducibly recomputed, and shown by Test Lab/Discovery Preview. Runtime discovery MUST NOT use map iteration order, creation time, database row order, or matcher insertion order as an implicit tie-breaker.

### 4.5 Require a reason for robots override

Robots policy remains respected by default.

An override requires an explicit non-empty user-provided reason before the run can be created or resumed with the override.

The immutable run snapshot and audit trail store at minimum:

- override decision;
- reason;
- actor;
- timestamp;
- affected origin/scope;
- active User-Agent;
- Crawler/Crawler Version when applicable.

The UI must display the override state prominently. A prior override reason MUST NOT be silently reused for a later independent run.

### 4.6 Define tri-state setting inheritance

Every inheritable ordinary setting uses exactly one of three layer states:

- `INHERIT` — continue to the next lower-precedence layer;
- `CUSTOM(value)` — use this value;
- `RESET_TO_BUILT_IN` — stop inheritance and use the built-in product default.

Resolution precedence for an operational setting is:

```text
per-run override
→ Run Profile
→ Crawler operational default
→ Collection override
→ Global setting
→ built-in default
```

Only layers that are applicable to the current run participate. Quick Scrape without a Crawler skips Crawler and Run Profile layers unless an equivalent ad-hoc run configuration explicitly supplies the value.

The resolved run snapshot stores both the final value and its effective source. Settings UI must distinguish inherit, custom, and reset-to-built-in rather than encoding them as nullable values with ambiguous meaning.

### 4.7 Tighten `SCHEMA_DRIFT`

`SCHEMA_DRIFT` is diagnostic evidence that the extraction contract no longer matches observed page structure or values.

For a normal `PRODUCTION_RUN`, drift that breaks required extraction/identity health MUST NOT be bypassed into trusted change/missing semantics. The run may retain artifacts and diagnostic/partial results, but a new Crawler Draft must correct the Page Type extraction configuration and pass the applicable validation/test flow before a later published version is used for normal production.

`TEST_RUN`, `DISCOVERY_PREVIEW`, and ad-hoc Quick Scrape may inspect drift output for diagnosis, but doing so does not mutate or repair a published Crawler Version and does not approve data automatically.

Remove generic `USE_ANYWAY` behavior where it would permit production trust semantics to bypass this rule.

### 4.8 Preserve destination DB separation

No correction is required to the core destination database policy.

The internal Erabi application database remains separate from user export destinations. SQLite/Turso destination publication remains atomic, typed, and namespaced/dedicated according to the export specification.

## 5. Implementation-plan reconciliation requirements

A regenerated plan must model the current public domain directly.

At minimum it must include:

- `Crawler`, `CrawlerVersion`, `Seed`, `PageType`, `DiscoveryTransition`, `RunProfile`, and `TestEvidence` domain contracts;
- the four official Crawl Run types;
- Quick Scrape single/batch submission without inventing a fifth run type;
- Source as supporting durable target/history identity rather than the central reusable crawler definition;
- Page Type-owned extraction configuration rather than an independently approved global Schema lifecycle;
- deterministic Page Type match resolution and `AMBIGUOUS_PAGE_TYPE` tests;
- complete snapshot semantics and missing-candidate safety;
- tri-state setting resolution across all current layers;
- robots override reason persistence/audit;
- direct-file Source/Asset handling;
- Crawler Studio, Test Lab, Discovery Preview, and Published-versus-Draft UX;
- navigation matching the current public specification;
- release E2E coverage matching `docs/specs/08-ux-accessibility-and-verification.md`.

## 6. Documents affected by the correction pass

Public specification corrections are expected in:

- `docs/specs/01-product-and-experience.md`
- `docs/specs/02-crawler-studio-domain.md`
- `docs/specs/03-discovery-graph-and-runs.md`
- `docs/specs/05-system-architecture-and-persistence.md`
- `docs/specs/06-security-reliability-and-operations.md`
- `docs/specs/08-ux-accessibility-and-verification.md`
- `docs/specs/README.md` where cross-spec invariants need clarification

Historical/stale markers are expected in:

- `docs/superpowers/specs/2026-07-22-erabi-design-index.md`
- `docs/superpowers/plans/2026-07-22-erabi-mvp-plan-index.md`
- the twelve July subsystem plans, or a shared prominent stale banner mechanism that is impossible to miss before execution.

## 7. Audit acceptance criteria

The correction pass is complete only when all of these are true:

1. no public spec calls Quick Scrape single-page-only while required E2E requires pasted URL batches;
2. Source has one canonical definition compatible with the Crawler-centered model;
3. Page Type tie resolution is deterministic and testable;
4. robots override cannot exist without a stored reason;
5. setting inheritance has explicit tri-state semantics and the full current precedence chain;
6. direct-file URLs have a defined non-HTML path;
7. `SCHEMA_DRIFT` cannot silently bypass production trust semantics;
8. destination DB separation remains intact;
9. current implementation planning does not instruct agents to build the superseded July Source/Schema-centric product;
10. regenerated plans reference the exact corrected public-spec commit SHA;
11. required MVP E2E journeys map to concrete plan tasks/tests;
12. a repository-wide terminology audit finds no unresolved conflict among `Crawler`, `Source`, `Seed`, `Page Type`, `Schema`, `Dataset`, and `Crawl Run` roles.

## 8. Non-goals

This correction does not add:

- scheduler/cron crawling;
- authenticated browser workflows;
- AI extraction/crawler copilot;
- distributed workers;
- full drag-and-drop programming;
- new export destinations;
- file-content parsing into records;
- collaboration/accounts.

Those remain governed by the existing roadmap.
