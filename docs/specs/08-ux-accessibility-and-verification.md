# Erabi UX, Accessibility, and Verification Specification

## 1. UX principles

Erabi should feel like a crawler IDE/studio without requiring crawler programming.

Core principles:

- input-first;
- progressive disclosure;
- explicit state;
- no silent destructive/approval behavior;
- evidence next to decisions;
- keyboard-accessible alternatives to visual interactions;
- long-running work always observable.

## 2. Crawler Overview

Crawler Overview acts as a command center.

Example information:

```text
Crawler: Example Shop
Published v3
Draft v4

Health
✓ Page Types valid
⚠ one untested Transition
✓ robots/default safety policy
✓ Crawl4AI connected

Last Production Run
pages / records / failures / duration

[Run Crawler]
[Test Draft]
[Discovery Preview]
```

The Overview MUST make Published vs Draft state visually unambiguous.

## 3. Graph UX

MVP Graph is inspectable rather than a full visual programming editor.

Capabilities:

- visual Page Type nodes;
- Transition edges;
- click node to open Page Type configuration;
- click edge to open Transition configuration;
- warning badges;
- optional Run metrics overlay;
- equivalent accessible list/table.

Full drag/drop node/edge editing is post-MVP.

## 4. Test Lab UX

Each focused test presents:

- exact Draft Version/config hash;
- input URL;
- canonicalization explanation;
- matched Page Types and deterministic specificity rationale;
- extraction preview;
- selector coverage;
- discovered links/Transition result;
- warnings/errors;
- saved Test Evidence.

When multiple Page Types tie through the complete resolution key, Test Lab must show the competing candidates and `AMBIGUOUS_PAGE_TYPE`; it must not hide an implicit insertion/database-order winner.

Users can compare relevant results against the active Published Version.

## 5. Discovery Preview UX

Discovery Preview emphasizes bounded exploration.

Required views:

- tree/graph discovery paths;
- tabular URL inspector;
- Page Type distribution;
- duplicate/canonicalization statistics;
- unmatched/ambiguous/external/blocked lists;
- cycle/Transition budget metrics;
- growth and scope warnings.

Any visual graph has a keyboard-accessible list/table equivalent.

## 6. Review UX

Default Dataset Review view is Table/Grid.

Card View is optional where useful.

Review supports:

- sort/filter;
- inline Draft editing;
- validation visibility;
- multi-select;
- field conflict resolution;
- provenance drawer;
- bulk Approve All Valid;
- rejection reasons;
- Close/Reopen review.

Approved values must be visually distinguishable from Draft candidates.

## 7. Bidirectional extraction highlighting

Extraction Studio supports:

```text
Preview element
↔ Field configuration
↔ Record preview
```

Hover/focus/selection highlights the corresponding evidence in the other surfaces where practical.

A manual selector/value inspector provides an accessible alternative to pointer-only visual picking.

## 8. Theme and localization readiness

MVP themes:

```text
Follow system
Light
Dark
```

English is the first UI language.

UI copy, error contracts, and layout should be localization-ready. Indonesian and Japanese are roadmap candidates.

## 9. Accessibility target

MVP targets WCAG 2.2 AA for product-owned UI.

Requirements include:

- keyboard navigation;
- visible focus;
- semantic HTML;
- accessible names/descriptions;
- appropriate landmarks/headings;
- screen-reader compatible validation/errors;
- sufficient contrast;
- no color-only state encoding;
- reduced-motion support;
- usable zoom to 200%;
- accessible dialogs and tables;
- accessible non-pointer extraction workflow.

Crawler content itself is external/untrusted and may not be accessible, but Erabi's controls around it must remain operable.

## 10. Command palette

`Ctrl/Cmd+K` opens a keyboard-accessible command/search palette.

Safe navigation/inspection actions may run immediately.

Destructive actions route to their dedicated confirmation UX.

## 11. Progress UX

Long-running work never appears frozen.

Progress surfaces show:

- current user-friendly step;
- durable Run status;
- counts/progress;
- warnings;
- cancellation availability;
- expandable technical events/logs.

Reconnect uses SSE replay to avoid losing visible progress events.

For pasted URL batches, the batch surface shows ordered per-item status and links each accepted item to its own `QUICK_SCRAPE` run. It must not present the envelope as a fifth run type.

## 12. Error and safety UX

Errors use stable codes internally and useful human explanations in UI.

Recoverable errors should show concrete next actions.

Examples:

```text
SCHEMA_DRIFT
AMBIGUOUS_PAGE_TYPE
UNRESOLVED_REFERENCE
STORAGE_CRITICAL
CRAWLER_UNAVAILABLE
```

The UI should avoid exposing raw stack traces by default.

A production-breaking `SCHEMA_DRIFT` must route the user toward a new Crawler Draft/Test Lab fix. The UI must not offer a generic `USE_ANYWAY` action that restores trusted complete-snapshot or missing-record semantics.

A robots override control requires a non-empty reason before the run can start. The override state and reason context are visibly distinguishable from default robots-respecting operation.

Settings controls for inheritable values explicitly distinguish **Inherit**, **Custom**, and **Reset to built-in** and show the effective source/value after resolution.

## 13. Testing strategy

### Rust

- domain unit tests;
- persistence/integration tests against Turso;
- migration tests;
- API route/service tests;
- durable job/recovery tests;
- adapter contract tests;
- export/backup/integrity tests.

### Frontend

- focused component tests for complex state;
- accessibility checks for critical surfaces;
- Playwright end-to-end journeys.

### Crawl4AI

PR/normal CI uses deterministic local fixtures and a mocked/stubbed Crawl4AI contract for reproducibility.

Scheduled smoke testing uses a real official Crawl4AI container against local deterministic fixture websites.

Tests MUST NOT depend on arbitrary public websites for correctness.

## 14. Required MVP end-to-end journeys

At minimum verify:

1. first-run Start → Quick Scrape → Review;
2. pasted URL batch → independent ordered Quick Scrape outcomes;
3. direct file URL → Source/Asset handling without HTML extraction;
4. Quick Scrape → Save as Crawler Draft;
5. multi-Seed / multi-Page-Type Draft → Test Lab → Publish;
6. Page Type ambiguity blocks publish;
7. equal-priority Page Types with equal complete specificity key remain `AMBIGUOUS_PAGE_TYPE` regardless of creation/insertion/database order;
8. Discovery Preview contains a cycle without runaway traversal;
9. external URL stays outside Domain Scope;
10. canonicalization prevents tracking-parameter duplicate crawling;
11. Production Run → live SSE → Cancel → recover/resume;
12. robots override cannot start without a reason, and retry/resume preserves the original run reason while a new run requires an explicit reason;
13. Listing + Detail enrich shared Dataset without silent overwrite;
14. schema drift produces diagnostics, blocks trusted complete-snapshot semantics, and requires a new Draft fix;
15. record duplicate candidates are never auto-merged;
16. complete snapshot creates missing candidates, partial snapshot does not;
17. provenance traces an approved value to source/artifact/version;
18. approved-only export + provenance bundle verification;
19. backup → verify → restore;
20. setting inheritance distinguishes Inherit, Custom, and Reset to built-in across Global → Collection → Crawler → Run Profile → per-run precedence;
21. remote bind is rejected without access token;
22. low-storage safety blocks artifact-heavy work without auto-deletion.

## 15. Release verification gate

Before a release is called MVP-complete:

- Rust tests pass;
- frontend tests pass;
- Playwright MVP journeys pass;
- migrations up from supported baseline pass;
- backup/restore verification passes;
- fixture Crawl4AI contract tests pass;
- scheduled/explicit real Crawl4AI smoke passes for the target release candidate;
- documentation links/placeholders are checked;
- no implementation claim is made for roadmap-only capabilities.
