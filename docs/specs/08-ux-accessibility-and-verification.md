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
- matched Page Types and specificity rationale;
- extraction preview;
- selector coverage;
- discovered links/Transition result;
- warnings/errors;
- saved Test Evidence.

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

## 12. Error UX

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
2. pasted URL batch;
3. direct file URL → Asset handling;
4. Quick Scrape → Save as Crawler Draft;
5. multi-Seed / multi-Page-Type Draft → Test Lab → Publish;
6. Page Type ambiguity blocks publish;
7. Discovery Preview contains a cycle without runaway traversal;
8. external URL stays outside Domain Scope;
9. canonicalization prevents tracking-parameter duplicate crawling;
10. Production Run → live SSE → Cancel → recover/resume;
11. Listing + Detail enrich shared Dataset without silent overwrite;
12. schema drift produces diagnostics and requires a new Draft fix;
13. record duplicate candidates are never auto-merged;
14. complete snapshot creates missing candidates, partial snapshot does not;
15. provenance traces an approved value to source/artifact/version;
16. approved-only export + provenance bundle verification;
17. backup → verify → restore;
18. remote bind is rejected without access token;
19. low-storage safety blocks artifact-heavy work without auto-deletion.

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
