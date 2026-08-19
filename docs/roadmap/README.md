# Erabi Roadmap

**Status:** Public product roadmap, 2026-08-20

This roadmap is intentionally more detailed than a feature wishlist. It records **what belongs in MVP, what is explicitly deferred, why it is deferred, what it depends on, and what evidence should exist before a milestone is considered ready**.

It is not a promise of calendar dates. Milestone order may change as real crawler workloads expose better priorities.

## 1. Roadmap principles

1. **Studio before ecosystem.** Erabi must first become excellent at designing, testing, operating, and curating crawlers locally.
2. **Correctness before automation.** Human-reviewed provenance, immutable versions, and complete-snapshot semantics come before scheduled auto-approval or autonomous AI.
3. **No hidden crawler magic.** Detection and recommendation are welcome; silent traversal, merge, approval, repair, or scope expansion are not.
4. **Local-first remains viable.** Hosted/distributed capabilities must not make local Docker/desktop users second-class.
5. **Crawl4AI remains an adapter.** Erabi benefits from Crawl4AI without forking or duplicating its crawler/browser engine.
6. **Product boundary stays narrow.** Erabi is not a generic CRUD/admin/dashboard framework.
7. **Roadmap features earn their complexity.** Features graduate when a real workflow is blocked, safety can be explained, migration is clear, and deterministic testing exists.

## 2. Current stage — specification stabilization

### Goal

Freeze the public Crawler Studio contracts before writing a new implementation plan.

### Deliverables

- public product positioning and UX contract;
- Crawler / Crawler Version domain;
- multiple Seeds;
- multiple Page Types;
- Page Type matcher conflict resolution;
- Discovery Transition graph;
- cycle guardrails;
- URL canonicalization;
- Domain Scope Policy;
- Draft/Published lifecycle;
- Test Lab and Test Evidence;
- Discovery Preview;
- Run Profiles and temporary per-run overrides;
- shared Dataset semantics;
- field-level merge/provenance;
- Dataset relationships;
- runtime/reliability/security/export contracts;
- detailed public roadmap.

### Exit criteria

- no known contradiction between public specs;
- MVP vs roadmap boundaries are explicit;
- no pre-Crawler-Studio implementation plan is treated as current;
- a future implementation plan references the exact spec commit it was derived from.

## 3. Roadmap documents

- [MVP 0.1 — Crawler Studio](01-mvp-0.1.md)
- [Post-MVP milestones 0.2 → 1.0](02-post-mvp.md)
- [Deferred feature ledger and graduation rules](03-feature-ledger.md)
