# Erabi Extraction, Curation, and Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement safe visual extraction owned by Page Types, schema-drift diagnostics, Dataset/Record review, immutable approvals, semantic change candidates, relationships, and field-level provenance.

**Architecture:** Extraction definitions are embedded in Draft/Published CrawlerVersion PageTypes rather than independently approved global schemas. Raw artifacts remain immutable evidence; curated record versions are separate.

**Tech Stack:** Rust HTML parsing/sanitization, CSS selectors, Serde, Turso, Axum APIs.

**Spec:** `docs/specs/04-extraction-curation-and-provenance.md`, `03-discovery-graph-and-runs.md`, `08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Safe preview and extraction editor backend

- [ ] Sanitize untrusted HTML; remove scripts/events/forms/navigation escapes/unsafe schemes and isolate preview.
- [ ] Produce deterministic node map and keyboard/manual-selector equivalent data.
- [ ] Document/Records mode suggestion is deterministic/local and switchable without recrawl.

### Task 2: PageType extraction definitions and typed extraction

- [ ] Draft PageType owns container, fields, relative selectors, fallbacks, value sources, normalization, validation, unique key, Dataset mapping.
- [ ] Support MVP field types Text/RichText/Number/Boolean/DateTime/URL/ImageURL/RawHTML.
- [ ] Store raw and normalized values separately; no silent locale inference.
- [ ] Shared Dataset compatibility checks block conflicting field/identity semantics.

### Task 3: Schema drift diagnostics

- [ ] Detect missing container/required selector, coverage drop, type mismatch, record-count anomaly, unique-key failure, structural divergence.
- [ ] Production-breaking drift marks run non-complete/partial diagnostic and cannot produce missing candidates.
- [ ] No production `USE_ANYWAY` escape; correction requires new Crawler Draft, tests, publish.
- [ ] Test/Discovery/Quick Scrape may inspect diagnostics without mutating Published config or auto-approving.

### Task 4: Review, validation, and immutable versions

- [ ] ERROR blocks approval and cannot be overridden; WARNING remains approvable.
- [ ] Implement draft autosave with optimistic concurrency, Approve All Valid, rejection, Close/Reopen.
- [ ] Approved record versions immutable; edits create new draft versions.

### Task 5: Change detection and candidates

- [ ] Compare normalized values by explicit field comparison policy.
- [ ] Complete healthy production snapshot only: same/new/updated/missing candidate semantics.
- [ ] Partial/failed/cancelled/test/discovery/drift-invalid runs never create missing candidates.
- [ ] Reappearance of confirmed deleted identity creates `RESTORED_CANDIDATE`.

### Task 6: Provenance and relationships

- [ ] Field provenance traces Source URL, canonical URL, Crawler/Version, Run, PageType, transition path, artifact, selector, raw/normalized value, transformations, time.
- [ ] Dataset relationships surface `UNRESOLVED_REFERENCE` without generic ORM/cascade framework.

**Gate:** Listing+Detail shared Dataset never silently overwrites; drift requires Draft fix; duplicate candidates never auto-merge; complete vs partial missing semantics and provenance trace pass.
