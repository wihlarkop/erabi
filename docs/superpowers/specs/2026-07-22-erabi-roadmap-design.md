# Erabi Roadmap and Deferred Capabilities Design

**Status:** Approved roadmap boundary
**Date:** 2026-07-22

## 1. Roadmap Purpose

This document records capabilities deliberately excluded from the MVP. They are not forgotten requirements and must not be implemented opportunistically while building foundational tasks.

The roadmap is ordered by dependency and product value, not by a promised release date.

## 2. Phase 1: MVP Foundation

The frozen MVP consists of the capabilities defined in the other design specifications:

- local-first Docker Compose deployment;
- Start page URL input;
- single-page public crawling through unmodified Crawl4AI;
- pagination detection and user-confirmed scope;
- durable queue, retry, cancellation, checkpoint, and SSE progress;
- Document and Records modes;
- visual extraction and Schema Versions;
- Review, validation, approval, rejection, versioning, diff, and provenance;
- JSON, JSONL, CSV, Markdown, SQLite, and Turso exports;
- provenance ZIP bundle;
- selected asset downloads;
- backup, restore, encryption, diagnostics, migrations, Recovery Mode, and integrity checks;
- security hardening, accessibility, and structured logging.

## 3. Phase 2: Workflow Convenience

Candidates after the MVP is stable:

- Source movement between Collections with historical ownership rules;
- schema JSON import/export;
- custom export filename;
- Regenerate Export from an existing Dataset Version;
- Undo/Redo and persistent Draft edit history;
- in-app Notification Center;
- richer batch URL import through CSV, JSON, and JSONL;
- sitemap XML and RSS/Atom ingestion;
- saved filter and view presets;
- full-text search across approved Record content using Turso FTS;
- scheduled crawl and scheduled retention cleanup UI;
- optional automatic backup defaults for selected deployment profiles.

## 4. Phase 3: Advanced Crawling

- browser cookie/session import;
- interactive authenticated browser sessions;
- encrypted saved browser profiles;
- reusable browser action workflows;
- click, fill, dropdown, keypress, wait, scroll, and repeat steps;
- visual action recording;
- Load More and infinite-scroll workflows;
- cursor/API pagination;
- authenticated workflow diagnostics;
- file source adapters for PDF, JSON, CSV, office documents, and images;
- OCR and document parsing integrations.

Erabi will not implement CAPTCHA bypass or access-control circumvention.

## 5. Phase 4: Automation and Multi-Instance Operation

- schedules using daily, weekly, or cron expressions;
- retry/backoff policies;
- change-only runs;
- notifications and auto-export;
- carefully controlled optional auto-approval;
- separate `erabi worker` deployment;
- Turso remote shared operation;
- distributed job leasing;
- adaptive concurrency and resource management;
- multi-device synchronization.

## 6. Phase 5: Additional Destinations

- PostgreSQL;
- MySQL;
- S3-compatible object storage;
- Cloudflare R2;
- webhooks;
- Qdrant;
- Milvus;
- Dify;
- RAGFlow;
- other connector adapters based on demonstrated user demand.

Database Append and Upsert modes belong in this phase and require explicit unique-key conflict semantics and destination capability contracts.

## 7. Phase 6: Optional AI Assistance

AI features remain optional, explicit, and BYOK.

Possible capabilities:

- field naming;
- type inference;
- Schema generation;
- normalization suggestions;
- container detection;
- pagination suggestions;
- schema-drift explanation;
- extraction failure explanation;
- computed fields;
- generated dataset summaries.

Before sending any page content externally, Erabi must show the provider, exact data scope, and receive explicit consent. Local heuristic extraction remains available without AI.

## 8. Phase 7: Generated Experiences

After data is curated and approved, Erabi may generate:

- dataset explorer;
- catalogue;
- knowledge base;
- search interface;
- AI assistant;
- custom dashboard;
- embeddable API or frontend package.

These features consume approved Dataset Versions and provenance; they do not bypass the curation workflow.

## 9. Phase 8: Desktop Distribution

- Tauri 2 Windows installer;
- managed local process lifecycle;
- local data-directory selection;
- desktop notifications;
- optional Crawl4AI sidecar/install management;
- macOS packaging;
- Linux packaging;
- OS keychain evaluation if `.env` becomes unsuitable for desktop users.

The Rust core and SvelteKit frontend are designed for this phase from the beginning, but web Docker distribution remains the MVP priority.

## 10. Phase 9: Accounts and Collaboration

- user accounts;
- teams/workspaces;
- role-based permissions;
- per-user approval actors;
- review assignments;
- comments;
- activity feed;
- access-controlled Sources and Collections;
- hosted Erabi service.

The existing `actor` fields and append-only audit events prepare for this without exposing incomplete account features in MVP.

## 11. Phase 10: Open-Source Governance

- DCO sign-off evaluation;
- CLA evaluation only if legally required;
- maintainer roles;
- contribution review policy;
- release governance;
- plugin/adapter compatibility policy;
- community translation process.

Apache-2.0 is already fixed for the project license.

## 12. Deferred Field Types and Transformations

Potential additions:

- email and phone;
- currency and percentage;
- enum/category;
- list/array;
- nested object;
- file URL;
- location and coordinates;
- rating;
- regular-expression extraction;
- reusable transformer pipeline;
- computed field;
- AI-generated field;
- defaults and null policies;
- richer cross-field validation.

## 13. Roadmap Admission Rule

A roadmap item moves into an implementation specification only after:

1. a concrete user need is identified;
2. its dependencies are stable;
3. its data and security implications are understood;
4. a focused design session is completed;
5. it receives its own specification and implementation plan.
