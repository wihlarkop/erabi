# Erabi Roadmap — Post-MVP Milestones

# 4. Milestone 0.2 — Browser Workflow Studio

## Why after MVP

The MVP proves crawler graph semantics first. Arbitrary browser actions introduce a second graph/state-machine dimension and should not destabilize core discovery/versioning before those contracts are real.

## Candidate scope

- visual browser action workflow;
- click;
- fill;
- select/dropdown;
- keypress;
- wait;
- scroll;
- repeat bounded action;
- Load More;
- richer infinite-scroll workflows;
- action test/debug trace;
- reusable action sequence per Page Type/Transition;
- browser interaction provenance.

## Guardrails

- bounded repeats/timeouts;
- no CAPTCHA bypass promises;
- clear separation between authenticated user-owned sessions and access-control circumvention;
- actions versioned inside Crawler Version;
- Test Lab support before production use.

## Exit criteria

A fixture site requiring a Load More button can be modeled visually, tested, versioned, and run reproducibly without custom code.

---

# 5. Milestone 0.3 — Scheduling, Monitoring, and Notification Center

## Candidate scope

- daily/weekly/cron schedules;
- per-Crawler schedules;
- Run Profile selection for schedule;
- pause/resume schedules;
- retry/backoff policy;
- schedule concurrency limits;
- change-only result notifications;
- optional auto-export after successful reviewed/eligible conditions where safely designed;
- in-app Notification Center;
- browser notification history linkage;
- scheduler health diagnostics.

## Important boundary

Automatic approval is **not automatically included** with scheduling. It requires a separate trust policy design because current MVP intentionally keeps humans in the approval loop.

## Exit criteria

A user can schedule a published crawler, understand what config version/profile will run, pause it, inspect failures, and receive durable notifications without changing production config silently.

---

# 6. Milestone 0.4 — Reuse, Portability, and Templates

## Candidate scope

- export/import extraction schema;
- export/import full crawler definition with format versioning;
- Clone Crawler;
- crawler templates;
- local template library;
- shareable `.erabi-crawler` or similar portable definition format after security review;
- schema/version compatibility checks;
- custom export filename;
- Regenerate Export from retained approved dataset version;
- Undo/Redo for Studio draft editing;
- persistent draft edit history.

## Deferred marketplace

A public template marketplace/gallery remains later than local portability. Untrusted imported crawler definitions need a mature permission/security story first.

## Exit criteria

A user can move a crawler definition between two Erabi instances, inspect exactly what the imported definition can crawl, and save it as a local Draft before publish.

---

# 7. Milestone 0.5 — Search, Data Destinations, and Dataset Operations

## Candidate scope

### Search

- Turso-backed full-text search over selected Dataset fields/documents;
- search index status/health;
- Collection/Crawler scoping;
- provenance-aware result navigation.

### Destinations

- PostgreSQL;
- MySQL where worthwhile;
- S3/R2/object storage;
- webhook destinations;
- vector destinations such as Qdrant/Milvus only after clear user workflows exist.

### Database export modes

- Append;
- Upsert;
- explicit conflict-key contract;
- destination migration/version behavior.

### Dataset operations

- richer relationship navigation;
- controlled Source/Crawler movement between Collections with history preserved;
- more field types and transformations;
- file parsing adapters (PDF/CSV/JSON/etc.) as deliberate source types rather than pretending they are HTML pages.

## Exit criteria

Additional destinations do not compromise the same atomicity/provenance/audit rules established in MVP.

---

# 8. Milestone 0.6 — Desktop Erabi (Tauri 2, Windows first)

## Why later

Web/Docker architecture should prove the product before installer/runtime packaging complexity is introduced.

## Candidate scope

- Tauri 2 shell;
- reuse SvelteKit frontend;
- reuse Rust core crates directly where practical;
- Windows installer first;
- local data directory management;
- native file dialogs;
- desktop notification integration;
- controlled Crawl4AI/local-service lifecycle strategy;
- update flow design (still no silent update by default);
- macOS/Linux portability preparation.

## Open design questions for this milestone

- bundle vs externally manage Crawl4AI/browser runtime;
- how local Turso files and backup paths migrate between web/Docker and desktop;
- whether desktop uses OS credential storage while retaining `.env` compatibility for server deployments.

## Exit criteria

A Windows user can install Erabi and run the core local workflow without manually assembling a developer environment.

---

# 9. Milestone 0.7 — Optional AI Copilot

## Principle

AI is a **copilot for the Studio**, not a requirement for crawling and never the authority that silently approves data.

## Candidate capabilities

- “propose a crawler for this site”;
- Page Type suggestions;
- field naming/type suggestions;
- selector explanations;
- normalization suggestions;
- discovery graph suggestions;
- Page Type ambiguity explanation;
- schema drift explanation;
- extraction failure diagnosis;
- natural-language Test Lab assistance.

## Privacy requirements

- BYOK initially;
- provider configuration is optional;
- explicit consent before page content is sent externally;
- clear preview of what data is transmitted;
- no secrets/cookies/auth tokens in prompts;
- AI suggestions always become Draft configuration requiring user review.

## Exit criteria

Disabling AI leaves the entire core product functional.

---

# 10. Milestone 0.8 — Authenticated Crawling

## Candidate scope

- browser cookie/session import;
- interactive user-owned authenticated browser session;
- saved authenticated profiles;
- encrypted credential/session storage design;
- controlled login workflow integration with Browser Workflow Studio;
- session expiry diagnostics.

## Safety boundary

Erabi does not position itself as a protection-bypass product. CAPTCHA/anti-bot/access-control circumvention is not a product goal.

## Exit criteria

A user can intentionally provide a session for a site they are authorized to access and understand where/session data is stored and when it expires.

---

# 11. Milestone 0.9 — Scale, Collaboration, and Extensibility

These capabilities are intentionally last in the `0.x` sequence because they can distort local-first architecture if introduced prematurely.

## Candidate collaboration scope

- account/auth model;
- multi-user actors replacing `local-user` default;
- roles/permissions;
- collaborative draft/review lifecycle;
- audit actor attribution;
- conflict-safe concurrent edits.

## Candidate scale scope

- remote Turso-first deployment patterns;
- split `erabi serve` / `erabi worker`;
- multiple workers;
- distributed leases;
- queue fairness;
- remote artifact/object storage;
- hosted deployment guidance.

## Candidate plugin/adapter scope

- additional crawler engines behind `CrawlerAdapter`;
- destination adapters;
- source/file parsing adapters;
- carefully versioned extension contracts.

A full plugin marketplace is not assumed. Stable extension boundaries must be earned from real adapters first.

## Governance scope

- DCO evaluation/adoption;
- CLA evaluation only if a concrete legal/product need arises;
- maintainer roles;
- release governance;
- security response process maturity.

---

# 12. 1.0 — Stable core contracts

Erabi 1.0 should mean more than “we shipped many features.”

Candidate 1.0 readiness criteria:

- Crawler/Crawler Version contracts have survived real-world evolution;
- Page Type/transition semantics are stable;
- migration/backup formats have documented compatibility policies;
- API deprecation policy is enforced;
- recovery/upgrade paths have real-world evidence;
- export/provenance format versions are stable;
- accessibility baseline is maintained;
- security review covers public-network deployment;
- public docs and API accurately represent behavior;
- no known data-integrity flaw permits silent loss/overwrite of approved data;
- clear path for desktop/hosted users without fragmenting the domain model.

---

# 13. Long-term bets (not committed milestones)

These ideas remain intentionally outside committed `0.x` scope until core workflows validate them.

## 13.1 Full visual graph programming

A drag-and-drop graph editor where users visually construct Page Types, transitions, browser actions, and branches. This is attractive but can become an entire low-code platform; it must stay crawler-specific.

## 13.2 AI-generated crawler drafts

Given one or more seeds, propose Page Types, transitions, selectors, unique keys, canonicalization, and datasets. Always Draft + Test Lab + user publish gate.

## 13.3 Dataset explorer / generated frontend

Generate read-only dataset explorers, catalogs, knowledge bases, or specialized dashboards from approved data.

This must not turn Erabi into a generic application framework.

## 13.4 RAG/knowledge integrations

Export/push approved datasets to RAG systems, vector databases, or generated assistants. RAG is a consumer of trusted Erabi data, not the identity of Erabi itself.

## 13.5 Change-monitoring intelligence

More advanced historical change analytics, anomaly detection, source-health scoring, and drift dashboards built on existing immutable run/version history.

## 13.6 Community crawler/template ecosystem

Share crawler definitions/templates after portable formats, security scanning, permissions, and trust UX mature.

---
