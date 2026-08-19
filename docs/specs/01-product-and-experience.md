# Product and Experience Specification

## 1. Purpose

Erabi exists to make serious crawling and extraction work approachable without hiding the operational reality of crawling. A user should be able to start with a single URL in seconds, then progressively reveal the controls needed to build a reusable crawler.

Erabi is neither a one-shot scraping API wrapper nor a generic application framework. Its job is to take web sources through a trustworthy acquisition lifecycle:

```text
Web → Discover → Crawl → Extract → Inspect → Curate → Approve → Export
```

## 2. Target job-to-be-done

Erabi serves anyone who needs to turn public webpages into clean, structured, trusted, auditable data without writing and maintaining custom crawler orchestration code. Typical use cases include research collections, product catalogs, forum archives, documentation indexes, content monitoring, AI/RAG ingestion preparation, competitive research, public directory extraction, and dataset creation.

The product should not require the user to identify as a “data engineer” or “scraping engineer.” The interface should explain crawler concepts visually and incrementally.

## 3. Start experience

The default landing page is **Start**, not Inbox, a dashboard, or a project picker.

The hero interaction is a prominent URL input and one primary action:

```text
Paste a URL
→ Scrape
→ Live progress
→ Draft auto-saved
→ Review opens automatically
```

Advanced configuration is secondary and collapsible. A first-time user should not need to understand Collections, Page Types, transitions, schemas, concurrency, or Turso.

### 3.1 Quick Scrape

Quick Scrape is a first-class run type. It MUST work without creating a reusable Crawler.

Quick Scrape stores the same evidence and operational metadata as other runs:

- immutable resolved run configuration;
- raw crawl artifacts;
- logs and progress events;
- extraction output;
- review state;
- provenance;
- status and error summaries.

After a useful Quick Scrape, Erabi SHOULD offer **Save as Crawler**, allowing the ad-hoc work to become a reusable Crawler draft without forcing that step up front.

### 3.2 Automatic mode selection

After scraping, Erabi analyzes the page and recommends one of two review modes:

- **Document Mode** for a primary content document such as an article, profile, or documentation page;
- **Records Mode** for repeated containers such as products, comments, listings, table rows, directory items, or forum posts.

The recommendation is confidence-based. The user can switch modes without recrawling because both modes operate from stored raw/rendered artifacts.

## 4. Crawler Studio experience

A reusable Crawler opens into a Studio workspace rather than a generic details page.

Recommended information architecture:

```text
Crawler: Example Catalog

Overview
Studio
├── Graph
├── Seeds
├── Page Types
├── Discovery
├── Extraction
└── Test Lab

Runs
Data
Assets
Export
Settings
```

The MVP graph view is inspectable and visually central, but configuration editing remains deterministic through forms/panels. A full drag-and-drop node programming environment is deferred.

### 4.1 Crawler Overview

The Overview acts as a command center and SHOULD surface:

- active published version;
- current draft version, if any;
- Page Type and transition counts;
- validation / warning summary;
- latest test evidence;
- crawler health indicators;
- Crawl4AI connection status;
- most recent production run summary;
- actions for Run Crawler, Test Draft, Discovery Preview, and Edit Draft.

## 5. Main navigation

MVP global navigation:

```text
Start
Crawlers
Collections
Runs
Datasets
Assets
Exports
Settings
```

`Schemas` is intentionally not a global primary navigation item in the Crawler Studio design. Extraction configuration belongs to Page Types within a Crawler Version. Cross-crawler schema libraries are a roadmap capability.

### 5.1 Recent activity

Start includes a compact recent activity region under the primary URL input. It SHOULD prioritize actionable items:

1. failed or partial runs;
2. drafts awaiting review;
3. recent sources/crawlers;
4. recent successful exports.

The activity section must not compete visually with the URL input.

## 6. First-run behavior

There is no blocking onboarding wizard.

Erabi initializes what it safely can, then exposes a non-blocking checklist:

```text
Getting started
✓ Local database ready
✓ Artifact storage ready
● Connecting to Crawl4AI
○ Scrape your first URL
```

A disconnected Crawl4AI service does not prevent the UI, existing data, diagnostics, or Settings from opening. Crawl actions are disabled with a clear connection status.

## 7. Collections

A Collection is an optional organizational workspace/folder for grouping related Crawlers and data. It may provide shared defaults such as retention, operational settings, export destinations, and tags.

Collections are intentionally lightweight. A user may use Erabi without creating one.

Moving an existing Crawler/Source between Collections is deferred until lifecycle behavior is intentionally designed; MVP should not invent silent migration semantics.

## 8. Global search and command palette

`Ctrl/Cmd + K` opens a combined metadata search and safe command palette.

Search covers at minimum:

- Crawlers;
- Collections;
- Crawl Runs;
- Datasets;
- Sources/URLs metadata;
- Exports.

Safe commands may include:

- Scrape a URL;
- Create Crawler;
- Create Collection;
- Open failed runs;
- Resume latest recoverable run;
- Create backup;
- Run integrity check;
- Open Settings.

Destructive actions such as permanent deletion, restore backup, or empty Trash require their dedicated confirmation flows and are not executed directly from the command palette.

Full-text search over extracted content is roadmap.

## 9. Naming

Erabi automatically names Sources and Datasets so a scrape never blocks on naming input.

Source naming priority:

1. page title;
2. Open Graph title;
3. useful domain + path representation;
4. domain;
5. `Untitled Source` fallback.

Dataset names derive from source/crawler context and Page Type. Names are display labels, not identities; duplicates are allowed because entities use UUIDv7.

Export filenames are generated automatically in MVP using a safe cross-platform pattern such as:

```text
{dataset-slug}-{date}-{short-export-id}
```

Custom export filename editing is roadmap.

## 10. Archive, Trash, and deletion

Erabi distinguishes three lifecycle actions:

- **Archive** — remove from active views without deleting data;
- **Move to Trash** — mark for potential later permanent deletion; related active jobs are disabled;
- **Permanent Delete** — explicit destructive deletion after impact analysis and typed/name confirmation.

Trash retention defaults to 30 days, but automatic cleanup is **OFF** by default.

Permanent deletion MUST display estimated impact and references. Audit evidence that deletion occurred is retained even after the deleted payload is gone.

## 11. Product anti-goals

MVP does not attempt to be:

- a generic CRUD/admin builder;
- a generic dashboard framework;
- a general-purpose relational schema designer;
- a browser automation replacement for Playwright;
- a distributed cloud crawler fleet;
- an anti-bot/CAPTCHA bypass product;
- a credential harvesting tool;
- a mandatory AI product;
- a hosted service requirement.

## 12. Localization and appearance

MVP UI is English-first, but all application copy uses localization keys from the beginning. Bahasa Indonesia and Japanese are roadmap languages.

Appearance supports:

- Follow system — default;
- Light;
- Dark.

Theme and locale preferences are ordinary settings stored in the application database.
