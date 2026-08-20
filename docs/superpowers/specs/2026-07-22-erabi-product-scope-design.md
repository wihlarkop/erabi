# Erabi Product Scope and Experience Design

**Status:** Approved specification
**Date:** 2026-07-22

## 1. Product Definition

Erabi is an open-source, local-first, no-code web data ingestion and curation application. It helps users transform messy webpages into clean, structured, reviewed, versioned, and auditable data without requiring them to write scraping code.

Erabi is not positioned only as a RAG tool. Its output can feed research, archives, catalogues, analytics, AI systems, databases, or custom applications. RAG assistants and generated frontends remain future capabilities.

### Core promise

> Turn messy websites into trusted data.

### Job to be done

A user should be able to:

1. paste a URL;
2. scrape the page;
3. see live progress;
4. review detected content or repeated records;
5. correct extraction visually;
6. approve only valid data;
7. export clean data with optional provenance; and
8. later recrawl without losing history or silently overwriting approved data.

## 2. Primary Users

Erabi is defined by the task rather than one narrow persona. Typical users include:

- AI and RAG developers preparing source data;
- researchers collecting public web information;
- data teams curating listings, directories, products, articles, or forum content;
- content managers building structured archives;
- technical users who want a reusable visual scraper rather than custom scripts;
- non-programmers who need inspectable, trusted extraction.

The MVP assumes one local operator and does not include account management or collaboration.

## 3. Product Principles

### 3.1 Immediate usefulness

The first screen must tell the user what to do. Erabi opens on the Start page with a prominent URL input. It does not begin with an empty dashboard, configuration wizard, schema builder, or Collection selector.

### 3.2 Progressive complexity

The default path is deliberately small:

```text
Paste URL → Scrape → Live progress → Review
```

Advanced settings remain available but collapsed. Multi-page crawling, link selection, pagination, and schema refinement appear after the first page has been scraped.

### 3.3 Human-controlled trust

Erabi may suggest a mode, container, fields, schema, duplicates, or changes, but it does not silently approve, merge, repair, delete, or export unreviewed records.

### 3.4 Local-first operation

A fresh local installation works without an Erabi account and without an external Erabi service. Turso Cloud, external Crawl4AI endpoints, browser notifications, telemetry, and network exposure are optional.

### 3.5 Provenance as a first-class feature

Every approved field must retain enough provenance to explain where it came from, how it was extracted, and how it was normalized.

## 4. Terminology

### Start

The landing page and fastest entry point for a new scrape.

### Inbox

The default destination for quick sources that are not assigned to a Collection.

### Collection

An optional grouping mechanism for related Sources, Schemas, Destinations, and configuration overrides. A Collection may contain one or multiple domains. It is intentionally lightweight and may be used like a folder.

### Source

The durable identity of a crawl target. A Source is not a specific result; it owns Crawl Runs, Documents or Datasets, Assets, and history.

### Crawl Run

One execution created by Scrape, Crawl, Recrawl, Retry, or Resume. It stores an immutable configuration snapshot and execution history.

### Raw Artifact

Unmodified or derived crawler output such as raw HTML, cleaned HTML, rendered DOM, Markdown, screenshots, logs, and structured crawler metadata.

### Extraction Schema

A reusable, versioned definition describing a container, fields, selectors, normalization, validation, unique keys, URL matching, and pagination behavior.

### Dataset

A reviewed output containing one or more Records. A single-page document is represented as a Dataset with Document mode semantics rather than as an unrelated subsystem.

### Record

One structured item in a Dataset, such as an article, product, comment, directory entry, or the main document content.

### Review

The human workflow for inspecting, editing, validating, approving, or rejecting extracted data.

## 5. Navigation

The primary sidebar is:

```text
Start
Inbox
Collections
Crawl Runs
Schemas
Datasets
Assets
Exports
Settings
```

System status appears at the bottom of the sidebar without becoming a separate primary destination:

- Crawl4AI connection state;
- active and queued job count;
- storage warning state;
- Recovery Mode indicator when applicable.

System Diagnostics remains accessible from Settings, command palette, error states, and `erabi doctor`.

## 6. Start Page

### 6.1 Main input

The Start page centers a large URL input and a single primary action:

```text
[ https://example.com/...                              ] [Scrape]
```

Single-page scraping is the default. The user does not need to choose between Scrape and Crawl before seeing the first page.

### 6.2 Optional controls

Collapsed advanced options may include:

- destination Collection, default Inbox;
- an existing matching Extraction Schema;
- screenshot override;
- wait selector;
- timeout;
- auto-scroll settings;
- User-Agent override;
- rate-limit override;
- Crawl4AI connection selection.

### 6.3 Recent activity

The lower portion of Start shows a concise activity area ordered by required attention:

1. failed or partial Crawl Runs;
2. drafts waiting for review;
3. recently scraped Sources;
4. recent exports.

The area must remain secondary to the URL input.

### 6.4 Automatic first-run setup

There is no blocking onboarding wizard. On first startup, Erabi automatically:

- creates or opens the Local Turso application database;
- creates artifact, asset, export, and backup directories;
- runs migrations;
- verifies storage permissions;
- checks Crawl4AI connectivity;
- opens the Start page.

A non-blocking checklist communicates state:

```text
Getting started
✓ Local database ready
✓ Artifact storage ready
✓ Crawl4AI connected
○ Scrape your first URL
```

If Crawl4AI is unavailable, the application remains usable for existing data, review, export, settings, diagnostics, and backup. New scraping is disabled with a clear recovery action.

## 7. Default Scrape Experience

### 7.1 Source creation

A pasted URL creates a Source in Inbox unless the user selects a Collection. Duplicate URL detection occurs before starting the Crawl Run and offers:

- Open Existing;
- Recrawl Existing;
- Create New Anyway;
- Cancel.

### 7.2 Progress

The Start page transitions into live progress without navigating to a generic job table. A user-friendly step list is always visible, with technical logs collapsed.

### 7.3 Completion

A successful scrape:

1. stores raw artifacts;
2. stores an extracted Draft;
3. detects Document or Records mode;
4. opens the appropriate Review automatically.

The success screen is not a dead end. Secondary actions in Review include:

- Crawl More Pages;
- Select Links;
- Configure or refine Schema;
- Open original page;
- Inspect Crawl Run.

## 8. Document and Records Modes

Erabi analyzes the stored page structure after crawling.

### Document Mode

Optimized for one main item, including:

- articles;
- documentation pages;
- profiles;
- informational pages;
- single-page content.

Suggested fields may include title, URL, main content, publication date, author, and metadata.

### Records Mode

Optimized for repeated containers, including:

- products;
- news listings;
- forum posts or comments;
- tables;
- directories;
- search results;
- cards or repeated content blocks.

### Confidence and manual switching

Erabi selects a recommended mode using a confidence score. When confidence is low, the UI clearly presents both choices. The user can switch modes without recrawling because both modes operate on the same stored raw artifact.

## 9. Global Search and Command Palette

`Ctrl/Cmd + K` opens a combined metadata search and safe-action palette.

### Searchable entities

- Sources by name, URL, domain, and status;
- Collections;
- Schemas;
- Datasets;
- Crawl Runs;
- Exports.

### Quick actions

- Scrape a URL;
- Create Collection;
- Open Inbox;
- View failed runs;
- Resume a cancelled crawl;
- Create backup;
- Run integrity check;
- Open Settings.

Destructive actions such as permanent deletion, restore, and Empty Trash are not executable without their dedicated confirmation screen.

Full-text search across record values, documents, raw HTML, and Markdown is deferred.

## 10. Naming

### Source naming

Erabi names Sources automatically after the first successful crawl using this priority:

1. page title;
2. Open Graph title;
3. domain plus meaningful path;
4. domain;
5. `Untitled Source`.

The user may rename a Source or Dataset after creation. Names are not identities; UUIDv7 remains the identifier, so duplicate display names are allowed.

### Dataset naming

Dataset names combine Source context and detected mode or record type, for example:

- `SCANDAL Announces New Album — Document`;
- `Example Shop — Products`;
- `Forum Thread — Comments`.

### Export filename

MVP filenames are generated automatically and cannot be overridden before export:

```text
{dataset-slug}-{date}-{short-export-id}.{extension}
```

Example:

```text
scandal-news-2026-07-22-019d8f.zip
```

Custom export filenames are deferred.

## 11. Appearance, Language, and Accessibility

### Language

The MVP UI is English-first. All UI copy must use translation keys from the first implementation. Bahasa Indonesia, Japanese, and community translations are deferred.

User-authored names and scraped content are never translated automatically.

### Appearance

The UI supports:

- Follow system, default;
- Light;
- Dark.

### Accessibility

The MVP targets WCAG 2.2 AA and requires:

- complete keyboard navigation;
- visible focus indicators;
- semantic HTML and correct ARIA usage;
- screen-reader labels and restrained live announcements;
- sufficient contrast;
- reduced-motion support;
- no color-only status communication;
- usable layout at 200% zoom.

The visual selector must also expose a keyboard-operable DOM tree and manual selector entry.

## 12. Notifications

Browser notifications are optional and off by default. Erabi asks permission only after the user enables them.

Supported MVP events:

- crawl completed, failed, or partial;
- export completed or failed;
- backup completed or failed;
- integrity check completed or failed.

Notifications do not expose source URLs, extracted content, or secret values. Clicking a notification opens the related Erabi page.

An in-app Notification Center is deferred, but event types and durable event records must be structured so it can be added later.

## 13. Telemetry

Erabi sends no telemetry or crash reports by default. It remains fully functional offline.

Future anonymous analytics or crash reporting must be explicit opt-in, reversible, and prohibited from sending scraped content, source URLs, secrets, configuration values, or raw artifacts.

## 14. MVP Non-Goals

The MVP does not include:

- accounts, teams, or role-based permissions;
- hosted Erabi SaaS;
- automatic AI extraction or schema generation;
- authenticated website crawling;
- action-recording browser workflows;
- automatic schedules;
- generated websites or AI assistants;
- full-text content search;
- schema import/export;
- Source movement between Collections;
- Undo/Redo;
- notification center;
- append/upsert database export;
- automatic software updates.

## 15. Product-Level Acceptance Criteria

The MVP is product-complete when a fresh user can:

1. start Erabi with Docker Compose;
2. open the Start page without onboarding;
3. paste a public URL and see live progress;
4. review a detected document or repeated records;
5. inspect field-level provenance;
6. correct fields visually or manually;
7. approve valid records while invalid records remain Draft;
8. export approved data with optional provenance bundle;
9. recrawl and review only meaningful changes;
10. recover from a stopped job, failed migration, or unavailable Crawl4AI without corrupting approved data.
