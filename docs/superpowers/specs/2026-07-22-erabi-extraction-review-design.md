# Erabi Extraction and Review Experience Design

**Status:** Approved specification
**Date:** 2026-07-22

## 1. Extraction Philosophy

Erabi’s primary extraction interaction is visual point-and-click within a constrained model:

1. choose one main container in Records mode;
2. detect and configure fields relative to that container;
3. find similar containers;
4. preview the resulting Records;
5. save and approve a Schema Version.

The MVP does not provide an unrestricted arbitrary graph of selections across unrelated page regions. This constraint keeps selectors understandable, reusable, and testable.

Document mode uses one main document container and the same field machinery.

## 2. Review Entry

A successful single-page scrape automatically stores a Draft and opens Review.

Erabi selects Document or Records mode based on structural analysis and confidence. The selected mode is visible and can be switched without recrawling.

## 3. Three-Panel Editor

Desktop layout:

```text
┌────────────────────────┬────────────────────────┬────────────────────────┐
│ Page Preview           │ Field Configuration    │ Record Preview         │
│                        │                        │                        │
│ sanitized page         │ container              │ Grid / Cards           │
│ hover/select           │ fields and coverage    │ records and validation │
│ match highlights       │ types and rules        │ provenance access      │
└────────────────────────┴────────────────────────┴────────────────────────┘
```

On narrow screens:

```text
Preview | Fields | Records
```

The layout is a baseline and may be visually refined after real usage, but the three responsibilities remain distinct.

## 4. Safe Page Preview

Erabi does not embed the public URL directly because websites may deny framing and because crawled content is untrusted.

Pipeline:

```text
Crawl4AI rendered DOM
→ Erabi sanitization
→ stored preview artifact
→ isolated sandbox document
```

Sanitization requirements:

- remove scripts and inline event handlers;
- block `javascript:` and unsafe URL schemes;
- disable form submission;
- disable top-level navigation;
- disable external embeds by default;
- prevent access to the Erabi application origin;
- add internal element identifiers used only for selection mapping;
- preserve a mapping to the original DOM selector and structural fingerprint.

Raw HTML remains stored for evidence but is never rendered directly in the main application DOM.

## 5. Bidirectional Highlighting

Preview, Fields, and Records are linked:

- selecting a Field highlights its source element;
- selecting an element focuses its mapped Field;
- selecting a Record highlights its source container;
- hovering a selector highlights every matching element;
- opening provenance can jump to the exact source element.

Highlighting always has a text equivalent for accessibility.

## 6. Container Selection

In Records mode, the user selects one candidate item. Erabi identifies structurally similar elements and shows:

- selected element summary;
- candidate selector;
- number of similar matches;
- coverage;
- parent/child alternatives;
- selector stability warning.

User actions:

- select parent;
- select child;
- narrow selector;
- broaden selector;
- ignore individual matches;
- enter or edit CSS manually;
- test selector.

The MVP uses CSS selectors as the primary format. XPath is deferred.

## 7. Selector Quality

Selector preference order:

1. stable, meaningful ID;
2. semantic class;
3. meaningful `data-*` or `aria-*` attribute;
4. semantic element structure;
5. positional selectors only as a last resort.

Erabi stores:

- primary CSS selector;
- optional fallback CSS selectors;
- structural fingerprint;
- example element signature.

Generated-looking IDs, excessive nesting, and `:nth-child` selectors produce warnings.

Field selectors are relative to the selected container.

## 8. Field Detection

After a container is selected, local heuristics suggest fields using:

- semantic HTML tags;
- class and ID names;
- `data-*` and `aria-*` attributes;
- common label concepts such as title, link, image, date, price, description, author, and rating;
- attribute types such as `href`, `src`, `datetime`, and `content`.

No page content is sent to an AI provider in the MVP.

Each field displays:

- name;
- type;
- relative selector;
- value source;
- sample values;
- coverage count;
- required/optional state;
- normalization;
- validation state;
- change-detection behavior.

## 9. MVP Field Types

- Text;
- Rich Text;
- Number;
- Boolean;
- Date/Time;
- URL;
- Image URL;
- Raw HTML.

Raw and normalized values are preserved separately.

Example:

```json
{
  "price_raw": "Rp 1.250.000",
  "price": 1250000
}
```

## 10. Value Sources

A field may derive its value from:

- text content;
- inner HTML;
- outer HTML;
- an attribute;
- an attribute resolved to an absolute URL;
- boolean presence.

For URL and Image URL fields, relative values are resolved against the Source URL while preserving the raw attribute value in provenance.

## 11. Live Record Preview

Every container, selector, type, or normalization change updates Record Preview.

The frontend debounces transient edits and cancels stale preview requests. The Rust backend extracts from the stored sanitized or rendered DOM artifact. Large result sets return paginated previews.

A preview response includes:

- sample Records;
- total detected count;
- coverage per field;
- errors and warnings;
- duplicate or empty unique keys;
- selector stability warnings;
- estimated diff from the currently approved Schema Version.

## 12. Manual Corrections

The user can:

- rename a field;
- change field type;
- mark required or optional;
- select another source element;
- enter a CSS selector;
- change value source or attribute;
- add or remove a field;
- configure trimming and normalization;
- configure validation;
- select the unique key;
- ignore a field in semantic change detection;
- exclude a Record from the current Draft preview.

## 13. Schema Draft Autosave

Temporary editor state autosaves as a Draft after a debounce interval.

Visible states:

```text
Editing
Saving
Saved
Save failed — Retry
```

Autosave never approves a Schema Version.

There is no Undo/Redo in the MVP. The UI warns before navigation when unsaved changes remain after a persistent save failure.

## 14. Review Views

### Grid view, default

- rows are Records;
- columns are fields;
- inline editing;
- sorting and filtering;
- validation indicators;
- missing-value indicators;
- multi-select;
- per-cell provenance access;
- keyboard cell navigation.

### Card view, optional

Suitable for article, product, media, directory, and profile-like Records. It uses the same Dataset and state transitions.

## 15. Provenance Drawer

Every field or cell exposes:

- source page;
- source element;
- selector;
- raw value;
- normalized value;
- transformations;
- Schema Version;
- Crawl Run;
- extraction timestamp;
- artifact hash.

Actions:

- highlight source element;
- copy selector;
- open original URL;
- open raw HTML or rendered DOM;
- open Crawl Run and technical logs;
- compare with previous approved value.

## 16. Draft Editing

Draft cell changes autosave with debounce and optimistic concurrency.

Bulk edits are recorded as one auditable operation. Undo/Redo and persistent Draft edit history are deferred.

Approved versions remain locked. Editing an approved Record begins `Create New Version` and produces a Draft.

## 17. Validation UX

Errors block approval and cannot be overridden. Warnings do not block approval and do not require an extra confirmation.

Users can filter:

- all Records;
- valid Records;
- errors;
- warnings;
- Draft;
- Approved;
- Rejected;
- new, updated, missing, or restored candidates.

An error links to the exact field and its rule.

## 18. Approval

Supported actions:

- Approve Record;
- Approve Selected;
- Approve All Valid;
- Reject Record;
- Reject Selected.

`Approve All Valid` reports:

- approved count;
- skipped error count;
- warning count;
- resulting Dataset state.

Approval uses an immutable version transition and optimistic concurrency.

## 19. Rejection

Single rejection reason is optional. Bulk rejection reason is required.

Rejected Records remain searchable in the Dataset and keep provenance and raw history.

## 20. Diff Review

A changed Record displays field-level differences:

- previous approved value;
- new raw and normalized value;
- transformation changes;
- provenance for both versions.

User actions:

- accept all new fields;
- accept selected fields;
- keep selected old fields;
- reject the new version;
- enter an optional reason.

A new approved version supersedes the previous approved version atomically.

## 21. Missing and Restored Review

`MISSING_CANDIDATE` appears only from a complete snapshot. The Review clearly distinguishes “not found in this complete crawl” from “deleted”.

Actions:

- Mark Deleted;
- Keep Active;
- Ignore This Run;
- Recrawl Again;
- Open Source.

`RESTORED_CANDIDATE` displays the deletion history and requires approval before returning to active state.

## 22. Closing Review

A Review can be closed or reopened without changing Record states.

If unresolved items exist, Erabi displays counts and requires confirmation. The state becomes `CLOSED_WITH_UNRESOLVED_ITEMS`.

A new recrawl may create a new Review while an older Review remains closed.

## 23. Accessibility Requirements

### Visual selector

- keyboard-operable DOM tree;
- focusable candidate elements;
- textual selector descriptions;
- manual selector input;
- highlight announcements that do not overwhelm screen readers.

### Grid

- predictable keyboard navigation;
- announced row, column, and validation state;
- sort/filter controls with labels;
- Card view as an alternative representation.

### Live updates

- polite announcements for major state transitions only;
- technical log events are not individually announced;
- reduced motion disables nonessential transitions and pulsing highlights.
