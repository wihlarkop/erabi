# Extraction, Curation, and Provenance Specification

## 1. Extraction model

Erabi uses one guided visual extraction model rather than several competing builders.

For repeated data:

1. select one representative container;
2. Erabi suggests fields inside the container using deterministic/local heuristics;
3. user corrects, removes, adds, or manually points fields to elements;
4. Erabi finds similar containers;
5. live record preview updates;
6. user maps output to a Dataset and saves the Page Type draft configuration.

For Document Mode, the same field model applies to a single primary document rather than repeated containers.

MVP does not require AI assistance to identify fields.

## 2. Visual extraction editor

Desktop layout is a three-panel Studio experience:

```text
┌────────────────────────┬──────────────────────────┬────────────────┐
│ Page Preview           │ Field Configuration      │ Record Preview │
│ sanitized DOM          │ selectors/types/rules   │ Grid / Cards   │
└────────────────────────┴──────────────────────────┴────────────────┘
```

On narrow layouts, the panels become accessible tabs.

### 2.1 Bidirectional highlighting

Preview, fields, and records are linked:

- select a field → highlight its source element(s);
- select a preview element → focus the mapped field or offer field creation;
- select a record → highlight its container;
- hover/focus a selector → highlight all matches and announce match count accessibly.

## 3. Safe preview rendering

Raw website HTML is untrusted and never rendered directly into the main application DOM.

Preview pipeline:

```text
Crawl4AI rendered/raw artifact
→ sanitize
→ remove active scripts/inline handlers
→ neutralize forms/navigation
→ rewrite or constrain resource references
→ add Erabi selection metadata
→ isolated sandbox preview
```

The application must block dangerous URL schemes such as `javascript:` and prevent the preview from escaping into top-level navigation.

Raw artifacts remain immutable evidence even though the rendered preview is sanitized.

## 4. Selector model

MVP uses CSS selectors as the primary selector format.

Field selectors are relative to the selected Page Type container whenever a container exists.

Selector generation preference:

1. stable semantic ID when clearly non-generated;
2. semantic class;
3. stable `data-*` / `aria-*` attributes;
4. semantic element structure;
5. positional selectors only as a last resort.

Fragile selectors such as heavy `:nth-child()` usage produce warnings.

The schema may preserve:

- primary selector;
- fallback selectors;
- structural fingerprint/signature for diagnostics;
- example match evidence.

Automatic selector repair is not MVP.

## 5. Field types

MVP field types:

- Text;
- Rich Text;
- Number;
- Boolean;
- Date/Time;
- URL;
- Image URL;
- Raw HTML.

Roadmap types include email/phone, currency/percentage, enum/category, list/array, nested object, file URL, coordinates, rating, regex-derived values, computed/AI fields, and richer validation.

## 6. Field value sources

A field may read from:

- text content;
- inner HTML;
- outer HTML where explicitly allowed;
- element attribute;
- resolved absolute URL from an attribute;
- Boolean element/attribute presence.

Every extracted field preserves raw and normalized values separately where normalization changes representation.

Example conceptual output:

```json
{
  "price_raw": "Rp 1.250.000",
  "price": 1250000
}
```

The public dataset does not need to duplicate every `_raw` field; provenance storage owns raw-value evidence.

## 7. Local heuristic suggestion

Initial field suggestions are local and deterministic. Heuristics may use:

- semantic HTML tags;
- class/id/attribute names such as title, price, date, image, description;
- link and image attributes;
- ARIA/data attributes;
- repeated structural patterns.

The UI shows coverage counts, for example `24/24`, and never presents heuristics as guaranteed truth.

AI field/schema assistance is post-MVP and must remain optional/BYOK with explicit consent before page content leaves the local Erabi boundary.

## 8. Dataset mapping

By default, each Page Type that produces records maps to one primary Dataset. One Crawler may therefore produce many Datasets.

```text
Crawler
├── Product Listing → Product Listings Dataset
├── Product Detail  → Products Dataset
└── Reviews         → Reviews Dataset
```

A Page Type may also be non-extracting/discovery-only.

## 9. Shared Datasets

Multiple Page Types may write to the same Dataset when they represent the same entity identity and pass compatibility validation.

Example:

```text
Dataset: Products
├── Product Listing → title, price, url
└── Product Detail  → title, price, description, sku, images
```

Publish validation for a shared Dataset checks at minimum:

- compatible unique-key contract;
- no conflicting field types;
- compatible required/optional semantics;
- compatible normalization semantics;
- same identity meaning.

An incompatible shared Dataset mapping blocks publish. Erabi does not invent an automatic merge contract.

## 10. Unique keys

Unique keys are mandatory for reliable multi-run identity when a Dataset participates in change detection.

A key may be:

- one field;
- composite ordered fields.

Configuration options may include trimming, case sensitivity, URL normalization, empty-value behavior, and composite order.

When no explicit key is available for a one-off draft, content hashing may provide duplicate hints, but it is not a substitute for an explicit identity contract when the user wants reliable recrawl lifecycle tracking.

Unique-key configuration is versioned with the Crawler Version/Page Type Dataset mapping.

## 11. Field-level merge

Shared Datasets use field-level candidate merging, never silent overwrite.

For a record identity:

```text
field
├── current approved value (optional)
└── candidate values
    ├── value
    ├── Page Type
    ├── source URL
    ├── Crawl Run
    ├── selector
    ├── raw value
    └── normalized value
```

Behavior:

- existing value missing + candidate value → enrichment candidate;
- existing value equals normalized candidate → no meaningful change;
- existing value differs → `FIELD_CONFLICT`/updated candidate;
- multiple differing candidates → preserve all candidates until resolution.

## 12. Source preference per field

A Dataset may define optional preferred Page Type sources per field.

Example:

```text
price
1. Product Detail
2. Product Listing
```

Preference only ranks/recommends candidates. It MUST NOT:

- auto-approve;
- silently overwrite;
- discard non-preferred candidates;
- alter provenance.

The Review UI marks the preferred candidate clearly while preserving user choice.

## 13. Validation

Validation has two severities:

### ERROR

- blocks approval;
- cannot be overridden during approval;
- must be corrected through data editing or an explicit schema/config version change.

Examples: missing required field, invalid declared type, invalid/empty required unique key, identity duplicate violating the Dataset contract.

### WARNING

- visible and filterable;
- does not block approval;
- does not require a second confirmation solely because it is a warning.

Examples: unusually short description, missing optional image, low selector coverage, unusual value distribution.

## 14. Review lifecycle

Review state is separate from Dataset/record approval state.

Review lifecycle:

- `OPEN`;
- `CLOSED`;
- `CLOSED_WITH_UNRESOLVED_ITEMS`;
- `REOPENED`.

A user may close a review containing drafts/errors after a clear unresolved-item summary and explicit confirmation. Closing does not mutate record states.

Recrawls create new review work when meaningful candidates exist; they do not automatically reopen old closed reviews.

## 15. Record lifecycle

Relevant record/candidate states include:

- DRAFT;
- NEW_CANDIDATE;
- UPDATED_CANDIDATE;
- APPROVED;
- REJECTED;
- MISSING_CANDIDATE;
- DELETED;
- RESTORED_CANDIDATE;
- SUPERSEDED version history where applicable.

Approved versions are immutable.

Editing approved data requires creating a new draft version derived from manual edit, recrawl candidate, or cloned prior version.

## 16. Bulk approval and rejection

`Approve All Valid`:

- approves valid records;
- skips records with validation ERROR;
- allows records with warnings;
- leaves skipped records in draft/candidate state;
- may result in a `PARTIALLY_APPROVED` Dataset/review summary.

Single-record rejection reason is optional.

Bulk rejection requires a reason. Preset reasons may include irrelevant, duplicate, incomplete, incorrect extraction, out of scope, and other/custom.

Rejected records are not erased; their evidence and provenance remain.

## 17. Semantic change detection

Meaningful record change is based on normalized extracted field values, not raw HTML equality.

Schema/field configuration may define comparison behavior such as:

- normal comparison;
- whitespace-normalized comparison;
- canonical URL comparison;
- ignore in change detection.

Raw artifact changes that do not alter meaningful normalized fields are stored as evidence according to retention policy but do not automatically create user review work.

## 18. Recrawl candidates

Given a healthy complete production snapshot:

- existing key + same normalized values → unchanged;
- existing key + changed values → `UPDATED_CANDIDATE`;
- new key → `NEW_CANDIDATE`;
- previously approved key absent → `MISSING_CANDIDATE`.

`MISSING_CANDIDATE` is prohibited when the run is partial, failed, cancelled, test-only, discovery-only, or otherwise not a trustworthy complete snapshot.

Missing candidates are human-reviewed. User actions may include mark deleted, keep active, ignore this run, recrawl, or inspect source evidence.

If a previously confirmed deleted identity reappears, Erabi creates `RESTORED_CANDIDATE` rather than silently reactivating it.

## 19. Dataset relationships

MVP supports domain-specific relationships between extracted Datasets using field/key references.

Example:

```text
Reviews.product_id → Products.product_id
```

Relationship validation can surface `UNRESOLVED_REFERENCE` warnings without deleting or blocking unrelated data by default.

The Review/Data UI may navigate from one record to related records.

This is not a generic relational database schema builder. MVP does not expose arbitrary cascades, ORM semantics, generic resource relationships, or admin CRUD generation.

## 20. Field-level provenance

Provenance is mandatory for curated field values.

At minimum, provenance can answer:

- which source URL;
- which original/canonical URL;
- which Crawler and Crawler Version;
- which Crawl Run;
- which Page Type;
- which transition/discovery path when relevant;
- which artifact;
- which element/selector;
- raw value;
- normalized value;
- applied transformations;
- extraction timestamp.

The Review UI provides a provenance drawer from a field/cell with actions such as highlight source element, inspect artifact, open original source, copy selector, view run/log context, and compare previous version.

## 21. Provenance durability

Retention cleanup may remove large raw artifacts, but approved curated records must retain minimum durable provenance metadata sufficient to establish source, run, crawler version, selector, value lineage, artifact hash/reference summary, and audit history.

Provenance sidecars in export are specified separately in the export specification.
