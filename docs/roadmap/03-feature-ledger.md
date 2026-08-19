# Erabi Roadmap — Deferred Feature Ledger

# 14. Explicitly deferred feature ledger

This ledger prevents earlier design ideas from disappearing or accidentally re-entering MVP without a deliberate decision.

| Feature | Target direction |
|---|---|
| Scheduler/cron | 0.3 |
| Notification Center | 0.3 |
| Browser click/fill/action workflows | 0.2 |
| Load More / advanced infinite scroll | 0.2 |
| Authenticated sessions/login workflows | 0.8 |
| CAPTCHA/access-control bypass | Not a product goal |
| Full drag-and-drop graph editor | Long-term / after 0.2 evidence |
| Schema import/export | 0.4 |
| Crawler clone/templates | 0.4 |
| Public template marketplace | Long-term |
| Undo/Redo & persistent draft edit history | 0.4 |
| Custom export filename | 0.4 |
| Regenerate Export | 0.4 |
| Append/Upsert export | 0.5 |
| PostgreSQL/MySQL destinations | 0.5 |
| S3/R2/webhook destinations | 0.5 |
| Full-text search of datasets | 0.5 |
| Move existing Crawler/Source between Collections | 0.5 design |
| PDF/CSV/JSON parsing as source adapters | 0.5+ |
| More advanced field types/transforms | 0.5+ |
| Bahasa Indonesia/Japanese UI translations | post-MVP |
| Tauri desktop Windows | 0.6 |
| macOS/Linux desktop packaging | after Windows path is stable |
| AI copilot/BYOK | 0.7 |
| Distributed workers | 0.9 |
| Multi-user/team auth | 0.9 |
| Plugin marketplace | long-term |
| DCO/CLA governance | 0.9/governance maturity |
| Generated frontend/RAG assistant | long-term |
| Auto-update | no silent auto-update; revisit UX later |
| Automatic destructive cleanup | remains opt-in policy |
| Automatic approval | requires separate trust-policy design |

---

# 15. How roadmap items graduate

A deferred feature should move toward implementation only when these questions have satisfactory answers:

1. **User value:** what real workflow is blocked without it?
2. **Domain fit:** does it strengthen crawling/extraction/curation rather than turn Erabi into another product category?
3. **Data integrity:** can it preserve immutable versions, provenance, and audit rules?
4. **Safety:** what new network/browser/file/security risks does it create?
5. **Local-first impact:** can local self-hosted users still run Erabi without hosted dependencies?
6. **Operational cost:** does it introduce a new service/daemon/queue/storage system that is justified?
7. **Testing:** can it be verified deterministically with local fixtures/mocks?
8. **Migration:** how does existing user data/config upgrade safely?
9. **Accessibility:** does the interaction have a non-visual/keyboard-accessible path?
10. **Scope:** can it be shipped as a bounded milestone rather than a hidden platform rewrite?

---

# 16. Next planning step

The **implementation plan is intentionally not part of this roadmap revision yet**.

After the Crawler Studio public specification receives another stability pass, a new implementation plan should be written from scratch or heavily regenerated. The previous pre-Studio implementation plan must not be treated as authoritative because the primary domain changed from Source-centric scraping to versioned Crawler Studio workflows.

The future implementation plan should be split by subsystem/phase, with a master index, exact file/interface contracts, tests, verification gates, and explicit spec commit reference.
