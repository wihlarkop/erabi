//! Provider-neutral Production Run submission and frozen-version loading.
//!
//! This module owns durable acceptance only. The `erabi-jobs` root handler
//! owns ordinary bounded execution, while this boundary ensures that handler
//! can address one exact immutable Published `CrawlerVersion` rather than an
//! active pointer, Draft, or "latest" lookup.

use std::collections::BTreeSet;

use erabi_db::{
    ErabiDatabase,
    repositories::{
        CrawlerRepository, CrawlerRepositoryError, CrawlerSemanticSnapshot, JobKind, JobRepository,
        JobRepositoryError, NewJob, ProductionRunJob,
    },
};
use erabi_domain::{
    CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunType, CrawlerId, CrawlerVersionId,
    RobotsAudit, RunConfiguration, SeedId, SnapshotError, SnapshotOperationalSettings,
};

const PRODUCTION_CRAWL_JOB_KIND: &str = "PRODUCTION_CRAWL";
/// Task 8 deliberately permits one total root execution only. Task 9 will
/// introduce durable frontier reconstruction before Production can re-enter.
pub const PRODUCTION_ROOT_MAX_ATTEMPTS: u32 = 1;

/// Validated inputs for one fresh Production Run. The API resolves operational
/// settings and creates the fresh robots audit before crossing this boundary.
#[derive(Clone, Debug)]
pub struct ProductionRunSubmissionRequest {
    pub crawler_id: CrawlerId,
    pub crawler_version_id: CrawlerVersionId,
    /// `None` means every enabled Seed in the immutable version, in its stored
    /// deterministic order. `Some` must contain distinct enabled seed IDs.
    pub selected_seed_ids: Option<Vec<SeedId>>,
    pub settings: SnapshotOperationalSettings,
    pub robots: RobotsAudit,
    pub actor: String,
    pub created_at: String,
    pub priority: i32,
}

/// Durable identities returned after the owning Production Run and root job
/// commit in the same transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductionRunSubmission {
    pub run_id: erabi_domain::CrawlRunId,
    pub job_id: String,
}

/// The frozen identity a worker is allowed to use to reload the exact
/// immutable semantic version. It deliberately contains no active pointers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductionSnapshotIdentity {
    pub crawler_id: CrawlerId,
    pub crawler_version_id: CrawlerVersionId,
    pub semantic_config_hash: String,
}

/// Submission and frozen-snapshot errors are typed at this application
/// boundary. They never contain provider errors, bodies, or credentials.
#[derive(Debug, thiserror::Error)]
pub enum ProductionRunSubmissionError {
    #[error("Production Run CrawlerVersion validation failed")]
    Crawler(#[source] CrawlerRepositoryError),
    #[error("Production Run settings exceed the immutable version guardrails")]
    Guardrails,
    #[error("Production Run has no enabled selected Seeds")]
    NoEnabledSeeds,
    #[error("Production Run selected Seed IDs contain a duplicate")]
    DuplicateSeedSelection,
    #[error("Production Run selected Seed is not owned by the CrawlerVersion")]
    SeedNotOwnedByVersion,
    #[error("Production Run selected Seed is disabled")]
    SeedDisabled,
    #[error("Production Run snapshot was invalid")]
    Snapshot(#[source] SnapshotError),
    #[error("Production Run durable submission failed")]
    Job(#[source] JobRepositoryError),
}

/// Immutable snapshot parse/load failures used by the root worker. A hash
/// mismatch fails closed instead of substituting mutable Crawler state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProductionSnapshotError {
    #[error("the run is not a Production Run snapshot")]
    WrongRunType,
    #[error("the frozen Production Run semantic configuration is invalid")]
    InvalidConfiguration,
    #[error("the exact Published CrawlerVersion no longer matches the frozen snapshot")]
    FrozenConfigurationMismatch,
    #[error("the exact Published CrawlerVersion could not be loaded")]
    Crawler,
}

/// One reusable Production Run acceptance service.
#[derive(Clone, Debug)]
pub struct ProductionRunSubmissionService {
    database: ErabiDatabase,
}

impl ProductionRunSubmissionService {
    #[must_use]
    pub const fn new(database: ErabiDatabase) -> Self {
        Self { database }
    }

    /// Validates one exact Published version, freezes selected Seeds/settings
    /// and robots audit, then atomically persists the run with its root job.
    /// No provider, robots fetch, DNS, artifact, or network operation occurs
    /// inside the durable transaction.
    ///
    /// # Errors
    /// Returns a typed version, seed, snapshot, guardrail, or durable-job
    /// error. No partial run/job pair is committed on a durable error.
    pub async fn submit(
        &self,
        request: ProductionRunSubmissionRequest,
        now: i64,
    ) -> Result<ProductionRunSubmission, ProductionRunSubmissionError> {
        let semantic = CrawlerRepository::new(&self.database)
            .published_semantic_snapshot(request.crawler_id, request.crawler_version_id)
            .await
            .map_err(ProductionRunSubmissionError::Crawler)?;
        semantic
            .version
            .guardrails()
            .validate_effective_operational_limits(&erabi_domain::ResolvedOperationalLimits {
                max_pages: request.settings.max_pages.value,
                max_depth: request.settings.max_depth.value,
                max_duration_seconds: request.settings.max_duration_seconds.value,
                max_downloaded_bytes: semantic.version.guardrails().max_downloaded_bytes,
                concurrency: request.settings.concurrency.value,
                request_delay_ms: request.settings.request_delay_ms.value,
            })
            .map_err(|_| ProductionRunSubmissionError::Guardrails)?;
        let selected_seed_ids = select_seed_ids(&semantic, request.selected_seed_ids.as_deref())?;
        let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::ProductionRun,
            configuration: RunConfiguration::CrawlerVersion {
                crawler_id: request.crawler_id,
                crawler_version_id: request.crawler_version_id,
                semantic_config_hash: semantic.config_hash,
            },
            selected_seed_ids,
            run_profile_id: None,
            settings: request.settings,
            robots: request.robots,
            actor: request.actor,
            created_at: request.created_at,
        })
        .map_err(ProductionRunSubmissionError::Snapshot)?;
        let job = NewJob::new(
            JobKind::new(PRODUCTION_CRAWL_JOB_KIND).map_err(ProductionRunSubmissionError::Job)?,
            request.priority,
            now,
            PRODUCTION_ROOT_MAX_ATTEMPTS,
        )
        .map_err(ProductionRunSubmissionError::Job)?;
        let ProductionRunJob {
            crawl_run_id,
            job_id,
        } = JobRepository::new(&self.database)
            .create_production_run_with_root_job(&snapshot, &job, now)
            .await
            .map_err(ProductionRunSubmissionError::Job)?;
        Ok(ProductionRunSubmission {
            run_id: crawl_run_id,
            job_id: job_id.to_string(),
        })
    }
}

/// Reads the exact immutable identity that a Production worker is allowed to
/// use; no active or latest `CrawlerVersion` pointer participates.
///
/// # Errors
/// Returns an error if this is not a valid frozen Production snapshot.
pub fn production_snapshot_identity(
    snapshot: &CrawlRunSnapshot,
) -> Result<ProductionSnapshotIdentity, ProductionSnapshotError> {
    if snapshot.run_type() != CrawlRunType::ProductionRun {
        return Err(ProductionSnapshotError::WrongRunType);
    }
    let RunConfiguration::CrawlerVersion {
        crawler_id,
        crawler_version_id,
        semantic_config_hash,
    } = snapshot.configuration()
    else {
        return Err(ProductionSnapshotError::WrongRunType);
    };
    if semantic_config_hash.len() != 64
        || !semantic_config_hash
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(ProductionSnapshotError::InvalidConfiguration);
    }
    Ok(ProductionSnapshotIdentity {
        crawler_id: *crawler_id,
        crawler_version_id: *crawler_version_id,
        semantic_config_hash: semantic_config_hash.clone(),
    })
}

/// Reloads only the immutable Published version addressed by a frozen run and
/// proves its semantic hash still matches. This guards durable corruption while
/// allowing a worker to construct the canonical Plan 05 semantic primitives.
///
/// # Errors
/// Returns an error if the snapshot is invalid, the exact version cannot be
/// read as Published, or its immutable semantic hash differs.
pub async fn load_frozen_production_semantics(
    database: &ErabiDatabase,
    snapshot: &CrawlRunSnapshot,
) -> Result<CrawlerSemanticSnapshot, ProductionSnapshotError> {
    let identity = production_snapshot_identity(snapshot)?;
    let semantic = CrawlerRepository::new(database)
        .published_semantic_snapshot(identity.crawler_id, identity.crawler_version_id)
        .await
        .map_err(|_| ProductionSnapshotError::Crawler)?;
    if semantic.config_hash != identity.semantic_config_hash {
        return Err(ProductionSnapshotError::FrozenConfigurationMismatch);
    }
    Ok(semantic)
}

fn select_seed_ids(
    semantic: &CrawlerSemanticSnapshot,
    requested: Option<&[SeedId]>,
) -> Result<Vec<SeedId>, ProductionRunSubmissionError> {
    let requested = match requested {
        Some(ids) => {
            let mut distinct = BTreeSet::new();
            if ids.iter().any(|id| !distinct.insert(id.to_string())) {
                return Err(ProductionRunSubmissionError::DuplicateSeedSelection);
            }
            Some(distinct)
        }
        None => None,
    };
    let mut selected = Vec::new();
    for seed in semantic.version.seeds() {
        if requested
            .as_ref()
            .is_none_or(|ids| ids.contains(&seed.id.to_string()))
        {
            if !seed.enabled {
                return Err(ProductionRunSubmissionError::SeedDisabled);
            }
            selected.push(seed.id);
        }
    }
    if let Some(requested) = requested
        && selected.len() != requested.len()
    {
        return Err(ProductionRunSubmissionError::SeedNotOwnedByVersion);
    }
    if selected.is_empty() {
        return Err(ProductionRunSubmissionError::NoEnabledSeeds);
    }
    Ok(selected)
}
