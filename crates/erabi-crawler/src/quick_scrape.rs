//! Provider-neutral durable submission semantics for one Quick Scrape URL.
//!
//! This module deliberately stops at durable acceptance. Provider execution is
//! owned by the Plan 04 job runtime, so an accepted HTTP request never waits
//! for a crawler provider or exposes provider-specific identities.

use std::{collections::BTreeMap, sync::Arc};

use erabi_db::{
    ErabiDatabase,
    repositories::{JobKind, JobRepository, JobRepositoryError, NewJob, QuickScrapeRunJob},
};
use erabi_domain::{
    CollectionId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunType, RobotsAudit,
    RunConfiguration, SnapshotError, SnapshotOperationalSettings, SourceTargetType,
};
use serde_json::json;
use url::Url;

use crate::{
    ContentEvidence, ContentProbe, ContentProbeDecision, ContentProbeExecutor, NetworkTargetPolicy,
    SourceIntakeError, SourceIntakeRequest, SourceIntakeService,
};

const QUICK_SCRAPE_JOB_KIND: &str = "QUICK_SCRAPE";

/// Validated inputs that belong to a fresh, independent Quick Scrape run.
/// Robots decisions are supplied by the API's accepted run-safety boundary;
/// crawler orchestration never manufactures or inherits an override reason.
#[derive(Clone, Debug)]
pub struct QuickScrapeSubmissionRequest {
    pub target_url: String,
    pub collection_id: Option<CollectionId>,
    pub source_name: Option<String>,
    pub settings: SnapshotOperationalSettings,
    pub robots: RobotsAudit,
    pub actor: String,
    pub created_at: String,
    pub priority: i32,
    pub max_attempts: u32,
}

/// The durable identities returned after accepted submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuickScrapeSubmission {
    pub run_id: erabi_domain::CrawlRunId,
    pub job_id: String,
    pub source_id: erabi_domain::SourceId,
}

/// Minimal immutable run material needed by the job handler. The Source is
/// intentionally frozen in the Quick Scrape configuration because the MVP
/// `crawl_runs` schema has no mutable source-association column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuickScrapeSnapshotTarget {
    pub target_url: Url,
    pub source_id: String,
    pub source_target_type: SourceTargetType,
}

/// Submission errors are typed at the orchestration boundary and never carry
/// raw provider data or an unredacted URL in their display text.
#[derive(Debug, thiserror::Error)]
pub enum QuickScrapeSubmissionError {
    #[error("Quick Scrape Source intake failed")]
    SourceIntake(#[source] SourceIntakeError),
    #[error("Quick Scrape snapshot was invalid")]
    Snapshot(#[source] SnapshotError),
    #[error("Quick Scrape durable submission failed")]
    Job(#[source] JobRepositoryError),
}

/// Immutable snapshot parsing errors used before a worker can perform network
/// work. Corruption is never replaced with current mutable Settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum QuickScrapeSnapshotError {
    #[error("the run is not a Quick Scrape snapshot")]
    WrongRunType,
    #[error("the Quick Scrape source association is missing or invalid")]
    SourceAssociation,
}

/// One reusable single-URL submission primitive. Task 7 can call this once
/// per batch item later without introducing a batch lifecycle here.
#[derive(Clone)]
pub struct QuickScrapeSubmissionService {
    database: ErabiDatabase,
    network_policy: NetworkTargetPolicy,
    probe: Arc<dyn ContentProbeExecutor>,
}

impl std::fmt::Debug for QuickScrapeSubmissionService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuickScrapeSubmissionService")
            .field("network_policy", &self.network_policy)
            .finish_non_exhaustive()
    }
}

impl QuickScrapeSubmissionService {
    #[must_use]
    pub fn new(database: ErabiDatabase, network_policy: NetworkTargetPolicy) -> Self {
        Self {
            database,
            network_policy,
            probe: Arc::new(ContentProbe::default()),
        }
    }

    /// Test/runtime seam for deterministic bounded Source probing.
    #[must_use]
    pub fn with_probe_executor(mut self, probe: Arc<dyn ContentProbeExecutor>) -> Self {
        self.probe = probe;
        self
    }

    /// Performs Task 4 Source intake, freezes a fresh Quick Scrape snapshot,
    /// and atomically commits the run together with its root job.
    ///
    /// Source identity is deliberately created or reused before the short
    /// submission transaction. The transaction itself never performs DNS,
    /// probing, provider, or artifact work.
    ///
    /// # Errors
    /// Returns typed Source-intake, snapshot, or atomic durable-job failures.
    pub async fn submit(
        &self,
        request: QuickScrapeSubmissionRequest,
        now: i64,
    ) -> Result<QuickScrapeSubmission, QuickScrapeSubmissionError> {
        let intake = SourceIntakeService::with_probe_executor(
            &self.database,
            self.network_policy.clone(),
            Arc::clone(&self.probe),
        )
        .intake(&SourceIntakeRequest {
            original_url: request.target_url,
            collection_id: request.collection_id,
            name: request.source_name,
        })
        .await
        .map_err(QuickScrapeSubmissionError::SourceIntake)?;

        let mut ad_hoc_configuration = BTreeMap::new();
        ad_hoc_configuration.insert("source_id".into(), json!(intake.source.id.to_string()));
        ad_hoc_configuration.insert(
            "source_target_type".into(),
            json!(source_target_type_name(intake.source.target_type)),
        );
        ad_hoc_configuration.insert(
            "source_canonical_url".into(),
            json!(intake.canonical_url.as_str()),
        );
        ad_hoc_configuration.insert(
            "source_intake_classification".into(),
            content_probe_snapshot_value(&intake.decision),
        );
        let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: intake.canonical_url,
                ad_hoc_configuration,
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: request.settings,
            robots: request.robots,
            actor: request.actor,
            created_at: request.created_at,
        })
        .map_err(QuickScrapeSubmissionError::Snapshot)?;
        let job = NewJob::new(
            JobKind::new(QUICK_SCRAPE_JOB_KIND).map_err(QuickScrapeSubmissionError::Job)?,
            request.priority,
            now,
            request.max_attempts,
        )
        .map_err(QuickScrapeSubmissionError::Job)?;
        let QuickScrapeRunJob {
            crawl_run_id,
            job_id,
        } = JobRepository::new(&self.database)
            .create_quick_scrape_run_with_root_job(&snapshot, &job, now)
            .await
            .map_err(QuickScrapeSubmissionError::Job)?;

        Ok(QuickScrapeSubmission {
            run_id: crawl_run_id,
            job_id: job_id.to_string(),
            source_id: intake.source.id,
        })
    }
}

/// Reads only the frozen values a Quick Scrape worker is allowed to use.
///
/// # Errors
/// Returns an invariant error instead of falling back to mutable state when a
/// stored snapshot lacks the source association frozen during submission.
pub fn quick_scrape_snapshot_target(
    snapshot: &CrawlRunSnapshot,
) -> Result<QuickScrapeSnapshotTarget, QuickScrapeSnapshotError> {
    if snapshot.run_type() != CrawlRunType::QuickScrape {
        return Err(QuickScrapeSnapshotError::WrongRunType);
    }
    let RunConfiguration::QuickScrape {
        target_url,
        ad_hoc_configuration,
    } = snapshot.configuration()
    else {
        return Err(QuickScrapeSnapshotError::WrongRunType);
    };
    let source_id = ad_hoc_configuration
        .get("source_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(QuickScrapeSnapshotError::SourceAssociation)?;
    let source_target_type = match ad_hoc_configuration
        .get("source_target_type")
        .and_then(serde_json::Value::as_str)
    {
        Some("WEB_PAGE") => SourceTargetType::WebPage,
        Some("FILE_ASSET") => SourceTargetType::FileAsset,
        _ => return Err(QuickScrapeSnapshotError::SourceAssociation),
    };
    Ok(QuickScrapeSnapshotTarget {
        target_url: target_url.clone(),
        source_id: source_id.to_owned(),
        source_target_type,
    })
}

fn source_target_type_name(value: SourceTargetType) -> &'static str {
    match value {
        SourceTargetType::WebPage => "WEB_PAGE",
        SourceTargetType::FileAsset => "FILE_ASSET",
    }
}

fn content_probe_snapshot_value(decision: &ContentProbeDecision) -> serde_json::Value {
    match decision {
        ContentProbeDecision::NormalWebCrawl => json!({"kind": "NORMAL_WEB_CRAWL"}),
        ContentProbeDecision::FileAsset {
            kind,
            media_type,
            evidence,
        } => json!({
            "kind": "FILE_ASSET",
            "file_kind": direct_file_kind_name(*kind),
            "media_type": media_type,
            "evidence": content_evidence_name(*evidence),
        }),
    }
}

fn direct_file_kind_name(value: crate::DirectFileKind) -> &'static str {
    match value {
        crate::DirectFileKind::Pdf => "PDF",
        crate::DirectFileKind::Csv => "CSV",
        crate::DirectFileKind::Json => "JSON",
        crate::DirectFileKind::Archive => "ARCHIVE",
        crate::DirectFileKind::Image => "IMAGE",
        crate::DirectFileKind::OfficeDocument => "OFFICE_DOCUMENT",
    }
}

fn content_evidence_name(value: ContentEvidence) -> &'static str {
    match value {
        ContentEvidence::ContentType => "CONTENT_TYPE",
        ContentEvidence::Signature => "SIGNATURE",
        ContentEvidence::ContentTypeAndSignature => "CONTENT_TYPE_AND_SIGNATURE",
    }
}
