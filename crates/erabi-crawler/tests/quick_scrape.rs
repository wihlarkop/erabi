use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use erabi_crawler::{
    ContentEvidence, ContentProbeDecision, ContentProbeExecutor, DirectFileKind,
    NetworkTargetPolicy, QuickScrapeSubmissionRequest, QuickScrapeSubmissionService,
    StaticNetworkResolver, ValidatedNetworkTarget, quick_scrape_snapshot_target,
};
use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{CrawlRunRepository, JobRepository},
};
use erabi_domain::{
    CrawlRunType, ResolvedValue, RobotsAudit, SettingSource, SnapshotOperationalSettings,
    SourceTargetType,
};

#[derive(Clone)]
struct FixedProbe(ContentProbeDecision);

impl ContentProbeExecutor for FixedProbe {
    fn probe<'probe>(
        &'probe self,
        _target: &'probe ValidatedNetworkTarget,
    ) -> erabi_crawler::ContentProbeFuture<'probe> {
        let decision = self.0.clone();
        Box::pin(async move { decision })
    }
}

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn policy() -> NetworkTargetPolicy {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443);
    NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::single(
        "example.test",
        address,
    )))
}

fn settings() -> SnapshotOperationalSettings {
    fn resolved<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }
    SnapshotOperationalSettings {
        max_pages: resolved(1),
        max_depth: resolved(0),
        max_duration_seconds: resolved(60),
        concurrency: resolved(1),
        request_delay_ms: resolved(250),
        timeout_ms: resolved(30_000),
        screenshot: resolved(false),
        asset_download_limit_bytes: resolved(1_000_000),
        retain_artifacts: resolved(true),
        user_agent: resolved("Erabi/0.1".to_owned()),
    }
}

fn request(target_url: &str) -> Result<QuickScrapeSubmissionRequest, erabi_domain::SnapshotError> {
    Ok(QuickScrapeSubmissionRequest {
        target_url: target_url.to_owned(),
        collection_id: None,
        source_name: None,
        settings: settings(),
        robots: RobotsAudit::override_with_reason(
            "operator approved this isolated exploration",
            "operator",
            "unix:100",
            "https://example.test:443",
            "Erabi/0.1",
            None,
        )?,
        actor: "operator".to_owned(),
        created_at: "unix:100".to_owned(),
        priority: 0,
        max_attempts: 3,
    })
}

#[tokio::test]
async fn one_submission_creates_one_quick_run_snapshot_and_root_job()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let service = QuickScrapeSubmissionService::new(database.clone(), policy())
        .with_probe_executor(Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl)));

    let accepted = service
        .submit(request("https://example.test/path")?, 100)
        .await?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot(accepted.run_id)
        .await?;
    let target = quick_scrape_snapshot_target(&snapshot)?;
    let job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let job = JobRepository::new(&database).job(&job_id).await?;

    assert_eq!(snapshot.run_type(), CrawlRunType::QuickScrape);
    assert_eq!(target.source_id, accepted.source_id.to_string());
    assert_eq!(target.source_target_type, SourceTargetType::WebPage);
    assert_eq!(snapshot.settings().user_agent.value, "Erabi/0.1");
    assert_eq!(
        job.crawl_run_id.as_deref(),
        Some(accepted.run_id.to_string().as_str())
    );
    assert!(snapshot.selected_seed_ids().is_empty());
    assert!(snapshot.run_profile_id().is_none());
    Ok(())
}

#[tokio::test]
async fn independent_submissions_reuse_source_but_never_deduplicate_runs()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let service = QuickScrapeSubmissionService::new(database, policy())
        .with_probe_executor(Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl)));

    let first = service
        .submit(request("https://example.test/path?utm_source=first")?, 100)
        .await?;
    let second = service
        .submit(request("https://example.test/path")?, 101)
        .await?;

    assert_eq!(first.source_id, second.source_id);
    assert_ne!(first.run_id, second.run_id);
    assert_ne!(first.job_id, second.job_id);
    Ok(())
}

#[tokio::test]
async fn confident_file_asset_is_frozen_without_scheduling_html_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let service = QuickScrapeSubmissionService::new(database.clone(), policy())
        .with_probe_executor(Arc::new(FixedProbe(ContentProbeDecision::FileAsset {
            kind: DirectFileKind::Pdf,
            media_type: Some("application/pdf".to_owned()),
            evidence: ContentEvidence::ContentType,
        })));

    let accepted = service
        .submit(request("https://example.test/report.pdf")?, 100)
        .await?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot(accepted.run_id)
        .await?;
    let target = quick_scrape_snapshot_target(&snapshot)?;

    assert_eq!(target.source_target_type, SourceTargetType::FileAsset);
    assert!(matches!(
        snapshot.configuration(),
        erabi_domain::RunConfiguration::QuickScrape { .. }
    ));
    Ok(())
}
