use erabi_crawler::{
    ProductionRunSubmissionError, ProductionRunSubmissionRequest, ProductionRunSubmissionService,
    production_snapshot_identity,
};
use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{CrawlerRepository, CrawlerRepositoryError},
};
use erabi_domain::{
    CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunType, Crawler, CrawlerId, CrawlerVersionId,
    ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings,
};

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn settings() -> SnapshotOperationalSettings {
    fn resolved<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }
    SnapshotOperationalSettings {
        max_pages: resolved(10),
        max_depth: resolved(3),
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

fn request(crawler_id: CrawlerId, version_id: CrawlerVersionId) -> ProductionRunSubmissionRequest {
    ProductionRunSubmissionRequest {
        crawler_id,
        crawler_version_id: version_id,
        selected_seed_ids: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "unix:1",
            "crawler scope",
            "Erabi/0.1",
            Some(version_id),
        ),
        actor: "operator".to_owned(),
        created_at: "unix:1".to_owned(),
        priority: 0,
    }
}

#[tokio::test]
async fn draft_and_wrong_owner_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = CrawlerRepository::new(&database);
    let crawler_a = Crawler::new("A");
    let crawler_b = Crawler::new("B");
    repository.create(&crawler_a).await?;
    repository.create(&crawler_b).await?;
    let draft_a = repository
        .create_draft(crawler_a.id(), "operator", "unix:1")
        .await?;
    let draft_b = repository
        .create_draft(crawler_b.id(), "operator", "unix:1")
        .await?;
    let service = ProductionRunSubmissionService::new(database);

    assert!(matches!(
        service
            .submit(request(crawler_a.id(), draft_a.id()), 1)
            .await,
        Err(ProductionRunSubmissionError::Crawler(
            CrawlerRepositoryError::VersionNotPublished
        ))
    ));
    assert!(matches!(
        service
            .submit(request(crawler_a.id(), draft_b.id()), 1)
            .await,
        Err(ProductionRunSubmissionError::Crawler(
            CrawlerRepositoryError::VersionNotOwnedByCrawler
        ))
    ));
    Ok(())
}

#[test]
fn frozen_identity_uses_only_the_exact_version_and_hash() -> Result<(), Box<dyn std::error::Error>>
{
    let crawler_id = CrawlerId::new();
    let version_id = CrawlerVersionId::new();
    let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::ProductionRun,
        configuration: RunConfiguration::CrawlerVersion {
            crawler_id,
            crawler_version_id: version_id,
            semantic_config_hash: "a".repeat(64),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "unix:1",
            "crawler scope",
            "Erabi/0.1",
            Some(version_id),
        ),
        actor: "operator".to_owned(),
        created_at: "unix:1".to_owned(),
    })?;
    let identity = production_snapshot_identity(&snapshot)?;
    assert_eq!(identity.crawler_id, crawler_id);
    assert_eq!(identity.crawler_version_id, version_id);
    assert_eq!(identity.semantic_config_hash, "a".repeat(64));
    Ok(())
}
