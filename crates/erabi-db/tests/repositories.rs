use std::collections::BTreeMap;

use erabi_db::{
    DbError, ErabiDatabase, MigrationRunner,
    repositories::{CrawlRunRepository, CrawlerRepository},
};
use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType, Crawler,
    CrawlerVersion, ResolvedValue, RobotsAudit, RunConfiguration, Seed, SettingSource,
    SnapshotOperationalSettings,
};

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

fn snapshot_settings() -> SnapshotOperationalSettings {
    SnapshotOperationalSettings {
        max_pages: resolved(100),
        max_depth: resolved(3),
        max_duration_seconds: resolved(60),
        concurrency: resolved(2),
        request_delay_ms: resolved(250),
        timeout_ms: resolved(30_000),
        screenshot: resolved(false),
        asset_download_limit_bytes: resolved(1_000_000),
        retain_artifacts: resolved(true),
        user_agent: resolved("Erabi/0.1".to_owned()),
    }
}

fn quick_snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
    let target_url = "https://example.test/item"
        .parse::<url::Url>()
        .map_err(|error| erabi_domain::SnapshotError::Invalid(error.to_string()))?;
    Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::QuickScrape,
        configuration: RunConfiguration::QuickScrape {
            target_url,
            ad_hoc_configuration: BTreeMap::new(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: snapshot_settings(),
        robots: RobotsAudit::respect(
            "operator",
            "2026-08-23T00:00:00Z",
            "https://example.test",
            "Erabi/0.1",
            None,
        ),
        actor: "operator".into(),
        created_at: "2026-08-23T00:00:00Z".into(),
    })?)
}

#[tokio::test]
async fn repositories_preserve_published_version_and_run_snapshot_immutability()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;

    let crawler_repository = CrawlerRepository::new(&database);
    let mut crawler = Crawler::new("Catalog");
    crawler_repository.create(&crawler).await?;
    let mut version = CrawlerVersion::draft(crawler.id());
    version.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    crawler_repository
        .save_draft(&version, "operator", "2026-08-23T00:00:00Z")
        .await?;
    version.publish()?;
    crawler.reactivate_published(&version)?;
    crawler_repository
        .publish_and_activate(&crawler, &version, "operator", "2026-08-23T00:00:00Z")
        .await?;

    assert_eq!(
        crawler_repository
            .pointers(&crawler)
            .await?
            .active_published_version_id,
        Some(version.id().to_string())
    );
    assert_eq!(
        crawler_repository
            .audit_event_count(&version.id().to_string())
            .await?,
        2
    );
    assert!(matches!(
        crawler_repository
            .save_draft(&version, "operator", "2026-08-23T00:00:00Z")
            .await,
        Err(DbError::Invariant(_))
    ));

    let run_repository = CrawlRunRepository::new(&database);
    let run_id = CrawlRunId::new();
    let snapshot = quick_snapshot()?;
    run_repository
        .create(run_id, CrawlRunStatus::Queued, &snapshot)
        .await?;
    let stored = run_repository.snapshot(run_id).await?;
    assert_eq!(stored.snapshot_hash(), snapshot.snapshot_hash());
    assert_eq!(
        stored.checkpoint_compatibility_hash(),
        snapshot.checkpoint_compatibility_hash()
    );
    Ok(())
}
