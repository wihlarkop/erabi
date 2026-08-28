use erabi_db::{
    DbError, ErabiDatabase, MigrationRunner,
    repositories::{CrawlRunRepository, CrawlerRepository},
};
use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType, Crawler,
    CrawlerId, CrawlerVersion, ResolvedValue, RobotsAudit, RunConfiguration, Seed, SettingSource,
    SnapshotOperationalSettings,
};

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

fn settings() -> SnapshotOperationalSettings {
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

#[tokio::test]
async fn fresh_repository_connection_enforces_crawler_foreign_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let orphan_draft = CrawlerVersion::draft(CrawlerId::new());

    assert!(matches!(
        repository
            .save_draft(&orphan_draft, "operator", "2026-08-23T00:00:00Z")
            .await,
        Err(DbError::Turso(_))
    ));
    Ok(())
}

#[tokio::test]
async fn persisted_crawler_lifecycle_keeps_an_unrelated_draft_active()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Catalog");
    repository.create(&crawler).await?;

    let mut draft_a = CrawlerVersion::draft(crawler.id());
    draft_a.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    repository
        .save_draft(&draft_a, "operator", "2026-08-23T00:00:00Z")
        .await?;
    assert_eq!(
        repository.pointers(&crawler).await?.active_draft_version_id,
        Some(draft_a.id().to_string())
    );

    draft_a.publish()?;
    repository
        .publish_and_activate(&crawler, &draft_a, "operator", "2026-08-23T00:01:00Z")
        .await?;
    let after_publish = repository.pointers(&crawler).await?;
    assert_eq!(
        after_publish.active_published_version_id,
        Some(draft_a.id().to_string())
    );
    assert_eq!(after_publish.active_draft_version_id, None);

    let draft_b = draft_a.draft_from_published()?;
    repository
        .save_draft(&draft_b, "operator", "2026-08-23T00:02:00Z")
        .await?;
    assert_eq!(
        repository.pointers(&crawler).await?.active_draft_version_id,
        Some(draft_b.id().to_string())
    );

    repository
        .reactivate_published(&crawler, &draft_a, "operator", "2026-08-23T00:03:00Z")
        .await?;
    let reactivated = repository.pointers(&crawler).await?;
    assert_eq!(
        reactivated.active_published_version_id,
        Some(draft_a.id().to_string())
    );
    assert_eq!(
        reactivated.active_draft_version_id,
        Some(draft_b.id().to_string())
    );
    Ok(())
}

#[tokio::test]
async fn crawl_run_created_audit_preserves_exact_robots_override_context()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Catalog");
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
    crawler_repository
        .publish_and_activate(&crawler, &version, "operator", "2026-08-23T00:01:00Z")
        .await?;

    let override_reason = "Documented permission from site owner";
    let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::ProductionRun,
        configuration: RunConfiguration::CrawlerVersion {
            crawler_id: crawler.id(),
            crawler_version_id: version.id(),
            semantic_config_hash: "a".repeat(64),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots: RobotsAudit::override_with_reason(
            override_reason,
            "operator",
            "2026-08-23T00:02:00Z",
            "https://example.test/catalog",
            "Erabi/0.1",
            Some(version.id()),
        )?,
        actor: "operator".into(),
        created_at: "2026-08-23T00:02:00Z".into(),
    })?;

    let run_id = CrawlRunId::new();
    let runs = CrawlRunRepository::new(&database);
    runs.create(run_id, CrawlRunStatus::Queued, &snapshot)
        .await?;
    let payload = runs.created_audit_payload(run_id).await?;
    assert_eq!(payload["robots"]["decision"], "OVERRIDE");
    assert_eq!(payload["robots"]["reason"], override_reason);
    assert_eq!(payload["actor"], "operator");
    assert_eq!(payload["decision_at"], "2026-08-23T00:02:00Z");
    assert_eq!(payload["affected_scope"], "https://example.test/catalog");
    assert_eq!(payload["user_agent"], "Erabi/0.1");
    assert_eq!(payload["crawler_id"], crawler.id().to_string());
    assert_eq!(payload["crawler_version_id"], version.id().to_string());

    let respect_snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::QuickScrape,
        configuration: RunConfiguration::QuickScrape {
            target_url: "https://example.test/item".parse()?,
            ad_hoc_configuration: std::collections::BTreeMap::new(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "2026-08-23T00:03:00Z",
            "https://example.test",
            "Erabi/0.1",
            None,
        ),
        actor: "operator".into(),
        created_at: "2026-08-23T00:03:00Z".into(),
    })?;
    let respect_run_id = CrawlRunId::new();
    runs.create(respect_run_id, CrawlRunStatus::Queued, &respect_snapshot)
        .await?;
    let respect_payload = runs.created_audit_payload(respect_run_id).await?;
    assert_eq!(respect_payload["robots"]["decision"], "RESPECT");
    assert!(respect_payload["robots"].get("reason").is_none());
    Ok(())
}
