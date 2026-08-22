use std::collections::BTreeMap;

use erabi_domain::{
    CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunType, CrawlerId, CrawlerVersionId,
    ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotError,
    SnapshotOperationalSettings,
};

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::GlobalSetting,
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

fn quick_scrape_draft() -> Result<CrawlRunSnapshotDraft, SnapshotError> {
    Ok(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::QuickScrape,
        configuration: RunConfiguration::QuickScrape {
            target_url: "https://example.test/products/1"
                .parse::<url::Url>()
                .map_err(|error| SnapshotError::Invalid(error.to_string()))?,
            ad_hoc_configuration: BTreeMap::new(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "2026-08-23T00:00:00Z",
            "https://example.test",
            "Erabi/0.1",
            None,
        ),
        actor: "operator".into(),
        created_at: "2026-08-23T00:00:00Z".into(),
    })
}

#[test]
fn crawl_snapshot_hash_is_canonical_and_deterministic() -> Result<(), SnapshotError> {
    let mut first = quick_scrape_draft()?;
    let mut second = quick_scrape_draft()?;
    if let RunConfiguration::QuickScrape {
        ad_hoc_configuration,
        ..
    } = &mut first.configuration
    {
        ad_hoc_configuration.insert("beta".into(), serde_json::json!(2));
        ad_hoc_configuration.insert("alpha".into(), serde_json::json!(1));
    }

    if let RunConfiguration::QuickScrape {
        ad_hoc_configuration,
        ..
    } = &mut second.configuration
    {
        ad_hoc_configuration.insert("alpha".into(), serde_json::json!(1));
        ad_hoc_configuration.insert("beta".into(), serde_json::json!(2));
    }

    assert_eq!(
        CrawlRunSnapshot::new(first)?.snapshot_hash(),
        CrawlRunSnapshot::new(second)?.snapshot_hash()
    );
    Ok(())
}

#[test]
fn crawl_snapshot_does_not_adopt_later_settings_changes() -> Result<(), SnapshotError> {
    let original = CrawlRunSnapshot::new(quick_scrape_draft()?)?;
    let original_hash = original.snapshot_hash().to_owned();
    let mut changed = quick_scrape_draft()?;
    changed.settings.max_pages.value = 1;
    changed.settings.max_pages.source = SettingSource::PerRunOverride;

    let later = CrawlRunSnapshot::new(changed)?;
    assert_eq!(original.settings().max_pages.value, 100);
    assert_eq!(original.snapshot_hash(), original_hash);
    assert_ne!(original.snapshot_hash(), later.snapshot_hash());
    Ok(())
}

#[test]
fn crawl_snapshot_retry_and_resume_reuse_the_same_immutable_identity() -> Result<(), SnapshotError>
{
    let run_snapshot = CrawlRunSnapshot::new(quick_scrape_draft()?)?;
    let retry_snapshot = run_snapshot.clone();
    let resume_snapshot = run_snapshot.clone();

    assert_eq!(retry_snapshot.snapshot_hash(), run_snapshot.snapshot_hash());
    assert_eq!(
        resume_snapshot.checkpoint_compatibility_hash(),
        run_snapshot.checkpoint_compatibility_hash()
    );
    Ok(())
}

#[test]
fn crawl_snapshot_requires_an_explicit_robots_override_reason_for_each_new_run() {
    let result = RobotsAudit::override_with_reason(
        "   ",
        "operator",
        "2026-08-23T00:00:00Z",
        "https://example.test",
        "Erabi/0.1",
        None,
    );
    assert!(matches!(result, Err(SnapshotError::Invalid(_))));
}

#[test]
fn crawl_snapshot_records_auditable_crawler_version_robots_override() -> Result<(), SnapshotError> {
    let crawler_id = CrawlerId::new();
    let crawler_version_id = CrawlerVersionId::new();
    let robots = RobotsAudit::override_with_reason(
        "Documented permission from site owner",
        "operator",
        "2026-08-23T00:00:00Z",
        "https://example.test/catalog",
        "Erabi/0.1",
        Some(crawler_version_id),
    )?;
    let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::ProductionRun,
        configuration: RunConfiguration::CrawlerVersion {
            crawler_id,
            crawler_version_id,
            semantic_config_hash: "a".repeat(64),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots,
        actor: "operator".into(),
        created_at: "2026-08-23T00:00:00Z".into(),
    })?;

    assert_eq!(snapshot.robots().actor(), "operator");
    assert_eq!(
        snapshot.robots().affected_scope(),
        "https://example.test/catalog"
    );
    assert_eq!(
        snapshot.robots().crawler_version_id(),
        Some(crawler_version_id)
    );
    Ok(())
}
