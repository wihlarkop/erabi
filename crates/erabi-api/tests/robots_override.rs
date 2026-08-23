use std::collections::BTreeMap;

use erabi_api::{
    RobotsDecisionContext, RobotsOverrideInput, new_run_robots_decision,
    reuse_frozen_robots_decision,
};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlRunRepository};
use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType,
    MAX_ROBOTS_OVERRIDE_REASON_CHARS, ResolvedValue, RobotsDecision, RunConfiguration,
    SettingSource, SnapshotOperationalSettings,
};

fn context() -> RobotsDecisionContext {
    RobotsDecisionContext {
        actor: "local-operator".to_owned(),
        decided_at: "2026-08-23T12:00:00Z".to_owned(),
        affected_scope: "https://example.test".to_owned(),
        user_agent: "Erabi/0.1".to_owned(),
        crawler_version_id: None,
    }
}

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

fn snapshot(
    robots: erabi_domain::RobotsAudit,
) -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
    Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::QuickScrape,
        configuration: RunConfiguration::QuickScrape {
            target_url: "https://example.test/".parse()?,
            ad_hoc_configuration: BTreeMap::new(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: SnapshotOperationalSettings {
            max_pages: resolved(1),
            max_depth: resolved(0),
            max_duration_seconds: resolved(30),
            concurrency: resolved(1),
            request_delay_ms: resolved(0),
            timeout_ms: resolved(1_000),
            screenshot: resolved(false),
            asset_download_limit_bytes: resolved(0),
            retain_artifacts: resolved(false),
            user_agent: resolved("Erabi/0.1".to_owned()),
        },
        robots,
        actor: "local-operator".to_owned(),
        created_at: "2026-08-23T12:00:00Z".to_owned(),
    })?)
}

#[test]
fn blank_and_overlong_override_reasons_are_rejected() {
    assert!(
        new_run_robots_decision(
            RobotsOverrideInput::Override {
                reason: " ".to_owned()
            },
            context()
        )
        .is_err()
    );
    assert!(
        new_run_robots_decision(
            RobotsOverrideInput::Override {
                reason: "r".repeat(MAX_ROBOTS_OVERRIDE_REASON_CHARS + 1)
            },
            context()
        )
        .is_err()
    );
}

#[test]
fn same_run_reuses_the_frozen_reason_but_new_runs_do_not_inherit_it()
-> Result<(), Box<dyn std::error::Error>> {
    let existing = snapshot(new_run_robots_decision(
        RobotsOverrideInput::Override {
            reason: "operator approved this test target".to_owned(),
        },
        context(),
    )?)?;
    assert_eq!(reuse_frozen_robots_decision(&existing), existing.robots());

    let independent = new_run_robots_decision(RobotsOverrideInput::Respect, context())?;
    assert!(matches!(independent.decision(), RobotsDecision::Respect));
    assert!(matches!(
        existing.robots().decision(),
        RobotsDecision::Override { reason } if reason == "operator approved this test target"
    ));
    Ok(())
}

#[tokio::test]
async fn durable_created_audit_contains_the_full_frozen_robots_context()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let run_id = CrawlRunId::new();
    let run = snapshot(new_run_robots_decision(
        RobotsOverrideInput::Override {
            reason: "operator approved this test target".to_owned(),
        },
        context(),
    )?)?;
    let repository = CrawlRunRepository::new(&database);
    repository
        .create(run_id, CrawlRunStatus::Queued, &run)
        .await?;

    let audit = repository.created_audit_payload(run_id).await?;
    assert_eq!(audit["actor"], "local-operator");
    assert_eq!(audit["decision_at"], "2026-08-23T12:00:00Z");
    assert_eq!(audit["affected_scope"], "https://example.test");
    assert_eq!(audit["user_agent"], "Erabi/0.1");
    assert_eq!(audit["robots"]["decision"], "OVERRIDE");
    assert_eq!(
        audit["robots"]["reason"],
        "operator approved this test target"
    );
    Ok(())
}
