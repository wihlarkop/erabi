use std::sync::Arc;

use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{
    Crawler, CrawlerVersion, Seed, ValidationIssueCode, VersionValidationContext,
    VersionValidationContribution, VersionValidationContributor, VersionValidationContributorError,
    VersionValidationIssue, VersionValidationRegistry, VersionValidationSeverity,
};

struct WarningContributor;

impl VersionValidationContributor for WarningContributor {
    fn key(&self) -> &'static str {
        "future-warning"
    }

    fn validate(
        &self,
        _context: &VersionValidationContext,
    ) -> Result<VersionValidationContribution, VersionValidationContributorError> {
        Ok(VersionValidationContribution::new(vec![
            VersionValidationIssue::new(
                ValidationIssueCode::new("FUTURE_WARNING")
                    .map_err(|_| VersionValidationContributorError::InvalidContribution)?,
                VersionValidationSeverity::Warning,
                "bounded warning",
            ),
        ]))
    }
}

async fn database(
    registry: Option<VersionValidationRegistry>,
) -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    let database = registry.map_or(database.clone(), |registry| {
        database.with_version_validation_registry(registry)
    });
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

async fn seeded_draft(
    database: &ErabiDatabase,
    crawler: &Crawler,
) -> Result<CrawlerVersion, Box<dyn std::error::Error>> {
    let repository = CrawlerRepository::new(database);
    let mut version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    version.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    repository
        .save_draft(&version, "operator", "unix:2")
        .await?;
    Ok(version)
}

#[tokio::test]
async fn configured_registry_runs_for_preflight_and_atomic_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let mut registry = VersionValidationRegistry::new();
    registry.register(Arc::new(WarningContributor))?;
    let database = database(Some(registry)).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Publication validation");
    repository.create(&crawler).await?;
    let version = seeded_draft(&database, &crawler).await?;

    let preflight = repository
        .publish_validation(crawler.id(), version.id())
        .await?;
    assert_eq!(preflight.version_id, version.id());
    assert_eq!(preflight.config_hash.len(), 64);
    assert!(preflight.is_publishable());
    assert!(
        preflight
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "FUTURE_WARNING")
    );

    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:3")
        .await?;
    assert_eq!(published.audit.warning_summary, vec!["FUTURE_WARNING"]);
    let read = repository.version(crawler.id(), version.id()).await?;
    assert_eq!(read.audit.warning_summary, vec!["FUTURE_WARNING"]);
    assert_eq!(
        read.audit.config_hash.as_deref(),
        Some(preflight.config_hash.as_str())
    );

    repository
        .reactivate_published_typed(crawler.id(), version.id(), "operator-2", "unix:4")
        .await?;
    let reactivated = repository.version(crawler.id(), version.id()).await?;
    assert_eq!(reactivated.audit.warning_summary, vec!["FUTURE_WARNING"]);
    assert_eq!(reactivated.audit.actor.as_deref(), Some("operator"));
    Ok(())
}

#[tokio::test]
async fn blockers_do_not_mutate_published_pointer_or_audit()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database(None).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Blocked publication");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let report = repository
        .publish_validation(crawler.id(), version.id())
        .await?;
    assert!(!report.is_publishable());
    assert!(
        report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "NO_ENABLED_SEED")
    );
    assert!(matches!(
        repository
            .publish(crawler.id(), version.id(), "operator", "unix:2")
            .await,
        Err(erabi_db::repositories::CrawlerRepositoryError::PublicationValidationFailed(_))
    ));
    let pointers = repository.pointers(&crawler).await?;
    assert_eq!(pointers.active_published_version_id, None);
    assert_eq!(
        pointers.active_draft_version_id,
        Some(version.id().to_string())
    );
    assert_eq!(
        repository
            .audit_event_count(&version.id().to_string())
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn stale_publish_preflight_is_never_authorization() -> Result<(), Box<dyn std::error::Error>>
{
    let data_dir = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(data_dir.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Stale preflight");
    repository.create(&crawler).await?;
    let version = seeded_draft(&database, &crawler).await?;
    let preflight = repository
        .publish_validation(crawler.id(), version.id())
        .await?;
    assert!(preflight.is_publishable());

    let raw_database =
        turso::Builder::new_local(data_dir.path().join("erabi.db").to_string_lossy().as_ref())
            .build()
            .await?;
    let connection = raw_database.connect()?;
    let mut configuration = serde_json::to_value(
        &repository
            .version(crawler.id(), version.id())
            .await?
            .version,
    )?;
    configuration["seeds"][0]["enabled"] = serde_json::json!(false);
    connection
        .execute(
            "UPDATE crawler_versions SET semantic_configuration_json = ?1 WHERE id = ?2",
            (configuration.to_string(), version.id().to_string()),
        )
        .await?;
    connection
        .execute(
            "UPDATE seeds SET enabled = 0 WHERE crawler_version_id = ?1",
            [version.id().to_string()],
        )
        .await?;

    let result = repository
        .publish(crawler.id(), version.id(), "operator", "unix:3")
        .await;
    assert!(matches!(
        result,
        Err(erabi_db::repositories::CrawlerRepositoryError::PublicationValidationFailed(report))
            if report.blockers.iter().any(|issue| issue.code.as_str() == "NO_ENABLED_SEED")
    ));
    assert_eq!(
        repository
            .pointers(&crawler)
            .await?
            .active_published_version_id,
        None
    );
    Ok(())
}
