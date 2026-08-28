use std::sync::Arc;

use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{CrawlerRepository, CrawlerRepositoryError, TestEvidenceRepository},
};
use erabi_domain::{
    Crawler, CrawlerVersion, DiscoveryTransition, DiscoveryTransitionEvidence, Seed,
    SelectorCoverageEvidence, SelectorCoverageStatus, TestEvidence, TestEvidenceId, TestKind,
    TransitionBudget, ValidationIssueCode, VersionValidationContext, VersionValidationContribution,
    VersionValidationContributor, VersionValidationContributorError, VersionValidationIssue,
    VersionValidationRegistry, VersionValidationSeverity,
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

async fn persistent_database()
-> Result<(tempfile::TempDir, ErabiDatabase), Box<dyn std::error::Error>> {
    let data_dir = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(data_dir.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    Ok((data_dir, database))
}

fn page_type_evidence(
    version_id: erabi_domain::CrawlerVersionId,
    config_hash: String,
    page_type_id: erabi_domain::PageTypeId,
) -> TestEvidence {
    TestEvidence {
        schema_version: erabi_domain::TEST_EVIDENCE_SCHEMA_VERSION,
        id: TestEvidenceId::new(),
        crawler_version_id: version_id,
        test_kind: TestKind::PageTypeMatching,
        input_urls: vec!["https://example.test/item".into()],
        evaluated_page_type_id: Some(page_type_id),
        tested_transition_id: None,
        canonicalization: Vec::new(),
        page_type_match: Vec::new(),
        extraction: None,
        selector_coverage: Vec::new(),
        pagination: None,
        discovery: None,
        warnings: Vec::new(),
        errors: Vec::new(),
        artifact_ids: Vec::new(),
        config_hash,
        executed_at: "unix:evidence".into(),
        published_comparison: None,
    }
}

fn transition_evidence(
    version_id: erabi_domain::CrawlerVersionId,
    config_hash: String,
    transition: &DiscoveryTransition,
) -> TestEvidence {
    TestEvidence {
        schema_version: erabi_domain::TEST_EVIDENCE_SCHEMA_VERSION,
        id: TestEvidenceId::new(),
        crawler_version_id: version_id,
        test_kind: TestKind::DiscoveryTransition,
        input_urls: vec!["https://example.test/listing".into()],
        evaluated_page_type_id: None,
        tested_transition_id: Some(transition.id),
        canonicalization: Vec::new(),
        page_type_match: Vec::new(),
        extraction: None,
        selector_coverage: Vec::new(),
        pagination: None,
        discovery: Some(DiscoveryTransitionEvidence {
            transition_id: Some(transition.id),
            transition_name: Some(transition.name.clone()),
            source_page_type_id: Some(transition.source_page_type_id),
            target_page_type_id: Some(transition.target_page_type_id),
            source_match: None,
            selector: SelectorCoverageEvidence {
                selector: transition.link_selector.clone(),
                matches_found: 0,
                status: SelectorCoverageStatus::NoMatches,
            },
            discovered_urls: Vec::new(),
            eligible_link_count: 0,
            per_page_limit: transition.budget.max_links_per_source_page,
            per_page_limit_reached: false,
        }),
        warnings: Vec::new(),
        errors: Vec::new(),
        artifact_ids: Vec::new(),
        config_hash,
        executed_at: "unix:evidence".into(),
        published_comparison: None,
    }
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

#[tokio::test]
async fn stale_page_type_evidence_remains_history_after_normal_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database(None).await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Stale PageType evidence");
    crawler_repository.create(&crawler).await?;
    let version = seeded_draft(&database, &crawler).await?;
    let deleted = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Deleted",
            1,
            "operator",
            "unix:3",
        )
        .await?;
    let retained = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Retained",
            1,
            "operator",
            "unix:4",
        )
        .await?;
    let evidence = page_type_evidence(
        version.id(),
        crawler_repository
            .configuration_hash(crawler.id(), version.id())
            .await?,
        deleted.id,
    );
    let evidence_repository = TestEvidenceRepository::new(&database);
    evidence_repository
        .persist_if_configuration_matches(crawler.id(), &evidence)
        .await?;

    crawler_repository
        .delete_page_type(crawler.id(), version.id(), deleted.id, "operator", "unix:5")
        .await?;

    let history = evidence_repository.list(crawler.id(), version.id()).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].evidence.id, evidence.id);
    assert!(!history[0].matches_current_configuration);

    let report = crawler_repository
        .publish_validation(crawler.id(), version.id())
        .await?;
    assert!(report.is_publishable());
    assert!(report.warnings.iter().any(|issue| {
        issue.code.as_str() == "PAGE_TYPE_TEST_EVIDENCE_MISSING"
            && issue
                .subject
                .as_ref()
                .and_then(|subject| subject.id.as_deref())
                == Some(retained.id.to_string().as_str())
    }));
    Ok(())
}

#[tokio::test]
async fn stale_transition_evidence_remains_history_after_normal_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database(None).await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Stale transition evidence");
    crawler_repository.create(&crawler).await?;
    let version = seeded_draft(&database, &crawler).await?;
    let source = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Source",
            1,
            "operator",
            "unix:3",
        )
        .await?;
    let target = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Target",
            1,
            "operator",
            "unix:4",
        )
        .await?;
    let transition = DiscoveryTransition {
        id: erabi_domain::DiscoveryTransitionId::new(),
        source_page_type_id: source.id,
        target_page_type_id: target.id,
        name: "Source to target".into(),
        enabled: true,
        link_selector: "a.target".into(),
        url_constraints: None,
        priority: 1,
        budget: TransitionBudget {
            max_links_per_source_page: 1,
            total_budget: None,
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    crawler_repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &transition,
            "operator",
            "unix:5",
        )
        .await?;
    let evidence = transition_evidence(
        version.id(),
        crawler_repository
            .configuration_hash(crawler.id(), version.id())
            .await?,
        &transition,
    );
    let evidence_repository = TestEvidenceRepository::new(&database);
    evidence_repository
        .persist_if_configuration_matches(crawler.id(), &evidence)
        .await?;

    crawler_repository
        .delete_discovery_transition(
            crawler.id(),
            version.id(),
            transition.id,
            "operator",
            "unix:6",
        )
        .await?;

    let history = evidence_repository.list(crawler.id(), version.id()).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].evidence.id, evidence.id);
    assert!(!history[0].matches_current_configuration);
    assert!(
        crawler_repository
            .publish_validation(crawler.id(), version.id())
            .await?
            .is_publishable()
    );
    Ok(())
}

#[tokio::test]
async fn exact_current_evidence_foreign_reference_and_row_identity_corruption_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let (data_dir, database) = persistent_database().await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Current evidence integrity");
    crawler_repository.create(&crawler).await?;
    let version = seeded_draft(&database, &crawler).await?;
    let page_type = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Current",
            1,
            "operator",
            "unix:3",
        )
        .await?;
    let evidence = page_type_evidence(
        version.id(),
        crawler_repository
            .configuration_hash(crawler.id(), version.id())
            .await?,
        page_type.id,
    );
    TestEvidenceRepository::new(&database)
        .persist_if_configuration_matches(crawler.id(), &evidence)
        .await?;

    let foreign_crawler = Crawler::new("Foreign evidence owner");
    crawler_repository.create(&foreign_crawler).await?;
    let foreign_version = seeded_draft(&database, &foreign_crawler).await?;
    let foreign_page_type = crawler_repository
        .create_page_type(
            foreign_crawler.id(),
            foreign_version.id(),
            "Foreign",
            1,
            "operator",
            "unix:4",
        )
        .await?;

    let raw_database =
        turso::Builder::new_local(data_dir.path().join("erabi.db").to_string_lossy().as_ref())
            .build()
            .await?;
    let connection = raw_database.connect()?;
    let original = serde_json::to_value(&evidence)?;
    let mut foreign_reference = original.clone();
    foreign_reference["evaluated_page_type_id"] = serde_json::json!(foreign_page_type.id);
    connection
        .execute(
            "UPDATE test_evidence SET evidence_json = ?1 WHERE id = ?2",
            (foreign_reference.to_string(), evidence.id.to_string()),
        )
        .await?;
    assert!(matches!(
        crawler_repository
            .publish_validation(crawler.id(), version.id())
            .await,
        Err(CrawlerRepositoryError::CorruptState)
    ));

    let mut mismatched_identity = original;
    mismatched_identity["id"] = serde_json::json!(TestEvidenceId::new());
    connection
        .execute(
            "UPDATE test_evidence SET evidence_json = ?1 WHERE id = ?2",
            (mismatched_identity.to_string(), evidence.id.to_string()),
        )
        .await?;
    assert!(matches!(
        crawler_repository
            .publish_validation(crawler.id(), version.id())
            .await,
        Err(CrawlerRepositoryError::CorruptState)
    ));
    Ok(())
}
