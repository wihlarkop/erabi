use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{CrawlerRepository, TestEvidenceRepository, TestEvidenceRepositoryError},
};
use erabi_domain::{
    ArtifactId, CanonicalizationEvidence, CanonicalizationOutcome, Crawler, CrawlerVersionId,
    TestEvidence, TestEvidenceId, TestKind,
};

async fn setup() -> Result<
    (
        tempfile::TempDir,
        ErabiDatabase,
        Crawler,
        erabi_domain::CrawlerVersion,
    ),
    Box<dyn std::error::Error>,
> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    let crawler = Crawler::new("Evidence repository");
    let repository = CrawlerRepository::new(&database);
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    Ok((directory, database, crawler, version))
}

async fn raw_connection(
    directory: &tempfile::TempDir,
) -> Result<turso::Connection, Box<dyn std::error::Error>> {
    let path = directory.path().join("erabi.db");
    let database = turso::Builder::new_local(path.to_string_lossy().as_ref())
        .build()
        .await?;
    Ok(database.connect()?)
}

fn evidence(version_id: CrawlerVersionId, config_hash: String, executed_at: &str) -> TestEvidence {
    TestEvidence {
        schema_version: erabi_domain::TEST_EVIDENCE_SCHEMA_VERSION,
        id: TestEvidenceId::new(),
        crawler_version_id: version_id,
        test_kind: TestKind::UrlCanonicalization,
        input_urls: vec!["https://example.test/items".to_owned()],
        evaluated_page_type_id: None,
        tested_transition_id: None,
        canonicalization: vec![CanonicalizationEvidence {
            original_url: "https://example.test/items".to_owned(),
            canonical_url: Some("https://example.test/items".to_owned()),
            outcome: CanonicalizationOutcome::Canonicalized,
            decisions: Vec::new(),
        }],
        page_type_match: Vec::new(),
        extraction: None,
        selector_coverage: Vec::new(),
        pagination: None,
        discovery: None,
        warnings: Vec::new(),
        errors: Vec::new(),
        artifact_ids: Vec::new(),
        config_hash,
        executed_at: executed_at.to_owned(),
        published_comparison: None,
    }
}

#[tokio::test]
async fn evidence_persists_lists_deterministically_and_becomes_stale_after_edit()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, crawler, version) = setup().await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let hash = crawler_repository
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let repository = TestEvidenceRepository::new(&database);
    let first = evidence(version.id(), hash.clone(), "unix:2");
    let second = evidence(version.id(), hash.clone(), "unix:1");
    repository
        .persist_if_configuration_matches(crawler.id(), &first)
        .await?;
    repository
        .persist_if_configuration_matches(crawler.id(), &second)
        .await?;
    let listed = repository.list(crawler.id(), version.id()).await?;
    assert_eq!(
        listed
            .iter()
            .map(|item| item.evidence.id)
            .collect::<Vec<_>>(),
        vec![second.id, first.id]
    );
    assert!(listed.iter().all(|item| item.matches_current_configuration));
    let changed_policy = erabi_domain::CanonicalizationPolicy::new(
        std::collections::BTreeSet::new(),
        std::collections::BTreeSet::from(["campaign".to_owned()]),
    )?;
    crawler_repository
        .update_canonicalization_policy(
            crawler.id(),
            version.id(),
            &changed_policy,
            "operator",
            "unix:3",
        )
        .await?;
    let stale = repository
        .read(crawler.id(), version.id(), first.id)
        .await?;
    assert!(!stale.matches_current_configuration);
    assert_eq!(stale.evidence.config_hash, hash);
    Ok(())
}

#[tokio::test]
async fn evidence_rejects_wrong_crawler_and_version_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, crawler, version) = setup().await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let hash = crawler_repository
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let repository = TestEvidenceRepository::new(&database);
    let stored = evidence(version.id(), hash, "unix:1");
    repository
        .persist_if_configuration_matches(crawler.id(), &stored)
        .await?;

    let other_crawler = Crawler::new("Other evidence crawler");
    crawler_repository.create(&other_crawler).await?;
    let other_version = crawler_repository
        .create_draft(other_crawler.id(), "operator", "unix:1")
        .await?;
    assert!(matches!(
        repository
            .read(other_crawler.id(), version.id(), stored.id)
            .await,
        Err(TestEvidenceRepositoryError::Crawler(
            erabi_db::repositories::CrawlerRepositoryError::VersionNotOwnedByCrawler
        ))
    ));
    assert!(matches!(
        repository
            .read(crawler.id(), other_version.id(), stored.id)
            .await,
        Err(TestEvidenceRepositoryError::TestEvidenceNotOwnedByVersion)
    ));
    let other_hash = crawler_repository
        .configuration_hash(other_crawler.id(), other_version.id())
        .await?;
    let wrong_owner_evidence = evidence(version.id(), other_hash, "unix:2");
    assert!(matches!(
        repository
            .persist_if_configuration_matches(other_crawler.id(), &wrong_owner_evidence)
            .await,
        Err(TestEvidenceRepositoryError::Crawler(
            erabi_db::repositories::CrawlerRepositoryError::VersionNotActiveDraft
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn hash_mismatch_and_bad_references_leave_no_partial_evidence_row()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, crawler, version) = setup().await?;
    let hash = CrawlerRepository::new(&database)
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let repository = TestEvidenceRepository::new(&database);
    let mismatch = evidence(version.id(), "11".repeat(32), "unix:1");
    assert!(matches!(
        repository
            .persist_if_configuration_matches(crawler.id(), &mismatch)
            .await,
        Err(TestEvidenceRepositoryError::ConfigurationChanged)
    ));
    assert!(
        repository
            .list(crawler.id(), version.id())
            .await?
            .is_empty()
    );
    let mut bad_page = evidence(version.id(), hash.clone(), "unix:2");
    bad_page.evaluated_page_type_id = Some(erabi_domain::PageTypeId::new());
    assert!(matches!(
        repository
            .persist_if_configuration_matches(crawler.id(), &bad_page)
            .await,
        Err(TestEvidenceRepositoryError::Crawler(
            erabi_db::repositories::CrawlerRepositoryError::PageTypeNotFound
        ))
    ));
    assert!(
        repository
            .list(crawler.id(), version.id())
            .await?
            .is_empty()
    );
    let mut bad_artifact = evidence(version.id(), hash, "unix:3");
    bad_artifact.artifact_ids.push(ArtifactId::new());
    assert!(matches!(
        repository
            .persist_if_configuration_matches(crawler.id(), &bad_artifact)
            .await,
        Err(TestEvidenceRepositoryError::ArtifactNotFound)
    ));
    assert!(
        repository
            .list(crawler.id(), version.id())
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn mismatched_projection_and_malformed_payload_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database, crawler, version) = setup().await?;
    let hash = CrawlerRepository::new(&database)
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let repository = TestEvidenceRepository::new(&database);
    let stored = evidence(version.id(), hash, "unix:1");
    repository
        .persist_if_configuration_matches(crawler.id(), &stored)
        .await?;
    let mut value = serde_json::to_value(&stored)?;
    value["id"] = serde_json::json!(TestEvidenceId::new().to_string());
    let raw = raw_connection(&directory).await?;
    raw.execute(
        "UPDATE test_evidence SET evidence_json = ?1 WHERE id = ?2",
        (serde_json::to_string(&value)?, stored.id.to_string()),
    )
    .await?;
    assert!(matches!(
        repository.read(crawler.id(), version.id(), stored.id).await,
        Err(TestEvidenceRepositoryError::CorruptState)
    ));

    let crawler_repository = CrawlerRepository::new(&database);
    let other_crawler = Crawler::new("Other evidence owner");
    crawler_repository.create(&other_crawler).await?;
    let other_version = crawler_repository
        .create_draft(other_crawler.id(), "operator", "unix:1")
        .await?;
    raw.execute(
        "UPDATE test_evidence SET evidence_json = ?1, crawler_version_id = ?2 WHERE id = ?3",
        (
            serde_json::to_string(&stored)?,
            other_version.id().to_string(),
            stored.id.to_string(),
        ),
    )
    .await?;
    assert!(matches!(
        repository.read(crawler.id(), version.id(), stored.id).await,
        Err(TestEvidenceRepositoryError::CorruptState)
    ));

    raw.execute(
        "UPDATE test_evidence SET crawler_version_id = ?1, executed_at = ?2 WHERE id = ?3",
        (version.id().to_string(), "unix:9", stored.id.to_string()),
    )
    .await?;
    assert!(matches!(
        repository.read(crawler.id(), version.id(), stored.id).await,
        Err(TestEvidenceRepositoryError::CorruptState)
    ));

    let mut bad_version = serde_json::to_value(&stored)?;
    bad_version["schema_version"] = serde_json::json!(2);
    raw.execute(
        "UPDATE test_evidence SET evidence_json = ?1, executed_at = ?2 WHERE id = ?3",
        (
            serde_json::to_string(&bad_version)?,
            stored.executed_at.as_str(),
            stored.id.to_string(),
        ),
    )
    .await?;
    assert!(matches!(
        repository.read(crawler.id(), version.id(), stored.id).await,
        Err(TestEvidenceRepositoryError::CorruptState)
    ));

    let (directory, database, crawler, version) = setup().await?;
    let hash = CrawlerRepository::new(&database)
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let repository = TestEvidenceRepository::new(&database);
    let stored = evidence(version.id(), hash, "unix:1");
    repository
        .persist_if_configuration_matches(crawler.id(), &stored)
        .await?;
    let raw = raw_connection(&directory).await?;
    raw.execute(
        "UPDATE test_evidence SET evidence_json = ?1 WHERE id = ?2",
        ("not-json", stored.id.to_string()),
    )
    .await?;
    assert!(matches!(
        repository.read(crawler.id(), version.id(), stored.id).await,
        Err(TestEvidenceRepositoryError::CorruptState)
    ));
    Ok(())
}
