use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{
    Crawler, CrawlerVersionGuardrails, DiscoveryTransition, DomainScopeKind, DomainScopePolicy,
    PageTypeDiscoveryGuardrails, Seed, TestEvidenceId, TransitionBudget,
};

fn transition(
    id: erabi_domain::DiscoveryTransitionId,
    source: erabi_domain::PageTypeId,
    target: erabi_domain::PageTypeId,
) -> DiscoveryTransition {
    DiscoveryTransition {
        id,
        source_page_type_id: source,
        target_page_type_id: target,
        name: "catalog links".into(),
        enabled: true,
        link_selector: "a[href]".into(),
        url_constraints: None,
        priority: 10,
        budget: TransitionBudget {
            max_links_per_source_page: 4,
            total_budget: Some(20),
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    }
}

async fn setup() -> Result<(ErabiDatabase, Crawler), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let crawler = Crawler::new("Discovery policy repository");
    CrawlerRepository::new(&database).create(&crawler).await?;
    Ok((database, crawler))
}

async fn persistent_setup()
-> Result<(tempfile::TempDir, ErabiDatabase, Crawler), Box<dyn std::error::Error>> {
    let data_dir = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(data_dir.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    let crawler = Crawler::new("Discovery policy repository");
    CrawlerRepository::new(&database).create(&crawler).await?;
    Ok((data_dir, database, crawler))
}

async fn raw_connection(
    data_dir: &tempfile::TempDir,
) -> Result<turso::Connection, Box<dyn std::error::Error>> {
    let database_path = data_dir.path().join("erabi.db");
    let raw_database = turso::Builder::new_local(database_path.to_string_lossy().as_ref())
        .build()
        .await?;
    Ok(raw_database.connect()?)
}

async fn seeded_draft(
    database: &ErabiDatabase,
    crawler: &Crawler,
) -> Result<(erabi_domain::CrawlerVersion, Seed), Box<dyn std::error::Error>> {
    let repository = CrawlerRepository::new(database);
    let mut version = repository
        .create_draft(crawler.id(), "operator", "now")
        .await?;
    let seed = Seed::new(
        "https://example.test/original".parse()?,
        "https://example.test/canonical".parse()?,
    );
    version.add_seed(seed.clone())?;
    repository.save_draft(&version, "operator", "now").await?;
    Ok((version, seed))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn transition_crud_is_typed_transactional_and_clone_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let version = repository
        .create_draft(crawler.id(), "operator", "2026-08-27T00:00:00Z")
        .await?;
    let source = repository
        .create_page_type(crawler.id(), version.id(), "Listing", 10, "operator", "now")
        .await?;
    let target = repository
        .create_page_type(crawler.id(), version.id(), "Product", 10, "operator", "now")
        .await?;
    let transition_id = erabi_domain::DiscoveryTransitionId::new();
    let mut authored = transition(transition_id, source.id, target.id);
    let created = repository
        .create_discovery_transition(crawler.id(), version.id(), &authored, "operator", "now")
        .await?;
    assert_eq!(created.transition.id, transition_id);
    assert_eq!(
        repository
            .list_discovery_transitions(crawler.id(), version.id())
            .await?
            .len(),
        1
    );
    assert_eq!(
        repository
            .version(crawler.id(), version.id())
            .await?
            .version
            .transition_ids(),
        &[transition_id]
    );

    let before = repository
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let mut evidence_metadata_transition = authored.clone();
    evidence_metadata_transition.latest_test_evidence_id = Some(TestEvidenceId::new());
    repository
        .update_discovery_transition(
            crawler.id(),
            version.id(),
            transition_id,
            &evidence_metadata_transition,
            "operator",
            "now",
        )
        .await?;
    assert_eq!(
        before,
        repository
            .configuration_hash(crawler.id(), version.id())
            .await?
    );
    authored.name = "updated links".into();
    repository
        .update_discovery_transition(
            crawler.id(),
            version.id(),
            transition_id,
            &authored,
            "operator",
            "now",
        )
        .await?;
    assert_ne!(
        before,
        repository
            .configuration_hash(crawler.id(), version.id())
            .await?
    );

    let mut guardrails = CrawlerVersionGuardrails::default();
    guardrails.page_types.push(PageTypeDiscoveryGuardrails {
        page_type_id: source.id,
        page_budget: Some(2),
        health_threshold: None,
    });
    let before_guardrail = repository
        .configuration_hash(crawler.id(), version.id())
        .await?;
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "now",
        )
        .await?;
    assert_ne!(
        before_guardrail,
        repository
            .configuration_hash(crawler.id(), version.id())
            .await?
    );
    let before_scope = repository
        .configuration_hash(crawler.id(), version.id())
        .await?;
    repository
        .update_domain_scope_policy(
            crawler.id(),
            version.id(),
            &DomainScopePolicy {
                version: 1,
                policy: DomainScopeKind::ExplicitAllowlist {
                    hosts: std::collections::BTreeSet::from(["example.test".into()]),
                },
            },
            "operator",
            "now",
        )
        .await?;
    assert_ne!(
        before_scope,
        repository
            .configuration_hash(crawler.id(), version.id())
            .await?
    );

    let published = repository
        .publish(crawler.id(), version.id(), "operator", "now")
        .await?;
    let clone = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "now")
        .await?;
    let cloned_transition = repository
        .list_discovery_transitions(crawler.id(), clone.id())
        .await?
        .pop()
        .ok_or_else(|| std::io::Error::other("cloned transition is missing"))?;
    assert_ne!(cloned_transition.transition.id, transition_id);
    assert_ne!(cloned_transition.transition.source_page_type_id, source.id);
    assert_ne!(cloned_transition.transition.target_page_type_id, target.id);
    assert_eq!(
        repository
            .configuration_hash(crawler.id(), clone.id())
            .await?,
        repository
            .configuration_hash(crawler.id(), published.version.id())
            .await?
    );

    repository
        .delete_discovery_transition(
            crawler.id(),
            clone.id(),
            cloned_transition.transition.id,
            "operator",
            "now",
        )
        .await?;
    assert!(
        repository
            .list_discovery_transitions(crawler.id(), clone.id())
            .await?
            .is_empty()
    );
    assert!(
        repository
            .version(crawler.id(), clone.id())
            .await?
            .version
            .transition_ids()
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn transition_page_type_ownership_and_persisted_corruption_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let version = repository
        .create_draft(crawler.id(), "operator", "now")
        .await?;
    let source = repository
        .create_page_type(crawler.id(), version.id(), "Source", 1, "operator", "now")
        .await?;
    let other_version = {
        repository
            .publish(crawler.id(), version.id(), "operator", "now")
            .await?;
        repository
            .create_draft(crawler.id(), "operator", "now")
            .await?
    };
    let other_page = repository
        .create_page_type(
            crawler.id(),
            other_version.id(),
            "Other",
            1,
            "operator",
            "now",
        )
        .await?;
    let invalid = transition(
        erabi_domain::DiscoveryTransitionId::new(),
        source.id,
        other_page.id,
    );
    assert!(matches!(
        repository
            .create_discovery_transition(
                crawler.id(),
                other_version.id(),
                &invalid,
                "operator",
                "now",
            )
            .await,
        Err(
            erabi_db::repositories::CrawlerRepositoryError::TransitionSourcePageTypeNotFound
                | erabi_db::repositories::CrawlerRepositoryError::TransitionTargetPageTypeNotFound
                | erabi_db::repositories::CrawlerRepositoryError::TransitionNotOwnedByVersion,
        )
    ));

    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_seed_projection_mismatches_fail_closed_on_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let (data_dir, database, crawler) = persistent_setup().await?;
    let repository = CrawlerRepository::new(&database);
    let (version, seed) = seeded_draft(&database, &crawler).await?;
    assert!(repository.version(crawler.id(), version.id()).await.is_ok());
    raw_connection(&data_dir)
        .await?
        .execute(
            "UPDATE seeds SET enabled = 0 WHERE id = ?1",
            [seed.id.to_string()],
        )
        .await?;
    assert!(matches!(
        repository.version(crawler.id(), version.id()).await,
        Err(erabi_db::repositories::CrawlerRepositoryError::CorruptState)
    ));

    let (data_dir, database, crawler) = persistent_setup().await?;
    let repository = CrawlerRepository::new(&database);
    let (version, seed) = seeded_draft(&database, &crawler).await?;
    raw_connection(&data_dir)
        .await?
        .execute(
            "UPDATE seeds SET canonical_url = ?1 WHERE id = ?2",
            ("https://other.test/", seed.id.to_string()),
        )
        .await?;
    assert!(matches!(
        repository.version(crawler.id(), version.id()).await,
        Err(erabi_db::repositories::CrawlerRepositoryError::CorruptState)
    ));

    let (data_dir, database, crawler) = persistent_setup().await?;
    let repository = CrawlerRepository::new(&database);
    let (version, seed) = seeded_draft(&database, &crawler).await?;
    raw_connection(&data_dir)
        .await?
        .execute("DELETE FROM seeds WHERE id = ?1", [seed.id.to_string()])
        .await?;
    assert!(matches!(
        repository.version(crawler.id(), version.id()).await,
        Err(erabi_db::repositories::CrawlerRepositoryError::CorruptState)
    ));

    let (data_dir, database, crawler) = persistent_setup().await?;
    let repository = CrawlerRepository::new(&database);
    let (version, _) = seeded_draft(&database, &crawler).await?;
    let extra_seed = Seed::new(
        "https://example.test/extra-original".parse()?,
        "https://example.test/extra-canonical".parse()?,
    );
    raw_connection(&data_dir)
        .await?
        .execute(
            "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                extra_seed.id.to_string(),
                version.id().to_string(),
                extra_seed.original_url.as_str(),
                extra_seed.canonical_url.as_str(),
                1_i64,
                Option::<String>::None,
                Option::<String>::None,
            ),
        )
        .await?;
    assert!(matches!(
        repository.version(crawler.id(), version.id()).await,
        Err(erabi_db::repositories::CrawlerRepositoryError::CorruptState)
    ));
    Ok(())
}
