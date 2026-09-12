use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{
        ArtifactRepository, CheckpointEnvelope, CheckpointIdentity, CheckpointPayloadKind,
        CrawlAdmissionState, CrawlExecutionArtifact, CrawlExecutionArtifactKind,
        CrawlExecutionRecord, CrawlExecutionRepository, CrawlExecutionRepositoryError,
        CrawlExecutionSummary, CrawlRecoveryActionKind, CrawlRunRepository, CrawlTraversalControl,
        CrawlTraversalRepository, CrawlTraversalSemanticProjection, CrawlUrlStateRecord,
        CrawlWorkState, CrawlerRepository, DiscoveredUrlRecord, JobKind, JobRepository, NewJob,
    },
};
use erabi_domain::{
    ArtifactId, CrawlExecutionErrorCode, CrawlExecutionId, CrawlExecutionOutcome, CrawlRunId,
    CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType, Crawler, CrawlerId,
    CrawlerVersionId, DiscoveryTransition, DiscoveryTransitionId, PageTypeId, ResolvedValue,
    RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings, Source, SourceId,
    SourceStatus, SourceTargetType, TransitionBudget,
};
use sha2::{Digest, Sha256};

#[derive(Clone)]
struct TestConnection {
    database: Arc<Mutex<rusqlite::Connection>>,
}

struct TestStatement {
    database: Arc<Mutex<rusqlite::Connection>>,
    sql: String,
}

struct TestRow {
    values: Vec<rusqlite::types::Value>,
}

#[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
impl TestConnection {
    async fn execute<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<usize, rusqlite::Error> {
        self.database
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?
            .execute(sql, params)
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), rusqlite::Error> {
        self.database
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?
            .execute_batch(sql)
    }

    async fn prepare(&self, sql: &str) -> Result<TestStatement, rusqlite::Error> {
        self.database
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?
            .prepare(sql)
            .map(|_| TestStatement {
                database: Arc::clone(&self.database),
                sql: sql.to_owned(),
            })
    }
}

#[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
impl TestStatement {
    async fn query_row<P: rusqlite::Params>(&self, params: P) -> Result<TestRow, rusqlite::Error> {
        let database = self
            .database
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        let mut statement = database.prepare(&self.sql)?;
        statement.query_row(params, |row| {
            let mut values = Vec::with_capacity(row.as_ref().column_count());
            for index in 0..row.as_ref().column_count() {
                let value = row.get_ref(index)?;
                values.push(rusqlite::types::Value::try_from(value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        index,
                        value.data_type(),
                        Box::new(error),
                    )
                })?);
            }
            Ok(TestRow { values })
        })
    }
}

impl TestRow {
    fn get<T: rusqlite::types::FromSql>(&self, index: usize) -> Result<T, rusqlite::Error> {
        let value = self
            .values
            .get(index)
            .ok_or(rusqlite::Error::InvalidColumnIndex(index))?;
        T::column_result(rusqlite::types::ValueRef::from(value)).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(index, value.data_type(), Box::new(error))
        })
    }
}

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

fn quick_snapshot(target_url: &str) -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
    Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::QuickScrape,
        configuration: RunConfiguration::QuickScrape {
            target_url: target_url.parse()?,
            ad_hoc_configuration: BTreeMap::new(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "2026-08-29T00:00:00Z",
            "https://example.test",
            "Erabi/0.1",
            None,
        ),
        actor: "operator".into(),
        created_at: "2026-08-29T00:00:00Z".into(),
    })?)
}

fn crawler_version_snapshot(
    run_type: CrawlRunType,
    crawler_id: CrawlerId,
    crawler_version_id: CrawlerVersionId,
    config_hash: &str,
) -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
    Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type,
        configuration: RunConfiguration::CrawlerVersion {
            crawler_id,
            crawler_version_id,
            semantic_config_hash: config_hash.to_owned(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "2026-08-29T00:00:00Z",
            "https://example.test",
            "Erabi/0.1",
            Some(crawler_version_id),
        ),
        actor: "operator".into(),
        created_at: "2026-08-29T00:00:00Z".into(),
    })?)
}

#[allow(clippy::unused_async)]
async fn raw_connection(
    directory: &tempfile::TempDir,
) -> Result<TestConnection, Box<dyn std::error::Error>> {
    let path = directory.path().join("erabi.db");
    let connection = rusqlite::Connection::open(path)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(TestConnection {
        database: Arc::new(Mutex::new(connection)),
    })
}

async fn quick_setup(
    target_url: &str,
) -> Result<(tempfile::TempDir, ErabiDatabase, CrawlRunId), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    let run_id = CrawlRunId::new();
    CrawlRunRepository::new(&database)
        .create(run_id, CrawlRunStatus::Queued, &quick_snapshot(target_url)?)
        .await?;
    Ok((directory, database, run_id))
}

async fn create_crawler_version_run(
    database: &ErabiDatabase,
    status: CrawlRunStatus,
    run_type: CrawlRunType,
    crawler_id: CrawlerId,
    crawler_version_id: CrawlerVersionId,
    configuration_hash: &str,
) -> Result<CrawlRunId, Box<dyn std::error::Error>> {
    let run_id = CrawlRunId::new();
    CrawlRunRepository::new(database)
        .create(
            run_id,
            status,
            &crawler_version_snapshot(
                run_type,
                crawler_id,
                crawler_version_id,
                configuration_hash,
            )?,
        )
        .await?;
    Ok(run_id)
}

async fn execution_setup() -> Result<ExecutionFixture, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    let crawler_repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Execution fixture");
    crawler_repository.create(&crawler).await?;
    let version = crawler_repository
        .create_draft(crawler.id(), "operator", "2026-08-29T00:00:00Z")
        .await?;
    let source_page_type = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Listing",
            1,
            "operator",
            "2026-08-29T00:01:00Z",
        )
        .await?;
    let target_page_type = crawler_repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Product",
            1,
            "operator",
            "2026-08-29T00:02:00Z",
        )
        .await?;
    let transition = DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id: source_page_type.id,
        target_page_type_id: target_page_type.id,
        name: "Product links".to_owned(),
        enabled: true,
        link_selector: "a.product".to_owned(),
        url_constraints: None,
        priority: 1,
        budget: TransitionBudget {
            max_links_per_source_page: 1,
            total_budget: Some(10),
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
            "2026-08-29T00:03:00Z",
        )
        .await?;
    let config_hash = crawler_repository
        .configuration_hash(crawler.id(), version.id())
        .await?;
    let run_id = create_crawler_version_run(
        &database,
        CrawlRunStatus::Running,
        CrawlRunType::TestRun,
        crawler.id(),
        version.id(),
        &config_hash,
    )
    .await?;

    let source = Source::new(
        "Execution source",
        "https://example.test/start".parse()?,
        "https://example.test/start".parse()?,
        SourceTargetType::WebPage,
    );
    insert_source(&directory, &source).await?;
    let discovered_url_id = "discovered-target".to_owned();
    insert_discovered_url(
        &directory,
        &discovered_url_id,
        run_id,
        source.id,
        "https://example.test/item",
        "https://example.test/item",
    )
    .await?;
    Ok(ExecutionFixture {
        directory,
        database,
        source,
        target_page_type: target_page_type.id,
        transition,
        run_id,
        discovered_url_id,
        crawler_id: crawler.id(),
        crawler_version_id: version.id(),
        configuration_hash: config_hash,
    })
}

struct ExecutionFixture {
    directory: tempfile::TempDir,
    database: ErabiDatabase,
    source: Source,
    target_page_type: PageTypeId,
    transition: DiscoveryTransition,
    run_id: CrawlRunId,
    discovered_url_id: String,
    crawler_id: CrawlerId,
    crawler_version_id: CrawlerVersionId,
    configuration_hash: String,
}

async fn insert_source(
    directory: &tempfile::TempDir,
    source: &Source,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = raw_connection(directory).await?;
    connection
        .execute(
            "INSERT INTO sources (id, collection_id, name, original_url, canonical_url, target_type, status) VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6)",
            (
                source.id.to_string(),
                source.name.as_str(),
                source.original_url.as_str(),
                source.canonical_url.as_str(),
                "WEB_PAGE",
                "ACTIVE",
            ),
        )
        .await?;
    Ok(())
}

async fn insert_discovered_url(
    directory: &tempfile::TempDir,
    id: &str,
    run_id: CrawlRunId,
    source_id: SourceId,
    original_url: &str,
    canonical_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = raw_connection(directory).await?;
    connection
        .execute(
            "INSERT INTO discovered_urls (id, crawl_run_id, source_id, raw_href, original_url, canonical_url, status, discovered_at, detail_json) VALUES (?1, ?2, ?3, NULL, ?4, ?5, 'IN_SCOPE_MATCHED', '2026-08-29T00:04:00Z', '{}')",
            (
                id,
                run_id.to_string(),
                source_id.to_string(),
                original_url,
                canonical_url,
            ),
        )
        .await?;
    Ok(())
}

async fn insert_artifact(
    directory: &tempfile::TempDir,
    artifact_id: ArtifactId,
    run_id: CrawlRunId,
    source_id: Option<SourceId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = raw_connection(directory).await?;
    connection
        .execute(
            "INSERT INTO artifacts (id, crawl_run_id, source_id, content_hash, byte_size, media_type, safe_relative_path, created_at, metadata_json) VALUES (?1, ?2, ?3, 'hash', 12, 'text/html', ?4, '2026-08-29T00:05:00Z', '{\"storage\":\"filesystem\"}')",
            (
                artifact_id.to_string(),
                run_id.to_string(),
                source_id.map_or(rusqlite::types::Value::Null, |id| {
                    rusqlite::types::Value::Text(id.to_string())
                }),
                format!("pages/{artifact_id}.html"),
            ),
        )
        .await?;
    Ok(())
}

fn record(
    id: CrawlExecutionId,
    run_id: CrawlRunId,
    requested_url: &str,
    canonical_url: &str,
    observed_final_url: Option<&str>,
    outcome: CrawlExecutionOutcome,
    error_code: Option<CrawlExecutionErrorCode>,
) -> CrawlExecutionRecord {
    CrawlExecutionRecord {
        id,
        crawl_run_id: run_id,
        requested_url: requested_url.to_owned(),
        canonical_url: canonical_url.to_owned(),
        observed_final_url: observed_final_url.map(str::to_owned),
        source_id: None,
        page_type_id: None,
        transition_id: None,
        discovered_url_id: None,
        outcome,
        error_code,
        http_status: None,
        media_type: None,
        content_length_bytes: None,
        provider_elapsed_ms: None,
        artifacts: Vec::new(),
    }
}

#[test]
fn execution_record_debug_redacts_url_query_values() {
    let record = record(
        CrawlExecutionId::new(),
        CrawlRunId::new(),
        "https://example.test/requested?token=secret",
        "https://example.test/canonical?token=secret",
        Some("https://example.test/final?token=secret"),
        CrawlExecutionOutcome::Completed,
        None,
    );

    let debug = format!("{record:?}");
    assert!(!debug.contains("secret"));
    assert!(!debug.contains("token="));
    assert!(debug.contains("https://example.test/requested"));
}

#[tokio::test]
async fn existing_0001_through_0004_database_upgrades_and_preserves_snapshots()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    let runner = MigrationRunner::default();
    assert_eq!(
        runner.apply_through(&database, "0004").await?.applied,
        ["0001", "0002", "0003", "0004"]
    );
    let run_id = CrawlRunId::new();
    let snapshot = quick_snapshot("https://example.test/upgrade")?;
    CrawlRunRepository::new(&database)
        .create(run_id, CrawlRunStatus::Queued, &snapshot)
        .await?;

    assert_eq!(runner.apply(&database).await?.applied, ["0005", "0006"]);
    assert_eq!(
        runner
            .status(&database)
            .await?
            .into_iter()
            .map(|version| version.version)
            .collect::<Vec<_>>(),
        ["0001", "0002", "0003", "0004", "0005", "0006"]
    );
    assert_eq!(
        CrawlRunRepository::new(&database).snapshot(run_id).await?,
        snapshot
    );

    let connection = raw_connection(&directory).await?;
    for table in [
        "crawl_execution_results",
        "crawl_execution_artifacts",
        "crawl_execution_summaries",
    ] {
        let row = connection
            .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1")
            .await?
            .query_row([table])
            .await?;
        assert_eq!(row.get::<i64>(0)?, 1);
    }
    assert!(
        connection
            .execute(
                "UPDATE crawl_runs SET snapshot_json = ?1 WHERE id = ?2",
                ("{}", run_id.to_string()),
            )
            .await
            .is_err()
    );
    assert_eq!(
        CrawlRunRepository::new(&database).snapshot(run_id).await?,
        snapshot
    );
    Ok(())
}

#[tokio::test]
async fn traversal_state_schema_enforces_canonical_identity_and_finite_work_contracts()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    let run_id = CrawlRunId::new();
    CrawlRunRepository::new(&database)
        .create(
            run_id,
            CrawlRunStatus::Queued,
            &quick_snapshot("https://example.test/root")?,
        )
        .await?;
    let connection = raw_connection(&directory).await?;
    connection.execute(
        "INSERT INTO crawl_traversal_control (crawl_run_id, next_admission_sequence) VALUES (?1, 1)",
        [run_id.to_string()],
    ).await?;
    connection.execute(
        "INSERT INTO crawl_url_state (id, crawl_run_id, canonical_url, requested_url, admission_state, admission_sequence, depth, current_work_state) VALUES ('state-1', ?1, 'https://example.test/a', 'https://example.test/a', 'ADMITTED', 0, 0, 'PENDING')",
        [run_id.to_string()],
    ).await?;
    assert!(connection.execute(
        "INSERT INTO crawl_url_state (id, crawl_run_id, canonical_url, requested_url, admission_state, admission_sequence, depth, current_work_state) VALUES ('state-2', ?1, 'https://example.test/a', 'https://example.test/b', 'ADMITTED', 1, 0, 'PENDING')",
        [run_id.to_string()],
    ).await.is_err());
    assert!(connection.execute(
        "INSERT INTO crawl_url_state (id, crawl_run_id, canonical_url, requested_url, admission_state, admission_sequence, depth, current_work_state) VALUES ('state-3', ?1, 'https://example.test/c', 'https://example.test/c', 'ADMITTED', 0, 0, 'PENDING')",
        [run_id.to_string()],
    ).await.is_err());
    assert!(connection.execute(
        "INSERT INTO crawl_url_state (id, crawl_run_id, canonical_url, requested_url, admission_state, preserve_reason) VALUES ('state-4', ?1, 'https://example.test/d', 'https://example.test/d', 'PRESERVE_ONLY', NULL)",
        [run_id.to_string()],
    ).await.is_err());
    assert!(connection.execute(
        "INSERT INTO crawl_url_state (id, crawl_run_id, canonical_url, requested_url, admission_state, admission_sequence, depth, current_work_state) VALUES ('state-5', ?1, 'https://example.test/e', 'https://example.test/e', 'ADMITTED', 2, 0, 'UNKNOWN')",
        [run_id.to_string()],
    ).await.is_err());
    Ok(())
}

#[tokio::test]
async fn page_execution_round_trip_preserves_distinct_urls_and_bounded_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/requested").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let mut expected = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/requested",
        "https://example.test/canonical",
        Some("https://final.test/observed"),
        CrawlExecutionOutcome::Completed,
        None,
    );
    expected.http_status = Some(200);
    expected.media_type = Some("text/html; charset=utf-8".to_owned());
    expected.content_length_bytes = Some(4_096);
    expected.provider_elapsed_ms = Some(37);
    repository.persist(&expected).await?;
    assert_eq!(repository.read(expected.id).await?, expected);
    assert_eq!(repository.list_for_run(run_id).await?, vec![expected]);
    Ok(())
}

#[tokio::test]
async fn absent_final_url_and_provider_neutral_outcomes_round_trip()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/outcome").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let cases = [
        (CrawlExecutionOutcome::Completed, None),
        (
            CrawlExecutionOutcome::Partial,
            Some(CrawlExecutionErrorCode::PartialResult),
        ),
        (
            CrawlExecutionOutcome::Failed,
            Some(CrawlExecutionErrorCode::Timeout),
        ),
        (
            CrawlExecutionOutcome::Cancelled,
            Some(CrawlExecutionErrorCode::Cancelled),
        ),
    ];
    for (outcome, error_code) in cases {
        let expected = record(
            CrawlExecutionId::new(),
            run_id,
            "https://example.test/outcome",
            "https://example.test/outcome",
            None,
            outcome,
            error_code,
        );
        repository.persist(&expected).await?;
        assert_eq!(repository.read(expected.id).await?, expected);
    }
    let outcomes = repository
        .list_for_run(run_id)
        .await?
        .into_iter()
        .map(|item| (item.outcome, item.error_code))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), cases.len());
    assert!(outcomes.contains(&(CrawlExecutionOutcome::Completed, None)));
    assert!(outcomes.contains(&(
        CrawlExecutionOutcome::Partial,
        Some(CrawlExecutionErrorCode::PartialResult)
    )));
    assert!(outcomes.contains(&(
        CrawlExecutionOutcome::Failed,
        Some(CrawlExecutionErrorCode::Timeout)
    )));
    assert!(outcomes.contains(&(
        CrawlExecutionOutcome::Cancelled,
        Some(CrawlExecutionErrorCode::Cancelled)
    )));
    Ok(())
}

#[tokio::test]
async fn source_page_type_transition_and_discovered_url_references_are_validated()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = execution_setup().await?;
    let repository = CrawlExecutionRepository::new(&fixture.database);
    let mut expected = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        Some("https://example.test/item?redirected=1"),
        CrawlExecutionOutcome::Completed,
        None,
    );
    expected.source_id = Some(fixture.source.id);
    expected.page_type_id = Some(fixture.target_page_type);
    expected.transition_id = Some(fixture.transition.id);
    expected.discovered_url_id = Some(fixture.discovered_url_id.clone());
    expected.http_status = Some(200);
    expected.media_type = Some("text/html".to_owned());
    repository.persist(&expected).await?;
    assert_eq!(repository.read(expected.id).await?, expected);
    Ok(())
}

#[tokio::test]
async fn production_execution_rejects_a_draft_crawler_version()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = execution_setup().await?;
    let invalid_run_id = create_crawler_version_run(
        &fixture.database,
        CrawlRunStatus::Running,
        CrawlRunType::ProductionRun,
        fixture.crawler_id,
        fixture.crawler_version_id,
        &fixture.configuration_hash,
    )
    .await?;

    let repository = CrawlExecutionRepository::new(&fixture.database);
    let invalid = record(
        CrawlExecutionId::new(),
        invalid_run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    assert!(matches!(
        repository.persist(&invalid).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_source_page_type_transition_and_provenance_ownership_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = execution_setup().await?;
    let foreign = create_foreign_crawler_fixture(&fixture).await?;
    reject_foreign_page_type(&fixture, foreign.page_type).await?;
    reject_foreign_source(&fixture).await?;
    reject_foreign_transition(&fixture, &foreign).await?;
    reject_foreign_discovered_url(&fixture).await?;
    Ok(())
}

#[tokio::test]
async fn persisted_cross_version_page_type_reference_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = execution_setup().await?;
    let foreign = create_foreign_crawler_fixture(&fixture).await?;
    let repository = CrawlExecutionRepository::new(&fixture.database);
    let mut expected = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    expected.page_type_id = Some(fixture.target_page_type);
    repository.persist(&expected).await?;

    let connection = raw_connection(&fixture.directory).await?;
    connection
        .execute(
            "UPDATE crawl_execution_results SET page_type_id = ?1 WHERE id = ?2",
            (foreign.page_type.to_string(), expected.id.to_string()),
        )
        .await?;
    assert!(matches!(
        repository.read(expected.id).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));
    Ok(())
}

struct ForeignCrawlerFixture {
    crawler: CrawlerId,
    version: CrawlerVersionId,
    page_type: PageTypeId,
}

async fn create_foreign_crawler_fixture(
    fixture: &ExecutionFixture,
) -> Result<ForeignCrawlerFixture, Box<dyn std::error::Error>> {
    let foreign_crawler = Crawler::new("Foreign execution fixture");
    let crawler_repository = CrawlerRepository::new(&fixture.database);
    crawler_repository.create(&foreign_crawler).await?;
    let foreign_version = crawler_repository
        .create_draft(foreign_crawler.id(), "operator", "2026-08-29T00:10:00Z")
        .await?;
    let foreign_page_type = crawler_repository
        .create_page_type(
            foreign_crawler.id(),
            foreign_version.id(),
            "Foreign",
            1,
            "operator",
            "2026-08-29T00:11:00Z",
        )
        .await?;
    Ok(ForeignCrawlerFixture {
        crawler: foreign_crawler.id(),
        version: foreign_version.id(),
        page_type: foreign_page_type.id,
    })
}

async fn reject_foreign_page_type(
    fixture: &ExecutionFixture,
    foreign_page_type_id: PageTypeId,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = CrawlExecutionRepository::new(&fixture.database);
    let mut page_type_record = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    page_type_record.page_type_id = Some(foreign_page_type_id);
    assert!(matches!(
        repository.persist(&page_type_record).await,
        Err(CrawlExecutionRepositoryError::PageTypeNotOwnedByRun)
    ));
    Ok(())
}

async fn reject_foreign_source(
    fixture: &ExecutionFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = CrawlExecutionRepository::new(&fixture.database);
    let foreign_source = Source::new(
        "Foreign source",
        "https://foreign.test/".parse()?,
        "https://foreign.test/".parse()?,
        SourceTargetType::WebPage,
    );
    insert_source(&fixture.directory, &foreign_source).await?;
    let mut source_record = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    source_record.source_id = Some(foreign_source.id);
    assert!(matches!(
        repository.persist(&source_record).await,
        Err(CrawlExecutionRepositoryError::SourceNotOwnedByRun)
    ));
    Ok(())
}

async fn reject_foreign_transition(
    fixture: &ExecutionFixture,
    foreign: &ForeignCrawlerFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = CrawlExecutionRepository::new(&fixture.database);
    let crawler_repository = CrawlerRepository::new(&fixture.database);
    let foreign_transition = DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id: foreign.page_type,
        target_page_type_id: foreign.page_type,
        name: "Foreign transition".to_owned(),
        enabled: true,
        link_selector: "a".to_owned(),
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
            foreign.crawler,
            foreign.version,
            &foreign_transition,
            "operator",
            "2026-08-29T00:12:00Z",
        )
        .await?;
    let mut transition_record = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    transition_record.transition_id = Some(foreign_transition.id);
    assert!(matches!(
        repository.persist(&transition_record).await,
        Err(CrawlExecutionRepositoryError::TransitionNotOwnedByRun)
    ));
    Ok(())
}

async fn reject_foreign_discovered_url(
    fixture: &ExecutionFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = CrawlExecutionRepository::new(&fixture.database);
    let other_run_id = CrawlRunId::new();
    CrawlRunRepository::new(&fixture.database)
        .create(
            other_run_id,
            CrawlRunStatus::Queued,
            &quick_snapshot("https://other.test/item")?,
        )
        .await?;
    insert_discovered_url(
        &fixture.directory,
        "discovered-foreign",
        other_run_id,
        fixture.source.id,
        "https://example.test/item",
        "https://example.test/item",
    )
    .await?;
    let mut provenance_record = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    provenance_record.discovered_url_id = Some("discovered-foreign".to_owned());
    assert!(matches!(
        repository.persist(&provenance_record).await,
        Err(CrawlExecutionRepositoryError::DiscoveredUrlNotOwnedByRun)
    ));
    Ok(())
}

#[tokio::test]
async fn artifact_references_round_trip_without_copying_artifact_bodies()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = execution_setup().await?;
    let artifact_id = ArtifactId::new();
    insert_artifact(
        &fixture.directory,
        artifact_id,
        fixture.run_id,
        Some(fixture.source.id),
    )
    .await?;
    let mut expected = record(
        CrawlExecutionId::new(),
        fixture.run_id,
        "https://example.test/item",
        "https://example.test/item",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    expected.source_id = Some(fixture.source.id);
    expected.artifacts.push(CrawlExecutionArtifact {
        artifact_id,
        kind: CrawlExecutionArtifactKind::RawHtml,
    });
    let repository = CrawlExecutionRepository::new(&fixture.database);
    repository.persist(&expected).await?;
    assert_eq!(repository.read(expected.id).await?, expected);

    let connection = raw_connection(&fixture.directory).await?;
    let artifact_ref = connection
        .prepare("SELECT artifact_id FROM crawl_execution_artifacts WHERE crawl_execution_id = ?1")
        .await?
        .query_row([expected.id.to_string()])
        .await?;
    assert_eq!(artifact_ref.get::<String>(0)?, artifact_id.to_string());
    let result_sql: String = connection
        .prepare("SELECT sql FROM sqlite_schema WHERE name = 'crawl_execution_results'")
        .await?
        .query_row(())
        .await?
        .get(0)?;
    assert!(!result_sql.contains("body"));
    Ok(())
}

#[tokio::test]
async fn duplicate_execution_identity_is_rejected_without_replacing_the_row()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/duplicate").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let expected = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/duplicate",
        "https://example.test/duplicate",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    repository.persist(&expected).await?;
    let mut changed = expected.clone();
    changed.outcome = CrawlExecutionOutcome::Failed;
    changed.error_code = Some(CrawlExecutionErrorCode::Timeout);
    assert!(matches!(
        repository.persist(&changed).await,
        Err(CrawlExecutionRepositoryError::DuplicateExecution)
    ));
    assert_eq!(repository.read(expected.id).await?, expected);
    Ok(())
}

#[tokio::test]
async fn summaries_are_checked_and_run_reads_are_deterministic()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/summary").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let summary = CrawlExecutionSummary {
        crawl_run_id: run_id,
        in_scope_pages_planned: 10,
        in_scope_pages_completed: 7,
        pagination_truncation_count: 1,
        unresolved_partial_work_count: 2,
        page_type_ambiguity_count: 3,
    };
    repository.save_summary(&summary).await?;
    assert_eq!(repository.summary(run_id).await?, summary);
    assert!(matches!(
        repository
            .save_summary(&CrawlExecutionSummary {
                in_scope_pages_completed: 11,
                ..summary
            })
            .await,
        Err(CrawlExecutionRepositoryError::CompletedExceedsPlanned)
    ));
    assert!(matches!(
        repository
            .save_summary(&CrawlExecutionSummary {
                in_scope_pages_planned: u64::MAX,
                in_scope_pages_completed: 0,
                ..summary
            })
            .await,
        Err(CrawlExecutionRepositoryError::CounterOutOfRange)
    ));

    let first = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/summary",
        "https://example.test/z",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    let second = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/summary",
        "https://example.test/a",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    repository.persist(&first).await?;
    repository.persist(&second).await?;
    let listed = repository.list_for_run(run_id).await?;
    assert_eq!(
        listed
            .iter()
            .map(|item| item.canonical_url.as_str())
            .collect::<Vec<_>>(),
        ["https://example.test/a", "https://example.test/z"]
    );
    Ok(())
}

#[tokio::test]
async fn malformed_result_and_summary_state_fails_closed_on_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database, run_id) = quick_setup("https://example.test/corruption").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let expected = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/corruption",
        "https://example.test/corruption",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    repository.persist(&expected).await?;
    let other_run_id = CrawlRunId::new();
    CrawlRunRepository::new(&database)
        .create(
            other_run_id,
            CrawlRunStatus::Queued,
            &quick_snapshot("https://other.test/corruption")?,
        )
        .await?;
    let connection = raw_connection(&directory).await?;
    connection
        .execute(
            "UPDATE crawl_execution_results SET crawl_run_id = ?1 WHERE id = ?2",
            (other_run_id.to_string(), expected.id.to_string()),
        )
        .await?;
    assert!(matches!(
        repository.read(expected.id).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));
    connection
        .execute(
            "UPDATE crawl_execution_results SET crawl_run_id = ?1, canonical_url = ?2 WHERE id = ?3",
            (
                run_id.to_string(),
                "not-an-http-url",
                expected.id.to_string(),
            ),
        )
        .await?;
    assert!(matches!(
        repository.read(expected.id).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));
    connection
        .execute(
            "UPDATE crawl_execution_results SET id = ?1 WHERE id = ?2",
            ("not-a-uuidv7", expected.id.to_string()),
        )
        .await?;
    assert!(matches!(
        repository.list_for_run(run_id).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));

    let (directory, database, run_id) =
        quick_setup("https://example.test/summary-corruption").await?;
    let repository = CrawlExecutionRepository::new(&database);
    repository
        .save_summary(&CrawlExecutionSummary {
            crawl_run_id: run_id,
            in_scope_pages_planned: 1,
            in_scope_pages_completed: 0,
            pagination_truncation_count: 0,
            unresolved_partial_work_count: 0,
            page_type_ambiguity_count: 0,
        })
        .await?;
    let connection = raw_connection(&directory).await?;
    connection
        .execute_batch(
            "DROP TABLE crawl_execution_summaries;
             CREATE TABLE crawl_execution_summaries (crawl_run_id TEXT, in_scope_pages_planned INTEGER, in_scope_pages_completed INTEGER, pagination_truncation_count INTEGER, unresolved_partial_work_count INTEGER, page_type_ambiguity_count INTEGER);",
        )
        .await?;
    connection
        .execute(
            "INSERT INTO crawl_execution_summaries VALUES (?1, -1, 2, 0, 0, 0)",
            [run_id.to_string()],
        )
        .await?;
    assert!(matches!(
        repository.summary(run_id).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));
    Ok(())
}

#[tokio::test]
async fn execution_repository_rejects_tampered_run_snapshot_projections()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database, run_id) =
        quick_setup("https://example.test/snapshot-integrity").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let expected = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/snapshot-integrity",
        "https://example.test/snapshot-integrity",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    repository.persist(&expected).await?;

    let connection = raw_connection(&directory).await?;
    connection
        .execute_batch("DROP TRIGGER crawl_runs_snapshot_immutable")
        .await?;
    connection
        .execute(
            "UPDATE crawl_runs SET snapshot_hash = ?1 WHERE id = ?2",
            ("0".repeat(64), run_id.to_string()),
        )
        .await?;

    assert!(matches!(
        repository.read(expected.id).await,
        Err(CrawlExecutionRepositoryError::CorruptState)
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_outcome_and_metadata_inputs_are_rejected_before_persistence()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/input").await?;
    let repository = CrawlExecutionRepository::new(&database);
    let mut invalid = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/input",
        "https://example.test/input",
        None,
        CrawlExecutionOutcome::Completed,
        Some(CrawlExecutionErrorCode::Timeout),
    );
    assert!(matches!(
        repository.persist(&invalid).await,
        Err(CrawlExecutionRepositoryError::InvalidInput(_))
    ));
    invalid.error_code = None;
    invalid.content_length_bytes = Some(u64::MAX);
    assert!(matches!(
        repository.persist(&invalid).await,
        Err(CrawlExecutionRepositoryError::CounterOutOfRange)
    ));
    invalid.content_length_bytes = None;
    invalid.media_type = Some(" text/html".to_owned());
    assert!(matches!(
        repository.persist(&invalid).await,
        Err(CrawlExecutionRepositoryError::InvalidInput(_))
    ));
    Ok(())
}

#[tokio::test]
async fn source_status_and_artifact_repository_contracts_remain_separate()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = execution_setup().await?;
    assert_eq!(fixture.source.status, SourceStatus::Active);
    let artifact_id = ArtifactId::new();
    insert_artifact(
        &fixture.directory,
        artifact_id,
        fixture.run_id,
        Some(fixture.source.id),
    )
    .await?;
    let path = ArtifactRepository::new(&fixture.database)
        .safe_relative_path(artifact_id)
        .await?;
    assert_eq!(
        std::path::Path::new(&path)
            .extension()
            .and_then(|extension| extension.to_str()),
        Some("html")
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn traversal_reconstruction_uses_persisted_order_parent_and_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/root").await?;
    let control = CrawlTraversalControl {
        crawl_run_id: run_id,
        consumed_bytes: 17,
        raw_link_count: 2,
        duplicate_count: 0,
        robots_excluded_count: 0,
        provider_error_count: 0,
        external_url_count: 0,
        blocked_url_count: 0,
        peak_expansion_count: 2,
        elapsed_millis: 3,
        time_budget_hit: false,
        duration_work_not_expanded: false,
        pagination_truncation_count: 0,
        next_admission_sequence: 10,
    };
    let root = CrawlUrlStateRecord {
        id: "work-root".to_owned(),
        crawl_run_id: run_id,
        canonical_url: "https://example.test/root".to_owned(),
        first_discovered_url_id: None,
        requested_url: "https://example.test/root".to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(8),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let child = CrawlUrlStateRecord {
        id: "work-child".to_owned(),
        crawl_run_id: run_id,
        canonical_url: "https://example.test/child".to_owned(),
        first_discovered_url_id: None,
        requested_url: "https://example.test/child".to_owned(),
        parent_url_state_id: Some(root.id.clone()),
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(3),
        depth: Some(1),
        target_page_type_id: None,
        transition_id: None,
        pagination: true,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Failed),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let repository = CrawlTraversalRepository::new(&database);
    repository
        .initialize_run_state(run_id, &[root.clone(), child], &control)
        .await?;

    let reconstructed = repository.reconstruct_recovery_state(run_id).await?;
    assert_eq!(reconstructed.control, control);
    assert_eq!(
        reconstructed
            .work
            .iter()
            .map(|state| state.id.as_str())
            .collect::<Vec<_>>(),
        vec!["work-root", "work-child"]
    );
    assert_eq!(
        reconstructed.work[1].parent_url_state_id.as_deref(),
        Some("work-root")
    );
    assert!(reconstructed.work[1].pagination);

    let mut source_job = NewJob::new(JobKind::new("PRODUCTION_CRAWL")?, 0, 0, 1)?;
    source_job.crawl_run_id = Some(run_id.to_string());
    JobRepository::new(&database)
        .enqueue(&source_job, 0)
        .await?;
    let source_acquired = JobRepository::new(&database)
        .acquire_next("recovery-action-source", 0, 30)
        .await?
        .ok_or("source job was not acquired")?;
    let source_lease = source_acquired
        .job
        .lease
        .clone()
        .ok_or("source lease missing")?;
    JobRepository::new(&database)
        .cancel(&source_job.id, &source_lease, 1)
        .await?;
    let action_job = JobRepository::new(&database)
        .enqueue_action_child(
            &source_job.id,
            JobKind::new("RETRY_FAILED_PARTS")?,
            2,
            erabi_db::repositories::ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;
    let action_acquired = JobRepository::new(&database)
        .acquire_next("recovery-action-worker", 2, 30)
        .await?
        .ok_or("action job was not acquired")?;
    let selection = repository
        .prepare_recovery_action(
            &action_job.id,
            &action_acquired.attempt.id,
            run_id,
            CrawlRecoveryActionKind::RetryFailedParts,
            2,
        )
        .await?;
    assert_eq!(selection.state_ids, vec!["work-child"]);
    let replay = repository
        .prepare_recovery_action(
            &action_job.id,
            &action_acquired.attempt.id,
            run_id,
            CrawlRecoveryActionKind::RetryFailedParts,
            2,
        )
        .await?;
    assert_eq!(replay, selection);
    let retried = repository.reconstruct_recovery_state(run_id).await?;
    assert_eq!(retried.work[0].work_generation, 0);
    assert_eq!(
        retried.work[0].current_work_state,
        Some(CrawlWorkState::Pending)
    );
    assert_eq!(retried.work[1].work_generation, 1);
    assert_eq!(
        retried.work[1].current_work_state,
        Some(CrawlWorkState::Pending)
    );

    let action_lease = action_acquired
        .job
        .lease
        .clone()
        .ok_or("action lease missing")?;
    JobRepository::new(&database)
        .cancel(&action_job.id, &action_lease, 3)
        .await?;
    let generic_action = JobRepository::new(&database)
        .enqueue_action_child(
            &source_job.id,
            JobKind::new("RETRY")?,
            4,
            erabi_db::repositories::ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;
    let generic_acquired = JobRepository::new(&database)
        .acquire_next("generic-recovery-action-worker", 4, 30)
        .await?
        .ok_or("generic action job was not acquired")?;
    let generic = repository
        .prepare_recovery_action(
            &generic_action.id,
            &generic_acquired.attempt.id,
            run_id,
            CrawlRecoveryActionKind::Retry,
            4,
        )
        .await?;
    assert_eq!(generic.state_ids, vec!["work-root", "work-child"]);
    let generic_replay = repository
        .prepare_recovery_action(
            &generic_action.id,
            &generic_acquired.attempt.id,
            run_id,
            CrawlRecoveryActionKind::Retry,
            4,
        )
        .await?;
    assert_eq!(generic_replay, generic);
    let distinct_retry_state = repository.reconstruct_recovery_state(run_id).await?;
    assert_eq!(distinct_retry_state.work[0].work_generation, 1);
    assert_eq!(distinct_retry_state.work[1].work_generation, 2);
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn empty_recovery_action_selection_commits_action_record()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/empty-recovery").await?;
    let mut source_job = NewJob::new(JobKind::new("PRODUCTION_CRAWL")?, 0, 0, 1)?;
    source_job.crawl_run_id = Some(run_id.to_string());
    let jobs = JobRepository::new(&database);
    jobs.enqueue(&source_job, 0).await?;
    let source_acquired = jobs
        .acquire_next("empty-recovery-source", 0, 30)
        .await?
        .ok_or("source job was not acquired")?;
    let source_lease = source_acquired
        .job
        .lease
        .clone()
        .ok_or("source lease missing")?;
    jobs.cancel(&source_job.id, &source_lease, 1).await?;
    let action_job = jobs
        .enqueue_action_child(
            &source_job.id,
            JobKind::new("RETRY")?,
            2,
            erabi_db::repositories::ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;
    let action_acquired = jobs
        .acquire_next("empty-recovery-worker", 2, 30)
        .await?
        .ok_or("action job was not acquired")?;
    let action_attempt_id = action_acquired.attempt.id.clone();
    let repository = CrawlTraversalRepository::new(&database);

    let selection = repository
        .prepare_recovery_action(
            &action_job.id,
            &action_attempt_id,
            run_id,
            CrawlRecoveryActionKind::Retry,
            2,
        )
        .await?;
    assert!(selection.state_ids.is_empty());

    let late_work = CrawlUrlStateRecord {
        id: "late-recovery-work".to_owned(),
        crawl_run_id: run_id,
        canonical_url: "https://example.test/empty-recovery/late".to_owned(),
        first_discovered_url_id: None,
        requested_url: "https://example.test/empty-recovery/late".to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(0),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let control = CrawlTraversalControl {
        crawl_run_id: run_id,
        consumed_bytes: 0,
        raw_link_count: 0,
        duplicate_count: 0,
        robots_excluded_count: 0,
        provider_error_count: 0,
        external_url_count: 0,
        blocked_url_count: 0,
        peak_expansion_count: 0,
        elapsed_millis: 0,
        time_budget_hit: false,
        duration_work_not_expanded: false,
        pagination_truncation_count: 0,
        next_admission_sequence: 1,
    };
    repository
        .initialize_run_state(run_id, &[late_work], &control)
        .await?;

    let replay = repository
        .prepare_recovery_action(
            &action_job.id,
            &action_attempt_id,
            run_id,
            CrawlRecoveryActionKind::Retry,
            2,
        )
        .await?;
    assert_eq!(replay, selection);
    let reconstructed = repository.reconstruct_recovery_state(run_id).await?;
    assert_eq!(reconstructed.work.len(), 1);
    assert_eq!(reconstructed.work[0].id, "late-recovery-work");
    Ok(())
}

#[tokio::test]
async fn discovery_evidence_replay_is_idempotent_and_conflicts_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/replay").await?;
    let control = CrawlTraversalControl {
        crawl_run_id: run_id,
        consumed_bytes: 0,
        raw_link_count: 1,
        duplicate_count: 0,
        robots_excluded_count: 0,
        provider_error_count: 0,
        external_url_count: 0,
        blocked_url_count: 0,
        peak_expansion_count: 0,
        elapsed_millis: 0,
        time_budget_hit: false,
        duration_work_not_expanded: false,
        pagination_truncation_count: 0,
        next_admission_sequence: 1,
    };
    let root = CrawlUrlStateRecord {
        id: "replay-root".to_owned(),
        crawl_run_id: run_id,
        canonical_url: "https://example.test/replay".to_owned(),
        first_discovered_url_id: None,
        requested_url: "https://example.test/replay".to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(0),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let repository = CrawlTraversalRepository::new(&database);
    repository
        .initialize_run_state(run_id, &[root], &control)
        .await?;
    let evidence = DiscoveredUrlRecord {
        id: uuid::Uuid::now_v7().to_string(),
        crawl_run_id: run_id,
        source_id: None,
        raw_href: Some("/child".to_owned()),
        original_url: "https://example.test/child".to_owned(),
        canonical_url: "https://example.test/child".to_owned(),
        status: "ADMITTED".to_owned(),
        discovered_at: "unix:1".to_owned(),
        detail: serde_json::json!({"origin":"DISCOVERY_PATH","stable":true}),
    };
    repository
        .apply_discovery_delta_with_evidence(
            run_id,
            std::slice::from_ref(&evidence),
            &[],
            &control,
            &[],
        )
        .await?;
    repository
        .apply_discovery_delta_with_evidence(
            run_id,
            std::slice::from_ref(&evidence),
            &[],
            &control,
            &[],
        )
        .await?;
    assert_eq!(
        CrawlRunRepository::new(&database)
            .discovered_urls(run_id)
            .await?
            .len(),
        1
    );
    let mut conflicting = evidence;
    conflicting.status = "PRESERVE_ONLY".to_owned();
    assert!(
        repository
            .apply_discovery_delta_with_evidence(
                run_id,
                std::slice::from_ref(&conflicting),
                &[],
                &control,
                &[],
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn initial_root_evidence_and_checkpoint_replay_as_one_idempotent_phase()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/seed").await?;
    let snapshot = CrawlRunRepository::new(&database).snapshot(run_id).await?;
    let mut job = NewJob::new(JobKind::new("QUICK_SCRAPE")?, 0, 0, 1)?;
    job.crawl_run_id = Some(run_id.to_string());
    let jobs = JobRepository::new(&database);
    jobs.enqueue(&job, 0).await?;
    let acquired = jobs
        .acquire_next("initialization-worker", 0, 30)
        .await?
        .ok_or("initialization job was not acquired")?;
    let lease = acquired
        .job
        .lease
        .clone()
        .ok_or("initialization lease missing")?;
    let evidence_id = uuid::Uuid::now_v7().to_string();
    let root = CrawlUrlStateRecord {
        id: "seed-root".to_owned(),
        crawl_run_id: run_id,
        canonical_url: "https://example.test/seed".to_owned(),
        first_discovered_url_id: Some(evidence_id.clone()),
        requested_url: "https://example.test/seed".to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(0),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let control = CrawlTraversalControl {
        crawl_run_id: run_id,
        consumed_bytes: 0,
        raw_link_count: 0,
        duplicate_count: 0,
        robots_excluded_count: 0,
        provider_error_count: 0,
        external_url_count: 0,
        blocked_url_count: 0,
        peak_expansion_count: 0,
        elapsed_millis: 0,
        time_budget_hit: false,
        duration_work_not_expanded: false,
        pagination_truncation_count: 0,
        next_admission_sequence: 1,
    };
    let evidence_a = DiscoveredUrlRecord {
        id: evidence_id,
        crawl_run_id: run_id,
        source_id: None,
        raw_href: None,
        original_url: "https://example.test/seed".to_owned(),
        canonical_url: "https://example.test/seed".to_owned(),
        status: "ADMITTED".to_owned(),
        discovered_at: "unix:1".to_owned(),
        detail: serde_json::json!({"origin":"SEED","seed_ids":[]}),
    };
    let evidence_b = DiscoveredUrlRecord {
        id: uuid::Uuid::now_v7().to_string(),
        crawl_run_id: run_id,
        source_id: None,
        raw_href: Some("/child".to_owned()),
        original_url: "https://example.test/child".to_owned(),
        canonical_url: "https://example.test/child".to_owned(),
        status: "ADMITTED".to_owned(),
        discovered_at: "unix:2".to_owned(),
        detail: serde_json::json!({"origin":"DISCOVERY_PATH","parent":"https://example.test/seed"}),
    };
    let checkpoint = CheckpointEnvelope::new(
        CheckpointIdentity::new(
            run_id.to_string(),
            snapshot.snapshot_hash(),
            snapshot.checkpoint_compatibility_hash(),
        )?,
        CheckpointPayloadKind::new("TEST_PAYLOAD")?,
        serde_json::json!({}),
    )?;
    let projection = CrawlTraversalSemanticProjection {
        url_states: Vec::new(),
        page_type_counts: Vec::new(),
    };
    let repository = CrawlTraversalRepository::new(&database);
    repository
        .initialize_run_state_with_checkpoint_and_evidence(
            run_id,
            std::slice::from_ref(&root),
            std::slice::from_ref(&evidence_a),
            &control,
            &projection,
            &job.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint,
            1,
        )
        .await?;
    repository
        .initialize_run_state_with_checkpoint_and_evidence(
            run_id,
            std::slice::from_ref(&root),
            std::slice::from_ref(&evidence_b),
            &control,
            &projection,
            &job.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint,
            2,
        )
        .await?;
    let discovered_urls = CrawlRunRepository::new(&database)
        .discovered_urls(run_id)
        .await?;
    assert_eq!(discovered_urls.len(), 2);
    assert!(
        discovered_urls
            .iter()
            .any(|record| record.id == evidence_a.id)
    );
    assert!(
        discovered_urls
            .iter()
            .any(|record| record.id == evidence_b.id)
    );
    assert_eq!(jobs.checkpoints(&job.id).await?.len(), 1);
    let reconstructed = repository.reconstruct_recovery_state(run_id).await?;
    assert_eq!(reconstructed.control, control);
    assert_eq!(reconstructed.work, vec![root]);
    assert_eq!(
        jobs.latest_checkpoint(&job.id)
            .await?
            .as_ref()
            .map(|record| record.created_at),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn current_execution_write_requires_current_generation_and_attempt_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database, run_id) = quick_setup("https://example.test/generation").await?;
    let control = CrawlTraversalControl {
        crawl_run_id: run_id,
        consumed_bytes: 0,
        raw_link_count: 0,
        duplicate_count: 0,
        robots_excluded_count: 0,
        provider_error_count: 0,
        external_url_count: 0,
        blocked_url_count: 0,
        peak_expansion_count: 0,
        elapsed_millis: 0,
        time_budget_hit: false,
        duration_work_not_expanded: false,
        pagination_truncation_count: 0,
        next_admission_sequence: 1,
    };
    let root = CrawlUrlStateRecord {
        id: "generation-root".to_owned(),
        crawl_run_id: run_id,
        canonical_url: "https://example.test/generation".to_owned(),
        first_discovered_url_id: None,
        requested_url: "https://example.test/generation".to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(0),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let traversal = CrawlTraversalRepository::new(&database);
    traversal
        .initialize_run_state(run_id, &[root], &control)
        .await?;

    let mut source = NewJob::new(JobKind::new("QUICK_SCRAPE")?, 0, 0, 2)?;
    source.crawl_run_id = Some(run_id.to_string());
    let jobs = JobRepository::new(&database);
    jobs.enqueue(&source, 0).await?;
    let source_acquired = jobs
        .acquire_next("generation-source-worker", 0, 30)
        .await?
        .ok_or("source job was not acquired")?;
    let source_lease = source_acquired
        .job
        .lease
        .clone()
        .ok_or("source lease missing")?;
    jobs.cancel(&source.id, &source_lease, 1).await?;
    let action = jobs
        .enqueue_action_child(
            &source.id,
            JobKind::new("RETRY")?,
            2,
            erabi_db::repositories::ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;
    let action_acquired = jobs
        .acquire_next("generation-action-worker", 2, 30)
        .await?
        .ok_or("action job was not acquired")?;
    traversal
        .prepare_recovery_action(
            &action.id,
            &action_acquired.attempt.id,
            run_id,
            CrawlRecoveryActionKind::Retry,
            2,
        )
        .await?;

    let execution_repository = CrawlExecutionRepository::new(&database);
    let stale_record = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/generation",
        "https://example.test/generation",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    assert!(matches!(
        execution_repository
            .persist_current_work(
                &stale_record,
                "generation-root",
                &source.id,
                &source_acquired.attempt.id,
                CrawlWorkState::Completed,
                1,
                2,
            )
            .await,
        Err(CrawlExecutionRepositoryError::InvalidReference)
    ));
    let current_record = record(
        CrawlExecutionId::new(),
        run_id,
        "https://example.test/generation",
        "https://example.test/generation",
        None,
        CrawlExecutionOutcome::Completed,
        None,
    );
    execution_repository
        .persist_current_work(
            &current_record,
            "generation-root",
            &action.id,
            &action_acquired.attempt.id,
            CrawlWorkState::Completed,
            1,
            2,
        )
        .await?;
    let durable = traversal.reconstruct_recovery_state(run_id).await?;
    let current = durable
        .work
        .iter()
        .find(|state| state.id == "generation-root")
        .ok_or("generation state missing")?;
    let current_execution_id = current_record.id.to_string();
    assert_eq!(current.work_generation, 1);
    assert_eq!(current.current_work_state, Some(CrawlWorkState::Completed));
    assert_eq!(
        current.current_execution_id.as_deref(),
        Some(current_execution_id.as_str())
    );
    Ok(())
}

#[test]
fn historical_migrations_have_not_changed() -> Result<(), Box<dyn std::error::Error>> {
    // Expected values are SHA-256 hashes of the canonical LF migration bytes.
    let expected = [
        (
            "../../migrations/0001_system.sql",
            "4F4FE87D479DCE142FE149E47FF54797ED972DB7945925933188B2FF3AF075B5",
        ),
        (
            "../../migrations/0002_crawler_core.sql",
            "05083743118DE67761233E0125D11A01ADD486705101B5C74BB787ED38F92A4E",
        ),
        (
            "../../migrations/0003_runs.sql",
            "A14114EEBD509D5D0A199E318B372B0D80D3F17BCF19752D839CBBB9E8BCFA5F",
        ),
        (
            "../../migrations/0004_jobs.sql",
            "A89172280D20B2BD35170BF705F46DD758CAFCB71CA92461AD7B3FBAD17E1882",
        ),
        (
            "../../migrations/0005_crawl_execution.sql",
            "28D347FAFA9886D82DD502FA619322270FDF15989EE6D15D0CDA48BC2772EC5E",
        ),
        (
            "../../migrations/0006_crawl_traversal_state.sql",
            "0D1B952B293CEA9DB376233DAC789315CE76A5E6E413E0724E5F5BEEF577CC61",
        ),
    ];
    for (relative_path, expected_hash) in expected {
        let bytes =
            std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path))?;
        let mut actual = String::new();
        for byte in Sha256::digest(bytes) {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            actual.push(HEX[(byte >> 4) as usize] as char);
            actual.push(HEX[(byte & 0x0F) as usize] as char);
        }
        assert_eq!(
            actual, expected_hash,
            "historical migration changed: {relative_path}"
        );
    }
    Ok(())
}
