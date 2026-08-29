use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{NewSource, SourceRepository, SourceRepositoryError},
};
use erabi_domain::{
    CanonicalizationPolicy, CollectionId, CrawlerId, CrawlerVersionId, SeedId, SourceTargetType,
};
use tempfile::TempDir;
use turso::Connection;

async fn database() -> Result<(TempDir, ErabiDatabase), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    Ok((directory, database))
}

async fn raw_connection(directory: &TempDir) -> Result<Connection, Box<dyn std::error::Error>> {
    let path = directory.path().join("erabi.db");
    let database = turso::Builder::new_local(path.to_string_lossy().as_ref())
        .build()
        .await?;
    let connection = database.connect()?;
    connection.pragma_update("foreign_keys", "ON").await?;
    Ok(connection)
}

fn source_input(
    original_url: &str,
    collection_id: Option<CollectionId>,
) -> Result<NewSource, Box<dyn std::error::Error>> {
    let canonical_url = CanonicalizationPolicy::default()
        .canonicalize(original_url)?
        .canonical_url;
    Ok(NewSource {
        collection_id,
        name: "Example source".to_owned(),
        original_url: original_url.to_owned(),
        canonical_url,
        target_type: SourceTargetType::WebPage,
    })
}

async fn insert_collection(
    connection: &Connection,
    id: CollectionId,
) -> Result<(), Box<dyn std::error::Error>> {
    connection
        .execute(
            "INSERT INTO collections (id, name, description, tags_json) VALUES (?1, ?2, NULL, ?3)",
            (id.to_string(), "Collection", "[]"),
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn creates_and_reuses_by_collection_and_canonical_url_without_overwriting_original()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database) = database().await?;
    let repository = SourceRepository::new(&database);
    let first = repository
        .create_or_reuse(&source_input(
            "HTTPS://Example.test/?utm_source=ignored",
            None,
        )?)
        .await?;
    let reused = repository
        .create_or_reuse(&source_input("https://example.test/", None)?)
        .await?;

    assert_eq!(first.id, reused.id);
    assert_eq!(
        first.original_url.as_str(),
        "https://example.test/?utm_source=ignored"
    );
    assert_eq!(first.canonical_url.as_str(), "https://example.test/");
    assert_eq!(reused.original_url, first.original_url);
    let connection = raw_connection(&directory).await?;
    let row = connection
        .prepare("SELECT original_url FROM sources WHERE id = ?1")
        .await?
        .query_row([first.id.to_string()])
        .await?;
    let stored_original: String = row.get(0)?;
    assert_eq!(stored_original, "HTTPS://Example.test/?utm_source=ignored");
    Ok(())
}

#[tokio::test]
async fn collection_is_an_identity_dimension() -> Result<(), Box<dyn std::error::Error>> {
    let (directory, database) = database().await?;
    let connection = raw_connection(&directory).await?;
    let first_collection = CollectionId::new();
    let second_collection = CollectionId::new();
    insert_collection(&connection, first_collection).await?;
    insert_collection(&connection, second_collection).await?;

    let repository = SourceRepository::new(&database);
    let first = repository
        .create_or_reuse(&source_input(
            "https://example.test/",
            Some(first_collection),
        )?)
        .await?;
    let second = repository
        .create_or_reuse(&source_input(
            "https://example.test/",
            Some(second_collection),
        )?)
        .await?;
    assert_ne!(first.id, second.id);
    assert_eq!(first.collection_id, Some(first_collection));
    assert_eq!(second.collection_id, Some(second_collection));
    Ok(())
}

#[tokio::test]
async fn duplicate_identity_fails_closed_instead_of_using_row_order()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database) = database().await?;
    let repository = SourceRepository::new(&database);
    let input = source_input("https://example.test/", None)?;
    let first = repository.create_or_reuse(&input).await?;
    let connection = raw_connection(&directory).await?;
    let duplicate_id = erabi_domain::SourceId::new();
    connection
        .execute(
            "INSERT INTO sources (id, collection_id, name, original_url, canonical_url, target_type, status) VALUES (?1, NULL, ?2, ?3, ?4, 'WEB_PAGE', 'ACTIVE')",
            (
                duplicate_id.to_string(),
                "Duplicate",
                first.original_url.as_str(),
                first.canonical_url.as_str(),
            ),
        )
        .await?;

    assert!(matches!(
        repository.create_or_reuse(&input).await,
        Err(SourceRepositoryError::CorruptState)
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_persisted_target_type_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let (directory, database) = database().await?;
    let repository = SourceRepository::new(&database);
    let source = repository
        .create_or_reuse(&source_input("https://example.test/", None)?)
        .await?;
    let connection = raw_connection(&directory).await?;
    connection
        .execute(
            "UPDATE sources SET target_type = 'NOT_A_TARGET' WHERE id = ?1",
            [source.id.to_string()],
        )
        .await?;

    assert!(matches!(
        repository.read(source.id).await,
        Err(SourceRepositoryError::CorruptState)
    ));
    Ok(())
}

#[tokio::test]
async fn file_classification_only_changes_source_target_type()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, database) = database().await?;
    let repository = SourceRepository::new(&database);
    let source = repository
        .create_or_reuse(&source_input("https://example.test/report.pdf", None)?)
        .await?;
    let file = repository.mark_file_asset(source.id).await?;
    let reread = repository.read(source.id).await?;

    assert_eq!(file.target_type, SourceTargetType::FileAsset);
    assert_eq!(reread.target_type, SourceTargetType::FileAsset);
    assert_eq!(reread.original_url, source.original_url);
    assert_eq!(reread.canonical_url, source.canonical_url);
    Ok(())
}

#[tokio::test]
async fn source_identity_operations_do_not_mutate_crawler_seeds()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database) = database().await?;
    let connection = raw_connection(&directory).await?;
    let crawler_id = CrawlerId::new();
    let version_id = CrawlerVersionId::new();
    let seed_id = SeedId::new();
    let original_url = "https://example.test/report.pdf";
    let canonical_url = CanonicalizationPolicy::default()
        .canonicalize(original_url)?
        .canonical_url;
    connection
        .execute(
            "INSERT INTO crawlers (id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id) VALUES (?1, ?2, NULL, ?3, NULL, ?4)",
            (
                crawler_id.to_string(),
                "Crawler",
                "{}",
                version_id.to_string(),
            ),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
            (version_id.to_string(), crawler_id.to_string(), "{}"),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES (?1, ?2, ?3, ?4, 1, ?5, NULL)",
            (
                seed_id.to_string(),
                version_id.to_string(),
                original_url,
                canonical_url.as_str(),
                "kept seed",
            ),
        )
        .await?;

    let repository = SourceRepository::new(&database);
    let source = repository
        .create_or_reuse(&source_input(original_url, None)?)
        .await?;
    repository.mark_file_asset(source.id).await?;
    let row = connection
        .prepare("SELECT original_url, canonical_url, enabled, label FROM seeds WHERE id = ?1")
        .await?
        .query_row([seed_id.to_string()])
        .await?;
    let stored_original: String = row.get(0)?;
    let stored_canonical: String = row.get(1)?;
    let enabled: i64 = row.get(2)?;
    let label: String = row.get(3)?;
    assert_eq!(stored_original, original_url);
    assert_eq!(stored_canonical, canonical_url.as_str());
    assert_eq!(enabled, 1);
    assert_eq!(label, "kept seed");
    assert_ne!(source.id.to_string(), seed_id.to_string());
    Ok(())
}

#[test]
fn new_source_debug_redacts_url_query_values() -> Result<(), Box<dyn std::error::Error>> {
    let source = source_input("https://example.test/path?token=secret", None)?;

    let debug = format!("{source:?}");
    assert!(!debug.contains("token=secret"));
    Ok(())
}
