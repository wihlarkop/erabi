use erabi_db::{DbError, ErabiDatabase, Migration, MigrationFailureState, MigrationRunner};

#[tokio::test]
async fn empty_database_applies_the_complete_migration_chain()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    let runner = MigrationRunner::default();

    let report = runner.apply(&database).await?;
    assert_eq!(report.applied, ["0001", "0002", "0003", "0004", "0005"]);
    assert_eq!(
        runner
            .status(&database)
            .await?
            .into_iter()
            .map(|version| version.version)
            .collect::<Vec<_>>(),
        ["0001", "0002", "0003", "0004", "0005"]
    );
    Ok(())
}

#[tokio::test]
async fn supported_0001_baseline_migrates_to_the_current_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    let runner = MigrationRunner::default();

    assert_eq!(
        runner.apply_through(&database, "0001").await?.applied,
        ["0001"]
    );
    assert_eq!(
        runner.apply(&database).await?.applied,
        ["0002", "0003", "0004", "0005"]
    );
    Ok(())
}

#[tokio::test]
async fn invalid_migration_sql_returns_a_typed_failure_without_partial_history()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    let runner = MigrationRunner::new(vec![
        Migration::new("0001", "valid", "CREATE TABLE valid_table (id INTEGER)"),
        Migration::new("0002", "invalid", "NOT VALID SQL"),
    ])?;

    let error = runner.apply(&database).await;
    assert!(matches!(
        error,
        Err(DbError::MigrationFailure { failure })
            if failure.version.as_deref() == Some("0002")
                && failure.state == MigrationFailureState::Apply
    ));
    assert!(runner.status(&database).await?.is_empty());
    Ok(())
}

#[test]
fn migration_plan_rejects_out_of_order_versions() {
    let result = MigrationRunner::new(vec![
        Migration::new("0002", "second", "SELECT 1"),
        Migration::new("0001", "first", "SELECT 1"),
    ]);
    assert!(matches!(
        result,
        Err(DbError::MigrationFailure { failure })
            if failure.state == MigrationFailureState::InvalidPlan
    ));
}

#[test]
fn migration_directory_contains_the_historical_chain_and_task_3_migration()
-> Result<(), Box<dyn std::error::Error>> {
    let migrations = std::fs::read_dir(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"),
    )?
    .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
    .collect::<Result<Vec<_>, _>>()?;
    let mut migrations = migrations;
    migrations.sort_unstable();
    assert_eq!(
        migrations,
        [
            "0001_system.sql",
            "0002_crawler_core.sql",
            "0003_runs.sql",
            "0004_jobs.sql",
            "0005_crawl_execution.sql"
        ]
    );
    Ok(())
}
