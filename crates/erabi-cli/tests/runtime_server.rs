use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use erabi::{
    BootstrapConfig, BootstrapConfigError, Crawl4AiStartupHealth, GRACEFUL_SHUTDOWN_DEADLINE,
    ProcessLock, ProcessLockMetadata, RunningRuntime, RuntimeError, RuntimeOptions, StartupOutcome,
};
use erabi_api::RuntimeMode;
use erabi_db::{
    ErabiDatabase, Migration, MigrationRunner,
    repositories::{AttemptOutcome, JobKind, JobRepository, JobState, NewJob},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const TOKEN: &str = "runtime-test-shared-bearer";

fn temporary_data_dir(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("erabi-runtime-{label}-{nonce}"))
}

fn config(
    data_dir: &std::path::Path,
    host: Option<&str>,
    include_token: bool,
) -> Result<BootstrapConfig, BootstrapConfigError> {
    let mut values = BTreeMap::from([
        ("ERABI_DATA_DIR".to_owned(), data_dir.display().to_string()),
        ("ERABI_PORT".to_owned(), "0".to_owned()),
    ]);
    if let Some(host) = host {
        values.insert("ERABI_HOST".to_owned(), host.to_owned());
    }
    if include_token {
        values.insert("ERABI_ACCESS_TOKEN".to_owned(), TOKEN.to_owned());
    }
    BootstrapConfig::from_values(&values)
}

fn client_address(listener_address: SocketAddr) -> SocketAddr {
    let ip = match listener_address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listener_address.port())
}

async fn request(
    listener_address: SocketAddr,
    method: &str,
    path: &str,
    host: &str,
    authorization: Option<&str>,
    body: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(client_address(listener_address)).await?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
    }
    if let Some(authorization) = authorization {
        request.push_str("Authorization: Bearer ");
        request.push_str(authorization);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(String::from_utf8(response)?)
}

fn assert_status(response: &str, expected: u16) {
    assert!(
        response.starts_with(&format!("HTTP/1.1 {expected}")),
        "unexpected response: {response}"
    );
}

#[tokio::test]
async fn loopback_runtime_binds_serves_and_releases_its_process_lock_by_shutdown_signal()
-> Result<(), Box<dyn std::error::Error>> {
    let data_dir = temporary_data_dir("loopback");
    let runtime = RunningRuntime::start_with_options(
        config(&data_dir, None, false)?,
        RuntimeOptions::default().with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
        }),
    )
    .await?;
    let listener_address = runtime.local_address();
    let host = listener_address.to_string();
    assert!(matches!(
        runtime.startup_outcome(),
        StartupOutcome::Ready {
            crawl4ai: Crawl4AiStartupHealth::Degraded { .. }
        }
    ));

    assert_status(
        &request(listener_address, "GET", "/", &host, None, "").await?,
        200,
    );
    let readiness = request(
        listener_address,
        "GET",
        "/api/v1/readiness",
        &host,
        None,
        "",
    )
    .await?;
    assert_status(&readiness, 200);
    assert!(readiness.contains("\"status\":\"degraded\""));

    let started = Instant::now();
    let report = runtime
        .serve_until_shutdown_signal(std::future::ready(()))
        .await?;
    assert!(started.elapsed() <= GRACEFUL_SHUTDOWN_DEADLINE);
    assert_eq!(report.deadline, GRACEFUL_SHUTDOWN_DEADLINE);

    let metadata = ProcessLockMetadata::current("test", "127.0.0.1:0");
    let reclaimed = ProcessLock::acquire(&data_dir, &metadata)?;
    drop(reclaimed);
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}

#[tokio::test]
async fn remote_runtime_refuses_missing_token_and_keeps_api_protected_after_browser_bootstrap()
-> Result<(), Box<dyn std::error::Error>> {
    let missing_token_dir = temporary_data_dir("missing-token");
    let Err(error) =
        RunningRuntime::start_from_bootstrap(config(&missing_token_dir, Some("0.0.0.0"), false))
            .await
    else {
        panic!("remote startup without a token must refuse service");
    };
    assert!(matches!(
        error,
        RuntimeError::Fatal(fatal) if fatal.code == "BOOTSTRAP_CONFIGURATION_INVALID"
    ));
    assert!(!missing_token_dir.exists());

    let data_dir = temporary_data_dir("remote");
    let runtime = RunningRuntime::start_with_options(
        config(&data_dir, Some("0.0.0.0"), true)?,
        RuntimeOptions::default().with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
        }),
    )
    .await?;
    let listener_address = runtime.local_address();
    let concrete_host = format!("127.0.0.1:{}", listener_address.port());

    assert_status(
        &request(listener_address, "GET", "/", &concrete_host, None, "").await?,
        200,
    );
    assert_status(
        &request(
            listener_address,
            "GET",
            "/assets/app.js",
            &concrete_host,
            None,
            "",
        )
        .await?,
        200,
    );

    let missing = request(
        listener_address,
        "GET",
        "/api/v1/readiness",
        &concrete_host,
        None,
        "",
    )
    .await?;
    assert_status(&missing, 401);
    let wrong = request(
        listener_address,
        "GET",
        "/api/v1/readiness",
        &concrete_host,
        Some("wrong"),
        "",
    )
    .await?;
    assert_status(&wrong, 403);
    let valid = request(
        listener_address,
        "GET",
        "/api/v1/readiness",
        &concrete_host,
        Some(TOKEN),
        "",
    )
    .await?;
    assert_status(&valid, 200);

    let valid_wildcard_host_mutation = request(
        listener_address,
        "POST",
        "/api/v1/runs",
        &concrete_host,
        Some(TOKEN),
        "{}",
    )
    .await?;
    assert_status(&valid_wildcard_host_mutation, 405);
    let attacker_host_mutation = request(
        listener_address,
        "POST",
        "/api/v1/runs",
        "attacker.example.test",
        Some(TOKEN),
        "{}",
    )
    .await?;
    assert_status(&attacker_host_mutation, 400);
    assert!(attacker_host_mutation.contains("HOST_NOT_ALLOWED"));

    assert_eq!(runtime.runtime_mode(), RuntimeMode::Normal);
    let report = runtime.shutdown().await?;
    assert!(report.elapsed <= GRACEFUL_SHUTDOWN_DEADLINE);
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}

#[tokio::test]
async fn runtime_process_lock_contention_is_a_fatal_startup_error()
-> Result<(), Box<dyn std::error::Error>> {
    let data_dir = temporary_data_dir("lock-contention");
    fs::create_dir_all(&data_dir)?;
    let metadata = ProcessLockMetadata::current("test", "127.0.0.1:0");
    let held_lock = ProcessLock::acquire(&data_dir, &metadata)?;

    let Err(error) = RunningRuntime::start(config(&data_dir, None, false)?).await else {
        panic!("a live process lock must refuse runtime startup");
    };
    assert!(matches!(
        error,
        RuntimeError::Fatal(fatal) if fatal.code == "PROCESS_LOCK_UNAVAILABLE"
    ));

    drop(held_lock);
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}

#[tokio::test]
async fn recorded_migrations_with_a_missing_critical_index_enter_recovery_mode()
-> Result<(), Box<dyn std::error::Error>> {
    let data_dir = temporary_data_dir("corrupt-index");
    let runtime = RunningRuntime::start_with_options(
        config(&data_dir, None, false)?,
        RuntimeOptions::default().with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
        }),
    )
    .await?;
    runtime.shutdown().await?;

    let database_path = data_dir.join("database").join("erabi.db");
    let database = turso::Builder::new_local(database_path.to_string_lossy().as_ref())
        .build()
        .await?;
    let connection = database.connect()?;
    connection
        .execute("DROP INDEX crawler_versions_by_crawler", ())
        .await?;
    drop(connection);
    drop(database);

    let recovery = RunningRuntime::start_with_options(
        config(&data_dir, None, false)?,
        RuntimeOptions::default().with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
        }),
    )
    .await?;
    assert!(matches!(
        recovery.startup_outcome(),
        StartupOutcome::Recovery(state) if state.code == "CRITICAL_SCHEMA_OBJECT_MISSING"
    ));
    let listener_address = recovery.local_address();
    let host = listener_address.to_string();
    let diagnostics = request(
        listener_address,
        "GET",
        "/api/v1/diagnostics/status",
        &host,
        None,
        "",
    )
    .await?;
    assert_status(&diagnostics, 200);
    let blocked = request(listener_address, "POST", "/api/v1/runs", &host, None, "{}").await?;
    assert_status(&blocked, 503);
    assert!(blocked.contains("RECOVERY_MODE_MUTATION_BLOCKED"));

    recovery.shutdown().await?;
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}

#[tokio::test]
async fn migration_risk_starts_safe_recovery_http_and_crawl4ai_degradation_remains_usable()
-> Result<(), Box<dyn std::error::Error>> {
    let data_dir = temporary_data_dir("recovery");
    let migration_runner =
        MigrationRunner::new(vec![Migration::new("0001", "broken", "CREATE TABLE")])?;
    let runtime = RunningRuntime::start_with_options(
        config(&data_dir, None, false)?,
        RuntimeOptions::default()
            .with_migration_runner(migration_runner)
            .with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
                message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
            }),
    )
    .await?;
    assert!(matches!(
        runtime.startup_outcome(),
        StartupOutcome::Recovery(_)
    ));
    assert!(matches!(
        runtime.runtime_mode(),
        RuntimeMode::Recovery { .. }
    ));
    let listener_address = runtime.local_address();
    let host = listener_address.to_string();

    assert_status(
        &request(
            listener_address,
            "GET",
            "/api/v1/diagnostics/status",
            &host,
            None,
            "",
        )
        .await?,
        200,
    );
    assert_status(
        &request(listener_address, "GET", "/", &host, None, "").await?,
        200,
    );
    let blocked = request(listener_address, "POST", "/api/v1/runs", &host, None, "{}").await?;
    assert_status(&blocked, 503);
    assert!(blocked.contains("RECOVERY_MODE_MUTATION_BLOCKED"));

    runtime.shutdown().await?;
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}

#[tokio::test]
async fn plan_four_startup_hook_recovers_stale_leases_before_runtime_readiness()
-> Result<(), Box<dyn std::error::Error>> {
    let data_dir = temporary_data_dir("stale-job-recovery");
    let options = RuntimeOptions::default().with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
        message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
    });
    RunningRuntime::start_with_options(config(&data_dir, None, false)?, options.clone())
        .await?
        .shutdown()
        .await?;

    let database_path = data_dir.join("database").join("erabi.db");
    let job_id = {
        let database = ErabiDatabase::open_local(&database_path).await?;
        let jobs = JobRepository::new(&database);
        let job = NewJob::new(JobKind::new("TEST_WORK")?, 1, 0, 2)?;
        jobs.enqueue(&job, 0).await?;
        jobs.acquire_next("worker-before-restart", 0, 1)
            .await?
            .ok_or("stale job was not acquired")?;
        job.id
    };

    let runtime =
        RunningRuntime::start_with_options(config(&data_dir, None, false)?, options).await?;
    assert!(matches!(
        runtime.startup_outcome(),
        StartupOutcome::Ready { .. }
    ));
    runtime.shutdown().await?;

    let database = ErabiDatabase::open_local(&database_path).await?;
    let jobs = JobRepository::new(&database);
    assert_eq!(jobs.job(&job_id).await?.state, JobState::Queued);
    assert_eq!(
        jobs.attempts(&job_id).await?[0].outcome,
        AttemptOutcome::LeaseExpired
    );
    drop(database);
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}

#[tokio::test]
async fn corrupt_queue_ownership_enters_recovery_mode_without_auto_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let data_dir = temporary_data_dir("queue-invariant");
    let options = RuntimeOptions::default().with_crawl4ai_health(Crawl4AiStartupHealth::Degraded {
        message: "Crawl4AI intentionally unavailable for this runtime test.".to_owned(),
    });
    RunningRuntime::start_with_options(config(&data_dir, None, false)?, options.clone())
        .await?
        .shutdown()
        .await?;

    let database_path = data_dir.join("database").join("erabi.db");
    let job_id = {
        let database = ErabiDatabase::open_local(&database_path).await?;
        let jobs = JobRepository::new(&database);
        let job = NewJob::new(JobKind::new("TEST_WORK")?, 1, 0, 2)?;
        jobs.enqueue(&job, 0).await?;
        jobs.acquire_next("worker-before-corruption", 0, 100)
            .await?
            .ok_or("job was not acquired")?;
        job.id
    };
    let raw_database = turso::Builder::new_local(database_path.to_string_lossy().as_ref())
        .build()
        .await?;
    let raw_connection = raw_database.connect()?;
    raw_connection
        .execute(
            "UPDATE jobs SET lease_id = 'tampered-lease' WHERE id = ?1",
            [job_id.as_str()],
        )
        .await?;
    drop(raw_connection);
    drop(raw_database);

    let recovery =
        RunningRuntime::start_with_options(config(&data_dir, None, false)?, options).await?;
    assert!(
        matches!(
            recovery.startup_outcome(),
            StartupOutcome::Recovery(state) if state.code == "QUEUE_INVARIANT_VIOLATION"
        ),
        "unexpected startup outcome: {:#?}",
        recovery.startup_outcome()
    );
    recovery.shutdown().await?;
    fs::remove_dir_all(&data_dir)?;
    Ok(())
}
