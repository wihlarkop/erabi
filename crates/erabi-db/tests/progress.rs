use std::collections::BTreeMap;

use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{
        JobId, JobKind, JobRepository, NewJob, NewProgressEvent, ProgressAttemptId, ProgressKey,
        ProgressMetadata, ProgressMetadataCode, ProgressMetadataKey, ProgressMetadataValue,
        ProgressReplayRequest, ProgressRepository, ProgressRepositoryError, ProgressSequence,
        ProgressTerminalState,
    },
};

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn new_job() -> Result<NewJob, ProgressRepositoryError> {
    NewJob::new(
        JobKind::new("TEST_WORK").map_err(|_| ProgressRepositoryError::ProgressInvariant)?,
        0,
        0,
        2,
    )
    .map_err(|_| ProgressRepositoryError::ProgressInvariant)
}

fn progress(job_id: JobId, key: &str) -> Result<NewProgressEvent, ProgressRepositoryError> {
    Ok(NewProgressEvent::new(
        job_id,
        ProgressKey::new(key)?,
        ProgressMetadata::default(),
    ))
}

#[tokio::test]
async fn first_and_sequential_progress_events_receive_monotonic_sequences()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let repository = ProgressRepository::new(&database);
    let job = new_job()?;
    jobs.enqueue(&job, 0).await?;

    let first = repository
        .append_at(&progress(job.id.clone(), "LOADING")?, 10)
        .await?;
    let second = repository
        .append_at(&progress(job.id.clone(), "RENDERING")?, 11)
        .await?;

    assert_eq!(first.sequence.get(), 1);
    assert_eq!(second.sequence.get(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_job_appends_receive_unique_ordered_sequences()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let repository = ProgressRepository::new(&database);
    let job = new_job()?;
    jobs.enqueue(&job, 0).await?;

    let loading = progress(job.id.clone(), "LOADING")?;
    let rendering = progress(job.id.clone(), "RENDERING")?;
    let left = repository.append_at(&loading, 10);
    let right = repository.append_at(&rendering, 10);
    let (left, right) = tokio::join!(left, right);
    let mut sequences = [left?.sequence.get(), right?.sequence.get()];
    sequences.sort_unstable();

    assert_eq!(sequences, [1, 2]);
    Ok(())
}

#[tokio::test]
async fn different_jobs_have_independent_progress_sequences()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let repository = ProgressRepository::new(&database);
    let first_job = new_job()?;
    let second_job = new_job()?;
    jobs.enqueue(&first_job, 0).await?;
    jobs.enqueue(&second_job, 0).await?;

    let first = repository
        .append_at(&progress(first_job.id.clone(), "LOADING")?, 0)
        .await?;
    let second = repository
        .append_at(&progress(second_job.id.clone(), "LOADING")?, 0)
        .await?;

    assert_eq!(first.sequence.get(), 1);
    assert_eq!(second.sequence.get(), 1);
    Ok(())
}

#[tokio::test]
async fn replay_is_exclusive_ascending_bounded_and_continues_without_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let repository = ProgressRepository::new(&database);
    let job = new_job()?;
    jobs.enqueue(&job, 0).await?;
    for (key, created_at) in [
        ("LOADING", 40),
        ("RENDERING", 10),
        ("DISCOVERING", 30),
        ("EXTRACTING", 20),
    ] {
        repository
            .append_at(&progress(job.id.clone(), key)?, created_at)
            .await?;
    }

    let first_page = repository
        .replay(&job.id, ProgressReplayRequest::new(None, 2)?)
        .await?;
    assert_eq!(
        first_page
            .events
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(first_page.next_after.map(ProgressSequence::get), Some(2));

    let second_page = repository
        .replay(
            &job.id,
            ProgressReplayRequest::new(first_page.next_after, 2)?,
        )
        .await?;
    assert_eq!(
        second_page
            .events
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert_eq!(second_page.next_after, None);

    let after_first = repository
        .replay(
            &job.id,
            ProgressReplayRequest::new(Some(ProgressSequence::new(1)?), 10)?,
        )
        .await?;
    assert_eq!(
        after_first
            .events
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        [2, 3, 4]
    );
    assert!(matches!(
        repository
            .replay(&JobId::new(), ProgressReplayRequest::new(None, 2)?)
            .await,
        Err(ProgressRepositoryError::JobNotFound)
    ));
    assert!(matches!(
        repository
            .replay(
                &job.id,
                ProgressReplayRequest::new(Some(ProgressSequence::new(u64::MAX)?), 2)?,
            )
            .await,
        Err(ProgressRepositoryError::InvalidReplayRequest)
    ));
    Ok(())
}

#[tokio::test]
async fn terminal_events_close_the_durable_stream_but_remain_replayable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let repository = ProgressRepository::new(&database);
    let job = new_job()?;
    jobs.enqueue(&job, 0).await?;

    let terminal = NewProgressEvent::terminal(
        job.id.clone(),
        ProgressTerminalState::Succeeded,
        ProgressMetadata::default(),
    )?;
    let persisted = repository.append_at(&terminal, 10).await?;
    assert_eq!(persisted.terminal, Some(ProgressTerminalState::Succeeded));
    assert!(matches!(
        repository
            .append_at(&progress(job.id.clone(), "LOADING")?, 11)
            .await,
        Err(ProgressRepositoryError::TerminalStreamClosed)
    ));
    assert!(matches!(
        repository.append_at(&terminal, 12).await,
        Err(ProgressRepositoryError::TerminalStreamClosed)
    ));
    let replay = repository
        .replay(&job.id, ProgressReplayRequest::new(None, 10)?)
        .await?;
    assert_eq!(replay.events, vec![persisted]);
    Ok(())
}

#[tokio::test]
async fn attempt_linkage_rejects_an_attempt_owned_by_another_job()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let repository = ProgressRepository::new(&database);
    let first_job = new_job()?;
    let mut second_job = new_job()?;
    second_job.scheduled_at = 1;
    jobs.enqueue(&first_job, 0).await?;
    jobs.enqueue(&second_job, 0).await?;
    let acquired = jobs
        .acquire_next("worker", 0, 30)
        .await?
        .ok_or("expected a leased job")?;
    let event = progress(second_job.id.clone(), "LOADING")?
        .with_attempt(ProgressAttemptId::new(acquired.attempt.id)?);

    assert!(matches!(
        repository.append_at(&event, 1).await,
        Err(ProgressRepositoryError::AttemptJobMismatch)
    ));
    Ok(())
}

#[test]
fn metadata_and_replay_inputs_are_bounded_and_safe() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        ProgressKey::new("raw log line"),
        Err(ProgressRepositoryError::InvalidProgressKey)
    ));
    assert!(matches!(
        ProgressMetadataKey::new("authorization"),
        Err(ProgressRepositoryError::InvalidProgressMetadata)
    ));
    assert!(matches!(
        ProgressMetadataKey::new(""),
        Err(ProgressRepositoryError::InvalidProgressMetadata)
    ));
    assert!(matches!(
        ProgressMetadataCode::new("raw page body"),
        Err(ProgressRepositoryError::InvalidProgressMetadata)
    ));
    assert!(matches!(
        ProgressMetadataCode::new("API_TOKEN"),
        Err(ProgressRepositoryError::InvalidProgressMetadata)
    ));

    let entries = (0..17)
        .map(|index| {
            Ok::<_, ProgressRepositoryError>((
                ProgressMetadataKey::new(format!("count_{index}"))?,
                ProgressMetadataValue::Count(index),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    assert!(matches!(
        ProgressMetadata::new(entries),
        Err(ProgressRepositoryError::InvalidProgressMetadata)
    ));
    assert!(matches!(
        ProgressReplayRequest::new(None, 0),
        Err(ProgressRepositoryError::InvalidReplayRequest)
    ));
    assert!(matches!(
        ProgressReplayRequest::new(None, 257),
        Err(ProgressRepositoryError::InvalidReplayRequest)
    ));
    Ok(())
}
