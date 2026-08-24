use std::sync::Mutex;

use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{JobId, JobKind, JobRepository, NewJob},
};
use erabi_jobs::{
    NewProgressEvent, ProgressEvent, ProgressKey, ProgressMetadata, ProgressPublication,
    ProgressPublisher, ProgressPublisherError, ProgressReplayRequest, ProgressRepository,
    ProgressService, ProgressServiceError,
};

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn new_job() -> Result<NewJob, Box<dyn std::error::Error>> {
    Ok(NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, 1)?)
}

fn progress(job_id: JobId) -> Result<NewProgressEvent, Box<dyn std::error::Error>> {
    Ok(NewProgressEvent::new(
        job_id,
        ProgressKey::new("LOADING")?,
        ProgressMetadata::default(),
    ))
}

struct AssertCommittedPublisher<'database> {
    repository: ProgressRepository<'database>,
    observed: Mutex<Vec<ProgressEvent>>,
}

impl ProgressPublisher for AssertCommittedPublisher<'_> {
    async fn publish(&self, event: ProgressEvent) -> Result<(), ProgressPublisherError> {
        let page = self
            .repository
            .replay(
                &event.job_id,
                ProgressReplayRequest::new(None, 16)
                    .map_err(|_| ProgressPublisherError::NotificationFailed)?,
            )
            .await
            .map_err(|_| ProgressPublisherError::NotificationFailed)?;
        if page.events.iter().any(|stored| stored.id == event.id) {
            self.observed
                .lock()
                .map_err(|_| ProgressPublisherError::NotificationFailed)?
                .push(event);
            Ok(())
        } else {
            Err(ProgressPublisherError::NotificationFailed)
        }
    }
}

struct FailingPublisher;

impl ProgressPublisher for FailingPublisher {
    fn publish(
        &self,
        _event: ProgressEvent,
    ) -> impl Future<Output = Result<(), ProgressPublisherError>> + Send {
        std::future::ready(Err(ProgressPublisherError::NotificationFailed))
    }
}

#[tokio::test]
async fn service_commits_progress_before_live_notification()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let job = new_job()?;
    jobs.enqueue(&job, 0).await?;
    let service = ProgressService::new(&database);
    let publisher = AssertCommittedPublisher {
        repository: ProgressRepository::new(&database),
        observed: Mutex::new(Vec::new()),
    };

    let result = service
        .append_and_publish_at(&publisher, &progress(job.id.clone())?, 10)
        .await?;
    let ProgressPublication::Published(event) = result else {
        return Err("expected successful live publication".into());
    };
    let observed = publisher
        .observed
        .lock()
        .map_err(|_| "publisher lock poisoned")?;
    assert_eq!(observed.as_slice(), &[event]);
    Ok(())
}

#[tokio::test]
async fn failed_durable_append_is_never_published_live() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let service = ProgressService::new(&database);
    let publisher = AssertCommittedPublisher {
        repository: ProgressRepository::new(&database),
        observed: Mutex::new(Vec::new()),
    };

    assert!(matches!(
        service
            .append_and_publish_at(&publisher, &progress(JobId::new())?, 10)
            .await,
        Err(ProgressServiceError::Repository(_))
    ));
    assert!(
        publisher
            .observed
            .lock()
            .map_err(|_| "publisher lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn live_notification_failure_preserves_the_durable_replay_event()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let job = new_job()?;
    jobs.enqueue(&job, 0).await?;
    let service = ProgressService::new(&database);

    let result = service
        .append_and_publish_at(&FailingPublisher, &progress(job.id.clone())?, 10)
        .await?;
    let ProgressPublication::DurableOnly { event, .. } = result else {
        return Err("expected durable-only publication result".into());
    };
    let replay = service
        .replay(&job.id, ProgressReplayRequest::new(None, 16)?)
        .await?;
    assert_eq!(replay.events, vec![event]);
    Ok(())
}
