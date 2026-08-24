//! Durable-before-live progress publication boundary.

use std::future::Future;

use erabi_db::{
    ErabiDatabase,
    repositories::{
        NewProgressEvent, ProgressEvent, ProgressReplayPage, ProgressReplayRequest,
        ProgressRepository, ProgressRepositoryError,
    },
};
use tokio::sync::broadcast;

const DEFAULT_PROGRESS_LIVE_CAPACITY: usize = 256;

/// A sanitized failure to notify an in-memory live-progress consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProgressPublisherError {
    /// The transient live-notification path could not accept the event.
    #[error("the live progress notification could not be delivered")]
    NotificationFailed,
}

/// Invalid construction of the bounded in-memory live progress fan-out.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProgressLiveHubError {
    /// A broadcast channel requires at least one retained slot.
    #[error("the live progress buffer capacity must be at least one")]
    InvalidCapacity,
}

/// Best-effort in-process fan-out for already-committed progress events.
///
/// This hub is deliberately not authoritative. Receivers that lag or reconnect
/// recover from the durable repository by sequence instead of trusting this
/// bounded in-memory buffer.
#[derive(Clone, Debug)]
pub struct ProgressLiveHub {
    sender: broadcast::Sender<ProgressEvent>,
}

impl ProgressLiveHub {
    /// Creates the default bounded live fan-out.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a live fan-out with an explicit capacity for deterministic tests
    /// or a future bounded runtime policy.
    ///
    /// # Errors
    /// Returns an error when `capacity` is zero.
    pub fn with_capacity(capacity: usize) -> Result<Self, ProgressLiveHubError> {
        if capacity == 0 {
            return Err(ProgressLiveHubError::InvalidCapacity);
        }
        let (sender, receiver) = broadcast::channel(capacity);
        drop(receiver);
        Ok(Self { sender })
    }

    /// Subscribes before durable replay so concurrent publication cannot fall
    /// into the reconnect handoff window.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ProgressEvent> {
        self.sender.subscribe()
    }

    /// Returns the current number of live subscribers for safe diagnostics and
    /// deterministic integration tests.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for ProgressLiveHub {
    fn default() -> Self {
        let (sender, receiver) = broadcast::channel(DEFAULT_PROGRESS_LIVE_CAPACITY);
        drop(receiver);
        Self { sender }
    }
}

/// Future Task 2B live-delivery seam. Implementations must not treat this as
/// authoritative storage; the durable repository is the source of replay.
pub trait ProgressPublisher: Send + Sync {
    /// Publishes an already-committed event to a best-effort live consumer.
    fn publish(
        &self,
        event: ProgressEvent,
    ) -> impl Future<Output = Result<(), ProgressPublisherError>> + Send;
}

impl ProgressPublisher for ProgressLiveHub {
    fn publish(
        &self,
        event: ProgressEvent,
    ) -> impl Future<Output = Result<(), ProgressPublisherError>> + Send {
        // A receiver-free broadcast is not a failure: durable replay remains
        // authoritative and a later subscriber can reconnect by sequence.
        let _ = self.sender.send(event);
        std::future::ready(Ok(()))
    }
}

/// Result of one durable append followed by its optional live notification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgressPublication {
    /// The event committed and the live publisher accepted it.
    Published(ProgressEvent),
    /// The event committed, but live notification failed. It remains available
    /// through durable replay and is never rolled back for this reason.
    DurableOnly {
        event: ProgressEvent,
        notification_error: ProgressPublisherError,
    },
}

/// Failure before a progress event could be committed durably.
#[derive(Debug, thiserror::Error)]
pub enum ProgressServiceError {
    /// The durable persistence boundary rejected or could not append the event.
    #[error("the durable progress operation could not complete")]
    Repository(#[from] ProgressRepositoryError),
}

/// Coarse service boundary that enforces durable commit before any future live
/// publisher observes user-facing progress.
#[derive(Clone, Copy, Debug)]
pub struct ProgressService<'database> {
    repository: ProgressRepository<'database>,
}

impl<'database> ProgressService<'database> {
    /// Creates the durable progress service over Erabi's controlled database.
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self {
            repository: ProgressRepository::new(database),
        }
    }

    /// Persists progress before publishing it to one best-effort live seam.
    ///
    /// # Errors
    /// Returns an error only when durable persistence cannot complete. A live
    /// notification failure is returned as [`ProgressPublication::DurableOnly`]
    /// because the committed event remains replayable.
    pub async fn append_and_publish_at<P: ProgressPublisher>(
        &self,
        publisher: &P,
        event: &NewProgressEvent,
        created_at: i64,
    ) -> Result<ProgressPublication, ProgressServiceError> {
        let committed = self.repository.append_at(event, created_at).await?;
        match publisher.publish(committed.clone()).await {
            Ok(()) => Ok(ProgressPublication::Published(committed)),
            Err(notification_error) => Ok(ProgressPublication::DurableOnly {
                event: committed,
                notification_error,
            }),
        }
    }

    /// Appends progress without any live notification.
    ///
    /// # Errors
    /// Returns an error when the durable repository rejects or cannot commit
    /// the event.
    pub async fn append_at(
        &self,
        event: &NewProgressEvent,
        created_at: i64,
    ) -> Result<ProgressEvent, ProgressServiceError> {
        self.repository
            .append_at(event, created_at)
            .await
            .map_err(ProgressServiceError::from)
    }

    /// Replays one bounded sequence-ordered page for a durable job stream.
    ///
    /// # Errors
    /// Returns an error for invalid replay input, unknown jobs, malformed
    /// history, or a database failure.
    pub async fn replay(
        &self,
        job_id: &erabi_db::repositories::JobId,
        request: ProgressReplayRequest,
    ) -> Result<ProgressReplayPage, ProgressServiceError> {
        self.repository
            .replay(job_id, request)
            .await
            .map_err(ProgressServiceError::from)
    }
}
