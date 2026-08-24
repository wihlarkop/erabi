//! Cooperative cancellation signals shared by the worker and shutdown paths.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use erabi_db::repositories::JobId;
use tokio::sync::Notify;

#[derive(Debug)]
struct CancellationState {
    cancelled: std::sync::atomic::AtomicBool,
    notify: Notify,
}

/// Cloneable signal passed to active handlers. Cancellation is an observable
/// request; it never aborts the handler task by itself.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    fn new(cancelled: bool) -> Self {
        let token = Self {
            state: Arc::new(CancellationState {
                cancelled: std::sync::atomic::AtomicBool::new(cancelled),
                notify: Notify::new(),
            }),
        };
        if cancelled {
            token.state.notify.notify_waiters();
        }
        token
    }

    /// Requests cooperative cancellation for this active handler.
    pub fn cancel(&self) {
        if !self
            .state
            .cancelled
            .swap(true, std::sync::atomic::Ordering::Release)
        {
            self.state.notify.notify_waiters();
        }
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Waits until cancellation is requested, without aborting the caller.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            self.state.notify.notified().await;
        }
    }
}

#[derive(Debug, Default)]
struct ControllerState {
    requested: HashSet<JobId>,
    active: HashMap<JobId, CancellationToken>,
    shutdown_requested: bool,
}

/// Coordinates cancellation requests for queued/active jobs and process
/// shutdown. It intentionally contains no durable state; job repositories own
/// durable terminal transitions and checkpoints.
#[derive(Clone, Debug, Default)]
pub struct CancellationController {
    state: Arc<Mutex<ControllerState>>,
}

impl CancellationController {
    /// Registers an active job and returns its cooperative handler token.
    #[must_use]
    pub fn register(&self, job_id: &JobId) -> CancellationToken {
        let (token, cancel_now) = {
            let Ok(mut state) = self.state.lock() else {
                return CancellationToken::new(true);
            };
            let cancel_now = state.shutdown_requested || state.requested.contains(job_id);
            let token = CancellationToken::new(cancel_now);
            state.active.insert(job_id.clone(), token.clone());
            (token, cancel_now)
        };
        if cancel_now {
            token.cancel();
        }
        token
    }

    /// Records a request and signals the active handler when one is present.
    pub fn request(&self, job_id: &JobId) {
        let active = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            state.requested.insert(job_id.clone());
            state.active.get(job_id).cloned()
        };
        if let Some(token) = active {
            token.cancel();
        }
    }

    /// Signals every active handler and prevents the worker from scheduling
    /// new units after process shutdown begins.
    pub fn cancel_all(&self) {
        let active = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            state.shutdown_requested = true;
            state.active.values().cloned().collect::<Vec<_>>()
        };
        for token in active {
            token.cancel();
        }
    }

    /// Returns whether shutdown has closed worker scheduling.
    #[must_use]
    pub fn shutdown_requested(&self) -> bool {
        self.state
            .lock()
            .map_or(true, |state| state.shutdown_requested)
    }

    /// Removes an active token after its worker turn has reached a durable
    /// boundary. The request record remains until the job identity is retired.
    pub(crate) fn release(&self, job_id: &JobId) {
        if let Ok(mut state) = self.state.lock() {
            state.active.remove(job_id);
        }
    }
}
