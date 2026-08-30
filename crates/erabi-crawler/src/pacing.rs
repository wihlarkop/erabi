//! Shared origin-scoped crawl admission, delay, and rate-limit pacing.
//!
//! This module deliberately owns the only public crawl-admission path used by
//! later Quick Scrape and Production orchestration. Callers receive a scoped
//! permit, so cancellation, failures, and panics release concurrency capacity
//! through RAII rather than fragile manual bookkeeping.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use erabi_domain::CrawlRunSnapshot;
use tokio::{sync::Notify, time::Instant};
use url::Url;

use crate::{CrawlerAdapterError, RobotsAdmission, RobotsAdmissionDecision};

/// One upper bound used for untrusted robots delays, configured delays, and
/// normalized `Retry-After` values. It prevents a remote server (or malformed
/// snapshot) from creating an effectively permanent in-process wait.
pub const MAX_PACING_DELAY: Duration = Duration::from_mins(5);

/// The base delay for deterministic, non-aggressive rate-limit backoff.
pub const RATE_LIMIT_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// The largest backoff produced by this layer.
pub const MAX_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(60);

/// A normalized HTTP origin. It intentionally excludes paths, queries,
/// fragments, durable IDs, and caller-provided labels.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OriginKey {
    scheme: String,
    host: String,
    port: u16,
}

impl OriginKey {
    /// Builds the effective HTTP(S) origin identity for a target URL.
    ///
    /// # Errors
    /// Returns a typed error when an origin cannot be determined safely.
    pub fn from_url(url: &Url) -> Result<Self, OriginKeyError> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(OriginKeyError::UnsupportedScheme);
        }
        let host = url.host_str().ok_or(OriginKeyError::MissingHost)?;
        let port = url
            .port_or_known_default()
            .ok_or(OriginKeyError::MissingEffectivePort)?;
        if port == 0 {
            return Err(OriginKeyError::MissingEffectivePort);
        }

        Ok(Self {
            scheme: url.scheme().to_ascii_lowercase(),
            host: host.to_ascii_lowercase(),
            port,
        })
    }

    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Debug for OriginKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginKey")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

impl fmt::Display for OriginKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        write!(formatter, "{}://{}:{}", self.scheme, host, self.port)
    }
}

/// Failure to derive a normalized origin identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OriginKeyError {
    #[error("crawl pacing only supports HTTP(S) origins")]
    UnsupportedScheme,
    #[error("crawl pacing requires a URL host")]
    MissingHost,
    #[error("crawl pacing requires an effective URL port")]
    MissingEffectivePort,
}

/// Monotonic time required by pacing. Production uses Tokio's monotonic
/// instant; deterministic tests can supply [`ManualPacingClock`].
pub trait PacingClock: Send + Sync {
    fn now(&self) -> Instant;

    fn sleep_until(&self, deadline: Instant) -> PacingSleepFuture<'_>;
}

pub type PacingSleepFuture<'clock> = Pin<Box<dyn Future<Output = ()> + Send + 'clock>>;

#[derive(Debug, Default)]
pub struct TokioPacingClock;

impl PacingClock for TokioPacingClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> PacingSleepFuture<'_> {
        Box::pin(tokio::time::sleep_until(deadline))
    }
}

/// A manually advanced monotonic clock for deterministic pacing tests.
#[derive(Debug)]
pub struct ManualPacingClock {
    started_at: Instant,
    elapsed_millis: AtomicU64,
    changed: Notify,
}

impl Default for ManualPacingClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ManualPacingClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            elapsed_millis: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }

    /// Moves the clock forward without waiting on wall-clock time.
    pub fn advance(&self, duration: Duration) {
        let delta = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let _ = self
            .elapsed_millis
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_add(delta))
            });
        self.changed.notify_waiters();
    }
}

impl PacingClock for ManualPacingClock {
    fn now(&self) -> Instant {
        self.started_at
            .checked_add(Duration::from_millis(
                self.elapsed_millis.load(Ordering::SeqCst),
            ))
            .unwrap_or(self.started_at)
    }

    fn sleep_until(&self, deadline: Instant) -> PacingSleepFuture<'_> {
        Box::pin(async move {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.now() >= deadline {
                    return;
                }
                notified.await;
            }
        })
    }
}

#[derive(Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    changed: Notify,
}

/// Cooperative cancellation signal for a wait that has not yet acquired crawl
/// admission. It does not abort an in-flight request.
#[derive(Clone, Debug)]
pub struct PacingCancellation {
    state: Arc<CancellationState>,
}

impl Default for PacingCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl PacingCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                changed: Notify::new(),
            }),
        }
    }

    /// Requests cooperative cancellation for an admission wait.
    pub fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::Release) {
            self.state.changed.notify_waiters();
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// Safe, bounded timing extracted from provider-neutral or robots HTTP rate
/// limit evidence. Raw headers are never retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryAfterTiming {
    Absent,
    Honored(Duration),
    Clamped(Duration),
    Invalid,
}

impl RetryAfterTiming {
    #[must_use]
    pub fn from_provider_millis(value: Option<u64>) -> Self {
        match value {
            None => Self::Absent,
            Some(milliseconds) => Self::from_duration(Duration::from_millis(milliseconds)),
        }
    }

    /// Parses an HTTP `Retry-After` value for the robots HTTP client. The
    /// caller provides wall time only to turn a valid HTTP-date into a bounded
    /// elapsed duration; pacing itself remains monotonic.
    #[must_use]
    pub fn from_http_header(value: &str, now: SystemTime) -> Self {
        let value = value.trim();
        if value.is_empty() {
            return Self::Invalid;
        }
        if let Ok(seconds) = value.parse::<u64>() {
            return Self::from_duration(Duration::from_secs(seconds));
        }
        let Ok(deadline) = httpdate::parse_http_date(value) else {
            return Self::Invalid;
        };
        let Ok(duration) = deadline.duration_since(now) else {
            return Self::Invalid;
        };
        Self::from_duration(duration)
    }

    #[must_use]
    pub const fn delay(self) -> Option<Duration> {
        match self {
            Self::Honored(delay) | Self::Clamped(delay) => Some(delay),
            Self::Absent | Self::Invalid => None,
        }
    }

    fn from_duration(duration: Duration) -> Self {
        if duration > MAX_PACING_DELAY {
            Self::Clamped(MAX_PACING_DELAY)
        } else {
            Self::Honored(duration)
        }
    }
}

/// One completed admitted request's safe pacing evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacingOutcome {
    Success,
    Failed,
    RateLimited { retry_after: RetryAfterTiming },
}

impl PacingOutcome {
    #[must_use]
    pub fn from_adapter_error(error: &CrawlerAdapterError) -> Self {
        match error {
            CrawlerAdapterError::RateLimited { retry_after_ms } => Self::RateLimited {
                retry_after: RetryAfterTiming::from_provider_millis(*retry_after_ms),
            },
            _ => Self::Failed,
        }
    }
}

/// A caller-visible failure before a crawl request has begun.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdmissionError {
    #[error("the target is disallowed by robots policy")]
    RobotsDisallowed,
    #[error("crawl admission was cancelled")]
    Cancelled,
    #[error("resolved crawl concurrency must be positive")]
    InvalidConcurrency,
    #[error("resolved crawl request delay exceeds the bounded pacing policy")]
    RequestDelayTooLarge,
    #[error("robots admission belongs to a different origin")]
    RobotsOriginMismatch,
    #[error("pacing registration is no longer active")]
    RegistrationReleased,
    #[error("the process-wide origin pacing registry is at capacity")]
    OriginCapacityExhausted,
    #[error("pacing registration identity space is exhausted")]
    RegistrationIdExhausted,
    #[error("monotonic pacing deadline overflowed")]
    ClockOverflow,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedPacingConfiguration {
    concurrency: u32,
    request_delay: Duration,
}

impl ResolvedPacingConfiguration {
    fn from_snapshot(snapshot: &CrawlRunSnapshot) -> Result<Self, AdmissionError> {
        let concurrency = snapshot.settings().concurrency.value;
        if concurrency == 0 {
            return Err(AdmissionError::InvalidConcurrency);
        }
        let request_delay = Duration::from_millis(snapshot.settings().request_delay_ms.value);
        if request_delay > MAX_PACING_DELAY {
            return Err(AdmissionError::RequestDelayTooLarge);
        }
        Ok(Self {
            concurrency,
            request_delay,
        })
    }
}

/// The production-wide upper bound for origin pacing state. This is runtime
/// state only; it is deliberately not a mutable operational setting.
pub const MAX_ORIGIN_PACING_STATES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RegistrationId(u64);

#[derive(Debug, Default)]
struct OriginPacingState {
    in_flight: u32,
    registrations: BTreeMap<RegistrationId, u32>,
    waiting_admissions: u32,
    next_request_at: Option<Instant>,
    backoff_until: Option<Instant>,
    retry_after_until: Option<Instant>,
    consecutive_rate_limits: u8,
}

impl OriginPacingState {
    fn effective_concurrency(&self) -> Option<u32> {
        self.registrations.values().copied().min()
    }

    fn normalize_expired_safety(&mut self, now: Instant) {
        for deadline in [
            &mut self.next_request_at,
            &mut self.backoff_until,
            &mut self.retry_after_until,
        ] {
            if deadline.is_some_and(|deadline| deadline <= now) {
                *deadline = None;
            }
        }
    }

    /// This is the one authoritative retirement predicate. Do not add a
    /// second, weaker idleness test at registry insertion or cleanup sites.
    fn is_safe_to_retire(&mut self, now: Instant) -> bool {
        self.normalize_expired_safety(now);
        self.registrations.is_empty()
            && self.in_flight == 0
            && self.waiting_admissions == 0
            && self.next_request_at.is_none()
            && self.backoff_until.is_none()
            && self.retry_after_until.is_none()
    }
}

#[derive(Debug, Default)]
struct OriginState {
    pacing: Mutex<OriginPacingState>,
    changed: Notify,
}

impl OriginState {
    fn add_registration(&self, registration_id: RegistrationId, concurrency: u32) {
        let mut state = recover_lock(&self.pacing);
        let previous = state.registrations.insert(registration_id, concurrency);
        debug_assert!(previous.is_none(), "registration identifiers are unique");
        drop(state);
        self.changed.notify_waiters();
    }

    fn release_registration(&self, registration_id: RegistrationId) {
        let mut state = recover_lock(&self.pacing);
        let removed = state.registrations.remove(&registration_id);
        drop(state);
        if removed.is_some() {
            self.changed.notify_waiters();
        }
    }

    fn register_waiter(&self) {
        let mut state = recover_lock(&self.pacing);
        state.waiting_admissions = state.waiting_admissions.saturating_add(1);
    }

    fn release_waiter(&self) {
        let mut state = recover_lock(&self.pacing);
        state.waiting_admissions = state.waiting_admissions.saturating_sub(1);
        drop(state);
        self.changed.notify_waiters();
    }

    fn release(&self) {
        let mut state = recover_lock(&self.pacing);
        state.in_flight = state.in_flight.saturating_sub(1);
        drop(state);
        self.changed.notify_waiters();
    }

    fn record_outcome(&self, outcome: PacingOutcome, now: Instant) -> Result<(), AdmissionError> {
        let mut state = recover_lock(&self.pacing);
        state.normalize_expired_safety(now);
        match outcome {
            PacingOutcome::Success => state.consecutive_rate_limits = 0,
            PacingOutcome::Failed => {}
            PacingOutcome::RateLimited { retry_after } => {
                state.consecutive_rate_limits = state.consecutive_rate_limits.saturating_add(1);
                let backoff = bounded_backoff(state.consecutive_rate_limits);
                let backoff_until = checked_add(now, backoff)?;
                state.backoff_until = Some(later_deadline(state.backoff_until, backoff_until));
                if let Some(delay) = retry_after.delay() {
                    let retry_after_until = checked_add(now, delay)?;
                    state.retry_after_until =
                        Some(later_deadline(state.retry_after_until, retry_after_until));
                }
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn is_safe_to_retire(&self, now: Instant) -> bool {
        recover_lock(&self.pacing).is_safe_to_retire(now)
    }
}

fn recover_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn latest_deadline(state: &OriginPacingState) -> Option<Instant> {
    [
        state.next_request_at,
        state.backoff_until,
        state.retry_after_until,
    ]
    .into_iter()
    .flatten()
    .max()
}

fn later_deadline(current: Option<Instant>, candidate: Instant) -> Instant {
    current.map_or(candidate, |current| current.max(candidate))
}

fn checked_add(now: Instant, duration: Duration) -> Result<Instant, AdmissionError> {
    now.checked_add(duration)
        .ok_or(AdmissionError::ClockOverflow)
}

fn bounded_backoff(consecutive_rate_limits: u8) -> Duration {
    let exponent = u32::from(consecutive_rate_limits.saturating_sub(1)).min(6);
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    RATE_LIMIT_BACKOFF_BASE
        .checked_mul(multiplier)
        .unwrap_or(MAX_RATE_LIMIT_BACKOFF)
        .min(MAX_RATE_LIMIT_BACKOFF)
}

#[derive(Debug)]
struct OriginRegistryState {
    states: BTreeMap<OriginKey, Arc<OriginState>>,
    next_registration_id: Option<u64>,
}

impl Default for OriginRegistryState {
    fn default() -> Self {
        Self {
            states: BTreeMap::new(),
            next_registration_id: Some(1),
        }
    }
}

/// Process-shared origin admission state. The registry is deliberately
/// bounded; only fully inactive states may be pruned to make room.
#[derive(Debug)]
struct OriginRegistry {
    state: Mutex<OriginRegistryState>,
    capacity: usize,
}

impl OriginRegistry {
    const fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(OriginRegistryState {
                states: BTreeMap::new(),
                next_registration_id: Some(1),
            }),
            capacity,
        }
    }

    fn register(
        &self,
        origin: OriginKey,
        concurrency: u32,
        now: Instant,
    ) -> Result<(Arc<OriginState>, RegistrationId), AdmissionError> {
        let (origin_state, registration_id) = {
            let mut registry = recover_lock(&self.state);
            let origin_state = if let Some(origin_state) = registry.states.get(&origin) {
                Arc::clone(origin_state)
            } else {
                if registry.states.len() >= self.capacity {
                    Self::prune_safe_states(&mut registry, now, self.capacity);
                }
                if registry.states.len() >= self.capacity {
                    return Err(AdmissionError::OriginCapacityExhausted);
                }
                let origin_state = Arc::new(OriginState::default());
                registry.states.insert(origin, Arc::clone(&origin_state));
                origin_state
            };
            let registration_id = RegistrationId(
                registry
                    .next_registration_id
                    .ok_or(AdmissionError::RegistrationIdExhausted)?,
            );
            registry.next_registration_id = registration_id.0.checked_add(1);
            origin_state.add_registration(registration_id, concurrency);
            (origin_state, registration_id)
        };
        // A newly added restrictive registration and a released restrictive
        // registration both need to wake waiters to re-evaluate the active
        // minimum under the origin lock.
        origin_state.changed.notify_waiters();
        Ok((origin_state, registration_id))
    }

    fn prune_safe_states(state: &mut OriginRegistryState, now: Instant, capacity: usize) {
        let removable = state
            .states
            .iter()
            .filter(|(_, origin_state)| origin_state.is_safe_to_retire(now))
            .map(|(origin, _)| origin.clone())
            .collect::<Vec<_>>();
        for origin in removable {
            if state.states.len() < capacity {
                break;
            }
            state.states.remove(&origin);
        }
    }

    #[cfg(test)]
    fn contains(&self, origin: &OriginKey) -> bool {
        recover_lock(&self.state).states.contains_key(origin)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        recover_lock(&self.state).states.len()
    }
}

fn process_origin_registry() -> Arc<OriginRegistry> {
    static PROCESS_ORIGINS: OnceLock<Arc<OriginRegistry>> = OnceLock::new();
    Arc::clone(
        PROCESS_ORIGINS.get_or_init(|| Arc::new(OriginRegistry::new(MAX_ORIGIN_PACING_STATES))),
    )
}

#[derive(Clone)]
pub struct PacingService {
    registry: Arc<OriginRegistry>,
    clock: Arc<dyn PacingClock>,
}

impl Default for PacingService {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PacingService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PacingService")
            .field("shared_origin_state", &true)
            .finish_non_exhaustive()
    }
}

impl PacingService {
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: process_origin_registry(),
            clock: Arc::new(TokioPacingClock),
        }
    }

    /// Builds a service backed by the same process-wide registry with an
    /// explicit monotonic clock for crate-local deterministic tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_clock(clock: Arc<dyn PacingClock>) -> Self {
        Self {
            registry: process_origin_registry(),
            clock,
        }
    }

    /// Registers one immutable execution configuration for an origin. The
    /// returned scope remains active until dropped; it is distinct from every
    /// request-level [`AdmissionPermit`] subsequently acquired through it.
    ///
    /// # Errors
    /// Returns if immutable snapshot pacing is invalid, runtime registration
    /// IDs are exhausted, or all process-wide registry entries are protected.
    pub fn register(
        &self,
        origin: OriginKey,
        snapshot: &CrawlRunSnapshot,
    ) -> Result<PacingRegistration, AdmissionError> {
        let configuration = ResolvedPacingConfiguration::from_snapshot(snapshot)?;
        self.register_configuration(origin, configuration)
    }

    fn register_configuration(
        &self,
        origin: OriginKey,
        configuration: ResolvedPacingConfiguration,
    ) -> Result<PacingRegistration, AdmissionError> {
        let (state, registration_id) =
            self.registry
                .register(origin.clone(), configuration.concurrency, self.clock.now())?;
        Ok(PacingRegistration {
            origin,
            state,
            registration_id,
            configuration,
            clock: Arc::clone(&self.clock),
        })
    }

    pub(crate) fn clock(&self) -> Arc<dyn PacingClock> {
        Arc::clone(&self.clock)
    }
}

/// RAII ownership of one active immutable pacing configuration. It does not
/// represent an in-flight request; see [`AdmissionPermit`] for that lifetime.
pub struct PacingRegistration {
    origin: OriginKey,
    state: Arc<OriginState>,
    registration_id: RegistrationId,
    configuration: ResolvedPacingConfiguration,
    clock: Arc<dyn PacingClock>,
}

impl PacingRegistration {
    /// Acquires a request permit only for the sealed robots admission that
    /// belongs to this registration's origin.
    ///
    /// # Errors
    /// Returns an error when the admission is disallowed or belongs to a
    /// different origin, the registration has been released, or the
    /// cancellation signal wins before a permit can be acquired.
    pub async fn acquire(
        &self,
        robots: &RobotsAdmission,
        cancellation: &PacingCancellation,
    ) -> Result<AdmissionPermit, AdmissionError> {
        if robots.origin() != &self.origin {
            return Err(AdmissionError::RobotsOriginMismatch);
        }
        if matches!(robots.decision(), RobotsAdmissionDecision::Disallowed) {
            return Err(AdmissionError::RobotsDisallowed);
        }
        self.acquire_with_delay(robots.crawl_delay(), cancellation)
            .await
    }

    pub(crate) async fn acquire_robots_fetch(
        &self,
        cancellation: &PacingCancellation,
    ) -> Result<AdmissionPermit, AdmissionError> {
        self.acquire_with_delay(None, cancellation).await
    }

    async fn acquire_with_delay(
        &self,
        robots_delay: Option<Duration>,
        cancellation: &PacingCancellation,
    ) -> Result<AdmissionPermit, AdmissionError> {
        let effective_delay = self
            .configuration
            .request_delay
            .max(robots_delay.unwrap_or(Duration::ZERO));
        let state = Arc::clone(&self.state);
        // The one guard spans all loop iterations. It protects the origin
        // while a future is pending and releases exactly once on success,
        // cancellation, task abort, or panic unwind.
        let waiter = WaiterGuard::new(Arc::clone(&state));

        loop {
            if cancellation.is_cancelled() {
                return Err(AdmissionError::Cancelled);
            }

            // Register interest before inspecting state so a permit release,
            // registration release, or deadline update cannot be lost between
            // the inspection and first wait poll.
            let notified = state.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let attempt = {
                let mut pacing = recover_lock(&state.pacing);
                let now = self.clock.now();
                pacing.normalize_expired_safety(now);
                let effective_concurrency = pacing
                    .effective_concurrency()
                    .ok_or(AdmissionError::RegistrationReleased)?;
                let not_before = latest_deadline(&pacing);
                if pacing.in_flight < effective_concurrency
                    && not_before.is_none_or(|deadline| deadline <= now)
                {
                    pacing.in_flight = pacing.in_flight.saturating_add(1);
                    pacing.next_request_at = Some(checked_add(now, effective_delay)?);
                    AdmissionAttempt::Acquired
                } else {
                    match not_before.filter(|deadline| *deadline > now) {
                        Some(deadline) => AdmissionAttempt::WaitUntil(deadline),
                        None => AdmissionAttempt::WaitForPermit,
                    }
                }
            };

            match attempt {
                AdmissionAttempt::Acquired => {
                    drop(waiter);
                    return Ok(AdmissionPermit {
                        state: Arc::clone(&state),
                        clock: Arc::clone(&self.clock),
                        outcome_recorded: AtomicBool::new(false),
                    });
                }
                AdmissionAttempt::WaitUntil(deadline) => {
                    tokio::select! {
                        () = self.clock.sleep_until(deadline) => {}
                        () = &mut notified => {}
                        () = cancellation.cancelled() => return Err(AdmissionError::Cancelled),
                    }
                }
                AdmissionAttempt::WaitForPermit => {
                    tokio::select! {
                        () = &mut notified => {}
                        () = cancellation.cancelled() => return Err(AdmissionError::Cancelled),
                    }
                }
            }
        }
    }
}

impl fmt::Debug for PacingRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PacingRegistration")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

impl Drop for PacingRegistration {
    fn drop(&mut self) {
        self.state.release_registration(self.registration_id);
    }
}

struct WaiterGuard {
    state: Arc<OriginState>,
}

impl WaiterGuard {
    fn new(state: Arc<OriginState>) -> Self {
        state.register_waiter();
        Self { state }
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        self.state.release_waiter();
    }
}

#[derive(Clone, Copy, Debug)]
enum AdmissionAttempt {
    Acquired,
    WaitUntil(Instant),
    WaitForPermit,
}

/// Scoped evidence token for exactly one admitted HTTP request.
pub struct AdmissionPermit {
    state: Arc<OriginState>,
    clock: Arc<dyn PacingClock>,
    outcome_recorded: AtomicBool,
}

impl AdmissionPermit {
    /// Records the bounded outcome timing once. Repeated calls are ignored so
    /// an adapter retry path cannot multiply the origin backoff accidentally.
    ///
    /// # Errors
    /// Returns an error only if monotonic deadline arithmetic cannot be
    /// represented safely.
    pub fn record_outcome(&self, outcome: PacingOutcome) -> Result<(), AdmissionError> {
        if self.outcome_recorded.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.state.record_outcome(outcome, self.clock.now())
    }
}

impl fmt::Debug for AdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionPermit")
            .field(
                "outcome_recorded",
                &self.outcome_recorded.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.state.release();
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    macro_rules! fixture_ok {
        ($result:expr, $context:literal) => {
            match $result {
                Ok(value) => value,
                Err(error) => panic!("{}: {error}", $context),
            }
        };
    }

    fn origin(value: &str) -> OriginKey {
        let url = fixture_ok!(value.parse(), "fixture URL parses");
        fixture_ok!(OriginKey::from_url(&url), "fixture origin normalizes")
    }

    fn service(capacity: usize) -> (PacingService, Arc<ManualPacingClock>) {
        let clock = Arc::new(ManualPacingClock::new());
        (
            PacingService {
                registry: Arc::new(OriginRegistry::new(capacity)),
                clock: clock.clone(),
            },
            clock,
        )
    }

    fn registration(
        service: &PacingService,
        origin: OriginKey,
        concurrency: u32,
        request_delay: Duration,
    ) -> PacingRegistration {
        fixture_ok!(
            service.register_configuration(
                origin,
                ResolvedPacingConfiguration {
                    concurrency,
                    request_delay,
                },
            ),
            "fixture registration succeeds"
        )
    }

    #[tokio::test]
    async fn active_permit_and_waiter_liveness_protect_retirement_then_release_exactly_once() {
        let (service, _) = service(1);
        let first_origin = origin("https://permit-and-waiter.test/");
        let second_origin = origin("https://second.test/");
        let owner = registration(&service, first_origin.clone(), 1, Duration::ZERO);
        let permit = fixture_ok!(
            owner.acquire_robots_fetch(&PacingCancellation::new()).await,
            "fixture permit succeeds"
        );
        drop(owner);

        assert!(matches!(
            service.register_configuration(
                second_origin.clone(),
                ResolvedPacingConfiguration {
                    concurrency: 1,
                    request_delay: Duration::ZERO,
                },
            ),
            Err(AdmissionError::OriginCapacityExhausted)
        ));
        drop(permit);

        let waiter_owner = registration(&service, first_origin, 1, Duration::ZERO);
        let waiter_state = Arc::clone(&waiter_owner.state);
        drop(waiter_owner);
        let waiter = WaiterGuard::new(waiter_state);
        assert!(matches!(
            service.register_configuration(
                second_origin.clone(),
                ResolvedPacingConfiguration {
                    concurrency: 1,
                    request_delay: Duration::ZERO,
                },
            ),
            Err(AdmissionError::OriginCapacityExhausted)
        ));
        drop(waiter);

        let replacement = registration(&service, second_origin, 1, Duration::ZERO);
        drop(replacement);
    }

    #[tokio::test]
    async fn cancelled_waiter_releases_liveness_before_the_origin_is_pruned() {
        let (service, _) = service(1);
        let first_origin = origin("https://cancelled-waiter.test/");
        let replacement_origin = origin("https://after-cancel.test/");
        let owner = registration(&service, first_origin.clone(), 1, Duration::ZERO);
        let permit = fixture_ok!(
            owner.acquire_robots_fetch(&PacingCancellation::new()).await,
            "fixture permit succeeds"
        );
        let waiting_owner = registration(&service, first_origin, 1, Duration::ZERO);
        let cancellation = PacingCancellation::new();
        let waiter_cancellation = cancellation.clone();
        let waiter = tokio::spawn(async move {
            let result = waiting_owner
                .acquire_robots_fetch(&waiter_cancellation)
                .await;
            (waiting_owner, result)
        });
        for _ in 0..8 {
            if recover_lock(&owner.state.pacing).waiting_admissions == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(recover_lock(&owner.state.pacing).waiting_admissions, 1);
        cancellation.cancel();
        let (waiting_owner, result) = fixture_ok!(waiter.await, "waiting task joins");
        assert!(matches!(result, Err(AdmissionError::Cancelled)));
        drop(waiting_owner);
        drop(permit);
        drop(owner);

        let replacement = registration(&service, replacement_origin, 1, Duration::ZERO);
        drop(replacement);
    }

    #[tokio::test]
    async fn registry_prunes_only_safe_states_and_rejects_all_protected_capacity() {
        let (service, clock) = service(2);
        let safe_origin = origin("https://a-safe.test/");
        let active_origin = origin("https://b-active.test/");
        let replacement_origin = origin("https://c-replacement.test/");

        let safe = registration(&service, safe_origin.clone(), 1, Duration::ZERO);
        drop(safe);
        let active = registration(&service, active_origin.clone(), 1, Duration::ZERO);
        let replacement = registration(&service, replacement_origin.clone(), 1, Duration::ZERO);
        assert!(!service.registry.contains(&safe_origin));
        assert!(service.registry.contains(&active_origin));
        assert!(service.registry.contains(&replacement_origin));
        assert_eq!(service.registry.len(), 2);
        drop(replacement);

        let deadline_origin = origin("https://d-deadline.test/");
        let deadline_owner = registration(&service, deadline_origin.clone(), 1, Duration::ZERO);
        let permit = fixture_ok!(
            deadline_owner
                .acquire_robots_fetch(&PacingCancellation::new())
                .await,
            "fixture deadline permit succeeds"
        );
        fixture_ok!(
            permit.record_outcome(PacingOutcome::RateLimited {
                retry_after: RetryAfterTiming::Honored(Duration::from_secs(120)),
            }),
            "fixture retry-after records"
        );
        drop(permit);
        drop(deadline_owner);

        let blocked_origin = origin("https://e-blocked.test/");
        assert!(matches!(
            service.register_configuration(
                blocked_origin.clone(),
                ResolvedPacingConfiguration {
                    concurrency: 1,
                    request_delay: Duration::ZERO,
                },
            ),
            Err(AdmissionError::OriginCapacityExhausted)
        ));
        assert!(service.registry.contains(&active_origin));
        assert!(service.registry.contains(&deadline_origin));
        assert!(!service.registry.contains(&blocked_origin));

        drop(active);
        clock.advance(Duration::from_secs(120));
        let admitted = registration(&service, blocked_origin.clone(), 1, Duration::ZERO);
        assert!(service.registry.contains(&blocked_origin));
        assert_eq!(service.registry.len(), 2);
        drop(admitted);
    }

    #[test]
    fn registry_pruning_order_is_normalized_origin_order_not_insertion_order() {
        for insertion_order in [
            ["https://z-safe.test/", "https://a-safe.test/"],
            ["https://a-safe.test/", "https://z-safe.test/"],
        ] {
            let (service, _) = service(2);
            for fixture_origin in insertion_order {
                drop(registration(
                    &service,
                    origin(fixture_origin),
                    1,
                    Duration::ZERO,
                ));
            }
            let inserted = origin("https://m-new.test/");
            drop(registration(&service, inserted.clone(), 1, Duration::ZERO));
            assert!(!service.registry.contains(&origin("https://a-safe.test/")));
            assert!(service.registry.contains(&origin("https://z-safe.test/")));
            assert!(service.registry.contains(&inserted));
            assert_eq!(service.registry.len(), 2);
        }
    }
}
