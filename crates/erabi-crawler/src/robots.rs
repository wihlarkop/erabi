//! Bounded robots policy retrieval, parsing, evaluation, and cache handling.
//!
//! Robots decisions are evaluated from the immutable run snapshot's actual
//! User-Agent and robots audit. An override may only change a parsed
//! disallow-result into an admitted result; unavailable or invalid policies
//! remain typed failures and never become implicit overrides.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use erabi_domain::{CrawlRunSnapshot, RobotsDecision};
use reqwest::header;
use tokio::{sync::watch, time::Instant};
use url::Url;

use crate::{
    AdmissionError, NetworkTargetError, NetworkTargetPolicy, OriginKey, OriginKeyError,
    PacingCancellation, PacingClock, PacingOutcome, PacingService, RetryAfterTiming,
    ValidatedNetworkTarget,
};

/// The largest robots response retained or parsed by this process.
pub const MAX_ROBOTS_RESPONSE_BYTES: usize = 512 * 1024;

/// The complete robots fetch timeout, including body read.
pub const DEFAULT_ROBOTS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Local cache lifetime for a successfully parsed (or explicit absence/deny)
/// policy. Transient failures are deliberately not cached.
pub const ROBOTS_CACHE_TTL: Duration = Duration::from_mins(10);

/// Maximum number of origin policies held in the local execution cache.
pub const MAX_ROBOTS_CACHE_ENTRIES: usize = 256;

/// A parsed robots result that later pacing may consume without re-reading the
/// raw robots body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RobotsAdmissionDecision {
    Allowed,
    Disallowed,
    Overridden,
}

/// Evidence about the successful policy source. It intentionally excludes raw
/// URLs, response headers, and body content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RobotsPolicyEvidence {
    NetworkPolicy,
    Cache,
    NotFound,
    AccessDenied,
}

/// Opaque admission material that can only be produced by this module's
/// evaluator. The pacing API consumes it instead of a caller-owned boolean.
#[derive(Clone, Debug)]
pub struct RobotsAdmission {
    origin: OriginKey,
    decision: RobotsAdmissionDecision,
    crawl_delay: Option<Duration>,
    evidence: RobotsPolicyEvidence,
}

impl RobotsAdmission {
    #[must_use]
    pub const fn decision(&self) -> RobotsAdmissionDecision {
        self.decision
    }

    #[must_use]
    pub const fn crawl_delay(&self) -> Option<Duration> {
        self.crawl_delay
    }

    #[must_use]
    pub const fn evidence(&self) -> RobotsPolicyEvidence {
        self.evidence
    }

    #[must_use]
    pub(crate) fn origin(&self) -> &OriginKey {
        &self.origin
    }
}

/// Sanitized robots retrieval failures. They remain separate from a parsed
/// Allow or Disallow decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RobotsUnavailable {
    #[error("robots policy request timed out")]
    Timeout,
    #[error("robots policy transport was unavailable")]
    Transport,
    #[error("robots policy redirects are not followed")]
    Redirect,
    #[error("robots policy server response was unavailable")]
    ServerFailure,
    #[error("robots policy request was rate limited")]
    RateLimited,
    #[error("robots policy response status was unavailable")]
    OtherStatus,
}

/// Sanitized robots content failures. No raw body is retained in the error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RobotsInvalidPolicy {
    #[error("robots policy response exceeded the bounded size")]
    ResponseTooLarge,
    #[error("robots policy was not valid UTF-8")]
    InvalidEncoding,
    #[error("robots policy had a malformed directive")]
    MalformedDirective,
    #[error("robots policy had an invalid crawl-delay")]
    InvalidCrawlDelay,
}

/// Typed failure while deriving a robots admission result.
#[derive(Debug, thiserror::Error)]
pub enum RobotsPolicyError {
    #[error("robots origin was invalid: {0}")]
    Origin(#[source] OriginKeyError),
    #[error("robots outbound target was rejected by network policy: {0}")]
    NetworkTarget(#[source] NetworkTargetError),
    #[error("robots fetch could not acquire origin admission: {0}")]
    Admission(#[source] AdmissionError),
    #[error("robots policy was unavailable: {0}")]
    Unavailable(#[source] RobotsUnavailable),
    #[error("robots policy was invalid: {0}")]
    Invalid(#[source] RobotsInvalidPolicy),
}

/// A bounded response from a robots transport. It has already discarded all
/// HTTP headers except normalized `Retry-After` timing.
#[derive(Clone, Eq, PartialEq)]
pub struct RobotsHttpResponse {
    status: u16,
    body: Vec<u8>,
    retry_after: RetryAfterTiming,
}

impl RobotsHttpResponse {
    #[must_use]
    pub fn new(status: u16, body: Vec<u8>, retry_after: RetryAfterTiming) -> Self {
        Self {
            status,
            body,
            retry_after,
        }
    }
}

impl fmt::Debug for RobotsHttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RobotsHttpResponse")
            .field("status", &self.status)
            .field("body_bytes", &self.body.len())
            .field("retry_after", &self.retry_after)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RobotsTransportError {
    Timeout,
    Unavailable,
    ResponseTooLarge,
}

pub type RobotsFetchFuture<'transport> = Pin<
    Box<dyn Future<Output = Result<RobotsHttpResponse, RobotsTransportError>> + Send + 'transport>,
>;

/// The narrow robots HTTP seam. The service validates every target with
/// [`NetworkTargetPolicy`] before this transport can be invoked.
pub trait RobotsTransport: Send + Sync {
    fn fetch<'transport>(
        &'transport self,
        target: &'transport ValidatedNetworkTarget,
        user_agent: &'transport str,
    ) -> RobotsFetchFuture<'transport>;
}

#[derive(Clone, Copy, Debug, Default)]
struct ReqwestRobotsTransport;

impl RobotsTransport for ReqwestRobotsTransport {
    fn fetch<'transport>(
        &'transport self,
        target: &'transport ValidatedNetworkTarget,
        user_agent: &'transport str,
    ) -> RobotsFetchFuture<'transport> {
        Box::pin(async move {
            let client = target
                .reqwest_builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(DEFAULT_ROBOTS_FETCH_TIMEOUT)
                .build()
                .map_err(|_| RobotsTransportError::Unavailable)?;
            let response = client
                .get(target.url().clone())
                .header(header::USER_AGENT, user_agent)
                .send()
                .await
                .map_err(|error| {
                    if error.is_timeout() {
                        RobotsTransportError::Timeout
                    } else {
                        RobotsTransportError::Unavailable
                    }
                })?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map_or(RetryAfterTiming::Absent, |value| {
                    RetryAfterTiming::from_http_header(value, SystemTime::now())
                });
            let body = if status.is_success() {
                read_bounded_body(response).await?
            } else {
                Vec::new()
            };
            Ok(RobotsHttpResponse::new(status.as_u16(), body, retry_after))
        })
    }
}

async fn read_bounded_body(
    mut response: reqwest::Response,
) -> Result<Vec<u8>, RobotsTransportError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ROBOTS_RESPONSE_BYTES as u64)
    {
        return Err(RobotsTransportError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        if error.is_timeout() {
            RobotsTransportError::Timeout
        } else {
            RobotsTransportError::Unavailable
        }
    })? {
        if chunk.len() > MAX_ROBOTS_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(RobotsTransportError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Clone, Debug)]
struct RobotsRule {
    pattern: String,
    allow: bool,
    specificity: usize,
    end_anchored: bool,
}

#[derive(Clone, Debug, Default)]
struct RobotsGroup {
    user_agents: Vec<String>,
    rules: Vec<RobotsRule>,
    crawl_delays: Vec<Duration>,
}

#[derive(Clone, Debug, Default)]
struct RobotsDocument {
    groups: Vec<RobotsGroup>,
    allow_all: bool,
    deny_all: bool,
}

impl RobotsDocument {
    fn allow_all() -> Self {
        Self {
            allow_all: true,
            ..Self::default()
        }
    }

    fn deny_all() -> Self {
        Self {
            deny_all: true,
            ..Self::default()
        }
    }

    fn permits_disallow_override(&self) -> bool {
        !self.allow_all && !self.deny_all
    }

    fn evaluate(&self, target: &Url, user_agent: &str) -> RobotsRuleEvaluation {
        if self.allow_all {
            return RobotsRuleEvaluation {
                allowed: true,
                crawl_delay: None,
            };
        }
        if self.deny_all {
            return RobotsRuleEvaluation {
                allowed: false,
                crawl_delay: None,
            };
        }

        let selected = select_groups(&self.groups, user_agent);
        let request_path = request_path_for_robots(target);
        let mut best_rule: Option<(usize, bool)> = None;
        let mut crawl_delay = None;
        for group in selected {
            for rule in &group.rules {
                if rule_matches(rule, &request_path) {
                    best_rule = match best_rule {
                        Some((specificity, allow))
                            if specificity > rule.specificity
                                || (specificity == rule.specificity && allow) =>
                        {
                            Some((specificity, allow))
                        }
                        _ => Some((rule.specificity, rule.allow)),
                    };
                }
            }
            for delay in &group.crawl_delays {
                crawl_delay =
                    Some(crawl_delay.map_or(*delay, |current: Duration| current.max(*delay)));
            }
        }

        RobotsRuleEvaluation {
            allowed: best_rule.is_none_or(|(_, allow)| allow),
            crawl_delay,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RobotsRuleEvaluation {
    allowed: bool,
    crawl_delay: Option<Duration>,
}

fn parse_robots_document(body: &str) -> Result<RobotsDocument, RobotsInvalidPolicy> {
    let mut groups = Vec::new();
    let mut current: Option<RobotsGroup> = None;
    let mut group_has_policy_directive = false;

    for raw_line in body.trim_start_matches('\u{feff}').lines() {
        let line = raw_line
            .split_once('#')
            .map_or(raw_line, |(before, _)| before)
            .trim();
        if line.is_empty() {
            if let Some(group) = current.take()
                && !group.user_agents.is_empty()
            {
                groups.push(group);
            }
            group_has_policy_directive = false;
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "user-agent" => {
                if value.is_empty() || value.chars().any(char::is_control) {
                    return Err(RobotsInvalidPolicy::MalformedDirective);
                }
                if group_has_policy_directive {
                    if let Some(group) = current.take()
                        && !group.user_agents.is_empty()
                    {
                        groups.push(group);
                    }
                    group_has_policy_directive = false;
                }
                current
                    .get_or_insert_with(RobotsGroup::default)
                    .user_agents
                    .push(value.to_ascii_lowercase());
            }
            "allow" | "disallow" => {
                let Some(group) = current.as_mut() else {
                    return Err(RobotsInvalidPolicy::MalformedDirective);
                };
                group_has_policy_directive = true;
                if value.is_empty() {
                    continue;
                }
                if value.chars().any(char::is_control) {
                    return Err(RobotsInvalidPolicy::MalformedDirective);
                }
                let (pattern, end_anchored) = value
                    .strip_suffix('$')
                    .map_or((value, false), |pattern| (pattern, true));
                if pattern.is_empty() {
                    continue;
                }
                group.rules.push(RobotsRule {
                    pattern: pattern.to_owned(),
                    allow: name == "allow",
                    specificity: pattern.bytes().filter(|byte| *byte != b'*').count(),
                    end_anchored,
                });
            }
            "crawl-delay" => {
                let Some(group) = current.as_mut() else {
                    return Err(RobotsInvalidPolicy::MalformedDirective);
                };
                group_has_policy_directive = true;
                group.crawl_delays.push(parse_crawl_delay(value)?);
            }
            _ => {}
        }
    }

    if let Some(group) = current
        && !group.user_agents.is_empty()
    {
        groups.push(group);
    }
    Ok(RobotsDocument {
        groups,
        ..RobotsDocument::default()
    })
}

fn parse_crawl_delay(value: &str) -> Result<Duration, RobotsInvalidPolicy> {
    let seconds = value
        .parse::<f64>()
        .map_err(|_| RobotsInvalidPolicy::InvalidCrawlDelay)?;
    if !seconds.is_finite() || seconds.is_sign_negative() {
        return Err(RobotsInvalidPolicy::InvalidCrawlDelay);
    }
    let duration =
        Duration::try_from_secs_f64(seconds).map_err(|_| RobotsInvalidPolicy::InvalidCrawlDelay)?;
    Ok(duration.min(crate::MAX_PACING_DELAY))
}

fn select_groups<'document>(
    groups: &'document [RobotsGroup],
    user_agent: &str,
) -> Vec<&'document RobotsGroup> {
    let active = user_agent.to_ascii_lowercase();
    let mut exact_matches = Vec::new();
    let mut highest_specificity = 0_usize;

    for group in groups {
        let specificity = group
            .user_agents
            .iter()
            .filter(|agent| agent.as_str() != "*")
            .filter(|agent| user_agent_matches(&active, agent))
            .map(String::len)
            .max();
        if let Some(specificity) = specificity {
            highest_specificity = highest_specificity.max(specificity);
            exact_matches.push((specificity, group));
        }
    }
    if highest_specificity > 0 {
        return exact_matches
            .into_iter()
            .filter_map(|(specificity, group)| {
                (specificity == highest_specificity).then_some(group)
            })
            .collect();
    }

    groups
        .iter()
        .filter(|group| group.user_agents.iter().any(|agent| agent == "*"))
        .collect()
}

fn user_agent_matches(active: &str, selector: &str) -> bool {
    if active == selector {
        return true;
    }
    let Some(remainder) = active.strip_prefix(selector) else {
        return false;
    };
    remainder
        .chars()
        .next()
        .is_some_and(|character| character == '/' || character.is_ascii_whitespace())
}

fn request_path_for_robots(target: &Url) -> String {
    match target.query() {
        Some(query) => format!("{}?{query}", target.path()),
        None => target.path().to_owned(),
    }
}

fn rule_matches(rule: &RobotsRule, request_path: &str) -> bool {
    let mut remaining = request_path;
    let mut parts = rule.pattern.split('*').peekable();
    let Some(first) = parts.next() else {
        return false;
    };
    if !remaining.starts_with(first) {
        return false;
    }
    remaining = &remaining[first.len()..];

    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            if rule.end_anchored {
                return remaining.ends_with(part);
            }
            return remaining.contains(part);
        }
        let Some(index) = remaining.find(part) else {
            return false;
        };
        remaining = &remaining[index + part.len()..];
    }
    !rule.end_anchored || remaining.is_empty()
}

#[derive(Clone, Debug)]
struct CacheEntry {
    document: RobotsDocument,
    expires_at: Instant,
    evidence: RobotsPolicyEvidence,
}

#[derive(Debug, Default)]
struct RobotsCache {
    entries: BTreeMap<OriginKey, CacheEntry>,
}

impl RobotsCache {
    fn get(
        &mut self,
        origin: &OriginKey,
        now: Instant,
    ) -> Option<(RobotsDocument, RobotsPolicyEvidence)> {
        self.entries.retain(|_, entry| entry.expires_at > now);
        self.entries
            .get(origin)
            .map(|entry| (entry.document.clone(), entry.evidence))
    }

    fn insert(
        &mut self,
        origin: OriginKey,
        document: RobotsDocument,
        evidence: RobotsPolicyEvidence,
        now: Instant,
        expires_at: Instant,
    ) {
        self.entries.retain(|_, entry| entry.expires_at > now);
        if !self.entries.contains_key(&origin) && self.entries.len() >= MAX_ROBOTS_CACHE_ENTRIES {
            let eviction = self
                .entries
                .iter()
                .min_by_key(|(key, entry)| (entry.expires_at, (*key).clone()))
                .map(|(key, _)| key.clone());
            if let Some(eviction) = eviction {
                self.entries.remove(&eviction);
            }
        }
        self.entries.insert(
            origin,
            CacheEntry {
                document,
                expires_at,
                evidence,
            },
        );
    }
}

type InFlightRobotsFetches = BTreeMap<OriginKey, watch::Sender<bool>>;

/// Removes and wakes an in-flight fetch entry even if its leader is cancelled
/// or aborted before it can fill the cache.
struct RobotsFetchFlight {
    origin: OriginKey,
    in_flight: Arc<Mutex<InFlightRobotsFetches>>,
}

impl Drop for RobotsFetchFlight {
    fn drop(&mut self) {
        if let Some(completion) = recover_lock(&self.in_flight).remove(&self.origin) {
            let _ = completion.send(true);
        }
    }
}

fn recover_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Shared robots policy/cache service. Clone it (or clone its pacing service)
/// for independent callers so same-origin state remains process-shared.
#[derive(Clone)]
pub struct RobotsPolicyService {
    network_policy: NetworkTargetPolicy,
    pacing: PacingService,
    transport: Arc<dyn RobotsTransport>,
    cache: Arc<Mutex<RobotsCache>>,
    in_flight: Arc<Mutex<InFlightRobotsFetches>>,
    clock: Arc<dyn PacingClock>,
}

impl fmt::Debug for RobotsPolicyService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RobotsPolicyService")
            .field("network_policy", &self.network_policy)
            .field("pacing", &self.pacing)
            .field("cache", &"bounded local cache")
            .finish_non_exhaustive()
    }
}

impl RobotsPolicyService {
    #[must_use]
    pub fn new(network_policy: NetworkTargetPolicy, pacing: PacingService) -> Self {
        Self::with_transport(network_policy, pacing, Arc::new(ReqwestRobotsTransport))
    }

    #[must_use]
    pub fn with_transport(
        network_policy: NetworkTargetPolicy,
        pacing: PacingService,
        transport: Arc<dyn RobotsTransport>,
    ) -> Self {
        let clock = pacing.clock();
        Self {
            network_policy,
            pacing,
            transport,
            cache: Arc::new(Mutex::new(RobotsCache::default())),
            in_flight: Arc::new(Mutex::new(BTreeMap::new())),
            clock,
        }
    }

    /// Fetches/evaluates robots policy for a target using the snapshot's
    /// actual resolved User-Agent and robots audit. A valid frozen override
    /// changes only a parsed disallow result; it does not bypass invalid,
    /// unavailable, network, pacing, or delay policy.
    ///
    /// # Errors
    /// Returns typed network, pacing, unavailable, or invalid-policy evidence.
    pub async fn evaluate(
        &self,
        target: &Url,
        snapshot: &CrawlRunSnapshot,
        cancellation: &PacingCancellation,
    ) -> Result<RobotsAdmission, RobotsPolicyError> {
        self.network_policy
            .validate_url(target)
            .map_err(RobotsPolicyError::NetworkTarget)?;
        let origin = OriginKey::from_url(target).map_err(RobotsPolicyError::Origin)?;
        let (document, evidence) = self
            .cached_or_fetch_document(target, snapshot, &origin, cancellation)
            .await?;

        let evaluated = document.evaluate(target, &snapshot.settings().user_agent.value);
        let decision = if evaluated.allowed {
            RobotsAdmissionDecision::Allowed
        } else if document.permits_disallow_override()
            && matches!(
                snapshot.robots().decision(),
                RobotsDecision::Override { .. }
            )
        {
            RobotsAdmissionDecision::Overridden
        } else {
            RobotsAdmissionDecision::Disallowed
        };
        Ok(RobotsAdmission {
            origin,
            decision,
            crawl_delay: evaluated.crawl_delay,
            evidence,
        })
    }

    async fn cached_or_fetch_document(
        &self,
        target: &Url,
        snapshot: &CrawlRunSnapshot,
        origin: &OriginKey,
        cancellation: &PacingCancellation,
    ) -> Result<(RobotsDocument, RobotsPolicyEvidence), RobotsPolicyError> {
        loop {
            let now = self.clock.now();
            if let Some((document, _)) = recover_lock(&self.cache).get(origin, now) {
                return Ok((document, RobotsPolicyEvidence::Cache));
            }

            let follower = {
                let mut in_flight = recover_lock(&self.in_flight);
                if let Some(completion) = in_flight.get(origin) {
                    Some(completion.subscribe())
                } else {
                    let (completion, _) = watch::channel(false);
                    in_flight.insert(origin.clone(), completion);
                    None
                }
            };

            if let Some(mut completion) = follower {
                tokio::select! {
                    _ = completion.changed() => {}
                    () = cancellation.cancelled() => {
                        return Err(RobotsPolicyError::Admission(AdmissionError::Cancelled));
                    }
                }
                continue;
            }

            let flight = RobotsFetchFlight {
                origin: origin.clone(),
                in_flight: Arc::clone(&self.in_flight),
            };
            let result = self
                .fetch_document(target, snapshot, origin, cancellation)
                .await;
            drop(flight);
            return result;
        }
    }

    async fn fetch_document(
        &self,
        target: &Url,
        snapshot: &CrawlRunSnapshot,
        origin: &OriginKey,
        cancellation: &PacingCancellation,
    ) -> Result<(RobotsDocument, RobotsPolicyEvidence), RobotsPolicyError> {
        let robots_url = robots_url(target);
        let target = self
            .network_policy
            .validate_and_resolve(&robots_url)
            .await
            .map_err(RobotsPolicyError::NetworkTarget)?;
        // A robots fetch participates in the same process-wide pacing state,
        // but its short-lived registration is intentionally not an execution
        // lifecycle owner. Later orchestration holds longer registrations for
        // page work through the public pacing boundary.
        let registration = self
            .pacing
            .register(origin.clone(), snapshot)
            .map_err(RobotsPolicyError::Admission)?;
        let permit = registration
            .acquire_robots_fetch(cancellation)
            .await
            .map_err(RobotsPolicyError::Admission)?;
        let response = self
            .transport
            .fetch(&target, &snapshot.settings().user_agent.value)
            .await
            .map_err(|error| match error {
                RobotsTransportError::Timeout => {
                    RobotsPolicyError::Unavailable(RobotsUnavailable::Timeout)
                }
                RobotsTransportError::Unavailable => {
                    RobotsPolicyError::Unavailable(RobotsUnavailable::Transport)
                }
                RobotsTransportError::ResponseTooLarge => {
                    RobotsPolicyError::Invalid(RobotsInvalidPolicy::ResponseTooLarge)
                }
            })?;

        if response.body.len() > MAX_ROBOTS_RESPONSE_BYTES {
            return Err(RobotsPolicyError::Invalid(
                RobotsInvalidPolicy::ResponseTooLarge,
            ));
        }

        let (document, evidence) = match response.status {
            200..=299 => {
                let body = std::str::from_utf8(&response.body).map_err(|_| {
                    RobotsPolicyError::Invalid(RobotsInvalidPolicy::InvalidEncoding)
                })?;
                (
                    parse_robots_document(body).map_err(RobotsPolicyError::Invalid)?,
                    RobotsPolicyEvidence::NetworkPolicy,
                )
            }
            401 | 403 => (
                RobotsDocument::deny_all(),
                RobotsPolicyEvidence::AccessDenied,
            ),
            404 => (RobotsDocument::allow_all(), RobotsPolicyEvidence::NotFound),
            300..=399 => {
                return Err(RobotsPolicyError::Unavailable(RobotsUnavailable::Redirect));
            }
            429 => {
                permit
                    .record_outcome(PacingOutcome::RateLimited {
                        retry_after: response.retry_after,
                    })
                    .map_err(RobotsPolicyError::Admission)?;
                return Err(RobotsPolicyError::Unavailable(
                    RobotsUnavailable::RateLimited,
                ));
            }
            500..=599 => {
                let _ = permit.record_outcome(PacingOutcome::Failed);
                return Err(RobotsPolicyError::Unavailable(
                    RobotsUnavailable::ServerFailure,
                ));
            }
            _ => {
                let _ = permit.record_outcome(PacingOutcome::Failed);
                return Err(RobotsPolicyError::Unavailable(
                    RobotsUnavailable::OtherStatus,
                ));
            }
        };
        permit
            .record_outcome(PacingOutcome::Success)
            .map_err(RobotsPolicyError::Admission)?;

        let now = self.clock.now();
        let expires_at = now
            .checked_add(ROBOTS_CACHE_TTL)
            .ok_or(RobotsPolicyError::Admission(AdmissionError::ClockOverflow))?;
        recover_lock(&self.cache).insert(
            origin.clone(),
            document.clone(),
            evidence,
            now,
            expires_at,
        );
        Ok((document, evidence))
    }
}

fn robots_url(target: &Url) -> Url {
    let mut robots = target.clone();
    robots.set_path("/robots.txt");
    robots.set_query(None);
    robots.set_fragment(None);
    robots
}
