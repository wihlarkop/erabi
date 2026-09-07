use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use crate::{
    AdmissionError, CrawlerAdapterError, MAX_PACING_DELAY, ManualPacingClock, NetworkTargetError,
    NetworkTargetPolicy, OriginKey, PacingCancellation, PacingOutcome, PacingRegistration,
    PacingService, ROBOTS_CACHE_TTL, RetryAfterTiming, RobotsAdmission, RobotsAdmissionDecision,
    RobotsHttpResponse, RobotsInvalidPolicy, RobotsPolicyError, RobotsPolicyEvidence,
    RobotsPolicyService, RobotsTransport, RobotsTransportError, RobotsUnavailable,
    StaticNetworkResolver,
};
use erabi_domain::{
    CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunType, ResolvedValue, RobotsAudit,
    RunConfiguration, SettingSource, SnapshotOperationalSettings,
};

macro_rules! fixture_ok {
    ($result:expr, $context:literal) => {
        match $result {
            Ok(value) => value,
            Err(error) => panic!("{}: {error}", $context),
        }
    };
}

fn fixture_url(value: &str) -> url::Url {
    fixture_ok!(value.parse(), "fixture URL parses")
}

fn fixture_socket(value: &str) -> std::net::SocketAddr {
    fixture_ok!(value.parse(), "fixture socket address parses")
}

#[derive(Debug)]
struct FixtureRobotsTransport {
    response: Mutex<Result<RobotsHttpResponse, RobotsTransportError>>,
    calls: AtomicUsize,
}

impl FixtureRobotsTransport {
    fn success(body: impl Into<Vec<u8>>) -> Self {
        Self {
            response: Mutex::new(Ok(RobotsHttpResponse::new(
                200,
                body.into(),
                RetryAfterTiming::Absent,
            ))),
            calls: AtomicUsize::new(0),
        }
    }

    fn status(status: u16) -> Self {
        Self {
            response: Mutex::new(Ok(RobotsHttpResponse::new(
                status,
                Vec::new(),
                RetryAfterTiming::Absent,
            ))),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl RobotsTransport for FixtureRobotsTransport {
    fn fetch<'transport>(
        &'transport self,
        _target: &'transport crate::ValidatedNetworkTarget,
        _user_agent: &'transport str,
    ) -> crate::RobotsFetchFuture<'transport> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self
            .response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Box::pin(async move { response })
    }
}

#[derive(Debug)]
struct BlockingRobotsTransport {
    response: RobotsHttpResponse,
    calls: AtomicUsize,
    gate: tokio::sync::Semaphore,
}

impl BlockingRobotsTransport {
    fn success(body: impl Into<Vec<u8>>) -> Self {
        Self {
            response: RobotsHttpResponse::new(200, body.into(), RetryAfterTiming::Absent),
            calls: AtomicUsize::new(0),
            gate: tokio::sync::Semaphore::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.gate.add_permits(1);
    }
}

impl RobotsTransport for BlockingRobotsTransport {
    fn fetch<'transport>(
        &'transport self,
        _target: &'transport crate::ValidatedNetworkTarget,
        _user_agent: &'transport str,
    ) -> crate::RobotsFetchFuture<'transport> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self.response.clone();
        Box::pin(async move {
            let permit = self
                .gate
                .acquire()
                .await
                .map_err(|_| RobotsTransportError::Unavailable)?;
            permit.forget();
            Ok(response)
        })
    }
}

async fn wait_for_calls(transport: &BlockingRobotsTransport, expected: usize) {
    for _ in 0..64 {
        if transport.calls() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(transport.calls(), expected);
}

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

fn snapshot(
    target: &str,
    user_agent: &str,
    concurrency: u32,
    request_delay_ms: u64,
    override_reason: Option<&str>,
) -> CrawlRunSnapshot {
    let robots = match override_reason {
        Some(reason) => fixture_ok!(
            RobotsAudit::override_with_reason(
                reason,
                "test-operator",
                "2026-08-30T00:00:00Z",
                "test scope",
                user_agent,
                None,
            ),
            "fixture override reason is valid"
        ),
        None => RobotsAudit::respect(
            "test-operator",
            "2026-08-30T00:00:00Z",
            "test scope",
            user_agent,
            None,
        ),
    };
    fixture_ok!(
        CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: fixture_url(target),
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: resolved(1),
                max_depth: resolved(1),
                max_duration_seconds: resolved(30),
                concurrency: resolved(concurrency),
                request_delay_ms: resolved(request_delay_ms),
                timeout_ms: resolved(1_000),
                screenshot: resolved(false),
                asset_download_limit_bytes: resolved(0),
                retain_artifacts: resolved(false),
                user_agent: resolved(user_agent.to_owned()),
            },
            robots,
            actor: "test-operator".to_owned(),
            created_at: "2026-08-30T00:00:00Z".to_owned(),
        }),
        "fixture snapshot is valid"
    )
}

fn network_policy(hosts: &[&str]) -> NetworkTargetPolicy {
    let address = fixture_socket("93.184.216.34:443");
    NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::new(
        hosts
            .iter()
            .map(|host| ((*host).to_owned(), Ok(vec![address])))
            .collect::<Vec<_>>(),
    )))
}

fn registration(
    pacing: &PacingService,
    target: &str,
    snapshot: &CrawlRunSnapshot,
) -> PacingRegistration {
    fixture_ok!(
        pacing.register(
            fixture_ok!(
                OriginKey::from_url(&fixture_url(target)),
                "fixture origin normalizes"
            ),
            snapshot,
        ),
        "immutable pacing registration succeeds"
    )
}

async fn acquire(
    registration: &PacingRegistration,
    admission: &RobotsAdmission,
    cancellation: &PacingCancellation,
) -> crate::AdmissionPermit {
    fixture_ok!(
        registration.acquire(admission, cancellation).await,
        "registered pacing admission succeeds"
    )
}

fn service(
    hosts: &[&str],
    body: impl Into<Vec<u8>>,
) -> (
    PacingService,
    RobotsPolicyService,
    Arc<ManualPacingClock>,
    Arc<FixtureRobotsTransport>,
) {
    let clock = Arc::new(ManualPacingClock::new());
    let pacing = PacingService::with_clock(clock.clone());
    let transport = Arc::new(FixtureRobotsTransport::success(body));
    let robots = RobotsPolicyService::with_transport(
        network_policy(hosts),
        pacing.clone(),
        transport.clone(),
    );
    (pacing, robots, clock, transport)
}

#[tokio::test]
async fn robots_applies_exact_groups_wildcards_combined_rules_and_crawl_delay() {
    let body = b"
        User-agent: *
        Disallow: /

        User-agent: erabi/0.1
        Disallow: /private
        Allow: /private/public
        Crawl-delay: 0.5

        User-agent: Erabi/0.1
        Disallow: /blocked
        Crawl-delay: 1.5
    ";
    let (_, robots, _, _) = service(&["example.test"], body.as_slice());
    let cancellation = PacingCancellation::new();

    let allowed = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://example.test/private/public?view=full"),
                &snapshot(
                    "https://example.test/private/public?view=full",
                    "Erabi/0.1",
                    1,
                    0,
                    None,
                ),
                &cancellation,
            )
            .await,
        "exact groups are valid"
    );
    assert_eq!(allowed.decision(), RobotsAdmissionDecision::Allowed);
    assert_eq!(allowed.crawl_delay(), Some(Duration::from_millis(1_500)));

    let blocked = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://example.test/blocked"),
                &snapshot("https://example.test/blocked", "Erabi/0.1", 1, 0, None),
                &cancellation,
            )
            .await,
        "cached policy evaluates"
    );
    assert_eq!(blocked.decision(), RobotsAdmissionDecision::Disallowed);

    let wildcard = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://example.test/anything"),
                &snapshot("https://example.test/anything", "OtherBot/1.0", 1, 0, None),
                &cancellation,
            )
            .await,
        "cached wildcard policy evaluates"
    );
    assert_eq!(wildcard.decision(), RobotsAdmissionDecision::Disallowed);
}

#[tokio::test]
async fn robots_respects_by_default_and_only_a_valid_frozen_override_changes_disallow() {
    let body = b"User-agent: Erabi\nDisallow: /private\nCrawl-delay: 2";
    let (_, robots, _, _) = service(&["override.test"], body.as_slice());
    let cancellation = PacingCancellation::new();

    let respect = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://override.test/private"),
                &snapshot("https://override.test/private", "Erabi/1.0", 1, 0, None),
                &cancellation,
            )
            .await,
        "respect decision evaluates"
    );
    assert_eq!(respect.decision(), RobotsAdmissionDecision::Disallowed);

    let overridden = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://override.test/private"),
                &snapshot(
                    "https://override.test/private",
                    "Erabi/1.0",
                    1,
                    0,
                    Some("operator approved this immutable run"),
                ),
                &cancellation,
            )
            .await,
        "frozen override evaluates"
    );
    assert_eq!(overridden.decision(), RobotsAdmissionDecision::Overridden);
    assert_eq!(overridden.crawl_delay(), Some(Duration::from_secs(2)));
    assert!(
        RobotsAudit::override_with_reason(
            " ",
            "operator",
            "2026-08-30T00:00:00Z",
            "scope",
            "Erabi/1.0",
            None,
        )
        .is_err()
    );
}

#[tokio::test]
async fn robots_cache_is_bounded_by_origin_and_expiry() {
    let (_, robots, clock, transport) = service(&["cache.test"], b"User-agent: *\nAllow: /");
    let cancellation = PacingCancellation::new();
    let first = snapshot("https://cache.test/one", "Erabi/1.0", 1, 0, None);
    let second = snapshot("https://cache.test/two", "OtherBot/1.0", 1, 0, None);

    let first_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://cache.test/one"),
                &first,
                &cancellation,
            )
            .await,
        "network policy is fetched"
    );
    assert_eq!(
        first_admission.evidence(),
        RobotsPolicyEvidence::NetworkPolicy
    );
    let second_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://cache.test/two"),
                &second,
                &cancellation,
            )
            .await,
        "same-origin policy is cached"
    );
    assert_eq!(second_admission.evidence(), RobotsPolicyEvidence::Cache);
    assert_eq!(transport.calls(), 1);

    clock.advance(ROBOTS_CACHE_TTL);
    let _ = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://cache.test/three"),
                &second,
                &cancellation,
            )
            .await,
        "expired policy refetches"
    );
    assert_eq!(transport.calls(), 2);
}

#[tokio::test]
async fn robots_cache_coalesces_concurrent_same_origin_fetches() {
    let clock = Arc::new(ManualPacingClock::new());
    let pacing = PacingService::with_clock(clock);
    let transport = Arc::new(BlockingRobotsTransport::success(b"User-agent: *\nAllow: /"));
    let robots = RobotsPolicyService::with_transport(
        network_policy(&["coalesce.test"]),
        pacing,
        transport.clone(),
    );
    let cancellation = PacingCancellation::new();
    let run = snapshot("https://coalesce.test/one", "Erabi/1.0", 2, 0, None);

    let first_robots = robots.clone();
    let first_run = run.clone();
    let first_cancellation = cancellation.clone();
    let first = tokio::spawn(async move {
        first_robots
            .evaluate(
                &fixture_url("https://coalesce.test/one"),
                &first_run,
                &first_cancellation,
            )
            .await
    });
    wait_for_calls(&transport, 1).await;

    let second_robots = robots.clone();
    let second_run = run.clone();
    let second_cancellation = cancellation.clone();
    let second = tokio::spawn(async move {
        second_robots
            .evaluate(
                &fixture_url("https://coalesce.test/two"),
                &second_run,
                &second_cancellation,
            )
            .await
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(transport.calls(), 1);

    transport.release();
    assert_eq!(
        fixture_ok!(
            fixture_ok!(first.await, "first robots evaluation joins"),
            "first robots evaluation succeeds"
        )
        .decision(),
        RobotsAdmissionDecision::Allowed
    );
    assert_eq!(
        fixture_ok!(
            fixture_ok!(second.await, "second robots evaluation joins"),
            "second robots evaluation succeeds"
        )
        .decision(),
        RobotsAdmissionDecision::Allowed
    );
}

#[tokio::test]
async fn robots_cache_keeps_scheme_and_effective_port_origins_isolated() {
    let (_, robots, _, transport) = service(&["origin.test"], b"User-agent: *\nAllow: /");
    let cancellation = PacingCancellation::new();
    for target in [
        "https://origin.test/one",
        "http://origin.test/two",
        "https://origin.test:8443/three",
    ] {
        let run = snapshot(target, "Erabi/1.0", 1, 0, None);
        let _ = fixture_ok!(
            robots
                .evaluate(&fixture_url(target), &run, &cancellation)
                .await,
            "distinct origin policy evaluates"
        );
    }
    assert_eq!(transport.calls(), 3);
}

#[tokio::test]
async fn robots_status_and_content_failures_remain_typed() {
    let clock = Arc::new(ManualPacingClock::new());
    let pacing = PacingService::with_clock(clock);
    let unavailable_transport = Arc::new(FixtureRobotsTransport::status(302));
    let unavailable = RobotsPolicyService::with_transport(
        network_policy(&["status.test"]),
        pacing.clone(),
        unavailable_transport,
    );
    let cancellation = PacingCancellation::new();
    let result = unavailable
        .evaluate(
            &fixture_url("https://status.test/"),
            &snapshot("https://status.test/", "Erabi/1.0", 1, 0, None),
            &cancellation,
        )
        .await;
    assert!(matches!(
        result,
        Err(RobotsPolicyError::Unavailable(RobotsUnavailable::Redirect))
    ));

    let not_found_transport = Arc::new(FixtureRobotsTransport::status(404));
    let not_found = RobotsPolicyService::with_transport(
        network_policy(&["missing.test"]),
        PacingService::with_clock(Arc::new(ManualPacingClock::new())),
        not_found_transport,
    );
    let absent = fixture_ok!(
        not_found
            .evaluate(
                &fixture_url("https://missing.test/"),
                &snapshot("https://missing.test/", "Erabi/1.0", 1, 0, None),
                &cancellation,
            )
            .await,
        "404 is an explicit allow-all robots absence"
    );
    assert_eq!(absent.decision(), RobotsAdmissionDecision::Allowed);
    assert_eq!(absent.evidence(), RobotsPolicyEvidence::NotFound);

    let denied_transport = Arc::new(FixtureRobotsTransport::status(403));
    let denied = RobotsPolicyService::with_transport(
        network_policy(&["denied.test"]),
        PacingService::with_clock(Arc::new(ManualPacingClock::new())),
        denied_transport,
    );
    let access_denied = fixture_ok!(
        denied
            .evaluate(
                &fixture_url("https://denied.test/"),
                &snapshot("https://denied.test/", "Erabi/1.0", 1, 0, None),
                &cancellation,
            )
            .await,
        "403 produces a conservative deny-all policy"
    );
    assert_eq!(
        access_denied.decision(),
        RobotsAdmissionDecision::Disallowed
    );
    assert_eq!(access_denied.evidence(), RobotsPolicyEvidence::AccessDenied);

    let (_, invalid, _, _) = service(&["invalid.test"], b"User-agent: *\nCrawl-delay: -1");
    let result = invalid
        .evaluate(
            &fixture_url("https://invalid.test/"),
            &snapshot("https://invalid.test/", "Erabi/1.0", 1, 0, None),
            &cancellation,
        )
        .await;
    assert!(matches!(
        result,
        Err(RobotsPolicyError::Invalid(
            RobotsInvalidPolicy::InvalidCrawlDelay
        ))
    ));

    let (_, oversized, _, _) = service(
        &["large.test"],
        vec![b'x'; crate::MAX_ROBOTS_RESPONSE_BYTES + 1],
    );
    let result = oversized
        .evaluate(
            &fixture_url("https://large.test/"),
            &snapshot("https://large.test/", "Erabi/1.0", 1, 0, None),
            &cancellation,
        )
        .await;
    assert!(matches!(
        result,
        Err(RobotsPolicyError::Invalid(
            RobotsInvalidPolicy::ResponseTooLarge
        ))
    ));
}

#[tokio::test]
async fn robots_override_does_not_bypass_access_denied_policy() {
    let transport = Arc::new(FixtureRobotsTransport::status(403));
    let robots = RobotsPolicyService::with_transport(
        network_policy(&["denied-override.test"]),
        PacingService::with_clock(Arc::new(ManualPacingClock::new())),
        transport,
    );
    let cancellation = PacingCancellation::new();
    let admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://denied-override.test/"),
                &snapshot(
                    "https://denied-override.test/",
                    "Erabi/1.0",
                    1,
                    0,
                    Some("a frozen override cannot bypass robots access denial"),
                ),
                &cancellation,
            )
            .await,
        "403 remains a deny-all policy even with a frozen override"
    );
    assert_eq!(admission.decision(), RobotsAdmissionDecision::Disallowed);
}

#[tokio::test]
async fn robots_fetch_reuses_task_four_network_policy_before_transport() {
    let public = fixture_socket("93.184.216.34:443");
    let private = fixture_socket("127.0.0.1:443");
    let policy = NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::new([(
        "mixed.test".to_owned(),
        Ok(vec![public, private]),
    )])));
    let clock = Arc::new(ManualPacingClock::new());
    let pacing = PacingService::with_clock(clock);
    let transport = Arc::new(FixtureRobotsTransport::success(b"User-agent: *\nAllow: /"));
    let robots = RobotsPolicyService::with_transport(policy, pacing, transport.clone());
    let cancellation = PacingCancellation::new();
    let result = robots
        .evaluate(
            &fixture_url("https://mixed.test/"),
            &snapshot("https://mixed.test/", "Erabi/1.0", 1, 0, None),
            &cancellation,
        )
        .await;

    assert!(matches!(
        result,
        Err(RobotsPolicyError::NetworkTarget(
            NetworkTargetError::ProhibitedResolvedAddress
        ))
    ));
    assert_eq!(transport.calls(), 0);
}

#[tokio::test]
async fn pacing_shares_same_origin_capacity_but_isolates_distinct_origins() {
    let (pacing, robots, _, _) = service(&["one.test", "two.test"], b"User-agent: *\nAllow: /");
    let cancellation = PacingCancellation::new();
    let one_snapshot = snapshot("https://one.test/a", "Erabi/1.0", 1, 0, None);
    let two_snapshot = snapshot("https://two.test/a", "Erabi/1.0", 1, 0, None);
    let one_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://one.test/a"),
                &one_snapshot,
                &cancellation,
            )
            .await,
        "first-origin robots evaluation succeeds"
    );
    let two_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://two.test/a"),
                &two_snapshot,
                &cancellation,
            )
            .await,
        "second-origin robots evaluation succeeds"
    );

    let first_registration = registration(&pacing, "https://one.test/a", &one_snapshot);
    let second_registration = registration(&pacing, "https://one.test/a", &one_snapshot);
    let other_registration = registration(&pacing, "https://two.test/a", &two_snapshot);
    let first = acquire(&first_registration, &one_admission, &cancellation).await;
    let second_admission = one_admission.clone();
    let second_cancel = cancellation.clone();
    let (second_tx, mut second_rx) = tokio::sync::oneshot::channel();
    let second_task = tokio::spawn(async move {
        let permit = second_registration
            .acquire(&second_admission, &second_cancel)
            .await;
        let _ = second_tx.send(permit.is_ok());
        (second_registration, permit)
    });
    tokio::task::yield_now().await;
    assert!(second_rx.try_recv().is_err());

    let other_origin = acquire(&other_registration, &two_admission, &cancellation).await;
    drop(other_origin);
    drop(first);
    tokio::task::yield_now().await;
    assert!(fixture_ok!(second_rx.await, "same-origin task reports"));
    let (second_registration, second) = fixture_ok!(second_task.await, "same-origin task joins");
    drop(fixture_ok!(second, "same-origin permit is acquired"));
    drop(second_registration);
    drop(first_registration);
    drop(other_registration);
}

#[tokio::test]
async fn concurrent_active_registrations_use_the_minimum_then_widen_on_release() {
    let (pacing, robots, _, _) = service(&["cap.test"], b"User-agent: *\nAllow: /");
    let cancellation = PacingCancellation::new();
    let low_snapshot = snapshot("https://cap.test/low", "Erabi/1.0", 2, 0, None);
    let high_snapshot = snapshot("https://cap.test/high", "Erabi/1.0", 5, 0, None);
    let low_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://cap.test/low"),
                &low_snapshot,
                &cancellation,
            )
            .await,
        "low-cap robots evaluation succeeds"
    );
    let high_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://cap.test/high"),
                &high_snapshot,
                &cancellation,
            )
            .await,
        "high-cap robots evaluation succeeds"
    );
    let restrictive = registration(&pacing, "https://cap.test/low", &low_snapshot);
    let permissive = registration(&pacing, "https://cap.test/high", &high_snapshot);
    let first = acquire(&permissive, &high_admission, &cancellation).await;
    let second = acquire(&permissive, &high_admission, &cancellation).await;
    let waiting_registration = registration(&pacing, "https://cap.test/high", &high_snapshot);
    let waiting_admission = high_admission.clone();
    let waiting_cancel = cancellation.clone();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let permit = waiting_registration
            .acquire(&waiting_admission, &waiting_cancel)
            .await;
        let _ = tx.send(permit.is_ok());
        (waiting_registration, permit)
    });
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    drop(restrictive);
    tokio::task::yield_now().await;
    assert!(fixture_ok!(rx.await, "permissive waiter reports"));
    let (waiting_registration, third) = fixture_ok!(task.await, "permissive task joins");
    drop(fixture_ok!(
        third,
        "released restrictive registration widens to five"
    ));
    drop(waiting_registration);
    drop(second);
    drop(first);
    drop(permissive);
    drop(low_admission);
}

#[tokio::test]
async fn released_historical_registration_does_not_sticky_throttle_a_later_run() {
    let (pacing, robots, _, _) = service(&["historical-cap.test"], b"User-agent: *\nAllow: /");
    let cancellation = PacingCancellation::new();
    let restrictive_snapshot = snapshot("https://historical-cap.test/a", "Erabi/1.0", 1, 0, None);
    let permissive_snapshot = snapshot("https://historical-cap.test/b", "Erabi/1.0", 8, 0, None);
    let restrictive_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://historical-cap.test/a"),
                &restrictive_snapshot,
                &cancellation,
            )
            .await,
        "restrictive robots evaluation succeeds"
    );
    let permissive_admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://historical-cap.test/b"),
                &permissive_snapshot,
                &cancellation,
            )
            .await,
        "permissive robots evaluation succeeds"
    );
    let restrictive = registration(
        &pacing,
        "https://historical-cap.test/a",
        &restrictive_snapshot,
    );
    let first = acquire(&restrictive, &restrictive_admission, &cancellation).await;
    drop(first);
    drop(restrictive);

    let permissive = registration(
        &pacing,
        "https://historical-cap.test/b",
        &permissive_snapshot,
    );
    let first_later = acquire(&permissive, &permissive_admission, &cancellation).await;
    let second_later = acquire(&permissive, &permissive_admission, &cancellation).await;
    drop(second_later);
    drop(first_later);
    drop(permissive);
}

#[tokio::test]
async fn retry_after_survives_registration_release() {
    let cancellation = PacingCancellation::new();

    let (retry_pacing, retry_robots, retry_clock, _) =
        service(&["retry-after-lifetime.test"], b"User-agent: *\nAllow: /");
    let restrictive_retry = snapshot(
        "https://retry-after-lifetime.test/a",
        "Erabi/1.0",
        1,
        0,
        None,
    );
    let permissive_retry = snapshot(
        "https://retry-after-lifetime.test/b",
        "Erabi/1.0",
        8,
        0,
        None,
    );
    let retry_admission = fixture_ok!(
        retry_robots
            .evaluate(
                &fixture_url("https://retry-after-lifetime.test/a"),
                &restrictive_retry,
                &cancellation,
            )
            .await,
        "retry-after robots evaluation succeeds"
    );
    let retry_owner = registration(
        &retry_pacing,
        "https://retry-after-lifetime.test/a",
        &restrictive_retry,
    );
    let retry_permit = acquire(&retry_owner, &retry_admission, &cancellation).await;
    fixture_ok!(
        retry_permit.record_outcome(PacingOutcome::RateLimited {
            retry_after: RetryAfterTiming::Honored(Duration::from_secs(120)),
        }),
        "retry-after outcome records"
    );
    drop(retry_permit);
    drop(retry_owner);
    retry_clock.advance(Duration::from_secs(10));
    let later_retry = registration(
        &retry_pacing,
        "https://retry-after-lifetime.test/b",
        &permissive_retry,
    );
    {
        let retry_wait = later_retry.acquire(&retry_admission, &cancellation);
        tokio::pin!(retry_wait);
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut retry_wait)
                .await
                .is_err()
        );
        retry_clock.advance(Duration::from_secs(110));
        drop(fixture_ok!(
            retry_wait.await,
            "retry-after remains after release"
        ));
    }
    drop(later_retry);
}

#[tokio::test]
async fn backoff_survives_registration_release() {
    let cancellation = PacingCancellation::new();
    let (backoff_pacing, backoff_robots, backoff_clock, _) =
        service(&["backoff-lifetime.test"], b"User-agent: *\nAllow: /");
    let restrictive_backoff = snapshot("https://backoff-lifetime.test/a", "Erabi/1.0", 1, 0, None);
    let permissive_backoff = snapshot("https://backoff-lifetime.test/b", "Erabi/1.0", 8, 0, None);
    let backoff_admission = fixture_ok!(
        backoff_robots
            .evaluate(
                &fixture_url("https://backoff-lifetime.test/a"),
                &restrictive_backoff,
                &cancellation,
            )
            .await,
        "backoff robots evaluation succeeds"
    );
    let backoff_owner = registration(
        &backoff_pacing,
        "https://backoff-lifetime.test/a",
        &restrictive_backoff,
    );
    let backoff_permit = acquire(&backoff_owner, &backoff_admission, &cancellation).await;
    fixture_ok!(
        backoff_permit.record_outcome(PacingOutcome::RateLimited {
            retry_after: RetryAfterTiming::Absent,
        }),
        "backoff outcome records"
    );
    drop(backoff_permit);
    drop(backoff_owner);
    let later_backoff = registration(
        &backoff_pacing,
        "https://backoff-lifetime.test/b",
        &permissive_backoff,
    );
    {
        let backoff_wait = later_backoff.acquire(&backoff_admission, &cancellation);
        tokio::pin!(backoff_wait);
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut backoff_wait)
                .await
                .is_err()
        );
        backoff_clock.advance(Duration::from_secs(1));
        drop(fixture_ok!(
            backoff_wait.await,
            "backoff remains after release"
        ));
    }
    drop(later_backoff);
}

#[tokio::test]
async fn request_delay_survives_registration_release() {
    let cancellation = PacingCancellation::new();
    let (delay_pacing, delay_robots, delay_clock, _) =
        service(&["request-delay-lifetime.test"], b"User-agent: *\nAllow: /");
    let restrictive_delay = snapshot(
        "https://request-delay-lifetime.test/a",
        "Erabi/1.0",
        1,
        120_000,
        None,
    );
    let permissive_delay = snapshot(
        "https://request-delay-lifetime.test/b",
        "Erabi/1.0",
        8,
        0,
        None,
    );
    let delay_admission = fixture_ok!(
        delay_robots
            .evaluate(
                &fixture_url("https://request-delay-lifetime.test/a"),
                &restrictive_delay,
                &cancellation,
            )
            .await,
        "request-delay robots evaluation succeeds"
    );
    // The initial robots fetch reserves the same immutable request delay.
    delay_clock.advance(Duration::from_secs(120));
    let delay_owner = registration(
        &delay_pacing,
        "https://request-delay-lifetime.test/a",
        &restrictive_delay,
    );
    let delay_permit = acquire(&delay_owner, &delay_admission, &cancellation).await;
    drop(delay_permit);
    drop(delay_owner);
    let later_delay = registration(
        &delay_pacing,
        "https://request-delay-lifetime.test/b",
        &permissive_delay,
    );
    {
        let delay_wait = later_delay.acquire(&delay_admission, &cancellation);
        tokio::pin!(delay_wait);
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut delay_wait)
                .await
                .is_err()
        );
        delay_clock.advance(Duration::from_secs(120));
        drop(fixture_ok!(
            delay_wait.await,
            "request delay remains after release"
        ));
    }
    drop(later_delay);
}

#[tokio::test]
async fn duplicate_equal_value_registrations_release_only_their_own_contribution() {
    let (pacing, robots, _, _) =
        service(&["duplicate-registration.test"], b"User-agent: *\nAllow: /");
    let cancellation = PacingCancellation::new();
    let snapshot = snapshot(
        "https://duplicate-registration.test/a",
        "Erabi/1.0",
        2,
        0,
        None,
    );
    let admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://duplicate-registration.test/a"),
                &snapshot,
                &cancellation,
            )
            .await,
        "robots evaluation succeeds"
    );
    let first_owner = registration(&pacing, "https://duplicate-registration.test/a", &snapshot);
    let second_owner = registration(&pacing, "https://duplicate-registration.test/a", &snapshot);
    drop(first_owner);
    let first = acquire(&second_owner, &admission, &cancellation).await;
    let second = acquire(&second_owner, &admission, &cancellation).await;
    drop(second);
    drop(first);
    drop(second_owner);
}

#[tokio::test]
async fn separately_constructed_default_pacing_services_share_process_origin_state() {
    let first_service = PacingService::new();
    let second_service = PacingService::new();
    let transport = Arc::new(FixtureRobotsTransport::success(b"User-agent: *\nAllow: /"));
    let robots = RobotsPolicyService::with_transport(
        network_policy(&["process-shared.test"]),
        first_service.clone(),
        transport,
    );
    let snapshot = snapshot("https://process-shared.test/a", "Erabi/1.0", 1, 0, None);
    let cancellation = PacingCancellation::new();
    let admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://process-shared.test/a"),
                &snapshot,
                &cancellation,
            )
            .await,
        "process-shared robots evaluation succeeds"
    );
    let first_registration =
        registration(&first_service, "https://process-shared.test/a", &snapshot);
    let second_registration =
        registration(&second_service, "https://process-shared.test/a", &snapshot);
    let first = acquire(&first_registration, &admission, &cancellation).await;
    let waiting_admission = admission.clone();
    let waiting_cancel = cancellation.clone();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let permit = second_registration
            .acquire(&waiting_admission, &waiting_cancel)
            .await;
        let _ = tx.send(permit.is_ok());
        (second_registration, permit)
    });
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    drop(first);
    tokio::task::yield_now().await;
    assert!(fixture_ok!(rx.await, "shared-service waiter reports"));
    let (second_registration, second) = fixture_ok!(task.await, "shared-service task joins");
    drop(fixture_ok!(second, "shared-service permit is acquired"));
    drop(second_registration);
    drop(first_registration);
}

#[tokio::test]
async fn pacing_uses_the_latest_request_delay_robots_delay_and_retry_after_deadline() {
    let (pacing, robots, clock, _) = service(
        &["delay.test"],
        b"User-agent: Erabi\nAllow: /\nCrawl-delay: 2",
    );
    let cancellation = PacingCancellation::new();
    let snapshot = snapshot("https://delay.test/a", "Erabi/1.0", 1, 1_000, None);
    let admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://delay.test/a"),
                &snapshot,
                &cancellation,
            )
            .await,
        "delay robots evaluation succeeds"
    );

    // The robots fetch itself honored the one-second configured delay.
    clock.advance(Duration::from_secs(1));
    let owner = registration(&pacing, "https://delay.test/a", &snapshot);
    let first = acquire(&owner, &admission, &cancellation).await;
    fixture_ok!(
        first.record_outcome(PacingOutcome::RateLimited {
            retry_after: RetryAfterTiming::from_provider_millis(Some(3_000)),
        }),
        "rate-limit outcome records"
    );
    drop(first);

    let waiting_registration = registration(&pacing, "https://delay.test/a", &snapshot);
    let waiting_admission = admission.clone();
    let waiting_cancel = cancellation.clone();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let permit = waiting_registration
            .acquire(&waiting_admission, &waiting_cancel)
            .await;
        let _ = tx.send(permit.is_ok());
        (waiting_registration, permit)
    });
    tokio::task::yield_now().await;
    clock.advance(Duration::from_secs(2));
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    clock.advance(Duration::from_secs(1));
    tokio::task::yield_now().await;
    assert!(fixture_ok!(rx.await, "rate-limit waiter reports"));
    let (waiting_registration, waiting) = fixture_ok!(task.await, "rate-limit task joins");
    drop(fixture_ok!(waiting, "rate-limit permit is acquired"));
    drop(waiting_registration);
    drop(owner);
}

#[tokio::test]
async fn retry_after_is_bounded_and_cancellation_releases_origin_capacity() {
    assert_eq!(
        RetryAfterTiming::from_http_header("-1", SystemTime::UNIX_EPOCH),
        RetryAfterTiming::Invalid
    );
    assert_eq!(
        RetryAfterTiming::from_provider_millis(Some(u64::MAX)),
        RetryAfterTiming::Clamped(MAX_PACING_DELAY)
    );
    assert_eq!(
        PacingOutcome::from_adapter_error(&CrawlerAdapterError::RateLimited {
            retry_after_ms: Some(2_000)
        }),
        PacingOutcome::RateLimited {
            retry_after: RetryAfterTiming::Honored(Duration::from_secs(2))
        }
    );

    let (pacing, robots, _, _) = service(&["cancel.test"], b"User-agent: *\nAllow: /");
    let snapshot = snapshot("https://cancel.test/a", "Erabi/1.0", 1, 0, None);
    let cancellation = PacingCancellation::new();
    let admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://cancel.test/a"),
                &snapshot,
                &cancellation,
            )
            .await,
        "cancellation robots evaluation succeeds"
    );
    let owner = registration(&pacing, "https://cancel.test/a", &snapshot);
    let first = acquire(&owner, &admission, &cancellation).await;
    let waiting_registration = registration(&pacing, "https://cancel.test/a", &snapshot);
    let wait_admission = admission.clone();
    let cancelled = PacingCancellation::new();
    let wait_cancel = cancelled.clone();
    let waiter = tokio::spawn(async move {
        let result = waiting_registration
            .acquire(&wait_admission, &wait_cancel)
            .await;
        (waiting_registration, result)
    });
    tokio::task::yield_now().await;
    cancelled.cancel();
    assert!(matches!(
        fixture_ok!(waiter.await, "cancelled task joins").1,
        Err(AdmissionError::Cancelled)
    ));
    drop(first);
    let replacement = acquire(&owner, &admission, &cancellation).await;
    drop(replacement);
    drop(owner);
}

#[tokio::test]
async fn repeated_rate_limits_use_a_bounded_backoff() {
    let (pacing, robots, clock, _) = service(&["backoff.test"], b"User-agent: *\nAllow: /");
    let snapshot = snapshot("https://backoff.test/a", "Erabi/1.0", 1, 0, None);
    let cancellation = PacingCancellation::new();
    let admission = fixture_ok!(
        robots
            .evaluate(
                &fixture_url("https://backoff.test/a"),
                &snapshot,
                &cancellation,
            )
            .await,
        "backoff robots evaluation succeeds"
    );

    let owner = registration(&pacing, "https://backoff.test/a", &snapshot);
    for attempt in 0..10 {
        let permit = acquire(&owner, &admission, &cancellation).await;
        fixture_ok!(
            permit.record_outcome(PacingOutcome::RateLimited {
                retry_after: RetryAfterTiming::Absent,
            }),
            "backoff outcome records"
        );
        drop(permit);
        if attempt < 9 {
            clock.advance(Duration::from_secs(60));
        }
    }

    let waiting_registration = registration(&pacing, "https://backoff.test/a", &snapshot);
    let waiting_admission = admission.clone();
    let waiting_cancel = cancellation.clone();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let permit = waiting_registration
            .acquire(&waiting_admission, &waiting_cancel)
            .await;
        let _ = tx.send(permit.is_ok());
        (waiting_registration, permit)
    });
    tokio::task::yield_now().await;
    clock.advance(Duration::from_secs(59));
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    clock.advance(Duration::from_secs(1));
    tokio::task::yield_now().await;
    assert!(fixture_ok!(rx.await, "bounded-backoff waiter reports"));
    let (waiting_registration, waiting) = fixture_ok!(task.await, "bounded-backoff task joins");
    drop(fixture_ok!(waiting, "bounded-backoff permit is acquired"));
    drop(waiting_registration);
    drop(owner);
}
