use std::{sync::Arc, time::Duration};

use erabi_crawler::{
    AutoScrollPolicy, CrawlerAdapter, CrawlerAdapterError, CrawlerArtifactEvidence,
    CrawlerCapabilities, CrawlerEvidencePolicy, CrawlerExecuteRequest, CrawlerExecuteResult,
    CrawlerHealth, CrawlerHealthStatus, CrawlerMediaType, CrawlerPartialReason,
    CrawlerProviderVersion, CrawlerRequestError, CrawlerResponseMetadata,
    CrawlerResultCompleteness, CrawlerValueError, DeterministicMockAdapter,
    DeterministicMockHealth, MAX_CRAWLER_AUTO_SCROLL_STEPS, MAX_CRAWLER_DISCOVERED_LINKS,
    MAX_CRAWLER_PAGINATION_OBSERVATIONS, MAX_CRAWLER_SCREENSHOT_BYTES, MAX_CRAWLER_SELECTOR_CHARS,
    MAX_CRAWLER_SELECTOR_OBSERVATIONS, MAX_CRAWLER_TEXT_ARTIFACT_BYTES, MAX_CRAWLER_URL_CHARS,
    MAX_CRAWLER_USER_AGENT_CHARS, MockAdapterConfigError, MockCrawlerFixture, ObservedLink,
    PageObservation, PaginationObservation, RenderingRequirement, ScreenshotPolicy,
    SelectorObservation,
};
use erabi_domain::PaginationKind;

fn url(value: &str) -> Result<url::Url, Box<dyn std::error::Error>> {
    Ok(value.parse()?)
}

fn health() -> Result<CrawlerHealth, Box<dyn std::error::Error>> {
    Ok(CrawlerHealth::new(
        CrawlerHealthStatus::Healthy,
        Some(CrawlerProviderVersion::new("fixture-1")?),
        CrawlerCapabilities {
            rendered_html: true,
            cleaned_html: true,
            markdown: true,
            screenshot: true,
            wait_for_selector: true,
            bounded_auto_scroll: true,
            discovered_links: true,
        },
    ))
}

fn request(
    target: &str,
    evidence: CrawlerEvidencePolicy,
) -> Result<CrawlerExecuteRequest, Box<dyn std::error::Error>> {
    Ok(CrawlerExecuteRequest::try_new(
        url(target)?,
        Duration::from_secs(30),
        "ErabiTest/1.0",
        RenderingRequirement::RenderedHtml,
        Some("main".to_owned()),
        Some(AutoScrollPolicy::new(2)?),
        evidence,
    )?)
}

fn page(target: &str, final_url: Option<&str>) -> PageObservation {
    PageObservation {
        requested_url: target.to_owned(),
        final_url: final_url.map(str::to_owned),
        artifact_ids: Vec::new(),
        discovered_links: vec![ObservedLink {
            raw_href: "/next".to_owned(),
            selector: Some("a.next".to_owned()),
        }],
        selector_observations: vec![SelectorObservation {
            selector: "main".to_owned(),
            matches_found: 1,
        }],
        pagination_observations: vec![PaginationObservation {
            kind: PaginationKind::RelNext,
            selector: Some("a.next".to_owned()),
            target_url: Some("https://example.test/next".to_owned()),
        }],
    }
}

fn result(
    request: &CrawlerExecuteRequest,
    final_url: Option<&str>,
    artifacts: Vec<CrawlerArtifactEvidence>,
    provider_reported_partial: bool,
) -> Result<CrawlerExecuteResult, Box<dyn std::error::Error>> {
    Ok(CrawlerExecuteResult::try_new(
        request,
        page(request.target_url().as_str(), final_url),
        CrawlerResponseMetadata::try_new(
            Some(200),
            Some(CrawlerMediaType::new("text/html")?),
            Some(32),
            Some(7),
        )?,
        artifacts,
        provider_reported_partial,
    )?)
}

fn adapter_with_fixture(
    target: &str,
    fixture: MockCrawlerFixture,
) -> Result<DeterministicMockAdapter, Box<dyn std::error::Error>> {
    let mut adapter = DeterministicMockAdapter::new(DeterministicMockHealth::Healthy(health()?));
    let target_url = url(target)?;
    adapter.insert_fixture(&target_url, fixture)?;
    Ok(adapter)
}

#[tokio::test]
async fn health_and_health_unavailable_are_normalized() -> Result<(), Box<dyn std::error::Error>> {
    let adapter = DeterministicMockAdapter::new(DeterministicMockHealth::Healthy(health()?));
    assert_eq!(
        adapter.health().await?.status(),
        CrawlerHealthStatus::Healthy
    );

    let unavailable = DeterministicMockAdapter::new(DeterministicMockHealth::Unavailable);
    assert_eq!(
        unavailable.health().await,
        Err(CrawlerAdapterError::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn success_is_deterministic_and_final_url_is_authoritative()
-> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/start",
        CrawlerEvidencePolicy {
            raw_html: true,
            discovered_links: true,
            selector_observations: true,
            pagination_observations: true,
            ..CrawlerEvidencePolicy::default()
        },
    )?;
    let artifacts = vec![CrawlerArtifactEvidence::raw_html("<html>ok</html>")?];
    let expected = result(
        &request,
        Some("https://example.test/final?secret=hidden"),
        artifacts,
        false,
    )?;
    let adapter = adapter_with_fixture(
        request.target_url().as_str(),
        MockCrawlerFixture::Success(expected.clone()),
    )?;

    let first = adapter.execute(request.clone()).await?;
    let second = adapter.execute(request).await?;
    assert_eq!(first, second);
    assert_eq!(
        first.observation().final_url.as_deref(),
        Some("https://example.test/final?secret=hidden")
    );
    assert_eq!(first.completeness(), CrawlerResultCompleteness::Complete);
    Ok(())
}

#[tokio::test]
async fn explicit_fixtures_cover_normalized_error_categories()
-> Result<(), Box<dyn std::error::Error>> {
    let target = "https://example.test/error";
    let cases = [
        (MockCrawlerFixture::Timeout, CrawlerAdapterError::Timeout),
        (
            MockCrawlerFixture::AccessDenied,
            CrawlerAdapterError::AccessDenied,
        ),
        (MockCrawlerFixture::NotFound, CrawlerAdapterError::NotFound),
        (
            MockCrawlerFixture::Unavailable,
            CrawlerAdapterError::Unavailable,
        ),
        (
            MockCrawlerFixture::InvalidProviderResponse,
            CrawlerAdapterError::InvalidProviderResponse,
        ),
        (
            MockCrawlerFixture::Cancelled,
            CrawlerAdapterError::Cancelled,
        ),
        (
            MockCrawlerFixture::RateLimited {
                retry_after_ms: Some(250),
            },
            CrawlerAdapterError::RateLimited {
                retry_after_ms: Some(250),
            },
        ),
        (
            MockCrawlerFixture::RemoteFailure {
                status_code: Some(503),
            },
            CrawlerAdapterError::RemoteFailure {
                status_code: Some(503),
            },
        ),
        (
            MockCrawlerFixture::UnsupportedCapability,
            CrawlerAdapterError::UnsupportedCapability,
        ),
    ];
    for (fixture, expected) in cases {
        let adapter = adapter_with_fixture(target, fixture)?;
        let actual = adapter
            .execute(request(target, CrawlerEvidencePolicy::default())?)
            .await;
        assert_eq!(actual, Err(expected));
    }
    Ok(())
}

#[tokio::test]
async fn unknown_fixture_fails_closed_as_contract_invalid() -> Result<(), Box<dyn std::error::Error>>
{
    let adapter = DeterministicMockAdapter::new(DeterministicMockHealth::Healthy(health()?));
    let actual = adapter
        .execute(request(
            "https://example.test/missing",
            CrawlerEvidencePolicy::default(),
        )?)
        .await;
    assert_eq!(actual, Err(CrawlerAdapterError::InvalidProviderResponse));
    Ok(())
}

#[tokio::test]
async fn partial_reason_precedence_is_closed_and_deterministic()
-> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/partial",
        CrawlerEvidencePolicy {
            raw_html: true,
            markdown: true,
            screenshot: ScreenshotPolicy::Viewport,
            ..CrawlerEvidencePolicy::default()
        },
    )?;
    let partial = result(
        &request,
        None,
        vec![CrawlerArtifactEvidence::raw_html("<html>partial</html>")?],
        true,
    )?;
    assert_eq!(
        partial.completeness(),
        CrawlerResultCompleteness::Partial {
            reason: CrawlerPartialReason::ProviderReportedPartial,
        }
    );

    let evidence_missing = result(&request, None, Vec::new(), false)?;
    assert_eq!(
        evidence_missing.completeness(),
        CrawlerResultCompleteness::Partial {
            reason: CrawlerPartialReason::RequestedEvidenceUnavailable,
        }
    );

    let adapter = adapter_with_fixture(
        request.target_url().as_str(),
        MockCrawlerFixture::Partial(evidence_missing.clone()),
    )?;
    let observed = adapter.execute(request).await?;
    assert_eq!(observed, evidence_missing);
    Ok(())
}

#[tokio::test]
async fn final_non_html_response_is_normalized_without_html_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/file.pdf",
        CrawlerEvidencePolicy::default(),
    )?;
    let observation = page(request.target_url().as_str(), None);
    let response = CrawlerResponseMetadata::try_new(
        Some(200),
        Some(CrawlerMediaType::new("application/pdf")?),
        Some(128),
        Some(3),
    )?;
    let result = CrawlerExecuteResult::try_new(&request, observation, response, Vec::new(), false)?;
    assert_eq!(
        result.response().media_type().map(ToString::to_string),
        Some("application/pdf".to_owned())
    );
    assert!(result.artifacts().is_empty());

    let adapter = adapter_with_fixture(
        "https://example.test/file.pdf",
        MockCrawlerFixture::Success(result.clone()),
    )?;
    let observed = adapter.execute(request).await?;
    assert_eq!(observed, result);
    Ok(())
}

#[test]
fn request_and_value_validation_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("https://example.test")?,
            Duration::from_secs(901),
            "agent",
            RenderingRequirement::RawHtml,
            None,
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::TimeoutTooLong)
    ));
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("ftp://example.test")?,
            Duration::from_secs(1),
            "agent",
            RenderingRequirement::RawHtml,
            None,
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::InvalidHttpUrl)
    ));
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("https://example.test")?,
            Duration::ZERO,
            "agent",
            RenderingRequirement::RawHtml,
            None,
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::TimeoutMustBePositive)
    ));
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("https://example.test")?,
            Duration::from_secs(1),
            " ",
            RenderingRequirement::RawHtml,
            None,
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::UserAgentEmpty)
    ));
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("https://example.test")?,
            Duration::from_secs(1),
            "x".repeat(MAX_CRAWLER_USER_AGENT_CHARS + 1),
            RenderingRequirement::RawHtml,
            None,
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::UserAgentInvalid)
    ));
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("https://example.test")?,
            Duration::from_secs(1),
            "agent",
            RenderingRequirement::RawHtml,
            Some("\n".to_owned()),
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::WaitForSelectorInvalid)
    ));
    assert!(matches!(
        CrawlerExecuteRequest::try_new(
            url("https://example.test")?,
            Duration::from_secs(1),
            "agent",
            RenderingRequirement::RawHtml,
            Some("x".repeat(MAX_CRAWLER_SELECTOR_CHARS + 1)),
            None,
            CrawlerEvidencePolicy::default(),
        ),
        Err(CrawlerRequestError::WaitForSelectorInvalid)
    ));
    Ok(())
}

#[test]
fn value_validation_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    assert!(AutoScrollPolicy::new(MAX_CRAWLER_AUTO_SCROLL_STEPS + 1).is_err());
    assert!(CrawlerMediaType::new(" \t").is_err());
    assert!(CrawlerMediaType::new("x\r\ny").is_err());
    assert!(CrawlerProviderVersion::new("x\n").is_err());
    assert!(matches!(
        CrawlerMediaType::new("x".repeat(257)),
        Err(CrawlerValueError::MediaTypeTooLong)
    ));
    assert!(matches!(
        CrawlerProviderVersion::new("x".repeat(129)),
        Err(CrawlerValueError::ProviderVersionTooLong)
    ));
    assert_eq!(
        CrawlerMediaType::new(" text/html; charset=utf-8 ")?.as_str(),
        "text/html; charset=utf-8"
    );
    Ok(())
}

#[test]
fn observation_and_response_bounds_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/bounds",
        CrawlerEvidencePolicy::default(),
    )?;
    let response = CrawlerResponseMetadata::try_new(None, None, None, None)?;

    let mut links = page(request.target_url().as_str(), None);
    links.discovered_links = vec![
        ObservedLink {
            raw_href: "/next".to_owned(),
            selector: None,
        };
        MAX_CRAWLER_DISCOVERED_LINKS + 1
    ];
    assert_eq!(
        CrawlerExecuteResult::try_new(&request, links, response.clone(), Vec::new(), false,),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );

    let mut selectors = page(request.target_url().as_str(), None);
    selectors.selector_observations = vec![
        SelectorObservation {
            selector: "main".to_owned(),
            matches_found: 1,
        };
        MAX_CRAWLER_SELECTOR_OBSERVATIONS + 1
    ];
    assert_eq!(
        CrawlerExecuteResult::try_new(&request, selectors, response.clone(), Vec::new(), false,),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );

    let mut pagination = page(request.target_url().as_str(), None);
    pagination.pagination_observations = vec![
        PaginationObservation {
            kind: PaginationKind::RelNext,
            selector: None,
            target_url: None,
        };
        MAX_CRAWLER_PAGINATION_OBSERVATIONS + 1
    ];
    assert_eq!(
        CrawlerExecuteResult::try_new(&request, pagination, response, Vec::new(), false,),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );

    let mut oversized_href = page(request.target_url().as_str(), None);
    oversized_href.discovered_links = vec![ObservedLink {
        raw_href: "x".repeat(MAX_CRAWLER_URL_CHARS + 1),
        selector: None,
    }];
    assert_eq!(
        CrawlerExecuteResult::try_new(
            &request,
            oversized_href,
            CrawlerResponseMetadata::try_new(None, None, None, None)?,
            Vec::new(),
            false,
        ),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );

    let mut control_character_href = page(request.target_url().as_str(), None);
    control_character_href.discovered_links = vec![ObservedLink {
        raw_href: "/next\n".to_owned(),
        selector: None,
    }];
    assert_eq!(
        CrawlerExecuteResult::try_new(
            &request,
            control_character_href,
            CrawlerResponseMetadata::try_new(None, None, None, None)?,
            Vec::new(),
            false,
        ),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );

    assert_eq!(
        CrawlerResponseMetadata::try_new(Some(600), None, None, None),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );
    Ok(())
}

#[test]
fn empty_raw_href_is_accepted_and_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/empty-href",
        CrawlerEvidencePolicy::default(),
    )?;
    let mut observation = page(request.target_url().as_str(), None);
    observation.discovered_links = vec![ObservedLink {
        raw_href: String::new(),
        selector: Some("a.empty".to_owned()),
    }];

    let result = CrawlerExecuteResult::try_new(
        &request,
        observation,
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        Vec::new(),
        false,
    )?;

    assert_eq!(result.observation().discovered_links[0].raw_href, "");
    Ok(())
}

#[test]
fn invalid_final_urls_and_artifact_identity_fail_closed() -> Result<(), Box<dyn std::error::Error>>
{
    let request = request(
        "https://example.test/final-url",
        CrawlerEvidencePolicy::default(),
    )?;
    let invalid = CrawlerExecuteResult::try_new(
        &request,
        page(request.target_url().as_str(), Some("/relative")),
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        Vec::new(),
        false,
    );
    assert_eq!(invalid, Err(CrawlerAdapterError::InvalidProviderResponse));

    let mut observation = page(request.target_url().as_str(), None);
    observation
        .artifact_ids
        .push(erabi_domain::ArtifactId::new());
    assert_eq!(
        CrawlerExecuteResult::try_new(
            &request,
            observation,
            CrawlerResponseMetadata::try_new(None, None, None, None)?,
            Vec::new(),
            false,
        ),
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );
    Ok(())
}

#[test]
fn observation_and_result_debug_are_redacted_and_summarized()
-> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/start?token=secret#fragment",
        CrawlerEvidencePolicy {
            raw_html: true,
            ..CrawlerEvidencePolicy::default()
        },
    )?;
    let result = result(
        &request,
        Some("https://example.test/final?token=secret"),
        vec![
            CrawlerArtifactEvidence::raw_html("<html>secret body</html>")?,
            CrawlerArtifactEvidence::screenshot(
                CrawlerMediaType::new("image/png")?,
                b"secret screenshot".to_vec(),
            )?,
        ],
        false,
    )?;
    let request_debug = format!("{request:?}");
    let result_debug = format!("{result:?}");
    let observation_debug = format!("{:?}", result.observation());
    let artifact_debug = format!("{:?}", result.artifacts());
    for debug in [
        &request_debug,
        &result_debug,
        &observation_debug,
        &artifact_debug,
    ] {
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("<html>"));
        assert!(!debug.contains("/next"));
    }
    assert!(format!("{:?}", result.artifacts()).contains("byte_len"));
    assert!(result_debug.contains("artifact_sizes_bytes"));
    for error in [
        CrawlerAdapterError::RemoteFailure {
            status_code: Some(503),
        },
        CrawlerAdapterError::RateLimited {
            retry_after_ms: Some(100),
        },
    ] {
        assert!(!format!("{error:?}").contains("secret"));
        assert!(!error.to_string().contains("secret"));
    }
    Ok(())
}

#[test]
fn artifact_limits_and_duplicate_kinds_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let request = request(
        "https://example.test/artifacts",
        CrawlerEvidencePolicy::default(),
    )?;
    let oversized_text =
        CrawlerArtifactEvidence::raw_html("x".repeat(MAX_CRAWLER_TEXT_ARTIFACT_BYTES + 1));
    assert_eq!(
        oversized_text,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );
    let oversized_screenshot = CrawlerArtifactEvidence::screenshot(
        CrawlerMediaType::new("image/png")?,
        vec![0; MAX_CRAWLER_SCREENSHOT_BYTES + 1],
    );
    assert_eq!(
        oversized_screenshot,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );
    let one = CrawlerArtifactEvidence::raw_html("one")?;
    let two = CrawlerArtifactEvidence::raw_html("two")?;
    let duplicate = CrawlerExecuteResult::try_new(
        &request,
        page(request.target_url().as_str(), None),
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        vec![one, two],
        false,
    );
    assert_eq!(duplicate, Err(CrawlerAdapterError::InvalidProviderResponse));

    let screenshot =
        CrawlerArtifactEvidence::screenshot(CrawlerMediaType::new("image/png")?, vec![0; 2])?;
    let valid = CrawlerExecuteResult::try_new(
        &request,
        page(request.target_url().as_str(), None),
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        vec![screenshot],
        false,
    )?;
    assert_eq!(valid.artifacts().len(), 1);

    let too_many = vec![
        CrawlerArtifactEvidence::raw_html("raw")?,
        CrawlerArtifactEvidence::cleaned_html("cleaned")?,
        CrawlerArtifactEvidence::rendered_html("rendered")?,
        CrawlerArtifactEvidence::markdown("markdown")?,
        CrawlerArtifactEvidence::screenshot(CrawlerMediaType::new("image/png")?, vec![0])?,
    ];
    let too_many = CrawlerExecuteResult::try_new(
        &request,
        page(request.target_url().as_str(), None),
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        too_many,
        false,
    );
    assert!(too_many.is_ok());
    let six = vec![
        CrawlerArtifactEvidence::raw_html("raw")?,
        CrawlerArtifactEvidence::cleaned_html("cleaned")?,
        CrawlerArtifactEvidence::rendered_html("rendered")?,
        CrawlerArtifactEvidence::markdown("markdown")?,
        CrawlerArtifactEvidence::screenshot(CrawlerMediaType::new("image/png")?, vec![0])?,
        CrawlerArtifactEvidence::screenshot(CrawlerMediaType::new("image/jpeg")?, vec![0])?,
    ];
    let six = CrawlerExecuteResult::try_new(
        &request,
        page(request.target_url().as_str(), None),
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        six,
        false,
    );
    assert_eq!(six, Err(CrawlerAdapterError::InvalidProviderResponse));

    let total_oversized = CrawlerExecuteResult::try_new(
        &request,
        page(request.target_url().as_str(), None),
        CrawlerResponseMetadata::try_new(None, None, None, None)?,
        vec![
            CrawlerArtifactEvidence::raw_html("x".repeat(MAX_CRAWLER_TEXT_ARTIFACT_BYTES))?,
            CrawlerArtifactEvidence::cleaned_html("x".repeat(MAX_CRAWLER_TEXT_ARTIFACT_BYTES))?,
            CrawlerArtifactEvidence::rendered_html("x".repeat(MAX_CRAWLER_TEXT_ARTIFACT_BYTES))?,
            CrawlerArtifactEvidence::markdown("x".repeat(MAX_CRAWLER_TEXT_ARTIFACT_BYTES))?,
            CrawlerArtifactEvidence::screenshot(CrawlerMediaType::new("image/png")?, vec![0])?,
        ],
        false,
    );
    assert_eq!(
        total_oversized,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    );
    Ok(())
}

#[tokio::test]
async fn fixture_lookup_is_explicit_and_insertion_order_independent()
-> Result<(), Box<dyn std::error::Error>> {
    let first_url = url("https://example.test/first")?;
    let second_url = url("https://example.test/second")?;
    let mut first = DeterministicMockAdapter::new(DeterministicMockHealth::Healthy(health()?));
    first.insert_fixture(&first_url, MockCrawlerFixture::Timeout)?;
    first.insert_fixture(&second_url, MockCrawlerFixture::NotFound)?;
    let mut second = DeterministicMockAdapter::new(DeterministicMockHealth::Healthy(health()?));
    second.insert_fixture(&second_url, MockCrawlerFixture::NotFound)?;
    second.insert_fixture(&first_url, MockCrawlerFixture::Timeout)?;

    assert_eq!(first.fixture_count(), 2);
    assert_eq!(second.fixture_count(), 2);
    assert_eq!(
        first
            .execute(request(
                "https://example.test/first",
                CrawlerEvidencePolicy::default(),
            )?)
            .await,
        Err(CrawlerAdapterError::Timeout)
    );
    assert_eq!(
        second
            .execute(request(
                "https://example.test/first",
                CrawlerEvidencePolicy::default(),
            )?)
            .await,
        Err(CrawlerAdapterError::Timeout)
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_fixture_rejection_preserves_first_fixture()
-> Result<(), Box<dyn std::error::Error>> {
    let target_url = url("https://example.test/duplicate")?;
    let mut adapter = DeterministicMockAdapter::new(DeterministicMockHealth::Healthy(health()?));
    adapter.insert_fixture(&target_url, MockCrawlerFixture::Timeout)?;

    assert_eq!(
        adapter.insert_fixture(&target_url, MockCrawlerFixture::NotFound),
        Err(MockAdapterConfigError::DuplicateFixtureUrl)
    );
    assert_eq!(
        adapter
            .execute(request(
                target_url.as_str(),
                CrawlerEvidencePolicy::default(),
            )?)
            .await,
        Err(CrawlerAdapterError::Timeout)
    );
    assert_eq!(adapter.fixture_count(), 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_fixture_use_has_identical_semantics() -> Result<(), Box<dyn std::error::Error>>
{
    let target = "https://example.test/concurrent";
    let request = request(
        target,
        CrawlerEvidencePolicy {
            raw_html: true,
            ..CrawlerEvidencePolicy::default()
        },
    )?;
    let expected = result(
        &request,
        None,
        vec![CrawlerArtifactEvidence::raw_html("<html>ok</html>")?],
        false,
    )?;
    let adapter = Arc::new(adapter_with_fixture(
        target,
        MockCrawlerFixture::Success(expected),
    )?);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let adapter = Arc::clone(&adapter);
        let request = request.clone();
        tasks.push(tokio::spawn(async move { adapter.execute(request).await }));
    }
    for task in tasks {
        assert_eq!(
            task.await??,
            result(
                &request,
                None,
                vec![CrawlerArtifactEvidence::raw_html("<html>ok</html>")?],
                false
            )?
        );
    }
    Ok(())
}
