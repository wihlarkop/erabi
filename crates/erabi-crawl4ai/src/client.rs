use std::{fmt, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use erabi_crawler::{
    CrawlerAdapter, CrawlerAdapterError, CrawlerArtifactEvidence, CrawlerCapabilities,
    CrawlerExecuteRequest, CrawlerExecuteResult, CrawlerHealth, CrawlerHealthStatus,
    CrawlerMediaType, CrawlerProviderVersion, CrawlerResponseMetadata, ObservedLink,
    PageObservation, ScreenshotPolicy,
};
use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use secrecy::ExposeSecret;
use serde::de::DeserializeOwned;

use crate::{config::Crawl4AiConfig, dto};

const CRAWL_PATH: &str = "crawl";
const HEALTH_PATH: &str = "health";
const MAX_PROVIDER_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_HEALTH_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_RETRY_AFTER_SECONDS: u64 = 86_400;

/// HTTP adapter for the synchronous `Crawl4AI` `/crawl` API.
pub struct Crawl4AiAdapter {
    config: Crawl4AiConfig,
    client: Client,
}

impl Crawl4AiAdapter {
    /// Builds one reusable Rustls-backed HTTP client for the configured server.
    ///
    /// # Errors
    /// Returns `Unavailable` if the HTTP client cannot be initialized.
    pub fn new(config: Crawl4AiConfig) -> Result<Self, CrawlerAdapterError> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| CrawlerAdapterError::Unavailable)?;
        Ok(Self { config, client })
    }

    async fn health_request(&self) -> Result<CrawlerHealth, CrawlerAdapterError> {
        let endpoint = self
            .config
            .endpoint(HEALTH_PATH)
            .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
        let response = self
            .client
            .get(endpoint)
            .timeout(Duration::from_secs(10))
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|error| map_transport_error(&error))?;

        if !response.status().is_success() {
            return Err(map_provider_status(response.status(), response.headers()));
        }

        let body = read_bounded_body(response, MAX_HEALTH_RESPONSE_BYTES).await?;
        let health: dto::HealthResponse = parse_json(&body)?;
        let status = match health.status.as_str() {
            "ok" => CrawlerHealthStatus::Healthy,
            "degraded" => CrawlerHealthStatus::Degraded,
            _ => return Err(CrawlerAdapterError::InvalidProviderResponse),
        };
        let provider_version = health
            .version
            .map(|version| {
                CrawlerProviderVersion::new(version)
                    .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)
            })
            .transpose()?;

        Ok(CrawlerHealth::new(status, provider_version, capabilities()))
    }

    async fn execute_request(
        &self,
        request: CrawlerExecuteRequest,
    ) -> Result<CrawlerExecuteResult, CrawlerAdapterError> {
        let crawl_request = build_crawl_request(&request)?;
        let endpoint = self
            .config
            .endpoint(CRAWL_PATH)
            .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
        let mut builder = self
            .client
            .post(endpoint)
            .timeout(request.timeout())
            .header(reqwest::header::ACCEPT, "application/json")
            .json(&crawl_request);
        if let Some(token) = self.config.api_token() {
            builder = builder.bearer_auth(token.expose_secret());
        }

        let response = builder
            .send()
            .await
            .map_err(|error| map_transport_error(&error))?;
        if !response.status().is_success() {
            return Err(map_provider_status(response.status(), response.headers()));
        }

        let body = read_bounded_body(response, MAX_PROVIDER_RESPONSE_BYTES).await?;
        let crawl_response: dto::CrawlResponse = parse_json(&body)?;
        normalize_crawl_response(&request, crawl_response)
    }
}

impl fmt::Debug for Crawl4AiAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Crawl4AiAdapter")
            .field("config", &self.config)
            .field("client", &"<reusable rustls client>")
            .finish()
    }
}

impl CrawlerAdapter for Crawl4AiAdapter {
    fn health(&self) -> erabi_crawler::CrawlerFuture<'_, CrawlerHealth> {
        Box::pin(self.health_request())
    }

    fn execute(
        &self,
        request: CrawlerExecuteRequest,
    ) -> erabi_crawler::CrawlerFuture<'_, CrawlerExecuteResult> {
        Box::pin(self.execute_request(request))
    }
}

fn capabilities() -> CrawlerCapabilities {
    CrawlerCapabilities {
        rendered_html: true,
        cleaned_html: true,
        markdown: true,
        // v0.9.2 can force viewport screenshots; its adaptive fallback is not
        // an exact FullPage implementation, which is rejected at request build.
        screenshot: true,
        wait_for_selector: true,
        bounded_auto_scroll: true,
        discovered_links: true,
    }
}

fn build_crawl_request(
    request: &CrawlerExecuteRequest,
) -> Result<dto::CrawlRequest, CrawlerAdapterError> {
    let evidence = request.evidence();
    if evidence.selector_observations || evidence.pagination_observations {
        return Err(CrawlerAdapterError::UnsupportedCapability);
    }
    if evidence.raw_html && evidence.rendered_html {
        return Err(CrawlerAdapterError::UnsupportedCapability);
    }
    if matches!(evidence.screenshot, ScreenshotPolicy::FullPage) {
        return Err(CrawlerAdapterError::UnsupportedCapability);
    }

    let timeout_ms = u64::try_from(request.timeout().as_millis())
        .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
    let auto_scroll = request.auto_scroll();
    let screenshot = request.evidence().screenshot;
    let (screenshot_enabled, force_viewport_screenshot) = match screenshot {
        ScreenshotPolicy::None => (false, None),
        ScreenshotPolicy::Viewport => (true, Some(true)),
        ScreenshotPolicy::FullPage => return Err(CrawlerAdapterError::UnsupportedCapability),
    };

    Ok(dto::CrawlRequest {
        urls: vec![request.target_url().as_str().to_owned()],
        browser_config: dto::TypedConfig {
            config_type: "BrowserConfig",
            params: dto::BrowserConfig {
                headless: true,
                user_agent: request.user_agent().to_owned(),
            },
        },
        crawler_config: dto::TypedConfig {
            config_type: "CrawlerRunConfig",
            params: dto::CrawlerRunConfig {
                cache_mode: dto::TypedConfig {
                    config_type: "CacheMode",
                    params: "bypass",
                },
                page_timeout: timeout_ms,
                stream: false,
                wait_for: request
                    .wait_for_selector()
                    .map(|selector| format!("css:{selector}")),
                scan_full_page: auto_scroll.map(|_| true),
                scroll_delay: auto_scroll.map(|_| 0.0),
                max_scroll_steps: auto_scroll.map(|policy| u32::from(policy.max_steps())),
                screenshot: screenshot_enabled,
                force_viewport_screenshot,
            },
        },
    })
}

fn normalize_crawl_response(
    request: &CrawlerExecuteRequest,
    response: dto::CrawlResponse,
) -> Result<CrawlerExecuteResult, CrawlerAdapterError> {
    let dto::CrawlResponse {
        success,
        results,
        server_processing_time_s,
    } = response;
    if !success || results.len() != 1 {
        return Err(CrawlerAdapterError::InvalidProviderResponse);
    }
    let result = results
        .into_iter()
        .next()
        .ok_or(CrawlerAdapterError::InvalidProviderResponse)?;
    let status_code = result
        .status_code
        .map(|status| {
            u16::try_from(status).map_err(|_| CrawlerAdapterError::InvalidProviderResponse)
        })
        .transpose()?;

    if !result.success {
        return Err(map_target_failure(
            status_code,
            result.response_headers.as_ref(),
        ));
    }

    let final_url = result
        .redirected_url
        .as_ref()
        .or(Some(&result.url))
        .cloned();
    let (media_type, content_length) = response_metadata(result.response_headers.as_ref())?;
    let provider_elapsed_ms = server_processing_time_s
        .map(seconds_to_millis)
        .transpose()?;
    let metadata = CrawlerResponseMetadata::try_new(
        status_code,
        media_type,
        content_length,
        provider_elapsed_ms,
    )
    .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
    let discovered_links = if request.evidence().discovered_links {
        discovered_links(&result.links)
    } else {
        Vec::new()
    };
    let observation = PageObservation {
        requested_url: request.target_url().as_str().to_owned(),
        final_url,
        artifact_ids: Vec::new(),
        discovered_links,
        selector_observations: Vec::new(),
        pagination_observations: Vec::new(),
    };
    let artifacts = artifacts_for(request, result)?;

    CrawlerExecuteResult::try_new(request, observation, metadata, artifacts, false)
        .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)
}

fn artifacts_for(
    request: &CrawlerExecuteRequest,
    result: dto::CrawlResult,
) -> Result<Vec<CrawlerArtifactEvidence>, CrawlerAdapterError> {
    let evidence = request.evidence();
    let mut artifacts = Vec::new();
    // Crawl4AI v0.9.2 exposes one `html` result field. Erabi therefore treats
    // it as the requested raw or rendered representation for a single-mode
    // request; build_crawl_request rejects asking for both representations.
    if evidence.raw_html {
        artifacts.push(
            CrawlerArtifactEvidence::raw_html(result.html.clone())
                .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
        );
    }
    if evidence.rendered_html {
        artifacts.push(
            CrawlerArtifactEvidence::rendered_html(result.html.clone())
                .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
        );
    }
    if evidence.cleaned_html
        && let Some(cleaned_html) = result.cleaned_html
    {
        artifacts.push(
            CrawlerArtifactEvidence::cleaned_html(cleaned_html)
                .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
        );
    }
    if evidence.markdown
        && let Some(markdown) = result.markdown
    {
        let raw_markdown = match markdown {
            dto::ProviderMarkdown::Structured(markdown) => markdown.raw_markdown,
            dto::ProviderMarkdown::Text(markdown) => markdown,
        };
        artifacts.push(
            CrawlerArtifactEvidence::markdown(raw_markdown)
                .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
        );
    }
    if !matches!(evidence.screenshot, ScreenshotPolicy::None)
        && let Some(encoded) = result.screenshot
    {
        let screenshot = BASE64_STANDARD
            .decode(encoded)
            .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
        if screenshot.is_empty() {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }
        let media_type = CrawlerMediaType::new("image/png")
            .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
        artifacts.push(
            CrawlerArtifactEvidence::screenshot(media_type, screenshot)
                .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
        );
    }
    Ok(artifacts)
}

fn discovered_links(links: &dto::ProviderLinks) -> Vec<ObservedLink> {
    links
        .internal
        .iter()
        .chain(links.external.iter())
        .map(|link| ObservedLink {
            raw_href: link.href.clone().unwrap_or_default(),
            selector: None,
        })
        .collect()
}

fn response_metadata(
    headers: Option<&std::collections::BTreeMap<String, serde_json::Value>>,
) -> Result<(Option<CrawlerMediaType>, Option<u64>), CrawlerAdapterError> {
    let Some(headers) = headers else {
        return Ok((None, None));
    };
    let content_type = header_string(headers, "content-type")?;
    let media_type = content_type
        .map(CrawlerMediaType::new)
        .transpose()
        .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
    let content_length = header_string(headers, "content-length")?
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
    Ok((media_type, content_length))
}

fn header_string(
    headers: &std::collections::BTreeMap<String, serde_json::Value>,
    name: &str,
) -> Result<Option<String>, CrawlerAdapterError> {
    let value = headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value);
    match value {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(CrawlerAdapterError::InvalidProviderResponse),
    }
}

fn map_target_failure(
    status_code: Option<u16>,
    response_headers: Option<&std::collections::BTreeMap<String, serde_json::Value>>,
) -> CrawlerAdapterError {
    match status_code {
        Some(401 | 403) => CrawlerAdapterError::AccessDenied,
        Some(404) => CrawlerAdapterError::NotFound,
        Some(429) => CrawlerAdapterError::RateLimited {
            retry_after_ms: retry_after_from_provider_headers(response_headers),
        },
        Some(status_code @ 500..=599) => CrawlerAdapterError::RemoteFailure {
            status_code: Some(status_code),
        },
        status_code => CrawlerAdapterError::RemoteFailure { status_code },
    }
}

fn map_provider_status(
    status: StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> CrawlerAdapterError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => CrawlerAdapterError::AccessDenied,
        StatusCode::TOO_MANY_REQUESTS => CrawlerAdapterError::RateLimited {
            retry_after_ms: retry_after_ms(headers),
        },
        status if status.is_server_error() => CrawlerAdapterError::Unavailable,
        _ => CrawlerAdapterError::InvalidProviderResponse,
    }
}

fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    retry_after_value_ms(value)
}

fn retry_after_from_provider_headers(
    headers: Option<&std::collections::BTreeMap<String, serde_json::Value>>,
) -> Option<u64> {
    let value = headers?
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))?
        .1
        .as_str()?
        .trim();
    retry_after_value_ms(value)
}

fn retry_after_value_ms(value: &str) -> Option<u64> {
    let seconds = value.parse::<u64>().ok()?;
    if seconds > MAX_RETRY_AFTER_SECONDS {
        return None;
    }
    seconds.checked_mul(1_000)
}

fn map_transport_error(error: &reqwest::Error) -> CrawlerAdapterError {
    if error.is_timeout() {
        CrawlerAdapterError::Timeout
    } else {
        CrawlerAdapterError::Unavailable
    }
}

async fn read_bounded_body(
    mut response: reqwest::Response,
    maximum_bytes: usize,
) -> Result<Vec<u8>, CrawlerAdapterError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum_bytes as u64)
    {
        return Err(CrawlerAdapterError::InvalidProviderResponse);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| map_transport_error(&error))?
    {
        if chunk.len() > maximum_bytes.saturating_sub(body.len()) {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn parse_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, CrawlerAdapterError> {
    serde_json::from_slice(body).map_err(|_| CrawlerAdapterError::InvalidProviderResponse)
}

fn seconds_to_millis(seconds: f64) -> Result<u64, CrawlerAdapterError> {
    if !seconds.is_finite() || seconds.is_sign_negative() {
        return Err(CrawlerAdapterError::InvalidProviderResponse);
    }
    let duration = Duration::try_from_secs_f64(seconds)
        .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
    u64::try_from(duration.as_millis()).map_err(|_| CrawlerAdapterError::InvalidProviderResponse)
}
