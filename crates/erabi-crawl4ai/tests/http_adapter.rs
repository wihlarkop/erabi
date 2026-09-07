use std::{
    collections::BTreeMap,
    error::Error,
    fmt::Write as _,
    future::pending,
    io::{Error as IoError, ErrorKind},
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use erabi_crawl4ai::{Crawl4AiAdapter, Crawl4AiConfig};
use erabi_crawler::{
    AutoScrollPolicy, CrawlerAdapter, CrawlerAdapterError, CrawlerArtifactEvidence,
    CrawlerEvidencePolicy, CrawlerExecuteRequest, CrawlerHealthStatus, CrawlerPartialReason,
    CrawlerResultCompleteness, RenderingRequirement, ScreenshotPolicy,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use url::Url;

const TOKEN: &str = "super-secret-crawl4ai-token";
const TARGET_URL: &str = "https://target.test/start?query=one";

#[derive(Clone)]
struct CapturedRequest {
    method: String,
    path: String,
    authorization_present: bool,
    authorization_matches_token: bool,
    body: Vec<u8>,
}

#[allow(clippy::large_enum_variant)]
enum FixtureResponse {
    Static {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        advertised_length: Option<usize>,
    },
    Hold,
    Close,
}

impl FixtureResponse {
    fn json(status: u16, body: &Value) -> Result<Self, Box<dyn Error>> {
        Ok(Self::Static {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: serde_json::to_vec(body)?,
            advertised_length: None,
        })
    }
}

struct FixtureServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    task: JoinHandle<()>,
}

impl FixtureServer {
    async fn start(response: FixtureResponse) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let endpoint = format!("http://{address}");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(captured) = read_request(&mut stream).await else {
                return;
            };
            if let Ok(mut requests) = captured_requests.lock() {
                requests.push(captured);
            }
            match response {
                FixtureResponse::Static {
                    status,
                    headers,
                    body,
                    advertised_length,
                } => {
                    let _ = write_response(&mut stream, status, &headers, &body, advertised_length)
                        .await;
                }
                FixtureResponse::Hold => pending::<()>().await,
                FixtureResponse::Close => {}
            }
        });
        Ok(Self {
            endpoint,
            requests,
            task,
        })
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<CapturedRequest, Box<dyn Error>> {
    let mut wire = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4_096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(fixture_error("fixture connection closed before headers").into());
        }
        wire.extend_from_slice(&chunk[..count]);
        if wire.len() > 2 * 1024 * 1024 {
            return Err(fixture_error("fixture request exceeded its test bound").into());
        }
        if let Some(index) = wire.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };

    let header_text = String::from_utf8_lossy(&wire[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| fixture_error("fixture request line missing"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| fixture_error("fixture method missing"))?
        .to_owned();
    let path = request_parts
        .next()
        .ok_or_else(|| fixture_error("fixture path missing"))?
        .to_owned();
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| fixture_error("fixture header malformed"))?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }

    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = wire[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..count]);
    }
    body.truncate(content_length);

    let authorization_present = headers.contains_key("authorization");
    let authorization_matches_token = headers
        .get("authorization")
        .is_some_and(|value| value == &format!("Bearer {TOKEN}"));

    Ok(CapturedRequest {
        method,
        path,
        authorization_present,
        authorization_matches_token,
        body,
    })
}

fn fixture_error(message: &'static str) -> IoError {
    IoError::new(ErrorKind::InvalidData, message)
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
    advertised_length: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Fixture Status",
    };
    let content_length = advertised_length.unwrap_or(body.len());
    let mut head = String::new();
    write!(&mut head, "HTTP/1.1 {status} {reason}\r\n")
        .map_err(|_| fixture_error("fixture response formatting failed"))?;
    write!(&mut head, "Content-Length: {content_length}\r\n")
        .map_err(|_| fixture_error("fixture response formatting failed"))?;
    head.push_str("Connection: close\r\n");
    for (name, value) in headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

fn adapter(server: &FixtureServer, token: Option<&str>) -> Result<Crawl4AiAdapter, Box<dyn Error>> {
    let config = Crawl4AiConfig::new(server.endpoint(), token.map(str::to_owned))?;
    Ok(Crawl4AiAdapter::new(config)?)
}

fn request_with(
    evidence: CrawlerEvidencePolicy,
    selector: Option<&str>,
    scroll_steps: Option<u16>,
    timeout: Duration,
) -> Result<CrawlerExecuteRequest, Box<dyn Error>> {
    let auto_scroll = scroll_steps.map(AutoScrollPolicy::new).transpose()?;
    Ok(CrawlerExecuteRequest::try_new(
        Url::parse(TARGET_URL)?,
        timeout,
        "ErabiCrawler/0.1",
        RenderingRequirement::RenderedHtml,
        selector.map(str::to_owned),
        auto_scroll,
        evidence,
    )?)
}

fn base_result() -> Value {
    json!({
        "url": "https://target.test/final?provider=1",
        "html": "<html><body>raw</body></html>",
        "cleaned_html": "<body>clean</body>",
        "success": true,
        "links": {
            "internal": [{"href": "/next"}, {"href": ""}],
            "external": [{"href": "https://external.test/page"}]
        },
        "markdown": {
            "raw_markdown": "# Markdown",
            "markdown_with_citations": "# Markdown",
            "references_markdown": "",
            "fit_markdown": null,
            "fit_html": null
        },
        "screenshot": BASE64_STANDARD.encode([1_u8, 2, 3]),
        "response_headers": {
            "Content-Type": "text/html; charset=utf-8",
            "Content-Length": "30"
        },
        "status_code": 200,
        "redirected_url": "https://target.test/redirected?provider=1"
    })
}

fn crawl_body(result: &Value) -> Value {
    json!({
        "success": true,
        "results": [result],
        "server_processing_time_s": 0.125
    })
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key(key) || object.values().any(|value| contains_key(value, key))
        }
        Value::Array(values) => values.iter().any(|value| contains_key(value, key)),
        _ => false,
    }
}

#[tokio::test]
async fn health_maps_version_and_omits_health_auth() -> Result<(), Box<dyn Error>> {
    let server = FixtureServer::start(FixtureResponse::json(
        200,
        &json!({"status": "ok", "timestamp": 0.0, "version": "0.9.2"}),
    )?)
    .await?;
    let adapter = adapter(&server, Some(TOKEN))?;

    let health = adapter.health().await?;

    assert_eq!(health.status(), CrawlerHealthStatus::Healthy);
    assert_eq!(
        health
            .provider_version()
            .map(ToString::to_string)
            .as_deref(),
        Some("0.9.2")
    );
    assert!(health.capabilities().rendered_html);
    assert!(health.capabilities().cleaned_html);
    assert!(health.capabilities().markdown);
    assert!(health.capabilities().screenshot);
    assert!(health.capabilities().wait_for_selector);
    assert!(health.capabilities().bounded_auto_scroll);
    assert!(health.capabilities().discovered_links);

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/health");
    assert!(!requests[0].authorization_present);
    Ok(())
}

#[tokio::test]
async fn malformed_health_is_invalid_provider_response() -> Result<(), Box<dyn Error>> {
    let server =
        FixtureServer::start(FixtureResponse::json(200, &json!({"status": "unknown"}))?).await?;
    let adapter = adapter(&server, None)?;

    assert!(matches!(
        adapter.health().await,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    ));
    Ok(())
}

#[test]
fn endpoint_validation_and_secret_debug_are_safe() -> Result<(), Box<dyn Error>> {
    assert!(Crawl4AiConfig::new("ftp://127.0.0.1:11235", None).is_err());
    assert!(Crawl4AiConfig::new("http://user:pass@127.0.0.1:11235", None).is_err());
    assert!(Crawl4AiConfig::new("http://127.0.0.1:11235/?query=forbidden", None).is_err());
    assert!(Crawl4AiConfig::new("http://127.0.0.1:11235/#fragment", None).is_err());
    assert!(Crawl4AiConfig::new("http://127.0.0.1:11235\n", None).is_err());

    let config = Crawl4AiConfig::new("http://127.0.0.1:11235", Some(TOKEN.to_owned()))?;
    let debug = format!("{config:?}");
    assert!(!debug.contains(TOKEN));
    assert!(debug.contains("<redacted>"));
    let public_error = CrawlerAdapterError::RemoteFailure {
        status_code: Some(503),
    };
    assert!(!format!("{public_error:?}").contains(TOKEN));
    assert!(!public_error.to_string().contains(TOKEN));
    Ok(())
}

#[tokio::test]
async fn crawl_request_is_safe_and_uses_bearer_auth() -> Result<(), Box<dyn Error>> {
    let server =
        FixtureServer::start(FixtureResponse::json(200, &crawl_body(&base_result()))?).await?;
    let adapter = adapter(&server, Some(TOKEN))?;
    let evidence = CrawlerEvidencePolicy {
        raw_html: true,
        cleaned_html: true,
        markdown: true,
        screenshot: ScreenshotPolicy::Viewport,
        discovered_links: true,
        ..CrawlerEvidencePolicy::default()
    };
    let request = request_with(
        evidence,
        Some("main article"),
        Some(3),
        Duration::from_secs(5),
    )?;

    let result = adapter.execute(request).await?;
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/crawl");
    assert!(requests[0].authorization_matches_token);
    assert!(!requests[0].path.contains(TOKEN));

    let payload: Value = serde_json::from_slice(&requests[0].body)?;
    assert_eq!(payload["urls"], json!([TARGET_URL]));
    assert_eq!(payload["browser_config"]["type"], "BrowserConfig");
    assert_eq!(payload["browser_config"]["params"]["headless"], true);
    assert_eq!(
        payload["browser_config"]["params"]["user_agent"],
        "ErabiCrawler/0.1"
    );
    assert_eq!(payload["crawler_config"]["type"], "CrawlerRunConfig");
    assert_eq!(
        payload["crawler_config"]["params"]["cache_mode"],
        json!({"type": "CacheMode", "params": "bypass"})
    );
    assert_eq!(payload["crawler_config"]["params"]["page_timeout"], 5_000);
    assert_eq!(payload["crawler_config"]["params"]["stream"], false);
    assert_eq!(
        payload["crawler_config"]["params"]["wait_for"],
        "css:main article"
    );
    assert_eq!(payload["crawler_config"]["params"]["scan_full_page"], true);
    assert_eq!(payload["crawler_config"]["params"]["scroll_delay"], 0.0);
    assert_eq!(payload["crawler_config"]["params"]["max_scroll_steps"], 3);
    assert_eq!(payload["crawler_config"]["params"]["screenshot"], true);
    assert_eq!(
        payload["crawler_config"]["params"]["force_viewport_screenshot"],
        true
    );

    for forbidden in [
        "js_code",
        "c4a_script",
        "proxy_config",
        "extra_args",
        "user_data_dir",
        "cdp_url",
        "cookies",
        "headers",
        "init_scripts",
        "deep_crawl_strategy",
        "provider_queue_id",
    ] {
        assert!(
            !contains_key(&payload, forbidden),
            "forbidden field was serialized: {forbidden}"
        );
    }
    assert!(!format!("{adapter:?}").contains(TOKEN));
    assert!(!format!("{result:?}").contains(TOKEN));
    Ok(())
}

#[tokio::test]
async fn full_page_screenshot_is_rejected_before_network() -> Result<(), Box<dyn Error>> {
    let server = FixtureServer::start(FixtureResponse::Hold).await?;
    let adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        screenshot: ScreenshotPolicy::FullPage,
        ..CrawlerEvidencePolicy::default()
    };

    let Err(error) = adapter
        .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
        .await
    else {
        return Err(fixture_error("full-page screenshot request was accepted").into());
    };
    assert!(matches!(error, CrawlerAdapterError::UnsupportedCapability));
    assert!(server.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn no_screenshot_policy_omits_capture_fields() -> Result<(), Box<dyn Error>> {
    let server =
        FixtureServer::start(FixtureResponse::json(200, &crawl_body(&base_result()))?).await?;
    let adapter = adapter(&server, None)?;

    adapter
        .execute(request_with(
            CrawlerEvidencePolicy::default(),
            None,
            None,
            Duration::from_secs(5),
        )?)
        .await?;

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    let payload: Value = serde_json::from_slice(&requests[0].body)?;
    let params = payload["crawler_config"]["params"]
        .as_object()
        .ok_or_else(|| fixture_error("crawler params are not an object"))?;
    assert_eq!(params.get("screenshot"), Some(&json!(false)));
    assert!(!params.contains_key("force_viewport_screenshot"));
    Ok(())
}

#[tokio::test]
async fn crawl_maps_result_metadata_artifacts_and_links() -> Result<(), Box<dyn Error>> {
    let server =
        FixtureServer::start(FixtureResponse::json(200, &crawl_body(&base_result()))?).await?;
    let adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        raw_html: true,
        cleaned_html: true,
        markdown: true,
        screenshot: ScreenshotPolicy::Viewport,
        discovered_links: true,
        ..CrawlerEvidencePolicy::default()
    };

    let result = adapter
        .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
        .await?;

    assert_eq!(result.observation().requested_url, TARGET_URL);
    assert_eq!(
        result.observation().final_url.as_deref(),
        Some("https://target.test/redirected?provider=1")
    );
    assert!(result.observation().artifact_ids.is_empty());
    assert_eq!(result.observation().discovered_links.len(), 3);
    assert_eq!(result.observation().discovered_links[0].raw_href, "/next");
    assert_eq!(result.observation().discovered_links[1].raw_href, "");
    assert_eq!(result.response().status_code(), Some(200));
    assert_eq!(
        result
            .response()
            .media_type()
            .map(ToString::to_string)
            .as_deref(),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(result.response().content_length_bytes(), Some(30));
    assert_eq!(result.response().provider_elapsed_ms(), Some(125));
    assert_eq!(result.artifacts().len(), 4);
    assert!(result.artifacts().iter().any(|artifact| matches!(
        artifact,
        CrawlerArtifactEvidence::RawHtml(body) if body.as_ref().contains("raw")
    )));
    assert!(result.artifacts().iter().any(|artifact| matches!(
        artifact,
        CrawlerArtifactEvidence::CleanedHtml(body) if body.as_ref().contains("clean")
    )));
    assert!(result.artifacts().iter().any(|artifact| matches!(
        artifact,
        CrawlerArtifactEvidence::Markdown(body) if body.as_ref().contains("Markdown")
    )));
    assert!(result.artifacts().iter().any(|artifact| matches!(
        artifact,
        CrawlerArtifactEvidence::Screenshot { bytes, .. }
            if bytes.as_ref() == &[1_u8, 2, 3][..]
    )));
    assert_eq!(result.completeness(), CrawlerResultCompleteness::Complete);
    Ok(())
}

#[tokio::test]
async fn rendered_html_uses_the_provider_html_field() -> Result<(), Box<dyn Error>> {
    let server =
        FixtureServer::start(FixtureResponse::json(200, &crawl_body(&base_result()))?).await?;
    let adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        rendered_html: true,
        ..CrawlerEvidencePolicy::default()
    };

    let result = adapter
        .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
        .await?;

    assert!(result.artifacts().iter().any(|artifact| matches!(
        artifact,
        CrawlerArtifactEvidence::RenderedHtml(body) if body.as_ref().contains("<html>")
    )));
    Ok(())
}

#[tokio::test]
async fn target_failures_remain_distinct_from_provider_failures() -> Result<(), Box<dyn Error>> {
    for (status, expected) in [
        (401, CrawlerAdapterError::AccessDenied),
        (403, CrawlerAdapterError::AccessDenied),
        (404, CrawlerAdapterError::NotFound),
        (
            429,
            CrawlerAdapterError::RateLimited {
                retry_after_ms: None,
            },
        ),
        (
            503,
            CrawlerAdapterError::RemoteFailure {
                status_code: Some(503),
            },
        ),
    ] {
        let mut result = base_result();
        result["success"] = json!(false);
        result["status_code"] = json!(status);
        let server =
            FixtureServer::start(FixtureResponse::json(200, &crawl_body(&result))?).await?;
        let adapter = adapter(&server, None)?;
        let Err(error) = adapter
            .execute(request_with(
                CrawlerEvidencePolicy::default(),
                None,
                None,
                Duration::from_secs(5),
            )?)
            .await
        else {
            return Err(fixture_error("target failure was accepted").into());
        };
        assert_eq!(error, expected);
    }

    let mut result = base_result();
    result["success"] = json!(false);
    result["status_code"] = json!(429);
    result["response_headers"] = json!({ "retry-after": "2" });
    let server = FixtureServer::start(FixtureResponse::json(200, &crawl_body(&result))?).await?;
    let adapter = adapter(&server, None)?;
    let Err(error) = adapter
        .execute(request_with(
            CrawlerEvidencePolicy::default(),
            None,
            None,
            Duration::from_secs(5),
        )?)
        .await
    else {
        return Err(fixture_error("rate-limited target was accepted").into());
    };
    assert_eq!(
        error,
        CrawlerAdapterError::RateLimited {
            retry_after_ms: Some(2_000),
        }
    );
    Ok(())
}

#[tokio::test]
async fn provider_statuses_do_not_become_target_not_found() -> Result<(), Box<dyn Error>> {
    for (status, response_headers, expected) in [
        (401, Vec::new(), CrawlerAdapterError::AccessDenied),
        (
            404,
            Vec::new(),
            CrawlerAdapterError::InvalidProviderResponse,
        ),
        (500, Vec::new(), CrawlerAdapterError::Unavailable),
        (
            429,
            vec![("retry-after".to_owned(), "2".to_owned())],
            CrawlerAdapterError::RateLimited {
                retry_after_ms: Some(2_000),
            },
        ),
        (
            429,
            vec![("retry-after".to_owned(), "999999".to_owned())],
            CrawlerAdapterError::RateLimited {
                retry_after_ms: None,
            },
        ),
    ] {
        let server = FixtureServer::start(FixtureResponse::Static {
            status,
            headers: response_headers,
            body: b"provider body is never exposed".to_vec(),
            advertised_length: None,
        })
        .await?;
        let adapter = adapter(&server, None)?;
        let Err(error) = adapter
            .execute(request_with(
                CrawlerEvidencePolicy::default(),
                None,
                None,
                Duration::from_secs(5),
            )?)
            .await
        else {
            return Err(fixture_error("provider status was accepted").into());
        };
        assert_eq!(error, expected);
    }
    Ok(())
}

#[tokio::test]
async fn malformed_success_and_oversized_response_fail_closed() -> Result<(), Box<dyn Error>> {
    let malformed = FixtureServer::start(FixtureResponse::Static {
        status: 200,
        headers: Vec::new(),
        body: b"{\"success\":true}".to_vec(),
        advertised_length: None,
    })
    .await?;
    let malformed_adapter = adapter(&malformed, None)?;
    assert!(matches!(
        malformed_adapter
            .execute(request_with(
                CrawlerEvidencePolicy::default(),
                None,
                None,
                Duration::from_secs(5),
            )?)
            .await,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    ));

    let oversized = FixtureServer::start(FixtureResponse::Static {
        status: 200,
        headers: Vec::new(),
        body: b"{}".to_vec(),
        advertised_length: Some(64 * 1024 * 1024 + 1),
    })
    .await?;
    let oversized_adapter = adapter(&oversized, None)?;
    assert!(matches!(
        oversized_adapter
            .execute(request_with(
                CrawlerEvidencePolicy::default(),
                None,
                None,
                Duration::from_secs(5),
            )?)
            .await,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    ));
    Ok(())
}

#[tokio::test]
async fn transport_and_timeout_are_bounded() -> Result<(), Box<dyn Error>> {
    let closed = FixtureServer::start(FixtureResponse::Close).await?;
    let closed_adapter = adapter(&closed, None)?;
    assert!(matches!(
        closed_adapter
            .execute(request_with(
                CrawlerEvidencePolicy::default(),
                None,
                None,
                Duration::from_secs(5),
            )?)
            .await,
        Err(CrawlerAdapterError::Unavailable)
    ));

    let held = FixtureServer::start(FixtureResponse::Hold).await?;
    let held_adapter = adapter(&held, None)?;
    assert!(matches!(
        held_adapter
            .execute(request_with(
                CrawlerEvidencePolicy::default(),
                None,
                None,
                Duration::from_millis(25),
            )?)
            .await,
        Err(CrawlerAdapterError::Timeout)
    ));
    Ok(())
}

#[tokio::test]
async fn unsupported_observation_provenance_stops_before_network() -> Result<(), Box<dyn Error>> {
    let server = FixtureServer::start(FixtureResponse::Hold).await?;
    let crawl_adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        selector_observations: true,
        pagination_observations: true,
        ..CrawlerEvidencePolicy::default()
    };

    assert!(matches!(
        crawl_adapter
            .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
            .await,
        Err(CrawlerAdapterError::UnsupportedCapability)
    ));
    assert!(server.requests().is_empty());

    let server = FixtureServer::start(FixtureResponse::Hold).await?;
    let adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        raw_html: true,
        rendered_html: true,
        ..CrawlerEvidencePolicy::default()
    };
    assert!(matches!(
        adapter
            .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
            .await,
        Err(CrawlerAdapterError::UnsupportedCapability)
    ));
    assert!(server.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn missing_requested_evidence_is_partial() -> Result<(), Box<dyn Error>> {
    let mut result = base_result();
    result["cleaned_html"] = Value::Null;
    result["screenshot"] = Value::Null;
    let server = FixtureServer::start(FixtureResponse::json(200, &crawl_body(&result))?).await?;
    let adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        cleaned_html: true,
        screenshot: ScreenshotPolicy::Viewport,
        ..CrawlerEvidencePolicy::default()
    };

    let result = adapter
        .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
        .await?;

    assert_eq!(
        result.completeness(),
        CrawlerResultCompleteness::Partial {
            reason: CrawlerPartialReason::RequestedEvidenceUnavailable
        }
    );
    Ok(())
}

#[tokio::test]
async fn oversized_inline_screenshot_is_rejected() -> Result<(), Box<dyn Error>> {
    let mut result = base_result();
    result["screenshot"] = json!(BASE64_STANDARD.encode(vec![0_u8; 16 * 1024 * 1024 + 1]));
    let server = FixtureServer::start(FixtureResponse::json(200, &crawl_body(&result))?).await?;
    let adapter = adapter(&server, None)?;
    let evidence = CrawlerEvidencePolicy {
        screenshot: ScreenshotPolicy::Viewport,
        ..CrawlerEvidencePolicy::default()
    };

    assert!(matches!(
        adapter
            .execute(request_with(evidence, None, None, Duration::from_secs(5))?)
            .await,
        Err(CrawlerAdapterError::InvalidProviderResponse)
    ));
    Ok(())
}
