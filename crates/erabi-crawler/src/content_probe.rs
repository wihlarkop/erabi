use std::time::Duration;

use erabi_domain::SourceTargetType;
use reqwest::{Response, StatusCode, header};
use url::Url;

use crate::ValidatedNetworkTarget;

/// The total time allowed for one HEAD plus any bounded GET fallback.
pub const DEFAULT_CONTENT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The maximum response prefix retained by the classifier.
pub const MAX_CONTENT_PROBE_PREFIX_BYTES: usize = 16 * 1024;

/// A transient direct-file category. It is intentionally mapped to the
/// provider-neutral durable `SourceTargetType` rather than replacing it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectFileKind {
    Pdf,
    Csv,
    Json,
    Archive,
    Image,
    OfficeDocument,
}

impl DirectFileKind {
    #[must_use]
    pub const fn source_target_type(self) -> SourceTargetType {
        SourceTargetType::FileAsset
    }
}

/// The evidence that made a direct-file decision possible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentEvidence {
    ContentType,
    Signature,
    ContentTypeAndSignature,
}

/// The safe pre-crawl routing decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentProbeDecision {
    NormalWebCrawl,
    FileAsset {
        kind: DirectFileKind,
        media_type: Option<String>,
        evidence: ContentEvidence,
    },
}

impl ContentProbeDecision {
    #[must_use]
    pub const fn source_target_type(&self) -> SourceTargetType {
        match self {
            Self::NormalWebCrawl => SourceTargetType::WebPage,
            Self::FileAsset { kind, .. } => kind.source_target_type(),
        }
    }

    #[must_use]
    pub const fn is_file_asset(&self) -> bool {
        matches!(self, Self::FileAsset { .. })
    }
}

/// A bounded, non-downloading direct-file probe.
#[derive(Clone, Copy, Debug)]
pub struct ContentProbe {
    timeout: Duration,
    prefix_limit: usize,
}

impl Default for ContentProbe {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_CONTENT_PROBE_TIMEOUT,
            prefix_limit: MAX_CONTENT_PROBE_PREFIX_BYTES,
        }
    }
}

impl ContentProbe {
    #[must_use]
    pub fn new(timeout: Duration, prefix_limit: usize) -> Self {
        Self {
            timeout: if timeout > DEFAULT_CONTENT_PROBE_TIMEOUT {
                DEFAULT_CONTENT_PROBE_TIMEOUT
            } else {
                timeout
            },
            prefix_limit: if prefix_limit > MAX_CONTENT_PROBE_PREFIX_BYTES {
                MAX_CONTENT_PROBE_PREFIX_BYTES
            } else {
                prefix_limit
            },
        }
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    #[must_use]
    pub const fn prefix_limit(&self) -> usize {
        self.prefix_limit
    }

    /// Probes one already-validated target and returns a conservative route.
    ///
    /// HEAD is authoritative only when it supplies a confident non-HTML media
    /// type. Unsupported or insufficient HEAD responses get one bounded GET
    /// fallback. Every transport, timeout, redirect, status, and contradictory
    /// evidence outcome falls back to normal web crawling.
    pub async fn probe(&self, target: &ValidatedNetworkTarget) -> ContentProbeDecision {
        if self.prefix_limit == 0 {
            return ContentProbeDecision::NormalWebCrawl;
        }
        let result = tokio::time::timeout(self.timeout, self.probe_inner(target)).await;
        result.unwrap_or(ContentProbeDecision::NormalWebCrawl)
    }

    async fn probe_inner(&self, target: &ValidatedNetworkTarget) -> ContentProbeDecision {
        let Ok(client) = target
            .reqwest_builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(self.timeout)
            .build()
        else {
            return ContentProbeDecision::NormalWebCrawl;
        };

        let Ok(head) = client.head(target.url().clone()).send().await else {
            return ContentProbeDecision::NormalWebCrawl;
        };
        match classify_head_response(&head) {
            HeadDecision::Confident(decision) => decision,
            HeadDecision::NormalWebCrawl => ContentProbeDecision::NormalWebCrawl,
            HeadDecision::GetFallback => self.get_fallback(&client, target).await,
        }
    }

    async fn get_fallback(
        &self,
        client: &reqwest::Client,
        target: &ValidatedNetworkTarget,
    ) -> ContentProbeDecision {
        let end = self.prefix_limit.saturating_sub(1);
        let Ok(response) = client
            .get(target.url().clone())
            .header(header::RANGE, format!("bytes=0-{end}"))
            .send()
            .await
        else {
            return ContentProbeDecision::NormalWebCrawl;
        };
        if !response.status().is_success() {
            return ContentProbeDecision::NormalWebCrawl;
        }

        let media_type = media_type(&response);
        let Some(prefix) = read_prefix(response, self.prefix_limit).await else {
            return ContentProbeDecision::NormalWebCrawl;
        };
        classify_prefix(target.url(), media_type.as_deref(), &prefix)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HeadDecision {
    Confident(ContentProbeDecision),
    GetFallback,
    NormalWebCrawl,
}

fn classify_head_response(response: &Response) -> HeadDecision {
    if response.status() == StatusCode::METHOD_NOT_ALLOWED
        || response.status() == StatusCode::NOT_IMPLEMENTED
    {
        return HeadDecision::GetFallback;
    }
    if !response.status().is_success() {
        return HeadDecision::NormalWebCrawl;
    }

    let media_type = media_type(response);
    if media_type.as_deref().is_some_and(is_html_media_type) {
        return HeadDecision::NormalWebCrawl;
    }
    match media_type.as_deref().and_then(media_type_kind) {
        Some(kind) => HeadDecision::Confident(ContentProbeDecision::FileAsset {
            kind,
            media_type,
            evidence: ContentEvidence::ContentType,
        }),
        None => HeadDecision::GetFallback,
    }
}

async fn read_prefix(mut response: Response, limit: usize) -> Option<Vec<u8>> {
    let mut prefix = Vec::with_capacity(limit);
    while prefix.len() < limit {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => return None,
        };
        if chunk.is_empty() {
            continue;
        }
        let remaining = limit - prefix.len();
        let amount = remaining.min(chunk.len());
        prefix.extend_from_slice(&chunk[..amount]);
        if amount < chunk.len() {
            break;
        }
    }
    Some(prefix)
}

fn classify_prefix(_url: &Url, media_type: Option<&str>, prefix: &[u8]) -> ContentProbeDecision {
    if media_type.is_some_and(is_html_media_type) || looks_like_html(prefix) {
        return ContentProbeDecision::NormalWebCrawl;
    }

    let signature_kind = signature_kind(prefix, media_type);
    let typed_kind = media_type.and_then(media_type_kind);
    if let (Some(signature), Some(media)) = (signature_kind, typed_kind) {
        if !compatible_evidence(signature, media) {
            return ContentProbeDecision::NormalWebCrawl;
        }
        return ContentProbeDecision::FileAsset {
            kind: media,
            media_type: media_type.map(str::to_owned),
            evidence: ContentEvidence::ContentTypeAndSignature,
        };
    }
    if let Some(signature) = signature_kind {
        if media_type.is_some_and(|value| value != "application/octet-stream") {
            return ContentProbeDecision::NormalWebCrawl;
        }
        return ContentProbeDecision::FileAsset {
            kind: signature,
            media_type: media_type.map(str::to_owned),
            evidence: ContentEvidence::Signature,
        };
    }
    if let Some(kind) = typed_kind {
        return ContentProbeDecision::FileAsset {
            kind,
            media_type: media_type.map(str::to_owned),
            evidence: ContentEvidence::ContentType,
        };
    }
    ContentProbeDecision::NormalWebCrawl
}

fn media_type(response: &Response) -> Option<String> {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or_default().trim())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value.chars().all(|character| !character.is_control())
        })
        .map(str::to_ascii_lowercase)
}

fn is_html_media_type(value: &str) -> bool {
    matches!(value, "text/html" | "application/xhtml+xml")
}

fn media_type_kind(value: &str) -> Option<DirectFileKind> {
    match value {
        "application/pdf" => Some(DirectFileKind::Pdf),
        "text/csv" | "application/csv" => Some(DirectFileKind::Csv),
        "application/json" | "text/json" | "application/ld+json" => Some(DirectFileKind::Json),
        "application/zip"
        | "application/x-zip-compressed"
        | "application/gzip"
        | "application/x-gzip"
        | "application/x-bzip2"
        | "application/x-xz"
        | "application/x-7z-compressed"
        | "application/vnd.rar"
        | "application/x-rar-compressed"
        | "application/x-tar" => Some(DirectFileKind::Archive),
        value if value.starts_with("image/") => Some(DirectFileKind::Image),
        "application/msword"
        | "application/vnd.ms-excel"
        | "application/vnd.ms-powerpoint"
        | "application/rtf"
        | "text/rtf"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        | "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        | "application/vnd.oasis.opendocument.text"
        | "application/vnd.oasis.opendocument.spreadsheet"
        | "application/vnd.oasis.opendocument.presentation"
        | "application/vnd.apple.pages" => Some(DirectFileKind::OfficeDocument),
        _ => None,
    }
}

fn signature_kind(prefix: &[u8], media_type: Option<&str>) -> Option<DirectFileKind> {
    if prefix.starts_with(b"%PDF-") {
        return Some(DirectFileKind::Pdf);
    }
    if prefix.starts_with(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1") || prefix.starts_with(br"{\rtf") {
        return Some(DirectFileKind::OfficeDocument);
    }
    if prefix.starts_with(b"\x89PNG\r\n\x1a\n")
        || prefix.starts_with(b"\xff\xd8\xff")
        || prefix.starts_with(b"GIF87a")
        || prefix.starts_with(b"GIF89a")
        || prefix.starts_with(b"BM")
        || prefix.starts_with(b"II*\0")
        || prefix.starts_with(b"MM\0*")
        || (prefix.len() >= 12 && prefix.starts_with(b"RIFF") && &prefix[8..12] == b"WEBP")
        || prefix.starts_with(b"\0\0\x01\0")
    {
        return Some(DirectFileKind::Image);
    }
    if prefix.starts_with(b"PK\x03\x04")
        || prefix.starts_with(b"PK\x05\x06")
        || prefix.starts_with(b"PK\x07\x08")
        || prefix.starts_with(b"\x1f\x8b")
        || prefix.starts_with(b"BZh")
        || prefix.starts_with(b"\xfd7zXZ\0")
        || prefix.starts_with(b"7z\xbc\xaf\x27\x1c")
        || prefix.starts_with(b"Rar!\x1a\x07")
        || (prefix.len() >= 262 && &prefix[257..262] == b"ustar")
    {
        if media_type
            == Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            || media_type
                == Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            || media_type
                == Some("application/vnd.openxmlformats-officedocument.presentationml.presentation")
        {
            return Some(DirectFileKind::OfficeDocument);
        }
        return Some(DirectFileKind::Archive);
    }
    None
}

fn compatible_evidence(signature: DirectFileKind, media: DirectFileKind) -> bool {
    signature == media
        || (signature == DirectFileKind::Archive && media == DirectFileKind::OfficeDocument)
        || (signature == DirectFileKind::OfficeDocument && media == DirectFileKind::Archive)
}

fn looks_like_html(prefix: &[u8]) -> bool {
    let prefix = prefix.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(prefix);
    let text = String::from_utf8_lossy(prefix);
    let text = text.trim_start().to_ascii_lowercase();
    [
        "<!doctype html",
        "<html",
        "<head",
        "<body",
        "<title",
        "<script",
    ]
    .iter()
    .any(|marker| text.starts_with(marker))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::{
        ContentEvidence, ContentProbe, ContentProbeDecision, DirectFileKind, classify_prefix,
    };
    use crate::network_policy::ValidatedNetworkTarget;

    #[derive(Clone)]
    struct FixtureResponse {
        status: u16,
        media_type: Option<&'static str>,
        location: Option<&'static str>,
        body: Vec<u8>,
    }

    impl FixtureResponse {
        fn new(status: u16, media_type: Option<&'static str>, body: Vec<u8>) -> Self {
            Self {
                status,
                media_type,
                location: None,
                body,
            }
        }
    }

    async fn fixture_target(
        path: &str,
        head: FixtureResponse,
        get: FixtureResponse,
        expected_requests: usize,
        saw_range: Arc<AtomicBool>,
    ) -> Result<(ValidatedNetworkTarget, tokio::task::JoinHandle<()>), Box<dyn Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let url = format!("http://probe.test:{}/{}", address.port(), path).parse()?;
        let target = ValidatedNetworkTarget::for_test(url, address);
        let task = tokio::spawn(async move {
            for _ in 0..expected_requests {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let saw_range = Arc::clone(&saw_range);
                let head = head.clone();
                let get = get.clone();
                tokio::spawn(async move {
                    respond(stream, head, get, saw_range).await;
                });
            }
        });
        Ok((target, task))
    }

    async fn respond(
        mut stream: TcpStream,
        head: FixtureResponse,
        get: FixtureResponse,
        saw_range: Arc<AtomicBool>,
    ) {
        let mut request = vec![0; 8 * 1024];
        let read = stream.read(&mut request).await.unwrap_or_default();
        let request = &request[..read];
        let is_head = request.starts_with(b"HEAD ");
        if !is_head
            && request
                .windows(b"range: bytes=0-31".len())
                .any(|window| window.eq_ignore_ascii_case(b"range: bytes=0-31"))
        {
            saw_range.store(true, Ordering::SeqCst);
        }
        let response = if is_head { head } else { get };
        let reason = match response.status {
            200 => "OK",
            206 => "Partial Content",
            302 => "Found",
            405 => "Method Not Allowed",
            501 => "Not Implemented",
            _ => "Fixture",
        };
        let mut headers = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            response.status,
            reason,
            response.body.len()
        );
        if let Some(media_type) = response.media_type {
            headers.push_str("Content-Type: ");
            headers.push_str(media_type);
            headers.push_str("\r\n");
        }
        if let Some(location) = response.location {
            headers.push_str("Location: ");
            headers.push_str(location);
            headers.push_str("\r\n");
        }
        headers.push_str("\r\n");
        let _ = stream.write_all(headers.as_bytes()).await;
        let _ = stream.write_all(&response.body).await;
        let _ = stream.shutdown().await;
    }

    fn classify(path: &str, media_type: Option<&str>, prefix: &[u8]) -> ContentProbeDecision {
        let url = match path.parse() {
            Ok(url) => url,
            Err(error) => panic!("test URL must parse: {error}"),
        };
        classify_prefix(&url, media_type, prefix)
    }

    #[test]
    fn recognizes_direct_file_signatures_without_trusting_extensions() {
        assert_eq!(
            classify("https://example.test/document.pdf", None, b"%PDF-1.7"),
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Pdf,
                media_type: None,
                evidence: ContentEvidence::Signature,
            }
        );
        assert_eq!(
            classify(
                "https://example.test/document.txt",
                Some("application/octet-stream"),
                b"%PDF-1.7"
            ),
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Pdf,
                media_type: Some("application/octet-stream".to_owned()),
                evidence: ContentEvidence::Signature,
            }
        );
        assert!(matches!(
            classify(
                "https://example.test/document.pdf",
                Some("text/plain"),
                b"plain text"
            ),
            ContentProbeDecision::NormalWebCrawl
        ));
    }

    #[test]
    fn recognizes_mime_categories_and_keeps_html_on_the_web_path() {
        for (media_type, kind) in [
            ("text/csv", DirectFileKind::Csv),
            ("application/json", DirectFileKind::Json),
            ("application/zip", DirectFileKind::Archive),
            ("image/png", DirectFileKind::Image),
            (
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                DirectFileKind::OfficeDocument,
            ),
        ] {
            assert!(matches!(
                classify("https://example.test/resource", Some(media_type), b"data"),
                ContentProbeDecision::FileAsset { kind: actual, .. } if actual == kind
            ));
        }
        assert_eq!(
            classify(
                "https://example.test/file.pdf",
                Some("text/html"),
                b"%PDF-1.7"
            ),
            ContentProbeDecision::NormalWebCrawl
        );
        assert_eq!(
            classify(
                "https://example.test/file.html",
                Some("application/pdf"),
                b"%PDF-1.7"
            ),
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Pdf,
                media_type: Some("application/pdf".to_owned()),
                evidence: ContentEvidence::ContentTypeAndSignature,
            }
        );
    }

    #[test]
    fn rejects_html_and_contradictory_evidence() {
        assert_eq!(
            classify(
                "https://example.test/file.pdf",
                Some("application/pdf"),
                b"<html>"
            ),
            ContentProbeDecision::NormalWebCrawl
        );
        assert_eq!(
            classify(
                "https://example.test/file.bin",
                Some("application/zip"),
                b"%PDF-1.7"
            ),
            ContentProbeDecision::NormalWebCrawl
        );
        assert_eq!(
            classify("https://example.test/file.pdf", None, b"<!doctype html>"),
            ContentProbeDecision::NormalWebCrawl
        );
    }

    #[test]
    fn does_not_classify_extension_alone() {
        assert_eq!(
            classify("https://example.test/report.csv", None, b"a,b\n1,2\n"),
            ContentProbeDecision::NormalWebCrawl
        );
        assert_eq!(
            classify(
                "https://example.test/archive.zip",
                Some("text/plain"),
                b"plain text"
            ),
            ContentProbeDecision::NormalWebCrawl
        );
    }

    #[test]
    fn zip_signature_and_office_extension_remain_an_archive_without_office_evidence() {
        assert_eq!(
            classify("https://example.test/document.docx", None, b"PK\x03\x04"),
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Archive,
                media_type: None,
                evidence: ContentEvidence::Signature,
            }
        );
    }

    #[test]
    fn maps_every_direct_file_kind_to_the_durable_file_asset_type() {
        for kind in [
            DirectFileKind::Pdf,
            DirectFileKind::Csv,
            DirectFileKind::Json,
            DirectFileKind::Archive,
            DirectFileKind::Image,
            DirectFileKind::OfficeDocument,
        ] {
            assert_eq!(
                kind.source_target_type(),
                erabi_domain::SourceTargetType::FileAsset
            );
        }
    }

    #[tokio::test]
    async fn falls_back_from_unsupported_head_to_one_bounded_get() -> Result<(), Box<dyn Error>> {
        let saw_range = Arc::new(AtomicBool::new(false));
        let mut body = b"%PDF-1.7".to_vec();
        body.resize(64 * 1024, b'x');
        let (target, task) = fixture_target(
            "report.pdf",
            FixtureResponse::new(405, None, Vec::new()),
            FixtureResponse::new(200, Some("application/octet-stream"), body),
            2,
            Arc::clone(&saw_range),
        )
        .await?;
        let decision = ContentProbe::new(Duration::from_secs(2), 32)
            .probe(&target)
            .await;
        task.await?;

        assert_eq!(
            decision,
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Pdf,
                media_type: Some("application/octet-stream".to_owned()),
                evidence: ContentEvidence::Signature,
            }
        );
        assert!(saw_range.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn confident_head_media_type_avoids_get_fallback() -> Result<(), Box<dyn Error>> {
        let saw_range = Arc::new(AtomicBool::new(false));
        let (target, task) = fixture_target(
            "report.pdf",
            FixtureResponse::new(200, Some("application/pdf"), Vec::new()),
            FixtureResponse::new(200, Some("text/html"), b"<html>".to_vec()),
            1,
            Arc::clone(&saw_range),
        )
        .await?;
        let decision = ContentProbe::default().probe(&target).await;
        task.await?;

        assert!(matches!(
            decision,
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Pdf,
                evidence: ContentEvidence::ContentType,
                ..
            }
        ));
        assert!(!saw_range.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn falls_back_when_head_has_no_media_type() -> Result<(), Box<dyn Error>> {
        let saw_range = Arc::new(AtomicBool::new(false));
        let (target, task) = fixture_target(
            "data.json",
            FixtureResponse::new(200, None, Vec::new()),
            FixtureResponse::new(206, Some("application/json"), br#"{"items":[]}"#.to_vec()),
            2,
            Arc::clone(&saw_range),
        )
        .await?;
        let decision = ContentProbe::default().probe(&target).await;
        task.await?;

        assert!(matches!(
            decision,
            ContentProbeDecision::FileAsset {
                kind: DirectFileKind::Json,
                evidence: ContentEvidence::ContentType,
                ..
            }
        ));
        assert!(!saw_range.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_bounded_get_falls_back_to_web_crawl() -> Result<(), Box<dyn Error>> {
        let saw_range = Arc::new(AtomicBool::new(false));
        let (target, task) = fixture_target(
            "report.pdf",
            FixtureResponse::new(405, None, Vec::new()),
            FixtureResponse::new(200, Some("text/plain"), b"not a document".to_vec()),
            2,
            Arc::clone(&saw_range),
        )
        .await?;
        let decision = ContentProbe::new(Duration::from_secs(2), 32)
            .probe(&target)
            .await;
        task.await?;

        assert_eq!(decision, ContentProbeDecision::NormalWebCrawl);
        assert!(saw_range.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn redirects_are_not_followed_and_return_web_crawl_fallback() -> Result<(), Box<dyn Error>>
    {
        let saw_range = Arc::new(AtomicBool::new(false));
        let mut head = FixtureResponse::new(302, Some("application/pdf"), Vec::new());
        head.location = Some("http://127.0.0.1/private");
        let (target, task) = fixture_target(
            "redirect",
            head,
            FixtureResponse::new(200, Some("application/pdf"), b"%PDF-1.7".to_vec()),
            1,
            Arc::clone(&saw_range),
        )
        .await?;
        let decision = ContentProbe::default().probe(&target).await;
        task.await?;

        assert_eq!(decision, ContentProbeDecision::NormalWebCrawl);
        assert!(!saw_range.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_and_zero_timeout_probes_fall_back_without_download()
    -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        drop(listener);
        let url = format!("http://unavailable.test:{}/file.pdf", address.port()).parse()?;
        let target = ValidatedNetworkTarget::for_test(url, address);

        assert_eq!(
            ContentProbe::new(Duration::from_secs(1), 64)
                .probe(&target)
                .await,
            ContentProbeDecision::NormalWebCrawl
        );
        assert_eq!(
            ContentProbe::new(Duration::ZERO, 64).probe(&target).await,
            ContentProbeDecision::NormalWebCrawl
        );
        Ok(())
    }
}
