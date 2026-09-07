use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
};

use erabi_crawler::{
    ContentEvidence, ContentProbeDecision, ContentProbeExecutor, DirectFileKind,
    NetworkTargetPolicy, SourceIntakeRequest, SourceIntakeService, StaticNetworkResolver,
    ValidatedNetworkTarget,
};
use erabi_db::{ErabiDatabase, MigrationRunner};
use erabi_domain::{Source, SourceId, SourceStatus, SourceTargetType};

struct FixedProbe(ContentProbeDecision);

impl ContentProbeExecutor for FixedProbe {
    fn probe<'probe>(
        &'probe self,
        _target: &'probe ValidatedNetworkTarget,
    ) -> erabi_crawler::ContentProbeFuture<'probe> {
        let decision = self.0.clone();
        Box::pin(async move { decision })
    }
}

fn policy() -> NetworkTargetPolicy {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443);
    NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::single(
        "example.test",
        address,
    )))
}

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

#[tokio::test]
async fn intake_preserves_original_url_and_reuses_the_canonical_source()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let probe = Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl));
    let service = SourceIntakeService::with_probe_executor(&database, policy(), probe);
    let first = service
        .intake(&SourceIntakeRequest::new(
            "HTTPS://Example.test/?utm_source=ignored",
            None,
        ))
        .await?;
    let second = service
        .intake(&SourceIntakeRequest::new("https://example.test/", None))
        .await?;

    assert_eq!(first.source.id, second.source.id);
    assert_eq!(
        first.original_url,
        "HTTPS://Example.test/?utm_source=ignored"
    );
    assert_eq!(first.canonical_url.as_str(), "https://example.test/");
    assert_eq!(
        first.source.original_url.as_str(),
        "https://example.test/?utm_source=ignored"
    );
    assert_eq!(second.source.original_url, first.source.original_url);
    assert_eq!(
        first.decision.source_target_type(),
        SourceTargetType::WebPage
    );
    Ok(())
}

#[tokio::test]
async fn confident_file_probe_upgrades_only_source_classification()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let probe = Arc::new(FixedProbe(ContentProbeDecision::FileAsset {
        kind: DirectFileKind::Pdf,
        media_type: Some("application/pdf".to_owned()),
        evidence: ContentEvidence::ContentType,
    }));
    let service = SourceIntakeService::with_probe_executor(&database, policy(), probe);
    let result = service
        .intake(&SourceIntakeRequest::new(
            "https://example.test/report.pdf",
            None,
        ))
        .await?;

    assert_eq!(
        result.decision.source_target_type(),
        SourceTargetType::FileAsset
    );
    assert_eq!(result.source.target_type, SourceTargetType::FileAsset);
    assert_eq!(result.source.status, erabi_domain::SourceStatus::Active);
    Ok(())
}

#[tokio::test]
async fn security_rejection_is_not_downgraded_to_web_crawl()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let probe = Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl));
    let service = SourceIntakeService::with_probe_executor(&database, policy(), probe);

    let result = service
        .intake(&SourceIntakeRequest::new("http://127.0.0.1/", None))
        .await;
    assert!(matches!(
        result,
        Err(erabi_crawler::SourceIntakeError::NetworkTarget(_))
    ));
    Ok(())
}

#[tokio::test]
async fn fragment_rejection_is_preserved_before_canonicalization_can_strip_it()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let probe = Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl));
    let service = SourceIntakeService::with_probe_executor(&database, policy(), probe);

    let result = service
        .intake(&SourceIntakeRequest::new(
            "https://example.test/page#fragment",
            None,
        ))
        .await;
    assert!(matches!(
        result,
        Err(erabi_crawler::SourceIntakeError::NetworkTarget(
            erabi_crawler::NetworkTargetError::FragmentNotAllowed
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn credential_bearing_urls_are_rejected_before_probe_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let probe = Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl));
    let service = SourceIntakeService::with_probe_executor(&database, policy(), probe);

    let result = service
        .intake(&SourceIntakeRequest::new(
            "https://user:password@example.test/page",
            None,
        ))
        .await;
    assert!(matches!(
        result,
        Err(erabi_crawler::SourceIntakeError::Canonicalization(_))
    ));
    Ok(())
}

#[tokio::test]
async fn generated_source_name_is_bounded_for_a_valid_long_url()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let probe = Arc::new(FixedProbe(ContentProbeDecision::NormalWebCrawl));
    let service = SourceIntakeService::with_probe_executor(&database, policy(), probe);
    let long_url = format!("https://example.test/{}", "a".repeat(600));

    let result = service
        .intake(&SourceIntakeRequest::new(long_url, None))
        .await?;

    assert_eq!(result.source.name.chars().count(), 512);
    Ok(())
}

#[test]
fn source_intake_debug_redacts_url_query_values() -> Result<(), Box<dyn std::error::Error>> {
    let request = SourceIntakeRequest::new("https://example.test/path?token=secret", None);
    let source = Source {
        id: SourceId::new(),
        collection_id: None,
        name: "Example source".to_owned(),
        original_url: "https://example.test/path?token=secret".parse()?,
        canonical_url: "https://example.test/path?token=secret".parse()?,
        target_type: SourceTargetType::WebPage,
        status: SourceStatus::Active,
        run_ids: Vec::new(),
        artifact_ids: Vec::new(),
    };
    let result = erabi_crawler::SourceIntakeResult {
        source,
        original_url: "https://example.test/path?token=secret".to_owned(),
        canonical_url: "https://example.test/path?token=secret".parse()?,
        decision: ContentProbeDecision::NormalWebCrawl,
    };

    let debug = format!("{request:?} {result:?}");
    assert!(!debug.contains("token=secret"));
    Ok(())
}

#[test]
fn probe_executor_future_is_send_for_runtime_composition() {
    fn assert_send<T: Send>() {}
    assert_send::<Pin<Box<dyn Future<Output = ContentProbeDecision> + Send>>>();
}
