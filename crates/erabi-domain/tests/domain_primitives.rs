use erabi_domain::{CrawlRunStatus, CrawlRunType, EntityId, ErrorCode};

#[test]
fn entity_ids_are_uuid_v7_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let id = EntityId::new();
    assert_eq!(id.as_uuid().get_version_num(), 7);
    assert_eq!(id.to_string().parse::<uuid::Uuid>()?, *id.as_uuid());
    Ok(())
}

#[test]
fn run_types_are_exactly_the_four_mvp_types() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        serde_json::to_string(&[
            CrawlRunType::QuickScrape,
            CrawlRunType::TestRun,
            CrawlRunType::DiscoveryPreview,
            CrawlRunType::ProductionRun
        ])?,
        r#"["QUICK_SCRAPE","TEST_RUN","DISCOVERY_PREVIEW","PRODUCTION_RUN"]"#
    );
    Ok(())
}

#[test]
fn lifecycle_and_error_codes_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        serde_json::to_string(&CrawlRunStatus::PartialResult)?,
        r#""PARTIAL_RESULT""#
    );
    assert_eq!(
        serde_json::to_string(&ErrorCode::AmbiguousPageType)?,
        r#""AMBIGUOUS_PAGE_TYPE""#
    );
    Ok(())
}
