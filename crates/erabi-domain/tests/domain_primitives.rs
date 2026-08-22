use erabi_domain::{
    ArtifactId, CanonicalizationPolicyId, CollectionId, CrawlRunId, CrawlRunStatus, CrawlRunType,
    CrawlerId, CrawlerVersionId, DiscoveryTransitionId, DomainScopeId, ErrorCode, PageTypeId,
    RunProfileId, SeedId, SourceId, TestEvidenceId,
};

#[test]
fn typed_ids_are_uuid_v7_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    macro_rules! assert_id_contract {
        ($id_type:ty) => {{
            let id = <$id_type>::new();
            assert_eq!(id.as_uuid().get_version_num(), 7);
            let restored: $id_type = serde_json::from_str(&serde_json::to_string(&id)?)?;
            assert_eq!(restored, id);
            assert_eq!(id.to_string().parse::<uuid::Uuid>()?, *id.as_uuid());
        }};
    }

    assert_id_contract!(CrawlerId);
    assert_id_contract!(CrawlerVersionId);
    assert_id_contract!(SeedId);
    assert_id_contract!(PageTypeId);
    assert_id_contract!(DiscoveryTransitionId);
    assert_id_contract!(SourceId);
    assert_id_contract!(CollectionId);
    assert_id_contract!(RunProfileId);
    assert_id_contract!(TestEvidenceId);
    assert_id_contract!(CrawlRunId);
    assert_id_contract!(ArtifactId);
    assert_id_contract!(CanonicalizationPolicyId);
    assert_id_contract!(DomainScopeId);
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
