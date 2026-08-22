use erabi_domain::{
    CanonicalizationPolicyId, Crawler, CrawlerId, CrawlerVersion, CrawlerVersionId,
    CrawlerVersionState, DiscoveryTransitionId, DomainScopeId, OperationalOverrides, PageTypeId,
    RunProfile, Seed,
};
fn seed() -> Result<Seed, url::ParseError> {
    Ok(Seed::new(
        "https://example.test/a".parse()?,
        "https://example.test/a".parse()?,
    ))
}
fn published(crawler_id: CrawlerId) -> Result<CrawlerVersion, erabi_domain::ProductError> {
    let mut version = CrawlerVersion::draft(crawler_id);
    version.publish()?;
    Ok(version)
}

#[test]
fn published_configuration_rejects_all_mutation_after_roundtrip()
-> Result<(), Box<dyn std::error::Error>> {
    let version = published(CrawlerId::new())?;
    let mut restored: CrawlerVersion = serde_json::from_str(&serde_json::to_string(&version)?)?;
    assert!(restored.add_seed(seed()?).is_err());
    assert!(restored.set_page_type_ids(vec![PageTypeId::new()]).is_err());
    assert!(
        restored
            .set_transition_ids(vec![DiscoveryTransitionId::new()])
            .is_err()
    );
    assert!(
        restored
            .set_canonicalization_policy_id(Some(CanonicalizationPolicyId::new()))
            .is_err()
    );
    assert!(
        restored
            .set_domain_scope_id(Some(DomainScopeId::new()))
            .is_err()
    );
    assert!(
        restored
            .set_operational_defaults(OperationalOverrides::default())
            .is_err()
    );
    assert_eq!(restored.state(), CrawlerVersionState::Published);
    Ok(())
}

#[test]
fn published_creates_new_typed_draft_and_draft_cannot_clone()
-> Result<(), Box<dyn std::error::Error>> {
    let crawler_id = CrawlerId::new();
    let published = published(crawler_id)?;
    let mut draft = published.draft_from_published()?;
    let _: CrawlerVersionId = draft.id();
    assert_ne!(draft.id(), published.id());
    assert_eq!(draft.state(), CrawlerVersionState::Draft);
    draft.add_seed(seed()?)?;
    assert!(
        CrawlerVersion::draft(crawler_id)
            .draft_from_published()
            .is_err()
    );
    Ok(())
}

#[test]
fn crawler_lifecycle_operations_validate_ownership_and_state()
-> Result<(), Box<dyn std::error::Error>> {
    let mut crawler = Crawler::new("Catalog");
    let own_draft = CrawlerVersion::draft(crawler.id());
    let foreign_draft = CrawlerVersion::draft(CrawlerId::new());
    crawler.activate_draft(&own_draft)?;
    assert_eq!(crawler.active_draft_version_id(), Some(own_draft.id()));
    assert!(crawler.activate_draft(&foreign_draft).is_err());
    assert_eq!(crawler.active_draft_version_id(), Some(own_draft.id()));
    assert!(crawler.reactivate_published(&own_draft).is_err());
    assert_eq!(crawler.active_published_version_id(), None);
    let published = published(crawler.id())?;
    crawler.reactivate_published(&published)?;
    assert_eq!(crawler.active_published_version_id(), Some(published.id()));
    Ok(())
}

#[test]
fn draft_configuration_workflow_uses_typed_references() -> Result<(), Box<dyn std::error::Error>> {
    let mut version = CrawlerVersion::draft(CrawlerId::new());
    let page_type = PageTypeId::new();
    let transition = DiscoveryTransitionId::new();
    version.add_seed(seed()?)?;
    version.set_page_type_ids(vec![page_type])?;
    version.set_transition_ids(vec![transition])?;
    version.set_canonicalization_policy_id(Some(CanonicalizationPolicyId::new()))?;
    version.set_domain_scope_id(Some(DomainScopeId::new()))?;
    version.set_operational_defaults(OperationalOverrides {
        max_pages: Some(5),
        ..OperationalOverrides::default()
    })?;
    assert_eq!(version.page_type_ids(), &[page_type]);
    assert_eq!(version.transition_ids(), &[transition]);
    let profile = RunProfile::new("Bounded", OperationalOverrides::default());
    let _: erabi_domain::RunProfileId = profile.id();
    Ok(())
}
