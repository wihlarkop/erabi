use erabi_domain::{
    Crawler, CrawlerVersion, CrawlerVersionState, EntityId, OperationalOverrides, RunProfile, Seed,
};

fn seed() -> Result<Seed, url::ParseError> {
    Ok(Seed::new(
        "https://example.test/a".parse()?,
        "https://example.test/a".parse()?,
    ))
}

#[test]
fn crawler_version_published_versions_reject_configuration_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut version = CrawlerVersion::fixture_published();
    assert!(version.add_seed(seed()?).is_err());
    assert_eq!(version.state(), CrawlerVersionState::Published);
    Ok(())
}

#[test]
fn published_version_clones_to_distinct_editable_draft() -> Result<(), Box<dyn std::error::Error>> {
    let published = CrawlerVersion::fixture_published();
    let mut draft = published.draft_from_published();
    assert_ne!(draft.id(), published.id());
    assert_eq!(draft.state(), CrawlerVersionState::Draft);
    draft.add_seed(seed()?)?;
    assert!(published.seeds().is_empty());
    Ok(())
}

#[test]
fn crawler_enforces_one_active_draft_and_can_reactivate_published()
-> Result<(), Box<dyn std::error::Error>> {
    let mut crawler = Crawler::new("Catalog");
    let draft = CrawlerVersion::draft(crawler.id);
    let other_draft = CrawlerVersion::draft(crawler.id);
    crawler.activate_draft(&draft)?;
    assert!(crawler.activate_draft(&other_draft).is_err());

    let mut published = draft.draft_from_published();
    published.publish()?;
    crawler.reactivate_published(&published)?;
    assert_eq!(crawler.active_published_version_id, Some(published.id()));
    Ok(())
}

#[test]
fn run_profiles_contain_operational_fields_only() -> Result<(), Box<dyn std::error::Error>> {
    let profile = RunProfile::new(
        "Quick Test",
        OperationalOverrides {
            max_pages: Some(10),
            max_depth: Some(1),
            ..OperationalOverrides::default()
        },
    );
    assert_eq!(profile.name(), "Quick Test");
    assert_eq!(profile.overrides().max_pages, Some(10));
    let serialized = serde_json::to_value(profile)?;
    assert!(serialized.get("overrides").is_some());
    assert!(serialized.get("page_type_ids").is_none());
    Ok(())
}

#[test]
fn draft_configuration_methods_reject_published_versions_after_roundtrip()
-> Result<(), Box<dyn std::error::Error>> {
    let version = CrawlerVersion::fixture_published();
    let mut restored: CrawlerVersion = serde_json::from_str(&serde_json::to_string(&version)?)?;
    assert!(restored.set_page_type_ids(vec![EntityId::new()]).is_err());
    assert!(restored.set_transition_ids(vec![EntityId::new()]).is_err());
    assert!(
        restored
            .set_canonicalization_policy_id(Some(EntityId::new()))
            .is_err()
    );
    assert!(restored.set_domain_scope_id(Some(EntityId::new())).is_err());
    assert!(
        restored
            .set_operational_defaults(OperationalOverrides {
                max_pages: Some(1),
                ..OperationalOverrides::default()
            })
            .is_err()
    );
    assert!(restored.add_seed(seed()?).is_err());
    assert!(restored.publish().is_err());
    Ok(())
}

#[test]
fn draft_configuration_methods_allow_normal_editing() -> Result<(), Box<dyn std::error::Error>> {
    let mut version = CrawlerVersion::draft(EntityId::new());
    let page_type_id = EntityId::new();
    let transition_id = EntityId::new();
    version.add_seed(seed()?)?;
    version.set_page_type_ids(vec![page_type_id])?;
    version.set_transition_ids(vec![transition_id])?;
    version.set_canonicalization_policy_id(Some(EntityId::new()))?;
    version.set_domain_scope_id(Some(EntityId::new()))?;
    version.set_operational_defaults(OperationalOverrides {
        max_pages: Some(5),
        ..OperationalOverrides::default()
    })?;
    assert_eq!(version.seeds().len(), 1);
    assert_eq!(version.page_type_ids(), &[page_type_id]);
    assert_eq!(version.transition_ids(), &[transition_id]);
    assert_eq!(version.operational_defaults().max_pages, Some(5));
    Ok(())
}
