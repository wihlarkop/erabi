use erabi_domain::{
    Crawler, CrawlerVersion, CrawlerVersionState, OperationalOverrides, RunProfile, Seed,
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
    assert_eq!(version.state, CrawlerVersionState::Published);
    Ok(())
}

#[test]
fn published_version_clones_to_distinct_editable_draft() -> Result<(), Box<dyn std::error::Error>> {
    let published = CrawlerVersion::fixture_published();
    let mut draft = published.draft_from_published();
    assert_ne!(draft.id, published.id);
    assert_eq!(draft.state, CrawlerVersionState::Draft);
    draft.add_seed(seed()?)?;
    assert!(published.seeds.is_empty());
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
    assert_eq!(crawler.active_published_version_id, Some(published.id));
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
