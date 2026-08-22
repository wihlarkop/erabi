use erabi_domain::{
    DiscoveryTransition, DiscoveryTransitionId, PageTypeId, Source, SourceTargetType,
    TransitionBudget, derive_source_name,
};
#[test]
fn budgeted_self_cycle_is_valid() {
    let page_type = PageTypeId::new();
    let transition = DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id: page_type,
        target_page_type_id: page_type,
        name: "pagination".into(),
        enabled: true,
        link_selector: "a[rel=next]".into(),
        url_constraints: None,
        priority: 0,
        budget: TransitionBudget {
            max_links_per_source_page: 25,
            total_budget: Some(100),
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    assert!(transition.validate().is_ok());
}
#[test]
fn source_is_independent_of_seed_configuration() -> Result<(), Box<dyn std::error::Error>> {
    let source = Source::new(
        "Product",
        "https://example.test/a".parse()?,
        "https://example.test/a".parse()?,
        SourceTargetType::WebPage,
    );
    assert_eq!(source.canonical_url().as_str(), "https://example.test/a");
    Ok(())
}
#[test]
fn names_are_deterministic() -> Result<(), Box<dyn std::error::Error>> {
    let url: url::Url = "https://example.test/products/42".parse()?;
    assert_eq!(
        derive_source_name(None, None, &url),
        "example.test/products/42"
    );
    Ok(())
}
