use erabi_domain::{PageType, PageTypeMatchDecision, UrlMatcher, resolve_page_type};
use std::collections::BTreeMap;

fn url() -> Result<url::Url, url::ParseError> {
    "https://example.test/products/42?locale=en".parse()
}
fn exact() -> Result<PageType, url::ParseError> {
    Ok(PageType::new(
        "Exact",
        0,
        vec![UrlMatcher::exact_url(url()?)],
    ))
}
fn template() -> PageType {
    PageType::new(
        "Template",
        0,
        vec![UrlMatcher::exact_host_path_template(
            "example.test",
            "/products/{id}",
            BTreeMap::from([("locale".into(), "en".into())]),
        )],
    )
}
fn prefix() -> PageType {
    PageType::new(
        "Prefix",
        0,
        vec![UrlMatcher::path_prefix(
            Some("example.test".into()),
            "/products",
        )],
    )
}

#[test]
fn resolver_prefers_canonical_matcher_kinds_in_order() -> Result<(), Box<dyn std::error::Error>> {
    let decision = resolve_page_type(&url()?, &[prefix(), template(), exact()?]);
    assert!(
        matches!(decision, PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "Exact")
    );
    Ok(())
}

#[test]
fn resolver_is_independent_of_page_type_and_matcher_order() -> Result<(), Box<dyn std::error::Error>>
{
    let page = PageType::new(
        "Product",
        0,
        vec![
            UrlMatcher::path_prefix(Some("example.test".into()), "/products"),
            UrlMatcher::exact_url(url()?),
        ],
    );
    let fallback = prefix();
    for types in [
        [page.clone(), fallback.clone()],
        [fallback.clone(), page.clone()],
    ] {
        assert!(
            matches!(resolve_page_type(&url()?, &types), PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "Product")
        );
    }
    Ok(())
}

#[test]
fn complete_specificity_tie_is_ambiguous() -> Result<(), Box<dyn std::error::Error>> {
    let a = PageType::new(
        "A",
        1,
        vec![UrlMatcher::path_prefix(
            Some("example.test".into()),
            "/products",
        )],
    );
    let b = PageType::new(
        "B",
        1,
        vec![UrlMatcher::path_prefix(
            Some("example.test".into()),
            "/products",
        )],
    );
    assert!(matches!(
        resolve_page_type(&url()?, &[b, a]),
        PageTypeMatchDecision::Ambiguous { .. }
    ));
    Ok(())
}

#[test]
fn unmatched_url_is_preserved_as_unmatched_decision() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        resolve_page_type(&"https://example.test/about".parse()?, &[prefix()]),
        PageTypeMatchDecision::Unmatched
    ));
    Ok(())
}

#[test]
fn invalid_matchers_are_rejected_at_construction_and_deserialization() {
    assert!(UrlMatcher::regex("[").is_err());
    assert!(UrlMatcher::path_glob(None, "").is_err());
    assert!(serde_json::from_str::<UrlMatcher>(r#"{"Regex":{"pattern":"["}}"#).is_err());
    assert!(
        serde_json::from_str::<UrlMatcher>(r#"{"PathGlob":{"host":null,"pattern":""}}"#).is_err()
    );
}

#[test]
fn valid_matcher_round_trips_through_serde() -> Result<(), Box<dyn std::error::Error>> {
    let matcher = UrlMatcher::path_glob(Some("example.test".into()), "/products/*")?;
    let restored: UrlMatcher = serde_json::from_str(&serde_json::to_string(&matcher)?)?;
    assert_eq!(restored.pattern(), "/products/*");
    Ok(())
}

#[test]
fn equal_best_matcher_evidence_is_order_independent() -> Result<(), Box<dyn std::error::Error>> {
    let a = UrlMatcher::exact_host_path_template("example.test", "/products/{id}", BTreeMap::new());
    let b = UrlMatcher::exact_host_path_template("example.test", "/products/{xx}", BTreeMap::new());
    let forward = PageType::new("Product", 0, vec![a.clone(), b.clone()]);
    let reverse = PageType {
        id: forward.id,
        name: forward.name.clone(),
        priority: forward.priority,
        matchers: vec![b, a],
    };
    let first = resolve_page_type(&url()?, &[forward]);
    let second = resolve_page_type(&url()?, &[reverse]);
    assert_eq!(first, second);
    match first {
        PageTypeMatchDecision::Matched(candidate) => assert_eq!(
            candidate.matched_patterns,
            vec![
                "example.test/products/{id}".to_owned(),
                "example.test/products/{xx}".to_owned(),
            ]
        ),
        _ => panic!("fixture must resolve to its single Page Type"),
    }
    Ok(())
}
