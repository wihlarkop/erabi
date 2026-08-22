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
