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
    assert!(UrlMatcher::path_glob(Some("bad host".into()), "/products/*").is_err());
    assert!(UrlMatcher::path_glob(None, "products/*").is_err());
    assert!(UrlMatcher::try_path_prefix(None, "products").is_err());
    assert!(UrlMatcher::try_path_prefix(Some("bad host".into()), "/products").is_err());
    assert!(
        UrlMatcher::try_exact_host_path_template("bad host", "/products/{id}", BTreeMap::new(),)
            .is_err()
    );
    assert!(
        UrlMatcher::try_exact_host_path_template("example.test", "/products/{id", BTreeMap::new(),)
            .is_err()
    );
    assert!(
        UrlMatcher::try_exact_host_path_template(
            "example.test",
            "/products/{id}",
            BTreeMap::from([("bad key".into(), "en".into())]),
        )
        .is_err()
    );
    assert!(serde_json::from_str::<UrlMatcher>(r#"{"Regex":{"pattern":"["}}"#).is_err());
    assert!(
        serde_json::from_str::<UrlMatcher>(r#"{"PathGlob":{"host":null,"pattern":""}}"#).is_err()
    );
    assert!(
        serde_json::from_str::<UrlMatcher>(r#"{"PathPrefix":{"host":null,"prefix":"products"}}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<UrlMatcher>(
        r#"{"ExactHostPathTemplate":{"host":"bad host","path_template":"/products/{id}","query":{}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<UrlMatcher>(
        r#"{"ExactHostPathTemplate":{"host":"example.test","path_template":"/products/{id","query":{}}}"#
    )
    .is_err());
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

#[test]
#[allow(clippy::too_many_lines)]
fn specificity_components_are_compared_in_the_frozen_order()
-> Result<(), Box<dyn std::error::Error>> {
    let exact_host = PageType::new(
        "Template",
        0,
        vec![UrlMatcher::exact_host_path_template(
            "example.test",
            "/products/{id}/reviews",
            BTreeMap::from([("locale".into(), "en".into())]),
        )],
    );
    let prefix = PageType::new(
        "Prefix",
        0,
        vec![UrlMatcher::path_prefix(
            Some("example.test".into()),
            "/products",
        )],
    );
    let regex = PageType::new("Regex", 0, vec![UrlMatcher::regex("products")?]);
    let decision = resolve_page_type(
        &"https://example.test/products/42/reviews?locale=en".parse()?,
        &[regex, prefix, exact_host],
    );
    assert!(matches!(
        decision,
        PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "Template"
    ));

    let high_priority_prefix = PageType::new(
        "High priority",
        10,
        vec![UrlMatcher::path_prefix(
            Some("example.test".into()),
            "/products",
        )],
    );
    let low_priority_exact = PageType::new(
        "Low priority",
        0,
        vec![UrlMatcher::exact_url(
            "https://example.test/products/42/reviews?locale=en".parse()?,
        )],
    );
    assert!(matches!(
        resolve_page_type(
            &"https://example.test/products/42/reviews?locale=en".parse()?,
            &[low_priority_exact, high_priority_prefix],
        ),
        PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "High priority"
    ));

    let more_literals = PageType::new(
        "More literals",
        0,
        vec![UrlMatcher::exact_host_path_template(
            "example.test",
            "/products/{id}/reviews",
            BTreeMap::new(),
        )],
    );
    let fewer_literals = PageType::new(
        "Fewer literals",
        0,
        vec![UrlMatcher::exact_host_path_template(
            "example.test",
            "/products/{id}/{section}",
            BTreeMap::new(),
        )],
    );
    assert!(matches!(
        resolve_page_type(
            &"https://example.test/products/42/reviews".parse()?,
            &[fewer_literals, more_literals],
        ),
        PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "More literals"
    ));

    let more_query = PageType::new(
        "More query",
        0,
        vec![UrlMatcher::exact_host_path_template(
            "example.test",
            "/products/{id}",
            BTreeMap::from([
                ("locale".into(), "en".into()),
                ("view".into(), "full".into()),
            ]),
        )],
    );
    let less_query = PageType::new(
        "Less query",
        0,
        vec![UrlMatcher::exact_host_path_template(
            "example.test",
            "/products/{id}",
            BTreeMap::from([("locale".into(), "en".into())]),
        )],
    );
    assert!(matches!(
        resolve_page_type(
            &"https://example.test/products/42?locale=en&view=full".parse()?,
            &[less_query, more_query],
        ),
        PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "More query"
    ));

    let more_literals_regex = PageType::new(
        "More regex literals",
        0,
        vec![UrlMatcher::regex(r"/products/[0-9]+")?],
    );
    let fewer_literals_regex = PageType::new(
        "Fewer regex literals",
        0,
        vec![UrlMatcher::regex(r"/products/.+")?],
    );
    assert!(matches!(
        resolve_page_type(
            &"https://example.test/products/42".parse()?,
            &[fewer_literals_regex, more_literals_regex],
        ),
        PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "More regex literals"
    ));

    let fewer_wildcards = PageType::new(
        "Fewer wildcards",
        0,
        vec![UrlMatcher::path_glob(None, "/products/*")?],
    );
    let more_wildcards = PageType::new(
        "More wildcards",
        0,
        vec![UrlMatcher::path_glob(None, "/products/**")?],
    );
    assert!(matches!(
        resolve_page_type(
            &"https://example.test/products/42".parse()?,
            &[more_wildcards, fewer_wildcards],
        ),
        PageTypeMatchDecision::Matched(candidate) if candidate.page_type_name == "Fewer wildcards"
    ));
    Ok(())
}

#[test]
fn reverse_page_type_order_preserves_winner_and_all_ties() -> Result<(), Box<dyn std::error::Error>>
{
    let a = PageType::new(
        "Same name",
        5,
        vec![UrlMatcher::path_prefix(
            Some("example.test".into()),
            "/same",
        )],
    );
    let b = PageType {
        id: erabi_domain::PageTypeId::new(),
        name: a.name.clone(),
        priority: a.priority,
        matchers: a.matchers.clone(),
    };
    let forward = resolve_page_type(
        &"https://example.test/same/page".parse()?,
        &[a.clone(), b.clone()],
    );
    let reverse = resolve_page_type(&"https://example.test/same/page".parse()?, &[b, a]);
    assert!(
        matches!(forward, PageTypeMatchDecision::Ambiguous { ref candidates } if candidates.len() == 2)
    );
    assert!(
        matches!(reverse, PageTypeMatchDecision::Ambiguous { ref candidates } if candidates.len() == 2)
    );
    assert_eq!(
        match forward {
            PageTypeMatchDecision::Ambiguous { candidates } => candidates
                .into_iter()
                .map(|candidate| candidate.page_type_id.to_string())
                .collect::<std::collections::BTreeSet<_>>(),
            _ => unreachable!(),
        },
        match reverse {
            PageTypeMatchDecision::Ambiguous { candidates } => candidates
                .into_iter()
                .map(|candidate| candidate.page_type_id.to_string())
                .collect::<std::collections::BTreeSet<_>>(),
            _ => unreachable!(),
        }
    );
    Ok(())
}
