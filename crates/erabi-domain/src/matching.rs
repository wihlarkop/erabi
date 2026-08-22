use crate::{EntityId, PageType, SpecificityKey, UrlMatcher, UrlMatcherKind};
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PageTypeCandidate {
    pub page_type_id: EntityId,
    pub page_type_name: String,
    pub priority: i32,
    pub matcher_kind: UrlMatcherKind,
    pub specificity: SpecificityKey,
    pub matched_patterns: Vec<String>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PageTypeMatchDecision {
    Matched(PageTypeCandidate),
    Ambiguous { candidates: Vec<PageTypeCandidate> },
    Unmatched,
}
#[must_use]
pub fn resolve_page_type(url: &url::Url, page_types: &[PageType]) -> PageTypeMatchDecision {
    let mut candidates: Vec<_> = page_types
        .iter()
        .filter_map(|page_type| {
            page_type
                .matchers
                .iter()
                .filter(|matcher| matcher.matches(url))
                .map(UrlMatcher::specificity)
                .max()
                .map(|specificity| {
                    let mut matched_patterns: Vec<_> = page_type
                        .matchers
                        .iter()
                        .filter(|matcher| {
                            matcher.matches(url) && matcher.specificity() == specificity
                        })
                        .map(UrlMatcher::pattern)
                        .collect();
                    matched_patterns.sort();
                    matched_patterns.dedup();
                    PageTypeCandidate {
                        page_type_id: page_type.id,
                        page_type_name: page_type.name.clone(),
                        priority: page_type.priority,
                        matcher_kind: page_type
                            .matchers
                            .iter()
                            .find(|matcher| {
                                matcher.matches(url) && matcher.specificity() == specificity
                            })
                            .map_or(UrlMatcherKind::Regex, UrlMatcher::kind),
                        specificity,
                        matched_patterns,
                    }
                })
        })
        .collect();
    if candidates.is_empty() {
        return PageTypeMatchDecision::Unmatched;
    }
    let best = candidates
        .iter()
        .map(|candidate| (candidate.priority, candidate.specificity))
        .max();
    candidates.retain(|candidate| Some((candidate.priority, candidate.specificity)) == best);
    candidates.sort_by(|left, right| {
        left.page_type_name
            .cmp(&right.page_type_name)
            .then(left.matched_patterns.cmp(&right.matched_patterns))
    });
    if candidates.len() == 1 {
        PageTypeMatchDecision::Matched(candidates.remove(0))
    } else {
        PageTypeMatchDecision::Ambiguous { candidates }
    }
}
