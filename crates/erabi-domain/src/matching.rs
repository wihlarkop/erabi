use crate::{EntityId, PageType, SpecificityKey, UrlMatcherKind};
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PageTypeCandidate {
    pub page_type_id: EntityId,
    pub page_type_name: String,
    pub priority: i32,
    pub matcher_kind: UrlMatcherKind,
    pub specificity: SpecificityKey,
    pub matched_pattern: String,
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
                .map(|matcher| PageTypeCandidate {
                    page_type_id: page_type.id,
                    page_type_name: page_type.name.clone(),
                    priority: page_type.priority,
                    matcher_kind: matcher.kind(),
                    specificity: matcher.specificity(),
                    matched_pattern: matcher.pattern(),
                })
                .max_by_key(|candidate| candidate.specificity)
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
            .then(left.matched_pattern.cmp(&right.matched_pattern))
    });
    if candidates.len() == 1 {
        PageTypeMatchDecision::Matched(candidates.remove(0))
    } else {
        PageTypeMatchDecision::Ambiguous { candidates }
    }
}
