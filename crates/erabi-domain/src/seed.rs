use crate::{PageTypeId, SeedId};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Seed {
    pub id: SeedId,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub enabled: bool,
    pub label: Option<String>,
    pub entry_page_type_hint: Option<PageTypeId>,
}

impl Seed {
    #[must_use]
    pub fn new(original_url: url::Url, canonical_url: url::Url) -> Self {
        Self {
            id: SeedId::new(),
            original_url,
            canonical_url,
            enabled: true,
            label: None,
            entry_page_type_hint: None,
        }
    }
}
