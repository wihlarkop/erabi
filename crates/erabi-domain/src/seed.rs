use crate::EntityId;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Seed {
    pub id: EntityId,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub enabled: bool,
    pub label: Option<String>,
    pub entry_page_type_hint: Option<EntityId>,
}

impl Seed {
    #[must_use]
    pub fn new(original_url: url::Url, canonical_url: url::Url) -> Self {
        Self {
            id: EntityId::new(),
            original_url,
            canonical_url,
            enabled: true,
            label: None,
            entry_page_type_hint: None,
        }
    }
}
