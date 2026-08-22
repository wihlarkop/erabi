use crate::{PageTypeId, UrlMatcher};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PageType {
    pub id: PageTypeId,
    pub name: String,
    pub priority: i32,
    pub matchers: Vec<UrlMatcher>,
}
impl PageType {
    #[must_use]
    pub fn new(name: impl Into<String>, priority: i32, matchers: Vec<UrlMatcher>) -> Self {
        Self {
            id: PageTypeId::new(),
            name: name.into(),
            priority,
            matchers,
        }
    }
}
