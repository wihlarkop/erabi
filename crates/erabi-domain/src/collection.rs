use crate::CollectionId;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Collection {
    pub id: CollectionId,
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
}
