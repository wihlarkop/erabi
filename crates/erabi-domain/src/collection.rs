use crate::EntityId;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Collection {
    pub id: EntityId,
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
}
