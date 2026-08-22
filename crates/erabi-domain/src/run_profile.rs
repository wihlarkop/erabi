use crate::EntityId;
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct OperationalOverrides {
    pub max_pages: Option<u64>,
    pub max_depth: Option<u32>,
    pub max_duration_seconds: Option<u64>,
    pub concurrency: Option<u32>,
    pub request_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub screenshot: Option<bool>,
    pub asset_download_limit_bytes: Option<u64>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RunProfile {
    id: EntityId,
    name: String,
    overrides: OperationalOverrides,
}
impl RunProfile {
    #[must_use]
    pub fn new(name: impl Into<String>, overrides: OperationalOverrides) -> Self {
        Self {
            id: EntityId::new(),
            name: name.into(),
            overrides,
        }
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub const fn overrides(&self) -> &OperationalOverrides {
        &self.overrides
    }
}
