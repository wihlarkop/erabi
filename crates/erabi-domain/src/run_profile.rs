use crate::{LayerValue, RunProfileId};

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationalOverrides {
    pub max_pages: LayerValue<u64>,
    pub max_depth: LayerValue<u32>,
    pub max_duration_seconds: LayerValue<u64>,
    pub concurrency: LayerValue<u32>,
    pub request_delay_ms: LayerValue<u64>,
    pub timeout_ms: LayerValue<u64>,
    pub screenshot: LayerValue<bool>,
    pub asset_download_limit_bytes: LayerValue<u64>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RunProfile {
    id: RunProfileId,
    name: String,
    overrides: OperationalOverrides,
}
impl RunProfile {
    #[must_use]
    pub fn new(name: impl Into<String>, overrides: OperationalOverrides) -> Self {
        Self {
            id: RunProfileId::new(),
            name: name.into(),
            overrides,
        }
    }
    #[must_use]
    pub const fn id(&self) -> RunProfileId {
        self.id
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
