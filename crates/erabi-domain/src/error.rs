use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    SchemaDrift,
    AmbiguousPageType,
    UnresolvedReference,
    StorageCritical,
    CrawlerUnavailable,
    ValidationError,
    Conflict,
    NotFound,
    AccessDenied,
    CrawlerTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SuggestedAction {
    pub label: String,
    pub action: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProductError {
    pub code: ErrorCode,
    pub safe_message: String,
    pub details: Value,
    pub recoverable: bool,
    pub suggested_actions: Vec<SuggestedAction>,
    pub trace_id: String,
}

impl ProductError {
    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Conflict,
            safe_message: message.into(),
            details: Value::Null,
            recoverable: false,
            suggested_actions: Vec::new(),
            trace_id: String::new(),
        }
    }
}

impl std::fmt::Display for ProductError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.safe_message.fmt(formatter)
    }
}

impl std::error::Error for ProductError {}
