//! API-safe parsing for durable job identities.

use std::str::FromStr;

use uuid::Uuid;

use super::JobId;

/// A sanitized failure to parse a durable job identifier from an API boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobIdParseError {
    /// Erabi job identities are always canonical UUIDv7 values.
    #[error("job identifiers must be UUIDv7 values")]
    Invalid,
}

impl FromStr for JobId {
    type Err = JobIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = Uuid::parse_str(value).map_err(|_| JobIdParseError::Invalid)?;
        if parsed.get_version_num() != 7 {
            return Err(JobIdParseError::Invalid);
        }
        Ok(Self::from_stored(parsed.to_string()))
    }
}
