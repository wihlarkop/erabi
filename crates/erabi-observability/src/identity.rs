use std::fmt;

use uuid::Uuid;

const UUID_TEXT_LENGTH: usize = 36;

/// An opaque `UUIDv7` identity reserved for durable internal telemetry
/// correlation.
///
/// The type deliberately has no `Display` or `Debug` implementation. Callers
/// can only obtain one through a `UUIDv7` generator or the bounded canonical
/// parser, so arbitrary text cannot become a durable telemetry identity.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TelemetryId(Uuid);

/// Bounded validation failure for a telemetry identity.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum TelemetryIdError {
    Invalid,
}

impl fmt::Debug for TelemetryIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TelemetryIdError::Invalid")
    }
}

impl fmt::Display for TelemetryIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("telemetry identity is invalid")
    }
}

impl std::error::Error for TelemetryIdError {}

impl TelemetryId {
    /// Generates a fresh canonical `UUIDv7` telemetry identity.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses only canonical lowercase hyphenated `UUIDv7` text.
    ///
    /// The error intentionally contains neither the rejected value nor any
    /// parser detail derived from it.
    ///
    /// # Errors
    ///
    /// Returns [`TelemetryIdError::Invalid`] for non-canonical, non-`UUIDv7`,
    /// or otherwise malformed input.
    pub fn parse(value: &str) -> Result<Self, TelemetryIdError> {
        if value.len() != UUID_TEXT_LENGTH {
            return Err(TelemetryIdError::Invalid);
        }

        let uuid = Uuid::parse_str(value).map_err(|_| TelemetryIdError::Invalid)?;
        if uuid.get_version_num() != 7 || uuid.hyphenated().to_string() != value {
            return Err(TelemetryIdError::Invalid);
        }
        Ok(Self(uuid))
    }

    pub(crate) fn as_string(&self) -> String {
        // Formatting is bounded to the canonical UUID representation. The
        // string is only used by the internal tracing renderer; callers cannot
        // obtain arbitrary identity text from this type.
        self.0.to_string()
    }
}

/// The request-header correlation identity. It is intentionally separate from
/// [`TelemetryId`] because the existing HTTP contract permits bounded safe
/// ASCII values that are not UUIDs.
#[derive(Clone, Eq, PartialEq)]
pub struct RequestTraceId(String);

impl RequestTraceId {
    /// Accepts an existing safe request trace value or generates a fresh
    /// `UUIDv7` value when the incoming value is absent or unsafe.
    #[must_use]
    pub fn from_incoming(value: Option<&str>) -> Self {
        value.filter(|value| is_safe_trace_id(value)).map_or_else(
            || Self(Uuid::now_v7().to_string()),
            |value| Self(value.to_owned()),
        )
    }

    /// Returns the value safe for the response header and API envelope.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_safe_trace_id(value: &str) -> bool {
    (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_id_accepts_canonical_lowercase_uuid_v7() {
        let value = "018f0f0f-0f0f-7f0f-8f0f-0f0f0f0f0f0f";
        assert!(TelemetryId::parse(value).is_ok());
    }

    #[test]
    fn telemetry_id_rejects_noncanonical_or_non_v7_values() {
        for value in [
            "not-a-uuid",
            "018f0f0f-0f0f-7F0f-8f0f-0f0f0f0f0f0f",
            "018f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f",
            "018f0f0f-0f0f-7f0f-8f0f-0f0f0f0f0f0f-extra",
        ] {
            assert!(matches!(
                TelemetryId::parse(value),
                Err(TelemetryIdError::Invalid)
            ));
        }
    }

    #[test]
    fn telemetry_id_rejection_does_not_echo_input() {
        let rejected = "DO_NOT_LOG_THIS_TELEMETRY_ID";
        let Err(error) = TelemetryId::parse(rejected) else {
            panic!("the sentinel must be rejected");
        };
        assert_eq!(error, TelemetryIdError::Invalid);
        assert!(!error.to_string().contains(rejected));
        assert!(!format!("{error:?}").contains(rejected));
    }

    #[test]
    fn request_trace_id_preserves_the_existing_safe_ascii_contract() {
        let incoming = "trace-id_0001.ok";
        assert_eq!(
            RequestTraceId::from_incoming(Some(incoming)).as_str(),
            incoming
        );

        let fresh = RequestTraceId::from_incoming(Some("unsafe trace id"));
        assert_ne!(fresh.as_str(), "unsafe trace id");
        assert_eq!(fresh.as_str().len(), UUID_TEXT_LENGTH);
    }
}
