//! Compact crawl recovery control carried by the generic checkpoint envelope.
//!
//! This module owns only immutable crawl identity and the recovery phase. The
//! durable crawl data plane owns URL work, execution history, and discovery
//! evidence; none of those structures are serialized into this checkpoint.

use erabi_db::repositories::{
    CHECKPOINT_ENVELOPE_FORMAT_VERSION, CheckpointEnvelope, CheckpointIdentity,
    CheckpointPayloadKind,
};
use erabi_domain::{CrawlRunId, CrawlRunSnapshot, CrawlRunType, CrawlerVersionId};

/// Current format of the crawl-specific recovery payload.
pub const CRAWL_RECOVERY_FORMAT_VERSION: u16 = 1;
/// Generic envelope payload kind owned by the crawler subsystem.
pub const CRAWL_RECOVERY_PAYLOAD_KIND: &str = "CRAWL_RECOVERY";
/// Maximum encoded size of one crawl-specific recovery payload.
pub const MAX_CRAWL_RECOVERY_PAYLOAD_BYTES: usize = 1_024;

/// Durable recovery-control phase for a crawl.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlRecoveryPhase {
    Initialized,
    Traversing,
    Finalizing,
}

/// Compact recovery control for one immutable crawl identity.
///
/// URL frontier, execution outcomes, discovery evidence, and extraction state
/// do not belong in this type. They remain in their authoritative durable
/// repositories and are reconstructed independently of checkpoint bytes.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CrawlRecoveryCheckpoint {
    pub format_version: u16,
    pub crawl_run_id: CrawlRunId,
    pub run_type: CrawlRunType,
    pub snapshot_hash: String,
    pub checkpoint_compatibility_hash: String,
    pub crawler_version_id: Option<CrawlerVersionId>,
    pub semantic_config_hash: Option<String>,
    pub recovery_phase: CrawlRecoveryPhase,
}

/// Failures while validating or converting crawler-owned recovery control.
#[derive(Debug, thiserror::Error)]
pub enum CrawlRecoveryCheckpointError {
    #[error("the crawl recovery format is unsupported")]
    UnsupportedFormatVersion,
    #[error("the checkpoint payload kind is not crawl recovery")]
    UnexpectedPayloadKind,
    #[error("the crawl recovery payload is malformed")]
    MalformedPayload,
    #[error("the crawl recovery identity is incompatible with the run")]
    IncompatibleRunIdentity,
    #[error("the run type is not supported by crawl recovery")]
    UnsupportedRunType,
    #[error("crawl recovery serialization failed")]
    Serialization,
}

impl CrawlRecoveryCheckpoint {
    /// Creates current recovery control validated against an immutable run
    /// snapshot.
    ///
    /// # Errors
    /// Returns an error when the run type is unsupported or the immutable
    /// crawler identity does not have the required shape.
    pub fn new(
        crawl_run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        recovery_phase: CrawlRecoveryPhase,
    ) -> Result<Self, CrawlRecoveryCheckpointError> {
        let (crawler_version_id, semantic_config_hash) = match snapshot.configuration() {
            erabi_domain::RunConfiguration::CrawlerVersion {
                crawler_version_id,
                semantic_config_hash,
                ..
            } => (
                Some(*crawler_version_id),
                Some(semantic_config_hash.clone()),
            ),
            erabi_domain::RunConfiguration::QuickScrape { .. } => (None, None),
        };
        let checkpoint = Self {
            format_version: CRAWL_RECOVERY_FORMAT_VERSION,
            crawl_run_id,
            run_type: snapshot.run_type(),
            snapshot_hash: snapshot.snapshot_hash().to_owned(),
            checkpoint_compatibility_hash: snapshot.checkpoint_compatibility_hash().to_owned(),
            crawler_version_id,
            semantic_config_hash,
            recovery_phase,
        };
        checkpoint.validate_against(snapshot)?;
        Ok(checkpoint)
    }

    /// Converts the recovery control to the current structured generic
    /// checkpoint envelope.
    ///
    /// # Errors
    /// Returns a serialization error when the control is invalid or cannot fit
    /// inside the bounded crawler payload.
    pub fn to_envelope(&self) -> Result<CheckpointEnvelope, CrawlRecoveryCheckpointError> {
        self.validate_shape()?;
        let payload =
            serde_json::to_value(self).map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        let payload_bytes = serde_json::to_vec(&payload)
            .map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        if payload_bytes.len() > MAX_CRAWL_RECOVERY_PAYLOAD_BYTES {
            return Err(CrawlRecoveryCheckpointError::Serialization);
        }
        let identity = CheckpointIdentity::new(
            self.crawl_run_id.to_string(),
            self.snapshot_hash.clone(),
            self.checkpoint_compatibility_hash.clone(),
        )
        .map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        let payload_kind = CheckpointPayloadKind::new(CRAWL_RECOVERY_PAYLOAD_KIND)
            .map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        let envelope = CheckpointEnvelope::new(identity, payload_kind, payload)
            .map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        envelope
            .encode()
            .map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        Ok(envelope)
    }

    /// Converts a current generic envelope into crawler-owned recovery control
    /// after validating its immutable identity against the supplied snapshot.
    ///
    /// # Errors
    /// Returns a typed error for wrong kinds, unsupported formats, malformed
    /// structured payloads, or incompatible run identity.
    pub fn from_envelope(
        envelope: &CheckpointEnvelope,
        snapshot: &CrawlRunSnapshot,
        crawl_run_id: CrawlRunId,
    ) -> Result<Self, CrawlRecoveryCheckpointError> {
        if envelope.format_version != CHECKPOINT_ENVELOPE_FORMAT_VERSION {
            return Err(CrawlRecoveryCheckpointError::UnsupportedFormatVersion);
        }
        if envelope.payload_kind.as_str() != CRAWL_RECOVERY_PAYLOAD_KIND {
            return Err(CrawlRecoveryCheckpointError::UnexpectedPayloadKind);
        }
        let payload = envelope
            .payload
            .as_object()
            .ok_or(CrawlRecoveryCheckpointError::MalformedPayload)?;
        if payload.contains_key("payload_version") {
            return Err(CrawlRecoveryCheckpointError::MalformedPayload);
        }
        let format_version = payload
            .get("format_version")
            .ok_or(CrawlRecoveryCheckpointError::MalformedPayload)?;
        if format_version.is_number()
            && format_version.as_u64() != Some(u64::from(CRAWL_RECOVERY_FORMAT_VERSION))
        {
            return Err(CrawlRecoveryCheckpointError::UnsupportedFormatVersion);
        }
        for required in [
            "crawl_run_id",
            "run_type",
            "snapshot_hash",
            "checkpoint_compatibility_hash",
            "crawler_version_id",
            "semantic_config_hash",
            "recovery_phase",
        ] {
            if !payload.contains_key(required) {
                return Err(CrawlRecoveryCheckpointError::MalformedPayload);
            }
        }
        let payload_bytes = serde_json::to_vec(&envelope.payload)
            .map_err(|_| CrawlRecoveryCheckpointError::Serialization)?;
        if payload_bytes.len() > MAX_CRAWL_RECOVERY_PAYLOAD_BYTES {
            return Err(CrawlRecoveryCheckpointError::MalformedPayload);
        }
        let checkpoint: Self = serde_json::from_value(envelope.payload.clone())
            .map_err(|_| CrawlRecoveryCheckpointError::MalformedPayload)?;
        checkpoint.validate_against(snapshot)?;
        if checkpoint.crawl_run_id != crawl_run_id
            || envelope.identity.snapshot_id != crawl_run_id.to_string()
            || envelope.identity.snapshot_hash != checkpoint.snapshot_hash
            || envelope.identity.compatibility_hash != checkpoint.checkpoint_compatibility_hash
        {
            return Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity);
        }
        Ok(checkpoint)
    }

    fn validate_against(
        &self,
        snapshot: &CrawlRunSnapshot,
    ) -> Result<(), CrawlRecoveryCheckpointError> {
        if !is_supported_run_type(snapshot.run_type()) {
            return Err(CrawlRecoveryCheckpointError::UnsupportedRunType);
        }
        self.validate_shape()?;
        if self.run_type != snapshot.run_type()
            || self.snapshot_hash != snapshot.snapshot_hash()
            || self.checkpoint_compatibility_hash != snapshot.checkpoint_compatibility_hash()
        {
            return Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity);
        }
        match (
            snapshot.configuration(),
            self.crawler_version_id,
            self.semantic_config_hash.as_deref(),
        ) {
            (
                erabi_domain::RunConfiguration::CrawlerVersion {
                    crawler_version_id,
                    semantic_config_hash,
                    ..
                },
                Some(actual_crawler_version_id),
                Some(actual_semantic_config_hash),
            ) if *crawler_version_id == actual_crawler_version_id
                && semantic_config_hash == actual_semantic_config_hash =>
            {
                Ok(())
            }
            (erabi_domain::RunConfiguration::QuickScrape { .. }, None, None) => Ok(()),
            _ => Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity),
        }
    }

    fn validate_shape(&self) -> Result<(), CrawlRecoveryCheckpointError> {
        if self.format_version != CRAWL_RECOVERY_FORMAT_VERSION {
            return Err(CrawlRecoveryCheckpointError::UnsupportedFormatVersion);
        }
        if !is_supported_run_type(self.run_type) {
            return Err(CrawlRecoveryCheckpointError::UnsupportedRunType);
        }
        if !valid_hash(&self.snapshot_hash) || !valid_hash(&self.checkpoint_compatibility_hash) {
            return Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity);
        }
        match self.run_type {
            CrawlRunType::ProductionRun => {
                if self.crawler_version_id.is_none()
                    || self
                        .semantic_config_hash
                        .as_deref()
                        .is_none_or(|hash| !valid_hash(hash))
                {
                    return Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity);
                }
            }
            CrawlRunType::QuickScrape => {
                if self.crawler_version_id.is_some() || self.semantic_config_hash.is_some() {
                    return Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity);
                }
            }
            CrawlRunType::TestRun | CrawlRunType::DiscoveryPreview => {
                return Err(CrawlRecoveryCheckpointError::UnsupportedRunType);
            }
        }
        Ok(())
    }
}

fn is_supported_run_type(run_type: CrawlRunType) -> bool {
    matches!(
        run_type,
        CrawlRunType::ProductionRun | CrawlRunType::QuickScrape
    )
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use erabi_domain::{
        CrawlRunSnapshotDraft, ResolvedValue, RobotsAudit, RunConfiguration, SettingSource,
        SnapshotOperationalSettings,
    };

    use super::*;

    fn value<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }

    fn quick_snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
        Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: "https://example.test/root".parse()?,
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: value(1),
                max_depth: value(0),
                max_duration_seconds: value(60),
                concurrency: value(1),
                request_delay_ms: value(0),
                timeout_ms: value(1_000),
                screenshot: value(false),
                asset_download_limit_bytes: value(1),
                retain_artifacts: value(false),
                user_agent: value("Erabi/0.1".to_owned()),
            },
            robots: RobotsAudit::respect(
                "operator",
                "unix:1",
                "https://example.test",
                "Erabi/0.1",
                None,
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?)
    }

    fn production_snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
        let crawler_version_id = CrawlerVersionId::new();
        Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::ProductionRun,
            configuration: RunConfiguration::CrawlerVersion {
                crawler_id: erabi_domain::CrawlerId::new(),
                crawler_version_id,
                semantic_config_hash: "c".repeat(64),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: quick_snapshot()?.settings().clone(),
            robots: RobotsAudit::respect(
                "operator",
                "unix:1",
                "https://example.test",
                "Erabi/0.1",
                Some(crawler_version_id),
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?)
    }

    fn envelope_for(
        checkpoint: &CrawlRecoveryCheckpoint,
    ) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
        Ok(checkpoint.to_envelope()?)
    }

    #[test]
    fn production_recovery_round_trips_through_structured_envelope()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = production_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;
        let envelope = envelope_for(&checkpoint)?;
        assert_eq!(envelope.payload_kind.as_str(), CRAWL_RECOVERY_PAYLOAD_KIND);
        assert!(envelope.payload.is_object());
        assert_eq!(
            CrawlRecoveryCheckpoint::from_envelope(&envelope, &snapshot, run_id)?,
            checkpoint
        );
        Ok(())
    }

    #[test]
    fn quick_scrape_recovery_round_trips_with_absent_crawler_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Initialized)?;
        assert_eq!(checkpoint.crawler_version_id, None);
        assert_eq!(checkpoint.semantic_config_hash, None);
        let envelope = envelope_for(&checkpoint)?;
        assert_eq!(
            CrawlRecoveryCheckpoint::from_envelope(&envelope, &snapshot, run_id)?,
            checkpoint
        );
        Ok(())
    }

    #[test]
    fn wrong_payload_kind_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;
        let mut envelope = envelope_for(&checkpoint)?;
        envelope.payload_kind = CheckpointPayloadKind::new("OTHER_KIND")?;
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&envelope, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::UnexpectedPayloadKind)
        ));
        Ok(())
    }

    #[test]
    fn unsupported_recovery_format_is_distinct_from_malformed_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;
        let mut unsupported = envelope_for(&checkpoint)?;
        unsupported.payload["format_version"] = serde_json::json!(2);
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&unsupported, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::UnsupportedFormatVersion)
        ));

        let mut malformed = envelope_for(&checkpoint)?;
        malformed.payload = serde_json::json!({"format_version": 1});
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&malformed, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::MalformedPayload)
        ));
        Ok(())
    }

    #[test]
    fn historical_payload_version_is_not_current_recovery_format()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;
        let mut envelope = envelope_for(&checkpoint)?;
        envelope.payload["payload_version"] = serde_json::json!(2);
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&envelope, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::MalformedPayload)
        ));
        Ok(())
    }

    #[test]
    fn identity_mismatches_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = production_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;

        let mut run_mismatch = envelope_for(&checkpoint)?;
        run_mismatch.payload["crawl_run_id"] = serde_json::json!(CrawlRunId::new());
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&run_mismatch, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity)
        ));

        let mut snapshot_mismatch = envelope_for(&checkpoint)?;
        snapshot_mismatch.payload["snapshot_hash"] = serde_json::json!("d".repeat(64));
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&snapshot_mismatch, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity)
        ));

        let mut compatibility_mismatch = envelope_for(&checkpoint)?;
        compatibility_mismatch.payload["checkpoint_compatibility_hash"] =
            serde_json::json!("e".repeat(64));
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&compatibility_mismatch, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity)
        ));

        let mut crawler_mismatch = envelope_for(&checkpoint)?;
        crawler_mismatch.payload["crawler_version_id"] = serde_json::json!(CrawlerVersionId::new());
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&crawler_mismatch, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity)
        ));

        let mut semantic_mismatch = envelope_for(&checkpoint)?;
        semantic_mismatch.payload["semantic_config_hash"] = serde_json::json!("f".repeat(64));
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&semantic_mismatch, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity)
        ));
        Ok(())
    }

    #[test]
    fn quick_scrape_rejects_crawler_identity_fields() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;
        let mut envelope = envelope_for(&checkpoint)?;
        envelope.payload["crawler_version_id"] = serde_json::json!(CrawlerVersionId::new());
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&envelope, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::IncompatibleRunIdentity)
        ));
        Ok(())
    }

    #[test]
    fn test_run_type_is_not_supported() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let run_id = CrawlRunId::new();
        let checkpoint =
            CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)?;
        let mut envelope = envelope_for(&checkpoint)?;
        envelope.payload["run_type"] = serde_json::json!("TEST_RUN");
        assert!(matches!(
            CrawlRecoveryCheckpoint::from_envelope(&envelope, &snapshot, run_id),
            Err(CrawlRecoveryCheckpointError::UnsupportedRunType)
        ));
        Ok(())
    }

    #[test]
    fn recovery_payload_is_compact_and_does_not_contain_frontier_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let checkpoint = CrawlRecoveryCheckpoint::new(
            CrawlRunId::new(),
            &snapshot,
            CrawlRecoveryPhase::Finalizing,
        )?;
        let envelope = envelope_for(&checkpoint)?;
        let encoded = serde_json::to_vec(&envelope.payload)?;
        assert!(encoded.len() <= MAX_CRAWL_RECOVERY_PAYLOAD_BYTES);
        assert!(!envelope.payload.to_string().contains("pending_units"));
        assert!(!envelope.payload.to_string().contains("artifact_references"));
        Ok(())
    }
}
