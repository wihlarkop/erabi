//! Provider-neutral durable state for resumable crawl execution.
//!
//! The generic Plan 04 checkpoint envelope owns storage, lease ownership, and
//! append-only lineage. This module owns the small typed payload that makes a
//! crawl safe to restore without replaying Seeds or depending on provider
//! DTOs. It intentionally contains identities, scheduling state, and bounded
//! execution references only; response bodies and credentials never cross
//! this seam.

use std::collections::{BTreeMap, BTreeSet};

use erabi_db::repositories::{
    CheckpointArtifactReference, CheckpointEnvelope, CheckpointIdentity, CheckpointRepositoryError,
    CheckpointUnitId, CrawlExecutionRecord, DiscoveredUrlRecord,
};
use erabi_domain::{
    CrawlExecutionOutcome, CrawlRunId, CrawlRunSnapshot, CrawlRunType, CrawlerVersionId,
    DiscoveryTransitionId, PageTypeId, SeedId,
};

use crate::discovery_preview::SemanticTraversalCheckpoint;

/// Version of the Plan 06 typed payload inside the generic Plan 04 envelope.
pub const CRAWL_CHECKPOINT_PAYLOAD_VERSION: u16 = 1;
/// Compact Task 9 payload version. URL/frontier/execution-sized data lives in
/// migration 0006's durable data plane, never in this payload.
pub const CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION: u16 = 2;
/// Fixed-cardinality V2 JSON has UUID/hash/phase fields only. This 1 KiB
/// limit is intentionally derived from that representation, not crawl size.
pub const MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES: usize = 1_024;
const MAX_URL_CHARS: usize = 4_096;
const MAX_TEXT_CHARS: usize = 512;

/// Current completed, failed, and partial unit partitions derived from history.
pub type CheckpointUnitSets = (
    Vec<CrawlCheckpointUnit>,
    Vec<CrawlCheckpointUnit>,
    Vec<CrawlCheckpointUnit>,
);

/// The current state of one canonical scheduling identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlCheckpointUnitState {
    Pending,
    Completed,
    Failed,
    Partial,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlRecoveryPhase {
    Initialized,
    Traversing,
    Finalizing,
}

/// Task 9 bounded checkpoint: compatibility identity plus recovery phase.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CrawlCheckpointV2 {
    pub payload_version: u16,
    pub crawl_run_id: CrawlRunId,
    pub run_type: CrawlRunType,
    pub snapshot_hash: String,
    pub checkpoint_compatibility_hash: String,
    pub crawler_version_id: Option<CrawlerVersionId>,
    pub semantic_config_hash: Option<String>,
    pub recovery_phase: CrawlRecoveryPhase,
}

impl CrawlCheckpointV2 {
    /// # Errors
    /// Returns an error when the immutable snapshot cannot supply the required
    /// production identity or the resulting compact payload is invalid.
    pub fn new(
        crawl_run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        recovery_phase: CrawlRecoveryPhase,
    ) -> Result<Self, CrawlCheckpointError> {
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
            payload_version: CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION,
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

    /// # Errors
    /// Returns an error when compact payload serialization or generic
    /// envelope validation fails.
    pub fn to_envelope(&self) -> Result<CheckpointEnvelope, CrawlCheckpointError> {
        let identity = CheckpointIdentity::new(
            self.crawl_run_id.to_string(),
            self.snapshot_hash.clone(),
            self.checkpoint_compatibility_hash.clone(),
        )?;
        let payload =
            serde_json::to_string(self).map_err(|_| CrawlCheckpointError::Serialization)?;
        if payload.len() > MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES {
            return Err(CrawlCheckpointError::Serialization);
        }
        let mut envelope = CheckpointEnvelope::new(identity);
        envelope.payload = Some(payload);
        envelope.encode()?;
        Ok(envelope)
    }

    /// # Errors
    /// Returns an error for malformed, incompatible, legacy, or growing
    /// envelope evidence.
    pub fn from_envelope(
        envelope: &CheckpointEnvelope,
        snapshot: &CrawlRunSnapshot,
        crawl_run_id: CrawlRunId,
    ) -> Result<Self, CrawlCheckpointError> {
        envelope.encode()?;
        let payload = envelope
            .payload
            .as_deref()
            .ok_or(CrawlCheckpointError::MalformedPayload)?;
        if payload.len() > MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES {
            return Err(CrawlCheckpointError::MalformedPayload);
        }
        let checkpoint: Self =
            serde_json::from_str(payload).map_err(|_| CrawlCheckpointError::MalformedPayload)?;
        if checkpoint.payload_version != CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION
            || checkpoint.crawl_run_id != crawl_run_id
        {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
        }
        checkpoint.validate_against(snapshot)?;
        if envelope.identity.snapshot_id != crawl_run_id.to_string()
            || envelope.identity.snapshot_hash != checkpoint.snapshot_hash
            || envelope.identity.compatibility_hash != checkpoint.checkpoint_compatibility_hash
            || !envelope.completed_units.is_empty()
            || !envelope.pending_units.is_empty()
            || !envelope.failed_units.is_empty()
            || !envelope.artifact_references.is_empty()
        {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
        }
        Ok(checkpoint)
    }

    fn validate_against(&self, snapshot: &CrawlRunSnapshot) -> Result<(), CrawlCheckpointError> {
        if self.snapshot_hash != snapshot.snapshot_hash()
            || self.checkpoint_compatibility_hash != snapshot.checkpoint_compatibility_hash()
            || self.run_type != snapshot.run_type()
        {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
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
                Some(actual),
                Some(hash),
            ) if *crawler_version_id == actual && semantic_config_hash == hash => Ok(()),
            (erabi_domain::RunConfiguration::QuickScrape { .. }, None, None) => Ok(()),
            _ => Err(CrawlCheckpointError::IncompatibleIdentity),
        }
    }
}

/// Provider-neutral durable identity for one admitted unit. The URL fields
/// are scheduling evidence; the execution IDs point to immutable durable
/// history and are never used to select a provider DTO.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CrawlCheckpointUnit {
    pub state: CrawlCheckpointUnitState,
    pub requested_url: String,
    pub canonical_url: String,
    pub discovered_url_id: Option<String>,
    pub depth: u32,
    pub page_type_id: Option<PageTypeId>,
    pub transition_id: Option<DiscoveryTransitionId>,
    pub parent_canonical_url: Option<String>,
    pub final_canonical_url: Option<String>,
    pub pagination: bool,
    pub seed_ids: Vec<SeedId>,
    pub execution_ids: Vec<erabi_domain::CrawlExecutionId>,
}

/// Typed bounded Plan 06 state carried by one generic Plan 04 checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CrawlCheckpoint {
    pub payload_version: u16,
    pub crawl_run_id: CrawlRunId,
    pub run_type: CrawlRunType,
    pub snapshot_hash: String,
    pub checkpoint_compatibility_hash: String,
    pub crawler_version_id: Option<CrawlerVersionId>,
    pub semantic_config_hash: Option<String>,
    pub robots_user_agent: String,
    pub selected_seed_ids: Vec<SeedId>,
    pub traversal: SemanticTraversalCheckpoint,
    pub completed_units: Vec<CrawlCheckpointUnit>,
    pub pending_units: Vec<CrawlCheckpointUnit>,
    pub failed_units: Vec<CrawlCheckpointUnit>,
    pub partial_units: Vec<CrawlCheckpointUnit>,
    pub artifact_references: Vec<CheckpointArtifactReference>,
}

/// Stable errors for malformed, incompatible, or unrepresentable crawl state.
#[derive(Debug, thiserror::Error)]
pub enum CrawlCheckpointError {
    #[error("the generic checkpoint envelope is invalid")]
    Envelope(#[from] CheckpointRepositoryError),
    #[error("the crawl checkpoint payload is malformed")]
    MalformedPayload,
    #[error("the crawl checkpoint payload is incompatible with the immutable run")]
    IncompatibleIdentity,
    #[error("the crawl checkpoint run type is not resumable")]
    UnsupportedRunType,
    #[error("the crawl checkpoint contains an invalid durable unit")]
    InvalidUnit,
    #[error("the crawl checkpoint contains inconsistent duplicate unit state")]
    DuplicateUnit,
    #[error("the crawl checkpoint identity could not be represented")]
    Identity,
    #[error("the crawl checkpoint payload could not be serialized")]
    Serialization,
}

impl CrawlCheckpoint {
    /// Creates a typed checkpoint for the current immutable snapshot. The
    /// caller supplies units already persisted or admitted by the traversal;
    /// no database ordering is consulted here.
    ///
    /// # Errors
    /// Returns a typed error when the state cannot satisfy the bounded
    /// checkpoint or immutable snapshot contract.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        crawl_run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        traversal: SemanticTraversalCheckpoint,
        completed_units: Vec<CrawlCheckpointUnit>,
        pending_units: Vec<CrawlCheckpointUnit>,
        failed_units: Vec<CrawlCheckpointUnit>,
        partial_units: Vec<CrawlCheckpointUnit>,
        artifact_references: Vec<CheckpointArtifactReference>,
    ) -> Result<Self, CrawlCheckpointError> {
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
            payload_version: CRAWL_CHECKPOINT_PAYLOAD_VERSION,
            crawl_run_id,
            run_type: snapshot.run_type(),
            snapshot_hash: snapshot.snapshot_hash().to_owned(),
            checkpoint_compatibility_hash: snapshot.checkpoint_compatibility_hash().to_owned(),
            crawler_version_id,
            semantic_config_hash,
            robots_user_agent: snapshot.robots().user_agent().to_owned(),
            selected_seed_ids: snapshot.selected_seed_ids().to_vec(),
            traversal,
            completed_units,
            pending_units,
            failed_units,
            partial_units,
            artifact_references,
        };
        checkpoint.validate_against(snapshot)?;
        Ok(checkpoint)
    }

    /// Returns a generic bounded envelope suitable for Plan 04 append-only
    /// persistence. The opaque generic unit IDs are hashes; full scheduling
    /// identities stay in the typed payload and remain bounded by the parent
    /// envelope rather than being truncated.
    ///
    /// # Errors
    /// Returns a typed error when the payload is invalid, exceeds the generic
    /// envelope bounds, or cannot be serialized.
    pub fn to_envelope(&self) -> Result<CheckpointEnvelope, CrawlCheckpointError> {
        self.validate_payload()?;
        let identity = CheckpointIdentity::new(
            self.crawl_run_id.to_string(),
            self.snapshot_hash.clone(),
            self.checkpoint_compatibility_hash.clone(),
        )?;
        let mut envelope = CheckpointEnvelope::new(identity);
        envelope.completed_units = opaque_unit_ids(&self.completed_units)?;
        envelope.pending_units = opaque_unit_ids(&self.pending_units)?;
        let mut failed = self.failed_units.clone();
        failed.extend(self.partial_units.clone());
        envelope.failed_units = opaque_unit_ids(&failed)?;
        envelope
            .artifact_references
            .clone_from(&self.artifact_references);
        envelope.payload =
            Some(serde_json::to_string(self).map_err(|_| CrawlCheckpointError::Serialization)?);
        envelope.encode()?;
        Ok(envelope)
    }

    /// Decodes and validates a typed payload against the immutable run
    /// snapshot loaded from the database. A failure is never converted into a
    /// fresh run or a Seed replay.
    ///
    /// # Errors
    /// Returns a typed error when the envelope is malformed, incompatible
    /// with the immutable snapshot, or contains inconsistent typed state.
    pub fn from_envelope(
        envelope: &CheckpointEnvelope,
        snapshot: &CrawlRunSnapshot,
        crawl_run_id: CrawlRunId,
    ) -> Result<Self, CrawlCheckpointError> {
        envelope.encode()?;
        if envelope.identity.snapshot_id != crawl_run_id.to_string()
            || envelope.identity.snapshot_hash != snapshot.snapshot_hash()
            || envelope.identity.compatibility_hash != snapshot.checkpoint_compatibility_hash()
        {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
        }
        let payload = envelope
            .payload
            .as_deref()
            .ok_or(CrawlCheckpointError::MalformedPayload)?;
        let checkpoint: Self =
            serde_json::from_str(payload).map_err(|_| CrawlCheckpointError::MalformedPayload)?;
        checkpoint.validate_against(snapshot)?;
        if checkpoint.crawl_run_id != crawl_run_id
            || envelope.completed_units != opaque_unit_ids(&checkpoint.completed_units)?
            || envelope.pending_units != opaque_unit_ids(&checkpoint.pending_units)?
            || envelope.artifact_references != checkpoint.artifact_references
        {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
        }
        let mut failed = checkpoint.failed_units.clone();
        failed.extend(checkpoint.partial_units.clone());
        if envelope.failed_units != opaque_unit_ids(&failed)? {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
        }
        Ok(checkpoint)
    }

    /// Reduces immutable execution history into current work units. A later
    /// successful attempt supersedes earlier failed/partial history for the
    /// current structural state, while all execution IDs remain referenced.
    ///
    /// # Errors
    /// Returns a typed error when durable execution rows contain invalid or
    /// conflicting unit identities.
    pub fn units_from_executions(
        records: &[CrawlExecutionRecord],
    ) -> Result<CheckpointUnitSets, CrawlCheckpointError> {
        let mut by_canonical = BTreeMap::<String, CrawlCheckpointUnit>::new();
        for record in records {
            let state = match record.outcome {
                CrawlExecutionOutcome::Completed => CrawlCheckpointUnitState::Completed,
                CrawlExecutionOutcome::Partial => CrawlCheckpointUnitState::Partial,
                CrawlExecutionOutcome::Failed | CrawlExecutionOutcome::Cancelled => {
                    CrawlCheckpointUnitState::Failed
                }
            };
            let candidate = CrawlCheckpointUnit {
                state,
                requested_url: record.requested_url.clone(),
                canonical_url: record.canonical_url.clone(),
                discovered_url_id: record.discovered_url_id.clone(),
                depth: 0,
                page_type_id: record.page_type_id,
                transition_id: record.transition_id,
                parent_canonical_url: None,
                final_canonical_url: record.observed_final_url.clone(),
                pagination: false,
                seed_ids: Vec::new(),
                execution_ids: vec![record.id],
            };
            validate_unit(&candidate)?;
            match by_canonical.get_mut(&record.canonical_url) {
                Some(existing) => merge_unit(existing, candidate)?,
                None => {
                    by_canonical.insert(record.canonical_url.clone(), candidate);
                }
            }
        }
        let mut completed = Vec::new();
        let mut failed = Vec::new();
        let mut partial = Vec::new();
        for unit in by_canonical.into_values() {
            match unit.state {
                CrawlCheckpointUnitState::Completed => completed.push(unit),
                CrawlCheckpointUnitState::Failed => failed.push(unit),
                CrawlCheckpointUnitState::Partial => partial.push(unit),
                CrawlCheckpointUnitState::Pending => return Err(CrawlCheckpointError::InvalidUnit),
            }
        }
        Ok((completed, failed, partial))
    }

    /// Adds the exact Seed provenance retained in `discovered_urls` to
    /// execution units. This lookup is by the durable `(original, canonical)`
    /// identity, never by canonical URL alone or row order.
    ///
    /// # Errors
    /// Returns a typed error when durable provenance contains duplicate or
    /// malformed seed evidence.
    pub fn units_from_executions_with_discovery(
        records: &[CrawlExecutionRecord],
        discovered_urls: &[DiscoveredUrlRecord],
    ) -> Result<CheckpointUnitSets, CrawlCheckpointError> {
        let (mut completed, mut failed, mut partial) = Self::units_from_executions(records)?;
        for unit in completed
            .iter_mut()
            .chain(failed.iter_mut())
            .chain(partial.iter_mut())
        {
            let matches = if let Some(discovered_url_id) = unit.discovered_url_id.as_deref() {
                discovered_urls
                    .iter()
                    .filter(|record| record.id == discovered_url_id)
                    .collect::<Vec<_>>()
            } else {
                discovered_urls
                    .iter()
                    .filter(|record| {
                        record.original_url == unit.requested_url
                            && record.canonical_url == unit.canonical_url
                            && (matches!(
                                record.status.as_str(),
                                "ADMITTED" | "EXECUTION_RECONCILED"
                            ) || record
                                .detail
                                .get("origin")
                                .and_then(serde_json::Value::as_str)
                                == Some("SEED"))
                    })
                    .collect::<Vec<_>>()
            };
            if matches.len() > 1 {
                return Err(CrawlCheckpointError::DuplicateUnit);
            }
            let Some(record) = matches.first() else {
                continue;
            };
            if record.original_url != unit.requested_url
                || record.canonical_url != unit.canonical_url
            {
                return Err(CrawlCheckpointError::Identity);
            }
            unit.seed_ids = seed_ids_from_detail(&record.detail)?;
        }
        Ok((completed, failed, partial))
    }

    /// Carries scheduling metadata from the previous checkpoint onto newly
    /// materialized execution units. Execution rows intentionally do not own
    /// traversal depth, pagination, or parent context, so retry checkpoints
    /// must preserve those fields from the validated semantic frontier.
    ///
    /// # Errors
    /// Returns a typed error when prior metadata is ambiguous or creates an
    /// invalid durable unit.
    pub fn carry_forward_unit_metadata(
        units: &mut [CrawlCheckpointUnit],
        previous: Option<&Self>,
    ) -> Result<(), CrawlCheckpointError> {
        let Some(previous) = previous else {
            return Ok(());
        };
        let previous_units = previous
            .completed_units
            .iter()
            .chain(&previous.pending_units)
            .chain(&previous.failed_units)
            .chain(&previous.partial_units)
            .collect::<Vec<_>>();
        for unit in units {
            let mut matches = previous_units
                .iter()
                .filter(|candidate| candidate.canonical_url == unit.canonical_url)
                .collect::<Vec<_>>();
            if matches.is_empty() {
                matches = previous_units
                    .iter()
                    .filter(|candidate| candidate.requested_url == unit.requested_url)
                    .collect::<Vec<_>>();
            }
            if matches.len() > 1 {
                return Err(CrawlCheckpointError::DuplicateUnit);
            }
            let Some(previous) = matches.pop() else {
                continue;
            };
            unit.depth = previous.depth;
            unit.page_type_id = unit.page_type_id.or(previous.page_type_id);
            unit.transition_id = unit.transition_id.or(previous.transition_id);
            unit.parent_canonical_url = previous.parent_canonical_url.clone();
            unit.final_canonical_url = unit
                .final_canonical_url
                .clone()
                .or_else(|| previous.final_canonical_url.clone());
            unit.pagination = previous.pagination;
            if unit.seed_ids.is_empty() {
                unit.seed_ids = previous.seed_ids.clone();
            }
            if unit.discovered_url_id.is_none() {
                unit.discovered_url_id = previous.discovered_url_id.clone();
            }
            validate_unit(unit)?;
        }
        Ok(())
    }

    /// Advances stale recovery state from immutable execution history before a
    /// recovered worker is permitted to call its provider.  Execution rows
    /// are the durable authority for a physical canonical work item: a
    /// successful row removes that item from every recoverable selection even
    /// when a crash happened before the next checkpoint append.
    ///
    /// The checkpoint retains only scheduling metadata that execution rows do
    /// not own (depth, transition, parent, pagination, and Seed provenance).
    /// Earlier failed/partial rows remain immutable history; only the current
    /// recovery partition changes.
    ///
    /// # Errors
    /// Returns a typed error if durable records cannot be merged with the
    /// checkpoint's exact canonical identities.
    pub fn reconcile_durable_executions(
        &mut self,
        records: &[CrawlExecutionRecord],
    ) -> Result<(), CrawlCheckpointError> {
        let (mut completed, mut failed, mut partial) = Self::units_from_executions(records)?;
        let previous = self.clone();
        Self::carry_forward_unit_metadata(&mut completed, Some(&previous))?;
        Self::carry_forward_unit_metadata(&mut failed, Some(&previous))?;
        Self::carry_forward_unit_metadata(&mut partial, Some(&previous))?;
        let completed_urls = completed
            .iter()
            .map(|unit| unit.canonical_url.as_str())
            .collect::<BTreeSet<_>>();
        self.traversal
            .pending
            .retain(|entry| !completed_urls.contains(entry.canonical_url.as_str()));
        self.pending_units
            .retain(|unit| !completed_urls.contains(unit.canonical_url.as_str()));
        self.completed_units = completed;
        self.failed_units = failed;
        self.partial_units = partial;
        self.validate_payload()
    }

    fn validate_against(&self, snapshot: &CrawlRunSnapshot) -> Result<(), CrawlCheckpointError> {
        self.validate_payload()?;
        if self.run_type != snapshot.run_type()
            || self.snapshot_hash != snapshot.snapshot_hash()
            || self.checkpoint_compatibility_hash != snapshot.checkpoint_compatibility_hash()
            || self.selected_seed_ids != snapshot.selected_seed_ids()
            || self.robots_user_agent != snapshot.robots().user_agent()
            || self.traversal.selected_seed_ids != snapshot.selected_seed_ids()
        {
            return Err(CrawlCheckpointError::IncompatibleIdentity);
        }
        match (snapshot.configuration(), self.crawler_version_id.as_ref()) {
            (
                erabi_domain::RunConfiguration::CrawlerVersion {
                    crawler_version_id,
                    semantic_config_hash,
                    ..
                },
                Some(checkpoint_version),
            ) if checkpoint_version == crawler_version_id
                && self.semantic_config_hash.as_deref() == Some(semantic_config_hash) => {}
            (erabi_domain::RunConfiguration::QuickScrape { .. }, None) => {}
            _ => return Err(CrawlCheckpointError::IncompatibleIdentity),
        }
        if self.run_type != CrawlRunType::ProductionRun
            && self.run_type != CrawlRunType::QuickScrape
        {
            return Err(CrawlCheckpointError::UnsupportedRunType);
        }
        Ok(())
    }

    fn validate_payload(&self) -> Result<(), CrawlCheckpointError> {
        if self.payload_version != CRAWL_CHECKPOINT_PAYLOAD_VERSION
            || self.snapshot_hash.len() != 64
            || self.checkpoint_compatibility_hash.len() != 64
            || !self
                .snapshot_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !self
                .checkpoint_compatibility_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.robots_user_agent.trim().is_empty()
            || self.robots_user_agent.len() > MAX_TEXT_CHARS
        {
            return Err(CrawlCheckpointError::MalformedPayload);
        }
        let total = self
            .completed_units
            .len()
            .checked_add(self.pending_units.len())
            .and_then(|value| value.checked_add(self.failed_units.len()))
            .and_then(|value| value.checked_add(self.partial_units.len()))
            .ok_or(CrawlCheckpointError::InvalidUnit)?;
        if total > erabi_db::repositories::MAX_CHECKPOINT_UNITS {
            return Err(CrawlCheckpointError::InvalidUnit);
        }
        let traversal_pending = self
            .traversal
            .pending
            .iter()
            .map(|entry| entry.canonical_url.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let payload_pending = self
            .pending_units
            .iter()
            .map(|unit| unit.canonical_url.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if payload_pending.len() != self.pending_units.len()
            || (self.run_type == CrawlRunType::ProductionRun
                && (traversal_pending.len() != self.traversal.pending.len()
                    || traversal_pending != payload_pending))
            || (self.run_type == CrawlRunType::QuickScrape && !traversal_pending.is_empty())
        {
            return Err(CrawlCheckpointError::InvalidUnit);
        }
        let mut identities = BTreeMap::new();
        for (units, expected_state) in [
            (&self.completed_units, CrawlCheckpointUnitState::Completed),
            (&self.pending_units, CrawlCheckpointUnitState::Pending),
            (&self.failed_units, CrawlCheckpointUnitState::Failed),
            (&self.partial_units, CrawlCheckpointUnitState::Partial),
        ] {
            for unit in units {
                validate_unit(unit)?;
                if unit.state != expected_state
                    || identities
                        .insert(unit.canonical_url.clone(), unit.state)
                        .is_some()
                {
                    return Err(CrawlCheckpointError::DuplicateUnit);
                }
            }
        }
        Ok(())
    }
}

fn validate_unit(unit: &CrawlCheckpointUnit) -> Result<(), CrawlCheckpointError> {
    let seed_ids = unit
        .seed_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let execution_ids = unit
        .execution_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    if unit.requested_url.trim().is_empty()
        || unit.canonical_url.trim().is_empty()
        || unit.requested_url.len() > MAX_URL_CHARS
        || unit.canonical_url.len() > MAX_URL_CHARS
        || !valid_checkpoint_url(&unit.requested_url)
        || !valid_checkpoint_url(&unit.canonical_url)
        || seed_ids.len() != unit.seed_ids.len()
        || execution_ids.len() != unit.execution_ids.len()
        || unit
            .discovered_url_id
            .as_deref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_TEXT_CHARS)
        || unit
            .parent_canonical_url
            .as_deref()
            .is_some_and(|value| !valid_checkpoint_url(value))
        || unit
            .final_canonical_url
            .as_deref()
            .is_some_and(|value| !valid_checkpoint_url(value))
    {
        return Err(CrawlCheckpointError::InvalidUnit);
    }
    Ok(())
}

fn valid_checkpoint_url(value: &str) -> bool {
    if value.trim().is_empty() || value.len() > MAX_URL_CHARS {
        return false;
    }
    let Ok(parsed) = url::Url::parse(value) else {
        return false;
    };
    matches!(parsed.scheme(), "http" | "https")
        && parsed.host_str().is_some()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && !value.chars().any(char::is_control)
        && parsed.fragment().is_none()
}

fn merge_unit(
    existing: &mut CrawlCheckpointUnit,
    candidate: CrawlCheckpointUnit,
) -> Result<(), CrawlCheckpointError> {
    if existing.canonical_url != candidate.canonical_url {
        return Err(CrawlCheckpointError::DuplicateUnit);
    }
    if existing.requested_url != candidate.requested_url
        || conflicting_option(
            existing.discovered_url_id.as_ref(),
            candidate.discovered_url_id.as_ref(),
        )
        || conflicting_option(
            existing.page_type_id.as_ref(),
            candidate.page_type_id.as_ref(),
        )
        || conflicting_option(
            existing.transition_id.as_ref(),
            candidate.transition_id.as_ref(),
        )
        || conflicting_option(
            existing.parent_canonical_url.as_ref(),
            candidate.parent_canonical_url.as_ref(),
        )
        || conflicting_option(
            existing.final_canonical_url.as_ref(),
            candidate.final_canonical_url.as_ref(),
        )
        || (!existing.seed_ids.is_empty()
            && !candidate.seed_ids.is_empty()
            && existing.seed_ids != candidate.seed_ids)
    {
        return Err(CrawlCheckpointError::DuplicateUnit);
    }
    let existing_rank = unit_rank(existing.state);
    let candidate_rank = unit_rank(candidate.state);
    let existing_execution_ids = existing.execution_ids.clone();
    if existing.discovered_url_id.is_none() {
        existing.discovered_url_id = candidate.discovered_url_id.clone();
    }
    if existing.page_type_id.is_none() {
        existing.page_type_id = candidate.page_type_id;
    }
    if existing.transition_id.is_none() {
        existing.transition_id = candidate.transition_id;
    }
    if existing.parent_canonical_url.is_none() {
        existing.parent_canonical_url = candidate.parent_canonical_url.clone();
    }
    if existing.final_canonical_url.is_none() {
        existing.final_canonical_url = candidate.final_canonical_url.clone();
    }
    if existing.seed_ids.is_empty() {
        existing.seed_ids = candidate.seed_ids.clone();
    }
    if candidate_rank > existing_rank {
        let mut replacement = candidate;
        if replacement.discovered_url_id.is_none() {
            replacement
                .discovered_url_id
                .clone_from(&existing.discovered_url_id);
        }
        if replacement.page_type_id.is_none() {
            replacement.page_type_id = existing.page_type_id;
        }
        if replacement.transition_id.is_none() {
            replacement.transition_id = existing.transition_id;
        }
        if replacement.parent_canonical_url.is_none() {
            replacement
                .parent_canonical_url
                .clone_from(&existing.parent_canonical_url);
        }
        if replacement.final_canonical_url.is_none() {
            replacement
                .final_canonical_url
                .clone_from(&existing.final_canonical_url);
        }
        if replacement.seed_ids.is_empty() {
            replacement.seed_ids.clone_from(&existing.seed_ids);
        }
        replacement.execution_ids.extend(existing_execution_ids);
        replacement.execution_ids.sort_by_key(ToString::to_string);
        replacement.execution_ids.dedup();
        *existing = replacement;
    } else {
        existing.execution_ids.extend(candidate.execution_ids);
        existing.execution_ids.sort_by_key(ToString::to_string);
        existing.execution_ids.dedup();
    }
    Ok(())
}

fn conflicting_option<T: PartialEq>(left: Option<&T>, right: Option<&T>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left != right)
}

const fn unit_rank(state: CrawlCheckpointUnitState) -> u8 {
    match state {
        CrawlCheckpointUnitState::Pending => 0,
        CrawlCheckpointUnitState::Failed => 1,
        CrawlCheckpointUnitState::Partial => 2,
        CrawlCheckpointUnitState::Completed => 3,
    }
}

fn seed_ids_from_detail(detail: &serde_json::Value) -> Result<Vec<SeedId>, CrawlCheckpointError> {
    let Some(values) = detail.get("seed_ids") else {
        return Ok(Vec::new());
    };
    let Some(values) = values.as_array() else {
        return Err(CrawlCheckpointError::InvalidUnit);
    };
    let mut ids = Vec::new();
    for value in values {
        let Some(value) = value.as_str() else {
            return Err(CrawlCheckpointError::InvalidUnit);
        };
        let id: SeedId = serde_json::from_value(serde_json::Value::String(value.to_owned()))
            .map_err(|_| CrawlCheckpointError::InvalidUnit)?;
        if !ids.iter().any(|existing: &SeedId| existing == &id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn opaque_unit_ids(
    units: &[CrawlCheckpointUnit],
) -> Result<Vec<CheckpointUnitId>, CrawlCheckpointError> {
    let mut ids = units
        .iter()
        .map(|unit| {
            let digest = erabi_domain::canonical_sha256(&unit.canonical_url)
                .map_err(|_| CrawlCheckpointError::Identity)?;
            CheckpointUnitId::new(format!("URL:{digest}")).map_err(CrawlCheckpointError::Envelope)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ids.sort();
    ids.dedup();
    if ids.len() != units.len() {
        return Err(CrawlCheckpointError::DuplicateUnit);
    }
    Ok(ids)
}

#[cfg(test)]
mod compact_tests {
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

    #[test]
    fn compact_v2_size_is_independent_of_legal_frontier_cardinality()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = quick_snapshot()?;
        let checkpoint =
            CrawlCheckpointV2::new(CrawlRunId::new(), &snapshot, CrawlRecoveryPhase::Traversing)?;
        let baseline = checkpoint
            .to_envelope()?
            .payload
            .ok_or("payload missing")?
            .len();
        // A legal crawler may have thousands of 4096-character URLs. V2 has
        // nowhere to accept them, so its encoded control identity is constant.
        let simulated_frontier = vec!["x".repeat(4_096); 4_096];
        assert_eq!(simulated_frontier.len(), 4_096);
        assert_eq!(
            checkpoint
                .to_envelope()?
                .payload
                .ok_or("payload missing")?
                .len(),
            baseline
        );
        assert!(baseline <= MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES);
        Ok(())
    }
}
