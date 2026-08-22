//! Immutable configuration frozen when a Crawl Run is created.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::{CrawlRunType, CrawlerId, CrawlerVersionId, ResolvedValue, RunProfileId, SeedId};

/// Errors that prevent creation of an auditable, deterministic run snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("invalid crawl run snapshot: {0}")]
    Invalid(String),
    #[error("could not serialize a crawl run snapshot canonically")]
    CanonicalSerialization(#[from] serde_json::Error),
}

/// The resolved operational settings frozen into a Crawl Run snapshot.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotOperationalSettings {
    pub max_pages: ResolvedValue<u64>,
    pub max_depth: ResolvedValue<u32>,
    pub max_duration_seconds: ResolvedValue<u64>,
    pub concurrency: ResolvedValue<u32>,
    pub request_delay_ms: ResolvedValue<u64>,
    pub timeout_ms: ResolvedValue<u64>,
    pub screenshot: ResolvedValue<bool>,
    pub asset_download_limit_bytes: ResolvedValue<u64>,
    pub retain_artifacts: ResolvedValue<bool>,
    pub user_agent: ResolvedValue<String>,
}

/// The immutable semantic configuration identity used by a Crawl Run.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "kind")]
pub enum RunConfiguration {
    CrawlerVersion {
        crawler_id: CrawlerId,
        crawler_version_id: CrawlerVersionId,
        semantic_config_hash: String,
    },
    QuickScrape {
        target_url: url::Url,
        ad_hoc_configuration: BTreeMap<String, serde_json::Value>,
    },
}

/// The robots-policy decision preserved with its audit context.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "decision")]
pub enum RobotsDecision {
    Respect,
    Override { reason: String },
}

/// Auditable robots-policy context for a run snapshot.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RobotsAudit {
    decision: RobotsDecision,
    actor: String,
    decided_at: String,
    affected_scope: String,
    user_agent: String,
    crawler_version_id: Option<CrawlerVersionId>,
}

impl RobotsAudit {
    /// Creates the default, robots-respecting decision.
    #[must_use]
    pub fn respect(
        actor: impl Into<String>,
        decided_at: impl Into<String>,
        affected_scope: impl Into<String>,
        user_agent: impl Into<String>,
        crawler_version_id: Option<CrawlerVersionId>,
    ) -> Self {
        Self {
            decision: RobotsDecision::Respect,
            actor: actor.into(),
            decided_at: decided_at.into(),
            affected_scope: affected_scope.into(),
            user_agent: user_agent.into(),
            crawler_version_id,
        }
    }

    /// Creates an explicit robots override with its required reason.
    ///
    /// # Errors
    /// Returns an error when the reason is empty or whitespace-only.
    pub fn override_with_reason(
        reason: impl Into<String>,
        actor: impl Into<String>,
        decided_at: impl Into<String>,
        affected_scope: impl Into<String>,
        user_agent: impl Into<String>,
        crawler_version_id: Option<CrawlerVersionId>,
    ) -> Result<Self, SnapshotError> {
        let reason = reason.into();
        require_non_empty("robots override reason", &reason)?;
        Ok(Self {
            decision: RobotsDecision::Override { reason },
            actor: actor.into(),
            decided_at: decided_at.into(),
            affected_scope: affected_scope.into(),
            user_agent: user_agent.into(),
            crawler_version_id,
        })
    }

    #[must_use]
    pub const fn decision(&self) -> &RobotsDecision {
        &self.decision
    }

    #[must_use]
    pub fn actor(&self) -> &str {
        &self.actor
    }

    #[must_use]
    pub fn decided_at(&self) -> &str {
        &self.decided_at
    }

    #[must_use]
    pub fn affected_scope(&self) -> &str {
        &self.affected_scope
    }

    #[must_use]
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    #[must_use]
    pub const fn crawler_version_id(&self) -> Option<CrawlerVersionId> {
        self.crawler_version_id
    }

    fn validate(&self) -> Result<(), SnapshotError> {
        require_non_empty("robots actor", &self.actor)?;
        require_non_empty("robots decision time", &self.decided_at)?;
        require_non_empty("robots affected scope", &self.affected_scope)?;
        require_non_empty("robots user agent", &self.user_agent)?;
        if let RobotsDecision::Override { reason } = &self.decision {
            require_non_empty("robots override reason", reason)?;
        }
        Ok(())
    }
}

/// Input used once to build an immutable snapshot.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CrawlRunSnapshotDraft {
    pub run_type: CrawlRunType,
    pub configuration: RunConfiguration,
    pub selected_seed_ids: Vec<SeedId>,
    pub run_profile_id: Option<RunProfileId>,
    pub settings: SnapshotOperationalSettings,
    pub robots: RobotsAudit,
    pub actor: String,
    pub created_at: String,
}

/// A fully resolved, immutable snapshot used for execution, retry, resume, and audit.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct CrawlRunSnapshot {
    run_type: CrawlRunType,
    configuration: RunConfiguration,
    selected_seed_ids: Vec<SeedId>,
    run_profile_id: Option<RunProfileId>,
    settings: SnapshotOperationalSettings,
    robots: RobotsAudit,
    actor: String,
    created_at: String,
    snapshot_hash: String,
    checkpoint_compatibility_hash: String,
}

#[derive(serde::Deserialize)]
struct CrawlRunSnapshotWire {
    run_type: CrawlRunType,
    configuration: RunConfiguration,
    selected_seed_ids: Vec<SeedId>,
    run_profile_id: Option<RunProfileId>,
    settings: SnapshotOperationalSettings,
    robots: RobotsAudit,
    actor: String,
    created_at: String,
    snapshot_hash: String,
    checkpoint_compatibility_hash: String,
}

impl CrawlRunSnapshot {
    /// Freezes a fully-resolved configuration into a deterministic snapshot.
    ///
    /// # Errors
    /// Returns an error when the run/configuration combination or required
    /// audit context is invalid.
    pub fn new(draft: CrawlRunSnapshotDraft) -> Result<Self, SnapshotError> {
        validate_draft(&draft)?;

        let snapshot_hash = canonical_sha256(&draft)?;
        let checkpoint_compatibility_hash = canonical_sha256(&CheckpointCompatibility {
            run_type: draft.run_type,
            configuration: &draft.configuration,
            selected_seed_ids: &draft.selected_seed_ids,
            settings: &draft.settings,
            robots_decision: draft.robots.decision(),
            robots_scope: draft.robots.affected_scope(),
            user_agent: draft.robots.user_agent(),
        })?;

        Ok(Self {
            run_type: draft.run_type,
            configuration: draft.configuration,
            selected_seed_ids: draft.selected_seed_ids,
            run_profile_id: draft.run_profile_id,
            settings: draft.settings,
            robots: draft.robots,
            actor: draft.actor,
            created_at: draft.created_at,
            snapshot_hash,
            checkpoint_compatibility_hash,
        })
    }

    fn rehydrate(
        draft: CrawlRunSnapshotDraft,
        stored_snapshot_hash: &str,
        stored_checkpoint_compatibility_hash: &str,
    ) -> Result<Self, SnapshotError> {
        let snapshot = Self::new(draft)?;
        if snapshot.snapshot_hash != stored_snapshot_hash {
            return Err(SnapshotError::Invalid(
                "stored snapshot hash does not match the canonical snapshot".into(),
            ));
        }
        if snapshot.checkpoint_compatibility_hash != stored_checkpoint_compatibility_hash {
            return Err(SnapshotError::Invalid(
                "stored checkpoint compatibility hash does not match the canonical snapshot".into(),
            ));
        }
        Ok(snapshot)
    }

    #[must_use]
    pub const fn run_type(&self) -> CrawlRunType {
        self.run_type
    }

    #[must_use]
    pub const fn configuration(&self) -> &RunConfiguration {
        &self.configuration
    }

    #[must_use]
    pub fn selected_seed_ids(&self) -> &[SeedId] {
        &self.selected_seed_ids
    }

    #[must_use]
    pub const fn run_profile_id(&self) -> Option<RunProfileId> {
        self.run_profile_id
    }

    #[must_use]
    pub const fn settings(&self) -> &SnapshotOperationalSettings {
        &self.settings
    }

    #[must_use]
    pub const fn robots(&self) -> &RobotsAudit {
        &self.robots
    }

    #[must_use]
    pub fn actor(&self) -> &str {
        &self.actor
    }

    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    #[must_use]
    pub fn snapshot_hash(&self) -> &str {
        &self.snapshot_hash
    }

    #[must_use]
    pub fn checkpoint_compatibility_hash(&self) -> &str {
        &self.checkpoint_compatibility_hash
    }
}

impl<'de> serde::Deserialize<'de> for CrawlRunSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = <CrawlRunSnapshotWire as serde::Deserialize>::deserialize(deserializer)?;
        Self::rehydrate(
            CrawlRunSnapshotDraft {
                run_type: wire.run_type,
                configuration: wire.configuration,
                selected_seed_ids: wire.selected_seed_ids,
                run_profile_id: wire.run_profile_id,
                settings: wire.settings,
                robots: wire.robots,
                actor: wire.actor,
                created_at: wire.created_at,
            },
            &wire.snapshot_hash,
            &wire.checkpoint_compatibility_hash,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(serde::Serialize)]
struct CheckpointCompatibility<'a> {
    run_type: CrawlRunType,
    configuration: &'a RunConfiguration,
    selected_seed_ids: &'a [SeedId],
    settings: &'a SnapshotOperationalSettings,
    robots_decision: &'a RobotsDecision,
    robots_scope: &'a str,
    user_agent: &'a str,
}

/// Produces a SHA-256 hash from canonical JSON with recursively sorted object keys.
///
/// # Errors
/// Returns an error when `value` cannot be represented as JSON.
pub fn canonical_sha256<T: serde::Serialize>(value: &T) -> Result<String, SnapshotError> {
    let value = serde_json::to_value(value)?;
    let mut canonical = String::new();
    write_canonical_json(&value, &mut canonical)?;
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(hex_encode(digest.as_slice()))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn write_canonical_json(
    value: &serde_json::Value,
    output: &mut String,
) -> Result<(), SnapshotError> {
    match value {
        serde_json::Value::Null => output.push_str("null"),
        serde_json::Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        serde_json::Value::Number(value) => output.push_str(&value.to_string()),
        serde_json::Value::String(value) => output.push_str(&serde_json::to_string(value)?),
        serde_json::Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(']');
        }
        serde_json::Value::Object(values) => {
            output.push('{');
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key)?);
                output.push(':');
                write_canonical_json(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn validate_draft(draft: &CrawlRunSnapshotDraft) -> Result<(), SnapshotError> {
    require_non_empty("run actor", &draft.actor)?;
    require_non_empty("run creation time", &draft.created_at)?;
    require_non_empty("resolved user agent", &draft.settings.user_agent.value)?;
    draft.robots.validate()?;

    if draft.settings.user_agent.value != draft.robots.user_agent {
        return Err(SnapshotError::Invalid(
            "resolved user agent must match robots audit user agent".into(),
        ));
    }

    match (&draft.run_type, &draft.configuration) {
        (CrawlRunType::QuickScrape, RunConfiguration::QuickScrape { .. }) => {
            if draft.robots.crawler_version_id().is_some() {
                return Err(SnapshotError::Invalid(
                    "Quick Scrape robots audit cannot reference a CrawlerVersion".into(),
                ));
            }
        }
        (CrawlRunType::QuickScrape, RunConfiguration::CrawlerVersion { .. }) => {
            return Err(SnapshotError::Invalid(
                "QUICK_SCRAPE requires an ad-hoc Quick Scrape configuration".into(),
            ));
        }
        (_, RunConfiguration::QuickScrape { .. }) => {
            return Err(SnapshotError::Invalid(
                "only QUICK_SCRAPE may use an ad-hoc Quick Scrape configuration".into(),
            ));
        }
        (
            _,
            RunConfiguration::CrawlerVersion {
                crawler_version_id,
                semantic_config_hash,
                ..
            },
        ) => {
            if draft.robots.crawler_version_id() != Some(*crawler_version_id) {
                return Err(SnapshotError::Invalid(
                    "robots audit must reference the snapshotted CrawlerVersion".into(),
                ));
            }
            if semantic_config_hash.len() != 64
                || !semantic_config_hash
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
            {
                return Err(SnapshotError::Invalid(
                    "semantic configuration hash must be a SHA-256 hex value".into(),
                ));
            }
        }
    }
    Ok(())
}

fn require_non_empty(label: &str, value: &str) -> Result<(), SnapshotError> {
    if value.trim().is_empty() {
        return Err(SnapshotError::Invalid(format!("{label} must not be empty")));
    }
    Ok(())
}
