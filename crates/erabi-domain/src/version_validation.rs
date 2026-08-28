use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use sha2::{Digest, Sha256};

use crate::{
    CrawlerVersion, CrawlerVersionId, DiscoveryTransition, ErrorCode, PageType,
    SelectorCoverageStatus, TestEvidence, TestKind, UrlMatcher, UrlMatcherDefinition,
    UrlMatcherKind, resolve_page_type,
};

pub const MAX_VALIDATION_IDENTIFIER_CHARS: usize = 64;
pub const MAX_VALIDATION_MESSAGE_CHARS: usize = 512;
pub const MAX_VALIDATION_ISSUES_PER_CONTRIBUTOR: usize = 256;
pub const MAX_VALIDATION_ISSUES: usize = 512;
pub const MAX_VALIDATION_DETAILS: usize = 16;
pub const MAX_VALIDATION_DETAIL_CHARS: usize = 256;

macro_rules! validation_identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Creates a bounded validation identifier.
            ///
            /// # Errors
            /// Returns an error when the value is empty, too long, or uses a
            /// character outside the stable ASCII identifier vocabulary.
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidValidationIdentifier> {
                let value = value.into();
                validate_identifier(&value)?;
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

validation_identifier!(ValidationContributorKey);
validation_identifier!(ValidationIssueCode);
validation_identifier!(ValidationSubjectKind);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("validation identifier is not a bounded ASCII identifier")]
pub struct InvalidValidationIdentifier;

fn validate_identifier(value: &str) -> Result<(), InvalidValidationIdentifier> {
    if value.is_empty()
        || value.chars().count() > MAX_VALIDATION_IDENTIFIER_CHARS
        || !value.bytes().enumerate().all(|(index, byte)| {
            (byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
                && (index != 0 || byte.is_ascii_alphabetic())
        })
    {
        return Err(InvalidValidationIdentifier);
    }
    Ok(())
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VersionValidationSeverity {
    Blocker,
    Warning,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize)]
pub struct VersionValidationSubject {
    pub kind: ValidationSubjectKind,
    pub id: Option<String>,
}

impl VersionValidationSubject {
    #[must_use]
    pub fn new(kind: ValidationSubjectKind, id: Option<String>) -> Self {
        Self { kind, id }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize)]
pub struct VersionValidationIssue {
    pub code: ValidationIssueCode,
    pub severity: VersionValidationSeverity,
    pub contributor: Option<ValidationContributorKey>,
    pub message: String,
    pub subject: Option<VersionValidationSubject>,
    pub details: BTreeMap<String, String>,
}

impl VersionValidationIssue {
    #[must_use]
    pub fn new(
        code: ValidationIssueCode,
        severity: VersionValidationSeverity,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            severity,
            contributor: None,
            message: message.into(),
            subject: None,
            details: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_subject(mut self, subject: VersionValidationSubject) -> Self {
        self.subject = Some(subject);
        self
    }

    #[must_use]
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    fn attributed(mut self, contributor: ValidationContributorKey) -> Self {
        self.contributor = Some(contributor);
        self
    }

    fn validate_bounds(&self) -> Result<(), VersionValidationContributorError> {
        if self.message.is_empty()
            || self.message.chars().count() > MAX_VALIDATION_MESSAGE_CHARS
            || self.message.chars().any(char::is_control)
            || self.details.len() > MAX_VALIDATION_DETAILS
            || self.details.iter().any(|(key, value)| {
                key.is_empty()
                    || key.chars().count() > MAX_VALIDATION_DETAIL_CHARS
                    || value.chars().count() > MAX_VALIDATION_DETAIL_CHARS
                    || key.chars().any(char::is_control)
                    || value.chars().any(char::is_control)
            })
            || self.subject.as_ref().is_some_and(|subject| {
                subject.id.as_ref().is_some_and(|id| {
                    id.is_empty()
                        || id.chars().count() > MAX_VALIDATION_DETAIL_CHARS
                        || id.chars().any(char::is_control)
                })
            })
        {
            return Err(VersionValidationContributorError::InvalidContribution);
        }
        Ok(())
    }

    #[must_use]
    pub fn audit_summary(&self) -> String {
        let subject = self.subject.as_ref().map_or_else(String::new, |subject| {
            subject.id.as_ref().map_or_else(
                || subject.kind.to_string(),
                |id| format!("{}:{id}", subject.kind),
            )
        });
        if subject.is_empty() {
            self.code.to_string()
        } else {
            format!("{}:{subject}", self.code)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionValidationContribution {
    pub issues: Vec<VersionValidationIssue>,
}

impl VersionValidationContribution {
    #[must_use]
    pub const fn new(issues: Vec<VersionValidationIssue>) -> Self {
        Self { issues }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum VersionValidationContributorError {
    #[error("validation contributor returned an invalid bounded issue")]
    InvalidContribution,
    #[error("validation contributor failed internally")]
    InternalFailure,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum VersionValidationError {
    #[error("validation contributor key is invalid")]
    InvalidContributorKey,
    #[error("validation contributor keys must be unique")]
    DuplicateContributorKey(ValidationContributorKey),
    #[error("validation contributor returned too many issues")]
    TooManyIssues(ValidationContributorKey),
    #[error("validation contributor failed")]
    ContributorFailed(ValidationContributorKey),
    #[error("validation contributor returned an invalid issue")]
    InvalidContribution(ValidationContributorKey),
}

pub trait VersionValidationContributor: Send + Sync {
    fn key(&self) -> &'static str;

    /// Validates one immutable publication context.
    ///
    /// # Errors
    /// Returns an internal failure or an invalid contribution when the
    /// contributor cannot produce a bounded deterministic result.
    fn validate(
        &self,
        context: &VersionValidationContext,
    ) -> Result<VersionValidationContribution, VersionValidationContributorError>;
}

#[derive(Clone, Debug)]
pub struct VersionValidationContext {
    version: CrawlerVersion,
    page_types: Vec<PageType>,
    transitions: Vec<DiscoveryTransition>,
    test_evidence: Vec<TestEvidence>,
    config_hash: String,
}

impl VersionValidationContext {
    #[must_use]
    pub fn new(
        version: CrawlerVersion,
        page_types: Vec<PageType>,
        transitions: Vec<DiscoveryTransition>,
        test_evidence: Vec<TestEvidence>,
        config_hash: String,
    ) -> Self {
        Self {
            version,
            page_types,
            transitions,
            test_evidence,
            config_hash,
        }
    }

    #[must_use]
    pub const fn version(&self) -> &CrawlerVersion {
        &self.version
    }

    #[must_use]
    pub fn page_types(&self) -> &[PageType] {
        &self.page_types
    }

    #[must_use]
    pub fn transitions(&self) -> &[DiscoveryTransition] {
        &self.transitions
    }

    #[must_use]
    pub fn test_evidence(&self) -> &[TestEvidence] {
        &self.test_evidence
    }

    #[must_use]
    pub fn config_hash(&self) -> &str {
        &self.config_hash
    }
}

#[derive(Clone)]
pub struct VersionValidationRegistry {
    contributors: Vec<Arc<dyn VersionValidationContributor>>,
}

impl fmt::Debug for VersionValidationRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VersionValidationRegistry")
            .field("keys", &self.keys())
            .finish()
    }
}

impl Default for VersionValidationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionValidationRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            contributors: vec![Arc::new(CoreVersionValidationContributor)],
        }
    }

    #[must_use]
    pub fn core_only() -> Self {
        Self::new()
    }

    /// Registers one contributor for this runtime's publication registry.
    ///
    /// # Errors
    /// Returns an error when the contributor key is invalid or duplicates an
    /// already registered contributor.
    pub fn register(
        &mut self,
        contributor: Arc<dyn VersionValidationContributor>,
    ) -> Result<(), VersionValidationError> {
        let key = ValidationContributorKey::new(contributor.key())
            .map_err(|_| VersionValidationError::InvalidContributorKey)?;
        if self
            .contributors
            .iter()
            .any(|registered| registered.key() == key.as_str())
        {
            return Err(VersionValidationError::DuplicateContributorKey(key));
        }
        self.contributors.push(contributor);
        Ok(())
    }

    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        let mut keys = self
            .contributors
            .iter()
            .map(|contributor| contributor.key().to_owned())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    /// Runs every registered contributor and returns the normalized report.
    ///
    /// # Errors
    /// Returns an error when a contributor key or contribution is invalid,
    /// when issue bounds are exceeded, or when a contributor fails internally.
    pub fn validate(
        &self,
        context: &VersionValidationContext,
    ) -> Result<VersionValidationReport, VersionValidationError> {
        let mut contributors = self
            .contributors
            .iter()
            .map(|contributor| {
                let key = ValidationContributorKey::new(contributor.key())
                    .map_err(|_| VersionValidationError::InvalidContributorKey)?;
                Ok((key, Arc::clone(contributor)))
            })
            .collect::<Result<Vec<_>, VersionValidationError>>()?;
        contributors.sort_by(|left, right| left.0.cmp(&right.0));

        let mut seen = BTreeSet::new();
        let mut issues = Vec::new();
        for (key, contributor) in contributors {
            if !seen.insert(key.clone()) {
                return Err(VersionValidationError::DuplicateContributorKey(key));
            }
            let contribution = match contributor.validate(context) {
                Ok(contribution) => contribution,
                Err(VersionValidationContributorError::InvalidContribution) => {
                    return Err(VersionValidationError::InvalidContribution(key));
                }
                Err(VersionValidationContributorError::InternalFailure) => {
                    return Err(VersionValidationError::ContributorFailed(key));
                }
            };
            if contribution.issues.len() > MAX_VALIDATION_ISSUES_PER_CONTRIBUTOR {
                return Err(VersionValidationError::TooManyIssues(key));
            }
            for issue in contribution.issues {
                issue
                    .validate_bounds()
                    .map_err(|_| VersionValidationError::InvalidContribution(key.clone()))?;
                issues.push(issue.attributed(key.clone()));
                if issues.len() > MAX_VALIDATION_ISSUES {
                    return Err(VersionValidationError::TooManyIssues(key.clone()));
                }
            }
        }
        issues.sort();
        issues.dedup();
        let blockers = issues
            .iter()
            .filter(|issue| issue.severity == VersionValidationSeverity::Blocker)
            .cloned()
            .collect::<Vec<_>>();
        let warnings = issues
            .into_iter()
            .filter(|issue| issue.severity == VersionValidationSeverity::Warning)
            .collect::<Vec<_>>();
        Ok(VersionValidationReport::new(
            context.version().id(),
            context.config_hash().to_owned(),
            blockers,
            warnings,
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct VersionValidationReport {
    pub version_id: CrawlerVersionId,
    pub config_hash: String,
    pub blockers: Vec<VersionValidationIssue>,
    pub warnings: Vec<VersionValidationIssue>,
    pub publishable: bool,
}

impl VersionValidationReport {
    #[must_use]
    pub fn new(
        version_id: CrawlerVersionId,
        config_hash: String,
        blockers: Vec<VersionValidationIssue>,
        warnings: Vec<VersionValidationIssue>,
    ) -> Self {
        Self {
            version_id,
            config_hash,
            publishable: blockers.is_empty(),
            blockers,
            warnings,
        }
    }

    #[must_use]
    pub const fn is_publishable(&self) -> bool {
        self.publishable
    }

    #[must_use]
    pub fn warning_summary(&self) -> Vec<String> {
        let mut summary = self
            .warnings
            .iter()
            .map(VersionValidationIssue::audit_summary)
            .collect::<Vec<_>>();
        summary.sort();
        summary.dedup();
        summary
    }
}

pub struct CoreVersionValidationContributor;

impl VersionValidationContributor for CoreVersionValidationContributor {
    fn key(&self) -> &'static str {
        "core"
    }

    #[allow(clippy::too_many_lines)]
    fn validate(
        &self,
        context: &VersionValidationContext,
    ) -> Result<VersionValidationContribution, VersionValidationContributorError> {
        let version = context.version();
        let page_type_ids = context
            .page_types()
            .iter()
            .map(|page_type| page_type.id.to_string())
            .collect::<BTreeSet<_>>();
        let mut issues = Vec::new();
        let enabled_seeds = version
            .seeds()
            .iter()
            .filter(|seed| seed.enabled)
            .collect::<Vec<_>>();

        if enabled_seeds.is_empty() {
            issues.push(core_issue(
                "NO_ENABLED_SEED",
                "CrawlerVersion must contain at least one enabled Seed.",
                None,
            ));
        }

        if version.canonicalization_policy().validate().is_err() {
            issues.push(core_issue(
                "INVALID_CANONICALIZATION",
                "The canonicalization policy is invalid.",
                None,
            ));
        } else {
            for seed in &enabled_seeds {
                match version
                    .canonicalization_policy()
                    .canonicalize(seed.original_url.as_str())
                {
                    Err(_) => issues.push(core_issue(
                        "INVALID_SEED_CANONICALIZATION",
                        "An enabled Seed cannot be canonicalized by the version policy.",
                        Some(subject("SEED", Some(seed.id.to_string()))),
                    )),
                    Ok(result) if result.canonical_url != seed.canonical_url => {
                        issues.push(core_issue(
                            "INVALID_SEED_CANONICALIZATION",
                            "An enabled Seed canonical URL does not match the version policy.",
                            Some(subject("SEED", Some(seed.id.to_string()))),
                        ));
                    }
                    Ok(_) => {}
                }
            }
        }

        for seed in version.seeds() {
            if seed
                .entry_page_type_hint
                .is_some_and(|id| !page_type_ids.contains(&id.to_string()))
            {
                issues.push(core_issue(
                    "MISSING_SEED_PAGE_TYPE",
                    "A Seed PageType hint must reference a PageType owned by the version.",
                    Some(subject("SEED", Some(seed.id.to_string()))),
                ));
            }
        }

        for page_type in context.page_types() {
            for matcher in &page_type.matchers {
                if matcher.validate_definition().is_err() {
                    issues.push(core_issue(
                        "INVALID_MATCHER_SYNTAX",
                        "A PageType contains an invalid URL matcher definition.",
                        Some(subject("PAGETYPE", Some(page_type.id.to_string()))),
                    ));
                }
            }
        }

        if version.domain_scope().validate().is_err() {
            issues.push(core_issue(
                "INVALID_DOMAIN_SCOPE",
                "The Domain Scope policy is invalid.",
                None,
            ));
        }

        match version.guardrails().validate() {
            Ok(()) => {
                for budget in &version.guardrails().page_types {
                    if !page_type_ids.contains(&budget.page_type_id.to_string()) {
                        issues.push(core_issue(
                            "INVALID_PAGE_TYPE_BUDGET",
                            "A PageType budget references a PageType outside the version.",
                            Some(subject("PAGETYPE", Some(budget.page_type_id.to_string()))),
                        ));
                    }
                }
            }
            Err(error) => issues.push(core_guardrail_issue(error.code)),
        }

        for transition in context.transitions() {
            if !page_type_ids.contains(&transition.source_page_type_id.to_string()) {
                issues.push(core_issue(
                    "MISSING_TRANSITION_SOURCE_PAGETYPE",
                    "A DiscoveryTransition source PageType is missing from the version.",
                    Some(subject("TRANSITION", Some(transition.id.to_string()))),
                ));
            }
            if !page_type_ids.contains(&transition.target_page_type_id.to_string()) {
                issues.push(core_issue(
                    "MISSING_TRANSITION_TARGET_PAGETYPE",
                    "A DiscoveryTransition target PageType is missing from the version.",
                    Some(subject("TRANSITION", Some(transition.id.to_string()))),
                ));
            }
            if let Err(error) = transition.validate() {
                issues.push(core_issue(
                    match error.code {
                        ErrorCode::InvalidTransitionBudget => "INVALID_TRANSITION_BUDGET",
                        _ => "INVALID_DISCOVERY_TRANSITION",
                    },
                    "A DiscoveryTransition or its budget is invalid.",
                    Some(subject("TRANSITION", Some(transition.id.to_string()))),
                ));
            }
        }

        for witness in design_time_ambiguity_witnesses(context) {
            issues.push(core_ambiguity_issue(version.id(), &witness));
        }

        for page_type in context.page_types() {
            let current = context.test_evidence().iter().filter(|evidence| {
                evidence.crawler_version_id == version.id()
                    && evidence.config_hash == context.config_hash()
                    && page_type_evidence_kind(evidence.test_kind)
                    && evidence.evaluated_page_type_id == Some(page_type.id)
            });
            let current = current.collect::<Vec<_>>();
            if current.is_empty() {
                issues.push(core_warning_issue(
                    "PAGE_TYPE_TEST_EVIDENCE_MISSING",
                    "No current TestEvidence evaluates this PageType configuration.",
                    Some(subject("PAGETYPE", Some(page_type.id.to_string()))),
                ));
            }
            let selector_observations = current
                .iter()
                .flat_map(|evidence| evidence.selector_coverage.iter())
                .collect::<Vec<_>>();
            let has_positive_selector_observation = selector_observations
                .iter()
                .any(|observation| observation.status == SelectorCoverageStatus::Observed);
            let has_usable_no_match_observation = selector_observations
                .iter()
                .any(|observation| observation.status == SelectorCoverageStatus::NoMatches);
            if has_usable_no_match_observation && !has_positive_selector_observation {
                issues.push(core_warning_issue(
                    "SELECTOR_COVERAGE_UNUSABLE",
                    "Current selector observations report no usable matches.",
                    Some(subject("PAGETYPE", Some(page_type.id.to_string()))),
                ));
            }
        }

        for transition in context.transitions() {
            let current = context.test_evidence().iter().any(|evidence| {
                evidence.crawler_version_id == version.id()
                    && evidence.config_hash == context.config_hash()
                    && evidence.test_kind == TestKind::DiscoveryTransition
                    && evidence.tested_transition_id == Some(transition.id)
                    && evidence
                        .discovery
                        .as_ref()
                        .is_some_and(|discovery| discovery.transition_id == Some(transition.id))
            });
            if !current {
                issues.push(core_warning_issue(
                    "TRANSITION_TEST_EVIDENCE_MISSING",
                    "No current TestEvidence evaluates this DiscoveryTransition configuration.",
                    Some(subject("TRANSITION", Some(transition.id.to_string()))),
                ));
            }
        }

        Ok(VersionValidationContribution::new(issues))
    }
}

fn page_type_evidence_kind(kind: TestKind) -> bool {
    matches!(
        kind,
        TestKind::PageTypeMatching
            | TestKind::Extraction
            | TestKind::SelectorCoverage
            | TestKind::CombinedUrlEvaluation
    )
}

fn core_code(value: &str) -> ValidationIssueCode {
    ValidationIssueCode(value.to_owned())
}

fn core_subject_kind(value: &str) -> ValidationSubjectKind {
    ValidationSubjectKind(value.to_owned())
}

fn subject(kind: &str, id: Option<String>) -> VersionValidationSubject {
    VersionValidationSubject::new(core_subject_kind(kind), id)
}

fn core_issue(
    code: &str,
    message: &str,
    subject: Option<VersionValidationSubject>,
) -> VersionValidationIssue {
    core_issue_with_severity(code, VersionValidationSeverity::Blocker, message, subject)
}

fn core_warning_issue(
    code: &str,
    message: &str,
    subject: Option<VersionValidationSubject>,
) -> VersionValidationIssue {
    core_issue_with_severity(code, VersionValidationSeverity::Warning, message, subject)
}

fn core_issue_with_severity(
    code: &str,
    severity: VersionValidationSeverity,
    message: &str,
    subject: Option<VersionValidationSubject>,
) -> VersionValidationIssue {
    let issue = VersionValidationIssue::new(core_code(code), severity, message);
    match subject {
        Some(subject) => issue.with_subject(subject),
        None => issue,
    }
}

fn core_guardrail_issue(code: ErrorCode) -> VersionValidationIssue {
    let (code, message) = match code {
        ErrorCode::InvalidPageTypeBudget => {
            ("INVALID_PAGE_TYPE_BUDGET", "A PageType budget is invalid.")
        }
        ErrorCode::InvalidTransitionBudget => (
            "INVALID_TRANSITION_BUDGET",
            "A transition budget is invalid.",
        ),
        _ => (
            "INVALID_CRAWLER_GUARDRAILS",
            "Crawler guardrails are invalid.",
        ),
    };
    core_issue(code, message, None)
}

fn core_ambiguity_issue(
    version_id: CrawlerVersionId,
    canonical_witness: &str,
) -> VersionValidationIssue {
    let issue = core_issue(
        "UNRESOLVED_PAGE_TYPE_AMBIGUITY",
        "The canonical PageType resolver has an unresolved ambiguity for a known URL.",
        Some(subject("CRAWLER_VERSION", Some(version_id.to_string()))),
    );
    if canonical_witness.chars().count() <= MAX_VALIDATION_DETAIL_CHARS {
        return issue.with_detail("witness_url", canonical_witness);
    }

    let prefix = canonical_witness
        .chars()
        .take(MAX_VALIDATION_DETAIL_CHARS - 64)
        .collect::<String>();
    let hash = sha256_hex(canonical_witness);
    issue
        .with_detail("witness_url_prefix", prefix)
        .with_detail("witness_url_sha256", hash)
}

fn sha256_hex(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn design_time_ambiguity_witnesses(context: &VersionValidationContext) -> BTreeSet<String> {
    let mut witnesses = BTreeSet::new();
    let page_types = context.page_types();
    for seed in context.version().seeds().iter().filter(|seed| seed.enabled) {
        let Ok(canonical) = context
            .version()
            .canonicalization_policy()
            .canonicalize(seed.original_url.as_str())
        else {
            continue;
        };
        if matches!(
            resolve_page_type(&canonical.canonical_url, page_types),
            crate::PageTypeMatchDecision::Ambiguous { .. }
        ) {
            witnesses.insert(canonical.canonical_url.to_string());
        }
    }
    for (left_index, left) in page_types.iter().enumerate() {
        for right in page_types.iter().skip(left_index + 1) {
            if left.priority != right.priority {
                continue;
            }
            for left_matcher in &left.matchers {
                for right_matcher in &right.matchers {
                    if left_matcher.kind() == UrlMatcherKind::Regex
                        || right_matcher.kind() == UrlMatcherKind::Regex
                        || left_matcher.specificity() != right_matcher.specificity()
                        || matcher_fingerprint(left_matcher) != matcher_fingerprint(right_matcher)
                    {
                        continue;
                    }
                    if let Some(witness) = matcher_witness(left_matcher)
                        && let Ok(canonical) = context
                            .version()
                            .canonicalization_policy()
                            .canonicalize(witness.as_str())
                        && matches!(
                            resolve_page_type(&canonical.canonical_url, page_types),
                            crate::PageTypeMatchDecision::Ambiguous { .. }
                        )
                    {
                        witnesses.insert(canonical.canonical_url.to_string());
                    }
                }
            }
        }
    }
    witnesses
}

fn matcher_fingerprint(matcher: &UrlMatcher) -> String {
    match matcher.definition() {
        UrlMatcherDefinition::ExactUrl { url } => format!("EXACT_URL|{url}"),
        UrlMatcherDefinition::ExactHostPathTemplate {
            host,
            path_template,
            query,
        } => format!(
            "EXACT_HOST_PATH_TEMPLATE|{}|{}|{:?}",
            host.to_ascii_lowercase(),
            normalize_template(&path_template),
            query
        ),
        UrlMatcherDefinition::PathPrefix { host, prefix } => format!(
            "PATH_PREFIX|{}|{prefix}",
            host.unwrap_or_default().to_ascii_lowercase()
        ),
        UrlMatcherDefinition::PathGlob { host, pattern } => format!(
            "PATH_GLOB|{}|{pattern}",
            host.unwrap_or_default().to_ascii_lowercase()
        ),
        UrlMatcherDefinition::Regex { pattern } => format!("REGEX|{pattern}"),
    }
}

fn normalize_template(template: &str) -> String {
    template
        .split('/')
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') {
                "{}"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn matcher_witness(matcher: &UrlMatcher) -> Option<url::Url> {
    let (host, mut path) = match matcher.definition() {
        UrlMatcherDefinition::ExactUrl { url } => return Some(url),
        UrlMatcherDefinition::ExactHostPathTemplate {
            host,
            path_template,
            query,
        } => {
            let path = normalize_template_witness(&path_template);
            let mut url = url::Url::parse(&format!("https://{host}{path}")).ok()?;
            for (key, value) in query {
                url.query_pairs_mut().append_pair(&key, &value);
            }
            return Some(url);
        }
        UrlMatcherDefinition::PathPrefix { host, prefix } => (host, prefix),
        UrlMatcherDefinition::PathGlob { host, pattern } => (host, pattern.replace('*', "witness")),
        UrlMatcherDefinition::Regex { .. } => return None,
    };
    let host = host.unwrap_or_else(|| "example.test".into());
    if path.is_empty() {
        path.push('/');
    }
    url::Url::parse(&format!("https://{host}{path}")).ok()
}

fn normalize_template_witness(template: &str) -> String {
    template
        .split('/')
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') {
                "witness"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}
