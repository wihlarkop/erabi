use std::{collections::BTreeSet, net::IpAddr};

use crate::{ErrorCode, ProductError, Seed};

/// The currently supported Domain Scope policy representation.
pub const DOMAIN_SCOPE_POLICY_VERSION: u16 = 1;

/// A small exact/boundary-aware custom host rule. No regex or substring rules
/// are accepted, so rule meaning cannot depend on iteration order.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum DomainScopeHostRule {
    Exact { host: String },
    Subdomains { host: String },
}

impl DomainScopeHostRule {
    /// Creates an exact host rule after normalizing its host.
    ///
    /// # Errors
    /// Returns an invalid-rule error for an empty, port-bearing, or malformed
    /// host.
    pub fn exact(host: impl Into<String>) -> Result<Self, ProductError> {
        let host = normalize_rule_host(&host.into())?;
        Ok(Self::Exact { host })
    }

    /// Creates a root-plus-subdomains rule after normalizing its host.
    ///
    /// # Errors
    /// Returns an invalid-rule error for an empty, port-bearing, or malformed
    /// host.
    pub fn subdomains(host: impl Into<String>) -> Result<Self, ProductError> {
        let host = normalize_rule_host(&host.into())?;
        Ok(Self::Subdomains { host })
    }

    fn host(&self) -> &str {
        match self {
            Self::Exact { host } | Self::Subdomains { host } => host,
        }
    }

    fn matches(&self, candidate: &str) -> bool {
        match self {
            Self::Exact { host } => candidate == host,
            Self::Subdomains { host } => {
                candidate == host
                    || candidate
                        .strip_suffix(host)
                        .is_some_and(|rest| rest.ends_with('.'))
            }
        }
    }

    fn validate(&self) -> Result<(), ProductError> {
        let normalized = normalize_rule_host(self.host())?;
        if normalized != self.host() {
            return Err(invalid_scope_rule("Domain Scope hosts must be normalized"));
        }
        Ok(())
    }
}

/// The four supported MVP scope variants.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum DomainScopeKind {
    SeedDomainsOnly,
    SameRegistrableDomain {
        #[serde(default)]
        explicit_subdomains: BTreeSet<String>,
    },
    ExplicitAllowlist {
        hosts: BTreeSet<String>,
    },
    Custom {
        #[serde(default)]
        allow: BTreeSet<DomainScopeHostRule>,
        #[serde(default)]
        block: BTreeSet<DomainScopeHostRule>,
    },
}

/// A versioned Domain Scope policy owned by a `CrawlerVersion`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainScopePolicy {
    pub version: u16,
    pub policy: DomainScopeKind,
}

impl Default for DomainScopePolicy {
    fn default() -> Self {
        Self {
            version: DOMAIN_SCOPE_POLICY_VERSION,
            policy: DomainScopeKind::SeedDomainsOnly,
        }
    }
}

impl DomainScopePolicy {
    #[must_use]
    pub fn seed_domains_only() -> Self {
        Self::default()
    }

    /// Validates scope representation without widening invalid policy state.
    ///
    /// # Errors
    /// Returns a typed invalid-scope error.
    pub fn validate(&self) -> Result<(), ProductError> {
        if self.version != DOMAIN_SCOPE_POLICY_VERSION {
            return Err(invalid_scope("unsupported Domain Scope policy version"));
        }
        match &self.policy {
            DomainScopeKind::SeedDomainsOnly => {}
            DomainScopeKind::SameRegistrableDomain {
                explicit_subdomains,
            } => {
                for host in explicit_subdomains {
                    validate_normalized_host(host)?;
                }
            }
            DomainScopeKind::ExplicitAllowlist { hosts } => {
                if hosts.is_empty() {
                    return Err(invalid_scope("Domain Scope allowlist must not be empty"));
                }
                for host in hosts {
                    validate_normalized_host(host)?;
                }
            }
            DomainScopeKind::Custom { allow, block } => {
                if allow.is_empty() {
                    return Err(invalid_scope(
                        "custom Domain Scope allow rules must not be empty",
                    ));
                }
                for rule in allow.iter().chain(block) {
                    rule.validate()?;
                }
            }
        }
        Ok(())
    }

    /// Classifies a canonical URL using enabled version seeds as context.
    ///
    /// # Errors
    /// Returns an error for invalid policy, invalid URL context, missing
    /// enabled seed context, or unavailable registrable-domain data. Errors
    /// are intentionally fail-closed; no invalid policy becomes `IN_SCOPE`.
    pub fn classify(
        &self,
        candidate: &url::Url,
        seeds: &[Seed],
    ) -> Result<DomainScopeClassification, ProductError> {
        self.validate()?;
        validate_crawl_url(candidate)?;
        let candidate_host = normalized_url_host(candidate)?;
        let seed_hosts = seeds
            .iter()
            .filter(|seed| seed.enabled)
            .map(|seed| {
                validate_crawl_url(&seed.canonical_url)?;
                normalized_url_host(&seed.canonical_url)
            })
            .collect::<Result<BTreeSet<_>, ProductError>>()?;

        match &self.policy {
            DomainScopeKind::SeedDomainsOnly => {
                if seed_hosts.is_empty() {
                    return Err(invalid_scope("Domain Scope requires an enabled seed"));
                }
                if seed_hosts.contains(&candidate_host) {
                    Ok(DomainScopeClassification::in_scope(
                        candidate_host,
                        DomainScopeRationale::SeedHost,
                    ))
                } else {
                    Ok(DomainScopeClassification::external(
                        candidate_host,
                        DomainScopeRationale::OutsideSeedDomains,
                    ))
                }
            }
            DomainScopeKind::SameRegistrableDomain {
                explicit_subdomains,
            } => {
                if seed_hosts.is_empty() {
                    return Err(invalid_scope("Domain Scope requires an enabled seed"));
                }
                let roots = seed_hosts
                    .iter()
                    .map(|host| registrable_domain(host))
                    .collect::<Result<BTreeSet<_>, ProductError>>()?;
                let candidate_root = registrable_domain(&candidate_host)?;
                let explicitly_selected = explicit_subdomains.contains(&candidate_host)
                    && roots
                        .iter()
                        .any(|root| host_is_within_root(&candidate_host, root));
                if explicitly_selected {
                    Ok(DomainScopeClassification::in_scope(
                        candidate_host,
                        DomainScopeRationale::ExplicitSubdomain,
                    ))
                } else if roots.contains(&candidate_root) && candidate_host == candidate_root {
                    Ok(DomainScopeClassification::in_scope(
                        candidate_host,
                        DomainScopeRationale::RegistrableDomain,
                    ))
                } else if seed_hosts.contains(&candidate_host) {
                    Ok(DomainScopeClassification::in_scope(
                        candidate_host,
                        DomainScopeRationale::SeedHost,
                    ))
                } else {
                    Ok(DomainScopeClassification::external(
                        candidate_host,
                        DomainScopeRationale::UnselectedSubdomain,
                    ))
                }
            }
            DomainScopeKind::ExplicitAllowlist { hosts } => {
                if hosts.contains(&candidate_host) {
                    Ok(DomainScopeClassification::in_scope(
                        candidate_host,
                        DomainScopeRationale::ExplicitAllowlist,
                    ))
                } else {
                    Ok(DomainScopeClassification::external(
                        candidate_host,
                        DomainScopeRationale::OutsideAllowlist,
                    ))
                }
            }
            DomainScopeKind::Custom { allow, block } => {
                // Block is evaluated first by contract, regardless of rule
                // insertion order or BTreeSet ordering.
                if block.iter().any(|rule| rule.matches(&candidate_host)) {
                    Ok(DomainScopeClassification::blocked(
                        candidate_host,
                        DomainScopeRationale::ExplicitBlock,
                    ))
                } else if allow.iter().any(|rule| rule.matches(&candidate_host)) {
                    Ok(DomainScopeClassification::in_scope(
                        candidate_host,
                        DomainScopeRationale::CustomAllow,
                    ))
                } else {
                    Ok(DomainScopeClassification::external(
                        candidate_host,
                        DomainScopeRationale::OutsideCustomAllow,
                    ))
                }
            }
        }
    }
}

/// Explainable, preserve-only-aware scope classification.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "classification", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DomainScopeClassification {
    InScope {
        host: String,
        rationale: DomainScopeRationale,
    },
    External {
        host: String,
        rationale: DomainScopeRationale,
    },
    Blocked {
        host: String,
        rationale: DomainScopeRationale,
    },
}

impl DomainScopeClassification {
    fn in_scope(host: String, rationale: DomainScopeRationale) -> Self {
        Self::InScope { host, rationale }
    }

    fn external(host: String, rationale: DomainScopeRationale) -> Self {
        Self::External { host, rationale }
    }

    fn blocked(host: String, rationale: DomainScopeRationale) -> Self {
        Self::Blocked { host, rationale }
    }
}

/// Stable explanation categories for scope decisions.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DomainScopeRationale {
    SeedHost,
    RegistrableDomain,
    ExplicitSubdomain,
    UnselectedSubdomain,
    ExplicitAllowlist,
    OutsideSeedDomains,
    OutsideAllowlist,
    ExplicitBlock,
    CustomAllow,
    OutsideCustomAllow,
}

fn validate_crawl_url(url: &url::Url) -> Result<(), ProductError> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ProductError::with_code(
            ErrorCode::InvalidUrl,
            "the URL is not a valid HTTP(S) crawl target",
        ));
    }
    Ok(())
}

fn host_is_within_root(candidate: &str, root: &str) -> bool {
    candidate == root
        || candidate
            .strip_suffix(root)
            .is_some_and(|rest| rest.ends_with('.'))
}

fn normalized_url_host(url: &url::Url) -> Result<String, ProductError> {
    url.host_str()
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| invalid_scope("the URL host is missing"))
}

fn normalize_rule_host(host: &str) -> Result<String, ProductError> {
    let host = host.trim().to_ascii_lowercase();
    validate_normalized_host(&host)?;
    Ok(host)
}

fn validate_normalized_host(host: &str) -> Result<(), ProductError> {
    if host.is_empty() || host.chars().any(char::is_whitespace) || host.contains('/') {
        return Err(invalid_scope_rule("Domain Scope host is invalid"));
    }
    if host.parse::<IpAddr>().is_ok() {
        return Ok(());
    }
    let parsed = url::Url::parse(&format!("https://{host}/"))
        .map_err(|_| invalid_scope_rule("Domain Scope host is invalid"))?;
    if parsed.host_str().is_none() || parsed.port().is_some() || parsed.host_str() != Some(host) {
        return Err(invalid_scope_rule("Domain Scope host is invalid"));
    }
    Ok(())
}

fn registrable_domain(host: &str) -> Result<String, ProductError> {
    if host.parse::<IpAddr>().is_ok() || !host.contains('.') {
        return Ok(host.to_owned());
    }
    // The crate ships the Public Suffix List data locally; classification is
    // deterministic and never depends on a runtime network lookup.
    psl::domain(host.as_bytes())
        .map(|domain| String::from_utf8_lossy(domain.as_bytes()).into_owned())
        .ok_or_else(|| {
            ProductError::with_code(
                ErrorCode::RegistrableDomainUnavailable,
                "registrable-domain data is unavailable for this host",
            )
        })
}

fn invalid_scope(message: &'static str) -> ProductError {
    ProductError::with_code(ErrorCode::InvalidDomainScope, message)
}

fn invalid_scope_rule(message: &'static str) -> ProductError {
    ProductError::with_code(ErrorCode::InvalidDomainScopeRule, message)
}
