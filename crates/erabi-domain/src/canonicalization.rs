use std::collections::BTreeSet;

use crate::{ErrorCode, ProductError};

/// The currently supported canonicalization policy representation.
pub const CANONICALIZATION_POLICY_VERSION: u16 = 1;

/// A versioned, deliberately small URL canonicalization policy.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalizationPolicy {
    pub version: u16,
    #[serde(default)]
    pub explicit_keep_parameters: BTreeSet<String>,
    #[serde(default)]
    pub explicit_drop_parameters: BTreeSet<String>,
}

impl Default for CanonicalizationPolicy {
    fn default() -> Self {
        Self {
            version: CANONICALIZATION_POLICY_VERSION,
            explicit_keep_parameters: BTreeSet::new(),
            explicit_drop_parameters: BTreeSet::new(),
        }
    }
}

impl CanonicalizationPolicy {
    /// Constructs and validates a policy from explicit exact parameter rules.
    ///
    /// # Errors
    /// Returns an invalid-policy error when a rule is blank, unsafe to use as
    /// a raw query key, or appears in both sets.
    pub fn new(
        explicit_keep_parameters: BTreeSet<String>,
        explicit_drop_parameters: BTreeSet<String>,
    ) -> Result<Self, ProductError> {
        let policy = Self {
            version: CANONICALIZATION_POLICY_VERSION,
            explicit_keep_parameters,
            explicit_drop_parameters,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Validates the bounded MVP policy contract.
    ///
    /// # Errors
    /// Returns a typed invalid-policy error. Explicit KEEP/DROP overlap is
    /// rejected rather than being resolved by set or iteration order.
    pub fn validate(&self) -> Result<(), ProductError> {
        if self.version != CANONICALIZATION_POLICY_VERSION {
            return Err(invalid_policy(
                "unsupported canonicalization policy version",
            ));
        }
        for parameter in self
            .explicit_keep_parameters
            .iter()
            .chain(&self.explicit_drop_parameters)
        {
            if !valid_parameter_name(parameter) {
                return Err(invalid_policy("canonicalization parameter rule is invalid"));
            }
        }
        if self
            .explicit_keep_parameters
            .intersection(&self.explicit_drop_parameters)
            .next()
            .is_some()
        {
            return Err(invalid_policy(
                "canonicalization KEEP and DROP rules overlap",
            ));
        }
        Ok(())
    }

    /// Canonicalizes one raw URL and retains provenance plus an explanation.
    ///
    /// # Errors
    /// Returns a sanitized invalid-URL, unsupported-scheme, or policy error.
    pub fn canonicalize(&self, original_url: &str) -> Result<CanonicalizationResult, ProductError> {
        self.validate()?;
        let mut url = url::Url::parse(original_url)
            .map_err(|_| ProductError::with_code(ErrorCode::InvalidUrl, "the URL is invalid"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ProductError::with_code(
                ErrorCode::UnsupportedUrlScheme,
                "only HTTP and HTTPS URLs are supported",
            ));
        }
        if url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() {
            return Err(ProductError::with_code(
                ErrorCode::InvalidUrl,
                "the URL is not a valid crawl target",
            ));
        }

        let mut decisions = Vec::new();
        let scheme = url.scheme().to_ascii_lowercase();
        let scheme_was_normalized =
            original_scheme(original_url).is_some_and(|original| original != scheme);
        if scheme_was_normalized {
            url.set_scheme(&scheme).map_err(|()| {
                ProductError::with_code(ErrorCode::InvalidUrl, "the URL scheme is invalid")
            })?;
            decisions.push(CanonicalizationDecision::SchemeNormalized);
        }

        let host = url.host_str().ok_or_else(|| {
            ProductError::with_code(ErrorCode::InvalidUrl, "the URL host is missing")
        })?;
        let normalized_host = host.to_ascii_lowercase();
        let host_was_normalized = normalized_host != host
            || original_host(original_url).is_some_and(|original| {
                original
                    .chars()
                    .any(|character| character.is_ascii_uppercase())
            });
        if host_was_normalized {
            url.set_host(Some(&normalized_host)).map_err(|_| {
                ProductError::with_code(ErrorCode::InvalidUrl, "the URL host is invalid")
            })?;
            decisions.push(CanonicalizationDecision::HostNormalized);
        }

        let default_port_was_removed = original_default_port(original_url, &scheme);
        if default_port_was_removed
            || url.port().is_some_and(|port| {
                (url.scheme() == "http" && port == 80) || (url.scheme() == "https" && port == 443)
            })
        {
            if url.port().is_some() {
                url.set_port(None).map_err(|()| {
                    ProductError::with_code(ErrorCode::InvalidUrl, "the URL port is invalid")
                })?;
            }
            decisions.push(CanonicalizationDecision::DefaultPortRemoved);
        }

        if url.fragment().is_some() {
            url.set_fragment(None);
            decisions.push(CanonicalizationDecision::FragmentRemoved);
        }

        if url.path().is_empty() {
            // The MVP rule is intentionally conservative: only an empty path
            // is rewritten. Repeated and non-root trailing slashes remain
            // content-significant and are preserved.
            url.set_path("/");
            decisions.push(CanonicalizationDecision::PathNormalized);
        }

        let had_query = url.query().is_some();
        let raw_query = url.query().unwrap_or_default();
        let original_pairs = raw_query
            .split('&')
            .map(RawQueryPair::new)
            .collect::<Vec<_>>();
        let mut sorted_pairs = original_pairs.clone();
        sorted_pairs.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        if sorted_pairs != original_pairs {
            decisions.push(CanonicalizationDecision::QuerySorted);
        }

        let mut retained_pairs = Vec::with_capacity(sorted_pairs.len());
        for pair in sorted_pairs {
            let parameter = pair.parameter_name();
            let tracking = is_default_tracking_parameter(&parameter);
            if self.explicit_keep_parameters.contains(&parameter) {
                decisions.push(CanonicalizationDecision::ExplicitParameterKept { parameter });
                retained_pairs.push(pair.raw);
            } else if self.explicit_drop_parameters.contains(&parameter) {
                decisions.push(CanonicalizationDecision::CustomParameterDropped { parameter });
            } else if tracking {
                decisions.push(CanonicalizationDecision::TrackingParameterRemoved { parameter });
            } else {
                retained_pairs.push(pair.raw);
            }
        }

        if !had_query || (raw_query.is_empty() && retained_pairs.is_empty()) {
            url.set_query(None);
        } else if retained_pairs.is_empty() {
            // A query made solely of removable tracking parameters has no
            // remaining semantic pairs and therefore has no query identity.
            url.set_query(None);
        } else {
            url.set_query(Some(&retained_pairs.join("&")));
        }

        Ok(CanonicalizationResult {
            original_url: original_url.to_owned(),
            canonical_url: url,
            decisions,
        })
    }
}

/// The explainable result of canonicalizing one URL.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CanonicalizationResult {
    pub original_url: String,
    pub canonical_url: url::Url,
    pub decisions: Vec<CanonicalizationDecision>,
}

/// Stable, useful canonicalization actions for later Test Lab consumers.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "code", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CanonicalizationDecision {
    SchemeNormalized,
    HostNormalized,
    DefaultPortRemoved,
    FragmentRemoved,
    PathNormalized,
    QuerySorted,
    TrackingParameterRemoved { parameter: String },
    CustomParameterDropped { parameter: String },
    ExplicitParameterKept { parameter: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawQueryPair {
    raw: String,
    parameter: String,
}

fn original_scheme(original_url: &str) -> Option<&str> {
    original_url.split_once("://").map(|(scheme, _)| scheme)
}

fn original_authority(original_url: &str) -> Option<&str> {
    let authority = original_url.split_once("://")?.1;
    let end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    Some(&authority[..end])
}

fn original_host(original_url: &str) -> Option<&str> {
    let authority = original_authority(original_url)?;
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if let Some(end) = authority.find(']') {
        return authority.get(1..end);
    }
    authority
        .rsplit_once(':')
        .map_or(Some(authority), |(host, port)| {
            port.parse::<u16>().ok().map(|_| host)
        })
}

fn original_default_port(original_url: &str, scheme: &str) -> bool {
    let Some(authority) = original_authority(original_url) else {
        return false;
    };
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let port = if let Some(end) = authority.find(']') {
        authority
            .get(end + 1..)
            .and_then(|suffix| suffix.strip_prefix(':'))
    } else {
        authority.rsplit_once(':').map(|(_, port)| port)
    };
    matches!(
        (scheme, port.and_then(|port| port.parse::<u16>().ok())),
        ("http", Some(80)) | ("https", Some(443))
    )
}

impl RawQueryPair {
    fn new(raw: &str) -> Self {
        let parameter = url::form_urlencoded::parse(raw.as_bytes())
            .next()
            .map_or_else(
                || raw.split('=').next().unwrap_or_default().to_owned(),
                |pair| pair.0.into_owned(),
            );
        Self {
            raw: raw.to_owned(),
            parameter,
        }
    }

    fn parameter_name(&self) -> String {
        self.parameter.clone()
    }

    fn sort_key(&self) -> (&str, &str) {
        (&self.parameter, &self.raw)
    }
}

fn is_default_tracking_parameter(parameter: &str) -> bool {
    parameter.starts_with("utm_") || matches!(parameter, "fbclid" | "gclid")
}

fn valid_parameter_name(parameter: &str) -> bool {
    !parameter.is_empty()
        && parameter
            .chars()
            .all(|character| !character.is_control() && !matches!(character, '&' | '=' | '#'))
}

fn invalid_policy(message: &'static str) -> ProductError {
    ProductError::with_code(ErrorCode::InvalidCanonicalizationPolicy, message)
}
