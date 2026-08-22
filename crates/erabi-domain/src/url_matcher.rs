use crate::ProductError;
use std::{cmp::Reverse, collections::BTreeMap};

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum UrlMatcherKind {
    ExactUrl,
    ExactHostPathTemplate,
    PathPrefixOrGlob,
    Regex,
}
impl UrlMatcherKind {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::ExactUrl => 4,
            Self::ExactHostPathTemplate => 3,
            Self::PathPrefixOrGlob => 2,
            Self::Regex => 1,
        }
    }
}
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct SpecificityKey {
    pub matcher_kind_rank: u8,
    pub literal_path_segments: u32,
    pub explicit_query_constraints: u32,
    pub literal_characters: u32,
    pub inverse_wildcards: Reverse<u32>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum UrlMatcher {
    ExactUrl {
        url: url::Url,
    },
    ExactHostPathTemplate {
        host: String,
        path_template: String,
        query: BTreeMap<String, String>,
    },
    PathPrefix {
        host: Option<String>,
        prefix: String,
    },
    PathGlob {
        host: Option<String>,
        pattern: String,
    },
    Regex {
        pattern: String,
    },
}
impl UrlMatcher {
    #[must_use]
    pub fn exact_url(url: url::Url) -> Self {
        Self::ExactUrl { url }
    }
    #[must_use]
    pub fn exact_host_path_template(
        host: impl Into<String>,
        path_template: impl Into<String>,
        query: BTreeMap<String, String>,
    ) -> Self {
        Self::ExactHostPathTemplate {
            host: host.into(),
            path_template: path_template.into(),
            query,
        }
    }
    #[must_use]
    pub fn path_prefix(host: Option<String>, prefix: impl Into<String>) -> Self {
        Self::PathPrefix {
            host,
            prefix: prefix.into(),
        }
    }
    /// Creates a non-empty path glob matcher.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the glob is empty.
    pub fn path_glob(
        host: Option<String>,
        pattern: impl Into<String>,
    ) -> Result<Self, ProductError> {
        let pattern = pattern.into();
        if pattern.is_empty() {
            return Err(ProductError::conflict("URL glob cannot be empty"));
        }
        Ok(Self::PathGlob { host, pattern })
    }
    /// Creates a validated regular-expression matcher.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the expression is invalid.
    pub fn regex(pattern: impl Into<String>) -> Result<Self, ProductError> {
        let pattern = pattern.into();
        regex::Regex::new(&pattern)
            .map_err(|_| ProductError::conflict("invalid URL matcher regex"))?;
        Ok(Self::Regex { pattern })
    }
    #[must_use]
    pub const fn kind(&self) -> UrlMatcherKind {
        match self {
            Self::ExactUrl { .. } => UrlMatcherKind::ExactUrl,
            Self::ExactHostPathTemplate { .. } => UrlMatcherKind::ExactHostPathTemplate,
            Self::PathPrefix { .. } | Self::PathGlob { .. } => UrlMatcherKind::PathPrefixOrGlob,
            Self::Regex { .. } => UrlMatcherKind::Regex,
        }
    }
    #[must_use]
    pub fn pattern(&self) -> String {
        match self {
            Self::ExactUrl { url } => url.as_str().to_owned(),
            Self::ExactHostPathTemplate {
                host,
                path_template,
                ..
            } => format!("{host}{path_template}"),
            Self::PathPrefix { prefix, .. } => prefix.clone(),
            Self::PathGlob { pattern, .. } | Self::Regex { pattern } => pattern.clone(),
        }
    }
    #[must_use]
    pub fn specificity(&self) -> SpecificityKey {
        let path = match self {
            Self::ExactUrl { url } => url.path(),
            Self::ExactHostPathTemplate { path_template, .. } => path_template,
            Self::PathPrefix { prefix, .. } => prefix,
            Self::PathGlob { pattern, .. } | Self::Regex { pattern } => pattern,
        };
        let wildcard_count =
            path.matches('*').count() + path.matches('{').count() + path.matches('(').count();
        SpecificityKey {
            matcher_kind_rank: self.kind().rank(),
            literal_path_segments: bounded_count(
                path.split('/')
                    .filter(|part| {
                        !part.is_empty()
                            && !part.contains('*')
                            && !part.contains('{')
                            && !part.contains('(')
                    })
                    .count(),
            ),
            explicit_query_constraints: match self {
                Self::ExactUrl { url } => bounded_count(url.query_pairs().count()),
                Self::ExactHostPathTemplate { query, .. } => bounded_count(query.len()),
                _ => 0,
            },
            literal_characters: bounded_count(
                path.chars().filter(char::is_ascii_alphanumeric).count(),
            ),
            inverse_wildcards: Reverse(bounded_count(wildcard_count)),
        }
    }
    #[must_use]
    pub fn matches(&self, url: &url::Url) -> bool {
        match self {
            Self::ExactUrl { url: expected } => expected == url,
            Self::ExactHostPathTemplate {
                host,
                path_template,
                query,
            } => {
                url.host_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(host))
                    && template_matches(path_template, url.path())
                    && query.iter().all(|(key, value)| {
                        url.query_pairs().any(|(actual_key, actual_value)| {
                            actual_key == key.as_str() && actual_value == value.as_str()
                        })
                    })
            }
            Self::PathPrefix { host, prefix } => {
                host.as_ref().is_none_or(|expected| {
                    url.host_str()
                        .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
                }) && url.path().starts_with(prefix)
            }
            Self::PathGlob { host, pattern } => {
                host.as_ref().is_none_or(|expected| {
                    url.host_str()
                        .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
                }) && glob_matches(pattern, url.path())
            }
            Self::Regex { pattern } => {
                regex::Regex::new(pattern).is_ok_and(|expression| expression.is_match(url.as_str()))
            }
        }
    }
}
fn bounded_count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
fn template_matches(template: &str, actual: &str) -> bool {
    let expected: Vec<_> = template
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    let actual: Vec<_> = actual.split('/').filter(|part| !part.is_empty()).collect();
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(left, right)| (left.starts_with('{') && left.ends_with('}')) || *left == right)
}
fn glob_matches(pattern: &str, actual: &str) -> bool {
    let escaped = regex::escape(pattern).replace(r"\*", ".*");
    regex::Regex::new(&format!("^{escaped}$")).is_ok_and(|expression| expression.is_match(actual))
}
