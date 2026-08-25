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

impl SpecificityKey {
    /// Returns the user-facing wildcard/capture count represented by the
    /// inverse value used for lexicographic resolution.
    #[must_use]
    pub const fn wildcard_capture_count(self) -> u32 {
        self.inverse_wildcards.0
    }
}

/// A typed, non-serde view of a validated URL matcher definition.
///
/// The private serde representation remains an implementation detail of the
/// durable domain contract. API layers can use this view to expose their own
/// stable transport representation without leaking that serde layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UrlMatcherDefinition {
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum Definition {
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
#[derive(Clone, Debug)]
pub struct UrlMatcher {
    definition: Definition,
}
impl serde::Serialize for UrlMatcher {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.definition.serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for UrlMatcher {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let definition = Definition::deserialize(deserializer)?;
        Self::validated(definition).map_err(serde::de::Error::custom)
    }
}
impl UrlMatcher {
    fn validated(definition: Definition) -> Result<Self, ProductError> {
        let matcher = Self { definition };
        matcher.validate_definition()?;
        Ok(matcher)
    }
    #[must_use]
    pub fn exact_url(url: url::Url) -> Self {
        Self {
            definition: Definition::ExactUrl { url },
        }
    }
    #[must_use]
    pub fn exact_host_path_template(
        host: impl Into<String>,
        path_template: impl Into<String>,
        query: BTreeMap<String, String>,
    ) -> Self {
        Self {
            definition: Definition::ExactHostPathTemplate {
                host: host.into(),
                path_template: path_template.into(),
                query,
            },
        }
    }
    #[must_use]
    pub fn path_prefix(host: Option<String>, prefix: impl Into<String>) -> Self {
        Self {
            definition: Definition::PathPrefix {
                host,
                prefix: prefix.into(),
            },
        }
    }
    /// Creates a validated path-prefix matcher.
    ///
    /// # Errors
    /// Returns a conflict when the optional host or prefix violates the
    /// matcher-definition contract.
    pub fn try_path_prefix(
        host: Option<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, ProductError> {
        Self::validated(Definition::PathPrefix {
            host,
            prefix: prefix.into(),
        })
    }
    /// Creates a validated, non-empty path glob matcher.
    ///
    /// # Errors
    /// Returns a conflict when the glob is empty.
    pub fn path_glob(
        host: Option<String>,
        pattern: impl Into<String>,
    ) -> Result<Self, ProductError> {
        Self::validated(Definition::PathGlob {
            host,
            pattern: pattern.into(),
        })
    }
    /// Creates a validated regular-expression matcher.
    ///
    /// # Errors
    /// Returns a conflict when the expression is invalid.
    pub fn regex(pattern: impl Into<String>) -> Result<Self, ProductError> {
        Self::validated(Definition::Regex {
            pattern: pattern.into(),
        })
    }
    /// Validates the structural matcher-definition contract.
    ///
    /// This is the authoritative Task 2 validity boundary. It is used by
    /// validated construction, serde deserialization, and repository writes
    /// so a definition cannot be accepted by one layer and rejected by
    /// another.
    ///
    /// # Errors
    /// Returns a conflict when the matcher definition is malformed.
    pub fn validate_definition(&self) -> Result<(), ProductError> {
        match &self.definition {
            Definition::ExactUrl { .. } => Ok(()),
            Definition::ExactHostPathTemplate {
                host,
                path_template,
                query,
            } if valid_host(host) && valid_path_template(path_template) && valid_query(query) => {
                Ok(())
            }
            Definition::ExactHostPathTemplate { .. } => Err(ProductError::conflict(
                "invalid exact host/path template matcher",
            )),
            Definition::PathPrefix { host, prefix }
                if valid_optional_host(host.as_deref()) && valid_path_prefix(prefix) =>
            {
                Ok(())
            }
            Definition::PathPrefix { .. } => {
                Err(ProductError::conflict("invalid URL path prefix matcher"))
            }
            Definition::PathGlob { host, pattern }
                if valid_optional_host(host.as_deref()) && valid_path_glob(pattern) =>
            {
                Ok(())
            }
            Definition::PathGlob { .. } => {
                Err(ProductError::conflict("invalid URL path glob matcher"))
            }
            Definition::Regex { pattern } => regex::Regex::new(pattern)
                .map(|_| ())
                .map_err(|_| ProductError::conflict("invalid URL matcher regex")),
        }
    }
    #[must_use]
    pub const fn kind(&self) -> UrlMatcherKind {
        match self.definition {
            Definition::ExactUrl { .. } => UrlMatcherKind::ExactUrl,
            Definition::ExactHostPathTemplate { .. } => UrlMatcherKind::ExactHostPathTemplate,
            Definition::PathPrefix { .. } | Definition::PathGlob { .. } => {
                UrlMatcherKind::PathPrefixOrGlob
            }
            Definition::Regex { .. } => UrlMatcherKind::Regex,
        }
    }
    /// Returns a typed view of the matcher without exposing its serde shape.
    #[must_use]
    pub fn definition(&self) -> UrlMatcherDefinition {
        match &self.definition {
            Definition::ExactUrl { url } => UrlMatcherDefinition::ExactUrl { url: url.clone() },
            Definition::ExactHostPathTemplate {
                host,
                path_template,
                query,
            } => UrlMatcherDefinition::ExactHostPathTemplate {
                host: host.clone(),
                path_template: path_template.clone(),
                query: query.clone(),
            },
            Definition::PathPrefix { host, prefix } => UrlMatcherDefinition::PathPrefix {
                host: host.clone(),
                prefix: prefix.clone(),
            },
            Definition::PathGlob { host, pattern } => UrlMatcherDefinition::PathGlob {
                host: host.clone(),
                pattern: pattern.clone(),
            },
            Definition::Regex { pattern } => UrlMatcherDefinition::Regex {
                pattern: pattern.clone(),
            },
        }
    }
    /// Creates a validated exact-host path-template matcher.
    ///
    /// This is the preferred constructor for new authoring code. The legacy
    /// infallible constructor remains available for compatibility; repository
    /// persistence and serde still validate every definition.
    ///
    /// # Errors
    /// Returns a conflict when the host, path template, or query keys are
    /// structurally invalid.
    pub fn try_exact_host_path_template(
        host: impl Into<String>,
        path_template: impl Into<String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, ProductError> {
        Self::validated(Definition::ExactHostPathTemplate {
            host: host.into(),
            path_template: path_template.into(),
            query,
        })
    }
    #[must_use]
    pub fn pattern(&self) -> String {
        match &self.definition {
            Definition::ExactUrl { url } => url.as_str().to_owned(),
            Definition::ExactHostPathTemplate {
                host,
                path_template,
                ..
            } => format!("{host}{path_template}"),
            Definition::PathPrefix { prefix, .. } => prefix.clone(),
            Definition::PathGlob { pattern, .. } | Definition::Regex { pattern } => pattern.clone(),
        }
    }
    #[must_use]
    pub fn specificity(&self) -> SpecificityKey {
        let path = match &self.definition {
            Definition::ExactUrl { url } => url.path(),
            Definition::ExactHostPathTemplate { path_template, .. } => path_template,
            Definition::PathPrefix { prefix, .. } => prefix,
            Definition::PathGlob { pattern, .. } | Definition::Regex { pattern } => pattern,
        };
        let wildcards =
            path.matches('*').count() + path.matches('{').count() + path.matches('(').count();
        SpecificityKey {
            matcher_kind_rank: self.kind().rank(),
            literal_path_segments: count(
                path.split('/')
                    .filter(|part| {
                        !part.is_empty()
                            && !part.contains('*')
                            && !part.contains('{')
                            && !part.contains('(')
                    })
                    .count(),
            ),
            explicit_query_constraints: match &self.definition {
                Definition::ExactUrl { url } => count(url.query_pairs().count()),
                Definition::ExactHostPathTemplate { query, .. } => count(query.len()),
                _ => 0,
            },
            literal_characters: count(path.chars().filter(char::is_ascii_alphanumeric).count()),
            inverse_wildcards: Reverse(count(wildcards)),
        }
    }
    #[must_use]
    pub fn matches(&self, url: &url::Url) -> bool {
        match &self.definition {
            Definition::ExactUrl { url: expected } => expected == url,
            Definition::ExactHostPathTemplate {
                host,
                path_template,
                query,
            } => {
                url.host_str()
                    .is_some_and(|actual| actual.eq_ignore_ascii_case(host))
                    && template_matches(path_template, url.path())
                    && query.iter().all(|(key, value)| {
                        url.query_pairs().any(|(actual_key, actual_value)| {
                            actual_key == key.as_str() && actual_value == value.as_str()
                        })
                    })
            }
            Definition::PathPrefix { host, prefix } => {
                host.as_ref().is_none_or(|expected| {
                    url.host_str()
                        .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
                }) && url.path().starts_with(prefix)
            }
            Definition::PathGlob { host, pattern } => {
                host.as_ref().is_none_or(|expected| {
                    url.host_str()
                        .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
                }) && glob_matches(pattern, url.path())
            }
            Definition::Regex { pattern } => {
                regex::Regex::new(pattern).is_ok_and(|expression| expression.is_match(url.as_str()))
            }
        }
    }
}
fn count(value: usize) -> u32 {
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

fn valid_host(host: &str) -> bool {
    if host.trim().is_empty() || host.chars().any(char::is_whitespace) {
        return false;
    }
    let Ok(parsed) = url::Url::parse(&format!("https://{host}/")) else {
        return false;
    };
    parsed.port().is_none()
        && parsed
            .host_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(host))
}

fn valid_optional_host(host: Option<&str>) -> bool {
    host.is_none_or(valid_host)
}

fn valid_path_template(path: &str) -> bool {
    !path.is_empty()
        && path.starts_with('/')
        && path.split('/').skip(1).all(|segment| {
            if segment.starts_with('{') || segment.ends_with('}') {
                segment.len() > 2
                    && segment.starts_with('{')
                    && segment.ends_with('}')
                    && !segment[1..segment.len() - 1]
                        .chars()
                        .any(|character| matches!(character, '{' | '}' | '/' | '*'))
            } else {
                !segment.contains(['{', '}', '*'])
            }
        })
}

fn valid_path_prefix(path: &str) -> bool {
    !path.is_empty() && path.starts_with('/') && !path.contains(['*', '{', '}'])
}

fn valid_path_glob(path: &str) -> bool {
    !path.is_empty() && path.starts_with('/')
}

fn valid_query(query: &BTreeMap<String, String>) -> bool {
    query
        .keys()
        .all(|key| !key.is_empty() && !key.chars().any(char::is_whitespace))
}
