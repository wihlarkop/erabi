use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use reqwest::ClientBuilder;
use tokio::net::lookup_host;
use url::{Host, Url};

/// The maximum time spent resolving one outbound crawler target.
pub const DEFAULT_NETWORK_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(3);

/// The maximum number of DNS answers accepted for one target.
pub const DEFAULT_NETWORK_RESOLUTION_ADDRESS_LIMIT: usize = 16;

pub type NetworkResolutionFuture<'resolver> =
    Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, NetworkResolveError>> + Send + 'resolver>>;

/// The sanitized classes of failure a resolver may report to the policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkResolveError {
    Failed,
    TooManyAddresses,
}

/// A provider-neutral resolver seam used by the runtime policy and deterministic
/// fixtures. Implementations must return all answers observed for the query.
pub trait NetworkResolver: Send + Sync {
    fn resolve<'resolver>(
        &'resolver self,
        host: &str,
        port: u16,
    ) -> NetworkResolutionFuture<'resolver>;
}

#[derive(Clone, Copy, Debug, Default)]
struct SystemNetworkResolver;

impl NetworkResolver for SystemNetworkResolver {
    fn resolve<'resolver>(
        &'resolver self,
        host: &str,
        port: u16,
    ) -> NetworkResolutionFuture<'resolver> {
        let host = host.to_owned();
        Box::pin(async move {
            let mut addresses = Vec::with_capacity(DEFAULT_NETWORK_RESOLUTION_ADDRESS_LIMIT + 1);
            for address in lookup_host((host.as_str(), port))
                .await
                .map_err(|_| NetworkResolveError::Failed)?
            {
                if addresses.len() >= DEFAULT_NETWORK_RESOLUTION_ADDRESS_LIMIT {
                    return Err(NetworkResolveError::TooManyAddresses);
                }
                addresses.push(address);
            }
            Ok(addresses)
        })
    }
}

/// A deterministic resolver for bounded tests and local callers that already
/// own a resolver fixture. It deliberately has no network behavior.
#[derive(Clone, Debug, Default)]
pub struct StaticNetworkResolver {
    answers: BTreeMap<String, Result<Vec<SocketAddr>, NetworkResolveError>>,
}

impl StaticNetworkResolver {
    #[must_use]
    pub fn new(
        answers: impl IntoIterator<Item = (String, Result<Vec<SocketAddr>, NetworkResolveError>)>,
    ) -> Self {
        Self {
            answers: answers.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn single(host: impl Into<String>, address: SocketAddr) -> Self {
        Self::new([(host.into(), Ok(vec![address]))])
    }
}

impl NetworkResolver for StaticNetworkResolver {
    fn resolve<'resolver>(
        &'resolver self,
        host: &str,
        _port: u16,
    ) -> NetworkResolutionFuture<'resolver> {
        let result = self
            .answers
            .get(host)
            .cloned()
            .unwrap_or(Err(NetworkResolveError::Failed));
        Box::pin(async move { result })
    }
}

/// Typed rejection of a target before any outbound HTTP request is attempted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NetworkTargetError {
    #[error("the outbound URL scheme is not allowed")]
    UnsupportedScheme,
    #[error("the outbound URL host is missing")]
    MissingHost,
    #[error("outbound URL credentials are not allowed")]
    CredentialsNotAllowed,
    #[error("outbound URL fragments are not allowed")]
    FragmentNotAllowed,
    #[error("the outbound URL has no effective port")]
    InvalidPort,
    #[error("the literal outbound IP address is not public unicast")]
    ProhibitedLiteralAddress,
    #[error("the outbound hostname could not be resolved")]
    ResolutionFailed,
    #[error("outbound hostname resolution timed out")]
    ResolutionTimedOut,
    #[error("outbound hostname resolution returned no addresses")]
    EmptyResolution,
    #[error("outbound hostname resolution returned too many addresses")]
    TooManyAddresses,
    #[error("outbound hostname resolution included a prohibited address")]
    ProhibitedResolvedAddress,
}

/// Shared runtime policy for outbound crawler target validation and resolution.
#[derive(Clone)]
pub struct NetworkTargetPolicy {
    resolver: Arc<dyn NetworkResolver>,
    resolution_timeout: Duration,
    address_limit: usize,
}

impl std::fmt::Debug for NetworkTargetPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkTargetPolicy")
            .field("resolution_timeout", &self.resolution_timeout)
            .field("address_limit", &self.address_limit)
            .finish_non_exhaustive()
    }
}

impl Default for NetworkTargetPolicy {
    fn default() -> Self {
        Self::with_system_resolver()
    }
}

impl NetworkTargetPolicy {
    #[must_use]
    pub fn with_system_resolver() -> Self {
        Self::new(Arc::new(SystemNetworkResolver))
    }

    #[must_use]
    pub fn new(resolver: Arc<dyn NetworkResolver>) -> Self {
        Self {
            resolver,
            resolution_timeout: DEFAULT_NETWORK_RESOLUTION_TIMEOUT,
            address_limit: DEFAULT_NETWORK_RESOLUTION_ADDRESS_LIMIT,
        }
    }

    #[must_use]
    pub fn with_resolution_timeout(mut self, timeout: Duration) -> Self {
        self.resolution_timeout = timeout.min(DEFAULT_NETWORK_RESOLUTION_TIMEOUT);
        self
    }

    #[must_use]
    pub fn with_address_limit(mut self, limit: usize) -> Self {
        self.address_limit = limit.min(DEFAULT_NETWORK_RESOLUTION_ADDRESS_LIMIT);
        self
    }

    /// Validates URL-level outbound target rules without DNS or network I/O.
    ///
    /// # Errors
    /// Returns a typed structural security rejection.
    pub fn validate_url(&self, url: &Url) -> Result<(), NetworkTargetError> {
        validate_url_shape(url)?;
        let port = url
            .port_or_known_default()
            .ok_or(NetworkTargetError::InvalidPort)?;
        if port == 0 {
            return Err(NetworkTargetError::InvalidPort);
        }
        Ok(())
    }

    /// Validates and resolves one outbound URL before any HTTP client is built.
    ///
    /// Every DNS answer is checked before a target is returned. The returned
    /// target owns the exact normalized addresses that the HTTP client must use.
    ///
    /// # Errors
    /// Returns a typed security or resolution rejection. No request is made by
    /// this method.
    pub async fn validate_and_resolve(
        &self,
        url: &Url,
    ) -> Result<ValidatedNetworkTarget, NetworkTargetError> {
        self.validate_url(url)?;
        let host = url.host_str().ok_or(NetworkTargetError::MissingHost)?;
        let port = url
            .port_or_known_default()
            .ok_or(NetworkTargetError::InvalidPort)?;

        let addresses = match url.host() {
            Some(Host::Ipv4(address)) => {
                if !is_public_unicast(IpAddr::V4(address)) {
                    return Err(NetworkTargetError::ProhibitedLiteralAddress);
                }
                vec![SocketAddr::new(address.into(), port)]
            }
            Some(Host::Ipv6(address)) => {
                if !is_public_unicast(IpAddr::V6(address)) {
                    return Err(NetworkTargetError::ProhibitedLiteralAddress);
                }
                vec![SocketAddr::new(address.into(), port)]
            }
            Some(Host::Domain(_)) => {
                let resolution = tokio::time::timeout(
                    self.resolution_timeout,
                    self.resolver.resolve(host, port),
                )
                .await
                .map_err(|_| NetworkTargetError::ResolutionTimedOut)?
                .map_err(|error| match error {
                    NetworkResolveError::Failed => NetworkTargetError::ResolutionFailed,
                    NetworkResolveError::TooManyAddresses => NetworkTargetError::TooManyAddresses,
                })?;
                normalize_and_validate_addresses(resolution, port, self.address_limit)?
            }
            None => return Err(NetworkTargetError::MissingHost),
        };

        Ok(ValidatedNetworkTarget {
            url: url.clone(),
            host: host.to_owned(),
            port,
            addresses: addresses.into_boxed_slice(),
        })
    }
}

/// A target that has passed the shared outbound network policy.
#[derive(Clone, Eq, PartialEq)]
pub struct ValidatedNetworkTarget {
    url: Url,
    host: String,
    port: u16,
    addresses: Box<[SocketAddr]>,
}

impl std::fmt::Debug for ValidatedNetworkTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValidatedNetworkTarget")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("address_count", &self.addresses.len())
            .finish_non_exhaustive()
    }
}

impl ValidatedNetworkTarget {
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    #[cfg(test)]
    pub(crate) fn for_test(url: Url, address: SocketAddr) -> Self {
        let host = url.host_str().unwrap_or_default().to_owned();
        let port = url.port_or_known_default().unwrap_or(address.port());
        Self {
            url,
            host,
            port,
            addresses: Box::new([SocketAddr::new(address.ip(), port)]),
        }
    }

    /// Configures a direct Reqwest client for this exact validated target.
    ///
    /// `no_proxy` prevents system proxy settings from replacing the direct
    /// validated connection, while `resolve_to_addrs` prevents a second DNS
    /// lookup. The URL hostname remains unchanged for HTTP Host and TLS SNI.
    pub(crate) fn reqwest_builder(&self) -> ClientBuilder {
        reqwest::Client::builder()
            .no_proxy()
            .resolve_to_addrs(&self.host, &self.addresses)
    }
}

fn validate_url_shape(url: &Url) -> Result<(), NetworkTargetError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(NetworkTargetError::UnsupportedScheme);
    }
    if url.host_str().is_none() {
        return Err(NetworkTargetError::MissingHost);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(NetworkTargetError::CredentialsNotAllowed);
    }
    if url.fragment().is_some() {
        return Err(NetworkTargetError::FragmentNotAllowed);
    }
    Ok(())
}

fn normalize_and_validate_addresses(
    addresses: Vec<SocketAddr>,
    port: u16,
    address_limit: usize,
) -> Result<Vec<SocketAddr>, NetworkTargetError> {
    if addresses.is_empty() {
        return Err(NetworkTargetError::EmptyResolution);
    }
    if address_limit == 0 || addresses.len() > address_limit {
        return Err(NetworkTargetError::TooManyAddresses);
    }

    let mut normalized = BTreeSet::new();
    for address in addresses {
        if !is_public_unicast(address.ip()) {
            return Err(NetworkTargetError::ProhibitedResolvedAddress);
        }
        normalized.insert(SocketAddr::new(address.ip(), port));
    }
    if normalized.is_empty() {
        return Err(NetworkTargetError::EmptyResolution);
    }
    Ok(normalized.into_iter().collect())
}

fn is_public_unicast(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    !(first == 0
        || first == 10
        || (first == 100 && (64..=127).contains(&second))
        || first == 127
        || (first == 169 && second == 254)
        || (first == 172 && (16..=31).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || (first == 192 && second == 0 && third == 2)
        || (first == 192 && second == 168)
        || (first == 192 && second == 88 && third == 99)
        || (first == 198 && (18..=19).contains(&second))
        || (first == 198 && second == 51 && third == 100)
        || (first == 203 && second == 0 && third == 113)
        || first >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    !(address.is_unspecified()
        || address.is_loopback()
        || address.is_multicast()
        || matches!(segments, [0, 0, 0, 0, 0, 0, _, _])
        || matches!(segments, [0, 0, 0, 0, 0, 0xffff, _, _])
        || matches!(segments, [0x64, 0xff9b, 0, 0, 0, 0, _, _])
        || matches!(segments, [0x64, 0xff9b, 1, _, _, _, _, _])
        || matches!(segments, [0x100, 0, 0, 0, _, _, _, _])
        || matches!(segments, [0x2001, 0..=0x01ff, _, _, _, _, _, _])
        || matches!(segments, [0x2002, _, _, _, _, _, _, _])
        || matches!(segments, [0x2001, 0xdb8, _, _, _, _, _, _])
        || matches!(segments, [0x3fff, 0..=0x0fff, _, _, _, _, _, _])
        || matches!(segments, [0x5f00, _, _, _, _, _, _, _])
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use super::{
        NetworkResolutionFuture, NetworkResolveError, NetworkResolver, NetworkTargetError,
        NetworkTargetPolicy, StaticNetworkResolver,
    };

    struct PendingResolver;

    impl NetworkResolver for PendingResolver {
        fn resolve<'resolver>(
            &'resolver self,
            _host: &str,
            _port: u16,
        ) -> NetworkResolutionFuture<'resolver> {
            Box::pin(std::future::pending())
        }
    }

    fn target_url(value: &str) -> url::Url {
        match value.parse() {
            Ok(url) => url,
            Err(error) => panic!("test URL must parse: {error}"),
        }
    }

    fn address(value: &str) -> SocketAddr {
        match value.parse() {
            Ok(address) => address,
            Err(error) => panic!("test address must parse: {error}"),
        }
    }

    #[tokio::test]
    async fn rejects_literal_private_and_loopback_addresses() {
        let policy = NetworkTargetPolicy::default();
        for value in [
            "http://127.0.0.1/",
            "http://10.0.0.1/",
            "http://169.254.1.1/",
            "http://100.64.0.1/",
            "http://224.0.0.1/",
            "http://192.0.2.1/",
            "http://[::1]/",
            "http://[fc00::1]/",
            "http://[fe80::1]/",
            "http://[ff02::1]/",
            "http://[::ffff:192.0.2.1]/",
            "http://[::7f00:1]/",
            "http://[64:ff9b::7f00:1]/",
        ] {
            assert_eq!(
                policy.validate_and_resolve(&target_url(value)).await,
                Err(NetworkTargetError::ProhibitedLiteralAddress)
            );
        }
    }

    #[tokio::test]
    async fn rejects_mixed_dns_answers_before_a_target_is_returned() {
        let resolver = StaticNetworkResolver::new([(
            "mixed.example".to_owned(),
            Ok(vec![address("93.184.216.34:443"), address("127.0.0.1:443")]),
        )]);
        let policy = NetworkTargetPolicy::new(Arc::new(resolver));

        assert_eq!(
            policy
                .validate_and_resolve(&target_url("https://mixed.example/path"))
                .await,
            Err(NetworkTargetError::ProhibitedResolvedAddress)
        );
    }

    #[tokio::test]
    async fn normalizes_validated_addresses_deterministically_and_uses_url_port() {
        let resolver = StaticNetworkResolver::new([(
            "example.test".to_owned(),
            Ok(vec![
                address("93.184.216.34:9999"),
                address("93.184.216.35:9999"),
                address("93.184.216.34:9999"),
            ]),
        )]);
        let policy = NetworkTargetPolicy::new(Arc::new(resolver));
        let target = match policy
            .validate_and_resolve(&target_url("https://example.test/path"))
            .await
        {
            Ok(target) => target,
            Err(error) => panic!("valid public fixture target: {error}"),
        };

        assert_eq!(target.host(), "example.test");
        assert_eq!(target.port(), 443);
        assert_eq!(
            target.addresses(),
            [address("93.184.216.34:443"), address("93.184.216.35:443"),]
        );
    }

    #[tokio::test]
    async fn accepts_public_ipv4_and_ipv6_literals_without_dns() {
        let policy = NetworkTargetPolicy::default();
        for value in [
            "https://93.184.216.34/path",
            "https://[2001:4860:4860::8888]/path",
        ] {
            let target = policy.validate_and_resolve(&target_url(value)).await;
            assert!(target.is_ok(), "public literal should be accepted: {value}");
        }
    }

    #[tokio::test]
    async fn rejects_too_many_dns_answers_before_connection() {
        let addresses = (1_u8..=4)
            .map(|last| SocketAddr::new([93, 184, 216, last].into(), 443))
            .collect::<Vec<_>>();
        let resolver = StaticNetworkResolver::new([("many.example".to_owned(), Ok(addresses))]);
        let policy = NetworkTargetPolicy::new(Arc::new(resolver)).with_address_limit(3);

        assert_eq!(
            policy
                .validate_and_resolve(&target_url("https://many.example/"))
                .await,
            Err(NetworkTargetError::TooManyAddresses)
        );
    }

    #[tokio::test]
    async fn rejects_resolution_failure_and_empty_answers() {
        let failed = NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::new([(
            "failed.example".to_owned(),
            Err(NetworkResolveError::Failed),
        )])));
        assert_eq!(
            failed
                .validate_and_resolve(&target_url("https://failed.example/"))
                .await,
            Err(NetworkTargetError::ResolutionFailed)
        );

        let empty = NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::new([(
            "empty.example".to_owned(),
            Ok(Vec::new()),
        )])));
        assert_eq!(
            empty
                .validate_and_resolve(&target_url("https://empty.example/"))
                .await,
            Err(NetworkTargetError::EmptyResolution)
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_schemes_credentials_and_fragments_before_resolution() {
        let policy = NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::new([(
            "example.test".to_owned(),
            Ok(vec![address("93.184.216.34:443")]),
        )])));
        for (value, expected) in [
            (
                "ftp://example.test/file",
                NetworkTargetError::UnsupportedScheme,
            ),
            (
                "https://user:password@example.test/file",
                NetworkTargetError::CredentialsNotAllowed,
            ),
            (
                "https://example.test/file#fragment",
                NetworkTargetError::FragmentNotAllowed,
            ),
            (
                "https://example.test:0/file",
                NetworkTargetError::InvalidPort,
            ),
        ] {
            assert_eq!(
                policy.validate_and_resolve(&target_url(value)).await,
                Err(expected)
            );
        }
    }

    #[tokio::test]
    async fn applies_explicit_resolution_timeout() {
        let resolver = PendingResolver;
        let policy =
            NetworkTargetPolicy::new(Arc::new(resolver)).with_resolution_timeout(Duration::ZERO);

        assert_eq!(
            policy
                .validate_and_resolve(&target_url("https://example.test/"))
                .await,
            Err(NetworkTargetError::ResolutionTimedOut)
        );
    }
}
