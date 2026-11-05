//! The route table: hostname to backend addresses.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

/// How a hostname gets TLS (`desired_state.spec.routes[].tls_mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TlsMode {
    /// The public proxy obtains a certificate through ACME DNS-01.
    Acme,
    /// TLS terminates at the tunnel provider; the node sees plain HTTP from
    /// `cloudflared` on loopback.
    Tunnel,
}

/// When a failed backend request may be sent to another backend.
///
/// The distinction is what keeps a non-idempotent request from being executed
/// twice: a node route may only move on while the body has not started to
/// stream, and an edge route retries `GET` and `HEAD` once, on another backend,
/// after a connect failure, a reset before the response, or its backend
/// leaving the rotation before it answered (`node-agent-protocol.md` section
/// 6.8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RetryPolicy {
    /// Try the next backend on a failure while the request body is unsent.
    /// This is the node's behaviour and the default.
    #[default]
    Unsent,
    /// Retry once, on another backend, only for `GET` and `HEAD`, and only
    /// when no response came: the connection failed or was reset, or the
    /// backend left the rotation. This is an edge route's policy.
    IdempotentOnce,
}

/// One hostname this node or edge serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    /// Lowercase DNS name, no trailing dot, no port, no wildcard.
    pub hostname: String,
    /// Candidate backends (one per app replica on this node, or one per
    /// instance of an edge route): the VM's address and the app port.
    /// Requests go round robin; a backend that refuses the connection may be
    /// skipped for that request according to [`Self::retry`].
    pub backends: Vec<SocketAddr>,
    /// How TLS terminates for this hostname.
    pub tls: TlsMode,
    /// When a failed request may try another backend.
    pub retry: RetryPolicy,
    /// When set, a backend that does not send response headers within this
    /// time fails the request with `502`. Edge routes use 60 s; node routes
    /// leave it unset.
    pub response_timeout: Option<Duration>,
}

/// The complete set of routes of a node, keyed by hostname.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteTable {
    routes: BTreeMap<String, Route>,
}

impl RouteTable {
    /// Builds a table, normalising hostnames to lowercase.
    ///
    /// A route without backends is refused when its retry policy is
    /// [`RetryPolicy::Unsent`]; an edge route is allowed to be empty so it can
    /// answer `502` while no instance is in rotation.
    ///
    /// # Errors
    ///
    /// A message naming the first invalid hostname, a duplicate hostname, or a
    /// route that must have backends but has none.
    pub fn new(routes: impl IntoIterator<Item = Route>) -> Result<Self, String> {
        let mut map = BTreeMap::new();
        for mut route in routes {
            route.hostname = route.hostname.to_ascii_lowercase();
            if !valid_hostname(&route.hostname) {
                return Err(format!("invalid hostname {:?}", route.hostname));
            }
            if route.backends.is_empty() && route.retry == RetryPolicy::Unsent {
                return Err(format!("route {} has no backends", route.hostname));
            }
            if map.contains_key(&route.hostname) {
                return Err(format!("duplicate hostname {}", route.hostname));
            }
            map.insert(route.hostname.clone(), route);
        }
        Ok(Self { routes: map })
    }

    /// The route for a request's host (already lowercased, port stripped).
    #[must_use]
    pub fn get(&self, hostname: &str) -> Option<&Route> {
        self.routes.get(hostname)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Route> {
        self.routes.values()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

/// RFC 1123 host name: 1-253 characters, labels of 1-63 `[a-z0-9-]` that do
/// not start or end with `-`, at least two labels. Lowercase only.
#[must_use]
pub fn valid_hostname(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 || name.ends_with('.') {
        return false;
    }
    let mut labels = 0;
    for label in name.split('.') {
        labels += 1;
        let bytes = label.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 63
            || bytes[0] == b'-'
            || bytes[bytes.len() - 1] == b'-'
            || !bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        {
            return false;
        }
    }
    labels >= 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(host: &str) -> Route {
        Route {
            hostname: host.to_string(),
            backends: vec!["10.42.0.2:8080".parse().unwrap()],
            tls: TlsMode::Acme,
            retry: RetryPolicy::Unsent,
            response_timeout: None,
        }
    }

    #[test]
    fn hostnames_are_validated_and_lowercased() {
        let table = RouteTable::new([route("App.Example.com")]).unwrap();
        assert!(table.get("app.example.com").is_some());
        for bad in [
            "",
            "localhost",
            "-a.example.com",
            "a-.example.com",
            "a..example.com",
            "*.example.com",
            "a.example.com.",
            "a_b.example.com",
            "a.example.com:443",
        ] {
            assert!(RouteTable::new([route(bad)]).is_err(), "{bad}");
        }
    }

    #[test]
    fn duplicates_and_empty_backends_are_refused() {
        assert!(RouteTable::new([route("a.example.com"), route("A.example.com")]).is_err());
        let mut empty = route("a.example.com");
        empty.backends.clear();
        assert!(RouteTable::new([empty]).is_err());
    }

    #[test]
    fn an_edge_route_may_have_no_backends() {
        let mut edge = route("a.example.com");
        edge.backends.clear();
        edge.retry = RetryPolicy::IdempotentOnce;
        edge.response_timeout = Some(Duration::from_secs(60));
        let table = RouteTable::new([edge]).unwrap();
        let route = table.get("a.example.com").unwrap();
        assert!(route.backends.is_empty());
        assert_eq!(route.retry, RetryPolicy::IdempotentOnce);
        assert_eq!(route.response_timeout, Some(Duration::from_secs(60)));
    }

    #[test]
    fn the_default_retry_policy_is_unsent() {
        assert_eq!(RetryPolicy::default(), RetryPolicy::Unsent);
    }
}
