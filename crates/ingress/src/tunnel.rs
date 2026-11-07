//! The Cloudflare Tunnel backend for hosts behind NAT (ADR 0024, 0055, 0094).
//!
//! The backend is split along the trust boundary of ADR 0094: the account
//! credential never leaves the primary.
//!
//! - [`TunnelController`] runs on the primary and holds the account's
//!   [`Cloudflare`] client. It creates or looks up each node's tunnel
//!   (`quoro-<node>`, one tunnel per node, never replicas of one tunnel across
//!   nodes: architecture.md section 10), writes the tunnel's ingress rules and
//!   the proxied CNAMEs that point each hostname at it.
//! - [`TunnelIngress`] runs on the node and holds no Cloudflare client, account
//!   id or zone id. It is given only the tunnel id, the tunnel's run token and
//!   an `origin`, binds the loopback proxy and runs `cloudflared` with the
//!   token. Every hostname the proxy serves is forwarded to it, so a route
//!   change never restarts `cloudflared`.
//!
//! TLS terminates at Cloudflare; the node holds no certificate for these
//! names. The node reports a hostname ready once the connector is up and the
//! hostname is in its proxy table; the DNS record and the tunnel ingress rule
//! are the primary's job.

use crate::cloudflared::{Cloudflared, CloudflaredConfig};
use crate::proxy::{self, ListenerMode, ProxyCore};
use crate::{Ingress, IngressError, IngressEvent, RouteTable, TlsMode};
use quoro_providers::DnsProvider;
use quoro_providers::Token;
use quoro_providers::cloudflare::{Cloudflare, CloudflareDns, IngressRule, Tunnel};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, broadcast, watch};
use tokio::task::JoinHandle;

/// The primary side of the tunnel: the account's Cloudflare client, used to
/// manage every node's tunnel and the DNS records that point at it (ADR 0094).
pub struct TunnelController {
    cf: Cloudflare,
    dns: CloudflareDns,
    account_id: String,
}

impl TunnelController {
    /// `account_id` and `zone_id` are the non-secret `config` of the `tunnel`
    /// provider entry (ADR 0051); `zone_id` selects the zone of the hostnames'
    /// CNAMEs.
    #[must_use]
    pub fn new(cf: Cloudflare, account_id: String, zone_id: String) -> Self {
        let dns = cf.dns(&zone_id);
        Self {
            cf,
            dns,
            account_id,
        }
    }

    /// Finds this node's tunnel `quoro-<node_name>` or creates it, and returns
    /// the tunnel with its run token.
    ///
    /// # Errors
    ///
    /// The Cloudflare API refuses the credential, the lookup or the creation.
    pub async fn ensure_tunnel(&self, node_name: &str) -> Result<(Tunnel, Token), IngressError> {
        let name = format!("quoro-{node_name}");
        let tunnel = match self.cf.find_tunnel(&self.account_id, &name).await? {
            Some(tunnel) => tunnel,
            None => self.cf.create_tunnel(&self.account_id, &name).await?,
        };
        tracing::info!(tunnel = %tunnel.id, %name, "cloudflare tunnel ready");
        let token = self.cf.tunnel_token(&self.account_id, &tunnel.id).await?;
        Ok((tunnel, token))
    }

    /// Reconciles one tunnel: PUT its ingress rules (every hostname to
    /// `http://127.0.0.1:<origin_port>`), upsert a proxied CNAME
    /// `<tunnel_id>.cfargotunnel.com` for every hostname, and delete the CNAME
    /// of every hostname in `published` that is not in `hostnames`, unless it
    /// now points at another tunnel: a hostname that moved between a node and
    /// the edge keeps the CNAME its new tunnel wrote, whichever tunnel syncs
    /// first.
    ///
    /// `published` is updated in place (insert on a successful upsert, remove
    /// on a successful delete) and is the caller's record of the CNAMEs this
    /// controller created and has not removed.
    ///
    /// Returns the per-hostname failures as `(hostname, reason)`. Only the
    /// ingress PUT itself is an `Err`: a failed CNAME leaves the hostname out
    /// of `published` and is retried by the next reconciliation.
    ///
    /// # Errors
    ///
    /// The Cloudflare API refuses the ingress configuration.
    pub async fn sync_routes(
        &self,
        tunnel_id: &str,
        origin_port: u16,
        hostnames: &BTreeSet<String>,
        published: &mut BTreeSet<String>,
    ) -> Result<Vec<(String, String)>, IngressError> {
        self.sync_routes_draining(
            tunnel_id,
            origin_port,
            hostnames,
            &BTreeSet::new(),
            published,
        )
        .await
    }

    /// [`Self::sync_routes`] for a tunnel that also drains the hostnames in
    /// `draining`: they moved to another tunnel, and this one keeps answering
    /// them for a while (ADR 0194). Each gets an ingress rule but no CNAME,
    /// and a CNAME of one of them in `published` is left in place.
    ///
    /// # Errors
    ///
    /// The Cloudflare API refuses the ingress configuration.
    pub async fn sync_routes_draining(
        &self,
        tunnel_id: &str,
        origin_port: u16,
        hostnames: &BTreeSet<String>,
        draining: &BTreeSet<String>,
        published: &mut BTreeSet<String>,
    ) -> Result<Vec<(String, String)>, IngressError> {
        let service = format!("http://127.0.0.1:{origin_port}");
        let rules: Vec<IngressRule> = hostnames
            .union(draining)
            .map(|hostname| IngressRule {
                hostname: hostname.clone(),
                service: service.clone(),
            })
            .collect();
        self.cf
            .put_tunnel_ingress(&self.account_id, tunnel_id, &rules)
            .await?;

        let target = format!("{tunnel_id}.cfargotunnel.com");
        let mut failures = Vec::new();
        for hostname in hostnames {
            match self.dns.upsert_cname(hostname, &target, true).await {
                Ok(_) => {
                    published.insert(hostname.clone());
                }
                Err(e) => failures.push((hostname.clone(), e.to_string())),
            }
        }
        let stale: Vec<String> = published
            .iter()
            .filter(|hostname| !hostnames.contains(*hostname) && !draining.contains(*hostname))
            .cloned()
            .collect();
        for hostname in stale {
            match self.dns.delete_cname(&hostname, &target).await {
                Ok(()) => {
                    published.remove(&hostname);
                }
                Err(e) => failures.push((hostname, e.to_string())),
            }
        }
        Ok(failures)
    }

    /// Deletes a tunnel. Its CNAMEs are not removed; pass an empty hostname set
    /// to [`Self::sync_routes`] first.
    ///
    /// # Errors
    ///
    /// The Cloudflare API refuses the deletion.
    pub async fn delete_tunnel(&self, tunnel_id: &str) -> Result<(), IngressError> {
        self.cf.delete_tunnel(&self.account_id, tunnel_id).await?;
        Ok(())
    }
}

/// This node's tunnel as received in `desired_state.spec.ingress.tunnel`
/// (node-agent-protocol.md section 5.11): the tunnel id and its run token.
/// The run token lets its holder run this one tunnel and nothing else.
#[derive(Clone, Debug)]
pub struct TunnelRun {
    pub tunnel_id: String,
    /// The connector's run token. [`Token`]'s `Debug` redacts it.
    pub token: Token,
}

/// Settings of the node side of the tunnel backend.
#[derive(Clone, Debug)]
pub struct TunnelConfig {
    /// Loopback address the proxy listens on for `cloudflared`, e.g.
    /// `127.0.0.1:8081`. Must be a loopback address.
    pub origin: SocketAddr,
    pub cloudflared: CloudflaredConfig,
}

struct Shared {
    core: Arc<ProxyCore>,
    origin: SocketAddr,
    /// The tunnel currently running; replaced by [`TunnelIngress::set_run`].
    tunnel_id: Mutex<String>,
    events: broadcast::Sender<IngressEvent>,
    cloudflared: Cloudflared,
    wake: Notify,
}

/// The node side of the Cloudflare Tunnel backend behind [`Ingress`].
pub struct TunnelIngress {
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl TunnelIngress {
    /// Starts the loopback proxy and `cloudflared` with the tunnel's run token.
    ///
    /// # Errors
    ///
    /// The origin is not loopback or cannot be bound.
    pub async fn start(config: TunnelConfig, run: TunnelRun) -> Result<Self, IngressError> {
        if !config.origin.ip().is_loopback() {
            return Err(IngressError::Other(format!(
                "tunnel origin address {} is not a loopback address",
                config.origin
            )));
        }
        let listener = TcpListener::bind(config.origin).await?;
        let origin = listener.local_addr()?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (events, _) = broadcast::channel(256);
        let core = ProxyCore::new();
        let mode = ListenerMode {
            serves: TlsMode::Tunnel,
            forwarded_proto: "https",
            trust_forwarded: true,
        };
        let mut tasks = Vec::new();
        {
            let (core, rx) = (Arc::clone(&core), shutdown_rx.clone());
            tasks.push(tokio::spawn(async move {
                if let Err(e) = proxy::serve_http(listener, core, mode, rx).await {
                    tracing::error!(error = %e, "tunnel loopback listener stopped");
                }
            }));
        }
        tracing::info!(tunnel = %run.tunnel_id, %origin, "tunnel ingress ready");
        let cloudflared = Cloudflared::spawn(config.cloudflared, run.token, shutdown_rx.clone());
        let shared = Arc::new(Shared {
            core,
            origin,
            tunnel_id: Mutex::new(run.tunnel_id),
            events,
            cloudflared,
            wake: Notify::new(),
        });
        {
            let (shared, rx) = (Arc::clone(&shared), shutdown_rx);
            tasks.push(tokio::spawn(readiness_loop(shared, rx)));
        }
        Ok(Self {
            shared,
            shutdown,
            tasks: Mutex::new(tasks),
        })
    }

    /// Replaces the tunnel this node runs: a rotated token or a new tunnel id.
    /// The connector restarts with the new token.
    pub async fn set_run(&self, run: TunnelRun) {
        {
            let mut tunnel_id = self
                .shared
                .tunnel_id
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *tunnel_id = run.tunnel_id;
        }
        self.shared.cloudflared.set_token(run.token);
        self.shared.wake.notify_one();
    }

    /// The tunnel this node is running.
    #[must_use]
    pub fn tunnel_id(&self) -> String {
        self.shared
            .tunnel_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The bound origin address of the proxy.
    #[must_use]
    pub fn origin(&self) -> SocketAddr {
        self.shared.origin
    }

    /// The proxy core behind the loopback listener, so the node can attach a
    /// backend observer.
    #[must_use]
    pub fn core(&self) -> &Arc<ProxyCore> {
        &self.shared.core
    }
}

/// Why the readiness wait ended.
enum Readiness {
    Ready,
    TimedOut,
    Shutdown,
}

/// Waits for the connector to answer `GET /ready`, polling every 2 s for up to
/// a minute (as the old sync loop did).
async fn wait_for_connector(shared: &Shared, shutdown: &mut watch::Receiver<bool>) -> Readiness {
    for _ in 0..30 {
        if shared.cloudflared.ready().await {
            return Readiness::Ready;
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(2)) => {}
            _ = shutdown.changed() => return Readiness::Shutdown,
        }
        if *shutdown.borrow() {
            return Readiness::Shutdown;
        }
    }
    Readiness::TimedOut
}

/// Reports the current table once the connector is up, and retries a failed
/// wait with backoff (5 s doubling to 5 min). Every apply, and every
/// [`TunnelIngress::set_run`], wakes it.
async fn readiness_loop(shared: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    let mut failures: u32 = 0;
    let mut pending = true;
    loop {
        if pending {
            match wait_for_connector(&shared, &mut shutdown).await {
                Readiness::Ready => {
                    failures = 0;
                    pending = false;
                    let table = shared.core.table();
                    for route in table.iter() {
                        let _ = shared.events.send(IngressEvent::TunnelRouteReady {
                            hostname: route.hostname.clone(),
                        });
                    }
                }
                Readiness::TimedOut => {
                    failures = failures.saturating_add(1);
                    tracing::warn!(failures, "cloudflared is not connected");
                    let table = shared.core.table();
                    for route in table.iter() {
                        let _ = shared.events.send(IngressEvent::TunnelRouteFailed {
                            hostname: route.hostname.clone(),
                            reason: "cloudflared is not connected".to_string(),
                        });
                    }
                }
                Readiness::Shutdown => return,
            }
        }
        let delay = if pending {
            Duration::from_secs(
                5u64.saturating_mul(1 << failures.saturating_sub(1).min(6))
                    .min(300),
            )
        } else {
            Duration::from_secs(3600)
        };
        tokio::select! {
            () = shared.wake.notified() => pending = true,
            () = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => return,
        }
        if *shutdown.borrow() {
            return;
        }
    }
}

impl Ingress for TunnelIngress {
    async fn apply(&self, table: RouteTable) -> Result<(), IngressError> {
        if let Some(route) = table.iter().find(|r| r.tls != TlsMode::Tunnel) {
            return Err(IngressError::InvalidRoutes(format!(
                "{} uses tls_mode acme, which the tunnel backend does not serve",
                route.hostname
            )));
        }
        self.shared.core.set_table(table);
        self.shared.wake.notify_one();
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<IngressEvent> {
        self.shared.events.subscribe()
    }

    async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for task in tasks {
            let _ = task.await;
        }
        // The connector supervisor stops on the same shutdown signal and kills
        // `cloudflared` (kill-on-drop covers an abrupt exit).
    }
}
