//! The public-IP backend: the embedded reverse proxy on 80 and 443 with
//! certificates from ACME DNS-01 (ADR 0024, 0059).

use crate::acme::{AcmeConfig, AcmeManager};
use crate::certs::CertStore;
use crate::proxy::{self, ListenerMode, ProxyCore};
use crate::{Ingress, IngressError, IngressEvent, RouteTable, TlsMode};
use quoro_providers::DnsProvider;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

/// Settings of the public backend.
#[derive(Clone, Debug)]
pub struct PublicConfig {
    /// Plain HTTP listener; every request is redirected to HTTPS. `0.0.0.0:80`.
    pub http: SocketAddr,
    /// TLS listener. `0.0.0.0:443`.
    pub https: SocketAddr,
    /// Node-local certificate directory, e.g. `/var/lib/quoro/ingress/certs`.
    pub cert_dir: PathBuf,
    pub acme: AcmeConfig,
}

/// The public reverse proxy behind [`Ingress`].
pub struct PublicIngress<D: DnsProvider> {
    core: Arc<ProxyCore>,
    acme: AcmeManager<D>,
    events: broadcast::Sender<IngressEvent>,
    shutdown: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    local: (SocketAddr, SocketAddr),
}

impl<D: DnsProvider> PublicIngress<D> {
    /// Binds both listeners, loads stored certificates and starts the ACME
    /// worker. Routes are empty until the first [`Ingress::apply`].
    ///
    /// # Errors
    ///
    /// A listener cannot be bound or the certificate directory is unusable.
    pub async fn start(config: PublicConfig, dns: Arc<D>) -> Result<Self, IngressError> {
        let store = CertStore::open(&config.cert_dir)?;
        let tls = proxy::tls_server_config(store.resolver())
            .map_err(|e| IngressError::Other(format!("tls config: {e}")))?;
        let http = TcpListener::bind(config.http).await?;
        let https = TcpListener::bind(config.https).await?;
        let local = (http.local_addr()?, https.local_addr()?);
        let (events, _) = broadcast::channel(256);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let core = ProxyCore::new();
        let acme = AcmeManager::new(config.acme, dns, store, events.clone());

        let mode = ListenerMode {
            serves: TlsMode::Acme,
            forwarded_proto: "https",
            trust_forwarded: false,
        };
        let mut tasks = Vec::new();
        {
            let (core, rx) = (Arc::clone(&core), shutdown_rx.clone());
            tasks.push(tokio::spawn(async move {
                if let Err(e) = proxy::serve_redirect(http, core, rx).await {
                    tracing::error!(error = %e, "http listener stopped");
                }
            }));
        }
        {
            let (core, rx) = (Arc::clone(&core), shutdown_rx.clone());
            tasks.push(tokio::spawn(async move {
                if let Err(e) = proxy::serve_https(https, core, mode, tls, rx).await {
                    tracing::error!(error = %e, "https listener stopped");
                }
            }));
        }
        {
            let (acme, rx) = (acme.clone(), shutdown_rx);
            tasks.push(tokio::spawn(async move { acme.run(rx).await }));
        }
        Ok(Self {
            core,
            acme,
            events,
            shutdown,
            tasks: Mutex::new(tasks),
            local,
        })
    }

    /// The bound `(http, https)` addresses (tests bind port 0).
    #[must_use]
    pub fn local_addrs(&self) -> (SocketAddr, SocketAddr) {
        self.local
    }

    /// The proxy core behind the listeners, so the node can attach a backend
    /// observer.
    #[must_use]
    pub fn core(&self) -> &Arc<ProxyCore> {
        &self.core
    }
}

impl<D: DnsProvider> Ingress for PublicIngress<D> {
    async fn apply(&self, table: RouteTable) -> Result<(), IngressError> {
        if let Some(route) = table.iter().find(|r| r.tls != TlsMode::Acme) {
            return Err(IngressError::InvalidRoutes(format!(
                "{} uses tls_mode tunnel, which the public backend does not serve",
                route.hostname
            )));
        }
        let hostnames: BTreeSet<String> = table.iter().map(|r| r.hostname.clone()).collect();
        self.core.set_table(table);
        self.acme.set_hostnames(hostnames);
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<IngressEvent> {
        self.events.subscribe()
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
    }
}
