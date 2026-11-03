//! Lab driver for the ingress backends, until the node agent wires them
//! (epic #64). Not shipped: examples are not part of the release tarball.
//! It plays both sides of the privilege split in one process (the control
//! plane's provider secret and the node's listeners and connector), so on a
//! lab host it runs as root; the product keeps them in separate services.
//!
//! It reads one provider entry from the node's own database through the
//! control plane's in-process accessor (the secret never crosses an API),
//! starts the chosen backend and applies a route table from a JSON file:
//!
//! ```text
//! ingress-lab tunnel --data-dir /var/lib/quoro --provider prv_... --node lab \
//!     --routes routes.json --cloudflared /usr/local/lib/quoro/cloudflared [--cleanup]
//! ingress-lab public --data-dir /var/lib/quoro --provider prv_... --routes routes.json \
//!     [--http 0.0.0.0:80] [--https 0.0.0.0:443] [--directory <acme directory url>]
//! ```
//!
//! In tunnel mode the primary side (the provider's account credential) ensures
//! the node's tunnel and publishes its routes; the node side only gets the
//! tunnel id and its run token. The tunnel is left in place on exit unless
//! `--cleanup` is given, which withdraws the CNAMEs and deletes the tunnel.
//!
//! `routes.json` is `[{"hostname": "app.example.com", "backends": ["10.42.0.2:8080"]}]`.
//! The `tunnel` provider's config needs `account_id` and `zone_id`; the `dns`
//! provider's config needs `zone_id`. The ACME directory defaults to Let's
//! Encrypt staging.

use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::store::Store;
use quoro_ingress::acme::{AcmeConfig, LETS_ENCRYPT_STAGING};
use quoro_ingress::cloudflared::CloudflaredConfig;
use quoro_ingress::public::{PublicConfig, PublicIngress};
use quoro_ingress::tunnel::{TunnelConfig, TunnelController, TunnelIngress, TunnelRun};
use quoro_ingress::{Ingress, IngressError, Route, RouteTable, TlsMode};
use quoro_providers::Token;
use quoro_providers::cloudflare::Cloudflare;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn parse() -> Result<(String, HashMap<String, String>)> {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .ok_or("usage: ingress-lab tunnel|public --flag value ...")?;
    let mut flags = HashMap::new();
    while let Some(flag) = args.next() {
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected {flag}"))?;
        // `--cleanup` is the only value-less flag.
        if name == "cleanup" {
            flags.insert(name.to_string(), String::new());
            continue;
        }
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        flags.insert(name.to_string(), value);
    }
    Ok((mode, flags))
}

fn flag<'a>(flags: &'a HashMap<String, String>, name: &str) -> Result<&'a str> {
    flags
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("missing --{name}").into())
}

fn config_str(config: &serde_json::Value, key: &str) -> Result<String> {
    config[key]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("provider config has no {key}").into())
}

fn routes(path: &str, tls: TlsMode) -> Result<RouteTable> {
    #[derive(serde::Deserialize)]
    struct Entry {
        hostname: String,
        backends: Vec<SocketAddr>,
    }
    let entries: Vec<Entry> = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(RouteTable::new(entries.into_iter().map(|e| Route {
        hostname: e.hostname,
        backends: e.backends,
        tls,
    }))?)
}

async fn run<I: Ingress>(ingress: I, table: RouteTable) -> Result<()> {
    let mut events = ingress.subscribe();
    ingress.apply(table).await?;
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => println!("event: {event:?}"),
                Err(e) => println!("events: {e}"),
            },
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    println!("shutting down");
    ingress.shutdown().await;
    Ok(())
}

fn report_routes(result: std::result::Result<Vec<(String, String)>, IngressError>) {
    match result {
        Ok(failures) => {
            for (hostname, reason) in failures {
                println!("route failed: {hostname}: {reason}");
            }
        }
        Err(e) => println!("route sync failed: {e}"),
    }
}

/// Tunnel mode: the primary side reconciles the tunnel's rules and CNAMEs, the
/// node side serves the hostnames. The tunnel is left in place on Ctrl-C
/// unless `--cleanup` was given.
async fn run_tunnel(
    ingress: TunnelIngress,
    table: RouteTable,
    controller: &TunnelController,
    tunnel_id: &str,
    published: &mut BTreeSet<String>,
    cleanup: bool,
) -> Result<()> {
    let hostnames: BTreeSet<String> = table.iter().map(|r| r.hostname.clone()).collect();
    let origin_port = ingress.origin().port();
    ingress.apply(table).await?;
    report_routes(
        controller
            .sync_routes(tunnel_id, origin_port, &hostnames, published)
            .await,
    );

    let mut events = ingress.subscribe();
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => println!("event: {event:?}"),
                Err(e) => println!("events: {e}"),
            },
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    println!("shutting down");
    ingress.shutdown().await;
    if cleanup {
        report_routes(
            controller
                .sync_routes(tunnel_id, origin_port, &BTreeSet::new(), published)
                .await,
        );
        controller.delete_tunnel(tunnel_id).await?;
        println!("tunnel {tunnel_id} deleted");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_ansi(false).init();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (mode, flags) = parse()?;
    let data_dir = PathBuf::from(flag(&flags, "data-dir")?);
    if !data_dir.join("keys/data.key").exists() {
        return Err("no data-encryption key in the data directory; start quoro serve first".into());
    }
    let store = Store::open(&data_dir.join("quoro.db")).await?;
    let key = DataKey::load_or_create(&data_dir)?;
    let provider_id = flag(&flags, "provider")?;
    let (record, secret) =
        quoro_control_plane::api::providers::provider_secret_in(&store, &key, provider_id)
            .await
            .map_err(|e| format!("provider {provider_id}: {e:?}"))?;
    println!("provider {} ({}, {})", record.id, record.kind, record.name);
    let cf = Cloudflare::new(Token::new(secret.expose().to_string()))?;
    drop(secret);
    let ingress_dir = data_dir.join("ingress");

    match mode.as_str() {
        "tunnel" => {
            let controller = TunnelController::new(
                cf,
                config_str(&record.config, "account_id")?,
                config_str(&record.config, "zone_id")?,
            );
            let (tunnel, token) = controller.ensure_tunnel(flag(&flags, "node")?).await?;
            let config = TunnelConfig {
                origin: flags
                    .get("loopback")
                    .map_or("127.0.0.1:8081", String::as_str)
                    .parse()?,
                cloudflared: CloudflaredConfig {
                    binary: PathBuf::from(flag(&flags, "cloudflared")?),
                    metrics: "127.0.0.1:20241".parse()?,
                    extra_args: Vec::new(),
                },
            };
            let ingress = TunnelIngress::start(
                config,
                TunnelRun {
                    tunnel_id: tunnel.id.clone(),
                    token,
                },
            )
            .await?;
            println!(
                "tunnel {} on origin {}",
                ingress.tunnel_id(),
                ingress.origin()
            );
            let table = routes(flag(&flags, "routes")?, TlsMode::Tunnel)?;
            let mut published = BTreeSet::new();
            run_tunnel(
                ingress,
                table,
                &controller,
                &tunnel.id,
                &mut published,
                flags.contains_key("cleanup"),
            )
            .await
        }
        "public" => {
            let dns = Arc::new(cf.dns(&config_str(&record.config, "zone_id")?));
            let directory = flags
                .get("directory")
                .map_or(LETS_ENCRYPT_STAGING, String::as_str);
            let config = PublicConfig {
                http: flags
                    .get("http")
                    .map_or("0.0.0.0:80", String::as_str)
                    .parse()?,
                https: flags
                    .get("https")
                    .map_or("0.0.0.0:443", String::as_str)
                    .parse()?,
                cert_dir: ingress_dir.join("certs"),
                acme: AcmeConfig::new(directory, ingress_dir.join("acme")),
            };
            let ingress = PublicIngress::start(config, dns).await?;
            println!("public proxy on {:?}", ingress.local_addrs());
            run(ingress, routes(flag(&flags, "routes")?, TlsMode::Acme)?).await
        }
        other => Err(format!("unknown mode {other}").into()),
    }
}
