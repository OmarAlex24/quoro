//! Spike for issue #65: obtain and renew one certificate through ACME DNS-01
//! with `instant-acme`, using the `ring` provider (ADR 0080).
//!
//! Usage:
//!
//! ```text
//! acme-dns01-spike obtain|renew --state <dir> --domain <name> --directory <url>
//!     [--root <pem>]                      trust this CA for the ACME directory (Pebble)
//!     --dns challtestsrv=<url>            Pebble's challenge test server management API
//!     --dns cloudflare --zone-id <id>     Cloudflare DNS API; the token is read from the
//!                                         file named by QUORO_SPIKE_CF_TOKEN_FILE
//! ```
//!
//! `obtain` creates (or reuses) the account in `<state>/account.json`, orders a
//! certificate for `<domain>`, answers the DNS-01 challenge, and writes
//! `<state>/cert.pem` and `<state>/key.pem`. `renew` reads the current
//! certificate, asks the server for its ARI renewal window (RFC 9773), orders a
//! replacement that names the old certificate in `replaces`, and checks that the
//! new certificate has a different serial.

use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, CertificateIdentifier, ChallengeType,
    Identifier, NewAccount, NewOrder, OrderStatus, RetryPolicy,
};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

enum Dns {
    ChallTestSrv(String),
    Cloudflare { zone_id: String, token: String },
}

struct Args {
    command: String,
    state: PathBuf,
    domain: String,
    directory: String,
    root: Option<PathBuf>,
    dns: Dns,
}

fn parse_args() -> Result<Args> {
    let mut it = std::env::args().skip(1);
    let command = it.next().ok_or("missing command: obtain or renew")?;
    let (mut state, mut domain, mut directory, mut root, mut dns, mut zone_id) =
        (None, None, None, None, None, None);
    while let Some(flag) = it.next() {
        let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--state" => state = Some(PathBuf::from(value)),
            "--domain" => domain = Some(value),
            "--directory" => directory = Some(value),
            "--root" => root = Some(PathBuf::from(value)),
            "--dns" => dns = Some(value),
            "--zone-id" => zone_id = Some(value),
            _ => return Err(format!("unknown flag {flag}").into()),
        }
    }
    let dns = dns.ok_or("missing --dns")?;
    let dns = if let Some(url) = dns.strip_prefix("challtestsrv=") {
        Dns::ChallTestSrv(url.to_string())
    } else if dns == "cloudflare" {
        let file = std::env::var("QUORO_SPIKE_CF_TOKEN_FILE")
            .map_err(|_| "QUORO_SPIKE_CF_TOKEN_FILE is not set")?;
        let token = std::fs::read_to_string(file)?.trim().to_string();
        Dns::Cloudflare {
            zone_id: zone_id.ok_or("--dns cloudflare needs --zone-id")?,
            token,
        }
    } else {
        return Err("--dns must be challtestsrv=<url> or cloudflare".into());
    };
    Ok(Args {
        command,
        state: state.ok_or("missing --state")?,
        domain: domain.ok_or("missing --domain")?,
        directory: directory.ok_or("missing --directory")?,
        root,
        dns,
    })
}

fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

async fn account(args: &Args) -> Result<Account> {
    let builder = match &args.root {
        Some(root) => Account::builder_with_root(root)?,
        None => Account::builder()?,
    };
    let path = args.state.join("account.json");
    if path.exists() {
        let credentials: AccountCredentials = serde_json::from_slice(&std::fs::read(&path)?)?;
        let account = builder.from_credentials(credentials).await?;
        println!("account: reused {}", account.id());
        return Ok(account);
    }
    let (account, credentials) = builder
        .create(
            &NewAccount {
                contact: &[],
                terms_of_service_agreed: true,
                only_return_existing: false,
            },
            args.directory.clone(),
            None,
        )
        .await?;
    write_private(&path, &serde_json::to_vec(&credentials)?)?;
    println!("account: created {}", account.id());
    Ok(account)
}

/// A DNS record set for one challenge, removed by [`Dns::clear`].
struct Record {
    name: String,
    cloudflare_id: Option<String>,
}

impl Dns {
    async fn set_txt(&self, http: &reqwest::Client, name: &str, value: &str) -> Result<Record> {
        match self {
            Self::ChallTestSrv(url) => {
                http.post(format!("{url}/set-txt"))
                    .json(&serde_json::json!({ "host": format!("{name}."), "value": value }))
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(Record {
                    name: name.to_string(),
                    cloudflare_id: None,
                })
            }
            Self::Cloudflare { zone_id, token } => {
                let response: serde_json::Value = http
                    .post(format!(
                        "https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records"
                    ))
                    .bearer_auth(token)
                    .json(&serde_json::json!({
                        "type": "TXT", "name": name, "content": value, "ttl": 60,
                        "comment": "acme dns-01 spike"
                    }))
                    .send()
                    .await?
                    .json()
                    .await?;
                if response["success"] != serde_json::Value::Bool(true) {
                    return Err(format!("cloudflare create failed: {}", response["errors"]).into());
                }
                Ok(Record {
                    name: name.to_string(),
                    cloudflare_id: response["result"]["id"].as_str().map(str::to_string),
                })
            }
        }
    }

    async fn clear(&self, http: &reqwest::Client, record: &Record) -> Result<()> {
        match self {
            Self::ChallTestSrv(url) => {
                http.post(format!("{url}/clear-txt"))
                    .json(&serde_json::json!({ "host": format!("{}.", record.name) }))
                    .send()
                    .await?
                    .error_for_status()?;
            }
            Self::Cloudflare { zone_id, token } => {
                if let Some(id) = &record.cloudflare_id {
                    http.delete(format!(
                        "https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records/{id}"
                    ))
                    .bearer_auth(token)
                    .send()
                    .await?
                    .error_for_status()?;
                }
            }
        }
        Ok(())
    }

    /// Waits until a public resolver returns `value` for `name` (Cloudflare
    /// only; the test server answers immediately).
    async fn wait_visible(&self, http: &reqwest::Client, name: &str, value: &str) -> Result<()> {
        if matches!(self, Self::ChallTestSrv(_)) {
            return Ok(());
        }
        let start = Instant::now();
        loop {
            let answer: serde_json::Value = http
                .get("https://cloudflare-dns.com/dns-query")
                .query(&[("name", name), ("type", "TXT")])
                .header("accept", "application/dns-json")
                .send()
                .await?
                .json()
                .await?;
            let seen = answer["Answer"].as_array().is_some_and(|a| {
                a.iter()
                    .any(|r| r["data"].as_str().is_some_and(|d| d.contains(value)))
            });
            if seen {
                println!("dns: TXT visible after {:?}", start.elapsed());
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(180) {
                return Err("TXT record not visible after 180 s".into());
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Orders, answers DNS-01, finalizes and returns `(cert_pem, key_pem)`.
async fn issue(
    args: &Args,
    account: &Account,
    http: &reqwest::Client,
    replaces: Option<CertificateIdentifier<'_>>,
) -> Result<(String, String)> {
    let identifiers = [Identifier::Dns(args.domain.clone())];
    let mut new_order = NewOrder::new(&identifiers);
    if let Some(replaces) = replaces {
        new_order = new_order.replaces(replaces);
    }
    let started = Instant::now();
    let mut order = account.new_order(&new_order).await?;
    let status = order.state().status;
    println!("order: {} status {status:?}", order.url());

    let mut records = Vec::new();
    let mut authorizations = order.authorizations();
    while let Some(authz) = authorizations.next().await {
        let mut authz = authz?;
        match authz.status {
            AuthorizationStatus::Pending => {}
            AuthorizationStatus::Valid => {
                println!("authz: already valid (reused)");
                continue;
            }
            other => return Err(format!("authorization status {other:?}").into()),
        }
        let mut challenge = authz
            .challenge(ChallengeType::Dns01)
            .ok_or("server offered no dns-01 challenge")?;
        // DNS-01 for a wildcard name uses the base domain (RFC 8555 section 8.4).
        let Identifier::Dns(host) = challenge.identifier().identifier else {
            return Err("not a DNS identifier".into());
        };
        let name = format!("_acme-challenge.{}", host.trim_start_matches("*."));
        let value = challenge.key_authorization().dns_value();
        let record = args.dns.set_txt(http, &name, &value).await?;
        println!("dns: set TXT {name}");
        args.dns.wait_visible(http, &name, &value).await?;
        challenge.set_ready().await?;
        records.push(record);
    }

    let retry = RetryPolicy::default().timeout(Duration::from_secs(300));
    let status = order.poll_ready(&retry).await;
    for record in &records {
        if let Err(e) = args.dns.clear(http, record).await {
            eprintln!("dns: could not remove TXT {}: {e}", record.name);
        } else {
            println!("dns: removed TXT {}", record.name);
        }
    }
    let status = status?;
    if status != OrderStatus::Ready {
        return Err(format!("order status {status:?}").into());
    }

    let key = rcgen::KeyPair::generate()?;
    let mut params = rcgen::CertificateParams::new(vec![args.domain.clone()])?;
    // rcgen's default subject CN "rcgen self signed cert" is rejected by Let's
    // Encrypt ("rejectedIdentifier"); Pebble accepts it. SAN only.
    params.distinguished_name = rcgen::DistinguishedName::new();
    let csr = params.serialize_request(&key)?;
    order.finalize_csr(csr.der()).await?;
    let cert = order.poll_certificate(&retry).await?;
    println!("order: certificate issued in {:?}", started.elapsed());
    Ok((cert, key.serialize_pem()))
}

fn first_cert(pem: &str) -> Result<CertificateDer<'static>> {
    Ok(CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or("no certificate in PEM")??)
}

fn describe(label: &str, pem: &str) -> Result<String> {
    let cert = first_cert(pem)?;
    let id = CertificateIdentifier::try_from(&cert)?;
    println!(
        "{label}: ARI id {id} ({} certificates in chain)",
        pem.matches("BEGIN CERTIFICATE").count()
    );
    Ok(id.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "crypto provider already installed")?;
    let args = parse_args()?;
    std::fs::create_dir_all(&args.state)?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let account = account(&args).await?;
    let cert_path = args.state.join("cert.pem");
    let key_path = args.state.join("key.pem");

    match args.command.as_str() {
        "obtain" => {
            let (cert, key) = issue(&args, &account, &http, None).await?;
            describe("obtained", &cert)?;
            write_private(&key_path, key.as_bytes())?;
            write_private(&cert_path, cert.as_bytes())?;
        }
        "renew" => {
            let old_pem = std::fs::read_to_string(&cert_path)?;
            let old_id = describe("current", &old_pem)?;
            let old_cert = first_cert(&old_pem)?;
            let id = CertificateIdentifier::try_from(&old_cert)?.into_owned();
            match account.renewal_info(&id).await {
                Ok((info, retry_after)) => println!(
                    "ari: suggested window {} .. {} (poll again in {retry_after:?})",
                    info.suggested_window.start, info.suggested_window.end
                ),
                Err(e) => println!("ari: not available: {e}"),
            }
            let (cert, key) = issue(&args, &account, &http, Some(id)).await?;
            let new_id = describe("renewed", &cert)?;
            if new_id == old_id {
                return Err("renewal returned the same certificate".into());
            }
            write_private(&key_path, key.as_bytes())?;
            write_private(&cert_path, cert.as_bytes())?;
            println!("renew: replaced certificate, new serial differs");
        }
        other => return Err(format!("unknown command {other}").into()),
    }
    Ok(())
}
