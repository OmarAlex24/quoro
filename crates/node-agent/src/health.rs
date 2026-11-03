// SPDX-License-Identifier: AGPL-3.0-only
//! Health checks for running app VMs (`node-agent-protocol.md` section 6.3).
//!
//! The probe is a plain HTTP/1.1 `GET` from the host to the guest's address and
//! app port. It uses a raw `tokio::net::TcpStream` rather than an HTTP client
//! because the only thing that matters is the status code, and a full client
//! would pull in connection pooling and TLS this path does not need.
//!
//! Defaults are the contract's: `path` `/`, `interval_s` 10, `timeout_s` 2 and
//! `failures` 3. Any status in `200..=399` is a pass.

use std::net::Ipv4Addr;
use std::time::Duration;

use quoro_proto::wire::SpecVm;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// Default probe path (section 6.3).
pub const DEFAULT_PATH: &str = "/";
/// Default interval between probes (section 6.3).
pub const DEFAULT_INTERVAL_S: u32 = 10;
/// Default probe timeout (section 6.3).
pub const DEFAULT_TIMEOUT_S: u32 = 2;
/// Default number of consecutive failures before `failing` (section 6.3).
pub const DEFAULT_FAILURES: u32 = 3;
/// How long after a VM reaches running the first probe runs.
pub const FIRST_PROBE_DELAY: Duration = Duration::from_secs(1);

/// A resolved health target: where to probe and how often.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthTarget {
    /// Guest address.
    pub ip: Ipv4Addr,
    /// App port inside the guest.
    pub port: u16,
    /// Request path, always starting with `/`.
    pub path: String,
    /// Interval between probes.
    pub interval: Duration,
    /// Per-probe timeout.
    pub timeout: Duration,
    /// Consecutive failures before the app is reported `failing`.
    pub failures: u32,
}

impl HealthTarget {
    /// Builds the target for a desired VM, or `None` when it has no `port`.
    ///
    /// `ip` comes from the store, because the engine's `VmInfo` does not carry
    /// the address.
    #[must_use]
    pub fn for_vm(vm: &SpecVm, ip: Ipv4Addr) -> Option<Self> {
        let port = vm.port?;
        let check = vm.health_check.as_ref();
        let path = check
            .and_then(|check| check.path.clone())
            .unwrap_or_else(|| DEFAULT_PATH.to_owned());
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        Some(Self {
            ip,
            port,
            path,
            interval: Duration::from_secs(u64::from(
                check
                    .and_then(|check| check.interval_s)
                    .unwrap_or(DEFAULT_INTERVAL_S)
                    .max(1),
            )),
            timeout: Duration::from_secs(u64::from(
                check
                    .and_then(|check| check.timeout_s)
                    .unwrap_or(DEFAULT_TIMEOUT_S)
                    .max(1),
            )),
            failures: check
                .and_then(|check| check.failures)
                .unwrap_or(DEFAULT_FAILURES)
                .max(1),
        })
    }
}

/// Probes the target once and returns the HTTP status code.
///
/// A connection error, a timeout or a response that is not HTTP/1.x is
/// returned as a human-readable reason, which becomes the `deployment_logs`
/// line and the log's detail.
///
/// # Errors
///
/// The reason the probe did not produce a status code.
pub async fn probe(target: &HealthTarget) -> Result<u16, String> {
    let deadline = target.timeout;
    let mut stream = tokio::time::timeout(deadline, TcpStream::connect((target.ip, target.port)))
        .await
        .map_err(|_| format!("connect to {}:{} timed out", target.ip, target.port))?
        .map_err(|e| format!("connect to {}:{}: {e}", target.ip, target.port))?;

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nUser-Agent: quoro-node-agent\r\n\r\n",
        target.path, target.ip, target.port
    );
    tokio::time::timeout(deadline, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| "the request timed out".to_owned())?
        .map_err(|e| format!("write the request: {e}"))?;

    let mut buffer = Vec::with_capacity(256);
    tokio::time::timeout(deadline, async {
        let mut chunk = [0u8; 256];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);
            if buffer.windows(2).any(|window| window == b"\r\n") {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|_| "the response timed out".to_owned())?
    .map_err(|e| format!("read the response: {e}"))?;

    let status_line = String::from_utf8_lossy(&buffer);
    let status_line = status_line.lines().next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "the response is not HTTP/1.x".to_owned())?;
    Ok(status)
}

/// Whether a status code counts as passing (`200..=399`).
#[must_use]
pub fn is_passing(status: u16) -> bool {
    (200..=399).contains(&status)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use quoro_proto::wire::{HealthCheck, SpecVm};

    use super::*;

    fn vm(port: Option<u16>, check: Option<HealthCheck>) -> SpecVm {
        SpecVm {
            vm_id: "vm_1".to_owned(),
            project_id: "prj_1".to_owned(),
            app_id: Some("app_1".to_owned()),
            deployment_id: None,
            name: "vm_1".to_owned(),
            image: "img".to_owned(),
            image_digest: "sha256:aa".to_owned(),
            vcpus: 1,
            memory_mb: 128,
            disk_gb: 1,
            port,
            env: BTreeMap::new(),
            health_check: check,
            desired_status: quoro_proto::wire::DesiredStatus::Running,
            restart_generation: 0,
            stateful: false,
            lease_id: None,
            fencing_token: None,
            volumes: Vec::new(),
        }
    }

    #[test]
    fn defaults_follow_the_contract() {
        let target = HealthTarget::for_vm(&vm(Some(8080), None), Ipv4Addr::LOCALHOST).unwrap();
        assert_eq!(target.path, "/");
        assert_eq!(target.interval, Duration::from_secs(10));
        assert_eq!(target.timeout, Duration::from_secs(2));
        assert_eq!(target.failures, 3);
        assert!(HealthTarget::for_vm(&vm(None, None), Ipv4Addr::LOCALHOST).is_none());

        let target = HealthTarget::for_vm(
            &vm(
                Some(8080),
                Some(HealthCheck {
                    path: Some("live".to_owned()),
                    interval_s: Some(5),
                    timeout_s: Some(1),
                    failures: Some(4),
                }),
            ),
            Ipv4Addr::LOCALHOST,
        )
        .unwrap();
        assert_eq!(target.path, "/live");
        assert_eq!(target.interval, Duration::from_secs(5));
        assert_eq!(target.timeout, Duration::from_secs(1));
        assert_eq!(target.failures, 4);
    }

    #[test]
    fn status_classes_match_the_contract() {
        assert!(is_passing(200));
        assert!(is_passing(399));
        assert!(!is_passing(199));
        assert!(!is_passing(400));
        assert!(!is_passing(500));
    }

    #[tokio::test]
    async fn a_real_http_listener_passes_and_a_closed_port_fails() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 512];
            let _ = socket.read(&mut buffer).await;
            let _ = socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await;
        });
        let target = HealthTarget {
            ip: Ipv4Addr::LOCALHOST,
            port,
            path: "/health".to_owned(),
            interval: Duration::from_secs(1),
            timeout: Duration::from_secs(1),
            failures: 1,
        };
        assert_eq!(probe(&target).await, Ok(204));
        server.await.unwrap();

        // The listener is gone; the same port now refuses the connection.
        assert!(probe(&target).await.is_err());
    }
}
