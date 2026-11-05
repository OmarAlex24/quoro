// SPDX-License-Identifier: AGPL-3.0-only
//! The two subject kinds that are not apps: Quoro's own components and the
//! nodes' host kernels.
//!
//! [`subject_for`] maps one embedded component to what it is matched as (an
//! OSV ecosystem, package and version), [`kernel_subject`] does the same for a
//! node's kernel release, and both `reconcile_*` functions match those subjects
//! against the cache and write what changed into `vulnerability_findings`.
//!
//! These findings raise no event and open no incident: the contract says so
//! for M5a (events.schema.json `data_app_vulnerable`). Every write still goes
//! through [`crate::vulns::findings::apply_subject`], so a row, its change-log
//! entry and the fingerprint behave exactly like an app's.

use crate::AppState;
use crate::error::ApiError;
use crate::vulns::components::{self, Component};
use crate::vulns::ecosystem;
use crate::vulns::findings::{self, DesiredFinding, FindingRow, Reconciled};
use crate::vulns::matcher;
use crate::vulns::version::{self, Order};
use serde_json::Value;
use sqlx::Row as _;
use std::collections::{BTreeMap, BTreeSet};

/// The OSV bucket host kernels are matched in. It is always fetched.
const LINUX: &str = "Linux";

/// What one component or kernel is matched as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subject {
    /// The OSV ecosystem name (`crates.io`, `Go`, `Linux`).
    pub ecosystem: &'static str,
    /// The OSV package name.
    pub package: String,
    /// The version the records are evaluated against.
    pub version: String,
}

/// One component the release ships, with the subject it is matched as.
struct QuoroSubject {
    /// The row's `component`: the component's own name.
    component: String,
    /// The row's `installed_version`: the component's own version string.
    installed: String,
    /// What the records are matched with.
    subject: Subject,
}

/// One node with the `node` role, as the host-kernel pass sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeKernel {
    pub node_id: String,
    /// The `kernel` field of the node's `kvm` JSON, or empty.
    pub kernel: String,
    pub status: String,
}

/// What `component` is matched as, or `None` when it is not matched in M5a.
///
/// A crate is matched only when the SBOM says it came from crates.io: a path or
/// git crate is not a published release. npm packages are not matched, because
/// the contract's `component` names crates and the four pinned artifacts only.
#[must_use]
pub fn subject_for(component: &Component) -> Option<Subject> {
    match component.kind.as_str() {
        "crate" if component.source.as_deref() == Some("crates.io") => Some(Subject {
            ecosystem: "crates.io",
            package: component.name.clone(),
            version: component.version.clone(),
        }),
        "pin" => pin_subject(&component.name, &component.version),
        _ => None,
    }
}

/// The subject of one pinned artifact.
fn pin_subject(name: &str, version: &str) -> Option<Subject> {
    let pin = |ecosystem, package: &str, version: &str| Subject {
        ecosystem,
        package: package.to_owned(),
        version: version.to_owned(),
    };
    match name {
        // The guest kernel is Quoro's own build of an upstream release; only
        // the upstream part is what the Linux records name.
        "guest-kernel" => Some(pin(LINUX, "Kernel", upstream_version(version)?)),
        // The OSV records name the project, not the binary.
        "cloud-hypervisor" => Some(pin(
            "crates.io",
            "cloud-hypervisor",
            without_leading_v(version),
        )),
        "cloudflared" => Some(pin("Go", "github.com/cloudflare/cloudflared", version)),
        "buildkit" => Some(pin(
            "Go",
            "github.com/moby/buildkit",
            without_leading_v(version),
        )),
        _ => None,
    }
}

/// What a node's kernel release is matched as, or `None` when the string names
/// no upstream release.
#[must_use]
pub fn kernel_subject(kernel: &str) -> Option<Subject> {
    Some(Subject {
        ecosystem: LINUX,
        package: "Kernel".to_owned(),
        version: upstream_version(kernel)?.to_owned(),
    })
}

/// The upstream part of a kernel release: the leading run of digits and dots
/// (`7.2.15-q1` is `7.2.15`, `6.12.48+deb13-amd64` is `6.12.48`).
///
/// A string with no leading release, or one the kernel order cannot read, names
/// nothing to match.
#[must_use]
pub fn upstream_version(version: &str) -> Option<&str> {
    let end = version
        .find(|character: char| !(character.is_ascii_digit() || character == '.'))
        .unwrap_or(version.len());
    let upstream = version.get(..end)?.trim_end_matches('.');
    // The kernel order refuses an empty release and a component too large to
    // compare, so this also rejects "v6.1" and "6.999999999999999999999".
    version::compare(Order::Kernel, upstream, upstream)?;
    Some(upstream)
}

/// A version without the leading `v` a release tag carries.
fn without_leading_v(version: &str) -> &str {
    version.strip_prefix('v').unwrap_or(version)
}

/// Asks the mirror for every ecosystem these subjects need, except `Linux`,
/// which the mirror always fetches.
pub fn want_ecosystems(state: &AppState) {
    let mut seen = BTreeSet::new();
    for component in components::embedded() {
        let Some(subject) = subject_for(component) else {
            continue;
        };
        if subject.ecosystem != LINUX && seen.insert(subject.ecosystem) {
            state.vulns.want(subject.ecosystem);
        }
    }
}

/// Reconciles the findings of the running release's own components.
///
/// # Errors
///
/// A database error, or an error writing the change log.
pub async fn reconcile_quoro(state: &AppState) -> Result<Reconciled, ApiError> {
    let subjects = quoro_subjects();
    if subjects.is_empty() {
        // An empty list is a broken embed, not an upgrade: resolving every
        // finding from it would claim the components are gone.
        tracing::warn!("the embedded component list names nothing to match");
        return Ok(Reconciled::default());
    }
    let cache = state.vulns.cache();
    // Never resolve a finding because data is missing.
    let ecosystems: BTreeSet<&str> = subjects
        .iter()
        .map(|subject| subject.subject.ecosystem)
        .collect();
    for name in ecosystems {
        let Some(bucket) = ecosystem::bucket_for(name) else {
            continue;
        };
        if cache.bucket(bucket).await?.is_none() {
            tracing::debug!(ecosystem = name, "vulnerability data is still pending");
            return Ok(Reconciled::default());
        }
    }

    let mut desired = Vec::new();
    for subject in &subjects {
        let candidates = matcher::match_package(
            cache,
            subject.subject.ecosystem,
            &subject.subject.package,
            &subject.subject.version,
        )
        .await?;
        for mut candidate in candidates {
            // The row names the component's own version, not the upstream part
            // the records are matched with (`7.2.15-q1`, not `7.2.15`).
            candidate.installed_version = subject.installed.clone();
            desired.push(DesiredFinding {
                component: Some(subject.component.clone()),
                node_id: None,
                candidate,
            });
        }
    }
    findings::apply_subject(state, "quoro", desired, quoro_reason).await
}

/// The distinct components of the embedded list that are matched, each once
/// however many architectures the list names.
fn quoro_subjects() -> Vec<QuoroSubject> {
    let mut subjects: BTreeMap<(String, String), QuoroSubject> = BTreeMap::new();
    for component in components::embedded() {
        let Some(subject) = subject_for(component) else {
            continue;
        };
        let key = (component.name.clone(), component.version.clone());
        subjects.entry(key).or_insert_with(|| QuoroSubject {
            component: component.name.clone(),
            installed: component.version.clone(),
            subject,
        });
    }
    subjects.into_values().collect()
}

/// Why a `quoro` finding no longer matches: `upgraded` when the running
/// release no longer ships that component at that version, otherwise the data
/// changed under it.
fn quoro_reason(row: &FindingRow) -> &'static str {
    let Some(component) = row.component() else {
        return "upgraded";
    };
    let installed = row.installed_version();
    if components::embedded()
        .iter()
        .any(|candidate| candidate.name == component && candidate.version == installed)
    {
        "data_changed"
    } else {
        "upgraded"
    }
}

/// Every node with the `node` role, removed ones included: a removed node's
/// finding still needs its reason.
///
/// # Errors
///
/// A database error, or a stored `roles`/`kvm` value that is not what the
/// schema says it is.
pub async fn node_kernels(state: &AppState) -> Result<Vec<NodeKernel>, ApiError> {
    let rows = sqlx::query("SELECT id, roles, kvm, status FROM nodes ORDER BY id")
        .fetch_all(state.store.pool())
        .await?;
    let mut nodes = Vec::new();
    for row in &rows {
        let roles: String = row.try_get("roles")?;
        let roles: Vec<String> = serde_json::from_str(&roles)
            .map_err(|error| ApiError::internal(&format!("stored node roles: {error}")))?;
        if !roles.iter().any(|role| role == "node") {
            continue;
        }
        let kvm: String = row.try_get("kvm")?;
        let kvm: Value = serde_json::from_str(&kvm)
            .map_err(|error| ApiError::internal(&format!("stored node kvm: {error}")))?;
        nodes.push(NodeKernel {
            node_id: row.try_get("id")?,
            kernel: kvm
                .get("kernel")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            status: row.try_get("status")?,
        });
    }
    Ok(nodes)
}

/// Reconciles the findings of every node's host kernel.
///
/// # Errors
///
/// A database error, or an error writing the change log.
pub async fn reconcile_host_kernels(
    state: &AppState,
    nodes: &[NodeKernel],
) -> Result<Reconciled, ApiError> {
    let cache = state.vulns.cache();
    if cache.bucket(LINUX).await?.is_none() {
        tracing::debug!("the Linux vulnerability data is still pending");
        return Ok(Reconciled::default());
    }

    let mut desired = Vec::new();
    for node in nodes {
        if node.status == "removed" {
            continue;
        }
        let Some(subject) = kernel_subject(&node.kernel) else {
            continue;
        };
        let candidates =
            matcher::match_package(cache, subject.ecosystem, &subject.package, &subject.version)
                .await?;
        for mut candidate in candidates {
            // The row names the whole release the node reported.
            candidate.installed_version = node.kernel.clone();
            desired.push(DesiredFinding {
                component: None,
                node_id: Some(node.node_id.clone()),
                candidate,
            });
        }
    }
    findings::apply_subject(state, "host_kernel", desired, |row| {
        kernel_reason(row, nodes)
    })
    .await
}

/// Why a `host_kernel` finding no longer matches: the node is gone or removed,
/// its kernel release changed, or the data changed under it.
fn kernel_reason(row: &FindingRow, nodes: &[NodeKernel]) -> &'static str {
    let node = row
        .node_id()
        .and_then(|node_id| nodes.iter().find(|node| node.node_id == node_id));
    match node {
        None => "node_removed",
        Some(node) if node.status == "removed" => "node_removed",
        Some(node) if node.kernel != row.installed_version() => "upgraded",
        Some(_) => "data_changed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn component(kind: &str, name: &str, version: &str, source: Option<&str>) -> Component {
        Component {
            kind: kind.to_owned(),
            name: name.to_owned(),
            version: version.to_owned(),
            purl: format!("pkg:generic/{name}@{version}"),
            license: None,
            source: source.map(str::to_owned),
            arch: None,
            sha256: None,
        }
    }

    fn subject(ecosystem: &'static str, package: &str, version: &str) -> Subject {
        Subject {
            ecosystem,
            package: package.to_owned(),
            version: version.to_owned(),
        }
    }

    #[test]
    fn a_crates_io_crate_is_matched_and_a_local_one_is_not() {
        assert_eq!(
            subject_for(&component("crate", "tokio", "1.47.1", Some("crates.io"))),
            Some(subject("crates.io", "tokio", "1.47.1"))
        );
        assert_eq!(
            subject_for(&component("crate", "quoro", "0.1.1", Some("path"))),
            None
        );
        assert_eq!(
            subject_for(&component("crate", "quoro", "0.1.1", Some("git"))),
            None
        );
        assert_eq!(
            subject_for(&component("crate", "tokio", "1.47.1", None)),
            None,
            "a crate with no source is not known to be published"
        );
    }

    #[test]
    fn the_four_pins_are_matched_by_their_upstream_names() {
        let cases = [
            (
                component("pin", "guest-kernel", "7.2.15-q1", None),
                subject("Linux", "Kernel", "7.2.15"),
            ),
            (
                component("pin", "cloud-hypervisor", "v54.0", None),
                subject("crates.io", "cloud-hypervisor", "54.0"),
            ),
            (
                component("pin", "cloudflared", "2026.10.7", None),
                subject("Go", "github.com/cloudflare/cloudflared", "2026.10.7"),
            ),
            (
                component("pin", "buildkit", "v0.34.2", None),
                subject("Go", "github.com/moby/buildkit", "0.34.2"),
            ),
        ];
        for (component, expected) in cases {
            assert_eq!(subject_for(&component), Some(expected), "{component:?}");
        }
    }

    #[test]
    fn npm_and_unknown_components_are_not_matched() {
        assert_eq!(
            subject_for(&component("npm", "react", "19.1.0", None)),
            None
        );
        assert_eq!(subject_for(&component("pin", "curl", "8.0", None)), None);
        assert_eq!(subject_for(&component("other", "x", "1", None)), None);
    }

    #[test]
    fn the_upstream_part_is_the_leading_release() {
        assert_eq!(upstream_version("7.2.15-q1"), Some("7.2.15"));
        assert_eq!(upstream_version("6.12.48+deb13-amd64"), Some("6.12.48"));
        assert_eq!(upstream_version("6.11.2"), Some("6.11.2"));
        assert_eq!(upstream_version("6.12.113.0-1"), Some("6.12.113.0"));
        assert_eq!(
            upstream_version("6.1."),
            Some("6.1"),
            "a trailing dot is trimmed"
        );
        assert_eq!(upstream_version(""), None);
        assert_eq!(upstream_version("v6.1"), None);
        assert_eq!(upstream_version("quoro"), None);
        assert_eq!(upstream_version("6.999999999999999999999999"), None);
    }

    #[test]
    fn a_host_kernel_maps_to_the_linux_kernel_package() {
        assert_eq!(
            kernel_subject("6.12.48+deb13-amd64"),
            Some(subject("Linux", "Kernel", "6.12.48"))
        );
        assert_eq!(
            kernel_subject("6.11.2"),
            Some(subject("Linux", "Kernel", "6.11.2"))
        );
        assert_eq!(kernel_subject(""), None);
        assert_eq!(kernel_subject("unknown"), None);
    }

    fn candidate(package: &str, installed: &str) -> matcher::Candidate {
        matcher::Candidate {
            vuln_id: "CVE-2026-98432".to_owned(),
            aliases: Vec::new(),
            summary: None,
            ecosystem: "Linux".to_owned(),
            package: package.to_owned(),
            source_package: None,
            installed_version: installed.to_owned(),
            fixed_version: Some("7.2.10".to_owned()),
            severity: "critical",
            cvss_score: Some(9.8),
            cvss_vector: None,
            kev: false,
        }
    }

    fn row(component: Option<&str>, installed: &str) -> FindingRow {
        FindingRow::new_subject(
            "quoro",
            component,
            None,
            &candidate("Kernel", installed),
            "now",
        )
    }

    #[test]
    fn a_host_kernel_reason_follows_the_node() {
        let kernel = FindingRow::new_subject(
            "host_kernel",
            None,
            Some("nod_a"),
            &candidate("Kernel", "7.2.8-1-amd64"),
            "now",
        );
        let connected = NodeKernel {
            node_id: "nod_a".to_owned(),
            kernel: "7.2.15".to_owned(),
            status: "connected".to_owned(),
        };
        assert_eq!(kernel.installed_version(), "7.2.8-1-amd64");
        assert_eq!(
            kernel_reason(&kernel, std::slice::from_ref(&connected)),
            "upgraded"
        );
        assert_eq!(
            kernel_reason(
                &kernel,
                &[NodeKernel {
                    status: "removed".to_owned(),
                    ..connected.clone()
                }]
            ),
            "node_removed"
        );
        assert_eq!(kernel_reason(&kernel, &[]), "node_removed");
        assert_eq!(
            kernel_reason(
                &kernel,
                &[NodeKernel {
                    kernel: kernel.installed_version().to_owned(),
                    ..connected
                }]
            ),
            "data_changed"
        );
    }

    #[test]
    fn a_quoro_reason_follows_the_embedded_list() {
        let current = components::embedded()
            .iter()
            .find(|component| component.name == "cloud-hypervisor")
            .expect("the cloud-hypervisor pin");
        let at_current = row(Some("cloud-hypervisor"), &current.version);
        assert_eq!(quoro_reason(&at_current), "data_changed");
        let at_older = row(Some("cloud-hypervisor"), "v1.0.0");
        assert_eq!(quoro_reason(&at_older), "upgraded");
        let gone = row(Some("gone-crate"), "1.0.0");
        assert_eq!(quoro_reason(&gone), "upgraded");
    }
}
