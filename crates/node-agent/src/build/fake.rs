// SPDX-License-Identifier: AGPL-3.0-only
//! [`FakeBuilder`]: an in-memory [`Builder`] for tests, mirroring
//! `FakeEngine`.
//!
//! A test scripts the outcome of one build id (or of every build), and the
//! fake plays it: it emits the phase, commit and line events a real builder
//! would, with a short delay so a caller observing the store sees the build
//! move through them, and it honours its cancel channel. Its success leaves a
//! layout file on disk, because that is what
//! [`Engine::prepare_layout`](quoro_engine::Engine::prepare_layout) needs.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use quoro_engine::{ImageDigest, OciLayout};
use quoro_proto::wire::{BuildPhase, FailedStep, LogStream};
use tokio::sync::{mpsc, watch};

use crate::util;

use super::{BuildEvent, BuildFailure, BuildOutput, BuildSpec, Builder};

/// The digest a success reports when the test names none.
pub const DEFAULT_DIGEST: &str =
    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
/// The commit a checkout resolves to.
pub const DEFAULT_COMMIT: &str = "cccccccccccccccccccccccccccccccccccccccc";

/// What one scripted build does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeOutcome {
    /// Succeed with this manifest digest.
    Succeed(String),
    /// Fail in this phase, with this reason and optional BuildKit step.
    Fail {
        /// The phase the build was in.
        phase: BuildPhase,
        /// The one-line reason.
        error: String,
        /// The failing step, when a step failed.
        step: Option<FailedStep>,
    },
    /// Run until the build is cancelled.
    Hang,
    /// Report a capacity problem once, then succeed with this digest.
    CapacityOnce(String),
}

/// The fake builder's state.
struct State {
    scripts: HashMap<String, VecDeque<FakeOutcome>>,
    default: FakeOutcome,
    /// A build that hit [`FakeOutcome::CapacityOnce`] succeeds with this digest
    /// the next time it runs.
    retry: HashMap<String, String>,
    specs: Vec<BuildSpec>,
    cleanups: Vec<String>,
    cache_removals: Vec<String>,
    delay: Duration,
    layout_dir: PathBuf,
}

/// An in-memory [`Builder`] for tests.
pub struct FakeBuilder {
    state: Mutex<State>,
}

impl Default for FakeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeBuilder {
    /// A fake whose builds all succeed with [`DEFAULT_DIGEST`] after a short
    /// delay.
    #[must_use]
    pub fn new() -> Self {
        let layout_dir = std::env::temp_dir().join(format!(
            "quoro-fake-builder-{}",
            util::hex(&util::random_bytes::<8>())
        ));
        Self {
            state: Mutex::new(State {
                scripts: HashMap::new(),
                default: FakeOutcome::Succeed(DEFAULT_DIGEST.to_owned()),
                retry: HashMap::new(),
                specs: Vec::new(),
                cleanups: Vec::new(),
                cache_removals: Vec::new(),
                delay: Duration::from_millis(5),
                layout_dir,
            }),
        }
    }

    /// Sets how long the fake waits between its events.
    #[must_use]
    pub fn with_delay(self, delay: Duration) -> Self {
        self.lock().delay = delay;
        self
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Scripts the outcome of one build id. Several outcomes run in order, one
    /// per call of that build id.
    pub fn script(&self, build_id: &str, outcome: FakeOutcome) {
        self.lock()
            .scripts
            .entry(build_id.to_owned())
            .or_default()
            .push_back(outcome);
    }

    /// Scripts the outcome of every build no script names.
    pub fn set_default(&self, outcome: FakeOutcome) {
        self.lock().default = outcome;
    }

    /// Every spec the fake was asked to build, in order.
    #[must_use]
    pub fn specs(&self) -> Vec<BuildSpec> {
        self.lock().specs.clone()
    }

    /// The build ids `cleanup` was called for, in order.
    #[must_use]
    pub fn cleanups(&self) -> Vec<String> {
        self.lock().cleanups.clone()
    }

    /// The app ids `remove_cache` was called for, in order.
    #[must_use]
    pub fn cache_removals(&self) -> Vec<String> {
        self.lock().cache_removals.clone()
    }

    /// How many builds the fake was asked to run.
    #[must_use]
    pub fn build_count(&self) -> usize {
        self.lock().specs.len()
    }

    fn delay(&self) -> Duration {
        self.lock().delay
    }

    fn next_outcome(&self, build_id: &str, spec: &BuildSpec) -> FakeOutcome {
        let mut state = self.lock();
        state.specs.push(spec.clone());
        if let Some(outcome) = state
            .scripts
            .get_mut(build_id)
            .and_then(std::collections::VecDeque::pop_front)
        {
            if let FakeOutcome::CapacityOnce(digest) = &outcome {
                state.retry.insert(build_id.to_owned(), digest.clone());
            }
            return outcome;
        }
        if let Some(digest) = state.retry.remove(build_id) {
            return FakeOutcome::Succeed(digest);
        }
        state.default.clone()
    }

    /// Writes the layout file a success returns and returns its path.
    fn write_layout(&self, build_id: &str) -> PathBuf {
        let state = self.lock();
        let safe: String = build_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let path = state.layout_dir.join(format!("{safe}.oci"));
        let dir = state.layout_dir.clone();
        drop(state);
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(&path, b"{\"schemaVersion\":2}");
        path
    }
}

impl Builder for FakeBuilder {
    async fn build(
        &self,
        spec: &BuildSpec,
        events: mpsc::Sender<BuildEvent>,
        cancel: watch::Receiver<bool>,
    ) -> Result<BuildOutput, BuildFailure> {
        let outcome = self.next_outcome(&spec.build_id, spec);
        let delay = self.delay();
        let mut cancel = cancel;
        let mut phase = BuildPhase::Preparing;

        match outcome {
            FakeOutcome::CapacityOnce(_) => Err(BuildFailure {
                phase: BuildPhase::Preparing,
                error: format!(
                    "node capacity: {} vCPU(s) and {} MiB do not fit",
                    spec.limits.vcpus, spec.limits.memory_mb
                ),
                step: None,
                cancelled: false,
            }),
            FakeOutcome::Fail { phase, error, step } => {
                let _ = events
                    .send(BuildEvent::Line {
                        stream: LogStream::Stderr,
                        vertex: step.as_ref().map(|step| step.vertex),
                        text: error.clone(),
                    })
                    .await;
                Err(BuildFailure {
                    phase,
                    error,
                    step,
                    cancelled: false,
                })
            }
            FakeOutcome::Hang => loop {
                if *cancel.borrow() {
                    return Err(cancelled(phase));
                }
                tokio::select! {
                    () = tokio::time::sleep(delay.max(Duration::from_millis(1))) => {}
                    changed = cancel.changed() => {
                        if changed.is_err() {
                            // The manager is gone; nothing will cancel us.
                            std::future::pending::<()>().await;
                        }
                    }
                }
            },
            FakeOutcome::Succeed(digest) => {
                let _ = events.send(BuildEvent::Phase(phase)).await;
                tokio::time::sleep(delay).await;
                if *cancel.borrow() {
                    return Err(cancelled(phase));
                }
                let _ = events
                    .send(BuildEvent::Commit(DEFAULT_COMMIT.to_owned()))
                    .await;
                let _ = events
                    .send(BuildEvent::Line {
                        stream: LogStream::Stdout,
                        vertex: None,
                        text: "fetching the source".to_owned(),
                    })
                    .await;

                phase = BuildPhase::Building;
                tokio::time::sleep(delay).await;
                if *cancel.borrow() {
                    return Err(cancelled(phase));
                }
                let _ = events.send(BuildEvent::Phase(phase)).await;
                let _ = events
                    .send(BuildEvent::Line {
                        stream: LogStream::Stdout,
                        vertex: Some(1),
                        text: "#1 [1/1] RUN echo built".to_owned(),
                    })
                    .await;

                phase = BuildPhase::Exporting;
                tokio::time::sleep(delay).await;
                if *cancel.borrow() {
                    return Err(cancelled(phase));
                }
                let _ = events.send(BuildEvent::Phase(phase)).await;
                let _ = events
                    .send(BuildEvent::Line {
                        stream: LogStream::Stdout,
                        vertex: None,
                        text: "writing the OCI layout".to_owned(),
                    })
                    .await;
                let path = self.write_layout(&spec.build_id);
                Ok(BuildOutput {
                    layout: OciLayout {
                        path,
                        manifest_digest: ImageDigest(digest),
                    },
                    commit: DEFAULT_COMMIT.to_owned(),
                })
            }
        }
    }

    async fn cleanup(&self, build_id: &str) {
        self.lock().cleanups.push(build_id.to_owned());
    }

    async fn remove_cache(&self, app_id: &str) {
        self.lock().cache_removals.push(app_id.to_owned());
    }
}

fn cancelled(phase: BuildPhase) -> BuildFailure {
    BuildFailure {
        phase,
        error: "cancelled".to_owned(),
        step: None,
        cancelled: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use quoro_proto::wire::BuildLimits;

    use super::super::{BuildSource, BuilderNet, Frontend};

    fn spec(build_id: &str) -> BuildSpec {
        BuildSpec {
            build_id: build_id.to_owned(),
            app_id: "app_test".to_owned(),
            source: BuildSource::Git {
                url: "https://example.test/app.git".to_owned(),
                git_ref: "main".to_owned(),
            },
            frontend: Frontend::Dockerfile {
                path: "Dockerfile".to_owned(),
                target: None,
            },
            context_dir: ".".to_owned(),
            build_args: BTreeMap::new(),
            secrets: BTreeMap::new(),
            limits: BuildLimits::default(),
            refresh_base: false,
            net: BuilderNet {
                ip: std::net::Ipv4Addr::new(10, 42, 1, 9),
                gateway: std::net::Ipv4Addr::new(10, 42, 1, 1),
            },
        }
    }

    #[tokio::test]
    async fn a_success_writes_a_layout_and_emits_events() {
        let builder = FakeBuilder::new().with_delay(Duration::ZERO);
        let (tx, mut rx) = mpsc::channel(16);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let output = builder
            .build(&spec("bld_1"), tx, cancel_rx)
            .await
            .expect("the fake succeeds");
        assert!(output.layout.path.exists());
        assert_eq!(output.layout.manifest_digest.0, DEFAULT_DIGEST);
        assert_eq!(builder.specs().len(), 1);

        let mut phases = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let BuildEvent::Phase(phase) = event {
                phases.push(phase);
            }
        }
        assert_eq!(
            phases,
            vec![
                BuildPhase::Preparing,
                BuildPhase::Building,
                BuildPhase::Exporting
            ]
        );
    }

    #[tokio::test]
    async fn capacity_once_then_succeeds() {
        let builder = FakeBuilder::new().with_delay(Duration::ZERO);
        builder.script("bld_1", FakeOutcome::CapacityOnce("sha256:aa".to_owned()));
        let (tx, _rx) = mpsc::channel(16);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let failure = builder
            .build(&spec("bld_1"), tx.clone(), cancel_rx)
            .await
            .expect_err("the first run reports capacity");
        assert!(failure.error.starts_with("node capacity: "));

        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let output = builder
            .build(&spec("bld_1"), tx, cancel_rx)
            .await
            .expect("the second run succeeds");
        assert_eq!(output.layout.manifest_digest.0, "sha256:aa");
    }

    #[tokio::test]
    async fn hanging_until_cancelled_reports_cancelled() {
        let builder = FakeBuilder::new().with_delay(Duration::from_millis(1));
        builder.script("bld_1", FakeOutcome::Hang);
        let (tx, _rx) = mpsc::channel(16);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let handle = tokio::spawn({
            let builder = std::sync::Arc::new(builder);
            let inner = std::sync::Arc::clone(&builder);
            async move {
                let spec = spec("bld_1");
                let outcome = inner.build(&spec, tx, cancel_rx).await;
                (builder, outcome)
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel_tx.send(true).unwrap();
        let (_builder, outcome) = handle.await.unwrap();
        let failure = outcome.expect_err("a cancelled build fails");
        assert!(failure.cancelled);
    }
}
