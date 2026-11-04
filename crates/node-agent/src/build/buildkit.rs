// SPDX-License-Identifier: AGPL-3.0-only
//! [`DockerfileBuilder`]: BuildKit's Dockerfile frontend in an ephemeral
//! builder microVM (ADR 0117, ADR 0118).
//!
//! One build is one microVM created through the [`Engine`](quoro_engine::Engine)
//! with `egress_only` networking and no isolation group, so a hostile
//! Dockerfile reaches the outside and nothing else on the node. The VM carries
//! three work disks the guest finds by serial:
//!
//! - `quoro-work`, the app's persistent BuildKit cache, which only the builder
//!   VM ever mounts;
//! - `quoro-src`, the tar [`source`](super::source) wrote before boot;
//! - `quoro-out`, where the builder writes the OCI layout tar, read afterwards
//!   in user space by [`layout_manifest_digest`].
//!
//! The host never mounts a disk the builder wrote. The VM's id is the build id
//! (`bld_`), so the reconciler neither reports nor removes it as an app VM.
//!
//! The builder image is pinned by digest in `crates/node-agent/pins.txt` and
//! prepared with the engine's normal `prepare_image` path, once per node.
//!
//! A build the node cannot fit yet fails in `preparing` with an error that
//! starts with [`CAPACITY_ERROR_PREFIX`], so the build manager can queue it
//! again later; no new public type carries that distinction.

use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::net::Ipv4Addr;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use quoro_engine::{
    ConsoleMode, Engine, EngineError, EngineEvent, ImageDigest, ImageRef, InitConfig, MacAddr,
    NetAttach, OciLayout, OutputStream, PreparedImage, SerialMode, StopMode, VmId, VmInfo, VmSpec,
    WorkDisk,
};
use quoro_proto::wire::{BuildPhase, FailedStep, LogStream};
use tokio::sync::{mpsc, watch};

use super::progress::ProgressParser;
use super::source::{fetch_commit, validate_secrets, write_source_disk};
use super::{
    BuildEvent, BuildFailure, BuildOutput, BuildSource, BuildSpec, Builder, Frontend,
    MAX_ERROR_CHARS, truncate_chars,
};

/// The pins file, verbatim.
pub const PINS_TXT: &str = include_str!("../../pins.txt");

/// The name of the builder image pin in `pins.txt`.
const BUILDER_PIN: &str = "buildkit-image";

/// The media type of an image manifest in an OCI `index.json`.
const MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

/// The prefix of a failure the manager must retry later: the node has no room
/// for the builder VM right now. The build failed in
/// [`BuildPhase::Preparing`] with this prefix, and nothing else did.
pub const CAPACITY_ERROR_PREFIX: &str = "node capacity: ";

/// The builder VM's writable data disk, which only holds the guest's overlay.
const BUILDER_DISK_GB: u32 = 4;
/// The declared size of the source disk; the node writes the file itself, so
/// the engine only uses this when the file is missing.
const SOURCE_DISK_GB: u32 = 1;
/// How long the builder's remaining output is read after its VM exited.
const DRAIN: Duration = Duration::from_secs(2);
/// The most the layout's `index.json` may be.
const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024;
/// The most entries the layout tar may hold before it is refused.
const MAX_LAYOUT_ENTRIES: usize = 100_000;
/// The longest relative path a spec may name.
const MAX_PATH_CHARS: usize = 500;
/// The longest build-arg value.
const MAX_ARG_BYTES: usize = 64 * 1024;
/// The most build args one build may carry.
const MAX_ARGS: usize = 256;
/// One byte in a gibibyte.
const GIB: u64 = 1024 * 1024 * 1024;

/// Everything the builder needs from the node.
#[derive(Clone, Debug)]
pub struct BuilderConfig {
    /// Root of per-build scratch directories and of the cache disks, for
    /// example `<data_dir>/builds`.
    pub builds_dir: PathBuf,
    /// Absolute path of the host's `git`, or `None` when it is not installed
    /// (builds then fail in `preparing` with "git is not installed on this
    /// node").
    pub git: Option<PathBuf>,
    /// The pinned BuildKit image (from [`pinned_image`]).
    pub image: ImageRef,
    /// Wall-clock limit of the Git fetch, default 5 minutes.
    pub fetch_timeout: Duration,
}

/// The default [`BuilderConfig::fetch_timeout`].
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(300);

/// The BuildKit Dockerfile frontend, one builder microVM per build.
pub struct DockerfileBuilder<E: Engine> {
    engine: Arc<E>,
    config: BuilderConfig,
}

impl<E: Engine + 'static> DockerfileBuilder<E> {
    /// A builder that creates its VMs through `engine`.
    #[must_use]
    pub fn new(engine: Arc<E>, config: BuilderConfig) -> Self {
        Self { engine, config }
    }

    /// `<builds_dir>/run/<build_id>/`.
    fn run_dir(&self, build_id: &str) -> PathBuf {
        self.config.builds_dir.join("run").join(build_id)
    }

    /// `<builds_dir>/cache/<app_id>.ext4`.
    fn cache_path(&self, app_id: &str) -> PathBuf {
        self.config
            .builds_dir
            .join("cache")
            .join(format!("{app_id}.ext4"))
    }

    /// Creates the directories a build writes to, all `0700`: the run tree,
    /// the cache tree and this build's run directory.
    fn prepare_directories(&self, spec: &BuildSpec) -> Result<(), String> {
        for directory in [
            self.config.builds_dir.join("run"),
            self.config.builds_dir.join("cache"),
            self.run_dir(&spec.build_id),
        ] {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&directory)
                .map_err(|e| format!("could not create {directory:?}: {e}"))?;
        }
        Ok(())
    }

    /// The builder VM's spec: its console socket, its three work disks and the
    /// guest script as the entrypoint.
    fn vm_spec(&self, spec: &BuildSpec, prepared: &PreparedImage, script: String) -> VmSpec {
        let run_dir = self.run_dir(&spec.build_id);
        VmSpec {
            id: VmId(spec.build_id.clone()),
            vcpus: u8::try_from(spec.limits.vcpus).unwrap_or(u8::MAX),
            memory_mb: spec.limits.memory_mb,
            rootfs: prepared.digest.clone(),
            disk_gb: BUILDER_DISK_GB,
            volumes: Vec::new(),
            net: NetAttach {
                ip: spec.net.ip,
                gateway: spec.net.gateway,
                mac: mac_for(spec.net.ip),
                // A builder VM reaches the outside and nothing else on the
                // node (engine-trait.md section 3.3, ADR 0118).
                isolation_group: None,
                egress_only: true,
            },
            init: InitConfig {
                entrypoint: vec!["/bin/sh".to_owned(), "-c".to_owned(), script],
                cmd: Vec::new(),
                env: builder_env(&prepared.config.env),
                user: Some("0".to_owned()),
                workdir: Some("/".to_owned()),
            },
            console: ConsoleMode::VirtioConsole {
                socket: run_dir.join("console.sock"),
            },
            // Boot diagnostics go to the VM's own serial log. No vsock: the
            // builder runs untrusted Dockerfiles and takes no exec session.
            serial: SerialMode::Off,
            vsock: None,
            work_disks: vec![
                WorkDisk {
                    path: self.cache_path(&spec.app_id),
                    size_gb: spec.limits.cache_gb,
                    serial: "quoro-work".to_owned(),
                },
                WorkDisk {
                    path: run_dir.join("src.disk"),
                    size_gb: SOURCE_DISK_GB,
                    serial: "quoro-src".to_owned(),
                },
                WorkDisk {
                    path: run_dir.join("out.disk"),
                    size_gb: spec.limits.output_gb,
                    serial: "quoro-out".to_owned(),
                },
            ],
        }
    }

    /// The guest script: mounts the work disk, extracts the secrets into
    /// memory and the context onto the work disk, runs `buildkitd`, runs one
    /// `buildctl build`, and prunes the cache before it exits with BuildKit's
    /// code.
    ///
    /// Every value from the spec goes through [`sh_quote`]; nothing else is
    /// ever interpolated into the script.
    fn script(&self, spec: &BuildSpec, plan: &DockerfilePlan) -> String {
        let context = format!("/mnt/work/ctx/src/{}", plan.context_dir);
        let dockerfile = if plan.dockerfile_dir == "." {
            context.clone()
        } else {
            format!("{context}/{}", plan.dockerfile_dir)
        };
        let keep_mb = u64::from(spec.limits.cache_gb) * 1024 * 80 / 100;
        let mut lines: Vec<String> = vec![
            "echo 'quoro-build phase=preparing'".to_owned(),
            "work=''".to_owned(),
            "src=''".to_owned(),
            "out=''".to_owned(),
            "for disk in /sys/block/vd*; do".to_owned(),
            "    [ -e \"$disk\" ] || continue".to_owned(),
            "    serial=$(cat \"$disk/serial\" 2>/dev/null) || continue".to_owned(),
            "    case \"$serial\" in".to_owned(),
            "        quoro-work) work=$(basename \"$disk\") ;;".to_owned(),
            "        quoro-src) src=$(basename \"$disk\") ;;".to_owned(),
            "        quoro-out) out=$(basename \"$disk\") ;;".to_owned(),
            "    esac".to_owned(),
            "done".to_owned(),
            "if [ -z \"$work\" ] || [ -z \"$src\" ] || [ -z \"$out\" ]; then".to_owned(),
            "    echo 'error: the builder VM is missing one of its disks' >&2".to_owned(),
            "    exit 91".to_owned(),
            "fi".to_owned(),
            "mkdir -p /mnt/work || { echo 'error: the builder VM cannot create /mnt/work' >&2; exit 92; }"
                .to_owned(),
            "mount -t ext4 -o nosuid,nodev \"/dev/$work\" /mnt/work || { echo 'error: the builder VM cannot mount its work disk' >&2; exit 92; }"
                .to_owned(),
            "rm -rf /mnt/work/ctx".to_owned(),
            "mkdir -p /mnt/work/ctx /mnt/work/buildkit /run/buildkit || { echo 'error: the builder VM cannot prepare its work disk' >&2; exit 92; }"
                .to_owned(),
            "mkdir -p /dev/shm/q && chmod 700 /dev/shm/q || { echo 'error: the builder VM cannot create the secret directory' >&2; exit 92; }"
                .to_owned(),
        ];
        if !spec.secrets.is_empty() {
            lines.push(
                "tar -x -f \"/dev/$src\" -C /dev/shm/q secrets || { echo 'error: the builder VM cannot extract the build secrets' >&2; exit 93; }"
                    .to_owned(),
            );
        }
        lines.extend([
            "tar -x -f \"/dev/$src\" -C /mnt/work/ctx src || { echo 'error: the builder VM cannot extract the build context' >&2; exit 93; }"
                .to_owned(),
            "buildkitd --root /mnt/work/buildkit --oci-worker-net=host --addr unix:///run/buildkit/buildkitd.sock >/dev/null 2>&1 &"
                .to_owned(),
            "daemon=$!".to_owned(),
            "ready=0".to_owned(),
            "tries=0".to_owned(),
            "while [ \"$tries\" -lt 30 ]; do".to_owned(),
            "    if buildctl --addr unix:///run/buildkit/buildkitd.sock debug workers >/dev/null 2>&1; then"
                .to_owned(),
            "        ready=1".to_owned(),
            "        break".to_owned(),
            "    fi".to_owned(),
            "    tries=$((tries + 1))".to_owned(),
            "    sleep 1".to_owned(),
            "done".to_owned(),
            "if [ \"$ready\" -ne 1 ]; then".to_owned(),
            "    echo 'error: the BuildKit daemon did not start' >&2".to_owned(),
            "    kill \"$daemon\" 2>/dev/null".to_owned(),
            "    wait \"$daemon\" 2>/dev/null".to_owned(),
            "    exit 90".to_owned(),
            "fi".to_owned(),
            "echo 'quoro-build phase=building'".to_owned(),
        ]);

        let mut build = String::from(
            "buildctl build --frontend dockerfile.v0 \
             --local ",
        );
        build.push_str(&sh_quote(&format!("context={context}")));
        build.push_str(" --local ");
        build.push_str(&sh_quote(&format!("dockerfile={dockerfile}")));
        build.push_str(" --opt ");
        build.push_str(&sh_quote(&format!("filename={}", plan.dockerfile_name)));
        if let Some(target) = &plan.target {
            build.push_str(" --opt ");
            build.push_str(&sh_quote(&format!("target={target}")));
        }
        for (name, value) in &plan.build_args {
            build.push_str(" --opt ");
            build.push_str(&sh_quote(&format!("build-arg:{name}={value}")));
        }
        for id in spec.secrets.keys() {
            build.push_str(" --secret ");
            build.push_str(&sh_quote(&format!("id={id},src=/dev/shm/q/secrets/{id}")));
        }
        build.push_str(" --progress=rawjson --output 'type=oci,dest=/dev/'\"$out\"");
        lines.push(build);
        lines.extend([
            "code=$?".to_owned(),
            "rm -rf /dev/shm/q".to_owned(),
            format!(
                "buildctl --addr unix:///run/buildkit/buildkitd.sock prune --keep-storage {keep_mb}MB >/dev/null 2>&1"
            ),
            "kill \"$daemon\" 2>/dev/null".to_owned(),
            "tries=0".to_owned(),
            "while [ \"$tries\" -lt 10 ] && kill -0 \"$daemon\" 2>/dev/null; do".to_owned(),
            "    tries=$((tries + 1))".to_owned(),
            "    sleep 1".to_owned(),
            "done".to_owned(),
            "kill -9 \"$daemon\" 2>/dev/null".to_owned(),
            "wait \"$daemon\" 2>/dev/null".to_owned(),
            "cd /".to_owned(),
            "sync".to_owned(),
            "exit \"$code\"".to_owned(),
        ]);
        lines.join("\n")
    }

    /// Sends one event, ignoring a closed receiver.
    async fn emit(&self, events: &mpsc::Sender<BuildEvent>, event: BuildEvent) {
        let _ = events.send(event).await;
    }

    /// Sends one phase event.
    async fn phase(&self, events: &mpsc::Sender<BuildEvent>, phase: BuildPhase) {
        self.emit(events, BuildEvent::Phase(phase)).await;
    }

    /// Sends one node-side line of the human log.
    async fn line(&self, events: &mpsc::Sender<BuildEvent>, text: String) {
        self.emit(
            events,
            BuildEvent::Line {
                stream: LogStream::Stdout,
                vertex: None,
                text,
            },
        )
        .await;
    }

    /// Stops the builder VM with a forced stop, best effort.
    async fn stop_vm(&self, id: &VmId) {
        if let Err(error) = self.engine.stop(id, StopMode::Force).await {
            tracing::warn!(vm_id = %id.0, error = ?error, "could not stop the builder VM");
        }
    }

    /// Deletes the builder VM, best effort.
    async fn delete_vm(&self, id: &VmId) {
        if let Err(error) = self.engine.delete(id).await
            && !matches!(error, EngineError::NotFound)
        {
            tracing::warn!(vm_id = %id.0, error = ?error, "could not delete the builder VM");
        }
    }

    /// Reads one output line, forwards the events it produced, remembers the
    /// current phase and the last plain `error:` line.
    async fn feed(
        &self,
        parser: &mut ProgressParser,
        phase: &mut BuildPhase,
        last_error: &mut Option<String>,
        events: &mpsc::Sender<BuildEvent>,
        line: quoro_engine::OutputLine,
    ) {
        let stream = match line.stream {
            OutputStream::Stdout => LogStream::Stdout,
            OutputStream::Stderr => LogStream::Stderr,
        };
        for event in parser.push(stream, &line.line) {
            if let BuildEvent::Phase(next) = &event {
                *phase = *next;
            }
            if let BuildEvent::Line {
                vertex: None, text, ..
            } = &event
                && text.trim_start().starts_with("error:")
            {
                *last_error = Some(truncate_chars(text.trim(), MAX_ERROR_CHARS));
            }
            self.emit(events, event).await;
        }
    }
}

impl<E: Engine + 'static> Builder for DockerfileBuilder<E> {
    async fn build(
        &self,
        spec: &BuildSpec,
        events: mpsc::Sender<BuildEvent>,
        cancel: watch::Receiver<bool>,
    ) -> Result<BuildOutput, BuildFailure> {
        let mut cancel = cancel;
        self.phase(&events, BuildPhase::Preparing).await;

        let plan = match validate_spec(spec) {
            Ok(plan) => plan,
            Err(error) => return Err(failure(BuildPhase::Preparing, error, None, false)),
        };
        if let Err(error) = self.prepare_directories(spec) {
            return Err(failure(BuildPhase::Preparing, error, None, false));
        }

        // 1. The source: resolve the ref, then write the source disk.
        let Some(git) = self.config.git.clone() else {
            return Err(failure(
                BuildPhase::Preparing,
                "git is not installed on this node".to_owned(),
                None,
                false,
            ));
        };
        let BuildSource::Git { url, git_ref } = &spec.source;
        self.line(&events, format!("fetching {url} {git_ref}"))
            .await;
        let repo = self.run_dir(&spec.build_id).join("repo.git");
        // A cancel during the fetch drops it, which kills Git.
        let commit = tokio::select! {
            fetched = fetch_commit(&git, url, git_ref, &repo, self.config.fetch_timeout) => {
                match fetched {
                    Ok(commit) => commit,
                    Err(error) => return Err(failure(BuildPhase::Preparing, error, None, false)),
                }
            }
            () = wait_cancel(&mut cancel) => return Err(cancelled(BuildPhase::Preparing)),
        };
        self.line(&events, format!("resolved {commit}")).await;
        self.emit(&events, BuildEvent::Commit(commit.clone())).await;
        if *cancel.borrow() {
            return Err(cancelled(BuildPhase::Preparing));
        }

        self.line(&events, "writing the source disk".to_owned())
            .await;
        let source_disk = self.run_dir(&spec.build_id).join("src.disk");
        tokio::select! {
            written = write_source_disk(&git, &repo, &commit, &spec.secrets, &source_disk) => {
                if let Err(error) = written {
                    return Err(failure(BuildPhase::Preparing, error, None, false));
                }
            }
            () = wait_cancel(&mut cancel) => return Err(cancelled(BuildPhase::Preparing)),
        }

        // 2. The output disk: an empty sparse file of the output limit.
        let output_disk = self.run_dir(&spec.build_id).join("out.disk");
        if let Err(error) = create_sparse(&output_disk, spec.limits.output_gb) {
            return Err(failure(BuildPhase::Preparing, error, None, false));
        }

        // 3. The builder image, cached after the first build.
        self.line(
            &events,
            format!(
                "preparing the builder image {}",
                self.config.image.reference
            ),
        )
        .await;
        let prepared = match self.engine.prepare_image(&self.config.image).await {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err(failure(
                    BuildPhase::Preparing,
                    error.to_string(),
                    None,
                    false,
                ));
            }
        };
        if *cancel.borrow() {
            return Err(cancelled(BuildPhase::Preparing));
        }

        // 4. The builder VM.
        let script = self.script(spec, &plan);
        let vm = self.vm_spec(spec, &prepared, script);
        let id = vm.id.clone();
        match self.engine.create(&vm).await {
            Ok(_) => {}
            Err(EngineError::ResourceExhausted(reason)) => {
                return Err(failure(
                    BuildPhase::Preparing,
                    format!("{CAPACITY_ERROR_PREFIX}{reason}"),
                    None,
                    false,
                ));
            }
            Err(error) => {
                return Err(failure(
                    BuildPhase::Preparing,
                    error.to_string(),
                    None,
                    false,
                ));
            }
        }

        // 5. Subscribe before boot, so the whole output is seen.
        let mut engine_events = self.engine.events();
        let mut output = match self.engine.output(&id) {
            Ok(output) => output,
            Err(error) => {
                self.delete_vm(&id).await;
                return Err(failure(
                    BuildPhase::Preparing,
                    error.to_string(),
                    None,
                    false,
                ));
            }
        };
        self.line(&events, "starting the builder VM".to_owned())
            .await;
        let deadline = tokio::time::sleep(Duration::from_secs(spec.limits.timeout_s));
        tokio::pin!(deadline);
        if let Err(error) = self.engine.start(&id).await {
            self.delete_vm(&id).await;
            return Err(failure(
                BuildPhase::Preparing,
                error.to_string(),
                None,
                false,
            ));
        }

        // 6. Follow the build until the VM goes away.
        let mut parser = ProgressParser::new();
        let mut phase = BuildPhase::Preparing;
        let mut last_error: Option<String> = None;
        let mut output_open = true;
        let mut events_open = true;
        let ending = loop {
            tokio::select! {
                line = output.next(), if output_open => match line {
                    Some(line) => {
                        self.feed(&mut parser, &mut phase, &mut last_error, &events, line).await;
                    }
                    None => output_open = false,
                },
                event = engine_events.next(), if events_open => match event {
                    Some(EngineEvent::Exited { id: event_id, code }) if event_id == id => {
                        break Ending::Exited(code);
                    }
                    // The engine reports a guest that exits on its own, with
                    // no stop asked, as a crash carrying the exit code on the
                    // VM; for the builder that is the normal way to finish.
                    Some(EngineEvent::Crashed { id: event_id, reason }) if event_id == id => {
                        match self.engine.inspect(&id).await {
                            Ok(VmInfo { exit_code: Some(code), .. }) => break Ending::Exited(code),
                            _ => break Ending::Crashed(reason),
                        }
                    }
                    Some(_) => {}
                    None => {
                        events_open = false;
                        if !output_open {
                            break Ending::Lost("the engine event stream ended".to_owned());
                        }
                    }
                },
                () = &mut deadline => {
                    self.stop_vm(&id).await;
                    self.delete_vm(&id).await;
                    return Err(failure(phase, "timeout".to_owned(), None, false));
                }
                () = wait_cancel(&mut cancel) => {
                    self.stop_vm(&id).await;
                    self.delete_vm(&id).await;
                    return Err(cancelled(phase));
                }
            }
        };

        // 7. Read what the guest already wrote after it exited.
        if matches!(ending, Ending::Exited(_)) {
            let drain = tokio::time::sleep(DRAIN);
            tokio::pin!(drain);
            while output_open {
                tokio::select! {
                    line = output.next(), if output_open => match line {
                        Some(line) => {
                            self.feed(&mut parser, &mut phase, &mut last_error, &events, line).await;
                        }
                        None => output_open = false,
                    },
                    () = &mut drain => break,
                }
            }
        }
        for event in parser.finish() {
            if let BuildEvent::Phase(next) = &event {
                phase = *next;
            }
            self.emit(&events, event).await;
        }

        // 8. The VM is gone in every outcome.
        self.delete_vm(&id).await;

        match ending {
            Ending::Exited(0) => match layout_manifest_digest(&output_disk) {
                Ok(manifest_digest) => Ok(BuildOutput {
                    layout: OciLayout {
                        path: output_disk,
                        manifest_digest,
                    },
                    commit,
                }),
                Err(error) => Err(failure(BuildPhase::Exporting, error, None, false)),
            },
            Ending::Exited(code) => {
                let step = parser.failed_step();
                let script_code = is_script_code(code);
                let error = if script_code {
                    last_error
                        .as_deref()
                        .map(plain_error)
                        .unwrap_or_else(|| format!("builder exited with code {code}"))
                } else if let Some(step) = &step {
                    truncate_chars(&step.error, MAX_ERROR_CHARS)
                } else if let Some(message) = last_error.as_deref() {
                    plain_error(message)
                } else {
                    format!("builder exited with code {code}")
                };
                let failure_phase = if script_code {
                    BuildPhase::Preparing
                } else {
                    phase
                };
                Err(failure(failure_phase, error, step, false))
            }
            Ending::Crashed(reason) => Err(failure(
                phase,
                format!("builder VM crashed: {}", truncate_chars(&reason, 200)),
                None,
                false,
            )),
            Ending::Lost(reason) => Err(failure(phase, reason, None, false)),
        }
    }

    async fn cleanup(&self, build_id: &str) {
        if !valid_id(build_id, "bld_") {
            tracing::warn!(build_id, "not cleaning up an invalid build id");
            return;
        }
        let id = VmId(build_id.to_owned());
        self.stop_vm(&id).await;
        self.delete_vm(&id).await;
        let directory = self.run_dir(build_id);
        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(?directory, %error, "could not remove the build's run directory");
            }
        }
    }

    async fn remove_cache(&self, app_id: &str) {
        if !valid_id(app_id, "app_") {
            tracing::warn!(app_id, "not removing an invalid app's cache");
            return;
        }
        let path = self.cache_path(app_id);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(?path, %error, "could not remove the app's build cache");
            }
        }
    }
}

/// How the builder VM's life ended, as far as the node saw it.
enum Ending {
    /// The guest powered off with this exit code.
    Exited(i32),
    /// The engine reported a crash.
    Crashed(String),
    /// The engine stopped reporting the VM before it exited.
    Lost(String),
}

/// What a validated [`BuildSpec`] means for the guest script.
struct DockerfilePlan {
    context_dir: String,
    dockerfile_dir: String,
    dockerfile_name: String,
    target: Option<String>,
    build_args: Vec<(String, String)>,
}

/// Validates the parts of a spec that reach a command line or a path.
fn validate_spec(spec: &BuildSpec) -> Result<DockerfilePlan, String> {
    if !valid_id(&spec.build_id, "bld_") {
        return Err("the build id is not bld_ and 26 Crockford characters".to_owned());
    }
    if !valid_id(&spec.app_id, "app_") {
        return Err("the app id is not app_ and 26 Crockford characters".to_owned());
    }
    if spec.limits.vcpus == 0 || spec.limits.vcpus > 64 {
        return Err("limits.vcpus must be between 1 and 64".to_owned());
    }
    if spec.limits.memory_mb < 128 || spec.limits.memory_mb > 1024 * 1024 {
        return Err("limits.memory_mb must be between 128 and 1048576".to_owned());
    }
    if spec.limits.timeout_s == 0 || spec.limits.timeout_s > 24 * 60 * 60 {
        return Err("limits.timeout_s must be between 1 and 86400".to_owned());
    }
    if spec.limits.cache_gb == 0 || spec.limits.cache_gb > 1024 {
        return Err("limits.cache_gb must be between 1 and 1024".to_owned());
    }
    if spec.limits.output_gb == 0 || spec.limits.output_gb > 1024 {
        return Err("limits.output_gb must be between 1 and 1024".to_owned());
    }
    validate_secrets(&spec.secrets)?;

    let Frontend::Dockerfile { path, target } = &spec.frontend;
    validate_relative_path("the Dockerfile path", path)?;
    let (dockerfile_dir, dockerfile_name) = split_path(path)?;
    if let Some(target) = target
        && !valid_target(target)
    {
        return Err("the target is not a valid stage name".to_owned());
    }
    let context_dir = if spec.context_dir.is_empty() {
        ".".to_owned()
    } else {
        validate_relative_path("the context directory", &spec.context_dir)?;
        spec.context_dir.clone()
    };

    if spec.build_args.len() > MAX_ARGS {
        return Err(format!("a build may carry at most {MAX_ARGS} build args"));
    }
    let mut build_args = Vec::with_capacity(spec.build_args.len());
    for (name, value) in &spec.build_args {
        if !valid_arg_name(name) {
            return Err(format!("the build-arg name {name:?} is not a valid name"));
        }
        if value.contains('\0') {
            return Err(format!("the build-arg {name:?} contains a NUL byte"));
        }
        if value.len() > MAX_ARG_BYTES {
            return Err(format!(
                "the build-arg {name:?} is larger than {MAX_ARG_BYTES} bytes"
            ));
        }
        build_args.push((name.clone(), value.clone()));
    }

    Ok(DockerfilePlan {
        context_dir,
        dockerfile_dir,
        dockerfile_name,
        target: target.clone(),
        build_args,
    })
}

/// A path that stays inside the repository: relative, no `..`, no NUL, no
/// leading `-`, at most [`MAX_PATH_CHARS`] characters.
fn validate_relative_path(label: &str, path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err(format!("{label} is empty"));
    }
    if path.chars().count() > MAX_PATH_CHARS {
        return Err(format!(
            "{label} is longer than {MAX_PATH_CHARS} characters"
        ));
    }
    if path.contains('\0') {
        return Err(format!("{label} contains a NUL byte"));
    }
    if path.starts_with('/') {
        return Err(format!("{label} must be relative"));
    }
    if path.starts_with('-') {
        return Err(format!("{label} must not start with -"));
    }
    if path.split('/').any(|segment| segment == "..") {
        return Err(format!("{label} contains .."));
    }
    Ok(())
}

/// Splits a relative Dockerfile path into its directory and file name.
fn split_path(path: &str) -> Result<(String, String), String> {
    match path.rsplit_once('/') {
        Some((directory, name)) if !name.is_empty() => Ok((
            if directory.is_empty() {
                ".".to_owned()
            } else {
                directory.to_owned()
            },
            name.to_owned(),
        )),
        Some(_) => Err("the Dockerfile path ends with /".to_owned()),
        None => Ok((".".to_owned(), path.to_owned())),
    }
}

/// `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`.
fn valid_target(target: &str) -> bool {
    let mut chars = target.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && target.len() <= 128
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
}

/// `^[A-Za-z_][A-Za-z0-9_]*$`, at most 128 characters.
fn valid_arg_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// True for `bld_` or `app_` followed by 26 Crockford base32 characters (the
/// ULID alphabet, upper case).
fn valid_id(id: &str, prefix: &str) -> bool {
    let Some(body) = id.strip_prefix(prefix) else {
        return false;
    };
    body.len() == 26 && body.chars().all(is_crockford)
}

/// One character of the Crockford base32 alphabet.
fn is_crockford(ch: char) -> bool {
    ch.is_ascii_digit() || (ch.is_ascii_uppercase() && !matches!(ch, 'I' | 'L' | 'O' | 'U'))
}

/// Quotes `value` as one POSIX shell word: single quotes, with `'` written as
/// `'\''`.
fn sh_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

/// The image's environment with the one variable the BuildKit image needs
/// from us, replacing the image's own value.
fn builder_env(env: &[(String, String)]) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = env
        .iter()
        .filter(|(name, _)| name != "BUILDKIT_SETUP_CGROUPV2_ROOT")
        .cloned()
        .collect();
    env.push(("BUILDKIT_SETUP_CGROUPV2_ROOT".to_owned(), "1".to_owned()));
    env
}

/// The same derivation as the reconciler's `mac_for`.
fn mac_for(ip: Ipv4Addr) -> MacAddr {
    let octets = ip.octets();
    MacAddr([0x52, 0x55, octets[0], octets[1], octets[2], octets[3]])
}

/// Creates `path` as a sparse file of `gb` gibibytes, mode `0600`.
fn create_sparse(path: &Path, gb: u32) -> Result<(), String> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("could not create {path:?}: {e}"))?;
    file.set_len(u64::from(gb) * GIB)
        .map_err(|e| format!("could not size {path:?}: {e}"))?;
    Ok(())
}

/// The first installed `git` of the usual locations.
#[must_use]
pub fn find_git() -> Option<PathBuf> {
    for candidate in ["/usr/bin/git", "/bin/git", "/usr/local/bin/git"] {
        let path = Path::new(candidate);
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
            return Some(path.to_path_buf());
        }
    }
    None
}

/// The pinned BuildKit image from `pins.txt`.
///
/// # Panics
///
/// Never for the shipped file: a unit test parses it. A malformed edit fails
/// that test before it can ship, and at run time the panic names the offending
/// line instead of silently running an unpinned image.
#[must_use]
pub fn pinned_image() -> ImageRef {
    match parse_pins(PINS_TXT) {
        Ok(image) => image,
        Err(message) => panic!("{message}"),
    }
}

/// Parses the three-column pins file: `name version reference@sha256:digest`,
/// with `#` comments and blank lines.
fn parse_pins(text: &str) -> Result<ImageRef, String> {
    let mut found: Option<ImageRef> = None;
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let number = index + 1;
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 3 {
            return Err(format!("pins.txt line {number}: expected three fields"));
        }
        if fields[0] != BUILDER_PIN {
            return Err(format!(
                "pins.txt line {number}: unknown pin {:?}",
                fields[0]
            ));
        }
        if fields[1].is_empty() {
            return Err(format!("pins.txt line {number}: no version"));
        }
        if found.is_some() {
            return Err("pins.txt names the builder image twice".to_owned());
        }
        let Some((reference, digest)) = fields[2].rsplit_once('@') else {
            return Err(format!(
                "pins.txt line {number}: the image is not pinned by digest"
            ));
        };
        if reference.is_empty() {
            return Err(format!("pins.txt line {number}: no image reference"));
        }
        let Some(digest) = digest.strip_prefix("sha256:") else {
            return Err(format!("pins.txt line {number}: the digest is not sha256"));
        };
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "pins.txt line {number}: the digest is not 64 lowercase hex characters"
            ));
        }
        found = Some(ImageRef {
            reference: reference.to_owned(),
            digest: ImageDigest(format!("sha256:{digest}")),
        });
    }
    found.ok_or_else(|| "pins.txt does not name the builder image".to_owned())
}

/// The manifest digest of the OCI layout tar at `path`.
///
/// The tar is read as a stream and nothing is extracted: only `index.json` is
/// read, at most [`MAX_INDEX_BYTES`] of it. Zero padding after the archive is
/// fine (the output disk is a raw file the builder wrote a tar to).
///
/// # Errors
///
/// A one-line reason when the tar is malformed, has no `index.json`, or its
/// index names anything other than exactly one image manifest.
pub fn layout_manifest_digest(path: &Path) -> Result<ImageDigest, String> {
    let file = File::open(path).map_err(|e| format!("could not open the layout {path:?}: {e}"))?;
    let mut archive = tar::Archive::new(std::io::BufReader::new(file));
    let entries = archive
        .entries()
        .map_err(|e| format!("could not read the layout tar: {e}"))?;
    let mut seen = 0usize;
    for entry in entries {
        seen += 1;
        if seen > MAX_LAYOUT_ENTRIES {
            return Err(format!(
                "the layout tar holds more than {MAX_LAYOUT_ENTRIES} entries"
            ));
        }
        let entry = entry.map_err(|e| format!("could not read the layout tar: {e}"))?;
        let path_bytes = entry.path_bytes();
        if path_bytes.as_ref() != b"index.json" && path_bytes.as_ref() != b"./index.json" {
            continue;
        }
        if entry.header().entry_type() != tar::EntryType::Regular {
            return Err("the layout's index.json is not a regular file".to_owned());
        }
        let mut data = Vec::new();
        Read::take(entry, MAX_INDEX_BYTES + 1)
            .read_to_end(&mut data)
            .map_err(|e| format!("could not read the layout's index.json: {e}"))?;
        if data.len() as u64 > MAX_INDEX_BYTES {
            return Err(format!(
                "the layout's index.json is larger than {MAX_INDEX_BYTES} bytes"
            ));
        }
        return parse_index(&data);
    }
    Err("the layout tar has no index.json".to_owned())
}

/// One descriptor of an OCI index.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciDescriptor {
    #[serde(default)]
    media_type: String,
    #[serde(default)]
    digest: String,
}

/// The part of an OCI `index.json` the node needs.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciIndex {
    #[serde(default)]
    schema_version: u32,
    #[serde(default)]
    manifests: Vec<OciDescriptor>,
}

/// The digest of the index's single image manifest.
fn parse_index(data: &[u8]) -> Result<ImageDigest, String> {
    let index: OciIndex = serde_json::from_slice(data)
        .map_err(|e| format!("the layout's index.json is malformed: {e}"))?;
    if index.schema_version != 2 {
        return Err("the layout's index.json is not schema version 2".to_owned());
    }
    let manifests: Vec<&OciDescriptor> = index
        .manifests
        .iter()
        .filter(|descriptor| descriptor.media_type == MANIFEST_MEDIA_TYPE)
        .collect();
    let digest = match manifests.as_slice() {
        [] => return Err("the layout's index names no image manifest".to_owned()),
        [manifest] => &manifest.digest,
        _ => return Err("the layout's index names more than one image manifest".to_owned()),
    };
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err("the layout's manifest digest is not sha256".to_owned());
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("the layout's manifest digest is not 64 lowercase hex characters".to_owned());
    }
    Ok(ImageDigest(digest.clone()))
}

/// Waits until `cancel` turns `true`. A dropped sender never cancels, because
/// the manager disappearing is not a cancellation.
async fn wait_cancel(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow_and_update() {
            return;
        }
        if cancel.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// A failure with an error cut to the protocol limit.
fn failure(
    phase: BuildPhase,
    error: String,
    step: Option<FailedStep>,
    cancelled: bool,
) -> BuildFailure {
    BuildFailure {
        phase,
        error: truncate_chars(&error, MAX_ERROR_CHARS),
        step,
        cancelled,
    }
}

/// A cancellation failure in `phase`.
fn cancelled(phase: BuildPhase) -> BuildFailure {
    failure(phase, "the build was cancelled".to_owned(), None, true)
}

/// Strips the script's `error:` prefix from a line of its own message.
fn plain_error(text: &str) -> String {
    let text = text.trim();
    let text = text.strip_prefix("error:").unwrap_or(text).trim();
    truncate_chars(text, MAX_ERROR_CHARS)
}

/// True for the builder script's own exit codes, which fail `preparing`
/// rather than `building`.
fn is_script_code(code: i32) -> bool {
    (90..=99).contains(&code)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use quoro_engine::{FakeEngine, FakeOp, ImageConfig, OutputLine, VmState};
    use quoro_proto::wire::BuildLimits;

    use super::*;
    use crate::build::BuilderNet;

    const BUILD_ID: &str = "bld_01JC9ZA2B3C4D5E6F7G8H9J0KM";
    const APP_ID: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E";
    const MANIFEST: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const OTHER_MANIFEST: &str =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222";

    /// A fresh temporary directory for one test.
    fn scratch(name: &str) -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "quoro-build-buildkit-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).expect("a temporary directory");
        base
    }

    /// Runs `git` in `directory` and returns its stdout.
    fn run_git(git: &Path, directory: &Path, args: &[&str]) {
        let output = std::process::Command::new(git)
            .args([
                "-c",
                "user.name=Quoro Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// A local repository with a Dockerfile, and the `file://` URL of it.
    struct Fixture {
        root: PathBuf,
        url: String,
    }

    fn fixture(name: &str) -> Option<Fixture> {
        let git = find_git()?;
        let root = scratch(name);
        let work = root.join("repo");
        fs::create_dir_all(work.join("app")).expect("a work tree");
        fs::write(work.join("Dockerfile"), b"FROM scratch\nCOPY . /app\n").expect("a file");
        fs::write(work.join("app/main.txt"), b"hello\n").expect("a file");
        run_git(&git, &work, &["init", "--quiet", "--initial-branch=main"]);
        run_git(&git, &work, &["add", "--all"]);
        run_git(&git, &work, &["commit", "--quiet", "-m", "initial"]);
        Some(Fixture {
            url: format!("file://{}", work.display()),
            root,
        })
    }

    fn config(root: &Path) -> BuilderConfig {
        BuilderConfig {
            builds_dir: root.join("builds"),
            git: find_git(),
            image: pinned_image(),
            fetch_timeout: Duration::from_secs(60),
        }
    }

    fn build_spec(url: &str) -> BuildSpec {
        BuildSpec {
            build_id: BUILD_ID.to_owned(),
            app_id: APP_ID.to_owned(),
            source: BuildSource::Git {
                url: url.to_owned(),
                git_ref: "main".to_owned(),
            },
            frontend: Frontend::Dockerfile {
                path: "Dockerfile".to_owned(),
                target: None,
            },
            context_dir: ".".to_owned(),
            build_args: BTreeMap::new(),
            secrets: BTreeMap::new(),
            limits: BuildLimits {
                vcpus: 2,
                memory_mb: 2048,
                timeout_s: 60,
                cache_gb: 1,
                output_gb: 1,
            },
            net: BuilderNet {
                ip: Ipv4Addr::new(10, 42, 1, 7),
                gateway: Ipv4Addr::new(10, 42, 1, 1),
            },
        }
    }

    fn prepared() -> PreparedImage {
        PreparedImage {
            digest: ImageDigest("sha256:aaaa".to_owned()),
            rootfs_path: PathBuf::from("/fake/rootfs"),
            size_bytes: 0,
            config: ImageConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: vec![
                    ("PATH".to_owned(), "/usr/local/bin:/usr/bin:/bin".to_owned()),
                    ("BUILDKIT_SETUP_CGROUPV2_ROOT".to_owned(), "0".to_owned()),
                ],
                user: None,
                workdir: None,
                exposed_ports: Vec::new(),
            },
        }
    }

    /// A running build: its task, its events and its cancel switch.
    struct Running {
        handle: tokio::task::JoinHandle<Result<BuildOutput, BuildFailure>>,
        cancel: watch::Sender<bool>,
    }

    fn spawn(
        builder: DockerfileBuilder<FakeEngine>,
        spec: BuildSpec,
    ) -> (Running, mpsc::Receiver<BuildEvent>) {
        let (events, receiver) = mpsc::channel(1024);
        let (cancel, cancel_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { builder.build(&spec, events, cancel_rx).await });
        (Running { handle, cancel }, receiver)
    }

    async fn wait_for_vm(engine: &FakeEngine, id: &VmId) {
        for _ in 0..200_000 {
            if engine.inspect(id).await.is_ok() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the builder did not create its VM");
    }

    /// Waits until the builder holds the VM's output stream. Probing with
    /// `output` is safe: the fake reports `Conflict` once a reader exists, and
    /// a probe that succeeds is dropped before the test yields.
    async fn wait_for_output_reader(engine: &FakeEngine, id: &VmId) {
        for _ in 0..200_000 {
            match engine.output(id) {
                Err(EngineError::Conflict(_)) => return,
                other => drop(other),
            }
            tokio::task::yield_now().await;
        }
        panic!("the builder did not read its VM's output");
    }

    async fn wait_running(engine: &FakeEngine, id: &VmId) {
        for _ in 0..200_000 {
            if let Ok(info) = engine.inspect(id).await
                && info.state == VmState::Running
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the builder did not start its VM");
    }

    /// A tar header with a fixed owner, time and mode.
    fn header(entry_type: tar::EntryType, mode: u32, size: u64) -> tar::Header {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(entry_type);
        header.set_mode(mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(size);
        header
    }

    fn index_json(manifests: &[&str]) -> String {
        let entries: Vec<String> = manifests
            .iter()
            .map(|digest| {
                format!(
                    r#"{{"mediaType":"{MANIFEST_MEDIA_TYPE}","digest":"{digest}","size":1,"platform":{{"architecture":"amd64","os":"linux"}}}}"#
                )
            })
            .collect();
        format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{}]}}"#,
            entries.join(",")
        )
    }

    /// A tar holding `index.json` and nothing else.
    fn layout_tar(index: &str) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = header(tar::EntryType::Regular, 0o644, index.len() as u64);
        builder
            .append_data(&mut header, "index.json", index.as_bytes())
            .expect("index.json");
        builder.into_inner().expect("a finished tar")
    }

    fn write_layout(directory: &Path, bytes: &[u8]) -> PathBuf {
        fs::create_dir_all(directory).expect("a directory");
        let path = directory.join("out.disk");
        let mut padded = bytes.to_vec();
        padded.resize(padded.len().div_ceil(1024 * 1024) * 1024 * 1024, 0);
        fs::write(&path, padded).expect("a layout");
        path
    }

    #[test]
    fn the_pinned_image_is_parsed() {
        let image = pinned_image();
        assert_eq!(image.reference, "docker.io/moby/buildkit:v0.34.2");
        assert_eq!(
            image.digest.0,
            "sha256:97719ccb2ac86fb44fd36f330a6d302f63dddd5326f3ada24f9c05ccac0d9098"
        );

        let text = "# a comment\n\nbuildkit-image v1 registry.invalid/x:y@sha256:0000000000000000000000000000000000000000000000000000000000000000\n";
        assert_eq!(
            parse_pins(text).expect("a pin").reference,
            "registry.invalid/x:y"
        );
        for bad in [
            "",
            "buildkit-image v1 registry.invalid/x:y",
            "buildkit-image v1 registry.invalid/x:y@sha256:abc",
            "buildkit-image v1 registry.invalid/x:y@sha512:0000000000000000000000000000000000000000000000000000000000000000",
            "other v1 registry.invalid/x:y@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "buildkit-image v1 registry.invalid/x:y@sha256:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
            "too many fields here",
        ] {
            assert!(parse_pins(bad).is_err(), "{bad:?} is refused");
        }
    }

    #[test]
    fn shell_quoting_survives_hostile_values() {
        assert_eq!(sh_quote("plain"), "'plain'");
        assert_eq!(sh_quote("a'b"), "'a'\\''b'");
        assert_eq!(sh_quote("$(id)"), "'$(id)'");
        assert_eq!(sh_quote("`id`"), "'`id`'");
        assert_eq!(sh_quote("a\nb"), "'a\nb'");
        assert_eq!(sh_quote("a;b"), "'a;b'");
        assert_eq!(sh_quote(""), "''");
    }

    #[test]
    fn spec_validation_table() {
        let url = "https://example.invalid/repo.git";
        assert!(validate_spec(&build_spec(url)).is_ok());

        let mut spec = build_spec(url);
        spec.build_id = "bld_short".to_owned();
        assert!(validate_spec(&spec).is_err());

        let mut spec = build_spec(url);
        spec.app_id = "app_01JC9ZA2B3C4D5E6F7G8H9J0K".to_owned();
        assert!(validate_spec(&spec).is_err());

        // The ULID alphabet has no I, L, O or U.
        let mut spec = build_spec(url);
        spec.build_id = "bld_01JC9ZA2B3C4D5E6F7G8H9J0KI".to_owned();
        assert!(validate_spec(&spec).is_err());

        for bad in [
            "/Dockerfile",
            "../Dockerfile",
            "-Dockerfile",
            "sub/../Dockerfile",
        ] {
            let mut spec = build_spec(url);
            spec.frontend = Frontend::Dockerfile {
                path: bad.to_owned(),
                target: None,
            };
            assert!(validate_spec(&spec).is_err(), "{bad:?} is refused");
        }
        let mut spec = build_spec(url);
        spec.frontend = Frontend::Dockerfile {
            path: "sub/Dockerfile".to_owned(),
            target: Some("bad target".to_owned()),
        };
        assert!(validate_spec(&spec).is_err());

        let mut spec = build_spec(url);
        spec.build_args
            .insert("1BAD".to_owned(), "value".to_owned());
        assert!(validate_spec(&spec).is_err());
        let mut spec = build_spec(url);
        spec.build_args.insert("GOOD".to_owned(), "a\0b".to_owned());
        assert!(validate_spec(&spec).is_err());
        let mut spec = build_spec(url);
        spec.secrets.insert("bad-id".to_owned(), "value".to_owned());
        assert!(validate_spec(&spec).is_err());

        let mut spec = build_spec(url);
        spec.limits.output_gb = 0;
        assert!(validate_spec(&spec).is_err());
        let mut spec = build_spec(url);
        spec.limits.timeout_s = 0;
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn the_vm_spec_is_a_builder_vm() {
        let root = scratch("vm-spec");
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(engine, config(&root));
        let spec = build_spec("https://example.invalid/repo.git");
        let prepared = prepared();
        let vm = builder.vm_spec(&spec, &prepared, "the script".to_owned());

        assert_eq!(vm.id, VmId(BUILD_ID.to_owned()));
        assert_eq!(vm.vcpus, 2);
        assert_eq!(vm.memory_mb, 2048);
        assert_eq!(vm.rootfs, prepared.digest);
        assert_eq!(vm.disk_gb, 4);
        assert!(vm.volumes.is_empty());
        assert!(vm.net.egress_only);
        assert_eq!(vm.net.isolation_group, None);
        assert_eq!(vm.net.ip, Ipv4Addr::new(10, 42, 1, 7));
        assert_eq!(vm.net.mac, mac_for(Ipv4Addr::new(10, 42, 1, 7)));
        assert_eq!(
            vm.console,
            ConsoleMode::VirtioConsole {
                socket: root.join("builds/run").join(BUILD_ID).join("console.sock")
            }
        );
        let serials: Vec<&str> = vm
            .work_disks
            .iter()
            .map(|disk| disk.serial.as_str())
            .collect();
        assert_eq!(serials, vec!["quoro-work", "quoro-src", "quoro-out"]);
        assert_eq!(
            vm.work_disks[0].path,
            root.join("builds/cache").join(format!("{APP_ID}.ext4"))
        );
        assert_eq!(vm.work_disks[0].size_gb, 1);
        assert_eq!(
            vm.work_disks[1].path,
            root.join("builds/run").join(BUILD_ID).join("src.disk")
        );
        assert_eq!(vm.work_disks[1].size_gb, 1);
        assert_eq!(
            vm.work_disks[2].path,
            root.join("builds/run").join(BUILD_ID).join("out.disk")
        );
        assert_eq!(vm.work_disks[2].size_gb, 1);

        assert_eq!(
            vm.init.entrypoint,
            vec![
                "/bin/sh".to_owned(),
                "-c".to_owned(),
                "the script".to_owned()
            ]
        );
        assert!(vm.init.cmd.is_empty());
        assert_eq!(vm.init.user.as_deref(), Some("0"));
        assert_eq!(vm.init.workdir.as_deref(), Some("/"));
        let mut env = vm.init.env.clone();
        env.sort();
        assert_eq!(
            env,
            vec![
                ("BUILDKIT_SETUP_CGROUPV2_ROOT".to_owned(), "1".to_owned()),
                ("PATH".to_owned(), "/usr/local/bin:/usr/bin:/bin".to_owned()),
            ]
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn the_script_quotes_every_spec_value() {
        let root = scratch("script");
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(engine, config(&root));
        let mut spec = build_spec("https://example.invalid/repo.git");
        spec.context_dir = "dir with 'quote' $(touch /tmp/pwned)".to_owned();
        spec.frontend = Frontend::Dockerfile {
            path: "sub dir/My 'Dockerfile".to_owned(),
            target: Some("final".to_owned()),
        };
        spec.build_args.insert(
            "GREETING".to_owned(),
            "hi'$(reboot)`reboot`\n; rm -rf / #".to_owned(),
        );
        spec.secrets
            .insert("TOKEN".to_owned(), "super-secret-value".to_owned());
        let plan = validate_spec(&spec).expect("a valid spec");
        let script = builder.script(&spec, &plan);

        assert!(script.contains(&sh_quote(&format!(
            "context=/mnt/work/ctx/src/{}",
            spec.context_dir
        ))));
        assert!(script.contains(&sh_quote(&format!(
            "dockerfile=/mnt/work/ctx/src/{}/sub dir",
            spec.context_dir
        ))));
        assert!(script.contains(&sh_quote("filename=My 'Dockerfile")));
        assert!(script.contains(&sh_quote("target=final")));
        assert!(script.contains(&sh_quote(
            "build-arg:GREETING=hi'$(reboot)`reboot`\n; rm -rf / #"
        )));
        assert!(
            script.contains(&sh_quote("id=TOKEN,src=/dev/shm/q/secrets/TOKEN")),
            "{script}"
        );
        assert_eq!(script.matches("$(reboot)").count(), 1);
        assert_eq!(script.matches("`reboot`").count(), 1);
        // The secret value never reaches the script or the VM's environment.
        assert!(!script.contains("super-secret-value"));
        assert!(script.contains("--progress=rawjson --output 'type=oci,dest=/dev/'\"$out\""));
        assert!(script.contains("--keep-storage 819MB"));
        assert!(script.contains("echo 'quoro-build phase=building'"));
        assert!(script.contains("echo 'quoro-build phase=preparing'"));
        assert!(script.contains("exit 91"));
        assert!(script.contains("exit 90"));
        assert!(script.contains("exit 92"));
        assert!(script.contains("exit 93"));

        // The shell parses it (nothing runs: `-n` reads only).
        if Path::new("/bin/sh").exists() {
            let path = root.join("script.sh");
            fs::write(&path, &script).expect("the script file");
            let status = std::process::Command::new("/bin/sh")
                .arg("-n")
                .arg(&path)
                .status()
                .expect("/bin/sh runs");
            assert!(status.success(), "the script parses");
        }
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_layout_without_secrets_does_not_extract_them() {
        let root = scratch("script-no-secrets");
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(engine, config(&root));
        let spec = build_spec("https://example.invalid/repo.git");
        let plan = validate_spec(&spec).expect("a valid spec");
        let script = builder.script(&spec, &plan);
        assert!(!script.contains("/dev/shm/q secrets"));
        assert!(!script.contains("--secret"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn the_layout_digest_is_the_single_manifest() {
        let root = scratch("layout");
        let path = write_layout(&root, &layout_tar(&index_json(&[MANIFEST])));
        assert_eq!(layout_manifest_digest(&path).expect("a digest").0, MANIFEST);

        // BuildKit may prefix entries with `./`.
        let index = index_json(&[MANIFEST]);
        let mut prefixed = tar::Builder::new(Vec::new());
        let mut header = header(tar::EntryType::Regular, 0o644, index.len() as u64);
        prefixed
            .append_data(&mut header, "./index.json", index.as_bytes())
            .expect("index.json");
        let path = write_layout(&root, &prefixed.into_inner().expect("a tar"));
        assert_eq!(layout_manifest_digest(&path).expect("a digest").0, MANIFEST);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_layout_that_is_not_one_manifest_is_refused() {
        let root = scratch("layout-bad");
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("no index", {
                let mut builder = tar::Builder::new(Vec::new());
                let mut header = header(tar::EntryType::Regular, 0o644, 2);
                builder
                    .append_data(&mut header, "oci-layout", b"{}".as_slice())
                    .expect("a file");
                builder.into_inner().expect("a tar")
            }),
            (
                "two manifests",
                layout_tar(&index_json(&[MANIFEST, OTHER_MANIFEST])),
            ),
            ("symlink", {
                let mut builder = tar::Builder::new(Vec::new());
                let mut header = header(tar::EntryType::Symlink, 0o777, 0);
                builder
                    .append_link(&mut header, "index.json", "/etc/passwd")
                    .expect("a link");
                builder.into_inner().expect("a tar")
            }),
            ("malformed", layout_tar("{not json")),
            (
                "schema 1",
                layout_tar(
                    &index_json(&[MANIFEST]).replace("\"schemaVersion\":2", "\"schemaVersion\":1"),
                ),
            ),
            ("no manifest", layout_tar(&index_json(&[]))),
            ("bad digest", layout_tar(&index_json(&["sha256:abc"]))),
        ];
        for (name, bytes) in cases {
            let path = write_layout(&root, &bytes);
            assert!(layout_manifest_digest(&path).is_err(), "{name} is refused");
        }
        assert!(layout_manifest_digest(&root.join("missing")).is_err());
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_successful_build_returns_the_layout_digest() {
        let Some(fixture) = fixture("success") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let (running, _events) = spawn(builder, build_spec(&fixture.url));
        let id = VmId(BUILD_ID.to_owned());

        wait_for_vm(&engine, &id).await;
        wait_for_output_reader(&engine, &id).await;
        wait_running(&engine, &id).await;

        let layout = fixture
            .root
            .join("builds/run")
            .join(BUILD_ID)
            .join("out.disk");
        fs::write(&layout, {
            let mut bytes = layout_tar(&index_json(&[MANIFEST]));
            bytes.resize(1024 * 1024, 0);
            bytes
        })
        .expect("the builder writes its layout");

        // The guest script exits on its own: the engine reports that as a
        // crash carrying the exit code.
        engine.exit_app(&id, 0);
        tokio::time::sleep(Duration::from_secs(3)).await;

        let output = running.handle.await.expect("the task").expect("a build");
        assert_eq!(output.commit.len(), 40);
        assert_eq!(output.layout.path, layout);
        assert_eq!(output.layout.manifest_digest.0, MANIFEST);
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test]
    async fn a_failed_step_becomes_the_failure() {
        let Some(fixture) = fixture("failed") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let (running, _events) = spawn(builder, build_spec(&fixture.url));
        let id = VmId(BUILD_ID.to_owned());

        wait_for_vm(&engine, &id).await;
        wait_for_output_reader(&engine, &id).await;
        engine.push_output(&id, OutputStream::Stdout, "quoro-build phase=building");
        engine.push_output(
            &id,
            OutputStream::Stdout,
            r#"{"vertexes":[{"digest":"sha256:aa","name":"[2/2] RUN false","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:00.100000000Z","error":"exit code: 1"}]}"#,
        );
        wait_running(&engine, &id).await;
        engine.exit_app(&id, 1);
        tokio::time::sleep(Duration::from_secs(3)).await;

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build fails");
        assert!(!failure.cancelled);
        assert_eq!(failure.phase, BuildPhase::Building);
        assert!(failure.error.contains("exit code: 1"), "{}", failure.error);
        let step = failure.step.expect("the failed step");
        assert_eq!(step.vertex, 1);
        assert!(step.name.contains("RUN false"));
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test]
    async fn a_crash_without_an_exit_code_fails_as_a_crash() {
        let Some(fixture) = fixture("crash") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let (running, _events) = spawn(builder, build_spec(&fixture.url));
        let id = VmId(BUILD_ID.to_owned());

        wait_for_vm(&engine, &id).await;
        wait_for_output_reader(&engine, &id).await;
        engine.push_output(&id, OutputStream::Stdout, "quoro-build phase=building");
        wait_running(&engine, &id).await;
        engine.crash(&id, "guest reset");

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build fails");
        assert!(!failure.cancelled);
        assert!(
            failure.error.contains("builder VM crashed: guest reset"),
            "{}",
            failure.error
        );
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_stops_the_vm_and_fails() {
        let Some(fixture) = fixture("timeout") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let mut spec = build_spec(&fixture.url);
        spec.limits.timeout_s = 5;
        let (running, _events) = spawn(builder, spec);
        let id = VmId(BUILD_ID.to_owned());

        wait_for_vm(&engine, &id).await;
        wait_for_output_reader(&engine, &id).await;
        wait_running(&engine, &id).await;
        tokio::time::sleep(Duration::from_secs(6)).await;

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build times out");
        assert_eq!(failure.error, "timeout");
        assert!(!failure.cancelled);
        assert!(
            engine
                .stop_calls()
                .iter()
                .any(|(vm, mode)| vm == &id && matches!(mode, StopMode::Force))
        );
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_stops_the_vm_and_reports_cancelled() {
        let Some(fixture) = fixture("cancel") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let (running, _events) = spawn(builder, build_spec(&fixture.url));
        let id = VmId(BUILD_ID.to_owned());

        wait_for_vm(&engine, &id).await;
        wait_for_output_reader(&engine, &id).await;
        wait_running(&engine, &id).await;
        running.cancel.send(true).expect("a live cancel channel");

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build is cancelled");
        assert!(failure.cancelled);
        assert!(
            engine
                .stop_calls()
                .iter()
                .any(|(vm, mode)| vm == &id && matches!(mode, StopMode::Force))
        );
        assert!(engine.inspect(&id).await.is_err(), "the VM is gone");
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test]
    async fn a_full_node_is_a_capacity_failure() {
        let Some(fixture) = fixture("capacity") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        engine.fail_next(
            FakeOp::Create,
            EngineError::ResourceExhausted("no room for another VM".to_owned()),
        );
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let (running, _events) = spawn(builder, build_spec(&fixture.url));

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build waits");
        assert_eq!(failure.phase, BuildPhase::Preparing);
        assert_eq!(failure.error, "node capacity: no room for another VM");
        assert!(!failure.cancelled);
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test]
    async fn a_build_without_git_fails_early() {
        let root = scratch("no-git");
        let engine = Arc::new(FakeEngine::new());
        let mut config = config(&root);
        config.git = None;
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config);
        let (running, _events) = spawn(builder, build_spec("https://example.invalid/repo.git"));

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build fails");
        assert_eq!(failure.phase, BuildPhase::Preparing);
        assert_eq!(failure.error, "git is not installed on this node");
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_invalid_spec_fails_before_any_vm() {
        let root = scratch("invalid");
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&root));
        let mut spec = build_spec("https://example.invalid/repo.git");
        spec.build_id = "vm_01JC9ZA2B3C4D5E6F7G8H9J0KM".to_owned();
        let (running, mut events) = spawn(builder, spec);

        let failure = running
            .handle
            .await
            .expect("the task")
            .expect_err("the build fails");
        assert_eq!(failure.phase, BuildPhase::Preparing);
        assert!(failure.error.contains("build id"));
        assert_eq!(
            events.recv().await,
            Some(BuildEvent::Phase(BuildPhase::Preparing))
        );
        assert!(engine.list().await.expect("the list").is_empty());
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_removes_the_vm_and_the_run_directory() {
        let Some(fixture) = fixture("cleanup") else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        let (running, _events) = spawn(builder, build_spec(&fixture.url));
        let id = VmId(BUILD_ID.to_owned());
        wait_for_vm(&engine, &id).await;
        wait_for_output_reader(&engine, &id).await;
        wait_running(&engine, &id).await;
        running.cancel.send(true).expect("a live cancel channel");
        let _ = running.handle.await.expect("the task");

        let run_dir = fixture.root.join("builds/run").join(BUILD_ID);
        assert!(run_dir.is_dir());
        let cleaner = DockerfileBuilder::new(Arc::clone(&engine), config(&fixture.root));
        cleaner.cleanup(BUILD_ID).await;
        assert!(!run_dir.exists());
        assert!(engine.inspect(&id).await.is_err());
        // Idempotent.
        cleaner.cleanup(BUILD_ID).await;
        cleaner.cleanup("not-a-build-id").await;
        let _ = fs::remove_dir_all(&fixture.root);
    }

    #[tokio::test]
    async fn the_cache_is_removed_by_app() {
        let root = scratch("cache");
        let engine = Arc::new(FakeEngine::new());
        let builder = DockerfileBuilder::new(engine, config(&root));
        let cache = root.join("builds/cache").join(format!("{APP_ID}.ext4"));
        fs::create_dir_all(cache.parent().expect("a parent")).expect("a directory");
        fs::write(&cache, b"cache").expect("a cache file");
        builder.remove_cache(APP_ID).await;
        assert!(!cache.exists());
        // Idempotent, and an invalid id touches nothing.
        builder.remove_cache(APP_ID).await;
        builder.remove_cache("../../etc/passwd").await;
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn find_git_uses_the_pinned_locations() {
        let Some(git) = find_git() else {
            return;
        };
        assert!(git.is_absolute());
        assert!(git.is_file());
    }

    #[test]
    fn output_lines_map_to_log_streams() {
        // A compile-time check of the mapping the parser depends on.
        let line = OutputLine {
            stream: OutputStream::Stderr,
            time: std::time::SystemTime::now(),
            line: "x".to_owned(),
        };
        assert_eq!(line.line, "x");
    }
}
