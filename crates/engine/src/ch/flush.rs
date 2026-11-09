// SPDX-License-Identifier: AGPL-3.0-only
//! The guest flush of a volume (contract §3.16, §3.22 and §6, ADR 0282).
//!
//! Before a volume snapshot, and in `flush_volume`, the engine asks the init
//! of the running VM that attaches the volume to flush the volume's
//! filesystem, since data an app wrote without `fsync` can still be in the
//! guest's page cache rather than in the volume file. It opens a sync session:
//! the hybrid vsock handshake of an exec session, then one `SYNC` frame that
//! names the volume's serial. The init runs `syncfs` on the volume's mount and
//! answers `SYNCED`, or `ERROR` with the reason, and closes.
//!
//! The flush takes no VM operation lock, so a stop never waits for it (§4).
//! One bound covers all of it, from the `/vm.info` that tells whether the VM
//! is paused through the connect to the answer. Every result is one
//! [`GuestFlush`] outcome, and none of them is an error: a snapshot is taken
//! whatever the outcome.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::time::Instant;

use quoro_init::exec::{FrameReader, GuestFrame, HostFrame, SyncRequest};

use super::api::{ApiClient, ApiError};
use crate::{GuestFlush, VmSpec, VolumeId};

/// The bound of the flush that begins a snapshot (§3.16).
pub(super) const SNAPSHOT_BOUND: Duration = Duration::from_secs(2);

/// What every init from M4 until ADR 0282 answers a sync frame, which the
/// contract pins so the engine can tell such an init from a failure (§6).
const OLD_INIT_ANSWER: &str = "the first frame must be open, got type 5";

/// The longest read from the socket.
const READ_BYTES: usize = 4096;

/// One VM the engine knows, as the flush sees it.
pub(super) struct Candidate {
    /// Its VMM is alive and its guest booted.
    pub(super) running: bool,
    /// Its spec, or `None` when it could not be read.
    pub(super) spec: Option<VmSpec>,
    /// Its Cloud Hypervisor API socket.
    pub(super) api_socket: PathBuf,
}

/// Whom a flush asks.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Target {
    /// No running VM attaches the volume.
    NoRunningVm,
    /// A running VM whose spec could not be read may attach it, so the
    /// engine cannot tell whether a guest holds unwritten data of it.
    Unknown,
    /// The running VM that attaches it has no vsock device.
    NoVsock,
    /// The running VM that attaches it.
    Vm {
        api_socket: PathBuf,
        vsock_socket: PathBuf,
        serial: String,
    },
}

/// Finds the running VM that attaches `volume` among `candidates`. A volume
/// is attached to one VM at most (§3.6), so the first one found is it.
pub(super) fn target(volume: &VolumeId, candidates: Vec<Candidate>) -> Target {
    let mut unknown = false;
    for candidate in candidates {
        if !candidate.running {
            continue;
        }
        let Some(spec) = candidate.spec else {
            unknown = true;
            continue;
        };
        let Some(attach) = spec.volumes.iter().find(|v| &v.volume_id == volume) else {
            continue;
        };
        return match spec.vsock {
            None => Target::NoVsock,
            Some(vsock) => Target::Vm {
                api_socket: candidate.api_socket,
                vsock_socket: vsock.socket,
                serial: attach.serial.clone(),
            },
        };
    }
    if unknown {
        Target::Unknown
    } else {
        Target::NoRunningVm
    }
}

/// Flushes the volume of `target` in its guest, within `within`.
pub(super) async fn flush(target: Target, within: Duration) -> GuestFlush {
    let (api_socket, vsock_socket, serial) = match target {
        Target::NoRunningVm => return GuestFlush::NoRunningVm,
        Target::Unknown => return GuestFlush::Failed,
        Target::NoVsock => return GuestFlush::Unsupported,
        Target::Vm {
            api_socket,
            vsock_socket,
            serial,
        } => (api_socket, vsock_socket, serial),
    };
    let deadline = Instant::now() + within;
    tokio::time::timeout_at(
        deadline,
        session(&api_socket, &vsock_socket, serial, deadline),
    )
    .await
    .unwrap_or(GuestFlush::TimedOut)
}

/// The flush itself: whether the VM is paused, then the sync session.
async fn session(
    api_socket: &Path,
    vsock_socket: &Path,
    serial: String,
    deadline: Instant,
) -> GuestFlush {
    // A paused guest cannot answer, so it is not asked (§3.16).
    match ApiClient::new(api_socket).by(deadline).info().await {
        Ok(document) => match document.get("state").and_then(Value::as_str) {
            Some("Running") => {}
            Some("Paused") => return GuestFlush::Paused,
            _ => return GuestFlush::Failed,
        },
        Err(ApiError::Timeout) => return GuestFlush::TimedOut,
        Err(_) => return GuestFlush::Failed,
    }

    let Ok(mut stream) = UnixStream::connect(vsock_socket).await else {
        return GuestFlush::Failed;
    };
    match super::exec::handshake(&mut stream).await {
        Ok(reply) if super::exec::is_connect_ok(&reply) => {}
        Ok(_) | Err(_) => return GuestFlush::Failed,
    }
    let request = SyncRequest {
        serials: vec![serial],
    };
    let Ok(frame) = HostFrame::Sync(request).encode() else {
        return GuestFlush::Failed;
    };
    if stream.write_all(&frame).await.is_err() {
        return GuestFlush::Failed;
    }
    answer(&mut stream).await
}

/// Reads the init's one answer.
async fn answer(stream: &mut UnixStream) -> GuestFlush {
    let mut reader = FrameReader::new();
    let mut buf = [0_u8; READ_BYTES];
    loop {
        match reader.next_frame() {
            Ok(Some((kind, payload))) => {
                return match GuestFrame::decode(kind, &payload) {
                    Ok(GuestFrame::Synced) => GuestFlush::Flushed,
                    Ok(GuestFrame::Error(message)) if message == OLD_INIT_ANSWER => {
                        GuestFlush::Unsupported
                    }
                    // Any other error, an output or exit frame, or a frame
                    // the codec refuses (a `SYNCED` with a payload included).
                    Ok(_) | Err(_) => GuestFlush::Failed,
                };
            }
            Ok(None) => {}
            // The header announced a payload over the limit.
            Err(_) => return GuestFlush::Failed,
        }
        match stream.read(&mut buf).await {
            // A close without an answer, a close inside a frame included.
            Ok(0) | Err(_) => return GuestFlush::Failed,
            Ok(n) => reader.push(&buf[..n]),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{UnixListener, UnixStream};

    use quoro_init::exec::{FrameReader, GuestFrame, HostFrame, SYNC, SYNCED, encode_frame};

    use super::{Candidate, OLD_INIT_ANSWER, Target, flush, target};
    use crate::ch::volume::tests::spec;
    use crate::{GuestFlush, VolumeAttach, VolumeId, VsockAttach};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    /// A short directory: a Unix socket path must fit in 108 bytes.
    fn temp_dir(tag: &str) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "quoro-flush-{tag}-{}-{sequence}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the test directory");
        dir
    }

    fn vol(n: u8) -> VolumeId {
        VolumeId(format!("vol_{n}"))
    }

    fn attach(n: u8) -> VolumeAttach {
        VolumeAttach {
            volume_id: vol(n),
            mount_path: format!("/data{n}"),
            serial: format!("quoro-vol-{n}"),
        }
    }

    fn candidate(running: bool, volumes: Vec<VolumeAttach>, vsock: bool) -> Candidate {
        let mut spec = spec(volumes);
        spec.vsock = vsock.then(|| VsockAttach {
            cid: 3,
            socket: PathBuf::from("/run/vm/vsock.sock"),
        });
        Candidate {
            running,
            spec: Some(spec),
            api_socket: PathBuf::from("/run/vm/api.sock"),
        }
    }

    #[test]
    fn the_target_is_the_running_vm_that_attaches_the_volume() {
        let found = target(
            &vol(1),
            vec![
                candidate(true, vec![attach(0)], true),
                candidate(true, vec![attach(2), attach(1)], true),
            ],
        );
        assert_eq!(
            found,
            Target::Vm {
                api_socket: PathBuf::from("/run/vm/api.sock"),
                vsock_socket: PathBuf::from("/run/vm/vsock.sock"),
                serial: "quoro-vol-1".to_owned(),
            }
        );
    }

    #[test]
    fn a_volume_only_a_stopped_vm_attaches_has_no_running_vm() {
        assert_eq!(
            target(&vol(1), vec![candidate(false, vec![attach(1)], true)]),
            Target::NoRunningVm
        );
        assert_eq!(target(&vol(1), Vec::new()), Target::NoRunningVm);
    }

    #[test]
    fn a_running_vm_without_vsock_cannot_be_asked() {
        assert_eq!(
            target(&vol(1), vec![candidate(true, vec![attach(1)], false)]),
            Target::NoVsock
        );
    }

    /// A running VM whose spec is unreadable may hold the volume: that is not
    /// reported as "no running VM", which would claim a flush that did not
    /// happen.
    #[test]
    fn an_unreadable_running_spec_is_not_no_running_vm() {
        let unreadable = Candidate {
            running: true,
            spec: None,
            api_socket: PathBuf::from("/run/vm/api.sock"),
        };
        assert_eq!(target(&vol(1), vec![unreadable]), Target::Unknown);
        let stopped_unreadable = Candidate {
            running: false,
            spec: None,
            api_socket: PathBuf::from("/run/vm/api.sock"),
        };
        assert_eq!(
            target(&vol(1), vec![stopped_unreadable]),
            Target::NoRunningVm
        );
    }

    #[tokio::test]
    async fn targets_that_are_not_asked_map_to_their_outcome() {
        let within = Duration::from_millis(100);
        assert_eq!(
            flush(Target::NoRunningVm, within).await,
            GuestFlush::NoRunningVm
        );
        assert_eq!(
            flush(Target::NoVsock, within).await,
            GuestFlush::Unsupported
        );
        assert_eq!(flush(Target::Unknown, within).await, GuestFlush::Failed);
    }

    #[test]
    fn the_old_init_answer_is_the_one_m4_inits_send() {
        // The wording of `read_open` from M4 until ADR 0282, for a type 5.
        assert_eq!(
            OLD_INIT_ANSWER,
            format!("the first frame must be open, got type {SYNC}")
        );
    }

    /// How the fake VMM API answers `vm.info`.
    #[derive(Clone, Copy)]
    enum Info {
        State(&'static str),
        Fails,
        Hangs,
    }

    /// Serves `vm.info` on `socket` and records each request's endpoint.
    fn fake_api(socket: &Path, info: Info) -> Arc<Mutex<Vec<String>>> {
        let listener = UnixListener::bind(socket).expect("bind the api socket");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&calls);
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = vec![0_u8; 4096];
                let read = stream.read(&mut request).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&request[..read]).into_owned();
                let endpoint = request
                    .split_whitespace()
                    .nth(1)
                    .and_then(|path| path.rsplit('/').next())
                    .unwrap_or_default()
                    .to_owned();
                recorded.lock().expect("calls").push(endpoint);
                let answer = match info {
                    Info::State(state) => {
                        let body = format!("{{\"state\":\"{state}\"}}");
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                    }
                    Info::Fails => "HTTP/1.1 500 \r\nContent-Length: 0\r\n\r\n".to_owned(),
                    Info::Hangs => {
                        held.push(stream);
                        continue;
                    }
                };
                let _ = stream.write_all(answer.as_bytes()).await;
            }
        });
        calls
    }

    /// How the fake hybrid-vsock endpoint answers.
    #[derive(Clone)]
    enum Guest {
        /// No `OK` to the `CONNECT`: the VMM closes the connection.
        NoListener,
        /// `OK`, then these bytes, then a close.
        Answers(Vec<u8>),
        /// `OK`, then nothing, with the connection held open.
        Silent,
    }

    fn frame(guest: &GuestFrame) -> Vec<u8> {
        guest.encode().expect("encode")
    }

    /// Serves the vsock socket once. The frame the engine sent comes back on
    /// the returned receiver.
    fn fake_vsock(socket: &Path, guest: Guest) -> tokio::sync::oneshot::Receiver<HostFrame> {
        let listener = UnixListener::bind(socket).expect("bind the vsock socket");
        let (seen, seen_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut line = Vec::new();
            let mut byte = [0_u8; 1];
            while stream.read(&mut byte).await.expect("read") == 1 {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            assert_eq!(line, b"CONNECT 1024\n");
            let answer = match guest {
                Guest::NoListener => return,
                Guest::Answers(answer) => Some(answer),
                Guest::Silent => None,
            };
            stream.write_all(b"OK 1073741824\n").await.expect("reply");
            let _ = seen.send(read_host_frame(&mut stream).await);
            match answer {
                Some(answer) => {
                    let _ = stream.write_all(&answer).await;
                }
                None => {
                    // Hold the connection until the engine gives up.
                    let mut rest = Vec::new();
                    let _ = stream.read_to_end(&mut rest).await;
                }
            }
        });
        seen_rx
    }

    async fn read_host_frame(stream: &mut UnixStream) -> HostFrame {
        let mut reader = FrameReader::new();
        loop {
            if let Some((kind, payload)) = reader.next_frame().expect("frame") {
                return HostFrame::decode(kind, &payload).expect("host frame");
            }
            let mut buf = [0_u8; 4096];
            let n = stream.read(&mut buf).await.expect("read");
            assert_ne!(n, 0, "the engine closed the connection");
            reader.push(&buf[..n]);
        }
    }

    /// One flush of `quoro-vol-0` against a fake VMM API and vsock socket.
    struct Run {
        outcome: GuestFlush,
        api_calls: Vec<String>,
        sent: Option<HostFrame>,
        elapsed: Duration,
    }

    async fn run(tag: &str, info: Info, guest: Option<Guest>, within: Duration) -> Run {
        let dir = temp_dir(tag);
        let api_socket = dir.join("api.sock");
        let vsock_socket = dir.join("vsock.sock");
        let calls = fake_api(&api_socket, info);
        let seen = guest.map(|guest| fake_vsock(&vsock_socket, guest));
        let started = std::time::Instant::now();
        let outcome = flush(
            Target::Vm {
                api_socket,
                vsock_socket,
                serial: "quoro-vol-0".to_owned(),
            },
            within,
        )
        .await;
        let elapsed = started.elapsed();
        let sent = match seen {
            Some(seen) => tokio::time::timeout(Duration::from_millis(500), seen)
                .await
                .ok()
                .and_then(Result::ok),
            None => None,
        };
        let api_calls = calls.lock().expect("calls").clone();
        let _ = std::fs::remove_dir_all(&dir);
        Run {
            outcome,
            api_calls,
            sent,
            elapsed,
        }
    }

    const WITHIN: Duration = Duration::from_secs(2);

    fn sync_of(serial: &str) -> HostFrame {
        HostFrame::Sync(quoro_init::exec::SyncRequest {
            serials: vec![serial.to_owned()],
        })
    }

    #[tokio::test]
    async fn a_synced_answer_is_flushed() {
        let run = run(
            "synced",
            Info::State("Running"),
            Some(Guest::Answers(frame(&GuestFrame::Synced))),
            WITHIN,
        )
        .await;
        assert_eq!(run.outcome, GuestFlush::Flushed);
        assert_eq!(run.api_calls, ["vm.info"]);
        // One sync frame naming only the volume's serial.
        assert_eq!(run.sent, Some(sync_of("quoro-vol-0")));
    }

    #[tokio::test]
    async fn a_paused_vm_is_not_asked() {
        let run = run(
            "paused",
            Info::State("Paused"),
            Some(Guest::Answers(frame(&GuestFrame::Synced))),
            WITHIN,
        )
        .await;
        assert_eq!(run.outcome, GuestFlush::Paused);
        assert_eq!(run.sent, None, "a paused guest got a sync frame");
    }

    #[tokio::test]
    async fn an_init_older_than_the_sync_session_is_unsupported() {
        let run = run(
            "old",
            Info::State("Running"),
            Some(Guest::Answers(frame(&GuestFrame::Error(
                OLD_INIT_ANSWER.to_owned(),
            )))),
            WITHIN,
        )
        .await;
        assert_eq!(run.outcome, GuestFlush::Unsupported);
    }

    #[tokio::test]
    async fn any_other_error_is_failed() {
        // Only the exact old wording is `Unsupported`: not the new init's
        // own first-frame error, and not a near match.
        for (tag, message) in [
            ("syncfs", "sync: syncfs /data of quoro-vol-0: EIO"),
            (
                "busy",
                "sync: an earlier syncfs of quoro-vol-0 has not returned",
            ),
            ("new", "the first frame must be open or sync, got type 6"),
            ("longer", "the first frame must be open, got type 5."),
            ("other", "the first frame must be open, got type 6"),
        ] {
            let run = run(
                tag,
                Info::State("Running"),
                Some(Guest::Answers(frame(&GuestFrame::Error(
                    message.to_owned(),
                )))),
                WITHIN,
            )
            .await;
            assert_eq!(run.outcome, GuestFlush::Failed, "{tag}");
        }
    }

    #[tokio::test]
    async fn an_unexpected_answer_is_failed() {
        let mut synced_with_payload = Vec::new();
        synced_with_payload.extend(encode_frame(SYNCED, b"x").expect("encode"));
        for (tag, answer) in [
            ("output", frame(&GuestFrame::Output(b"hi".to_vec()))),
            ("exit", frame(&GuestFrame::Exit(0))),
            ("payload", synced_with_payload),
            ("unknown", encode_frame(200, b"").expect("encode")),
            // A close inside a frame, and a close without any answer.
            ("partial", frame(&GuestFrame::Synced)[..2].to_vec()),
            ("closed", Vec::new()),
        ] {
            let run = run(
                tag,
                Info::State("Running"),
                Some(Guest::Answers(answer)),
                WITHIN,
            )
            .await;
            assert_eq!(run.outcome, GuestFlush::Failed, "{tag}");
        }
    }

    #[tokio::test]
    async fn a_vmm_that_cannot_tell_the_state_is_failed() {
        // The guest would answer, so only the `/vm.info` check can fail it.
        for (tag, info) in [
            ("info500", Info::Fails),
            ("created", Info::State("Created")),
        ] {
            let run = run(
                tag,
                info,
                Some(Guest::Answers(frame(&GuestFrame::Synced))),
                WITHIN,
            )
            .await;
            assert_eq!(run.outcome, GuestFlush::Failed, "{tag}");
            assert_eq!(run.sent, None, "{tag}: the guest got a sync frame");
        }
    }

    #[tokio::test]
    async fn a_connection_that_fails_is_failed() {
        // No vsock socket at all, and a VMM that answers no `OK`.
        let missing = run("nosock", Info::State("Running"), None, WITHIN).await;
        assert_eq!(missing.outcome, GuestFlush::Failed);
        let refused = run(
            "nolisten",
            Info::State("Running"),
            Some(Guest::NoListener),
            WITHIN,
        )
        .await;
        assert_eq!(refused.outcome, GuestFlush::Failed);
    }

    #[tokio::test]
    async fn a_silent_guest_times_out_within_the_bound() {
        let within = Duration::from_millis(300);
        let run = run(
            "silent",
            Info::State("Running"),
            Some(Guest::Silent),
            within,
        )
        .await;
        assert_eq!(run.outcome, GuestFlush::TimedOut);
        assert!(run.elapsed >= within, "{:?}", run.elapsed);
        assert!(run.elapsed < within * 4, "{:?}", run.elapsed);
    }

    /// The bound covers the `/vm.info` too (§3.16).
    #[tokio::test]
    async fn a_slow_vm_info_times_out_within_the_bound() {
        let within = Duration::from_millis(300);
        let run = run("slowinfo", Info::Hangs, None, within).await;
        assert_eq!(run.outcome, GuestFlush::TimedOut);
        assert!(run.elapsed < within * 4, "{:?}", run.elapsed);
    }
}
