// SPDX-License-Identifier: AGPL-3.0-only
//! The vsock listener: one thread accepting connections, one thread per
//! session, sixteen at most.
//!
//! The listener binds before the app starts and then lives as long as the VM.
//! A bind failure is logged on the serial console and is not a boot failure:
//! the app still runs, only the web terminal is missing.
//!
//! `vsock` delivers a guest listener only connections from the host, so the
//! listener needs no address filtering of its own (ADR 0113).

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use quoro_init::GuestConfig;
use quoro_init::exec::GuestFrame;
use vsock::{VMADDR_CID_ANY, VsockListener, VsockStream};

use crate::guest::diag;
use crate::guest::exec::session::{self, Context, Helper, Wait};
use crate::guest::user::Identity;

/// The most sessions a VM serves at once.
pub(super) const MAX_SESSIONS: usize = 16;
/// Wait between failed `accept` calls, so a broken listener does not spin.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// Sessions currently running, including the ones still starting.
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Bind the vsock listener and serve it in a thread for the VM's lifetime.
/// Called once, after the switch into the app's root and before the app.
pub(crate) fn start(port: u32, config: &GuestConfig, identity: &Identity) {
    let listener = match VsockListener::bind_with_cid_port(VMADDR_CID_ANY, port) {
        Ok(listener) => listener,
        Err(err) => {
            diag::log(&format!(
                "exec: cannot listen on vsock port {port}: {err}; the terminal is unavailable"
            ));
            return;
        }
    };
    diag::log(&format!("exec: listening on vsock port {port}"));
    let context = Arc::new(Context::new(config, identity));
    let spawned = thread::Builder::new()
        .name("exec-listener".to_string())
        .spawn(move || accept_loop(listener, context));
    if let Err(err) = spawned {
        diag::log(&format!("exec: cannot start the listener thread: {err}"));
    }
}

fn accept_loop(listener: VsockListener, context: Arc<Context>) {
    loop {
        match listener.accept() {
            Ok((stream, _peer)) => {
                let Some(slot) = Slot::acquire() else {
                    reject(stream);
                    continue;
                };
                let context = Arc::clone(&context);
                let spawned = thread::Builder::new()
                    .name("exec-session".to_string())
                    .spawn(move || {
                        let _slot = slot;
                        session::serve(stream, &context, &Helper::quoro_init(), Wait::Registry);
                    });
                if let Err(err) = spawned {
                    diag::log(&format!("exec: cannot start a session thread: {err}"));
                }
            }
            Err(err) => {
                diag::log(&format!("exec: accept: {err}"));
                thread::sleep(ACCEPT_RETRY);
            }
        }
    }
}

/// The sixteenth connection already has one session too many: answer with an
/// `ERROR` frame before the host sends anything and close.
fn reject(mut stream: VsockStream) {
    if let Ok(bytes) = GuestFrame::Error("too many sessions".to_string()).encode() {
        let _ = stream.write_all(&bytes);
    }
    session::close_stream(&stream);
}

/// One occupied session slot, released when the session thread ends.
struct Slot;

impl Slot {
    fn acquire() -> Option<Self> {
        let mut current = ACTIVE.load(Ordering::SeqCst);
        loop {
            if current >= MAX_SESSIONS {
                return None;
            }
            match ACTIVE.compare_exchange_weak(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(Self),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}
