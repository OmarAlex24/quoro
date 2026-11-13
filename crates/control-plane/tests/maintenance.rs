//! The maintenance window's voter support over real HTTP and WebSocket
//! (`node-agent-protocol.md` sections 5.27 and 8.11; issue #764), with a fake
//! voter that answers as the node agent does. Support is measured on a
//! manual `CLOCK_BOOTTIME`, and nothing here sets the authority's support by
//! hand: the window gets its majority from the voter's own answer.

mod common;

use common::machine::{Machine, enroll, manual_clock};
use common::start_with_patient_node_session;
use quoro_control_plane::session::maintenance::Ending;
use std::time::Duration;
use tokio::time::{Instant, timeout};

/// `D`, the `max_lease_ms` of every request outside a window.
const D: u64 = 15_000;

/// Section 8.11 step 1 and section 5.27: a window that starts opening sends
/// the voter a `support_request` with `max_lease_ms` = `W` at once, not at
/// its next `S / 3`; the voter's answer is the majority the window needs, so
/// with no `expiring` lease it opens and covers nothing. `--end` sends a
/// request with `D` at once.
#[tokio::test]
async fn a_window_asks_the_voters_for_w_at_once_and_ends_back_at_d() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let (_, first) = on_a.request().await;
    assert_eq!(first["body"]["max_lease_ms"], D);
    on_a.grant(&first).await;
    assert!(server.state.hub.authority().supports(D));
    assert!(!server.state.hub.authority().supports(30_000));

    // A voter's newest support is the one with the later `p_send`, so the
    // request for `W` is sent a moment after the one for `D`.
    clock.advance(Duration::from_secs(1));
    let hub = server.state.hub.clone();
    let started = Instant::now();
    let opening = tokio::spawn(async move { hub.open_maintenance(30).await });
    let (asked_at, asked) = on_a.request().await;
    assert_eq!(asked["body"]["max_lease_ms"], 30_000);
    assert!(
        asked_at - started < Duration::from_secs(2),
        "at once, not at the next S / 3: {:?}",
        asked_at - started
    );
    on_a.grant(&asked).await;
    let opened = timeout(Duration::from_secs(10), opening)
        .await
        .expect("the window opens")
        .unwrap()
        .expect("a majority supports W");
    assert_eq!((opened.duration_s, opened.leases), (30, 0));
    assert_eq!(opened.covered_until, None);
    assert!(server.state.hub.authority().supports(30_000));

    let ended = Instant::now();
    assert_eq!(
        server.state.hub.end_maintenance().await.unwrap(),
        Ending::Ended
    );
    let (back_at, back) = on_a.request().await;
    assert_eq!(back["body"]["max_lease_ms"], D);
    assert!(back_at - ended < Duration::from_secs(2), "at once");
}
