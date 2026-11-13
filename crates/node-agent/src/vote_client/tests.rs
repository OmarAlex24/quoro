// SPDX-License-Identifier: AGPL-3.0-only
//! The vote client (section 8.9, section 7.3 steps 2 and 3). The pure tests
//! drive the term, the tally and the reading of a reply without sockets;
//! `over_sockets` runs the client against in-process vote listeners.

use std::time::{Duration, Instant};

use hyper::StatusCode;
use quoro_proto::wire::{MAX_TERM, SpecVoter, SpecVotes, VoteAnswer, VoteOutcome, VoteRefusal};
use serde_json::json;

use super::{
    Candidate, CheckOutcome, NoAnswer, Reply, Round, VoteClient, VoteClientError,
    VoteClientOptions, VoterReply, classify, settle, tally,
};

const VERSION: u64 = 7;
const ME: &str = "nod_c";
const B: &str = "nod_b";
const D: &str = "nod_d";
const W: &str = "nod_w";
const N: &str = "nod_n";

fn voter(node_id: &str, roles: &[&str], address: Option<&str>) -> SpecVoter {
    SpecVoter {
        node_id: node_id.to_owned(),
        roles: roles.iter().map(|role| (*role).to_owned()).collect(),
        mesh_address: address.map(str::to_owned),
    }
}

fn votes(voters: Vec<SpecVoter>) -> SpecVotes {
    SpecVotes {
        version: VERSION,
        port: 51821,
        voters,
        listen: Some(true),
    }
}

/// The candidate, two other nodes, a witness and a node with no mesh
/// address: five voters, so a majority is three.
fn five() -> SpecVotes {
    votes(vec![
        voter(ME, &["node"], Some("10.43.0.2")),
        voter(B, &["node"], Some("10.43.0.3")),
        voter(D, &["node"], Some("10.43.0.4")),
        voter(W, &["edge", "witness"], Some("10.43.0.1")),
        voter(N, &["node"], None),
    ])
}

fn answer(node_id: &str, term: u64, vote: VoteOutcome) -> VoteAnswer {
    VoteAnswer {
        node_id: node_id.to_owned(),
        term,
        vote,
        reason: None,
        retry_after_ms: None,
        lease_wait_ms: (vote == VoteOutcome::Granted).then_some(0),
        voters_version: VERSION,
        voters: None,
    }
}

fn granted(node_id: &str, term: u64, wait_ms: u64, received: Instant) -> VoterReply {
    VoterReply {
        node_id: node_id.to_owned(),
        reply: Reply::Answer {
            answer: VoteAnswer {
                lease_wait_ms: Some(wait_ms),
                ..answer(node_id, term, VoteOutcome::Granted)
            },
            received,
        },
    }
}

fn refused(node_id: &str, term: u64, reason: VoteRefusal, received: Instant) -> VoterReply {
    VoterReply {
        node_id: node_id.to_owned(),
        reply: Reply::Answer {
            answer: VoteAnswer {
                reason: Some(reason),
                ..answer(node_id, term, VoteOutcome::Refused)
            },
            received,
        },
    }
}

fn none(node_id: &str, why: NoAnswer) -> VoterReply {
    VoterReply {
        node_id: node_id.to_owned(),
        reply: Reply::NoAnswer(why),
    }
}

/// A `granted` answer at `term` from `node_id` that names another voter.
fn uncounted(node_id: &str, term: u64) -> VoterReply {
    none(
        node_id,
        NoAnswer::Uncounted {
            answer: Box::new(answer("nod_x", term, VoteOutcome::Granted)),
            why: "it names nod_x, not the voter asked".to_owned(),
        },
    )
}

/// Section 7.3 step 2: `T` starts one above the highest term seen. A check
/// answer at or above `T` raises `T` to one above the highest answer, the
/// term of an answer that does not count included, and every voter is
/// checked again; the checks settle at the first `T` no answer reaches.
#[tokio::test]
async fn the_check_raises_t_above_every_answer_and_checks_again() {
    let set = five();
    let at = Instant::now();
    let mut asked = Vec::new();
    let outcome = settle(3, 8, |term| {
        asked.push(term);
        let replies = match term {
            // Stale voters at 5 and 7; the 7 comes in an answer that does
            // not count, and an answer above the ceiling carries no term.
            4 => vec![
                granted(ME, 3, 0, at),
                uncounted(B, 7),
                granted(D, 2, 0, at),
                refused(W, 5, VoteRefusal::StaleTerm, at),
                none(N, NoAnswer::AboveCeiling),
            ],
            // A voter that already granted 8 to this candidate answers a
            // check at 8 `granted`, with term 8: at `T`, so `T` rises again.
            8 => vec![
                granted(ME, 3, 0, at),
                granted(B, 7, 0, at),
                granted(D, 8, 0, at),
                granted(W, 5, 0, at),
                none(N, NoAnswer::NoMeshAddress),
            ],
            _ => vec![
                granted(ME, 3, 0, at),
                granted(B, 7, 0, at),
                granted(D, 8, 0, at),
                granted(W, 5, 0, at),
                none(N, NoAnswer::NoMeshAddress),
            ],
        };
        std::future::ready(tally(term, true, &set, replies))
    })
    .await;
    assert_eq!(asked, [4, 8, 9]);
    let CheckOutcome::Settled(round) = outcome else {
        panic!("settled: {outcome:?}");
    };
    assert_eq!(
        (round.term, round.check, round.highest_term),
        (9, true, Some(8))
    );
    assert_eq!((round.voters, round.granted, round.majority), (5, 4, true));
}

/// Section 7.3 step 2: when `T` would pass 2^53 - 1 the checks stop, and no
/// round runs at such a term. `T` at the ceiling itself is still asked.
#[tokio::test]
async fn the_checks_stop_before_t_passes_the_ceiling() {
    let set = five();
    let at = Instant::now();

    let mut asked = Vec::new();
    let outcome = settle(MAX_TERM, 8, |term| {
        asked.push(term);
        std::future::ready(tally(term, true, &set, Vec::new()))
    })
    .await;
    assert_eq!(
        outcome,
        CheckOutcome::AboveCeiling {
            highest: MAX_TERM,
            last: None
        }
    );
    assert!(asked.is_empty(), "{asked:?}");

    // An answer at the ceiling: the next `T` would be 2^53.
    let mut asked = Vec::new();
    let outcome = settle(MAX_TERM - 2, 8, |term| {
        asked.push(term);
        let replies = vec![
            refused(B, MAX_TERM, VoteRefusal::StaleTerm, at),
            granted(D, 1, 0, at),
        ];
        std::future::ready(tally(term, true, &set, replies))
    })
    .await;
    assert_eq!(asked, [MAX_TERM - 1]);
    let CheckOutcome::AboveCeiling {
        highest,
        last: Some(last),
    } = outcome
    else {
        panic!("above the ceiling: {outcome:?}");
    };
    assert_eq!((highest, last.term), (MAX_TERM, MAX_TERM - 1));

    // An answer one below: `T` becomes the ceiling, which is asked and
    // settles.
    let mut asked = Vec::new();
    let outcome = settle(MAX_TERM - 2, 8, |term| {
        asked.push(term);
        let top = if term == MAX_TERM { 1 } else { MAX_TERM - 1 };
        let replies = vec![refused(B, top, VoteRefusal::StaleTerm, at)];
        std::future::ready(tally(term, true, &set, replies))
    })
    .await;
    assert_eq!(asked, [MAX_TERM - 1, MAX_TERM]);
    assert!(
        matches!(&outcome, CheckOutcome::Settled(round) if round.term == MAX_TERM),
        "{outcome:?}"
    );
}

/// Terms that keep rising stop the checks after `max_checks` rounds, with no
/// real round: the next `T` is reported.
#[tokio::test]
async fn the_checks_give_up_when_the_terms_keep_rising() {
    let set = five();
    let at = Instant::now();
    let mut asked = Vec::new();
    let outcome = settle(1, 3, |term| {
        asked.push(term);
        let replies = vec![refused(B, term, VoteRefusal::StaleTerm, at)];
        std::future::ready(tally(term, true, &set, replies))
    })
    .await;
    assert_eq!(asked, [2, 3, 4]);
    assert!(
        matches!(&outcome, CheckOutcome::Unsettled { term: 5, last } if last.term == 4),
        "{outcome:?}"
    );
}

fn counts(round: &Round) -> (usize, usize, bool) {
    (round.voters, round.granted, round.majority)
}

/// Sections 8.8 and 8.9: a majority is more than half of the whole voter
/// set, the candidate itself, a witness, a voter with no mesh address and a
/// voter that gives no answer included, each node id once.
#[test]
fn the_majority_counts_the_whole_voter_set() {
    let set = five();
    let at = Instant::now();
    let silent = |node_id| none(node_id, NoAnswer::Timeout("silent".to_owned()));
    let no_mesh = |node_id| none(node_id, NoAnswer::NoMeshAddress);

    // The candidate's own vote, a witness's and another node's: 3 of 5.
    let round = tally(
        2,
        false,
        &set,
        vec![
            granted(ME, 2, 0, at),
            granted(W, 2, 0, at),
            granted(B, 2, 0, at),
            silent(D),
            no_mesh(N),
        ],
    );
    assert_eq!(counts(&round), (5, 3, true));

    // Every voter that answered granted, but 2 of 5 is no majority.
    let round = tally(
        2,
        false,
        &set,
        vec![
            granted(ME, 2, 0, at),
            granted(W, 2, 0, at),
            silent(B),
            silent(D),
            no_mesh(N),
        ],
    );
    assert_eq!(counts(&round), (5, 2, false));

    // A refusal and an answer that does not count are not grants.
    let round = tally(
        2,
        false,
        &set,
        vec![
            granted(ME, 2, 0, at),
            granted(W, 2, 0, at),
            refused(B, 2, VoteRefusal::SupportHeld, at),
            uncounted(D, 2),
            no_mesh(N),
        ],
    );
    assert_eq!(counts(&round), (5, 2, false));

    // The voter with no mesh address is one of four: two grants are not
    // enough.
    let four = votes(vec![
        voter(ME, &["node"], Some("10.43.0.2")),
        voter(B, &["node"], Some("10.43.0.3")),
        voter(D, &["node"], Some("10.43.0.4")),
        voter(N, &["node"], None),
    ]);
    let round = tally(
        2,
        false,
        &four,
        vec![
            granted(ME, 2, 0, at),
            granted(B, 2, 0, at),
            silent(D),
            no_mesh(N),
        ],
    );
    assert_eq!(counts(&round), (4, 2, false));

    // A node id listed twice is one voter with one vote.
    let twice = votes(vec![
        voter(ME, &["node"], Some("10.43.0.2")),
        voter(B, &["node"], Some("10.43.0.3")),
        voter(B, &["node"], Some("10.43.0.5")),
        voter(D, &["node"], Some("10.43.0.4")),
    ]);
    let round = tally(
        2,
        false,
        &twice,
        vec![
            silent(ME),
            granted(B, 2, 0, at),
            granted(B, 2, 0, at),
            silent(D),
        ],
    );
    assert_eq!(counts(&round), (3, 1, false));

    // A grant from a node id outside the set does not count.
    let round = tally(2, false, &four, vec![granted("nod_x", 2, 0, at)]);
    assert_eq!(counts(&round), (4, 0, false));

    // One voter needs its vote; two need both.
    let one = votes(vec![voter(ME, &["node"], Some("10.43.0.2"))]);
    assert_eq!(
        counts(&tally(2, false, &one, vec![granted(ME, 2, 0, at)])),
        (1, 1, true)
    );
    assert_eq!(
        counts(&tally(
            2,
            false,
            &one,
            vec![refused(ME, 2, VoteRefusal::SupportHeld, at)]
        )),
        (1, 0, false)
    );
    let two = votes(vec![
        voter(ME, &["node"], Some("10.43.0.2")),
        voter(W, &["witness"], Some("10.43.0.1")),
    ]);
    assert_eq!(
        counts(&tally(
            2,
            false,
            &two,
            vec![granted(ME, 2, 0, at), silent(W)]
        )),
        (2, 1, false)
    );
    assert_eq!(
        counts(&tally(
            2,
            false,
            &two,
            vec![granted(ME, 2, 0, at), granted(W, 2, 0, at)]
        )),
        (2, 2, true)
    );
    assert_eq!(
        counts(&tally(2, false, &votes(Vec::new()), Vec::new())),
        (0, 0, false)
    );
}

/// Section 7.3 step 3: the wait ends at the latest receipt time plus
/// `lease_wait_ms` over the granting answers: not at the latest receipt,
/// and not at the longest wait added to it. Refusals and answers that do
/// not count add nothing.
#[test]
fn the_wait_is_the_latest_receipt_plus_lease_wait_over_the_grants() {
    let start = Instant::now();
    let at = |secs| start + Duration::from_secs(secs);
    let late_grant = none(
        N,
        NoAnswer::Uncounted {
            answer: Box::new(VoteAnswer {
                lease_wait_ms: Some(60_000),
                voters_version: VERSION + 1,
                ..answer(N, 2, VoteOutcome::Granted)
            }),
            why: "granted at voter-set version 8, not 7".to_owned(),
        },
    );
    let round = tally(
        2,
        false,
        &five(),
        vec![
            granted(ME, 2, 5_000, at(10)),
            granted(B, 2, 16_000, at(1)),
            granted(W, 2, 1_000, at(12)),
            refused(D, 2, VoteRefusal::StaleTerm, at(30)),
            late_grant,
        ],
    );
    assert_eq!(round.wait_until, Some(at(17)));
    assert_eq!(counts(&round), (5, 3, true));

    let round = tally(
        2,
        false,
        &five(),
        vec![refused(D, 2, VoteRefusal::StaleTerm, at(30))],
    );
    assert_eq!(round.wait_until, None);
}

/// Section 8.9: only a `200` carries an answer, and only one whose body is
/// a vote answer whose raw `term` is within the ceiling (section 8.8). An
/// answer that names another voter, or a grant without `lease_wait_ms`, at
/// another voter-set version, or for a real request at another term, does
/// not count.
#[test]
fn each_reply_that_is_no_answer() {
    let at = Instant::now();
    let read = |status: u16, body: &str, check: bool| {
        classify(
            B,
            VERSION,
            5,
            check,
            StatusCode::from_u16(status).unwrap(),
            body.as_bytes(),
            at,
        )
    };
    let grant = json!({
        "node_id": B, "term": 5, "vote": "granted", "lease_wait_ms": 100, "voters_version": VERSION
    });
    let with = |change: serde_json::Value| {
        let mut body = grant.clone();
        for (key, value) in change.as_object().unwrap() {
            if value.is_null() {
                body.as_object_mut().unwrap().remove(key);
            } else {
                body[key] = value.clone();
            }
        }
        body.to_string()
    };

    let counted = read(200, &grant.to_string(), false);
    assert!(
        matches!(&counted, Reply::Answer { answer, received } if answer.vote == VoteOutcome::Granted && *received == at),
        "{counted:?}"
    );

    // Every status but `200` is no answer, even with a granted body.
    for status in [503, 400, 404, 405, 408, 413, 500, 201, 202] {
        assert_eq!(
            read(status, &grant.to_string(), false),
            Reply::NoAnswer(NoAnswer::Status {
                status,
                error: None
            }),
            "{status}"
        );
    }
    assert_eq!(
        read(503, r#"{"error": "votes are not open"}"#, false),
        Reply::NoAnswer(NoAnswer::Status {
            status: 503,
            error: Some("votes are not open".to_owned())
        })
    );

    for body in [
        "not json".to_owned(),
        "{}".to_owned(),
        with(json!({"term": -1})),
        with(json!({"term": "5"})),
        with(json!({"vote": "maybe"})),
        with(json!({"node_id": null})),
    ] {
        let reply = read(200, &body, false);
        assert!(
            matches!(reply, Reply::NoAnswer(NoAnswer::Malformed(_))),
            "{body}: {reply:?}"
        );
    }

    // Above the ceiling however the term is written, refusals included.
    for term in [
        "9007199254740992",
        "18446744073709551615",
        "18446744073709551616",
        "1e300",
    ] {
        let body = format!(
            r#"{{"node_id":"{B}","term":{term},"vote":"refused","reason":"stale_term","voters_version":{VERSION}}}"#
        );
        assert_eq!(
            read(200, &body, true),
            Reply::NoAnswer(NoAnswer::AboveCeiling),
            "{term}"
        );
    }
    // The ceiling itself is a term.
    let at_ceiling = read(200, &with(json!({"term": MAX_TERM})), true);
    assert_eq!(at_ceiling.term(), Some(MAX_TERM), "{at_ceiling:?}");
    assert!(matches!(at_ceiling, Reply::Answer { .. }), "{at_ceiling:?}");

    for (body, check, why) in [
        (with(json!({"node_id": D})), true, "names nod_d"),
        (
            with(json!({"lease_wait_ms": null})),
            true,
            "without lease_wait_ms",
        ),
        (
            with(json!({"voters_version": VERSION + 1})),
            true,
            "version 8",
        ),
        (with(json!({"term": 4})), false, "at term 4, not 5"),
        (with(json!({"term": 6})), false, "at term 6, not 5"),
    ] {
        let reply = read(200, &body, check);
        let Reply::NoAnswer(NoAnswer::Uncounted { why: got, .. }) = &reply else {
            panic!("{body}: {reply:?}");
        };
        assert!(got.contains(why), "{body}: {got}");
        // Its term still counts for `T`.
        assert!(reply.term().is_some(), "{reply:?}");
    }

    // A check grant carries the voter's own term, below `T`: it counts. A
    // refusal at another voter-set version is an answer too.
    assert!(matches!(
        read(200, &with(json!({"term": 3})), true),
        Reply::Answer { .. }
    ));
    let changed = format!(
        r#"{{"node_id":"{B}","term":2,"vote":"refused","reason":"voters_changed","voters_version":8,"voters":["{B}"]}}"#
    );
    assert!(matches!(read(200, &changed, false), Reply::Answer { .. }));
}

/// The mesh address the client for `ME` over `voters` asks from, or why it
/// was not made.
fn own_address(voters: Vec<SpecVoter>) -> Result<std::net::IpAddr, VoteClientError> {
    VoteClient::new(
        votes(voters),
        Candidate {
            cluster_id: "cls_1".to_owned(),
            node_id: ME.to_owned(),
        },
        VoteClientOptions::default(),
    )
    .map(|client| client.mesh_address())
}

/// The source of every request is the mesh address of the candidate's own
/// entry in `spec.votes.voters`, without its prefix length; the first entry
/// with its node id decides.
#[test]
fn the_client_asks_from_its_own_entry_in_the_voter_set() {
    assert_eq!(own_address(five().voters), Ok("10.43.0.2".parse().unwrap()));
    assert_eq!(
        own_address(vec![
            voter(B, &["node"], Some("10.43.0.3")),
            voter(ME, &["node"], Some("10.43.0.9/16")),
            voter(ME, &["node"], Some("10.43.0.8")),
        ]),
        Ok("10.43.0.9".parse().unwrap())
    );
}

/// An IPv4-mapped IPv6 address in the candidate's entry is used as plain
/// IPv4: the address an IPv4 socket binds and the voters check.
#[test]
fn an_ipv4_mapped_own_address_is_used_as_ipv4() {
    for address in ["::ffff:10.43.0.2", "::ffff:10.43.0.2/112"] {
        assert_eq!(
            own_address(vec![voter(ME, &["node"], Some(address))]),
            Ok("10.43.0.2".parse().unwrap()),
            "{address}"
        );
    }
    assert_eq!(
        own_address(vec![voter(ME, &["node"], Some("fd00::2/64"))]),
        Ok("fd00::2".parse().unwrap())
    );
}

/// Section 7.3: a machine that is not a voter cannot be promoted, and every
/// voter would answer it `not_allowed` (section 8.9).
#[test]
fn a_candidate_outside_the_voter_set_is_refused() {
    let not_a_voter = Err(VoteClientError::NotAVoter(ME.to_owned()));
    assert_eq!(own_address(Vec::new()), not_a_voter);
    assert_eq!(
        own_address(vec![voter(B, &["node"], Some("10.43.0.2"))]),
        not_a_voter
    );
}

/// Section 7.3: only a machine with the `node` role is promoted, and a
/// voter answers only a source with that role (section 8.9).
#[test]
fn a_candidate_without_the_node_role_is_refused() {
    for roles in [&["witness"][..], &["edge", "witness"], &[]] {
        assert_eq!(
            own_address(vec![voter(ME, roles, Some("10.43.0.2"))]),
            Err(VoteClientError::NotANode(ME.to_owned())),
            "{roles:?}"
        );
    }
}

/// A candidate with no mesh address of its own, or one that names no one
/// machine, has no source a voter would answer.
#[test]
fn a_candidate_without_a_mesh_address_is_refused() {
    for address in [
        None,
        Some(""),
        Some("not an address"),
        Some("0.0.0.0/16"),
        Some("::ffff:0.0.0.0"),
        Some("224.0.0.1"),
    ] {
        assert_eq!(
            own_address(vec![voter(ME, &["node"], address)]),
            Err(VoteClientError::NoMeshAddress(ME.to_owned())),
            "{address:?}"
        );
    }
}

/// The client against vote listeners on 127.0.0.x addresses, as in the
/// listener's own tests: the candidate is 127.0.0.21, the other nodes
/// 127.0.0.22, .23 and .25, and the witness 127.0.0.24. Linux routes all of
/// 127.0.0.0/8 to the loopback interface, so the client can bind any of them
/// as its source.
#[cfg(target_os = "linux")]
mod over_sockets {
    use std::net::{IpAddr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use quoro_proto::wire::{
        DesiredState, MAX_TERM, Spec, SpecMesh, SpecVoter, SpecVotes, VoteOutcome, VoteRefusal,
    };
    use serde_json::json;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    use super::super::{
        Candidate, CheckOutcome, NoAnswer, Reply, Round, VoteClient, VoteClientError,
        VoteClientOptions, effective_uid,
    };
    use crate::lease::ManualHostClock;
    use crate::mesh::ListenerLink;
    use crate::reconcile::Snapshot;
    use crate::store::NodeStore;
    use crate::util;
    use crate::vote::{StoredSupport, StoredVote};
    use crate::vote_listener::{VoteListener, VoteListenerOptions};

    const CLUSTER: &str = "cls_1";
    const ME: &str = "nod_c";
    const B: &str = "nod_b";
    const D: &str = "nod_d";
    const E: &str = "nod_e";
    const W: &str = "nod_w";
    const N: &str = "nod_n";
    const OWN: &str = "127.0.0.21";
    const B_AT: &str = "127.0.0.22";
    const D_AT: &str = "127.0.0.23";
    const W_AT: &str = "127.0.0.24";
    const E_AT: &str = "127.0.0.25";
    /// `CLOCK_BOOTTIME` of every voter when its test starts.
    const BOOT: Duration = Duration::from_secs(1000);

    struct TempDir(PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Whether this test process is root. A CI runner is not: the
    /// candidate's own listener then answers it `not_allowed`, since a
    /// caller on the voter's own host must be root's (section 8.9).
    fn root() -> bool {
        effective_uid() == 0
    }

    /// The listener on the candidate's own address, on a port the kernel
    /// picks. Every test binds it first: the port is then free on every
    /// other 127.0.0.x address the tests use, since only they use them and
    /// each holds its own port on this one.
    async fn own_port() -> (TcpListener, u16) {
        let listener = TcpListener::bind((OWN, 0))
            .await
            .expect("bind the own address");
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    fn voter(node_id: &str, roles: &[&str], address: Option<&str>) -> SpecVoter {
        SpecVoter {
            node_id: node_id.to_owned(),
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            mesh_address: address.map(str::to_owned),
        }
    }

    /// The candidate, nodes B and D, the witness W and a node N with no mesh
    /// address, then `more`.
    fn spec_votes(port: u16, more: Vec<SpecVoter>) -> SpecVotes {
        let mut voters = vec![
            voter(ME, &["node"], Some(OWN)),
            voter(B, &["node"], Some(B_AT)),
            voter(D, &["node"], Some(D_AT)),
            voter(W, &["witness"], Some(W_AT)),
            voter(N, &["node"], None),
        ];
        voters.extend(more);
        SpecVotes {
            version: 7,
            port,
            voters,
            listen: Some(true),
        }
    }

    /// The answer timeout of a test that waits out a voter that never
    /// answers.
    const SHORT: Duration = Duration::from_millis(1500);
    /// The answer timeout of every other test: a real voter writes to its
    /// store before it answers `granted`, which a loaded runner can slow.
    const LONG: Duration = Duration::from_secs(10);

    fn client(votes: &SpecVotes, euid: fn() -> u32) -> VoteClient {
        client_waiting(votes, euid, LONG)
    }

    /// The client of the candidate `ME`, whose effective uid is `euid()`,
    /// waiting `answer_timeout` for each answer.
    fn client_waiting(
        votes: &SpecVotes,
        euid: fn() -> u32,
        answer_timeout: Duration,
    ) -> VoteClient {
        VoteClient::new(
            votes.clone(),
            Candidate {
                cluster_id: CLUSTER.to_owned(),
                node_id: ME.to_owned(),
            },
            VoteClientOptions {
                answer_timeout,
                ..VoteClientOptions::default()
            },
        )
        .expect("the candidate is a node voter with a mesh address")
        .with_effective_uid(euid)
    }

    fn as_root() -> u32 {
        0
    }

    fn as_user() -> u32 {
        1000
    }

    /// A vote listener in this process, as a voter's agent runs it.
    struct Voter {
        store: NodeStore,
        task: JoinHandle<()>,
        _dir: TempDir,
        _desired: watch::Sender<Option<Snapshot>>,
        _firewall: watch::Sender<Option<SocketAddr>>,
    }

    impl Drop for Voter {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Voter {
        async fn on(address: &str, port: u16, node_id: &str, votes: &SpecVotes) -> Self {
            let listener = TcpListener::bind((address, port))
                .await
                .expect("bind the voter's address");
            Self::serve(listener, node_id, votes, Some("boot-a")).await
        }

        /// Serves votes on `listener` as `node_id`, with a host clock on
        /// boot `boot_id` (none: a host that cannot tell its boot).
        async fn serve(
            listener: TcpListener,
            node_id: &str,
            votes: &SpecVotes,
            boot_id: Option<&str>,
        ) -> Self {
            let address = listener.local_addr().unwrap();
            let dir = std::env::temp_dir().join(format!(
                "quoro-vote-client-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let store = NodeStore::open(&dir).await.unwrap();
            let state = DesiredState {
                rev: 1,
                issued_at: String::new(),
                term: 1,
                spec: Spec {
                    mesh: Some(SpecMesh {
                        address: format!("{}/8", address.ip()),
                        listen_port: 51820,
                        peers: Vec::new(),
                        allow: Vec::new(),
                    }),
                    votes: Some(votes.clone()),
                    ..Spec::default()
                },
                signature: String::new(),
            };
            let (desired, desired_rx) = watch::channel(Some(Snapshot { rev: 1, state }));
            let (firewall, firewall_rx) = watch::channel(Some(address));
            let (link, _bound) = ListenerLink::detached(firewall_rx);
            let listener_task = VoteListener::new(
                store.clone(),
                node_id.to_owned(),
                CLUSTER.to_owned(),
                Arc::new(ManualHostClock::new(boot_id, BOOT)),
                desired_rx,
                link,
                VoteListenerOptions::quick(),
            );
            let task = tokio::spawn(async move { listener_task.serve(listener).await });
            Self {
                store,
                task,
                _dir: TempDir(dir),
                _desired: desired,
                _firewall: firewall,
            }
        }

        async fn stored(&self) -> (Option<u64>, Option<StoredVote>) {
            (
                self.store.highest_term().await.unwrap(),
                self.store.vote().await.unwrap(),
            )
        }

        /// Stores the support of an old primary, `nod_old`, whose promise
        /// ends `promise` and whose lease horizon ends `horizon` after this
        /// test's start, on the voter's `CLOCK_BOOTTIME`.
        async fn supports(&self, promise: i64, horizon: i64) {
            let at = |secs: i64| {
                u64::try_from(BOOT.as_nanos())
                    .unwrap()
                    .saturating_add_signed(secs * 1_000_000_000)
            };
            self.store
                .save_support(&StoredSupport {
                    term: 1,
                    primary_node_id: "nod_old".to_owned(),
                    support_ms: 15_000,
                    max_lease_ms: 15_000,
                    boot_id: "boot-a".to_owned(),
                    received_ns: at(-60),
                    promise_ns: at(promise),
                    horizon_ns: at(horizon),
                })
                .await
                .unwrap();
        }
    }

    /// A voter that answers every request with one canned reply, or never.
    struct Fake {
        accepted: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Fake {
        /// Answers `status` with `body`, or holds the connection open with
        /// `None`.
        async fn on(address: &str, port: u16, answer: Option<(u16, String)>) -> Self {
            let listener = TcpListener::bind((address, port))
                .await
                .expect("bind the fake voter's address");
            Self::serve(listener, answer)
        }

        fn serve(listener: TcpListener, answer: Option<(u16, String)>) -> Self {
            let accepted = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&accepted);
            let task = tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((mut stream, _)) = listener.accept().await {
                    counter.fetch_add(1, Ordering::SeqCst);
                    read_request(&mut stream).await;
                    match &answer {
                        Some((status, body)) => {
                            let reply = format!(
                                "HTTP/1.1 {status} Canned\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = stream.write_all(reply.as_bytes()).await;
                            let _ = stream.shutdown().await;
                        }
                        None => held.push(stream),
                    }
                }
            });
            Self { accepted, task }
        }

        fn accepted(&self) -> usize {
            self.accepted.load(Ordering::SeqCst)
        }
    }

    /// Reads one request with a `content-length` body, or what arrives of
    /// it within a second.
    async fn read_request(stream: &mut TcpStream) {
        let read = async {
            let mut raw = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = stream.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                raw.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&raw).to_lowercase();
                if let Some(end) = text.find("\r\n\r\n") {
                    let length = text[..end]
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if raw.len() >= end + 4 + length {
                        return;
                    }
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(1), read).await;
    }

    fn reply<'a>(round: &'a Round, node_id: &str) -> &'a Reply {
        &round
            .replies
            .iter()
            .find(|reply| reply.node_id == node_id)
            .unwrap_or_else(|| panic!("a reply from {node_id}: {round:?}"))
            .reply
    }

    /// The candidate's own answer: granted when this test runs as root,
    /// `not_allowed` otherwise (section 8.9).
    fn assert_own(round: &Round) {
        let Reply::Answer { answer, .. } = reply(round, ME) else {
            panic!("the own agent answers: {round:?}");
        };
        if root() {
            assert_eq!(answer.vote, VoteOutcome::Granted, "{answer:?}");
        } else {
            assert_eq!(answer.reason, Some(VoteRefusal::NotAllowed), "{answer:?}");
        }
    }

    fn assert_vote(reply: &Reply, vote: VoteOutcome, term: u64) {
        let Reply::Answer { answer, .. } = reply else {
            panic!("an answer: {reply:?}");
        };
        assert_eq!((answer.vote, answer.term), (vote, term), "{answer:?}");
    }

    fn stored(term: u64) -> Option<StoredVote> {
        Some(StoredVote {
            term,
            candidate: ME.to_owned(),
        })
    }

    /// Section 7.3 step 2 over real listeners: voters at terms 5 and 3 make
    /// the check at `T` = 2 rise to 6, the check at 6 settles, and no check
    /// writes anything. The real round at 6 is granted and stored.
    #[tokio::test]
    async fn a_check_round_raises_t_over_the_voters_and_changes_nothing() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, Vec::new());
        let me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let b = Voter::on(B_AT, port, B, &votes).await;
        let d = Voter::on(D_AT, port, D, &votes).await;
        let w = Voter::on(W_AT, port, W, &votes).await;
        b.store.raise_term(5).await.unwrap();
        d.store.raise_term(3).await.unwrap();
        let client = client(&votes, as_root);

        let outcome = client.check(1).await.unwrap();
        let CheckOutcome::Settled(round) = outcome else {
            panic!("settled: {outcome:?}");
        };
        assert_eq!((round.term, round.check), (6, true), "{round:?}");
        assert_vote(reply(&round, B), VoteOutcome::Granted, 5);
        assert_vote(reply(&round, D), VoteOutcome::Granted, 3);
        assert_vote(reply(&round, W), VoteOutcome::Granted, 0);
        assert_eq!(reply(&round, N), &Reply::NoAnswer(NoAnswer::NoMeshAddress));
        assert_own(&round);
        assert_eq!(round.granted, 3 + usize::from(root()), "{round:?}");
        assert!(round.majority, "{round:?}");
        assert_eq!(b.stored().await, (Some(5), None), "a check writes nothing");
        assert_eq!(d.stored().await, (Some(3), None));
        assert_eq!(w.stored().await, (None, None));
        assert_eq!(me.stored().await, (None, None));

        let round = client.vote(round.term).await.unwrap();
        assert!(round.majority, "{round:?}");
        for voter in [&b, &d, &w] {
            assert_eq!(voter.stored().await, (Some(6), stored(6)));
        }
    }

    /// Section 8.9: a real round granted by a majority. The voters' lease
    /// horizons give their `lease_wait_ms`, and the wait ends at the latest
    /// receipt plus `lease_wait_ms` over the grants: here B's, whose horizon
    /// is the furthest. A grant also shows that the request came from the
    /// candidate's own mesh address, the only source the voters grant.
    #[tokio::test]
    async fn a_real_round_with_a_majority_and_its_wait() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, Vec::new());
        let _me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let b = Voter::on(B_AT, port, B, &votes).await;
        let d = Voter::on(D_AT, port, D, &votes).await;
        let w = Voter::on(W_AT, port, W, &votes).await;
        // Promises that ran out, with horizons 30 s and 10 s ahead.
        b.supports(-5, 30).await;
        d.supports(-5, 10).await;

        let round = client(&votes, as_root).vote(2).await.unwrap();
        assert_eq!(
            (round.term, round.check, round.voters, round.majority),
            (2, false, 5, true),
            "{round:?}"
        );
        assert_eq!(round.granted, 3 + usize::from(root()), "{round:?}");
        assert_own(&round);
        let waits = |node_id| match reply(&round, node_id) {
            Reply::Answer { answer, received } => (*received, answer.lease_wait_ms.unwrap()),
            other => panic!("{node_id} granted: {other:?}"),
        };
        let (b_at, b_wait) = waits(B);
        let (d_at, d_wait) = waits(D);
        assert!((20_000..=30_000).contains(&b_wait), "{b_wait}");
        assert!(d_wait <= 10_000, "{d_wait}");
        assert_eq!(waits(W).1, 0);
        let until = b_at + Duration::from_millis(b_wait);
        assert_eq!(round.wait_until, Some(until), "{round:?}");
        assert!(until > d_at + Duration::from_millis(d_wait));
        for voter in [&b, &d, &w] {
            assert_eq!(voter.stored().await, (Some(2), stored(2)));
        }
    }

    /// Section 8.9: a majority is of the whole voter set. Two voters are
    /// down and one has no mesh address; every voter that answered granted,
    /// root's own vote included, and still three or two of six is none.
    #[tokio::test]
    async fn a_real_round_without_a_majority() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, vec![voter(E, &["node"], Some(E_AT))]);
        let _me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let b = Voter::on(B_AT, port, B, &votes).await;
        let w = Voter::on(W_AT, port, W, &votes).await;

        let round = client(&votes, as_root).vote(2).await.unwrap();
        assert_eq!(
            (round.voters, round.granted, round.majority),
            (6, 2 + usize::from(root()), false),
            "{round:?}"
        );
        for down in [D, E] {
            assert!(
                matches!(reply(&round, down), Reply::NoAnswer(NoAnswer::Connect(_))),
                "{down}: {round:?}"
            );
        }
        // The voters that granted stored their vote: only the command
        // decides what follows a round without a majority.
        assert_eq!(b.stored().await, (Some(2), stored(2)));
        assert_eq!(w.stored().await, (Some(2), stored(2)));
    }

    /// Section 8.9 "When the voter cannot tell": a `503` is no answer, even
    /// when its body reads as a grant, and a voter that does not answer in
    /// time is no answer. Neither raises `T`, and nothing is sent again.
    #[tokio::test]
    async fn a_503_or_a_silent_voter_is_no_answer() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, Vec::new());
        let _me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let grant = json!({
            "node_id": B, "term": 9, "vote": "granted", "lease_wait_ms": 0, "voters_version": 7
        });
        let b = Fake::on(B_AT, port, Some((503, grant.to_string()))).await;
        // A host that cannot read its boot id answers `503` (section 8.9).
        let d = Voter::serve(
            TcpListener::bind((D_AT, port)).await.unwrap(),
            D,
            &votes,
            None,
        )
        .await;
        let w = Fake::on(W_AT, port, None).await;
        let client = client_waiting(&votes, as_root, SHORT);

        let outcome = client.check(1).await.unwrap();
        let CheckOutcome::Settled(round) = outcome else {
            panic!("settled: {outcome:?}");
        };
        assert_eq!(round.term, 2, "a 503's body raises nothing: {round:?}");

        let round = client.vote(2).await.unwrap();
        assert_eq!(
            reply(&round, B),
            &Reply::NoAnswer(NoAnswer::Status {
                status: 503,
                error: None
            })
        );
        let Reply::NoAnswer(NoAnswer::Status {
            status: 503,
            error: Some(error),
        }) = reply(&round, D)
        else {
            panic!("D answers 503: {round:?}");
        };
        assert!(error.contains("boot id"), "{error}");
        assert!(
            matches!(reply(&round, W), Reply::NoAnswer(NoAnswer::Timeout(_))),
            "{round:?}"
        );
        assert_eq!(round.granted, usize::from(root()), "{round:?}");
        assert!(!round.majority);
        assert_eq!(d.stored().await, (None, None));
        // One connection per round: the check and the vote, nothing again.
        assert_eq!((b.accepted(), w.accepted()), (2, 2));
    }

    /// A round asks every voter at the same time: two voters that never
    /// answer cost one answer timeout, where asking one after the other
    /// would cost two, and the voter that answers is counted in the same
    /// round.
    #[tokio::test]
    async fn a_round_asks_every_voter_at_the_same_time() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, Vec::new());
        let _me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let b = Fake::on(B_AT, port, None).await;
        let d = Voter::on(D_AT, port, D, &votes).await;
        let w = Fake::on(W_AT, port, None).await;
        let wait = Duration::from_secs(3);
        let client = client_waiting(&votes, as_root, wait);

        let start = Instant::now();
        let round = client.vote(2).await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= wait && elapsed < wait.mul_f64(1.7),
            "{elapsed:?} for answer timeouts of {wait:?}: {round:?}"
        );
        for silent in [B, W] {
            assert!(
                matches!(reply(&round, silent), Reply::NoAnswer(NoAnswer::Timeout(_))),
                "{silent}: {round:?}"
            );
        }
        assert_vote(reply(&round, D), VoteOutcome::Granted, 2);
        assert_eq!(d.stored().await, (Some(2), stored(2)));
        assert_eq!((b.accepted(), w.accepted()), (1, 1));
    }

    /// The candidate's own entry may give its mesh address as an
    /// IPv4-mapped IPv6 address: the client binds the IPv4 address, the
    /// source the voters check, and B grants.
    #[tokio::test]
    async fn an_ipv4_mapped_own_address_is_the_source() {
        let (own, port) = own_port().await;
        let mut votes = spec_votes(port, Vec::new());
        votes.voters[0].mesh_address = Some(format!("::ffff:{OWN}"));
        let _me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let b = Voter::on(B_AT, port, B, &votes).await;
        let client = client(&votes, as_root);
        assert_eq!(client.mesh_address(), OWN.parse::<IpAddr>().unwrap());

        let round = client.vote(2).await.unwrap();
        assert_vote(reply(&round, B), VoteOutcome::Granted, 2);
        assert_eq!(b.stored().await, (Some(2), stored(2)));
    }

    /// Section 8.8: an answer whose `term` is above 2^53 - 1 is no answer. It
    /// neither raises `T` nor stops the checks at the ceiling, and its grant
    /// does not count.
    #[tokio::test]
    async fn an_answer_above_the_ceiling_is_no_answer() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, Vec::new());
        let _me = Voter::serve(own, ME, &votes, Some("boot-a")).await;
        let body = json!({
            "node_id": B, "term": MAX_TERM + 1, "vote": "granted", "lease_wait_ms": 0, "voters_version": 7
        });
        let _b = Fake::on(B_AT, port, Some((200, body.to_string()))).await;
        let _d = Voter::on(D_AT, port, D, &votes).await;
        let _w = Voter::on(W_AT, port, W, &votes).await;
        let client = client(&votes, as_root);

        let outcome = client.check(1).await.unwrap();
        let CheckOutcome::Settled(round) = outcome else {
            panic!("settled: {outcome:?}");
        };
        assert_eq!(round.term, 2, "{round:?}");
        assert_eq!(reply(&round, B), &Reply::NoAnswer(NoAnswer::AboveCeiling));

        let round = client.vote(2).await.unwrap();
        assert_eq!(reply(&round, B), &Reply::NoAnswer(NoAnswer::AboveCeiling));
        assert_eq!(round.granted, 2 + usize::from(root()), "{round:?}");
    }

    /// Sections 6.9 and 8.9: a vote request is sent only as root. With
    /// another effective uid both rounds refuse before any connection, and
    /// a real vote outside 1..=2^53 - 1 is refused before any connection
    /// too. As root, the same voters are asked.
    #[tokio::test]
    async fn the_client_refuses_before_any_connection_unless_root() {
        let (own, port) = own_port().await;
        let votes = spec_votes(port, Vec::new());
        let error = Some((503, json!({"error": "a fake"}).to_string()));
        let me = Fake::serve(own, error.clone());
        let b = Fake::on(B_AT, port, error).await;

        let user = client(&votes, as_user);
        assert_eq!(
            user.check(1).await,
            Err(VoteClientError::NotRoot { euid: 1000 })
        );
        assert_eq!(
            user.vote(2).await,
            Err(VoteClientError::NotRoot { euid: 1000 })
        );
        let admin = client(&votes, as_root);
        for term in [0, MAX_TERM + 1] {
            assert_eq!(admin.vote(term).await, Err(VoteClientError::BadTerm(term)));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!((me.accepted(), b.accepted()), (0, 0), "no connection");

        let round = admin.vote(2).await.unwrap();
        assert_eq!((me.accepted(), b.accepted()), (1, 1), "{round:?}");
    }
}
