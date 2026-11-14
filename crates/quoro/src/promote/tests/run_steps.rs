//! Steps 2 to 6 of `quoro cluster promote` (section 7.3) through the
//! recording fake, after the refusals ran on the fixture's host: the order
//! of the effects, every stop before the freeze and after it, and what a
//! stop says.

use quoro_control_plane::promotion::Promotion;
use quoro_node_agent::vote_client::CheckOutcome;
use quoro_proto::wire::MAX_TERM;
use serde_json::json;

use super::fake::{HostCheck, MAX_TOKEN, URL, WITNESS_ID, default_log};
use super::{Fixture, NODE_ID, OTHER_NODE_ID, accept_loss};
use crate::host::DATABASE;
use crate::promote::command::{Flags, Outcome, exit_status};
use crate::promote::refusals::Refusal;
use crate::promote::steps::{OwnVote, Stop, Stopped};

/// The events up to the end of step 2's pre-check: the refusals of this
/// layer, then the pre-check.
const PRE_CHECK: [&str; 7] = [
    "check_host",
    "log",
    "open_keys",
    "check_log",
    "votes",
    "seen",
    "check",
];

/// The events from the lock to the real vote: the lock, the freeze, and the
/// checks again on the frozen log.
const FROZEN: [&str; 6] = ["lock", "freeze", "reads", "log", "open_keys", "check_log"];

/// The events of a promotion that runs to the end.
fn promotion() -> Vec<&'static str> {
    let mut events = PRE_CHECK.to_vec();
    events.extend(FROZEN);
    events.extend([
        // Step 2's vote and this machine's stored vote, then step 3's
        // wait: 12.5 s in steps of 5 s.
        "vote",
        "own_vote",
        "sleep",
        "sleep",
        "sleep",
        // Step 4.
        "reads",
        "log",
        "build",
        // Step 5, then step 6.
        "write",
        "install_keys",
        "prepare_host",
        "unfreeze",
        "point_agent",
    ]);
    events
}

/// `events` with `more` after them.
fn then(events: &[&'static str], more: &[&'static str]) -> Vec<&'static str> {
    events.iter().chain(more).copied().collect()
}

/// `--ack-fence` with its statement.
fn ack_fence() -> Flags {
    Flags {
        ack_fence: true,
        statement: Some("the old primary's host is down and stays down".to_string()),
        ..Flags::default()
    }
}

/// The stop of a promotion, which must have stopped after its freeze.
fn stopped(result: Result<Outcome, Refusal>) -> Stopped {
    assert_eq!(exit_status(&result), 1);
    match result {
        Ok(Outcome::Stopped(stopped)) => stopped,
        other => panic!("{other:?}"),
    }
}

/// Issue #19's acceptance criterion and section 7.3's order: the refusals,
/// the pre-check, the freeze, the vote, the wait, the build, the write, the
/// keys, the start, the end of the freeze and the agent pointed at the new
/// primary. Every vote connection, the pre-check's and the real one's, is
/// opened before step 6 hands the data directory to the control plane's
/// user and starts it as that user. The pin test
/// `the_command_never_runs_as_the_data_directory_s_owner` checks that
/// nothing changes the process's user meanwhile.
#[tokio::test]
async fn a_promotion_opens_every_vote_connection_before_it_hands_anything_to_the_owner() {
    let fixture = Fixture::new().await;
    let (result, lines) = fixture.promote(Flags::default()).await;
    assert_eq!(exit_status(&result), 0);
    assert_eq!(result.unwrap(), Outcome::Promoted, "{lines:#?}");
    let events = fixture.steps.events();
    assert_eq!(events, promotion());

    let handed = events
        .iter()
        .position(|event| *event == "prepare_host")
        .unwrap();
    let votes: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| matches!(**event, "check" | "vote"))
        .map(|(at, _)| at)
        .collect();
    assert_eq!(votes.len(), 2, "{events:?}");
    assert!(votes.iter().all(|at| *at < handed), "{events:?}");

    // The term is one above the highest this machine has seen, the one the
    // pre-check settled on, and the build and the write take it.
    assert_eq!(fixture.steps.voted.get(), Some(5));
    assert_eq!(fixture.steps.built.get(), Some(5));
    assert_eq!(
        fixture.steps.written.borrow().clone(),
        Some(Promotion {
            term: 5,
            to_node_id: NODE_ID.to_string(),
            log_seq: 3,
            ack_fence: false,
            accept_loss: false,
            statement: None,
        })
    );
    assert_eq!(fixture.steps.pointed.borrow().as_deref(), Some(URL));
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);

    // Step 3 printed the time left and slept until the latest receipt time
    // plus `lease_wait_ms`.
    for line in [
        "step 3, the wait for the old primary's grants: 13 s left",
        "step 3, the wait for the old primary's grants: 8 s left",
        "step 3, the wait for the old primary's grants: 3 s left",
        "step 3, the wait for the old primary's grants: over",
    ] {
        assert!(lines.iter().any(|l| l == line), "{line:?} in {lines:#?}");
    }
    let slept: std::time::Duration = fixture.steps.slept.borrow().iter().sum();
    assert_eq!(slept.as_millis(), 12_500);
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("  vote round at term 5")),
        "{lines:#?}"
    );
    assert_eq!(
        lines.last().unwrap(),
        "promoted node nod_01JC9Z0000000000000000000A to primary of cluster \
         cls_01JC9Z0000000000000000000Q at term 5"
    );
}

/// Issue #862: when the next term would pass 2^53 - 1, or the terms keep
/// rising, the command stops after the pre-check, before any real vote
/// request and before the freeze, and changes nothing.
#[tokio::test]
async fn a_pre_check_that_does_not_settle_stops_before_the_freeze() {
    let fixture = Fixture::new().await;
    fixture.steps.seen.set(MAX_TERM);
    *fixture.steps.check.borrow_mut() = Some(CheckOutcome::AboveCeiling {
        highest: MAX_TERM,
        last: None,
    });
    let (result, _) = fixture.promote(Flags::default()).await;
    assert_eq!(exit_status(&result), 1);
    let refusal = result.unwrap_err();
    assert!(
        matches!(refusal, Refusal::AboveCeiling { highest: MAX_TERM }),
        "{refusal}"
    );
    let text = refusal.to_string();
    assert!(
        text.contains("before any real vote request and before the freeze")
            && text.contains("Nothing was changed"),
        "{text}"
    );
    assert_eq!(fixture.steps.events(), PRE_CHECK);
    assert_eq!(fixture.steps.voted.get(), None);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
    assert!(fixture.steps.written.borrow().is_none());

    fixture.steps.events.borrow_mut().clear();
    fixture.steps.seen.set(4);
    let last = fixture.steps.round(12, true);
    *fixture.steps.check.borrow_mut() = Some(CheckOutcome::Unsettled { term: 13, last });
    let (result, lines) = fixture.promote(Flags::default()).await;
    let refusal = result.unwrap_err();
    assert!(
        matches!(refusal, Refusal::Unsettled { term: 13 }),
        "{refusal}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("  check round at term 12")),
        "{lines:#?}"
    );
    assert_eq!(fixture.steps.events(), PRE_CHECK);
    assert_eq!(fixture.steps.voted.get(), None);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
}

/// Issue #931: with `--ack-fence`, a lease the old primary's node holds at
/// the token cap refuses before any vote request: no term is taken, the
/// agent is not frozen, nothing is written, and the refusal names the lease.
/// One below the cap passes, and step 5 declares that node lost.
#[tokio::test]
async fn ack_fence_with_a_lease_at_the_cap_is_refused_before_any_vote() {
    let fixture = Fixture::new().await;
    fixture.steps.set_log(&default_log(MAX_TOKEN));
    let (result, _) = fixture.promote(ack_fence()).await;
    assert_eq!(exit_status(&result), 1);
    let refusal = result.unwrap_err();
    let Refusal::TokenExhausted { node_id, detail } = &refusal else {
        panic!("{refusal}");
    };
    assert_eq!(node_id, OTHER_NODE_ID);
    assert!(
        detail.contains("lease lse_1 is at fencing token 9007199254740991"),
        "{detail}"
    );
    let text = refusal.to_string();
    assert!(
        text.contains("no term was taken and the agent was not frozen")
            && text.contains("Nothing was changed"),
        "{text}"
    );
    assert_eq!(fixture.steps.events(), ["check_host", "log", "open_keys"]);
    assert_eq!(fixture.steps.voted.get(), None);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
    assert!(fixture.steps.written.borrow().is_none());

    // Without --ack-fence the cap is not checked: no node is declared lost.
    fixture.steps.events.borrow_mut().clear();
    let (result, lines) = fixture.promote(Flags::default()).await;
    assert_eq!(result.unwrap(), Outcome::Promoted, "{lines:#?}");

    fixture.steps.events.borrow_mut().clear();
    fixture.steps.set_log(&default_log(MAX_TOKEN - 1));
    let (result, lines) = fixture.promote(ack_fence()).await;
    assert_eq!(result.unwrap(), Outcome::Promoted, "{lines:#?}");
    assert!(
        lines.iter().any(|line| line.contains(&format!(
            "step 5 declares the old primary's node {OTHER_NODE_ID} lost"
        ))),
        "{lines:#?}"
    );
    let written = fixture.steps.written.borrow().clone().unwrap();
    assert!(written.ack_fence, "{written:?}");
    // --ack-fence skips step 3's wait, and only it.
    assert_eq!(
        fixture.steps.events(),
        promotion()
            .into_iter()
            .filter(|event| *event != "sleep")
            .collect::<Vec<_>>()
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("step 3, the wait for the old primary's grants: skipped")),
        "{lines:#?}"
    );
}

/// `--dry-run` stops after the pre-check, freezes nothing and writes
/// nothing. It exits 0 when a majority would grant the term, and 1 when the
/// promotion would stop at step 2.
#[tokio::test]
async fn a_dry_run_stops_after_the_pre_check_and_exits_by_its_majority() {
    let fixture = Fixture::new().await;
    let dry_run = || Flags {
        dry_run: true,
        ..Flags::default()
    };
    let (result, lines) = fixture.promote(dry_run()).await;
    assert_eq!(exit_status(&result), 0);
    assert_eq!(result.unwrap(), Outcome::DryRun { majority: true });
    assert_eq!(fixture.steps.events(), PRE_CHECK);
    assert!(
        lines
            .iter()
            .any(|line| line.contains("step 3 would wait about 13 s")),
        "{lines:#?}"
    );
    assert!(
        lines
            .last()
            .unwrap()
            .starts_with("dry run: stopped after step 2's vote pre-check"),
        "{lines:#?}"
    );

    fixture.steps.events.borrow_mut().clear();
    *fixture.steps.grant.borrow_mut() = vec![NODE_ID];
    let (result, lines) = fixture.promote(dry_run()).await;
    assert_eq!(exit_status(&result), 1);
    assert_eq!(result.unwrap(), Outcome::DryRun { majority: false });
    assert!(
        lines
            .iter()
            .any(|line| line == "  1 of 3 voters would grant term 5: not a majority"),
        "{lines:#?}"
    );
    assert_eq!(fixture.steps.events(), PRE_CHECK);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
    assert!(fixture.steps.written.borrow().is_none());
}

/// No majority in the pre-check stops before the lock and the freeze, and
/// prints each voter's answer.
#[tokio::test]
async fn no_majority_in_the_pre_check_stops_before_the_freeze() {
    let fixture = Fixture::new().await;
    *fixture.steps.grant.borrow_mut() = vec![NODE_ID];
    let (result, lines) = fixture.promote(Flags::default()).await;
    let refusal = result.unwrap_err();
    assert!(
        matches!(
            refusal,
            Refusal::NoMajority {
                term: 5,
                granted: 1,
                voters: 3
            }
        ),
        "{refusal}"
    );
    assert!(refusal.to_string().contains("no voter moved"), "{refusal}");
    for line in [
        // A check answer carries the voter's own term, below the one asked.
        format!("    {NODE_ID} (this machine): granted, at term 4, lease wait 12500 ms"),
        format!("    {OTHER_NODE_ID}: refused, support_held, at term 4, retry after 1000 ms"),
        format!("    {WITNESS_ID}: refused, support_held, at term 4, retry after 1000 ms"),
    ] {
        assert!(lines.contains(&line), "{line:?} in {lines:#?}");
    }
    assert_eq!(fixture.steps.events(), PRE_CHECK);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
}

/// No majority in the real round stops with each answer printed, and the
/// freeze ends. The message says whether this machine's agent granted the
/// term: when it did, it now demotes the old primary.
#[tokio::test]
async fn no_majority_in_the_real_round_ends_the_freeze() {
    let fixture = Fixture::new().await;
    *fixture.steps.check.borrow_mut() = Some(CheckOutcome::Settled(fixture.steps.round(5, true)));
    for (grant, own) in [(WITNESS_ID, false), (NODE_ID, true)] {
        fixture.steps.events.borrow_mut().clear();
        *fixture.steps.grant.borrow_mut() = vec![grant];
        let (result, lines) = fixture.promote(Flags::default()).await;
        let stopped = stopped(result);
        assert_eq!(stopped.at, Stop::NoMajority);
        let expected = if own {
            OwnVote::Granted
        } else {
            OwnVote::NotGranted
        };
        assert_eq!(stopped.own_vote, Some(expected));
        assert_eq!(stopped.freeze, Ok(()));
        assert_eq!(stopped.database, None);
        assert_eq!(
            fixture.steps.events(),
            then(
                &then(&PRE_CHECK, &FROZEN),
                &["vote", "own_vote", "unfreeze"]
            )
        );
        assert_eq!(*fixture.steps.frozen_at.borrow(), None);
        assert!(fixture.steps.written.borrow().is_none());
        assert!(
            lines
                .iter()
                .any(|line| line == "  1 of 3 voters granted term 5: not a majority"),
            "{lines:#?}"
        );
        let text = stopped.to_string();
        assert!(text.contains("The freeze ended"), "{text}");
        if own {
            assert!(
                text.contains("granted term 5") && text.contains("stale_term"),
                "{text}"
            );
        } else {
            assert!(text.contains("did not grant term 5"), "{text}");
        }
    }
}

/// A log that changed after the freeze stops step 4 before the build: the
/// freeze ends and nothing is written.
#[tokio::test]
async fn a_log_that_changed_after_the_freeze_stops_step_4() {
    let fixture = Fixture::new().await;
    *fixture.steps.grow.borrow_mut() =
        Some(("vote", vec![("project", "prj_9", json!({"id": "prj_9"}))]));
    let (result, _) = fixture.promote(Flags::default()).await;
    let stopped = stopped(result);
    assert_eq!(stopped.at, Stop::Build);
    assert!(
        stopped
            .why
            .contains("the followed log changed since the freeze: it was at seq 3 and is at seq 4"),
        "{}",
        stopped.why
    );
    assert_eq!(stopped.database, None);
    assert_eq!(stopped.freeze, Ok(()));
    let mut events = then(&PRE_CHECK, &FROZEN);
    events.extend([
        "vote", "own_vote", "sleep", "sleep", "sleep", "reads", "log", "unfreeze",
    ]);
    assert_eq!(fixture.steps.events(), events);
    assert_eq!(fixture.steps.built.get(), None);
    assert!(fixture.steps.written.borrow().is_none());
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
    assert!(!stopped.to_string().contains("rejoin"), "{stopped}");
}

/// A log quarantined between the refusals and the freeze stops before the
/// first real vote request, `--accept-loss` or not (ADR 0280): the freeze
/// ends and no voter moved.
#[tokio::test]
async fn a_frozen_log_that_fails_a_refusal_stops_before_the_real_vote() {
    let fixture = Fixture::new().await;
    fixture.steps.quarantined.set(true);
    for flags in [Flags::default(), accept_loss()] {
        fixture.steps.events.borrow_mut().clear();
        let (result, _) = fixture.promote(flags).await;
        let stopped = stopped(result);
        assert_eq!(stopped.at, Stop::FrozenLog);
        assert!(stopped.why.contains("quarantined"), "{}", stopped.why);
        assert!(
            !stopped.why.contains("Nothing was changed"),
            "{}",
            stopped.why
        );
        assert_eq!(stopped.own_vote, None);
        assert_eq!(
            fixture.steps.events(),
            then(&PRE_CHECK, &["lock", "freeze", "reads", "unfreeze"])
        );
        assert_eq!(fixture.steps.voted.get(), None);
        assert!(
            stopped
                .to_string()
                .contains("No real vote request was sent"),
            "{stopped}"
        );
    }
}

/// A stop once step 4 wrote the database ends the freeze too (section 7.3,
/// "If the promotion stops") and says how to retry: the database stays, so
/// a rerun is refused until `quoro node rejoin --force --statement` moves it
/// aside. A stop at the build itself leaves no database and says nothing of
/// a rejoin. Once step 6 started the control plane and ended the freeze, a
/// stop says how to point the agent at it.
#[tokio::test]
async fn a_stop_after_the_database_is_written_ends_the_freeze_and_says_how_to_retry() {
    let fixture = Fixture::new().await;
    let database = fixture.paths.data_dir.join(DATABASE);
    let shown = database.display().to_string();
    for (fail, at) in [
        ("write", Stop::Write),
        ("install_keys", Stop::Keys),
        ("prepare_host", Stop::Prepare),
    ] {
        fixture.steps.events.borrow_mut().clear();
        fixture.steps.fail.set(Some(fail));
        let (result, _) = fixture.promote(Flags::default()).await;
        let stopped = stopped(result);
        assert_eq!(stopped.at, at, "{fail}");
        assert_eq!(stopped.database.as_deref(), Some(database.as_path()));
        assert_eq!(stopped.freeze, Ok(()));
        let events = fixture.steps.events();
        assert_eq!(events[events.len() - 2..], [fail, "unfreeze"], "{events:?}");
        assert!(events.contains(&"build"), "{events:?}");
        assert_eq!(*fixture.steps.frozen_at.borrow(), None);
        let text = stopped.to_string();
        for words in [
            "The freeze ended",
            "granted term 5",
            "a rerun is refused",
            "quoro node rejoin --force --statement",
            shown.as_str(),
        ] {
            assert!(text.contains(words), "{words:?} in {text}");
        }
        assert_eq!(
            text.contains("quoro.service may already be installed, enabled or started"),
            at == Stop::Prepare,
            "{text}"
        );
    }

    fixture.steps.events.borrow_mut().clear();
    fixture.steps.fail.set(Some("build"));
    let (result, _) = fixture.promote(Flags::default()).await;
    let stopped_at_build = stopped(result);
    assert_eq!(stopped_at_build.at, Stop::Build);
    assert_eq!(stopped_at_build.database, None);
    assert!(
        !stopped_at_build.to_string().contains("rejoin"),
        "{stopped_at_build}"
    );

    fixture.steps.events.borrow_mut().clear();
    fixture.steps.fail.set(Some("point_agent"));
    let (result, _) = fixture.promote(Flags::default()).await;
    let pointed = stopped(result);
    assert_eq!(pointed.at, Stop::Point);
    assert_eq!(pointed.started.as_deref(), Some(URL));
    let events = fixture.steps.events();
    assert_eq!(
        events[events.len() - 3..],
        ["prepare_host", "unfreeze", "point_agent"],
        "the freeze is ended once: {events:?}"
    );
    let text = pointed.to_string();
    assert!(
        text.contains(&format!("quoro node set-primary {URL}")) && !text.contains("rejoin"),
        "{text}"
    );
}

/// The refusals of this layer come before any vote request and freeze
/// nothing: a host step 6 would refuse, keys it would refuse, a log that
/// does not fold, another promotion holding the lock, and a freeze that
/// fails.
#[tokio::test]
async fn the_refusals_of_steps_2_to_6_come_before_any_real_vote() {
    let fixture = Fixture::new().await;
    for (fail, name, events) in [
        ("check_host", "host", PRE_CHECK[..1].to_vec()),
        ("open_keys", "primary keys", PRE_CHECK[..3].to_vec()),
        ("check_log", "does not fold", PRE_CHECK[..4].to_vec()),
        ("lock", "another promotion", then(&PRE_CHECK, &["lock"])),
        (
            "freeze",
            "freeze failed",
            then(&PRE_CHECK, &["lock", "freeze"]),
        ),
    ] {
        fixture.steps.events.borrow_mut().clear();
        fixture.steps.fail.set(Some(fail));
        let (result, _) = fixture.promote(Flags::default()).await;
        assert_eq!(exit_status(&result), 1);
        let refusal = result.unwrap_err();
        assert_eq!(super::name_of(&refusal), name, "{refusal}");
        assert!(
            refusal.to_string().ends_with("Nothing was changed"),
            "{refusal}"
        );
        assert_eq!(fixture.steps.events(), events, "{fail}");
        assert_eq!(fixture.steps.voted.get(), None);
        assert_eq!(*fixture.steps.frozen_at.borrow(), None);
    }
}

/// Step 6's host refusals run before any vote request (section 7.3), on the
/// host as it is before the database and the keys exist: a host that would
/// pass is promoted, and a symbolic link at `/etc/quoro` or a `quoro-api`
/// with a login uid is refused with no vote request, nothing frozen and
/// nothing written, saying once that nothing was changed.
#[tokio::test]
async fn a_host_step_6_would_refuse_is_refused_before_any_vote() {
    let fixture = Fixture::new().await;
    *fixture.steps.host.borrow_mut() = Some(HostCheck::new(&fixture.root.join("host")));
    fixture.passes().await;

    let etc = fixture.steps.host.borrow().as_ref().unwrap().etc();
    let real = etc.with_file_name("quoro-real");
    std::fs::rename(&etc, &real).unwrap();
    std::os::unix::fs::symlink("quoro-real", &etc).unwrap();
    refused_by_host(&fixture, "is a symbolic link").await;
    std::fs::remove_file(&etc).unwrap();
    std::fs::rename(&real, &etc).unwrap();

    if let Some(host) = fixture.steps.host.borrow().as_ref() {
        host.uid.set(Some(1000));
        host.gid.set(Some(1000));
    }
    refused_by_host(&fixture, "uid 1000, which is a login account").await;
}

/// Asserts that a promotion is refused at step 6's host check, with a text
/// that holds `says` and says once that nothing was changed, before any
/// other step: no vote request, no stored vote, no freeze, no write.
async fn refused_by_host(fixture: &Fixture, says: &str) {
    fixture.steps.events.borrow_mut().clear();
    *fixture.steps.stored_vote.borrow_mut() = None;
    *fixture.steps.written.borrow_mut() = None;
    let (result, _) = fixture.promote(Flags::default()).await;
    assert_eq!(exit_status(&result), 1);
    let refusal = result.unwrap_err();
    assert_eq!(super::name_of(&refusal), "host", "{refusal}");
    let text = refusal.to_string();
    assert!(text.contains(says), "{says:?} in {text}");
    assert_eq!(
        text.to_lowercase().matches("nothing was changed").count(),
        1,
        "{text}"
    );
    assert_eq!(fixture.steps.events(), ["check_host"]);
    assert_eq!(*fixture.steps.stored_vote.borrow(), None);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
    assert!(fixture.steps.written.borrow().is_none());
}

/// The followed log's check before the votes: a log the fold refuses is
/// refused with the fold's error, and a check that could not run is not
/// reported as a log that does not fold. Neither sends a vote request.
#[tokio::test]
async fn a_log_the_fold_refuses_is_refused_before_any_vote_and_a_check_that_did_not_run_says_so() {
    let fixture = Fixture::new().await;
    fixture.steps.fail.set(Some("check_log"));
    let refusal = fixture.promote(Flags::default()).await.0.unwrap_err();
    assert_eq!(super::name_of(&refusal), "does not fold", "{refusal}");
    assert!(
        refusal
            .to_string()
            .contains("the user usr_9 never received a create"),
        "{refusal}"
    );
    assert_eq!(fixture.steps.events(), PRE_CHECK[..4]);

    fixture.steps.events.borrow_mut().clear();
    fixture.steps.fail.set(None);
    fixture.steps.check_unrun.set(true);
    let refusal = fixture.promote(Flags::default()).await.0.unwrap_err();
    assert_eq!(super::name_of(&refusal), "unreadable", "{refusal}");
    let text = refusal.to_string();
    assert!(
        text.contains("cannot check the followed log") && text.contains("out of memory"),
        "{text}"
    );
    assert!(!text.contains("does not fold"), "{text}");
    assert_eq!(fixture.steps.events(), PRE_CHECK[..4]);
    assert_eq!(fixture.steps.voted.get(), None);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
}

/// Whether this machine's agent granted the term comes from the vote it
/// stored, not from its answer: an answer lost after the agent stored its
/// vote still says that it granted, and a vote that cannot be read says
/// that it may have.
#[tokio::test]
async fn this_machine_s_grant_is_read_from_the_vote_its_agent_stored() {
    let fixture = Fixture::new().await;
    *fixture.steps.check.borrow_mut() = Some(CheckOutcome::Settled(fixture.steps.round(5, true)));
    *fixture.steps.grant.borrow_mut() = vec![NODE_ID];
    fixture.steps.lose_own_answer.set(true);
    let (result, lines) = fixture.promote(Flags::default()).await;
    let lost = stopped(result);
    assert_eq!(lost.at, Stop::NoMajority);
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with(&format!("    {NODE_ID} (this machine): no answer"))),
        "{lines:#?}"
    );
    assert_eq!(lost.own_vote, Some(OwnVote::Granted));
    let text = lost.to_string();
    assert!(
        text.contains("This machine's agent granted term 5") && text.contains("stale_term"),
        "{text}"
    );
    assert_eq!(
        fixture.steps.events(),
        then(
            &then(&PRE_CHECK, &FROZEN),
            &["vote", "own_vote", "unfreeze"]
        )
    );

    fixture.steps.events.borrow_mut().clear();
    fixture.steps.fail.set(Some("own_vote"));
    let unknown = stopped(fixture.promote(Flags::default()).await.0);
    assert_eq!(unknown.at, Stop::NoMajority);
    assert!(
        matches!(&unknown.own_vote, Some(OwnVote::Unknown(why)) if why.contains("own_vote failed")),
        "{:?}",
        unknown.own_vote
    );
    let text = unknown.to_string();
    assert!(
        text.contains("may have granted term 5") && text.contains("its stored vote cannot be read"),
        "{text}"
    );
    assert_eq!(unknown.freeze, Ok(()));
}

/// The real vote requests not sent stop at step 2 with no voter moved, and
/// the freeze ends.
#[tokio::test]
async fn real_vote_requests_that_were_not_sent_stop_with_no_voter_moved() {
    let fixture = Fixture::new().await;
    fixture.steps.fail.set(Some("vote"));
    let stopped = stopped(fixture.promote(Flags::default()).await.0);
    assert_eq!(stopped.at, Stop::Vote);
    assert_eq!(stopped.own_vote, None);
    assert_eq!(stopped.freeze, Ok(()));
    assert_eq!(
        fixture.steps.events(),
        then(&then(&PRE_CHECK, &FROZEN), &["vote", "unfreeze"])
    );
    let text = stopped.to_string();
    assert!(
        text.contains("the vote requests were not sent: vote failed")
            && text.contains("No real vote request was sent, so no voter moved"),
        "{text}"
    );
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
}

/// A freeze step 6 cannot end stops there: the stop tries once more, and
/// when that fails too it says that `--abort` ends the freeze and that the
/// agent is pointed at the started control plane once it has ended.
#[tokio::test]
async fn a_freeze_step_6_cannot_end_says_how_to_end_it() {
    let fixture = Fixture::new().await;
    fixture.steps.fail.set(Some("unfreeze"));
    let stopped = stopped(fixture.promote(Flags::default()).await.0);
    assert_eq!(stopped.at, Stop::Unfreeze);
    assert_eq!(stopped.freeze, Err("unfreeze failed".to_string()));
    assert_eq!(stopped.started.as_deref(), Some(URL));
    let events = fixture.steps.events();
    assert_eq!(
        events[events.len() - 3..],
        ["prepare_host", "unfreeze", "unfreeze"],
        "{events:?}"
    );
    let text = stopped.to_string();
    let point = format!("quoro node set-primary {URL}");
    let says: [&str; 4] = [
        "stopped at step 6, the end of the freeze: cannot end the freeze: unfreeze failed",
        "The freeze could not be ended (unfreeze failed): quoro cluster promote --abort ends it",
        "End the freeze first",
        &point,
    ];
    for words in says {
        assert!(text.contains(words), "{words:?} in {text}");
    }
    assert!(fixture.steps.frozen_at.borrow().is_some());
}

/// The frozen log is the one checked again and built: when the agent stored
/// an entry between the pre-check's log and the freeze, the frozen checks,
/// the build and the write all take the log with it.
#[tokio::test]
async fn the_frozen_checks_and_the_build_read_the_frozen_log() {
    let fixture = Fixture::new().await;
    *fixture.steps.grow.borrow_mut() =
        Some(("check", vec![("project", "prj_9", json!({"id": "prj_9"}))]));
    let (result, lines) = fixture.promote(Flags::default()).await;
    assert_eq!(result.unwrap(), Outcome::Promoted, "{lines:#?}");
    assert_eq!(*fixture.steps.checked.borrow(), [3, 4]);
    assert_eq!(fixture.steps.built_seq.get(), Some(4));
    assert_eq!(
        fixture.steps.written.borrow().as_ref().map(|w| w.log_seq),
        Some(4)
    );
}

/// With `--ack-fence`, a lease of the old primary's node that reached the
/// token cap between the pre-check's log and the freeze stops before the
/// first real vote request: the freeze ends and no voter moved.
#[tokio::test]
async fn ack_fence_with_a_lease_at_the_cap_in_the_frozen_log_stops_before_the_real_vote() {
    let fixture = Fixture::new().await;
    *fixture.steps.grow.borrow_mut() = Some((
        "check",
        vec![(
            "lease",
            "lse_1",
            json!({"id": "lse_1", "holder_node_id": OTHER_NODE_ID, "fencing_token": MAX_TOKEN}),
        )],
    ));
    let stopped = stopped(fixture.promote(ack_fence()).await.0);
    assert_eq!(stopped.at, Stop::FrozenLog);
    let declared = format!("--ack-fence declares the old primary's node {OTHER_NODE_ID} lost");
    let says: [&str; 2] = [
        &declared,
        "lease lse_1 is at fencing token 9007199254740991",
    ];
    for words in says {
        assert!(stopped.why.contains(words), "{words:?} in {}", stopped.why);
    }
    assert_eq!(stopped.own_vote, None);
    assert_eq!(
        fixture.steps.events(),
        then(
            &PRE_CHECK,
            &["lock", "freeze", "reads", "log", "open_keys", "unfreeze"]
        )
    );
    assert_eq!(fixture.steps.voted.get(), None);
    assert_eq!(*fixture.steps.frozen_at.borrow(), None);
}
