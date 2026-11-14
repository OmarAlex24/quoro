//! Steps 2 to 6 of `quoro cluster promote` (section 7.3) through the
//! recording fake, after the refusals ran on the fixture's host: the order
//! of the effects, every stop before the freeze and after it, and what a
//! stop says.

use quoro_control_plane::promotion::Promotion;
use quoro_node_agent::vote_client::CheckOutcome;
use quoro_proto::wire::MAX_TERM;

use super::fake::{MAX_TOKEN, URL, WITNESS_ID, default_log};
use super::{Fixture, NODE_ID, OTHER_NODE_ID, accept_loss};
use crate::host::DATABASE;
use crate::promote::command::{Flags, Outcome, exit_status};
use crate::promote::refusals::Refusal;
use crate::promote::steps::{Stop, Stopped};

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
        // Step 2's vote, then step 3's wait: 12.5 s in steps of 5 s.
        "vote",
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
        format!("    {NODE_ID} (this machine): granted, at term 5, lease wait 12500 ms"),
        format!("    {OTHER_NODE_ID}: refused, support_held, at term 5, retry after 1000 ms"),
        format!("    {WITNESS_ID}: refused, support_held, at term 5, retry after 1000 ms"),
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
        assert_eq!(stopped.own_granted, Some(own));
        assert_eq!(stopped.freeze, Ok(()));
        assert_eq!(stopped.database, None);
        assert_eq!(
            fixture.steps.events(),
            then(&then(&PRE_CHECK, &FROZEN), &["vote", "unfreeze"])
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
    fixture.steps.grow_during_vote.set(true);
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
        "vote", "sleep", "sleep", "sleep", "reads", "log", "unfreeze",
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
        assert_eq!(stopped.own_granted, None);
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
