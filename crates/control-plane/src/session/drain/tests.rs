// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use proptest::prelude::*;

const NODE: &str = "nod_node";
const EDGE: &str = "nod_edge";
const HOST: &str = "app.example.com";

fn at(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1_000_000_000 + seconds)
}

fn set(hostnames: &[&str]) -> BTreeSet<String> {
    hostnames
        .iter()
        .map(|hostname| (*hostname).to_string())
        .collect()
}

fn ends(drains: &Drains, machine: &str, now: OffsetDateTime) -> Vec<OffsetDateTime> {
    drains
        .open_of(machine, now)
        .into_iter()
        .map(|drain| drain.until)
        .collect()
}

#[test]
fn a_drain_ends_one_window_after_the_cname_moved() {
    let drains = Drains::default();
    // The node held the CNAME, then the hostname left it.
    drains.record_cnames(NODE, &set(&[HOST]), &set(&[HOST]), at(0));
    assert!(drains.start(NODE, HOST, "app_1", at(10)));
    assert_eq!(ends(&drains, NODE, at(10)), [at(130)], "provisional end");

    // The edge's tunnel takes the CNAME over: the window starts there.
    let affected = drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(12));
    assert_eq!(affected, set(&[NODE]));
    assert_eq!(ends(&drains, NODE, at(12)), [at(132)]);

    assert_eq!(drains.expire(at(131)), Expired::default());
    assert_eq!(drains.open_of(NODE, at(131)).len(), 1);
    // Past the end the drain is left out at once, before the sweep.
    assert!(drains.open_of(NODE, at(132)).is_empty());
    let expired = drains.expire(at(132));
    assert_eq!(expired.ended, set(&[NODE]));
    assert!(expired.waiting.is_empty());
    assert!(drains.next_end().is_none());
}

#[test]
fn a_drain_waits_for_the_cname_to_move() {
    let drains = Drains::default();
    drains.record_cnames(NODE, &set(&[HOST]), &set(&[HOST]), at(0));
    drains.start(NODE, HOST, "app_1", at(0));
    // The gaining tunnel never published: the CNAME still points here, so
    // the drain does not end on its own; the sweep checks it again.
    let expired = drains.expire(at(120));
    assert!(expired.ended.is_empty());
    assert_eq!(
        expired.waiting,
        [Waiting {
            machine: NODE.to_string(),
            hostname: HOST.to_string(),
            app_id: "app_1".to_string(),
        }]
    );
    drains.extend(NODE, HOST, at(120));
    assert_eq!(ends(&drains, NODE, at(120)), [at(240)]);
    assert_eq!(drains.next_end(), Some(at(240)));
    drains.end(NODE, HOST);
    assert!(drains.next_end().is_none());
}

#[test]
fn a_drain_whose_cname_moved_keeps_its_end_when_extended() {
    let drains = Drains::default();
    drains.start(NODE, HOST, "app_1", at(0));
    drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(5));
    drains.extend(NODE, HOST, at(100));
    assert_eq!(ends(&drains, NODE, at(100)), [at(125)]);
}

#[test]
fn a_drain_started_after_the_repoint_counts_from_the_repoint() {
    let drains = Drains::default();
    drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(0));
    drains.start(NODE, HOST, "app_1", at(100));
    assert_eq!(ends(&drains, NODE, at(100)), [at(120)]);

    // Seen again after its window, it does not start: a caller that keeps
    // seeing the departure cannot start and end it in a loop.
    assert_eq!(drains.expire(at(120)).ended, set(&[NODE]));
    assert!(!drains.start(NODE, HOST, "app_1", at(300)));
    assert!(drains.open_of(NODE, at(300)).is_empty());
    assert!(drains.next_end().is_none());
    assert_eq!(drains.expire(at(300)), Expired::default());
}

#[test]
fn starting_twice_keeps_the_first_end() {
    let drains = Drains::default();
    assert!(drains.start(NODE, HOST, "app_1", at(0)));
    assert!(!drains.start(NODE, HOST, "app_1", at(50)));
    assert_eq!(ends(&drains, NODE, at(50)), [at(120)]);
}

#[test]
fn serving_a_hostname_again_ends_only_older_drains() {
    let drains = Drains::default();
    let before = Instant::now();
    std::thread::sleep(Duration::from_millis(2));
    drains.start(NODE, HOST, "app_1", at(0));
    // A view read before the drain started may predate the move.
    drains.clear_served(NODE, &set(&[HOST]), before);
    assert_eq!(drains.open_of(NODE, at(0)).len(), 1);
    std::thread::sleep(Duration::from_millis(2));
    drains.clear_served(NODE, &set(&[HOST]), Instant::now());
    assert!(drains.open_of(NODE, at(0)).is_empty());
}

#[test]
fn a_hostname_that_moves_back_restarts_the_window_of_the_other_side() {
    let drains = Drains::default();
    drains.record_cnames(NODE, &set(&[HOST]), &set(&[HOST]), at(0));
    // One to two replicas: the node drains, the edge takes the CNAME.
    drains.start(NODE, HOST, "app_1", at(10));
    drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(11));
    // Back to one replica within the window: the node serves it again and
    // the edge drains until a window after the node took the CNAME back.
    std::thread::sleep(Duration::from_millis(2));
    drains.clear_served(NODE, &set(&[HOST]), Instant::now());
    drains.start(EDGE, HOST, "app_1", at(40));
    let affected = drains.record_cnames(NODE, &set(&[HOST]), &set(&[HOST]), at(41));
    assert_eq!(affected, set(&[EDGE]));
    assert!(drains.open_of(NODE, at(41)).is_empty());
    assert_eq!(ends(&drains, EDGE, at(41)), [at(161)]);
}

#[test]
fn a_cname_deleted_by_its_tunnel_has_no_owner() {
    let drains = Drains::default();
    drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(0));
    drains.record_cnames(EDGE, &set(&[]), &set(&[]), at(5));
    drains.start(NODE, HOST, "app_1", at(10));
    // Nobody holds the CNAME any more: it has not moved away from the node.
    assert_eq!(ends(&drains, NODE, at(10)), [at(130)]);
}

#[test]
fn forgetting_a_machine_drops_its_drains_and_cnames() {
    let drains = Drains::default();
    drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(0));
    drains.start(NODE, HOST, "app_1", at(1));
    drains.start(EDGE, "other.example.com", "app_2", at(1));
    drains.forget(EDGE);
    assert!(drains.open_of(EDGE, at(1)).is_empty());
    // The CNAME the edge held is forgotten too: the node's drain waits.
    assert_eq!(ends(&drains, NODE, at(1)), [at(121)]);
}

#[test]
fn a_shorter_window_applies_to_open_drains() {
    let drains = Drains::default();
    drains.record_cnames(EDGE, &set(&[HOST]), &set(&[HOST]), at(0));
    drains.start(NODE, HOST, "app_1", at(1));
    drains.set_window(Duration::from_secs(2));
    assert_eq!(ends(&drains, NODE, at(1)), [at(2)]);
}

// ---------------------------------------------------------------------------
// State machine: the registry against a model of the CNAME owners
// ---------------------------------------------------------------------------

const MACHINES: [&str; 3] = ["nod_a", "nod_b", "nod_e"];
const HOSTS: [&str; 2] = ["one.example.com", "two.example.com"];

#[derive(Clone, Debug)]
enum Op {
    Start(usize, usize),
    Publish(usize, Vec<bool>),
    Clear(usize, usize),
    Advance(i64),
    Expire,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..MACHINES.len(), 0..HOSTS.len()).prop_map(|(m, h)| Op::Start(m, h)),
        (
            0..MACHINES.len(),
            proptest::collection::vec(any::<bool>(), HOSTS.len())
        )
            .prop_map(|(m, hosts)| Op::Publish(m, hosts)),
        (0..MACHINES.len(), 0..HOSTS.len()).prop_map(|(m, h)| Op::Clear(m, h)),
        (0..200_i64).prop_map(Op::Advance),
        Just(Op::Expire),
    ]
}

proptest! {
    /// Whatever the order of starts, CNAME writes, clears and sweeps, a drain
    /// ends exactly one window after its CNAME moved to another machine, and
    /// never while the CNAME still points at its own machine or at nobody.
    #[test]
    fn a_drain_ends_one_window_after_its_cname_moved_and_never_before(
        ops in proptest::collection::vec(op(), 1..60)
    ) {
        let window = time::Duration::seconds(120);
        let drains = Drains::default();
        let mut now = at(0);
        // The model: CNAME owners with the time they took over, and the open
        // drains.
        let mut owners: HashMap<&str, (&str, OffsetDateTime)> = HashMap::new();
        let mut open: BTreeSet<(&str, &str)> = BTreeSet::new();
        let over = |owners: &HashMap<&str, (&str, OffsetDateTime)>, m: &str, h: &str, now| {
            owners
                .get(h)
                .is_some_and(|(owner, since)| *owner != m && now >= *since + window)
        };
        for op in ops {
            match op {
                Op::Start(m, h) => {
                    let started = drains.start(MACHINES[m], HOSTS[h], "app_1", now);
                    let key = (MACHINES[m], HOSTS[h]);
                    let expected = !open.contains(&key) && !over(&owners, key.0, key.1, now);
                    prop_assert_eq!(started, expected);
                    if expected {
                        open.insert(key);
                    }
                }
                Op::Publish(m, hosts) => {
                    let machine = MACHINES[m];
                    let written: Vec<&str> = HOSTS
                        .iter()
                        .zip(&hosts)
                        .filter(|(_, on)| **on)
                        .map(|(h, _)| *h)
                        .collect();
                    let names = set(&written);
                    let affected = drains.record_cnames(machine, &names, &names, now);
                    let mut expected = BTreeSet::new();
                    for h in &written {
                        if owners.get(h).is_none_or(|(owner, _)| *owner != machine) {
                            owners.insert(h, (machine, now));
                            for (other, _) in open.iter().filter(|(o, oh)| o != &machine && oh == h) {
                                expected.insert((*other).to_string());
                            }
                        }
                    }
                    owners.retain(|h, (owner, _)| *owner != machine || written.contains(h));
                    prop_assert_eq!(affected, expected);
                }
                Op::Clear(m, h) => {
                    let read_at = Instant::now() + Duration::from_nanos(1);
                    drains.clear_served(MACHINES[m], &set(&[HOSTS[h]]), read_at);
                    open.remove(&(MACHINES[m], HOSTS[h]));
                }
                Op::Advance(seconds) => now += time::Duration::seconds(seconds),
                Op::Expire => {
                    let expired = drains.expire(now);
                    // The sweep found every waiting drain still departed.
                    for waiting in &expired.waiting {
                        drains.extend(&waiting.machine, &waiting.hostname, now);
                    }
                    let ended: BTreeSet<String> = open
                        .iter()
                        .filter(|(m, h)| over(&owners, m, h, now))
                        .map(|(m, _)| (*m).to_string())
                        .collect();
                    prop_assert_eq!(expired.ended, ended);
                    open.retain(|(m, h)| !over(&owners, m, h, now));
                    // A drain whose CNAME has not moved got a new end ahead.
                    for machine in MACHINES {
                        for drain in drains.open_of(machine, now) {
                            let moved = owners
                                .get(drain.hostname.as_str())
                                .is_some_and(|(owner, _)| *owner != machine);
                            prop_assert!(moved || drain.until > now);
                        }
                    }
                }
            }
            for machine in MACHINES {
                let seen = drains.open_of(machine, now);
                let expected: Vec<&str> = open
                    .iter()
                    .filter(|(m, h)| *m == machine && !over(&owners, m, h, now))
                    .map(|(_, h)| *h)
                    .collect();
                let hosts: Vec<&str> = seen.iter().map(|drain| drain.hostname.as_str()).collect();
                prop_assert_eq!(&hosts, &expected);
                for drain in &seen {
                    if let Some((owner, since)) = owners.get(drain.hostname.as_str())
                        && *owner != machine
                    {
                        prop_assert_eq!(drain.until, *since + window);
                    }
                }
            }
        }
    }
}
