//! `quoro cluster promote` against a host laid out in a temporary directory:
//! the agent's data directory with its role, identity, custody key, mark and
//! store, the control-plane data directory, and a fake systemd, uid and
//! clock. The followed log is written into the agent's store as the agent
//! writes it, with seals made by `quoro_proto::custody::seal`.

use std::cell::{Cell, RefCell};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::SystemTime;

use quoro_control_plane::custody::TlsIdentity;
use quoro_control_plane::store::Store;
use quoro_node_agent::follow::{LogEntry, MAX_SEQ};
use quoro_node_agent::freeze::{self, PromotionLock};
use quoro_node_agent::identity::{self, CUSTODY_KEY_FILE, IDENTITY_FILE, Identity, LOG_MARK_FILE};
use quoro_node_agent::store::{
    FreezeChecked, NodeStore, PROMOTION_FREEZE, Refollowing, WELCOME_SCHEMA_VERSION,
};
use quoro_proto::custody::{self, CustodyKey, CustodyPublicKey, KeyKind, SealContext};
use quoro_proto::sign;
use serde_json::{Value, json};

use super::command::{Flags, Host, Out, Outcome, Paths, Request, parse_args, run};
use super::refusals::{RESTORE_PENDING, Refusal};
use crate::node::{Role, write_role};

const NODE_ID: &str = "nod_01JC9Z0000000000000000000A";
const OTHER_NODE_ID: &str = "nod_01JC9Z0000000000000000000B";
const CLUSTER_ID: &str = "cls_01JC9Z0000000000000000000Q";
/// The data key sealed to this machine.
const DEK_ID: &str = "00112233445566778899aabbccddeeff";
/// The data key sealed to the other machine only.
const OTHER_DEK_ID: &str = "ffeeddccbbaa99887766554433221100";
/// When every entry was written and the head received.
const TIME: &str = "2026-11-13T10:00:00.000Z";
/// The clock: 95 seconds after [`TIME`].
const NOW: &str = "2026-11-13T10:01:35Z";
/// The cluster signing key's seed.
const SEED: [u8; 32] = [7; 32];
/// The pinned certificate. `TlsIdentity` hashes the DER of its first block,
/// so any base64 does.
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nAQIDBA==\n-----END CERTIFICATE-----\n";
/// Another certificate.
const OTHER_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nBQYHCA==\n-----END CERTIFICATE-----\n";
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nCQoLDA==\n-----END PRIVATE KEY-----\n";

static NEXT: AtomicU32 = AtomicU32::new(0);

/// One change before it is a log entry.
struct Change {
    entity_type: &'static str,
    entity_id: String,
    op: &'static str,
    payload: Value,
}

/// A host in a temporary directory, and the fake reads of [`Host`].
struct Fixture {
    root: PathBuf,
    paths: Paths,
    euid: Cell<Option<u32>>,
    unit: Cell<bool>,
    now: SystemTime,
    /// Every [`Host`] call, in order.
    reads: RefCell<Vec<&'static str>>,
    /// This machine's custody key.
    custody_key: CustodyKey,
}

impl Host for Fixture {
    fn euid(&self) -> Option<u32> {
        self.reads.borrow_mut().push("euid");
        self.euid.get()
    }

    fn paths(&self) -> Result<Paths, String> {
        self.reads.borrow_mut().push("paths");
        Ok(self.paths.clone())
    }

    fn control_plane_unit_known(&self) -> bool {
        self.reads.borrow_mut().push("unit");
        self.unit.get()
    }

    fn now(&self) -> SystemTime {
        self.reads.borrow_mut().push("clock");
        self.now
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl Fixture {
    /// A machine with the node role, enrolled, with its custody key and an
    /// agent store whose log is empty, run as root, with no control plane.
    async fn bare() -> Self {
        let root = std::env::temp_dir().join(format!(
            "quoro-promote-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        let paths = Paths {
            data_dir: root.join("quoro"),
            node_data_dir: root.join("quoro-node"),
        };
        fs::create_dir_all(&paths.data_dir).unwrap();
        identity::ensure_data_dir(&paths.node_data_dir).unwrap();
        let fixture = Self {
            root,
            paths,
            euid: Cell::new(Some(0)),
            unit: Cell::new(false),
            now: at(NOW),
            reads: RefCell::new(Vec::new()),
            custody_key: CustodyKey::generate(&mut custody::os_rng()),
        };
        let dir = fixture.node_dir();
        write_role(dir, Role::Node).unwrap();
        fixture.pin(SEED, CERT_PEM);
        identity::write_credential(dir, "credential").unwrap();
        identity::write_seed(dir, &[1; 32]).unwrap();
        fs::write(
            dir.join(CUSTODY_KEY_FILE),
            &fixture.custody_key.to_bytes()[..],
        )
        .unwrap();
        drop(fixture.store().await);
        fixture
    }

    /// A machine that passes every refusal: [`Fixture::bare`] with a log of
    /// six entries, a head at 9 received at [`TIME`], its mark at 6 and the
    /// schema version of this binary. The log holds the signing key's row,
    /// this machine's three seals, and a data key sealed to another machine
    /// only.
    async fn new() -> Self {
        Self::with_head(Some(9)).await
    }

    async fn with_head(head: Option<u64>) -> Self {
        let fixture = Self::bare().await;
        let mine = fixture.custody_key.public_key().clone();
        let other = CustodyKey::generate(&mut custody::os_rng());
        fixture
            .append(
                vec![
                    Change {
                        entity_type: "project",
                        entity_id: "prj_1".to_string(),
                        op: "create",
                        payload: json!({ "id": "prj_1", "name": "web" }),
                    },
                    signing_row(SEED, "current"),
                    seal(NODE_ID, &mine, KeyKind::Signing, &key_id(SEED), &SEED),
                    seal(NODE_ID, &mine, KeyKind::Dek, DEK_ID, &[9; 32]),
                    seal(
                        NODE_ID,
                        &mine,
                        KeyKind::Tls,
                        &tls_id(CERT_PEM),
                        &tls_plaintext(CERT_PEM),
                    ),
                    seal(
                        OTHER_NODE_ID,
                        other.public_key(),
                        KeyKind::Dek,
                        OTHER_DEK_ID,
                        &[3; 32],
                    ),
                ],
                head,
            )
            .await;
        if head.is_some() {
            fixture
                .store()
                .await
                .set_meta("log_head_seq_at", TIME)
                .await
                .unwrap();
        }
        fixture.mark(6);
        fixture.set_schema(shipped()).await;
        fixture
    }

    fn node_dir(&self) -> &Path {
        &self.paths.node_data_dir
    }

    async fn store(&self) -> NodeStore {
        NodeStore::open(self.node_dir()).await.unwrap()
    }

    /// Writes the identity: the cluster key of `seed` and the pin of
    /// `cert_pem`.
    fn pin(&self, seed: [u8; 32], cert_pem: &str) {
        let public = signing_public(seed);
        let identity = Identity {
            node_id: NODE_ID.to_string(),
            cluster_id: CLUSTER_ID.to_string(),
            primary_url: "https://192.0.2.10:8443".to_string(),
            primary_urls: Vec::new(),
            fingerprint: format!("sha256:{}", tls_id(cert_pem)),
            cluster_key_id: Identity::key_id_for(&public).unwrap(),
            cluster_public_key: public,
        };
        identity::save_identity(self.node_dir(), &identity).unwrap();
    }

    /// Appends `changes` to the followed log, as the agent stores a batch.
    async fn append(&self, changes: Vec<Change>, head: Option<u64>) {
        let store = self.store().await;
        let (last, _) = store.log_position().await.unwrap();
        let entries: Vec<LogEntry> = changes
            .into_iter()
            .zip(last + 1..)
            .map(|(change, seq)| LogEntry {
                seq,
                term: 1,
                time: TIME.to_string(),
                actor_type: "system".to_string(),
                actor_id: "test".to_string(),
                entity_type: change.entity_type.to_string(),
                entity_id: change.entity_id,
                op: change.op.to_string(),
                payload: change.payload.to_string(),
                prev_hash: hash_of(seq - 1),
                hash: hash_of(seq),
            })
            .collect();
        store.store_log_batch(&entries, head).await.unwrap();
    }

    /// Writes the change-log mark.
    fn mark(&self, seq: u64) {
        fs::write(self.node_dir().join(LOG_MARK_FILE), seq.to_string()).unwrap();
    }

    /// Stores `version` as the newest `welcome.schema_version`.
    async fn set_schema(&self, version: u32) {
        self.store()
            .await
            .set_meta(WELCOME_SCHEMA_VERSION, &version.to_string())
            .await
            .unwrap();
    }

    async fn set_refollowing(&self, state: Refollowing) {
        assert_eq!(
            self.store().await.set_refollowing(state).await.unwrap(),
            FreezeChecked::Written
        );
    }

    /// Runs a promotion with `flags`: its result and the lines it printed.
    async fn promote(&self, flags: Flags) -> (Result<Outcome, Refusal>, Vec<String>) {
        let mut out = Out::quiet();
        let result = run(&Request::Promote(flags), self, &mut out).await;
        (result, out.lines)
    }

    /// The refusal of a plain promotion and of one with `--accept-loss`,
    /// which must be the same.
    async fn refusal(&self) -> Refusal {
        let lifted = self.promote(accept_loss()).await.0.unwrap_err();
        let refusal = self.promote(Flags::default()).await.0.unwrap_err();
        assert_eq!(name_of(&lifted), name_of(&refusal), "{lifted} / {refusal}");
        refusal
    }

    /// Asserts that a plain promotion passes every refusal.
    async fn passes(&self) {
        let (result, lines) = self.promote(Flags::default()).await;
        assert_eq!(result.unwrap(), Outcome::NotBuilt, "{lines:#?}");
    }

    async fn run_abort(&self) -> (Result<Outcome, Refusal>, Vec<String>) {
        let mut out = Out::quiet();
        let result = run(&Request::Abort, self, &mut out).await;
        (result, out.lines)
    }
}

/// `--accept-loss` with its statement.
fn accept_loss() -> Flags {
    Flags {
        accept_loss: true,
        statement: Some("no other candidate holds a newer log".to_string()),
        ..Flags::default()
    }
}

fn at(text: &str) -> SystemTime {
    SystemTime::from(
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).unwrap(),
    )
}

/// The newest schema version this binary knows.
fn shipped() -> u32 {
    u32::try_from(Store::shipped_schema_version()).unwrap()
}

/// A fake entry hash, unique per `seq`: `hash_of(0)` is the zero hash, so
/// the entries chain from `seq` 1.
fn hash_of(seq: u64) -> String {
    format!("{seq:064x}")
}

fn signing_public(seed: [u8; 32]) -> String {
    sign::public_key_to_base64(&sign::signing_key_from_seed(&seed).verifying_key())
}

/// The key id of the signing key of `seed`.
fn key_id(seed: [u8; 32]) -> String {
    Identity::key_id_for(&signing_public(seed)).unwrap()
}

/// The key id of a TLS pair with `cert_pem`: the SHA-256 of its leaf's DER.
fn tls_id(cert_pem: &str) -> String {
    TlsIdentity::from_pem(cert_pem.to_string(), KEY_PEM.to_string())
        .unwrap()
        .key_id()
        .to_string()
}

/// The `tls` seal's plaintext for `cert_pem` (section 7.2).
fn tls_plaintext(cert_pem: &str) -> Vec<u8> {
    json!({ "cert_pem": cert_pem, "key_pem": KEY_PEM })
        .to_string()
        .into_bytes()
}

/// Standard base64 with padding, as a seal's payload carries `sealed`.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let padded = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(padded[0]) << 16) | (u32::from(padded[1]) << 8) | u32::from(padded[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A `node_key_custody` create: `plaintext` sealed to `recipient` for
/// `node_id`, kind and `key_id`, as the primary writes it.
fn seal(
    node_id: &str,
    recipient: &CustodyPublicKey,
    kind: KeyKind,
    key_id: &str,
    plaintext: &[u8],
) -> Change {
    let context = SealContext {
        cluster_id: CLUSTER_ID,
        node_id,
        key_kind: kind,
        key_id,
    };
    let sealed = custody::seal(recipient, &context, plaintext, &mut custody::os_rng()).unwrap();
    Change {
        entity_type: "node_key_custody",
        entity_id: format!("{node_id}/{}/{key_id}", kind.as_str()),
        op: "create",
        payload: json!({
            "node_id": node_id,
            "key_kind": kind.as_str(),
            "key_id": key_id,
            "sealed": base64(&sealed),
            "created_at": TIME,
        }),
    }
}

/// A `node_key_custody` delete.
fn unseal(node_id: &str, kind: KeyKind, key_id: &str) -> Change {
    Change {
        entity_type: "node_key_custody",
        entity_id: format!("{node_id}/{}/{key_id}", kind.as_str()),
        op: "delete",
        payload: json!({}),
    }
}

/// A `signing_keys` create for the key of `seed`.
fn signing_row(seed: [u8; 32], state: &str) -> Change {
    let id = key_id(seed);
    Change {
        entity_type: "signing_key",
        entity_id: id.clone(),
        op: "create",
        payload: json!({
            "key_id": id,
            "public_key": signing_public(seed),
            "state": state,
            "created_at": TIME,
        }),
    }
}

fn name_of(refusal: &Refusal) -> &'static str {
    match refusal {
        Refusal::NotRoot => "root",
        Refusal::Unreadable(_) => "unreadable",
        Refusal::NotANode(_) => "node role",
        Refusal::NoIdentity(_) => "identity",
        Refusal::ControlPlaneDatabase(_) => "control-plane database",
        Refusal::ControlPlaneUnit => "control-plane unit",
        Refusal::LogQuarantined => "quarantined",
        Refusal::LogEmpty => "empty",
        Refusal::Refollowing { .. } => "refollowing",
        Refusal::BelowMark { .. } => "below mark",
        Refusal::RestorePending(_) => "restore-pending",
        Refusal::SchemaTooNew { .. } => "schema",
        Refusal::Keys(_) => "keys",
        Refusal::PromotionRunning => "promotion running",
        Refusal::NoAgentStore(_) => "no agent store",
    }
}

/// Whether `refusal` is the keys refusal with a problem that holds every
/// one of `words`.
fn keys_say(refusal: &Refusal, words: &[&str]) -> bool {
    matches!(refusal, Refusal::Keys(problems)
        if problems.iter().any(|problem| words.iter().all(|word| problem.contains(word))))
}

/// The names in `dir`, sorted, without the agent's SQLite files.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| !name.starts_with("node.db"))
        .collect();
    names.sort();
    names
}

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|a| (*a).to_string()).collect()
}

/// Section 7.3's command line: the flags, `--ack-fence` and
/// `--accept-loss` only with a statement, a statement only with one of
/// them, and `--abort` alone.
#[test]
fn the_command_line_is_checked() {
    assert_eq!(
        parse_args(&args(&[])).unwrap(),
        Request::Promote(Flags::default())
    );
    assert_eq!(
        parse_args(&args(&["--dry-run"])).unwrap(),
        Request::Promote(Flags {
            dry_run: true,
            ..Flags::default()
        })
    );
    assert_eq!(
        parse_args(&args(&[
            "--ack-fence",
            "--accept-loss",
            "--statement",
            "the old host is down",
            "--dry-run",
        ]))
        .unwrap(),
        Request::Promote(Flags {
            dry_run: true,
            ack_fence: true,
            accept_loss: true,
            statement: Some("the old host is down".to_string()),
        })
    );
    assert_eq!(parse_args(&args(&["--abort"])).unwrap(), Request::Abort);

    for (list, says) in [
        (&["--ack-fence"][..], "need --statement"),
        (&["--accept-loss"][..], "need --statement"),
        (&["--ack-fence", "--dry-run"][..], "need --statement"),
        (
            &["--statement", "why"][..],
            "goes with --ack-fence or --accept-loss",
        ),
        (&["--accept-loss", "--statement", "  "][..], "non-empty"),
        (&["--accept-loss", "--statement"][..], "needs a value"),
        (
            &["--accept-loss", "--statement", "a", "--statement", "b"][..],
            "given twice",
        ),
        (
            &["--abort", "--dry-run"][..],
            "--abort takes no other option",
        ),
        (
            &["--dry-run", "--abort"][..],
            "--abort takes no other option",
        ),
        (
            &["--abort", "--accept-loss", "--statement", "x"][..],
            "--abort takes no other option",
        ),
        (&["--force"][..], "unknown option"),
        (&["now"][..], "unexpected argument"),
    ] {
        let error = parse_args(&args(list)).unwrap_err();
        assert!(error.contains(says), "{list:?}: {error}");
    }
}

/// The root check comes first and alone: a run whose effective uid is not
/// 0, or cannot be read, stops before it asks for the paths, systemd or the
/// clock, so it reads no file either, for `--abort` too.
#[tokio::test]
async fn a_run_that_is_not_root_stops_before_it_reads_anything() {
    let fixture = Fixture::new().await;
    for euid in [Some(1000), None] {
        for request in [
            Request::Abort,
            Request::Promote(Flags::default()),
            Request::Promote(Flags {
                dry_run: true,
                ..Flags::default()
            }),
        ] {
            fixture.euid.set(euid);
            fixture.reads.borrow_mut().clear();
            let mut out = Out::quiet();
            let refusal = run(&request, &fixture, &mut out).await.unwrap_err();
            assert!(matches!(refusal, Refusal::NotRoot), "{refusal}");
            assert_eq!(*fixture.reads.borrow(), ["euid"], "{request:?}");
            assert!(out.lines.is_empty());
        }
    }
    fixture.euid.set(Some(0));
    fixture.passes().await;
}

/// Issue #19: the command opens its vote connections as root, so no step of
/// it may run as the data directory's owner. No file of this module names
/// the function that re-executes the command as that owner, nor the module
/// that holds it.
#[test]
fn the_command_never_runs_as_the_data_directory_s_owner() {
    let needles = [
        concat!("run_as_", "data_dir_owner"),
        concat!("privilege", "::"),
    ];
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = vec![src.join("promote.rs")];
    for entry in fs::read_dir(src.join("promote")).unwrap() {
        files.push(entry.unwrap().path());
    }
    for file in &files {
        let text = fs::read_to_string(file).unwrap();
        for needle in needles {
            assert!(!text.contains(needle), "{} names {needle}", file.display());
        }
    }
    for name in ["command.rs", "refusals.rs", "keys.rs"] {
        assert!(
            files.contains(&src.join("promote").join(name)),
            "{name} is read: {files:?}"
        );
    }
}

/// Refusal 1: a machine enrolled as an edge or a witness.
#[tokio::test]
async fn a_machine_without_the_node_role_is_refused() {
    let fixture = Fixture::new().await;
    for role in [Role::Edge, Role::Witness] {
        write_role(fixture.node_dir(), role).unwrap();
        let refusal = fixture.refusal().await;
        assert!(
            matches!(&refusal, Refusal::NotANode(found) if *found == role),
            "{refusal}"
        );
    }
    write_role(fixture.node_dir(), Role::Node).unwrap();
    fixture.passes().await;
}

/// Refusal 1: an agent without an identity, or with one that does not load.
#[tokio::test]
async fn a_machine_whose_agent_has_no_identity_is_refused() {
    let fixture = Fixture::new().await;
    let path = fixture.node_dir().join(IDENTITY_FILE);
    let text = fs::read(&path).unwrap();
    fs::remove_file(&path).unwrap();
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::NoIdentity(_)), "{refusal}");

    fs::write(&path, "{}").unwrap();
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::NoIdentity(_)), "{refusal}");

    fs::write(&path, text).unwrap();
    fixture.passes().await;
}

/// Refusal 2: a control-plane database, then a control-plane unit.
#[tokio::test]
async fn a_control_plane_database_or_unit_on_this_host_is_refused() {
    let fixture = Fixture::new().await;
    let database = fixture.paths.data_dir.join("quoro.db");
    fs::write(&database, b"").unwrap();
    let refusal = fixture.refusal().await;
    assert!(
        matches!(&refusal, Refusal::ControlPlaneDatabase(path) if *path == database),
        "{refusal}"
    );
    fs::remove_file(&database).unwrap();

    fixture.unit.set(true);
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::ControlPlaneUnit), "{refusal}");
    fixture.unit.set(false);
    fixture.passes().await;
}

/// Refusal 3: an empty log, and a host whose agent has no store, where none
/// is created.
#[tokio::test]
async fn an_empty_log_is_refused_and_no_store_is_created() {
    let fixture = Fixture::bare().await;
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::LogEmpty), "{refusal}");

    for name in ["node.db", "node.db-wal", "node.db-shm"] {
        let _ = fs::remove_file(fixture.node_dir().join(name));
    }
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::LogEmpty), "{refusal}");
    assert!(!fixture.node_dir().join("node.db").exists(), "no store");
}

/// Refusal 3: a quarantined log, by the quarantine's mark or by "head
/// unknown", is refused, and `--accept-loss` does not lift it.
#[tokio::test]
async fn a_quarantined_log_is_refused_even_with_accept_loss() {
    let fixture = Fixture::new().await;
    fixture.mark(MAX_SEQ);
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::LogQuarantined), "{refusal}");

    fixture.mark(6);
    fixture.set_refollowing(Refollowing::HeadUnknown).await;
    let refusal = fixture.refusal().await;
    assert!(matches!(&refusal, Refusal::LogQuarantined), "{refusal}");
}

/// Refusal 4: a refollowing agent is refused, and `--accept-loss` lifts it.
#[tokio::test]
async fn a_refollowing_agent_is_refused_unless_accept_loss() {
    let fixture = Fixture::new().await;
    fixture.set_refollowing(Refollowing::Head(12)).await;
    let refusal = fixture.promote(Flags::default()).await.0.unwrap_err();
    assert!(
        matches!(
            &refusal,
            Refusal::Refollowing {
                log_seq: 6,
                head: 12
            }
        ),
        "{refusal}"
    );
    assert_eq!(
        fixture.promote(accept_loss()).await.0.unwrap(),
        Outcome::NotBuilt
    );
}

/// Refusal 4: a log below the mark is refused, and `--accept-loss` lifts it.
#[tokio::test]
async fn a_log_below_the_mark_is_refused_unless_accept_loss() {
    let fixture = Fixture::new().await;
    fixture.mark(8);
    let refusal = fixture.promote(Flags::default()).await.0.unwrap_err();
    assert!(
        matches!(
            &refusal,
            Refusal::BelowMark {
                log_seq: 6,
                mark: 8
            }
        ),
        "{refusal}"
    );
    assert_eq!(
        fixture.promote(accept_loss()).await.0.unwrap(),
        Outcome::NotBuilt
    );
}

/// `--accept-loss` lifts the two log refusals together and nothing after
/// them: every other refusal holds with it ([`Fixture::refusal`] runs each
/// one with and without the flag).
#[tokio::test]
async fn accept_loss_lifts_exactly_the_two_log_refusals() {
    let fixture = Fixture::new().await;
    fixture.set_refollowing(Refollowing::Head(12)).await;
    fixture.mark(12);
    assert_eq!(
        fixture.promote(accept_loss()).await.0.unwrap(),
        Outcome::NotBuilt
    );
    let marker = fixture.paths.data_dir.join(RESTORE_PENDING);
    fs::write(&marker, "").unwrap();
    let refusal = fixture.promote(accept_loss()).await.0.unwrap_err();
    assert!(matches!(&refusal, Refusal::RestorePending(_)), "{refusal}");
    fs::remove_file(&marker).unwrap();
    fixture.set_schema(shipped() + 1).await;
    let refusal = fixture.promote(accept_loss()).await.0.unwrap_err();
    assert!(
        matches!(&refusal, Refusal::SchemaTooNew { .. }),
        "{refusal}"
    );
}

/// Refusal 5: a `restore-pending` marker next to the control-plane
/// database.
#[tokio::test]
async fn a_restore_pending_marker_is_refused() {
    let fixture = Fixture::new().await;
    let marker = fixture.paths.data_dir.join(RESTORE_PENDING);
    fs::write(&marker, "{}").unwrap();
    let refusal = fixture.refusal().await;
    assert!(
        matches!(&refusal, Refusal::RestorePending(path) if *path == marker),
        "{refusal}"
    );
    fs::remove_file(&marker).unwrap();
    fixture.passes().await;
}

/// Refusal 6: a stored `welcome.schema_version` above this binary's newest;
/// this binary's own passes.
#[tokio::test]
async fn a_schema_newer_than_this_binary_is_refused() {
    let fixture = Fixture::new().await;
    fixture.set_schema(shipped() + 1).await;
    let refusal = fixture.refusal().await;
    let newest = u64::from(shipped());
    assert!(
        matches!(&refusal, Refusal::SchemaTooNew { stored, newest: known }
            if *stored == shipped() + 1 && *known == newest),
        "{refusal}"
    );
    fixture.set_schema(shipped()).await;
    fixture.passes().await;
}

/// The refusals come in section 7.3's order: with every one of them
/// holding, each run reports the next, and passes once all are lifted.
#[tokio::test]
async fn the_refusals_come_in_the_contract_s_order() {
    let fixture = Fixture::new().await;
    let dir = fixture.node_dir().to_path_buf();
    let identity_text = fs::read(dir.join(IDENTITY_FILE)).unwrap();
    let custody_bytes = fs::read(dir.join(CUSTODY_KEY_FILE)).unwrap();
    write_role(&dir, Role::Edge).unwrap();
    fs::remove_file(dir.join(IDENTITY_FILE)).unwrap();
    fs::write(fixture.paths.data_dir.join("quoro.db"), b"").unwrap();
    fixture.unit.set(true);
    fixture.set_refollowing(Refollowing::Head(12)).await;
    fixture.mark(MAX_SEQ);
    fs::write(fixture.paths.data_dir.join(RESTORE_PENDING), "").unwrap();
    fixture.set_schema(shipped() + 1).await;
    fs::remove_file(dir.join(CUSTODY_KEY_FILE)).unwrap();

    let mut seen = Vec::new();
    loop {
        let refusal = match fixture.promote(Flags::default()).await.0 {
            Ok(outcome) => {
                assert_eq!(outcome, Outcome::NotBuilt);
                break;
            }
            Err(refusal) => refusal,
        };
        seen.push(name_of(&refusal));
        assert!(seen.len() <= 10, "{seen:?}");
        match refusal {
            Refusal::NotANode(_) => write_role(&dir, Role::Node).unwrap(),
            Refusal::NoIdentity(_) => {
                fs::write(dir.join(IDENTITY_FILE), identity_text.as_slice()).unwrap();
            }
            Refusal::ControlPlaneDatabase(path) | Refusal::RestorePending(path) => {
                fs::remove_file(path).unwrap();
            }
            Refusal::ControlPlaneUnit => fixture.unit.set(false),
            Refusal::LogQuarantined => fixture.mark(8),
            Refusal::Refollowing { .. } => {
                let store = fixture.store().await;
                sqlx::query("DELETE FROM node_meta WHERE key = 'log_refollowing'")
                    .execute(store.pool())
                    .await
                    .unwrap();
            }
            Refusal::BelowMark { .. } => fixture.mark(6),
            Refusal::SchemaTooNew { .. } => fixture.set_schema(shipped()).await,
            Refusal::Keys(_) => {
                fs::write(dir.join(CUSTODY_KEY_FILE), custody_bytes.as_slice()).unwrap();
            }
            other => panic!("unexpected refusal: {other}"),
        }
    }
    assert_eq!(
        seen,
        [
            "node role",
            "identity",
            "control-plane database",
            "control-plane unit",
            "quarantined",
            "refollowing",
            "below mark",
            "restore-pending",
            "schema",
            "keys",
        ]
    );
}

/// Step 1: the last verified `seq`, the head with its time and age, and the
/// difference; `--dry-run` then stops and says the vote pre-check is not
/// built yet, and a plain run stops and says the rest is not built yet.
/// Neither changes anything: no file, no freeze, no lock, the same log.
#[tokio::test]
async fn step_1_shows_the_log_position_and_nothing_changes() {
    let fixture = Fixture::new().await;
    let store = fixture.store().await;
    let position = store.log_position().await.unwrap();
    let node_files = names(fixture.node_dir());

    let (result, lines) = fixture
        .promote(Flags {
            dry_run: true,
            ..Flags::default()
        })
        .await;
    assert_eq!(result.unwrap(), Outcome::DryRun);
    for line in [
        "step 1, the log position:",
        "  last verified seq: 6",
        "  head_seq the primary last reported: 9, received 2026-11-13T10:00:00.000Z, 95 s ago",
        "  difference: 3",
    ] {
        assert!(lines.iter().any(|l| l == line), "{line:?} in {lines:#?}");
    }
    assert!(
        lines.iter().any(|line| line.starts_with("keys in use")
            && line.contains(&key_id(SEED))
            && line.contains(DEK_ID)
            && line.contains(&tls_id(CERT_PEM))
            && !line.contains(OTHER_DEK_ID)),
        "{lines:#?}"
    );
    let last = lines.last().unwrap();
    assert!(last.starts_with("dry run: stopped after step 1"), "{last}");
    assert!(last.contains("vote pre-check is not built yet"), "{last}");

    let (result, lines) = fixture
        .promote(Flags {
            ack_fence: true,
            accept_loss: true,
            statement: Some("the old host is gone".to_string()),
            ..Flags::default()
        })
        .await;
    assert_eq!(result.unwrap(), Outcome::NotBuilt);
    assert!(
        lines.contains(
            &"  with --accept-loss, every entry past seq 6 is lost to the cluster, replicated \
              or not"
                .to_string()
        ),
        "{lines:#?}"
    );
    let last = lines.last().unwrap();
    assert!(last.contains("is not built yet"), "{last}");
    assert!(last.contains("Nothing was changed"), "{last}");
    assert!(last.contains("the old host is gone"), "{last}");

    assert!(names(&fixture.paths.data_dir).is_empty());
    assert_eq!(names(fixture.node_dir()), node_files);
    assert!(!fixture.node_dir().join(freeze::PROMOTE_LOCK_FILE).exists());
    assert_eq!(store.meta(PROMOTION_FREEZE).await.unwrap(), None);
    assert_eq!(store.log_position().await.unwrap(), position);
}

/// Step 1 without a head the primary reported, and with a freeze a stopped
/// promotion left.
#[tokio::test]
async fn step_1_says_when_no_head_is_stored_and_when_the_agent_is_frozen() {
    let fixture = Fixture::with_head(None).await;
    freeze::freeze(fixture.node_dir()).await.unwrap();
    let (result, lines) = fixture.promote(Flags::default()).await;
    assert_eq!(result.unwrap(), Outcome::NotBuilt);
    for line in [
        "  head_seq the primary last reported: none stored",
        "  difference: unknown",
    ] {
        assert!(lines.iter().any(|l| l == line), "{line:?} in {lines:#?}");
    }
    assert!(
        lines
            .iter()
            .any(|line| line.contains("the agent is frozen since") && line.contains("--abort")),
        "{lines:#?}"
    );
}

/// `--abort` ends a freeze, is refused while a promotion holds the lock and
/// leaves the freeze then, finds nothing to end the second time, and on a
/// host whose agent has no store refuses without creating one.
#[tokio::test]
async fn abort_ends_a_freeze_and_is_refused_while_a_promotion_runs() {
    let fixture = Fixture::new().await;
    let dir = fixture.node_dir().to_path_buf();
    let store = fixture.store().await;
    freeze::freeze(&dir).await.unwrap();

    let lock = PromotionLock::acquire(&dir).unwrap();
    let refusal = fixture.run_abort().await.0.unwrap_err();
    assert!(matches!(refusal, Refusal::PromotionRunning), "{refusal}");
    assert!(freeze::is_frozen(&store).await.unwrap(), "the freeze stays");
    drop(lock);

    let (result, lines) = fixture.run_abort().await;
    assert_eq!(result.unwrap(), Outcome::Aborted);
    assert!(!freeze::is_frozen(&store).await.unwrap());
    assert!(
        lines[0].starts_with("ended the promotion freeze set at"),
        "{lines:?}"
    );

    let (result, lines) = fixture.run_abort().await;
    assert_eq!(result.unwrap(), Outcome::Aborted);
    assert_eq!(
        lines,
        ["the agent was not frozen; there was no freeze to end"]
    );

    let bare = Fixture::bare().await;
    for name in ["node.db", "node.db-wal", "node.db-shm"] {
        let _ = fs::remove_file(bare.node_dir().join(name));
    }
    let refusal = bare.run_abort().await.0.unwrap_err();
    assert!(matches!(refusal, Refusal::NoAgentStore(_)), "{refusal}");
    assert!(!bare.node_dir().join("node.db").exists());
}

/// The signing keys in use are the identity's key and every `signing_keys`
/// row: a rotation's next key needs its seal, and an identity that names a
/// key with no seal and no row is refused.
#[tokio::test]
async fn the_signing_keys_in_use_are_the_identity_s_and_every_row() {
    let fixture = Fixture::new().await;
    let mine = fixture.custody_key.public_key().clone();
    let next = [8u8; 32];
    fixture.append(vec![signing_row(next, "next")], None).await;
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &["signing key", &key_id(next), "no seal"]),
        "{refusal}"
    );
    fixture
        .append(
            vec![seal(NODE_ID, &mine, KeyKind::Signing, &key_id(next), &next)],
            None,
        )
        .await;
    fixture.passes().await;

    let rotated = [6u8; 32];
    fixture.pin(rotated, CERT_PEM);
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &["signing key", &key_id(rotated), "no seal"]),
        "{refusal}"
    );
}

/// The TLS key in use is the pinned fingerprint: a pin of another
/// certificate needs that certificate's seal, and the old one's seal no
/// longer counts.
#[tokio::test]
async fn the_tls_key_in_use_is_the_pinned_fingerprint() {
    let fixture = Fixture::new().await;
    let mine = fixture.custody_key.public_key().clone();
    let other = tls_id(OTHER_CERT_PEM);
    fixture.pin(SEED, OTHER_CERT_PEM);
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &["TLS key", &other, "no seal"]),
        "{refusal}"
    );

    fixture
        .append(
            vec![seal(
                NODE_ID,
                &mine,
                KeyKind::Tls,
                &other,
                &tls_plaintext(OTHER_CERT_PEM),
            )],
            None,
        )
        .await;
    let (result, lines) = fixture.promote(Flags::default()).await;
    assert_eq!(result.unwrap(), Outcome::NotBuilt);
    assert!(
        lines.iter().any(|line| line.starts_with("keys in use")
            && line.contains(&other)
            && !line.contains(&tls_id(CERT_PEM))),
        "{lines:#?}"
    );
}

/// The data keys in use are every `dek` seal made for this machine, and
/// there must be exactly one: another machine's data key is not counted
/// (the fixture passes with one), a second seal for this machine refuses,
/// and so does none.
#[tokio::test]
async fn the_data_key_in_use_is_the_one_sealed_to_this_machine() {
    let fixture = Fixture::new().await;
    fixture.passes().await;
    let mine = fixture.custody_key.public_key().clone();
    let second = "0123456789abcdef0123456789abcdef";
    fixture
        .append(
            vec![seal(NODE_ID, &mine, KeyKind::Dek, second, &[4; 32])],
            None,
        )
        .await;
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &["2 data-encryption keys", DEK_ID, second]),
        "{refusal}"
    );

    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Dek, second),
                unseal(NODE_ID, KeyKind::Dek, DEK_ID),
            ],
            None,
        )
        .await;
    let refusal = fixture.refusal().await;
    assert!(keys_say(&refusal, &["no data-encryption key"]), "{refusal}");

    fixture
        .append(
            vec![seal(NODE_ID, &mine, KeyKind::Dek, second, &[4; 32])],
            None,
        )
        .await;
    fixture.passes().await;
}

/// A seal that does not open with this machine's custody key refuses: one
/// sealed to another custody key, and one sealed under another key id but
/// stored as this one's.
#[tokio::test]
async fn a_seal_that_does_not_open_refuses() {
    let fixture = Fixture::new().await;
    let mine = fixture.custody_key.public_key().clone();
    let stranger = CustodyKey::generate(&mut custody::os_rng());
    let tls = tls_id(CERT_PEM);
    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Tls, &tls),
                seal(
                    NODE_ID,
                    stranger.public_key(),
                    KeyKind::Tls,
                    &tls,
                    &tls_plaintext(CERT_PEM),
                ),
            ],
            None,
        )
        .await;
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &["TLS key", &tls, "does not open"]),
        "{refusal}"
    );
    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Tls, &tls),
                seal(NODE_ID, &mine, KeyKind::Tls, &tls, &tls_plaintext(CERT_PEM)),
            ],
            None,
        )
        .await;
    fixture.passes().await;

    let mut moved = seal(NODE_ID, &mine, KeyKind::Dek, OTHER_DEK_ID, &[9; 32]);
    moved.entity_id = format!("{NODE_ID}/dek/{DEK_ID}");
    moved.payload["key_id"] = json!(DEK_ID);
    fixture
        .append(vec![unseal(NODE_ID, KeyKind::Dek, DEK_ID), moved], None)
        .await;
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &["data-encryption key", DEK_ID, "does not open"]),
        "{refusal}"
    );
}

/// A seal that opens to another key than its key id names refuses: a
/// signing seed of another key, a TLS pair of another certificate, and a
/// data key that is not 32 bytes.
#[tokio::test]
async fn a_seal_that_opens_to_another_key_refuses() {
    let fixture = Fixture::new().await;
    let mine = fixture.custody_key.public_key().clone();
    let signing = key_id(SEED);
    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Signing, &signing),
                seal(NODE_ID, &mine, KeyKind::Signing, &signing, &[5; 32]),
            ],
            None,
        )
        .await;
    let refusal = fixture.refusal().await;
    let says = format!("opens to the signing key {}", key_id([5; 32]));
    assert!(
        keys_say(&refusal, &[signing.as_str(), says.as_str()]),
        "{refusal}"
    );
    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Signing, &signing),
                seal(NODE_ID, &mine, KeyKind::Signing, &signing, &SEED),
            ],
            None,
        )
        .await;
    fixture.passes().await;

    let tls = tls_id(CERT_PEM);
    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Tls, &tls),
                seal(
                    NODE_ID,
                    &mine,
                    KeyKind::Tls,
                    &tls,
                    &tls_plaintext(OTHER_CERT_PEM),
                ),
            ],
            None,
        )
        .await;
    let refusal = fixture.refusal().await;
    let says = format!("opens to the certificate {}", tls_id(OTHER_CERT_PEM));
    assert!(
        keys_say(&refusal, &[tls.as_str(), says.as_str()]),
        "{refusal}"
    );
    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Tls, &tls),
                seal(NODE_ID, &mine, KeyKind::Tls, &tls, &tls_plaintext(CERT_PEM)),
            ],
            None,
        )
        .await;
    fixture.passes().await;

    fixture
        .append(
            vec![
                unseal(NODE_ID, KeyKind::Dek, DEK_ID),
                seal(NODE_ID, &mine, KeyKind::Dek, DEK_ID, &[9; 31]),
            ],
            None,
        )
        .await;
    let refusal = fixture.refusal().await;
    assert!(
        keys_say(&refusal, &[DEK_ID, "not a 32-byte key"]),
        "{refusal}"
    );
}

/// Without `custody.key` no seal can be opened: refused, and the command
/// creates no key.
#[tokio::test]
async fn a_missing_custody_key_refuses_and_none_is_created() {
    let fixture = Fixture::new().await;
    let path = fixture.node_dir().join(CUSTODY_KEY_FILE);
    fs::remove_file(&path).unwrap();
    let refusal = fixture.refusal().await;
    assert!(keys_say(&refusal, &["custody key"]), "{refusal}");
    assert!(!path.exists(), "no custody key is created");
}
