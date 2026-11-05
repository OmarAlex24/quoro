// SPDX-License-Identifier: AGPL-3.0-only
//! The primary status after `quoro restore` onto another host
//! (`backup-format.md` section 8.2, "The primary status moves with the
//! restore").
//!
//! `quoro restore` names the node on the new host in `<data_dir>/primary-node`.
//! At the next start, before anything serves, [`apply_marker`] moves
//! `nodes.cp_status = 'primary'` to that node in one write transaction, through
//! the change log, and records `cluster.primary_moved`
//! (`audit-log-format.md` section 3.5). The file is deleted once it was read.

use std::path::Path;

use serde_json::{Value, json};

use crate::audit::{Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::store::Store;

/// The file `quoro restore` writes into the data directory: one `nod_` id.
pub const MARKER_FILE: &str = "primary-node";

/// The system actor of the move (`audit-log-format.md` section 3.5).
const ACTOR: &str = "cluster";

/// Reads `<data_dir>/primary-node`, moves the primary status to the node it
/// names when it can, and deletes the file.
///
/// A missing file does nothing. A file that cannot be read, for example one
/// left owned by root, is logged and kept, so a start after its owner is fixed
/// still moves the status. A file that names a node the database does not
/// hold, holds as `removed`, or that already holds the status, is logged and
/// deleted. None of these stops the start.
///
/// # Errors
///
/// A database error while reading the nodes or inside the move's transaction.
/// The file is then left in place, so the next start tries again.
pub async fn apply_marker(store: &Store, data_dir: &Path) -> Result<(), String> {
    let path = data_dir.join(MARKER_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "cannot read the primary-node file left by quoro restore; it stays for the next start (give it the database file's owner)"
            );
            return Ok(());
        }
    };
    let node_id = text.trim();
    match move_primary(store, node_id).await? {
        Moved::Yes { from } => tracing::info!(
            from_node_id = from.as_deref().unwrap_or("none"),
            to_node_id = node_id,
            "moved the primary status to the node named by quoro restore"
        ),
        Moved::No(reason) => tracing::warn!(
            node_id = node_id,
            reason = reason,
            "the primary-node file left by quoro restore was not used; the primary status stays where it is"
        ),
    }
    remove_marker(&path);
    Ok(())
}

enum Moved {
    Yes { from: Option<String> },
    No(&'static str),
}

/// The move itself: one write transaction with the two node updates, their
/// change-log entries and the audit record.
async fn move_primary(store: &Store, node_id: &str) -> Result<Moved, String> {
    if !quoro_types::is_id(quoro_types::Prefix::Node, node_id) {
        return Ok(Moved::No("the file does not hold a nod_ id"));
    }
    let mut w = store
        .begin_write()
        .await
        .map_err(|error| format!("cannot move the primary status: {error}"))?;
    let status: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT status, cp_status FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *w.tx)
            .await
            .map_err(|error| format!("cannot read the node named by primary-node: {error}"))?;
    match status {
        None => return Ok(Moved::No("the database holds no such node")),
        Some((status, _)) if status == "removed" => {
            return Ok(Moved::No("the node is removed"));
        }
        Some((_, Some(cp_status))) if cp_status == "primary" => {
            return Ok(Moved::No("the node already holds the primary status"));
        }
        Some(_) => {}
    }

    let actor = Actor::system(ACTOR);
    let holders: Vec<(String,)> =
        sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary' ORDER BY id")
            .fetch_all(&mut *w.tx)
            .await
            .map_err(|error| format!("cannot read the primary node: {error}"))?;
    for (holder,) in &holders {
        sqlx::query("UPDATE nodes SET cp_status = NULL WHERE id = ?")
            .bind(holder)
            .execute(&mut *w.tx)
            .await
            .map_err(|error| format!("cannot clear the primary status: {error}"))?;
        let payload = crate::driver::node_payload(&mut w.tx, holder)
            .await
            .map_err(|error| format!("cannot read node {holder}: {error:?}"))?;
        changelog::append(&mut w.tx, &actor, "node", holder, Op::Update, payload)
            .await
            .map_err(|error| format!("cannot log the primary status move: {error}"))?;
    }

    sqlx::query("UPDATE nodes SET cp_status = 'primary' WHERE id = ?")
        .bind(node_id)
        .execute(&mut *w.tx)
        .await
        .map_err(|error| format!("cannot set the primary status: {error}"))?;
    let payload = crate::driver::node_payload(&mut w.tx, node_id)
        .await
        .map_err(|error| format!("cannot read node {node_id}: {error:?}"))?;
    let seq = changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload)
        .await
        .map_err(|error| format!("cannot log the primary status move: {error}"))?;

    // The cluster has one primary (D-0016); `from_node_id` is null when no node
    // held the status.
    let from = holders.into_iter().next().map(|(id,)| id);
    let record = Record {
        actor,
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: "cluster.primary_moved".to_string(),
        scope: None,
        target_type: Some("node".to_string()),
        target_id: Some(node_id.to_string()),
        project_id: None,
        params: json!({
            "from_node_id": from.as_deref().map_or(Value::Null, Value::from),
            "to_node_id": node_id,
        }),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: quoro_types::new_id(quoro_types::Prefix::Request),
        result: Outcome::Ok,
        result_detail: None,
        change_seq: Some(seq),
    };
    crate::audit::append(&mut w.tx, &record)
        .await
        .map_err(|error| format!("cannot record the primary status move: {error}"))?;
    w.commit()
        .await
        .map_err(|error| format!("cannot move the primary status: {error}"))?;
    Ok(Moved::Yes { from })
}

fn remove_marker(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        tracing::warn!(
            path = %path.display(),
            error = %error,
            "cannot delete the primary-node file left by quoro restore"
        );
    }
}
