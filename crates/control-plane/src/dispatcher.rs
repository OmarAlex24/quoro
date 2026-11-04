//! The webhook dispatcher (D-0032, D-0140, permission-model.md section 8).
//!
//! Started by [`crate::AppState::open`] next to the grant sweeper. Every second
//! it reads events with `seq` greater than its durable cursor, matches them
//! against the enabled webhooks, records the first delivery attempt and POSTs
//! the raw event envelope signed with the webhook's secret. Failed attempts are
//! retried with exponential backoff (10 s doubling, capped at 1 h, at most 10
//! attempts).
//!
//! Delivery is *at-least-once* across restarts of the primary (D-0140). The
//! cursor — the seq of the last event the dispatcher has dispatched — lives in
//! the local `meta` table under `webhook_cursor` and advances in the same
//! transaction that records the event's first delivery attempts. Start-up
//! resumes after it, so a committed event is never skipped; an attempt recorded
//! but not sent before a stop is requeued. A delivery may repeat after a crash
//! between the send and the commit, so receivers dedupe on the event id (the
//! `Quoro-Event-Id` header). On the very first start the cursor is seeded at
//! the end of the event log, and catch-up is bounded: events older than
//! [`CATCH_UP_WINDOW_SECONDS`] are skipped with one `dispatcher.gap` log line.
//!
//! An `incident.opened` delivery to a webhook with a `wake` object issues the
//! incident grant *before* the delivery, in the same transaction as the
//! delivery row, so a grant can never be issued without its delivery and no
//! retry issues a second grant.

use crate::AppState;
use crate::api::webhooks::DeliveryStatus;
use crate::audit::{self, Channel, Outcome, Record};
use crate::authz::{self, Selector};
use crate::changelog::Actor;
use crate::error::ApiError;
use crate::service::grants::{NewGrant, insert_grant};
use crate::service::webhooks::{WebhookRow, webhook_row_from_row};
use crate::store::{now_rfc3339, rfc3339_in};
use hmac::{Hmac, Mac};
use quoro_types::{Prefix, is_id, new_id};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{Row, SqliteConnection};
use std::time::Duration;
use tokio::time::MissedTickBehavior;

/// How often the dispatcher looks for new events and due retries.
pub const TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Timeout of one delivery POST.
pub const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// At most this many delivery attempts per event.
pub const MAX_ATTEMPTS: i64 = 10;

/// First retry delay, doubled per failed attempt.
pub const RETRY_BASE_SECONDS: i64 = 10;

/// Retry delay cap.
pub const RETRY_MAX_SECONDS: i64 = 3600;

/// How far back the dispatcher catches up after a restart. Events whose `time`
/// is older than this window are skipped, with one `dispatcher.gap` log line
/// per run of consecutive skipped seqs.
pub const CATCH_UP_WINDOW_SECONDS: i64 = 86_400;

/// The `meta` key holding the seq of the last event the dispatcher dispatched.
const WEBHOOK_CURSOR_KEY: &str = "webhook_cursor";

/// One row of the event log, as the dispatcher reads it.
#[derive(Clone, Debug)]
pub(crate) struct EventRow {
    pub id: String,
    pub seq: i64,
    pub kind: String,
    pub time: String,
    pub source: Value,
    pub subject: Value,
    pub subject_type: String,
    pub subject_id: String,
    pub project_id: Option<String>,
    pub incident_id: Option<String>,
    pub request_id: Option<String>,
    pub actor: Option<Value>,
    pub data: Value,
}

/// The envelope sent as the delivery body, identical to `GET /api/v1/events`
/// (events.schema.json).
#[must_use]
pub(crate) fn envelope(event: &EventRow) -> Value {
    let mut out = json!({
        "schema_version": 1,
        "id": event.id,
        "seq": event.seq,
        "type": event.kind,
        "time": event.time,
        "source": event.source,
        "subject": event.subject,
        "data": event.data,
    });
    if let Some(project_id) = &event.project_id {
        out["project_id"] = Value::from(project_id.clone());
    }
    if let Some(incident_id) = &event.incident_id {
        out["incident_id"] = Value::from(incident_id.clone());
    }
    if let Some(request_id) = &event.request_id {
        out["request_id"] = Value::from(request_id.clone());
    }
    if let Some(actor) = &event.actor {
        out["actor"] = actor.clone();
    }
    out
}

/// Builds an [`EventRow`] from a row selected with [`EVENT_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON columns do not parse.
pub(crate) fn event_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<EventRow, ApiError> {
    let source: String = row.try_get("source")?;
    let subject: String = row.try_get("subject")?;
    let actor: Option<String> = row.try_get("actor")?;
    let data: String = row.try_get("data")?;
    let subject: Value = serde_json::from_str(&subject)
        .map_err(|e| ApiError::internal(&format!("event subject: {e}")))?;
    let subject_type = subject["type"].as_str().unwrap_or_default().to_string();
    let subject_id = subject["id"].as_str().unwrap_or_default().to_string();
    Ok(EventRow {
        id: row.try_get("id")?,
        seq: row.try_get("seq")?,
        kind: row.try_get("type")?,
        time: row.try_get("time")?,
        source: serde_json::from_str(&source)
            .map_err(|e| ApiError::internal(&format!("event source: {e}")))?,
        subject,
        subject_type,
        subject_id,
        project_id: row.try_get("project_id")?,
        incident_id: row.try_get("incident_id")?,
        request_id: row.try_get("request_id")?,
        actor: actor
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| ApiError::internal(&format!("event actor: {e}")))
            })
            .transpose()?,
        data: serde_json::from_str(&data)
            .map_err(|e| ApiError::internal(&format!("event data: {e}")))?,
    })
}

/// Every column of [`EventRow`], in the order [`event_from_row`] reads them.
pub(crate) const EVENT_COLUMNS: &str =
    "id, seq, type, time, source, subject, project_id, incident_id, request_id, actor, data";

/// Reads one event by id.
///
/// # Errors
///
/// `not_found` for an unknown id, or a database error.
pub(crate) async fn read_event(
    conn: &mut SqliteConnection,
    event_id: &str,
) -> Result<EventRow, ApiError> {
    let row = sqlx::query(&format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = ?"))
        .bind(event_id)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| ApiError::not_found("event"))?;
    event_from_row(&row)
}

/// The HMAC-SHA256 signature value: `v1=<hex of HMAC(secret, "<t>.<body>")>`.
#[must_use]
pub(crate) fn signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(format!("{timestamp}.").as_bytes());
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let mut out = String::with_capacity(3 + digest.len() * 2);
    out.push_str("v1=");
    for byte in digest {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

/// The incident grant a wake delivery carries in `data.wake` (section 8.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WakeRef {
    pub agent_id: String,
    pub grant_id: String,
}

/// The delivery body: the event envelope, plus `data.wake` for a wake delivery.
///
/// Only a copy is extended; [`envelope`] stays untouched so the event log and
/// the SSE stream never carry `wake`. The extra field is inserted before
/// serialization, so `Quoro-Signature` covers it like the rest of the body.
#[must_use]
fn delivery_body(event: &EventRow, wake: Option<&WakeRef>) -> Value {
    let mut body = envelope(event);
    if let Some(wake) = wake
        && let Value::Object(data) = &mut body["data"]
    {
        data.insert(
            "wake".to_string(),
            json!({ "agent_id": wake.agent_id, "grant_id": wake.grant_id }),
        );
    }
    body
}

/// Outcome of one POST.
#[derive(Clone, Debug, Default)]
pub(crate) struct SendOutcome {
    pub response_code: Option<i64>,
    pub error: Option<String>,
}

impl SendOutcome {
    /// Whether the receiver answered 2xx.
    #[must_use]
    pub fn delivered(&self) -> bool {
        self.response_code
            .is_some_and(|code| (200..300).contains(&code))
    }
}

/// POSTs the signed envelope to the webhook receiver. A test delivery passes
/// no `wake`, so it never carries `data.wake`.
pub(crate) async fn send_once(
    client: &reqwest::Client,
    webhook: &WebhookRow,
    event: &EventRow,
    delivery_id: &str,
    secret: &str,
    wake: Option<&WakeRef>,
) -> SendOutcome {
    let body = match serde_json::to_vec(&delivery_body(event, wake)) {
        Ok(body) => body,
        Err(error) => {
            return SendOutcome {
                response_code: None,
                error: Some(format!("cannot serialize the event: {error}")),
            };
        }
    };
    let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
    let signed = signature(secret, timestamp, &body);
    let signature_header = format!("t={timestamp},{signed}");
    let result = client
        .post(&webhook.url)
        .header("Quoro-Event-Id", &event.id)
        .header("Quoro-Event-Type", &event.kind)
        .header("Quoro-Delivery-Id", delivery_id)
        .header("Quoro-Signature", signature_header)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await;
    match result {
        Ok(response) => {
            let code = i64::from(response.status().as_u16());
            if (200..300).contains(&code) {
                let _ = response.bytes().await;
                SendOutcome {
                    response_code: Some(code),
                    error: None,
                }
            } else {
                SendOutcome {
                    response_code: Some(code),
                    error: Some(format!("receiver answered {code}")),
                }
            }
        }
        Err(error) => SendOutcome {
            response_code: None,
            error: Some(format!("delivery failed: {error}")),
        },
    }
}

/// Starts the dispatcher before `AppState::open` returns: resumes the durable
/// cursor, requeues first attempts a previous process recorded but never sent,
/// and spawns the tick loop.
///
/// # Errors
///
/// A message for the operator when the cursor cannot be read or seeded, or the
/// orphaned attempts cannot be requeued.
pub async fn start(state: AppState) -> Result<(), String> {
    let (cursor, seeded) = resume_cursor(&state)
        .await
        .map_err(|e| format!("cannot resume the webhook dispatcher: {e:?}"))?;
    requeue_orphaned_attempts(&state)
        .await
        .map_err(|e| format!("cannot requeue orphaned webhook deliveries: {e:?}"))?;
    if seeded {
        tracing::info!(
            "webhook dispatcher resumes after seq {cursor} (first start, seeded at the end of the event log)"
        );
    } else {
        tracing::info!("webhook dispatcher resumes after seq {cursor}");
    }
    spawn(state, cursor);
    Ok(())
}

/// Spawns the tick loop at `cursor`. Errors inside a tick are logged, never
/// propagated: a failed tick is retried on the next one.
fn spawn(state: AppState, cursor: i64) {
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(DELIVERY_TIMEOUT)
            // A receiver must not move a signed delivery to another address.
            .redirect(reqwest::redirect::Policy::none())
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                tracing::error!(error = ?error, "webhook dispatcher has no HTTP client");
                return;
            }
        };
        let mut cursor = cursor;
        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(error) = tick(&state, &client, &mut cursor).await {
                tracing::warn!(error = ?error, "webhook dispatch failed");
            }
        }
    });
}

/// Reads the durable cursor, seeding and persisting it on the very first start.
///
/// The seed is the current `MAX(seq)` (no backlog of history) and it is written
/// immediately, so a second restart before any event does not re-seed at a
/// newer seq and skip an event committed in between.
///
/// Returns the cursor and whether it was seeded.
async fn resume_cursor(state: &AppState) -> Result<(i64, bool), ApiError> {
    let stored: Option<(String,)> = sqlx::query_as("SELECT value FROM meta WHERE key = ?")
        .bind(WEBHOOK_CURSOR_KEY)
        .fetch_optional(state.store.pool())
        .await?;
    if let Some((value,)) = stored {
        let cursor = value
            .parse::<i64>()
            .map_err(|_| ApiError::internal(&format!("webhook cursor {value:?} is not a seq")))?;
        return Ok((cursor, false));
    }
    let cursor = max_seq(state).await?;
    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(WEBHOOK_CURSOR_KEY)
    .bind(cursor.to_string())
    .execute(&mut *w.tx)
    .await?;
    w.commit().await?;
    Ok((cursor, true))
}

/// Requeues first attempts that were recorded before a stop but never sent.
///
/// A real attempt-1 row is `pending` with `next_attempt_at IS NULL`; the retry
/// loop ignores it in that state, so start-up gives it a due time. Test
/// deliveries are synchronous and never retried, so `test = 0` leaves them
/// alone.
async fn requeue_orphaned_attempts(state: &AppState) -> Result<(), ApiError> {
    let now = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    let result = sqlx::query(
        "UPDATE webhook_deliveries SET next_attempt_at = ? \
         WHERE status = 'pending' AND next_attempt_at IS NULL AND test = 0",
    )
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    w.commit().await?;
    if result.rows_affected() > 0 {
        tracing::info!(
            count = result.rows_affected(),
            "webhook dispatcher requeued orphaned first attempts"
        );
    }
    Ok(())
}

async fn max_seq(state: &AppState) -> Result<i64, ApiError> {
    let (seq,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM events")
        .fetch_one(state.store.pool())
        .await?;
    Ok(seq)
}

async fn tick(
    state: &AppState,
    client: &reqwest::Client,
    cursor: &mut i64,
) -> Result<(), ApiError> {
    deliver_due_retries(state, client).await?;
    // Fixed-width RFC 3339 UTC timestamps compare correctly as strings.
    let cutoff = rfc3339_in(-CATCH_UP_WINDOW_SECONDS);
    loop {
        let rows = sqlx::query(&format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE seq > ? ORDER BY seq LIMIT 100"
        ))
        .bind(*cursor)
        .fetch_all(state.store.pool())
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            events.push(event_from_row(row)?);
        }
        let count = events.len();
        // Whether a processed seq was not yet written durably: events with no
        // matching webhook and skipped events do not open a transaction each.
        let mut dirty = false;
        // A run of consecutive skipped events, coalesced into one log line.
        let mut gap: Option<(i64, i64, u64)> = None;
        for event in &events {
            if event.time.as_str() < cutoff.as_str() {
                gap = Some(match gap {
                    Some((first, _, count)) => (first, event.seq, count + 1),
                    None => (event.seq, event.seq, 1),
                });
                *cursor = event.seq;
                dirty = true;
                continue;
            }
            if let Some((first, last, skipped)) = gap.take() {
                log_gap(first, last, skipped);
            }
            match dispatch_event(state, client, event).await {
                // A database failure stops the tick before this event: the
                // cursor stays behind it and the next tick retries it.
                Err(error) => {
                    tracing::warn!(
                        event_id = %event.id,
                        error = ?error,
                        "webhook event dispatch failed; retrying from this event"
                    );
                    return Ok(());
                }
                Ok(persisted) => {
                    *cursor = event.seq;
                    dirty |= !persisted;
                }
            }
        }
        if let Some((first, last, skipped)) = gap.take() {
            log_gap(first, last, skipped);
        }
        if dirty {
            persist_cursor(state, *cursor).await?;
        }
        if count < 100 {
            return Ok(());
        }
    }
}

fn log_gap(first: i64, last: i64, count: u64) {
    tracing::warn!(
        first_seq = first,
        last_seq = last,
        count,
        "dispatcher.gap: skipped {count} event(s) older than the {CATCH_UP_WINDOW_SECONDS}s catch-up window (seq {first}..{last})"
    );
}

/// Writes the durable cursor without moving it backwards.
async fn persist_cursor(state: &AppState, cursor: i64) -> Result<(), ApiError> {
    let mut w = state.store.begin_write().await?;
    upsert_cursor(&mut w.tx, cursor).await?;
    w.commit().await?;
    Ok(())
}

/// Upserts the durable cursor. The `WHERE` clause refuses a lower value, so a
/// coalesced end-of-page write never moves the cursor backwards.
async fn upsert_cursor(conn: &mut SqliteConnection, seq: i64) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value \
         WHERE CAST(excluded.value AS INTEGER) > CAST(meta.value AS INTEGER)",
    )
    .bind(WEBHOOK_CURSOR_KEY)
    .bind(seq.to_string())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// One first attempt recorded but not yet sent.
struct PreparedDelivery {
    webhook: WebhookRow,
    delivery_id: String,
    wake: Option<WakeRef>,
}

/// Matches one event against the enabled webhooks and delivers it.
///
/// Returns `true` when the transaction with the cursor committed, and `false`
/// when the event was skipped here (no match, or its subject could not be
/// resolved): the caller then persists the cursor with the rest of the page.
/// `Err` means the database write failed and the event must be retried.
async fn dispatch_event(
    state: &AppState,
    client: &reqwest::Client,
    event: &EventRow,
) -> Result<bool, ApiError> {
    let mut conn = state.store.pool().acquire().await?;
    // Resolving a subject or reading the webhook list failing for one event
    // must not block the event log forever: log it and advance past the event.
    let resolved = match authz::resolve(&mut conn, &event.subject_type, &event.subject_id, 0).await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            drop(conn);
            tracing::warn!(
                event_id = %event.id,
                error = ?error,
                "webhook subject cannot be resolved; event skipped"
            );
            return Ok(false);
        }
    };
    let rows = match sqlx::query("SELECT * FROM webhooks WHERE enabled = 1 ORDER BY id")
        .fetch_all(&mut *conn)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            drop(conn);
            tracing::warn!(
                event_id = %event.id,
                error = ?error,
                "webhook list cannot be read; event skipped"
            );
            return Ok(false);
        }
    };
    drop(conn);

    let mut matched = Vec::with_capacity(rows.len());
    for row in &rows {
        let webhook = match webhook_row_from_row(row) {
            Ok(webhook) => webhook,
            Err(error) => {
                tracing::warn!(
                    event_id = %event.id,
                    error = ?error,
                    "webhook row is unreadable; delivery skipped"
                );
                continue;
            }
        };
        if !webhook.event_types.is_empty()
            && !webhook
                .event_types
                .iter()
                .any(|kind| kind.as_str() == event.kind)
        {
            continue;
        }
        if !webhook.selector.matches(&resolved) {
            continue;
        }
        matched.push(webhook);
    }
    if matched.is_empty() {
        return Ok(false);
    }

    // One transaction records the first attempt of every matching webhook, with
    // its wake grant, and the cursor. Either the event is fully scheduled and
    // the cursor moves past it, or nothing changed and the next tick retries.
    let now = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    let mut prepared = Vec::with_capacity(matched.len());
    for webhook in matched {
        // Each webhook gets its own savepoint: one that cannot be scheduled
        // (its wake grant fails, say) is rolled back and logged, and must not
        // hold the event, and with it the whole log, back for the others.
        let mut savepoint = sqlx::Connection::begin(&mut *w.tx).await?;
        match schedule_first_attempt(&mut savepoint, &webhook, event, &now).await {
            Ok((delivery_id, wake)) => {
                savepoint.commit().await?;
                prepared.push(PreparedDelivery {
                    webhook,
                    delivery_id,
                    wake,
                });
            }
            Err(error) => {
                savepoint.rollback().await?;
                tracing::warn!(
                    webhook_id = %webhook.id,
                    event_id = %event.id,
                    error = ?error,
                    "webhook delivery cannot be scheduled; skipped for this event"
                );
            }
        }
    }
    upsert_cursor(&mut w.tx, event.seq).await?;
    w.commit().await?;

    for delivery in prepared {
        // One webhook's failure must not stop the event's remaining deliveries.
        if let Err(error) = send_prepared(state, client, event, delivery).await {
            tracing::warn!(
                event_id = %event.id,
                error = ?error,
                "webhook delivery failed"
            );
        }
    }
    Ok(true)
}

/// Records the first attempt of one matched webhook and, for a wake webhook and
/// an `incident.opened` event, issues the incident grant before the delivery,
/// in the caller's transaction.
async fn schedule_first_attempt(
    conn: &mut SqliteConnection,
    webhook: &WebhookRow,
    event: &EventRow,
    now: &str,
) -> Result<(String, Option<WakeRef>), ApiError> {
    let delivery_id = new_id(Prefix::WebhookDelivery);
    sqlx::query(
        "INSERT INTO webhook_deliveries (id, webhook_id, event_id, attempt, status, response_code, error, next_attempt_at, created_at, test) \
         VALUES (?, ?, ?, 1, 'pending', NULL, NULL, NULL, ?, 0)",
    )
    .bind(&delivery_id)
    .bind(&webhook.id)
    .bind(&event.id)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    let wake = match (event.kind.as_str(), &webhook.wake) {
        ("incident.opened", Some(configured)) => {
            issue_wake(conn, webhook, event)
                .await?
                .map(|grant_id| WakeRef {
                    agent_id: configured.agent_id.clone(),
                    grant_id,
                })
        }
        _ => None,
    };
    Ok((delivery_id, wake))
}

/// Decrypts the secret and sends one already-recorded first attempt.
async fn send_prepared(
    state: &AppState,
    client: &reqwest::Client,
    event: &EventRow,
    delivery: PreparedDelivery,
) -> Result<(), ApiError> {
    let PreparedDelivery {
        webhook,
        delivery_id,
        wake,
    } = delivery;
    let secret = match decrypt_secret(state, &webhook) {
        Ok(secret) => secret,
        Err(error) => {
            tracing::warn!(webhook_id = %webhook.id, error = ?error, "webhook secret unavailable");
            let outcome = SendOutcome {
                response_code: None,
                error: Some("the webhook secret cannot be decrypted".to_string()),
            };
            record_outcome(state, &webhook.id, event, &delivery_id, 1, &outcome).await?;
            return Ok(());
        }
    };
    let outcome = send_once(
        client,
        &webhook,
        event,
        &delivery_id,
        &secret,
        wake.as_ref(),
    )
    .await;
    record_outcome(state, &webhook.id, event, &delivery_id, 1, &outcome).await
}

/// Processes pending retry rows whose `next_attempt_at` has come.
async fn deliver_due_retries(state: &AppState, client: &reqwest::Client) -> Result<(), ApiError> {
    let now = now_rfc3339();
    let rows = sqlx::query(
        "SELECT id, webhook_id, event_id, attempt FROM webhook_deliveries \
         WHERE status = 'pending' AND next_attempt_at IS NOT NULL AND next_attempt_at <= ? \
         ORDER BY next_attempt_at LIMIT 100",
    )
    .bind(&now)
    .fetch_all(state.store.pool())
    .await?;
    for row in &rows {
        // One failing row must not abort the rest of the batch.
        if let Err(error) = deliver_retry(state, client, row).await {
            let id = row.try_get::<String, _>("id").unwrap_or_default();
            tracing::warn!(delivery_id = %id, error = ?error, "webhook retry failed");
        }
    }
    Ok(())
}

/// Processes one due retry row.
async fn deliver_retry(
    state: &AppState,
    client: &reqwest::Client,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(), ApiError> {
    let id: String = row.try_get("id")?;
    let webhook_id: String = row.try_get("webhook_id")?;
    let event_id: String = row.try_get("event_id")?;
    let attempt: i64 = row.try_get("attempt")?;
    let mut conn = state.store.pool().acquire().await?;
    let webhook_row = sqlx::query("SELECT * FROM webhooks WHERE id = ?")
        .bind(&webhook_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(webhook_row) = webhook_row else {
        drop(conn);
        // The webhook was deleted; its deliveries cascade.
        return Ok(());
    };
    let webhook = webhook_row_from_row(&webhook_row)?;
    if !webhook.enabled {
        let outcome = SendOutcome {
            response_code: None,
            error: Some("webhook is disabled".to_string()),
        };
        drop(conn);
        // A disabled webhook does not retry: record the attempt as the last.
        record_outcome(
            state,
            &webhook.id,
            &dummy_event(&event_id),
            &id,
            MAX_ATTEMPTS,
            &outcome,
        )
        .await?;
        return Ok(());
    }
    let event = match read_event(&mut conn, &event_id).await {
        Ok(event) => event,
        Err(error) => {
            drop(conn);
            tracing::warn!(event_id = %event_id, error = ?error, "retry event is gone");
            return Ok(());
        }
    };
    // A wake retry carries the grant attempt 1 issued, recorded in the audit
    // log by `issue_wake` in the same transaction as that attempt.
    let wake = retry_wake(&mut conn, &webhook, &event).await?;
    drop(conn);
    let secret = match decrypt_secret(state, &webhook) {
        Ok(secret) => secret,
        Err(error) => {
            tracing::warn!(webhook_id = %webhook.id, error = ?error, "webhook secret unavailable");
            let outcome = SendOutcome {
                response_code: None,
                error: Some("the webhook secret cannot be decrypted".to_string()),
            };
            record_outcome(state, &webhook.id, &event, &id, attempt, &outcome).await?;
            return Ok(());
        }
    };
    let outcome = send_once(client, &webhook, &event, &id, &secret, wake.as_ref()).await;
    record_outcome(state, &webhook.id, &event, &id, attempt, &outcome).await
}

/// Recovers the wake grant of a retried delivery from the audit record
/// `issue_wake` wrote for it. Only an `incident.opened` event delivered to a
/// webhook with `wake` ever has one; every other retry sends no `data.wake`.
async fn retry_wake(
    conn: &mut SqliteConnection,
    webhook: &WebhookRow,
    event: &EventRow,
) -> Result<Option<WakeRef>, ApiError> {
    if event.kind != "incident.opened" {
        return Ok(None);
    }
    if webhook.wake.is_none() {
        return Ok(None);
    }
    // The agent comes from the issued grant's record, not from the webhook's
    // current `wake`, which a PATCH may have pointed at another agent since.
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT grant_id, json_extract(params, '$.agent_id') FROM audit_log \
         WHERE operation = 'grant.issue' \
         AND json_extract(params, '$.webhook_id') = ? \
         AND json_extract(params, '$.event_id') = ? \
         AND grant_id IS NOT NULL \
         AND json_extract(params, '$.agent_id') IS NOT NULL \
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(&webhook.id)
    .bind(&event.id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(grant_id, agent_id)| WakeRef { agent_id, grant_id }))
}

/// A placeholder only used when a retry row has no readable event; scheduling
/// still needs the event id.
fn dummy_event(event_id: &str) -> EventRow {
    EventRow {
        id: event_id.to_string(),
        seq: 0,
        kind: String::new(),
        time: String::new(),
        source: Value::Null,
        subject: Value::Null,
        subject_type: String::new(),
        subject_id: String::new(),
        project_id: None,
        incident_id: None,
        request_id: None,
        actor: None,
        data: Value::Null,
    }
}

/// Records the result of one attempt and, for a failed attempt below the cap,
/// queues the next attempt as its own `pending` row with its `next_attempt_at`.
async fn record_outcome(
    state: &AppState,
    webhook_id: &str,
    event: &EventRow,
    delivery_id: &str,
    attempt: i64,
    outcome: &SendOutcome,
) -> Result<(), ApiError> {
    let delivered = outcome.delivered();
    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "UPDATE webhook_deliveries SET status = ?, response_code = ?, error = ? WHERE id = ?",
    )
    .bind(if delivered {
        DeliveryStatus::Delivered.as_str()
    } else {
        DeliveryStatus::Failed.as_str()
    })
    .bind(outcome.response_code)
    .bind(outcome.error.as_deref())
    .bind(delivery_id)
    .execute(&mut *w.tx)
    .await?;
    if !delivered && attempt < MAX_ATTEMPTS {
        let next = new_id(Prefix::WebhookDelivery);
        let due = rfc3339_in(retry_delay(attempt));
        sqlx::query(
            "INSERT INTO webhook_deliveries (id, webhook_id, event_id, attempt, status, response_code, error, next_attempt_at, created_at, test) \
             VALUES (?, ?, ?, ?, 'pending', NULL, NULL, ?, ?, 0)",
        )
        .bind(&next)
        .bind(webhook_id)
        .bind(&event.id)
        .bind(attempt + 1)
        .bind(&due)
        .bind(now_rfc3339())
        .execute(&mut *w.tx)
        .await?;
    }
    w.commit().await?;
    Ok(())
}

/// Delay before the attempt after failed attempt `attempt`: 10 s doubling,
/// capped at one hour.
#[must_use]
pub(crate) fn retry_delay(attempt: i64) -> i64 {
    let exponent = u32::try_from(attempt.max(1) - 1).unwrap_or(u32::MAX);
    RETRY_BASE_SECONDS
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(RETRY_MAX_SECONDS)
}

fn decrypt_secret(state: &AppState, webhook: &WebhookRow) -> Result<String, ApiError> {
    let secret = state
        .data_key
        .decrypt(
            crate::service::webhooks::SECRET_CONTEXT,
            &webhook.id,
            &webhook.secret_enc,
        )
        .map_err(|error| ApiError::internal(&format!("webhook secret: {error}")))?;
    Ok(secret.expose().to_string())
}

/// Issues the incident grant a wake webhook asks for (section 8.2). Runs in the
/// caller's transaction, before the delivery is sent.
async fn issue_wake(
    conn: &mut SqliteConnection,
    webhook: &WebhookRow,
    event: &EventRow,
) -> Result<Option<String>, ApiError> {
    let Some(wake) = &webhook.wake else {
        return Ok(None);
    };
    let Some(incident_id) = event
        .incident_id
        .clone()
        .or_else(|| event.data["incident_id"].as_str().map(str::to_string))
    else {
        return Ok(None);
    };
    // Only an open incident wakes an agent; one resolved before its delivery
    // left would hand out authority nobody needs any more.
    let incident: Option<(String, String)> = sqlx::query_as(
        "SELECT subject, trigger_event_id FROM incidents WHERE id = ? AND status = 'open'",
    )
    .bind(&incident_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((subject, trigger_event_id)) = incident else {
        return Ok(None);
    };
    let subject: Value = serde_json::from_str(&subject)
        .map_err(|e| ApiError::internal(&format!("incident subject: {e}")))?;
    let trigger_event_id = Some(trigger_event_id);
    let Some(selector) = incident_selector(conn, &subject).await? else {
        return Ok(None);
    };
    let grant = NewGrant {
        agent_id: wake.agent_id.clone(),
        kind: "incident",
        scopes: wake.grant_scopes.clone(),
        selector,
        expires_at: rfc3339_in(wake.grant_ttl_s),
        max_uses: wake.grant_max_uses,
        incident_id: Some(incident_id.clone()),
        approval_id: None,
        on_behalf_of: webhook.created_by.clone(),
        created_by_type: "system",
        created_by_id: "webhooks".to_string(),
        reason: format!(
            "incident {incident_id} opened: webhook {} ({}) wakes agent {}",
            webhook.id, webhook.name, wake.agent_id
        ),
    };
    let request_id = event
        .request_id
        .clone()
        .unwrap_or_else(|| new_id(Prefix::Request));
    let actor = Actor::system("webhooks");
    let (grant, change_seq) =
        insert_grant(conn, &actor, event.request_id.as_deref(), grant).await?;
    let record = Record {
        actor: actor.clone(),
        agent_model: None,
        on_behalf_of: Some(webhook.created_by.clone()),
        channel: Channel::System,
        operation: "grant.issue".to_string(),
        scope: None,
        target_type: Some("grant".to_string()),
        target_id: Some(grant.id.clone()),
        project_id: None,
        params: json!({
            "webhook_id": webhook.id,
            "event_id": event.id,
            "incident_id": incident_id,
            "agent_id": wake.agent_id,
            "scopes": wake.grant_scopes,
            "selector": grant.selector,
            "expires_at": grant.expires_at,
            "max_uses": grant.max_uses,
        }),
        justification: None,
        trigger_event_id,
        incident_id: Some(incident_id),
        grant_id: Some(grant.id.clone()),
        approval_id: None,
        request_id,
        result: Outcome::Ok,
        result_detail: None,
        change_seq: Some(change_seq),
    };
    audit::append(conn, &record).await?;
    Ok(Some(grant.id))
}

/// The selector for the incident subject only: an app id, a node id or a
/// project slug (section 8.2).
async fn incident_selector(
    conn: &mut SqliteConnection,
    subject: &Value,
) -> Result<Option<Selector>, ApiError> {
    let Some(kind) = subject["type"].as_str() else {
        return Ok(None);
    };
    let Some(id) = subject["id"].as_str() else {
        return Ok(None);
    };
    let selector = match kind {
        "app" => Selector {
            apps: Some(vec![id.to_string()]),
            ..Selector::default()
        },
        "node" => Selector {
            nodes: Some(vec![id.to_string()]),
            ..Selector::default()
        },
        "project" => {
            let slug = if is_id(Prefix::Project, id) {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT slug FROM projects WHERE id = ?")
                        .bind(id)
                        .fetch_optional(&mut *conn)
                        .await?;
                row.map(|row| row.0)
            } else {
                Some(id.to_string())
            };
            let Some(slug) = slug else {
                return Ok(None);
            };
            Selector {
                projects: Some(vec![slug]),
                ..Selector::default()
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(selector))
}
