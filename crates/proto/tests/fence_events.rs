// SPDX-License-Identifier: AGPL-3.0-only
//! The typed data of `node.fenced` and `node.fence_failed` matches the
//! examples of `events.schema.json` (node-agent-protocol.md section 8.5).

use quoro_proto::{FenceReason, NodeFenceFailed, NodeFenced};
use serde_json::Value;

const SCHEMA: &str = include_str!("../../../docs/contracts/events.schema.json");

/// The `data` of the schema's one example of event type `kind`.
fn example_data(kind: &str) -> Value {
    let schema: Value = serde_json::from_str(SCHEMA).expect("the schema parses");
    let examples = schema["examples"]
        .as_array()
        .expect("the schema has examples");
    let mut found = examples.iter().filter(|example| example["type"] == kind);
    let example = found
        .next()
        .unwrap_or_else(|| panic!("the schema has an example of {kind}"));
    assert!(found.next().is_none(), "one example of {kind}");
    example["data"].clone()
}

/// `data` without the keys the schema does not require, as a node before M5b
/// sent it.
fn without(mut data: Value, keys: &[&str]) -> Value {
    let object = data.as_object_mut().expect("data is an object");
    for key in keys {
        assert!(object.remove(*key).is_some(), "the example carries {key}");
    }
    data
}

#[test]
fn node_fenced_round_trips_the_schema_example() {
    let data = example_data("node.fenced");
    let fenced: NodeFenced = serde_json::from_value(data.clone()).expect("the example parses");

    assert_eq!(fenced.reason, FenceReason::LeaseExpired);
    assert_eq!(fenced.vm_ids.len(), 1);
    assert_eq!(
        fenced.fencing_tokens.keys().collect::<Vec<_>>(),
        fenced.lease_ids.iter().collect::<Vec<_>>(),
        "a token for each fenced lease"
    );
    assert_eq!(
        fenced.fencing_tokens.values().copied().collect::<Vec<_>>(),
        [7]
    );
    assert_eq!(serde_json::to_value(&fenced).expect("serializes"), data);
}

#[test]
fn node_fence_failed_round_trips_the_schema_example() {
    let data = example_data("node.fence_failed");
    let failed: NodeFenceFailed = serde_json::from_value(data.clone()).expect("the example parses");

    assert_eq!(failed.reason, FenceReason::LeaseRevoked);
    assert_eq!(failed.vm_ids.len(), 1);
    assert_eq!(
        failed.fencing_tokens.keys().collect::<Vec<_>>(),
        failed.lease_ids.iter().collect::<Vec<_>>(),
        "a token for each fenced lease"
    );
    assert_eq!(
        failed.fencing_tokens.values().copied().collect::<Vec<_>>(),
        [7]
    );
    assert!(failed.error.is_some());
    assert_eq!(serde_json::to_value(&failed).expect("serializes"), data);
}

#[test]
fn an_event_from_a_node_before_m5b_parses_without_the_new_fields() {
    let fenced: NodeFenced = serde_json::from_value(without(
        example_data("node.fenced"),
        &["fencing_tokens", "vm_ids"],
    ))
    .expect("parses without fencing_tokens and vm_ids");
    assert!(fenced.fencing_tokens.is_empty());
    assert!(fenced.vm_ids.is_empty());

    let failed: NodeFenceFailed = serde_json::from_value(without(
        example_data("node.fence_failed"),
        &["fencing_tokens", "error"],
    ))
    .expect("parses without fencing_tokens and error");
    assert!(failed.fencing_tokens.is_empty());
    assert_eq!(failed.error, None);

    let written = serde_json::to_value(&failed).expect("serializes");
    assert!(
        written.get("error").is_none(),
        "no error is written as none"
    );
}

#[test]
fn a_fence_event_with_a_field_a_later_schema_adds_parses() {
    let mut data = example_data("node.fenced");
    data["a_later_field"] = Value::from(true);
    let fenced: NodeFenced =
        serde_json::from_value(data).expect("additionalProperties: an unknown field parses");
    assert_eq!(fenced.reason, FenceReason::LeaseExpired);
}
