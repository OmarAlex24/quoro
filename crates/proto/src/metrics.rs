// SPDX-License-Identifier: AGPL-3.0-only
//! Metric samples: the `metrics` object of a `metrics_query` answer and of a
//! `metrics` stream's `stream_data` (`node-agent-protocol.md` section 5.17).
//!
//! The node samples every `interval_s` into an in-memory ring of 10 minutes
//! per node and per running VM (ADR 0014). Nothing here is stored or
//! replicated by the control plane.

use serde::{Deserialize, Serialize};

/// The `metrics` object (section 5.17). A `metrics_query` answer carries the
/// ring, oldest first; a `metrics` stream sends the same shape with one sample
/// per array.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsReport {
    /// The node's sampling interval, 5 s.
    pub interval_s: u32,
    /// Present when the node was asked for: the node's samples, oldest first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<Vec<NodeSample>>,
    /// Present when VMs were asked for: one entry per asked VM the node runs;
    /// a VM it does not run is left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vms: Option<Vec<VmSamples>>,
}

/// One node sample (section 5.17).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeSample {
    /// RFC 3339 time of the sample.
    pub time: String,
    /// Busy fraction of all host CPUs over the interval, 0 to 1.
    pub cpu_usage: f64,
    /// Number of host CPUs.
    pub cpu_count: u32,
    /// `MemTotal`.
    pub memory_total_bytes: u64,
    /// `MemTotal - MemAvailable`.
    pub memory_used_bytes: u64,
}

/// The samples of one VM (section 5.17).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VmSamples {
    /// The VM (`vm_`).
    pub vm_id: String,
    /// Samples, oldest first.
    pub samples: Vec<VmSample>,
}

/// One VM sample, as the host sees the VM's VMM process (section 5.17).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VmSample {
    /// RFC 3339 time of the sample.
    pub time: String,
    /// CPU time of the VMM process over the interval, divided by the interval
    /// and by `vcpus`. It can exceed 1 briefly because the VMM's own threads
    /// count.
    pub cpu_usage: f64,
    /// The VM's virtual CPUs.
    pub vcpus: u32,
    /// Resident memory of the VMM process on the host.
    pub memory_rss_bytes: u64,
    /// The VM's configured memory.
    pub memory_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_protocol_example_round_trips() {
        let value = json!({
            "interval_s": 5,
            "node": [
                { "time": "2026-11-04T10:15:30Z", "cpu_usage": 0.12, "cpu_count": 8, "memory_total_bytes": 17_179_869_184_u64, "memory_used_bytes": 6_442_450_944_u64 }
            ],
            "vms": [
                {
                    "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F",
                    "samples": [
                        { "time": "2026-11-04T10:15:30Z", "cpu_usage": 0.04, "vcpus": 2, "memory_rss_bytes": 201_326_592, "memory_bytes": 536_870_912 }
                    ]
                }
            ]
        });
        let report: MetricsReport = serde_json::from_value(value.clone()).expect("parses");
        assert_eq!(report.interval_s, 5);
        assert_eq!(report.node.as_ref().map(Vec::len), Some(1));
        assert_eq!(serde_json::to_value(&report).expect("serializes"), value);
    }

    #[test]
    fn absent_parts_are_omitted() {
        let report = MetricsReport {
            interval_s: 5,
            node: None,
            vms: Some(Vec::new()),
        };
        assert_eq!(
            serde_json::to_value(&report).expect("serializes"),
            json!({ "interval_s": 5, "vms": [] })
        );
    }
}
