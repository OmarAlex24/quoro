// SPDX-License-Identifier: AGPL-3.0-only
//! Metric sampling for the `metrics_query` command and the `metrics` stream
//! (`node-agent-protocol.md` sections 5.15 and 5.17).
//!
//! The node samples the host and every running VM every [`INTERVAL`] into an
//! in-memory ring of ten minutes per node and per VM. Nothing is written to
//! disk and nothing is sent unless asked (ADR 0014); the ring is lost when the
//! agent restarts.
//!
//! The host reads `/proc/stat` and `/proc/meminfo` through [`HostSource`], so
//! tests on a machine without `/proc` drive the arithmetic with a fake source.
//! VM counters come from the engine (`engine-trait.md` section 3.15).

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use quoro_engine::{Engine, VmId};
use quoro_proto::metrics::{MetricsReport, NodeSample, VmSample, VmSamples};
use quoro_proto::wire::{ObservedStatus, VmStatus};
use tokio::sync::{broadcast, watch};

use crate::reconcile::Snapshot;
use crate::util;

/// Sampling interval (section 5.17).
pub const INTERVAL: Duration = Duration::from_secs(5);

/// The interval in the wire shape (section 5.17).
pub const INTERVAL_S: u32 = 5;

/// Samples kept per ring: ten minutes at one sample per five seconds.
pub const RING_LEN: usize = 120;

/// Buffered sampling rounds a subscriber may fall behind before it is told it
/// lagged.
const TICK_CAPACITY: usize = 32;

/// Aggregate CPU counters from `/proc/stat`, in the kernel's clock ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuTimes {
    /// Sum of the first eight fields of the aggregate `cpu` line.
    pub total: u64,
    /// `total - idle - iowait`.
    pub busy: u64,
}

/// Where the sampler reads the host's CPU and memory.
///
/// The trait keeps the sampler testable on a machine without `/proc`; the
/// production implementation is [`ProcHost`].
pub trait HostSource: Send + Sync {
    /// The aggregate CPU counters, or `None` when they cannot be read.
    fn cpu_times(&self) -> Option<CpuTimes>;
    /// `(MemTotal, MemAvailable)` in bytes, or `None` when they cannot be read.
    fn memory(&self) -> Option<(u64, u64)>;
    /// The number of host CPUs.
    fn cpu_count(&self) -> u32;
}

/// Reads `/proc/stat` and `/proc/meminfo`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcHost;

impl HostSource for ProcHost {
    fn cpu_times(&self) -> Option<CpuTimes> {
        let text = std::fs::read_to_string("/proc/stat").ok()?;
        parse_proc_stat(&text).map(|(times, _count)| times)
    }

    fn memory(&self) -> Option<(u64, u64)> {
        let text = std::fs::read_to_string("/proc/meminfo").ok()?;
        parse_meminfo(&text)
    }

    fn cpu_count(&self) -> u32 {
        let from_stat = std::fs::read_to_string("/proc/stat")
            .ok()
            .and_then(|text| parse_proc_stat(&text))
            .map(|(_times, count)| count)
            .filter(|count| *count > 0);
        from_stat.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map_or(1, |count| u32::try_from(count.get()).unwrap_or(u32::MAX))
        })
    }
}

/// Parses the aggregate line and counts the per-CPU lines of `/proc/stat`.
///
/// The first field set is `user nice system idle iowait irq softirq steal`;
/// guest time is already part of `user` and `nice`, so it is not added again.
pub fn parse_proc_stat(text: &str) -> Option<(CpuTimes, u32)> {
    let mut aggregate = None;
    let mut count = 0u32;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("cpu") else {
            continue;
        };
        if rest.starts_with(|c: char| c.is_ascii_digit()) {
            count += 1;
            continue;
        }
        if aggregate.is_some() || !(rest.is_empty() || rest.starts_with(' ')) {
            continue;
        }
        let fields: Vec<u64> = rest
            .split_whitespace()
            .map(str::parse)
            .collect::<Result<_, _>>()
            .ok()?;
        if fields.len() < 4 {
            return None;
        }
        let total: u64 = fields.iter().take(8).sum();
        let idle = fields[3];
        let iowait = fields.get(4).copied().unwrap_or(0);
        aggregate = Some(CpuTimes {
            total,
            busy: total.saturating_sub(idle).saturating_sub(iowait),
        });
    }
    aggregate.map(|times| (times, count))
}

/// Parses `MemTotal` and `MemAvailable` of `/proc/meminfo` into bytes.
pub fn parse_meminfo(text: &str) -> Option<(u64, u64)> {
    let mut total = None;
    let mut available = None;
    for line in text.lines() {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        let Some(value) = rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        match name {
            "MemTotal" => total = Some(value.saturating_mul(1024)),
            "MemAvailable" => available = Some(value.saturating_mul(1024)),
            _ => {}
        }
    }
    Some((total?, available?))
}

#[derive(Default)]
struct RingInner {
    node: VecDeque<NodeSample>,
    vms: HashMap<String, VecDeque<VmSample>>,
}

/// What one sampling round produced, broadcast to open `metrics` streams.
#[derive(Clone, Debug, PartialEq)]
pub struct Tick {
    /// The node sample of the round, when the round produced one.
    pub node: Option<NodeSample>,
    /// One sample per VM the round produced one for.
    pub vms: Vec<(String, VmSample)>,
}

/// The in-memory metric rings, shared by the sampler and every session.
#[derive(Clone)]
pub struct MetricsRing {
    inner: Arc<Mutex<RingInner>>,
    ticks: broadcast::Sender<Tick>,
}

impl MetricsRing {
    /// Creates empty rings.
    #[must_use]
    pub fn new() -> Self {
        let (ticks, _receiver) = broadcast::channel(TICK_CAPACITY);
        Self {
            inner: Arc::new(Mutex::new(RingInner::default())),
            ticks,
        }
    }

    /// Follows live sampling rounds. A subscriber that falls behind receives
    /// `Lagged` and may continue.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Tick> {
        self.ticks.subscribe()
    }

    /// Appends one round to the rings and broadcasts it.
    pub(crate) fn record(&self, node: Option<NodeSample>, vms: Vec<(String, VmSample)>) {
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(sample) = node.clone() {
                push_capped(&mut inner.node, sample);
            }
            for (vm_id, sample) in &vms {
                push_capped(inner.vms.entry(vm_id.clone()).or_default(), sample.clone());
            }
        }
        let _ = self.ticks.send(Tick { node, vms });
    }

    /// Drops rings of VMs that are no longer running, so a stopped VM
    /// disappears from later queries.
    fn prune(&self, keep: &HashSet<String>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.vms.retain(|vm_id, _samples| keep.contains(vm_id));
    }

    /// Answers `metrics_query` (section 5.17): the whole ring, oldest first.
    #[must_use]
    pub fn query(&self, node: bool, vm_ids: &[String]) -> MetricsReport {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let node = if node {
            Some(inner.node.iter().cloned().collect())
        } else {
            None
        };
        let vms = if vm_ids.is_empty() {
            None
        } else {
            Some(
                vm_ids
                    .iter()
                    .filter_map(|vm_id| {
                        inner.vms.get(vm_id).map(|samples| VmSamples {
                            vm_id: vm_id.clone(),
                            samples: samples.iter().cloned().collect(),
                        })
                    })
                    .collect(),
            )
        };
        MetricsReport {
            interval_s: INTERVAL_S,
            node,
            vms,
        }
    }
}

impl Default for MetricsRing {
    fn default() -> Self {
        Self::new()
    }
}

/// Appends one sample, dropping the oldest once the ring is full.
fn push_capped<T>(ring: &mut VecDeque<T>, sample: T) {
    if ring.len() >= RING_LEN {
        ring.pop_front();
    }
    ring.push_back(sample);
}

/// The previous counters of one VM, for the interval delta.
#[derive(Clone, Copy)]
struct PreviousVm {
    cpu_time_ms: u64,
    at: Instant,
}

/// Samples the host and the running VMs into a [`MetricsRing`].
pub struct MetricsSampler<E: Engine> {
    engine: Arc<E>,
    observed: watch::Receiver<Vec<VmStatus>>,
    desired: watch::Receiver<Option<Snapshot>>,
    host: Box<dyn HostSource>,
    ring: MetricsRing,
    previous_node: Option<CpuTimes>,
    previous_vms: HashMap<String, PreviousVm>,
}

impl<E: Engine> MetricsSampler<E> {
    /// Builds a sampler that reads the host through [`ProcHost`].
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        observed: watch::Receiver<Vec<VmStatus>>,
        desired: watch::Receiver<Option<Snapshot>>,
        ring: MetricsRing,
    ) -> Self {
        Self::with_host(engine, observed, desired, ring, Box::new(ProcHost))
    }

    /// Builds a sampler with an explicit host source, for tests.
    #[must_use]
    pub fn with_host(
        engine: Arc<E>,
        observed: watch::Receiver<Vec<VmStatus>>,
        desired: watch::Receiver<Option<Snapshot>>,
        ring: MetricsRing,
        host: Box<dyn HostSource>,
    ) -> Self {
        Self {
            engine,
            observed,
            desired,
            host,
            ring,
            previous_node: None,
            previous_vms: HashMap::new(),
        }
    }

    /// Runs one round per [`INTERVAL`] until `stop` resolves.
    pub async fn run(mut self, stop: impl Future<Output = ()>) {
        let mut stop = std::pin::pin!(stop);
        let mut ticker = tokio::time::interval(INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = &mut stop => break,
                _ = ticker.tick() => self.round(Instant::now(), SystemTime::now()).await,
            }
        }
    }

    /// Runs one sampling round. Split out so tests drive rounds without
    /// waiting for the interval.
    pub async fn round(&mut self, now_wall: Instant, now: SystemTime) {
        let time = util::rfc3339(now);

        let times = self.host.cpu_times();
        let memory = self.host.memory();
        let mut node = None;
        if let Some(current) = times {
            if let (Some(previous), Some((total, available))) = (self.previous_node, memory) {
                let delta_total = current.total.saturating_sub(previous.total);
                if delta_total > 0 {
                    let delta_busy = current.busy.saturating_sub(previous.busy);
                    node = Some(NodeSample {
                        time: time.clone(),
                        cpu_usage: (delta_busy as f64 / delta_total as f64).clamp(0.0, 1.0),
                        cpu_count: self.host.cpu_count(),
                        memory_total_bytes: total,
                        memory_used_bytes: total.saturating_sub(available),
                    });
                }
            }
            // Recorded even when no sample is emitted, so the next round has a
            // previous reading (the first round never produces a bogus 0).
            self.previous_node = Some(current);
        }

        let observed = self.observed.borrow().clone();
        let desired = self.desired.borrow().clone();
        let specs: HashMap<String, (u32, u64)> = desired
            .map(|snapshot| {
                snapshot
                    .state
                    .spec
                    .vms
                    .iter()
                    .map(|vm| {
                        let memory_bytes = u64::from(vm.memory_mb) * 1024 * 1024;
                        (vm.vm_id.clone(), (vm.vcpus, memory_bytes))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let running: Vec<String> = observed
            .iter()
            .filter(|vm| vm.observed_status == ObservedStatus::Running)
            .map(|vm| vm.vm_id.clone())
            .collect();
        let running_set: HashSet<String> = running.iter().cloned().collect();

        let mut vms = Vec::new();
        for vm_id in &running {
            let Some(&(vcpus, memory_bytes)) = specs.get(vm_id) else {
                continue;
            };
            let counters = match self.engine.counters(&VmId(vm_id.clone())).await {
                Ok(counters) => counters,
                Err(e) => {
                    // One VM's engine error skips that VM for this round only.
                    tracing::debug!(vm_id, error = %e, "could not read VM counters this round");
                    continue;
                }
            };
            let previous = self.previous_vms.insert(
                vm_id.clone(),
                PreviousVm {
                    cpu_time_ms: counters.cpu_time_ms,
                    at: now_wall,
                },
            );
            let Some(previous) = previous else {
                continue;
            };
            if counters.cpu_time_ms < previous.cpu_time_ms {
                // The VMM restarted; the next round starts a fresh interval.
                continue;
            }
            let wall_ms = now_wall
                .saturating_duration_since(previous.at)
                .as_secs_f64()
                * 1000.0;
            if wall_ms <= 0.0 {
                continue;
            }
            let delta_ms = counters.cpu_time_ms - previous.cpu_time_ms;
            let usage = (delta_ms as f64 / (wall_ms * f64::from(vcpus.max(1)))).max(0.0);
            vms.push((
                vm_id.clone(),
                VmSample {
                    time: time.clone(),
                    cpu_usage: usage,
                    vcpus,
                    memory_rss_bytes: counters.memory_rss_bytes,
                    memory_bytes,
                },
            ));
        }

        // A stopped VM leaves no ring and no previous reading behind.
        self.ring.prune(&running_set);
        self.previous_vms
            .retain(|vm_id, _previous| running_set.contains(vm_id));
        self.ring.record(node, vms);
    }
}

/// Parses the `metrics_query` and `metrics` stream params (section 5.17).
///
/// Returns `(node, vm_ids)`; at least one of them must be set.
pub fn parse_metrics_params(params: &serde_json::Value) -> Result<(bool, Vec<String>), String> {
    let node = match params.get("node") {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(value)) => *value,
        Some(_) => return Err("node must be a boolean".to_owned()),
    };
    let vm_ids = match params.get("vm_ids") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => {
            let mut ids = Vec::with_capacity(items.len());
            for item in items {
                let Some(vm_id) = item.as_str() else {
                    return Err("vm_ids must be an array of vm_ ids".to_owned());
                };
                if !is_vm_id(vm_id) {
                    return Err(format!("vm_ids contains an invalid VM id {vm_id:?}"));
                }
                ids.push(vm_id.to_owned());
            }
            ids
        }
        Some(_) => return Err("vm_ids must be an array of vm_ ids".to_owned()),
    };
    if !node && vm_ids.is_empty() {
        return Err("at least one of node or vm_ids is required".to_owned());
    }
    Ok((node, vm_ids))
}

/// `vm_` followed by the 26 characters of a ULID.
fn is_vm_id(vm_id: &str) -> bool {
    vm_id.len() == 29
        && vm_id.starts_with("vm_")
        && vm_id[3..].bytes().all(|byte| byte.is_ascii_alphanumeric())
}

/// Builds the `metrics` object of one `metrics` stream frame from one round.
///
/// Returns `None` when the round would carry nothing the stream asked for.
#[must_use]
pub fn tick_report(tick: &Tick, node: bool, vm_ids: &[String]) -> Option<MetricsReport> {
    let node = if node {
        tick.node.clone().map(|sample| vec![sample])
    } else {
        None
    };
    let vms: Option<Vec<VmSamples>> = if vm_ids.is_empty() {
        None
    } else {
        Some(
            vm_ids
                .iter()
                .filter_map(|vm_id| {
                    tick.vms
                        .iter()
                        .find(|(sampled, _sample)| sampled == vm_id)
                        .map(|(_sampled, sample)| VmSamples {
                            vm_id: vm_id.clone(),
                            samples: vec![sample.clone()],
                        })
                })
                .collect(),
        )
    };
    let has_node = node.as_ref().is_some_and(|samples| !samples.is_empty());
    let has_vms = vms.as_ref().is_some_and(|vms| !vms.is_empty());
    if !has_node && !has_vms {
        return None;
    }
    Some(MetricsReport {
        interval_s: INTERVAL_S,
        node,
        vms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use quoro_engine::{
        Capabilities, ConsoleStream, EngineError, EventStream, HostProbe, ImageDigest, ImageRef,
        LineStream, NodeNetwork, PreparedImage, StopMode, VmCounters, VmInfo, VmSpec,
    };
    use quoro_proto::wire::{DesiredState, DesiredStatus, Spec, SpecNetwork, SpecVm};

    const VM_A: &str = "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F";

    #[test]
    fn parses_the_aggregate_cpu_line_and_counts_cpus() {
        let text = "\
cpu  10 20 30 40 50 60 70 80 90 100
cpu0 1 2 3 4 5 6 7 8 9 10
cpu1 1 2 3 4 5 6 7 8 9 10
intr 12345
";
        let (times, count) = parse_proc_stat(text).expect("parses");
        // The first eight fields; guest and guest_nice are already in user and
        // nice and must not be added twice.
        assert_eq!(times.total, 10 + 20 + 30 + 40 + 50 + 60 + 70 + 80);
        assert_eq!(times.busy, times.total - 40 - 50);
        assert_eq!(count, 2);
    }

    #[test]
    fn a_malformed_stat_line_has_no_cpu_times() {
        assert!(parse_proc_stat("cpu  1 2\n").is_none());
        assert!(parse_proc_stat("intr 1 2 3\n").is_none());
    }

    #[test]
    fn parses_meminfo_into_bytes() {
        let text = "\
MemTotal:       16384000 kB
MemFree:         1000000 kB
MemAvailable:    8192000 kB
Buffers:          100000 kB
";
        let (total, available) = parse_meminfo(text).expect("parses");
        assert_eq!(total, 16_384_000 * 1024);
        assert_eq!(available, 8_192_000 * 1024);
    }

    #[test]
    fn meminfo_without_the_needed_fields_is_unreadable() {
        assert!(parse_meminfo("MemTotal: 1 kB\n").is_none());
        assert!(parse_meminfo("").is_none());
    }

    #[test]
    fn params_require_a_selector_and_well_formed_vm_ids() {
        let err = parse_metrics_params(&serde_json::json!({})).unwrap_err();
        assert!(err.contains("at least one"));
        let err = parse_metrics_params(&serde_json::json!({"vm_ids": ["vm_short"]})).unwrap_err();
        assert!(err.contains("invalid VM id"));
        let err = parse_metrics_params(&serde_json::json!({"node": "yes"})).unwrap_err();
        assert!(err.contains("boolean"));
        let (node, vms) = parse_metrics_params(&serde_json::json!({
            "node": true,
            "vm_ids": [VM_A]
        }))
        .expect("parses");
        assert!(node);
        assert_eq!(vms, vec![VM_A.to_owned()]);
        let (node, vms) = parse_metrics_params(&serde_json::json!({"node": true})).expect("parses");
        assert!(node);
        assert!(vms.is_empty());
    }

    /// A host whose readings the test changes between rounds.
    struct TestHost {
        cpu: Arc<Mutex<Option<CpuTimes>>>,
        memory: Option<(u64, u64)>,
        cpus: u32,
    }

    impl HostSource for TestHost {
        fn cpu_times(&self) -> Option<CpuTimes> {
            *self.cpu.lock().unwrap()
        }

        fn memory(&self) -> Option<(u64, u64)> {
            self.memory
        }

        fn cpu_count(&self) -> u32 {
            self.cpus
        }
    }

    /// An engine whose `counters` the test scripts; nothing else is reachable.
    struct CountersEngine {
        counters: Mutex<HashMap<String, Result<VmCounters, ()>>>,
    }

    impl CountersEngine {
        fn new() -> Self {
            Self {
                counters: Mutex::new(HashMap::new()),
            }
        }

        fn set(&self, vm_id: &str, cpu_time_ms: u64, memory_rss_bytes: u64) {
            self.counters.lock().unwrap().insert(
                vm_id.to_owned(),
                Ok(VmCounters {
                    cpu_time_ms,
                    memory_rss_bytes,
                    net_rx_bytes: 0,
                    net_tx_bytes: 0,
                    block_read_bytes: 0,
                    block_write_bytes: 0,
                }),
            );
        }

        fn fail(&self, vm_id: &str) {
            self.counters
                .lock()
                .unwrap()
                .insert(vm_id.to_owned(), Err(()));
        }
    }

    impl Engine for CountersEngine {
        async fn capabilities(&self) -> Capabilities {
            unimplemented!("not used by the sampler")
        }

        async fn probe_host(&self) -> Result<HostProbe, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn ensure_network(&self, _net: &NodeNetwork) -> Result<(), EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn prepare_image(&self, _image: &ImageRef) -> Result<PreparedImage, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn remove_image(&self, _digest: &ImageDigest) -> Result<(), EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn create(&self, _spec: &VmSpec) -> Result<VmInfo, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn start(&self, _id: &VmId) -> Result<VmInfo, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn stop(&self, _id: &VmId, _mode: StopMode) -> Result<VmInfo, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn delete(&self, _id: &VmId) -> Result<(), EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn inspect(&self, _id: &VmId) -> Result<VmInfo, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn list(&self) -> Result<Vec<VmInfo>, EngineError> {
            unimplemented!("not used by the sampler")
        }

        fn events(&self) -> EventStream {
            unimplemented!("not used by the sampler")
        }

        fn output(&self, _id: &VmId) -> Result<LineStream, EngineError> {
            unimplemented!("not used by the sampler")
        }

        fn console(&self, _id: &VmId) -> Result<ConsoleStream, EngineError> {
            unimplemented!("not used by the sampler")
        }

        async fn counters(&self, id: &VmId) -> Result<VmCounters, EngineError> {
            match self.counters.lock().unwrap().get(&id.0).cloned() {
                Some(Ok(counters)) => Ok(counters),
                Some(Err(())) | None => Err(EngineError::NotFound),
            }
        }
    }

    fn running(vm_id: &str) -> VmStatus {
        VmStatus {
            vm_id: vm_id.to_owned(),
            observed_status: ObservedStatus::Running,
            health: quoro_proto::wire::Health::Unknown,
            ip: None,
            restart_count: 0,
            last_exit_code: None,
        }
    }

    fn spec_vm(vm_id: &str, vcpus: u32, memory_mb: u32) -> SpecVm {
        SpecVm {
            vm_id: vm_id.to_owned(),
            project_id: "prj_test".to_owned(),
            app_id: None,
            deployment_id: None,
            name: "app".to_owned(),
            image: "img".to_owned(),
            image_digest: "sha256:00".to_owned(),
            vcpus,
            memory_mb,
            disk_gb: 1,
            port: None,
            env: BTreeMap::new(),
            health_check: None,
            desired_status: DesiredStatus::Running,
            restart_generation: 0,
            stateful: false,
            lease_id: None,
            fencing_token: None,
            volumes: Vec::new(),
            build_id: None,
        }
    }

    fn snapshot(vms: Vec<SpecVm>) -> Snapshot {
        Snapshot {
            rev: 1,
            state: DesiredState {
                rev: 1,
                issued_at: "2026-11-04T10:15:30Z".to_owned(),
                term: 1,
                spec: Spec {
                    vms,
                    routes: Vec::new(),
                    network: SpecNetwork {
                        subnet: "10.0.0.0/24".to_owned(),
                        gateway: "10.0.0.1".to_owned(),
                    },
                    ingress: None,
                    builds: Vec::new(),
                },
                signature: String::new(),
            },
        }
    }

    #[tokio::test]
    async fn the_first_round_records_cpu_but_emits_no_node_sample() {
        let cpu = Arc::new(Mutex::new(Some(CpuTimes {
            total: 1_000,
            busy: 200,
        })));
        let host = TestHost {
            cpu: Arc::clone(&cpu),
            memory: Some((8 * 1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024)),
            cpus: 4,
        };
        let (_observed_tx, observed) = watch::channel(Vec::new());
        let (_desired_tx, desired) = watch::channel(None);
        let ring = MetricsRing::new();
        let engine = Arc::new(CountersEngine::new());
        let mut sampler =
            MetricsSampler::with_host(engine, observed, desired, ring.clone(), Box::new(host));

        let at = Instant::now();
        sampler.round(at, SystemTime::UNIX_EPOCH).await;
        assert!(ring.query(true, &[]).node.unwrap().is_empty());

        *cpu.lock().unwrap() = Some(CpuTimes {
            total: 1_200,
            busy: 300,
        });
        sampler
            .round(at + Duration::from_secs(5), SystemTime::UNIX_EPOCH)
            .await;
        let samples = ring.query(true, &[]).node.unwrap();
        assert_eq!(samples.len(), 1);
        assert!((samples[0].cpu_usage - 0.5).abs() < 1e-9);
        assert_eq!(samples[0].cpu_count, 4);
        assert_eq!(samples[0].memory_total_bytes, 8 * 1024 * 1024 * 1024);
        assert_eq!(samples[0].memory_used_bytes, 6 * 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn node_cpu_usage_is_clamped_to_one() {
        let cpu = Arc::new(Mutex::new(Some(CpuTimes {
            total: 100,
            busy: 10,
        })));
        let host = TestHost {
            cpu: Arc::clone(&cpu),
            memory: Some((1024, 512)),
            cpus: 1,
        };
        let (_observed_tx, observed) = watch::channel(Vec::new());
        let (_desired_tx, desired) = watch::channel(None);
        let ring = MetricsRing::new();
        let mut sampler = MetricsSampler::with_host(
            Arc::new(CountersEngine::new()),
            observed,
            desired,
            ring.clone(),
            Box::new(host),
        );
        let at = Instant::now();
        sampler.round(at, SystemTime::UNIX_EPOCH).await;
        // Busy cannot exceed total in a real kernel, but a wrap must still not
        // produce a value above 1.
        *cpu.lock().unwrap() = Some(CpuTimes {
            total: 200,
            busy: 500,
        });
        sampler
            .round(at + Duration::from_secs(5), SystemTime::UNIX_EPOCH)
            .await;
        let samples = ring.query(true, &[]).node.unwrap();
        assert_eq!(samples[0].cpu_usage, 1.0);
    }

    #[tokio::test]
    async fn vm_cpu_math_uses_the_interval_and_the_vcpu_count() {
        let engine = Arc::new(CountersEngine::new());
        engine.set(VM_A, 1_000, 100 * 1024 * 1024);
        let (observed_tx, observed) = watch::channel(vec![running(VM_A)]);
        let (desired_tx, desired) = watch::channel(Some(snapshot(vec![spec_vm(VM_A, 2, 512)])));
        let ring = MetricsRing::new();
        let host = TestHost {
            cpu: Arc::new(Mutex::new(None)),
            memory: None,
            cpus: 1,
        };
        let mut sampler = MetricsSampler::with_host(
            Arc::clone(&engine),
            observed,
            desired,
            ring.clone(),
            Box::new(host),
        );

        let at = Instant::now();
        sampler.round(at, SystemTime::UNIX_EPOCH).await;
        assert!(
            ring.query(false, &[VM_A.to_owned()])
                .vms
                .unwrap()
                .is_empty()
        );

        // 1000 ms of CPU time over 5 s on 2 vCPUs is 0.1.
        engine.set(VM_A, 2_000, 100 * 1024 * 1024);
        sampler
            .round(at + Duration::from_secs(5), SystemTime::UNIX_EPOCH)
            .await;
        let vms = ring.query(false, &[VM_A.to_owned()]).vms.unwrap();
        assert_eq!(vms.len(), 1);
        let sample = &vms[0].samples[0];
        assert!((sample.cpu_usage - 0.1).abs() < 1e-9);
        assert_eq!(sample.vcpus, 2);
        assert_eq!(sample.memory_rss_bytes, 100 * 1024 * 1024);
        assert_eq!(sample.memory_bytes, 512 * 1024 * 1024);

        // A VMM restart (a lower cumulative counter) emits no sample.
        engine.set(VM_A, 500, 100 * 1024 * 1024);
        sampler
            .round(at + Duration::from_secs(10), SystemTime::UNIX_EPOCH)
            .await;
        assert_eq!(
            ring.query(false, &[VM_A.to_owned()]).vms.unwrap()[0]
                .samples
                .len(),
            1
        );

        // An engine error skips the VM for the round; its ring stays.
        engine.fail(VM_A);
        sampler
            .round(at + Duration::from_secs(15), SystemTime::UNIX_EPOCH)
            .await;
        assert_eq!(
            ring.query(false, &[VM_A.to_owned()]).vms.unwrap()[0]
                .samples
                .len(),
            1
        );

        // The next good round samples against the reading before the failure.
        engine.set(VM_A, 1_000, 100 * 1024 * 1024);
        sampler
            .round(at + Duration::from_secs(20), SystemTime::UNIX_EPOCH)
            .await;
        assert_eq!(
            ring.query(false, &[VM_A.to_owned()]).vms.unwrap()[0]
                .samples
                .len(),
            2
        );

        drop(observed_tx);
        drop(desired_tx);
    }

    #[tokio::test]
    async fn a_stopped_vm_disappears_and_unknown_vms_are_left_out() {
        let engine = Arc::new(CountersEngine::new());
        engine.set(VM_A, 0, 0);
        let (observed_tx, observed) = watch::channel(vec![running(VM_A)]);
        let (_desired_tx, desired) = watch::channel(Some(snapshot(vec![spec_vm(VM_A, 1, 128)])));
        let ring = MetricsRing::new();
        let host = TestHost {
            cpu: Arc::new(Mutex::new(None)),
            memory: None,
            cpus: 1,
        };
        let mut sampler = MetricsSampler::with_host(
            Arc::clone(&engine),
            observed,
            desired,
            ring.clone(),
            Box::new(host),
        );
        let at = Instant::now();
        sampler.round(at, SystemTime::UNIX_EPOCH).await;
        engine.set(VM_A, 1_000, 0);
        sampler
            .round(at + Duration::from_secs(5), SystemTime::UNIX_EPOCH)
            .await;

        let unknown = "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0G";
        assert!(
            ring.query(false, &[VM_A.to_owned(), unknown.to_owned()])
                .vms
                .unwrap()
                .iter()
                .all(|vms| vms.vm_id != unknown)
        );

        observed_tx.send(Vec::new()).unwrap();
        sampler
            .round(at + Duration::from_secs(10), SystemTime::UNIX_EPOCH)
            .await;
        assert!(
            ring.query(false, &[VM_A.to_owned()])
                .vms
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_ring_keeps_only_the_newest_samples() {
        let ring = MetricsRing::new();
        for index in 0..(RING_LEN + 5) {
            ring.record(
                Some(NodeSample {
                    time: format!("t{index}"),
                    cpu_usage: 0.0,
                    cpu_count: 1,
                    memory_total_bytes: 0,
                    memory_used_bytes: 0,
                }),
                Vec::new(),
            );
        }
        let samples = ring.query(true, &[]).node.unwrap();
        assert_eq!(samples.len(), RING_LEN);
        assert_eq!(samples[0].time, format!("t{}", 5));
        assert_eq!(samples[RING_LEN - 1].time, format!("t{}", RING_LEN + 4));
    }

    #[test]
    fn tick_report_carries_only_what_the_stream_asked_for() {
        let sample = VmSample {
            time: "2026-11-04T10:15:30Z".to_owned(),
            cpu_usage: 0.1,
            vcpus: 1,
            memory_rss_bytes: 0,
            memory_bytes: 0,
        };
        let tick = Tick {
            node: Some(NodeSample {
                time: "2026-11-04T10:15:30Z".to_owned(),
                cpu_usage: 0.2,
                cpu_count: 2,
                memory_total_bytes: 4,
                memory_used_bytes: 1,
            }),
            vms: vec![(VM_A.to_owned(), sample)],
        };
        assert!(tick_report(&tick, false, &[]).is_none());
        let report = tick_report(&tick, true, &[VM_A.to_owned()]).expect("carries samples");
        assert_eq!(report.node.unwrap().len(), 1);
        assert_eq!(report.vms.unwrap()[0].samples.len(), 1);
        let other = "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0G".to_owned();
        assert!(tick_report(&tick, false, &[other]).is_none());
    }
}
