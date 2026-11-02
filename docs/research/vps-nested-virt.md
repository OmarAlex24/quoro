# Nested virtualization (KVM inside a VM) across common VPS and cloud providers

- **Question:** Q-0015 VERIFY: which common VPS providers expose nested virtualization / KVM
- **Researched:** 2026-11-02 by agent:ds-research-3
- **Status:** answered (11 providers now have a fetchable official statement; 4 remain "not found" and 2 are reported-only)

## Short answer

Nested KVM is the exception, not the rule, on cheap VPS. It is explicitly refused by Hetzner Cloud, Hostinger VPS, Contabo VPS and netcup (SVM flag disabled), and reported refused by Akamai/Linode. It is offered on OVHcloud Public Cloud (with vmx exposed and a live-migration caveat), on DigitalOcean Droplets (per a DigitalOcean support-engineer answer; any Intel/AMD split is unconfirmed), on Contabo VDS, on Vultr VX1 only, and on specific cloud families: AWS opt-in (M8i/C8i/R8i/M7i/C7i/R7i/I7i/X8i), GCP Intel and N4D, Azure on Dv3/Dv4-class VMs but not with Trusted Launch, and OCI Intel VM shapes (OCI AMD VM shapes are excluded in writing). Bare metal is the reliable answer: a dedicated server has no hypervisor under you, so `/dev/kvm` is yours as long as VT-x/AMD-V is on in firmware.

For Quoro's MVP the practical shortlist is: bare metal (Hetzner Robot/OVH/Scaleway Elastic Metal), OVHcloud Public Cloud, DigitalOcean Droplets (verify CPU family yourself — the Intel/AMD split is unconfirmed), AWS `c8i`/`m8i`/`r8i` with nested virtualization enabled, GCP Intel instances, or OCI Intel VM shapes. Everything else on the list must be tested with `ls /dev/kvm` before it is promised to a user.

"KVM VPS" in marketing means the *host* runs KVM. It says nothing about `/dev/kvm` inside your guest; most such plans do not expose it.

## Findings

### Provider matrix

Accessed 2026-11-02. "Bare metal" rows are inferred: with no hypervisor layer, the guest OS is the host OS and owns VT-x/AMD-V. That inference is labelled Hypothesis even where the product description is Confirmed. Rows marked "Not found" mean a real search was run and no official statement exists (the queries are listed under Open risks); they are not a negative claim.

| Provider | Product | Nested virt / `/dev/kvm` in guest? | Notes | Source | Label |
| --- | --- | --- | --- | --- | --- |
| Hetzner | Cloud (CX/CPX/CAX/CCX) | **No** | FAQ answer "Can I run virtual machines on cloud servers, or rather is nested virtualization possible? — No, this is not possible on cloud server." Page last changed 2024-09-12 (older than 12 months) | https://docs.hetzner.com/cloud/servers/faq/ | Confirmed |
| Hetzner | Dedicated / Robot (bare metal) | Expected yes | Dedicated hardware, you install the OS and hypervisor; CPU must have VT-x/AMD-V enabled in BIOS. No Hetzner page verified `/dev/kvm` | — (inference) | Hypothesis |
| DigitalOcean | Droplets | **Yes** | A DigitalOcean support engineer (employee badge, "Sr. Developer Support Engineer") answered: "Currently, DigitalOcean Droplets support KVM or nested virtualization in all regions. We do not recommend nested virtualization on the platform because there is often very poor performance." Answer dated 2021-03-03 — old, and a community answer rather than a docs page, but it is DO-authored | https://www.digitalocean.com/community/questions/does-digitalocean-support-kvm-or-nested-virtulzation (accessed 2026-11-02) | Confirmed |
| DigitalOcean | Droplets (live migration caveat) | Nested VMs must be restarted after host migration | "If you have Droplets running workloads leveraging nested virtualization, you need to restart any nested VMs after a live migration." | https://docs.digitalocean.com/products/droplets/details/live-migration/ (last verified 2026-07-29) | Confirmed |
| DigitalOcean | Droplets (CPU-family split) | Intel yes / AMD no | **No DO page states any CPU restriction.** The Intel/AMD split comes from community reports only; DO's plan docs describe Intel vs AMD premium CPUs without mentioning virtualization | — (community) | Hypothesis |
| Vultr | Cloud Compute / Optimized Cloud Compute | Not found | No official nested-virt statement for these plan families. Vultr's own docs index (`docs.vultr.com/llms.txt`, fetched) contains no "nested virtualization" entry; the only documented product is VX1 (next row) | https://docs.vultr.com/llms.txt | Not found (open risk) |
| Vultr | VX1 Cloud Compute | **Yes** | "For sandbox support: Vultr VX1 supports nested virtualization, enabling businesses to deploy microVM solutions with their own kernels for secure sandbox isolation." A separate Vultr whitepaper/landing page repeats "Vultr VX1 supports nested virtualization, enabling secure provisioning of microVMs for AI sandboxes" (seen only in search snippet) | https://blogs.vultr.com/cpus-workhorse-agentic-ai-infrastructure (accessed 2026-11-02) | Confirmed (VX1 only) |
| Vultr | Bare Metal | Expected yes | "Bare Metal instances don't use any virtualization" — single-tenant physical hardware | https://docs.vultr.com/products/compute/instances/bare-metal/provisioning.md (last updated 2026-10-23) | Confirmed (no virtualization layer); Hypothesis (KVM available) |
| Akamai / Linode | Compute Instances | **No** (reported) | Official answers exist only on `linode.com/community` and `akamai.com/cloud/community`, both of which return HTTP 403 to every fetch attempt. Snippets show "At present, there is no option to enable nested virtualization for a Linode" and "A Linode running on our host's KVM hypervisor has nested virtualization disabled by default". **Seen only in search snippets; page not fetched** | https://www.akamai.com/cloud/community/questions/17491/how-do-i-enable-nested-virtualization-for-my-linode (403) | Hypothesis |
| OVHcloud | VPS | Unclear; marketing implies VMs inside | The KVM VPS page says "KVM VPS turns the Linux kernel into a hypervisor and enables you to launch multiple virtual machines", but this is generic marketing copy (it also distinguishes itself from the KVM *console*) and nowhere states that `/dev/kvm`/vmx is exposed in the guest. No OVH VPS FAQ entry on nested virt was found | https://www.ovhcloud.com/en/vps/vps-kvm/ (accessed 2026-11-02) | Hypothesis |
| OVHcloud | Public Cloud instances | **Yes, with a live-migration caveat** | Exact FAQ answer: "Is nested virtualization supported? Yes and no. Yes because it's enabled (we provide vmx flag to your instance), which means you can start any virtualization technology that relies on this in your instance (such as KVM, QEMU, VirtualBox, Xen, HyperV, etc.). No because as soon we will live-migrate your instance (and this can happen any time, based on our hypervisor management), then your kernel may panic." | https://docs.ovhcloud.com/en/guides/public-cloud/cross-functional/faq-pci (accessed 2026-11-02) | Confirmed |
| OVHcloud | Bare Metal / Rise / Advance | Expected yes | Bare metal product line; you own the hypervisor choice (OVH documents Proxmox, Hyper-V, ESXi installs) | — (inference) | Hypothesis |
| Scaleway | Instances (DEV1/PLAY2/PRO2/ENT1/POP2) | Not documented | Instance docs describe Instances as VMs on a shared hypervisor and never mention exposing vmx/svm. Scaleway's only KVM-inside-a-server tutorial is explicitly scoped to Elastic Metal, which is consistent with (but does not prove) Instances not supporting nesting | https://www.scaleway.com/en/docs/instances/concepts | Not found (open risk) |
| Scaleway | Elastic Metal (bare metal) | Expected yes | Official tutorial walks through `kvm-ok` on an Elastic Metal server and expects "INFO: /dev/kvm exists / KVM acceleration can be used", then `virt-install`. Reviewed 2025-07-02 | https://www.scaleway.com/en/docs/tutorials/install-kvm-elastic-metal/ (accessed 2026-11-02) | Confirmed (procedure documented); Hypothesis (works on every plan) |
| Contabo | VPS | **No** | "You can only setup nested virtualization with Contabo Dedicated Servers and Virtual Dedicated Servers (VDS). You cannot set up nested virtualization with Contabo Virtual Private Servers (VPS)." | https://help.contabo.com/en/support/solutions/articles/103000271595-can-i-setup-nested-virtualization-on-my-server- (accessed 2026-11-02) | Confirmed |
| Contabo | VDS (virtual dedicated server) | **Yes** | Same article: "You can only setup nested virtualization with Contabo Dedicated Servers and Virtual Dedicated Servers (VDS)"; and "Virtual Dedicated Servers have their own dedicated hardware resources and support nested virtualization". Caveat: Hyper-V inside a Windows VDS is broken per Contabo's own KB | https://help.contabo.com/en/support/solutions/articles/103000271595-can-i-setup-nested-virtualization-on-my-server- and https://contabo.com/blog/kb/103000270408-why-am-i-unable-to-create-virtual-machines-with-hyper-v-on-a-virtual-dedicated-server/ (both accessed 2026-11-02) | Confirmed |
| Netcup | VPS (x86 / ARM) | **No (SVM flag disabled)** | Help-centre FAQ "Can I virtualize on my server?" — "Using your own virtualization solutions on our servers is only possible to a limited extent. Activating nested virtualization (e.g., via the SVM flag) can lead to problems at the processor level... For security and stability reasons, the SVM flag is therefore disabled on our systems and cannot be activated." Wording is AMD-specific (SVM) | https://www.netcup.com/en/helpcenter/documentation/server (accessed 2026-11-02) | Confirmed |
| Netcup | Root Server (dedicated) | Unclear / do not assume yes | The same "Servers" docs page compares VPS, ARM, Root Server and Dedicated CPU Cores products and its nesting FAQ is written for "our servers" generally, with the SVM flag "disabled on our systems". No netcup page confirms nesting on a root server | https://www.netcup.com/en/helpcenter/documentation/server (accessed 2026-11-02) | Hypothesis |
| Hostinger | VPS (KVM) | **No** | "Nested virtualization is not supported on Hostinger Virtual Private Server (VPS) hosting plans. Hostinger disables it to protect resource allocation, system stability, and server performance"; "Hostinger VPS plans run on KVM virtualization without nested virtualization" | https://www.hostinger.com/support/10429687-is-nested-virtualization-supported-in-hostinger/ (updated ~2026-10-29, accessed 2026-11-02) | Confirmed |
| IONOS | VPS / Cloud Servers | Not found | No IONOS page states support or refusal for VPS or Cloud Servers. IONOS publishes KVM guidance only for *Dedicated Servers* ("Guidelines: Hosting Virtual Machines", seen in search results, not fetched) | — | Not found (open risk) |
| UpCloud | Cloud Servers | Not found | The Cloud Servers docs index and product pages were fetched and contain no occurrence of "nested"; no support/KB article exists. UpCloud does appear as a sponsor in third-party microVM write-ups, which is not evidence | https://upcloud.com/docs/products/cloud-servers/ (accessed 2026-11-02) | Not found (open risk) |
| AWS | EC2 virtual (non-.metal) | **Yes, opt-in, specific families** | M7i, M7i-flex, M8i, M8id, M8i-flex; C7i, C7i-flex, C8i, C8id, C8i-flex; R7i, R7iz, R8i, R8id, R8i-flex, X8i; I7i, I7ie. Enable with `--cpu-options "NestedVirtualization=enabled"` at launch or on a stopped instance. KVM and Hyper-V supported as L1. No extra cost. AWS recommends bare metal for performance-sensitive workloads | https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html | Confirmed |
| Google Cloud | Compute Engine | **Yes, Intel (and N4D) only** | Only Linux KVM as L1. Not available on E2, memory-optimized, ARM, or AMD (except N4D). Enabled per-VM; may be blocked by an org constraint | https://cloud.google.com/compute/docs/instances/nested-virtualization/overview | Confirmed |
| Microsoft Azure | Virtual Machines | **Yes, on supported series** | Microsoft's own troubleshooting doc uses Dv3/Dv4 as examples of "a series that supports nested virtualization". Trusted Launch security type does **not** support nested virtualization (Hyper-V install then fails with "virtualization support is not enabled in the BIOS") | https://learn.microsoft.com/en-us/troubleshoot/azure/virtual-machines/windows/troubleshoot-vm-by-use-nested-virtualization (updated 2026-07-16) | Confirmed |
| Oracle Cloud | VM shapes (x86) — Intel | **Yes** | Oracle's Oracle Linux KVM image doc lists as a release feature: "Nested virtualization for Oracle Cloud Infrastructure Intel-based virtual machine (VM) shapes", linking to Creating a Virtual Machine. Also: "Instances with Nested Virtualization enabled do not support Live Migration." | https://docs.oracle.com/en-us/iaas/oracle-linux/oci/index.htm and https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/movinganinstance.htm (both accessed 2026-11-02) | Confirmed |
| Oracle Cloud | VM shapes (x86) — AMD | **No** | "You can create Oracle KVM instances using any of the compute shapes. However, AMD processor-based virtual machines do not support nested virtualization." | https://docs.oracle.com/en-us/iaas/oracle-linux/oci/index.htm (accessed 2026-11-02) | Confirmed |
| Oracle Cloud | Ampere A1 (Arm) | Unlikely / not stated | The OCI docs exclude AMD VM shapes by name but say nothing about Arm VM shapes; no page states A1 can expose EL2. Assume unsupported until tested | — (absence of statement) | Hypothesis |
| Oracle Cloud | Bare Metal shapes | Expected yes | Bare metal | — (inference) | Hypothesis |
| Equinix | Metal | **Product is gone** | "Equinix Metal was sunset on June 30, 2026. The user documentation was removed on October 1, 2026." Timeline: End-of-Sale/EOL announced 2024-11-07; final day of service 2026-06-30; all resources removed 2026-07-01; console access removed 2027-01-01 | https://docs.equinix.com/goodbye-metal/ (accessed 2026-11-02) | Confirmed |

### Why "KVM VPS" does not mean nested KVM

- The KVM kernel modules do not enable nesting by default: you must set `nested=1` (AMD) or `nested=Y` (Intel) on the *host*. So a provider can run KVM and still hand you a CPU without vmx/svm. **Confirmed.** Source: https://www.linux-kvm.org/page/Nested_Guests (accessed 2026-11-02)
- The same page calls nested guests "working but experimental" (as of Feb 2018) and documents a hard limitation: once an L1 guest runs an L2 guest it cannot be migrated, saved or loaded until the L2 shuts down. **Confirmed.** Same URL.
- Therefore "KVM VPS" is a statement about the host's hypervisor (and usually about virtio/ISO support), not about guest CPU features. Choosing a plan because it says KVM tells you nothing about `/dev/kvm`. **Hypothesis** (interpretation; no provider publishes a "KVM VPS implies no nesting" statement).
- Corollary for bare metal: because there is no hypervisor under the OS, `/dev/kvm` works if the CPU exposes VT-x/AMD-V and firmware enables it. **Hypothesis** for every provider except where the product page states "no virtualization layer" (Vultr, Confirmed).

### How a user checks support

- `/dev/kvm` present and writable: `ls -l /dev/kvm` (expect `crw-rw---- root kvm`). Then test an actual VM boot; presence alone does not prove the CPU supports it.
- CPU flags: `grep -E 'vmx|svm' /proc/cpuinfo` (`vmx` = Intel VT-x, `svm` = AMD-V). This is exactly the check Proxmox's own nested-virt guide recommends. **Confirmed.** Source: https://pve.proxmox.com/wiki/Nested_Virtualization (accessed 2026-11-02; last edited 2025-12-03)
- Module parameter on a host you control: `cat /sys/module/kvm_intel/parameters/nested` (or `kvm_amd`). **Confirmed.** Same URL.
- `kvm-ok` (from the `cpu-checker` package on Debian/Ubuntu) wraps the above and prints whether KVM acceleration can be used. **Hypothesis** (widely used; not verified against a vendor doc in this pass).
- Quoro's node-agent should run this check at enrolment and refuse to advertise "KVM-capable" without a real microVM boot test — `/dev/kvm` can exist while L2 guest entry fails.

### Performance cost of nested virtualization

- GCP: "Even with hardware-assisted nested virtualization, nested VMs might experience a 10% or greater decrease in performance for workloads that are CPU-bound and possibly greater than a 10% decrease for workloads that are input/output bound." **Confirmed.** Source: https://cloud.google.com/compute/docs/instances/nested-virtualization/overview (accessed 2026-11-02)
- Proxmox: running VMs with no hardware-assisted extensions exposed is "nearly unusable (you can expect 10x slower or more)"; with extensions exposed, nesting still "obviously adds an overhead". **Confirmed.** Source: https://pve.proxmox.com/wiki/Nested_Virtualization (accessed 2026-11-02)
- AWS: recommends evaluating bare metal for workloads that need hardware virtualization extensions and are performance- or latency-sensitive. **Confirmed.** Source: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html (accessed 2026-11-02)
- Proxmox also warns: "VMs with nesting active (vmx/svm flag) cannot be live-migrated". **Confirmed.** Same Proxmox URL.
- Practical reading for Quoro: expect ~10%+ CPU overhead and worse I/O in a nested lab, plus no live migration; benchmarking on such a lab is not representative.

### Enabling nested virtualization on a Proxmox host

Generic procedure on a physical Proxmox VE (PVE) host:

1. Confirm the host CPU is AMD or a recent Intel, kernel >= 3.10. **Confirmed.** Source: https://pve.proxmox.com/wiki/Nested_Virtualization
2. Check `cat /sys/module/kvm_intel/parameters/nested` (or `kvm_amd`); `N` means off.
3. Persist it:
   - Intel: `echo "options kvm-intel nested=Y" > /etc/modprobe.d/kvm-intel.conf`
   - AMD: `echo "options kvm-amd nested=1" > /etc/modprobe.d/kvm-amd.conf`
   Then `modprobe -r kvm_intel && modprobe kvm_intel` (or reboot).
4. Set each guest VM's CPU type to `host`: `qm set <vmid> --cpu host`.
5. Verify inside the guest with `egrep '(vmx|svm)' /proc/cpuinfo`.
6. Expect no live migration for those VMs while nesting is active.

All six points are **Confirmed** by the Proxmox wiki page above (accessed 2026-11-02).

## Versions checked

| Component | Version / date | Source |
| --- | --- | --- |
| Hetzner Cloud FAQ | last changed 2024-09-12 (older than 12 months) | https://docs.hetzner.com/cloud/servers/faq/ |
| DigitalOcean Live Migrations doc | last verified 2026-07-29 | https://docs.digitalocean.com/products/droplets/details/live-migration/ |
| DigitalOcean community answer on KVM/nested virt | posted 2021-03-03 (5.5 years old) | https://www.digitalocean.com/community/questions/does-digitalocean-support-kvm-or-nested-virtulzation |
| Vultr docs index (`llms.txt`) | no nested-virt entry | https://docs.vultr.com/llms.txt |
| Vultr blog on VX1 CPUs | undated; fetched 2026-11-02 | https://blogs.vultr.com/cpus-workhorse-agentic-ai-infrastructure |
| Vultr Bare Metal provisioning doc | last updated 2026-10-23 | https://docs.vultr.com/products/compute/instances/bare-metal/provisioning.md |
| OVHcloud Public Cloud FAQ | current, undated | https://docs.ovhcloud.com/en/guides/public-cloud/cross-functional/faq-pci |
| OVHcloud KVM VPS product page | current, undated | https://www.ovhcloud.com/en/vps/vps-kvm/ |
| Scaleway KVM on Elastic Metal tutorial | reviewed 2025-07-02 | https://www.scaleway.com/en/docs/tutorials/install-kvm-elastic-metal/ |
| Scaleway Instances concepts | current, undated | https://www.scaleway.com/en/docs/instances/concepts |
| Contabo KB on nested virtualization | last updated 2025-04-10 | https://help.contabo.com/en/support/solutions/articles/103000271595-can-i-setup-nested-virtualization-on-my-server- |
| Contabo virtualization product page | current, undated | https://contabo.com/en-us/virtualization/ |
| Contabo KB on Hyper-V in a VDS | last updated 2025-04-10 | https://contabo.com/blog/kb/103000270408-why-am-i-unable-to-create-virtual-machines-with-hyper-v-on-a-virtual-dedicated-server/ |
| netcup help centre, Servers docs | current, undated | https://www.netcup.com/en/helpcenter/documentation/server |
| Hostinger support article | "Updated 4 days ago" as of 2026-11-02 | https://www.hostinger.com/support/10429687-is-nested-virtualization-supported-in-hostinger/ |
| AWS EC2 nested virtualization doc | current (undated page) | https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html |
| GCP nested virtualization overview | current (undated page) | https://cloud.google.com/compute/docs/instances/nested-virtualization/overview |
| Azure nested-virt troubleshooting doc | updated 2026-07-16 | https://learn.microsoft.com/en-us/troubleshoot/azure/virtual-machines/windows/troubleshoot-vm-by-use-nested-virtualization |
| OCI Oracle Linux KVM image doc | current, undated | https://docs.oracle.com/en-us/iaas/oracle-linux/oci/index.htm |
| OCI live-migration doc | current, undated | https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/movinganinstance.htm |
| Proxmox VE wiki, Nested Virtualization | last edited 2025-12-03 | https://pve.proxmox.com/wiki/Nested_Virtualization |
| KVM wiki, Nested Guests | revision 173935 (content dated Feb 2018) | https://www.linux-kvm.org/page/Nested_Guests |
| Equinix Metal shutdown doc | current, documentation removed 2026-10-01 | https://docs.equinix.com/goodbye-metal/ |

## Implications for Quoro

- **D-0009 is sound and the research supports keeping the container fallback deferred**, but the addressable market is narrower than "VPS" suggests: of the providers checked, KVM inside a *virtual* product is documented for DigitalOcean Droplets, OVHcloud Public Cloud (with a live-migration caveat), Contabo VDS, Vultr VX1, AWS (specific families), GCP (Intel/N4D), Azure (Dv3/Dv4) and OCI Intel VM shapes — and AWS/GCP/Azure/OCI require an explicit opt-in or a specific CPU vendor that most users will not have chosen. Hetzner Cloud, Hostinger VPS, Contabo VPS and netcup explicitly refuse it, and Linode is reported to refuse it. Hetzner Cloud — the default cheap European VPS — is explicitly ruled out.
- **Onboarding must verify, not trust the product name.** A preflight (`/dev/kvm`, `vmx|svm`, then an actual Cloud Hypervisor boot) belongs in the node-agent handshake, because "KVM VPS" is not evidence. Without this, Quoro will fail at first microVM launch on a host the user believes is supported.
- **Position bare metal as the supported path for self-hosters**, and name the cheap ones that still exist: Hetzner Robot/dedicated, OVH Bare Metal, Scaleway Elastic Metal. Equinix Metal is no longer an option at all (sunset 2026-06-30) and must not appear in docs, pricing or provider lists.
- **Cloud-provider support is a per-instance-family matrix, not a boolean.** If Quoro documents AWS/GCP/Azure/OCI as "supported", it must also document `--cpu-options NestedVirtualization=enabled` (AWS) / the nested-virt flag and org constraint (GCP) / avoiding Trusted Launch (Azure) / choosing an Intel shape rather than AMD (OCI, which excludes AMD VM shapes in writing). Getting this wrong yields a "supported" host where KVM is absent.
- **OVHcloud Public Cloud is the one cheap virtual product with an explicit "yes"**, but its own FAQ says a live migration "may panic" the guest kernel. That makes it acceptable for a lab or a failover-tolerant pool, not for a workload Quoro promises to keep up across host maintenance.
- **Nested virtualization is slower** (GCP states 10%+ CPU degradation; Proxmox warns nesting blocks live migration), so a nested test lab is not valid for performance benchmarks.
- **A Proxmox-based test host needs `kvm_intel nested=Y` / `kvm_amd nested=1` on the physical host** in addition to CPU type `host` in the guest. If lab VMs show no `vmx`/`svm`, this is the fix, and it needs a module reload (and possibly a host reboot), i.e. it touches the shared host and should be scheduled with whoever owns it.
- **No accepted decision rests on a false premise here**, but D-0009's "VPS with nested virt" should be read as "a small, specific set of VPS products", not as a general category.

## Open risks and unknowns

- **Still "not found" after a dedicated search pass** (treat as unknown, not as "no"): Vultr Cloud Compute and Optimized Cloud Compute, Scaleway Instances, IONOS VPS/Cloud Servers, UpCloud Cloud Servers. Queries run per provider: `"<provider> nested virtualization"`, `"<provider> /dev/kvm"`, `"<provider> vmx"`, `"<provider> run KVM inside VPS"`, `"site:<provider docs domain> nested virtualization"`, plus `"site:<provider docs domain> nested"`. For Vultr the whole docs index (`docs.vultr.com/llms.txt`) was fetched and contains no nested-virtualization entry; for UpCloud the Cloud Servers docs index was fetched and contains no occurrence of "nested". Absence of documentation is not proof of refusal.
- **Akamai/Linode is Hypothesis, not Confirmed.** Three separate official-looking answers ("no option to enable nested virtualization for a Linode", "nested virtualization disabled by default", "we don't support nested virtualization at the moment") live on `linode.com/community` and `akamai.com/cloud/community`, and every one of them returns HTTP 403 to a direct fetch. Only search snippets were available.
- **DigitalOcean's "yes" is thin.** It rests on a 2021 community answer by a DO support engineer plus the live-migration doc; there is no DO docs page that says "nested virtualization is supported". The Intel/AMD split has no DO source at all and must be treated as folklore until tested per region/CPU.
- **OVHcloud's VPS (as opposed to Public Cloud) answer is marketing, not a statement.** The KVM VPS page's "launch multiple virtual machines" line may simply describe how the VPS itself is built. Do not promise nesting on OVH VPS without a `/dev/kvm` test.
- Hetzner's statement is from 2024-09-12 and covers Cloud only. Hetzner could change it, and dedicated servers were not verified either way.
- Contabo VDS supports nesting, but Contabo's own KB says Hyper-V inside a Windows VDS is broken; only KVM/Proxmox/XenServer paths are safe.
- netcup's refusal is phrased in AMD terms ("SVM flag"); an Intel-based netcup host was not checked, and netcup's own docs page covers root servers too without confirming nesting there.
- Azure's supported-series list is not enumerated in one authoritative page any more; Dv3/Dv4 are confirmed only as *examples*. Other series (Ev3, Fsv2, Mv2) are unverified, and Trusted Launch blocks nesting.
- OCI's docs confirm Intel VM shapes and exclude AMD VM shapes, but say nothing about Ampere A1 (Arm). A1 is a Hypothesis, and the linked Oracle blog posts (blogs.oracle.com) return HTTP 403, so the how-to detail was not read.
- Actual performance overhead per workload is not measured; the 10% figure is GCP's, on GCP hardware, and may be optimistic or pessimistic elsewhere.
- Cloud Hypervisor's behaviour under nested KVM (not just "KVM boots") was not tested here; some features (e.g. specific CPU features, MSR handling) may differ from bare metal.

## Sources

1. https://docs.hetzner.com/cloud/servers/faq/ (accessed 2026-11-02)
2. https://docs.digitalocean.com/products/droplets/details/live-migration/ (accessed 2026-11-02)
3. https://www.digitalocean.com/community/questions/does-digitalocean-support-kvm-or-nested-virtulzation (accessed 2026-11-02)
4. https://docs.vultr.com/llms.txt (accessed 2026-11-02)
5. https://blogs.vultr.com/cpus-workhorse-agentic-ai-infrastructure (accessed 2026-11-02)
6. https://docs.vultr.com/products/compute/instances/bare-metal/provisioning.md (accessed 2026-11-02)
7. https://www.ovhcloud.com/en/vps/vps-kvm/ (accessed 2026-11-02)
8. https://docs.ovhcloud.com/en/guides/public-cloud/cross-functional/faq-pci (accessed 2026-11-02)
9. https://www.scaleway.com/en/docs/instances/concepts (accessed 2026-11-02)
10. https://www.scaleway.com/en/docs/tutorials/install-kvm-elastic-metal/ (accessed 2026-11-02)
11. https://help.contabo.com/en/support/solutions/articles/103000271595-can-i-setup-nested-virtualization-on-my-server- (accessed 2026-11-02)
12. https://contabo.com/en-us/virtualization/ (accessed 2026-11-02)
13. https://contabo.com/blog/kb/103000270408-why-am-i-unable-to-create-virtual-machines-with-hyper-v-on-a-virtual-dedicated-server/ (accessed 2026-11-02)
14. https://www.netcup.com/en/helpcenter/documentation/server (accessed 2026-11-02)
15. https://www.hostinger.com/support/10429687-is-nested-virtualization-supported-in-hostinger/ (accessed 2026-11-02)
16. https://upcloud.com/docs/products/cloud-servers/ (accessed 2026-11-02)
17. https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html (accessed 2026-11-02)
18. https://cloud.google.com/compute/docs/instances/nested-virtualization/overview (accessed 2026-11-02)
19. https://learn.microsoft.com/en-us/troubleshoot/azure/virtual-machines/windows/troubleshoot-vm-by-use-nested-virtualization (accessed 2026-11-02)
20. https://docs.oracle.com/en-us/iaas/oracle-linux/oci/index.htm (accessed 2026-11-02)
21. https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/movinganinstance.htm (accessed 2026-11-02)
22. https://pve.proxmox.com/wiki/Nested_Virtualization (accessed 2026-11-02)
23. https://www.linux-kvm.org/page/Nested_Guests (accessed 2026-11-02)
24. https://docs.equinix.com/goodbye-metal/ (accessed 2026-11-02)

Not fetchable, cited only for a Hypothesis-level claim: https://www.akamai.com/cloud/community/questions/17491/how-do-i-enable-nested-virtualization-for-my-linode (HTTP 403), https://www.linode.com/community/questions/25027/kvm-not-available (HTTP 403), https://blogs.oracle.com/linux/kvm-nested-virtualization-in-oci (HTTP 403).
