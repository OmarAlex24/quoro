# Security Policy

Quoro runs microVMs, deploys applications and hands scoped access to AI agents. Security reports are taken seriously and handled privately.

## Reporting a vulnerability

**Do not open a public issue, pull request or discussion for a security problem.**

Report it privately through GitHub Security Advisories:

1. Go to the repository's **Security** tab: https://github.com/OmarAlex24/quoro/security
2. Click **Report a vulnerability**.
3. Describe the issue in the advisory form.

Direct link: https://github.com/OmarAlex24/quoro/security/advisories/new

This keeps the report private between you and the maintainer until a fix and an advisory are ready.

### What to include

- A description of the issue and the impact you believe it has.
- The affected version, commit or date you tested, and the configuration used.
- Steps to reproduce, ideally minimal. Proof-of-concept code or a packet capture is welcome.
- Any suggested fix or mitigation, if you have one.
- How you would like to be credited in the advisory, or whether you prefer to stay anonymous.

Do not include secrets, credentials or personal data belonging to others in the report.

### What to expect

- **Acknowledgement:** within 7 days of the report, on a best-effort basis. This is a volunteer project and there is no service-level agreement.
- We will confirm the report, agree on a disclosure timeline with you, and keep you updated while a fix is developed.
- Please give us a reasonable window to ship a fix before any public disclosure.
- Credit is given in the published advisory unless you ask otherwise.

## Supported versions

None yet. Quoro has no releases. This section will list the supported versions once releases exist.

## Release integrity

Every release binary will be signed, and builds will be reproducible. Quoro is installed with a `curl | sh` installer and controls infrastructure, so an unsigned or non-reproducible release is not acceptable. Verification instructions will be published together with the first release; the signing mechanism has not been chosen yet.

## Scope

Because Quoro gives AI agents scoped, time-limited access to infrastructure, the following are in scope and treated as security issues, not feature requests:

- **Authorization and grants** — privilege escalation, bypassing a grant, extending a grant beyond its scope or lifetime, or acting after a grant expires.
- **Agent scope escapes** — an agent reaching resources, hosts or operations outside the scope it was granted.
- **Audit log integrity** — tampering with, omitting or forging audit log entries, or breaking the append-only property.
- **Isolation** — escaping a microVM, crossing the boundary between apps or tenants, or interfering with another workload.
- **The installer and release chain** — supply-chain attacks, signature or reproducibility failures.
- **Secrets and credentials** — exposure, leakage into logs or audit entries, or insecure storage.

Vulnerabilities in third-party dependencies should be reported upstream as well; tell us if Quoro's use of the dependency makes the issue exploitable here.
