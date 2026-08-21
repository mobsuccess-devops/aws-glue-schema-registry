# Security Policy

This repository is a fork of [`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry),
maintained by Mobsuccess. **Do not report vulnerabilities found here to AWS.** The fork
carries its own build, its own dependency set and its own code — the JSON `className`
allowlist and the cross-account `assumeRole` path, among others, exist only here — so a
report routed to Amazon reaches people who cannot act on it.

## Supported versions

Only the latest release is supported. Fixes land on `master` and ship in the next release;
there are no backport branches.

| Version        | Supported |
| -------------- | --------- |
| Latest release | yes       |
| Anything older | no        |

## Reporting a vulnerability

Use GitHub's private vulnerability reporting on this repository:
[**Report a vulnerability**](https://github.com/mobsuccess-devops/aws-glue-schema-registry/security/advisories/new).
The report stays private between you and the maintainers until an advisory is published.

If that page is not reachable, email <jean-vincent.d-adda@mobsuccess.com> with `SECURITY` in
the subject rather than opening an issue.

**Please do not open a public issue or pull request for a security problem**, and please do
not disclose it publicly before a fix is available.

A useful report contains:

- the affected artifact and version — for example `com.mobsuccess:schema-registry-serde:2.0.0`;
- what an attacker gains, and what they need in order to get it;
- a reproducer: a minimal payload, schema or configuration, and the observed behaviour;
- the JVM and the Kafka, Connect or Flink version in use, when they matter.

## What to expect

- **Acknowledgement** within 5 working days.
- **An assessment** — accepted, or explained if declined — within 15 working days.
- **A fix and a published advisory** as soon as the severity warrants, with credit to the
  reporter unless you ask otherwise.

This is a best-effort policy from a small team; it carries no contractual commitment and no
bug bounty.

## Vulnerabilities in the upstream project

A flaw in code inherited unchanged from upstream affects the AWS artifact too. Report it
here so the fork can be fixed, and expect the maintainers to forward it to
[AWS security](https://aws.amazon.com/security/vulnerability-reporting/) — upstream is
dormant, so a report sent only to Amazon is unlikely to reach this fork's users.

## Vulnerabilities in dependencies

A published CVE in a third-party dependency is not a private matter — open a normal public
issue. Dependabot alerts and security updates are enabled on this repository, so the bump
may already be in flight.
