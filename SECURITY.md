# Security policy

## Supported versions

Security fixes are applied to the latest released semver line. Consumers must
pin an immutable release tag; `main`, pseudo-versions, and uncommitted changes
are not supported production versions.

| Version | Security updates |
|---|---|
| Latest release | Yes |
| Older releases | Best effort; upgrade may be required |
| `main` / unreleased | No production support |

## Reporting a vulnerability

Use GitHub's private vulnerability reporting or Security Advisory flow for
this repository. Do not open a public issue with exploit details, credentials,
internal endpoints, medical payloads, patient identifiers, or other sensitive
data. If private reporting is unavailable, contact a repository maintainer
through their verified GitHub profile only to arrange a private channel; do not
send vulnerability details publicly.

Include the affected release, package and exported API, impact, minimal
reproduction, and any suggested mitigation. Use synthetic data and redact all
secrets and internal infrastructure details.

Maintainers will acknowledge the report, validate scope, coordinate a fix and
release, and publish an advisory when disclosure is safe. Response and release
timing depends on severity and reproducibility; no fixed remediation window is
promised by this policy.

## Scope

Reports are especially useful for credential or sensitive-data disclosure,
TLS/authentication bypass, unbounded resource consumption, unsafe retry or
message-loss behavior, dependency vulnerabilities reachable through public
APIs, and concurrency defects that affect integrity or availability.
