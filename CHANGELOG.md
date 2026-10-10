# Changelog

All notable changes are documented here. This project follows semantic
versioning; unreleased APIs may still change before their first tagged release.

## Unreleased

## v0.2.0 - 2026-10-10

### Added

- Bounded `cache` contract with memory and Redis implementations.
- Redis Streams broker with consumer groups, stale-message reclaim, atomic DLQ
  settlement, reconnect backoff, and consumer-aware readiness.
- Production implementation guide and reusable AI-agent integration rules.
- Plain and verified-TLS/authenticated Redis integration jobs.
- Security reporting policy.

### Changed

- Redis queue production validation can validate `broker.Options`, including a
  positive handler timeout and safe `ClaimMinIdle` relationship.
- Redis deployment guidance now covers `noeviction`, persistence, HA,
  least-privilege ACLs, capacity/retention, backup/restore, and failover tests.

## v0.1.0 - 2026-10-10

### Added

- Initial public release of production-hardened logger, HTTP client, database,
  and broker foundations.
