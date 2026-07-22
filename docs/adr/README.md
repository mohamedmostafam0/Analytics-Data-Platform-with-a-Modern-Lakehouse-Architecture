# Architecture decision records

ADRs record material platform decisions that must survive across migration
sessions. Status values are `Proposed`, `Accepted`, or `Superseded`. New decisions
use the next numeric prefix and link back to the migration plan when relevant.

| ADR | Status | Decision |
| --- | --- | --- |
| [001](001-kubernetes-packaging-strategy.md) | Accepted | Helm packaging with a thin platform chart |
| [002](002-feature-flag-strategy.md) | Accepted | Central feature flags and dependency validation |
| [003](003-compose-coexistence.md) | Accepted | Preserve Compose during phased migration |
| [004](004-stateful-service-strategy.md) | Accepted | Prefer maintained charts/operators for stateful systems |
| [005](005-secrets-management-strategy.md) | Accepted | Secret references now; external management later |
| [006](006-first-vertical-slice.md) | Accepted | PostgreSQL-backed item seeding slice |
| [007](007-local-kubernetes-distribution.md) | Accepted | kind for the disposable local cluster |
