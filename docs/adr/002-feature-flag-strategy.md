# ADR-002: Feature-flag strategy

## Status

Accepted

## Context

The complete platform is unsafe for normal laptop use. Compose currently has no
profiles, and service start order does not fully describe runtime dependencies.

## Decision

Centralize Kubernetes component flags under `features` in the platform chart.
Every flag defaults to false in base values. Profile files set `global.profile`
and remain workload-free until their components are migrated; Phase 2 `minimal`
explicitly enables only `sourcePostgresql` and `itemsLoadGenerator`. Helm validation
checks known dependency combinations and rejects every not-yet-implemented feature.

Profiles may override individual flags once the relevant templates are available.
The values schema rejects unknown flags and invalid types.

## Alternatives considered

- Separate values scattered beside each component: rejected because users could
  not see or validate the complete feature graph.
- Helm tags/conditions only: rejected because they do not express dependency
  failures clearly.
- Compose profiles as the Kubernetes control plane: rejected because Compose must
  remain independently operable.

## Consequences

The default chart is safe and disabled features require no secrets. Enabling a
planned feature currently fails with an explicit Phase 2 message instead of
silently rendering nothing.

## Risks

Dependency rules can drift from application behavior. Any component migration
must update values, schema, validation template, feature matrix, profile overlays,
and tests together.

## Validation

Render all profiles; test dependency failures; test the current implementation
gate; inspect base and minimal rendered resource kinds.
