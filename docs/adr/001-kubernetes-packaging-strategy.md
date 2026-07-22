# ADR-001: Kubernetes packaging strategy

## Status

Accepted

## Context

The platform has 29 Compose services, many optional and several operationally
complex. Profiles and component dependency validation are required. There was no
existing Kubernetes, Kustomize, Helm, or GitOps convention.

## Decision

Use a Helm v3-compatible `apiVersion: v2` platform chart as the configuration and
validation boundary. Keep the chart thin: in-repository lightweight applications
may be templated directly, while complex stateful systems should remain separate,
pinned chart/operator releases selected by later ADRs. The foundation initially
rendered only a Namespace; Phase 2 adds the accepted minimal slice.

## Alternatives considered

- Kustomize overlays: good patching model, but weaker for a large boolean feature
  matrix and dependency failures.
- One chart containing hand-written manifests for every system: too coupled and
  would duplicate upstream lifecycle expertise.
- Independent charts without a platform control chart: weak profile-level
  validation and discoverability.

## Consequences

Feature policy has one entry point and environment overlays stay small. Stateful
releases may have separate lifecycles, so future automation must coordinate them.

## Risks

An umbrella chart can become monolithic if service templates and vendor internals
are copied into it. Chart dependencies must not be added without version and
maintenance review.

## Validation

Lint and render the base values and every profile; schema-validate merged values;
assert that base values contain only the Namespace and the minimal overlay contains
exactly the accepted Phase 2 resource set.
