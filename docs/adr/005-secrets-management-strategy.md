# ADR-005: Secrets-management strategy

## Status

Accepted

## Context

Compose reads a gitignored root `.env`, but tracked files also contain development
credential literals and some services receive the entire environment file. No
Kubernetes secret-management system exists.

## Decision

No phase commits Secret objects or credential values. Phase 2 workloads reference
a pre-created Secret name and fixed keys. Its local helper generates an application
password in a mode-0700 temporary directory, pipes the Secret directly to kubectl,
prints no value, and reuses rather than rotates an existing Secret. Before
production, select External Secrets Operator, SOPS, Sealed Secrets, or a
cloud-native secret manager through a separate ADR; do not install one during
foundation work.

## Alternatives considered

- Plain Secret manifests in Git: rejected because base64 is not encryption.
- Put credentials in Helm values: rejected because rendered releases and CI logs
  can expose them.
- Choose an external-secret controller now: premature without a target cluster or
  secret backend.

## Consequences

Base values need no secrets. The minimal profile documents local Secret creation;
disabled features do not require it.

## Risks

Existing Compose literals remain a risk until a separately approved compatibility
change removes them. Connector setup currently risks logging an expanded connector
payload after registration failure.

## Validation

Scan tracked deployment files, inspect rendered manifests, and confirm that no
committed values contain credentials or `kind: Secret` with data.
