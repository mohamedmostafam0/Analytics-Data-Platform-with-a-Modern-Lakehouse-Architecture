# Kubernetes bootstrap resources

`namespaces/lakehouse-platform.yaml` is a raw, cluster-independent namespace
definition for tools that do not use Helm. The Helm chart creates the equivalent
namespace by default. Choose one ownership path for a cluster; do not alternate
between `kubectl` and Helm for the same object.

The approved Phase 2 and Phase 3A helpers create this namespace before local
Secrets. Runtime Helm commands therefore set `namespace.create=false`, preserving
one ownership path and avoiding an ownership conflict with the raw object.

The chart adds only the NetworkPolicies relevant to enabled features. It creates
no service account, role, binding, or Secret. External Secrets are created locally
by scripts that validate existing key/type contracts and neither print nor rotate
credential data. Phase 3A's main PostgreSQL and local object storage use retained
PVCs and are installed in the separate `lakehouse-storage` Helm release.

## Enforcement caveats

The accepted slices render and install NetworkPolicy resources, but API acceptance
does not prove traffic enforcement. The local kind configuration does not select
or validate a policy-capable CNI. A production environment must install and test
an enforcing CNI with explicit allowed and denied connection checks.

Kubernetes Secret objects in the local cluster are an integration mechanism, not
the final secret-management design. Production requires encryption at rest,
audited external secret delivery or an equivalent managed mechanism, rotation,
and workload-specific access controls. Likewise, retained local PVCs prove restart
semantics only; they do not provide replicated storage, snapshots, off-cluster
backup, or disaster recovery.
