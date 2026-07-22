# Kubernetes bootstrap resources

`namespaces/lakehouse-platform.yaml` is a raw, cluster-independent namespace
definition for tools that do not use Helm. The Helm chart creates the equivalent
namespace by default. Choose one ownership path for a cluster; do not alternate
between `kubectl` and Helm for the same object.

The approved Phase 2 helper creates this namespace before the local Secret. Runtime
Helm commands therefore set `namespace.create=false`, preserving one ownership
path and avoiding an ownership conflict with the raw object.

The Phase 2 chart adds NetworkPolicies only when both slice features are enabled.
It creates no service account, role, binding, or Secret. The external Secret is
created locally by a script that neither prints nor commits its generated password.
