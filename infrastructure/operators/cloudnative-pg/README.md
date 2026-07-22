# CloudNativePG Phase 2 pin

Phase 2 uses the official CloudNativePG `v1.29.2` release manifest and the exact
PostgreSQL image tag `ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie`.
The operator is installed in `cnpg-system`; the application Cluster lives in
`lakehouse-platform`.

| Artifact | Pin | SHA-256 |
| --- | --- | --- |
| Operator manifest | `v1.29.2/releases/cnpg-1.29.2.yaml` | `856312bb13e64c5b03861092eac9045e45f7b7d601aafe75a9016260a762ac8a` |
| PostgreSQL image | `18.4-system-trixie` | OCI index `9287ce030c6f3ce822e383b019ae4aaf1e8370bff3b39f9c51dc10d69dc97219` |

`infrastructure/scripts/install-cloudnative-pg.sh` downloads the manifest from
the immutable release tag, checks the committed digest, applies it server-side,
and waits for the controller. It does not install a second database operator or
create application credentials.

The Phase 2 NetworkPolicy permits the operator namespace to reach instance-manager
port 8000. The application Job can reach only DNS and PostgreSQL port 5432.
