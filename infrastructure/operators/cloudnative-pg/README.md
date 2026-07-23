# CloudNativePG Phase 2 and Phase 3A pins

Phase 2 and Phase 3A share the official CloudNativePG `v1.29.2` operator. The
operator is installed in `cnpg-system`; both application Clusters live in
`lakehouse-platform` but remain owned by separate Helm releases.

| Artifact | Pin | SHA-256 |
| --- | --- | --- |
| Operator manifest | `v1.29.2/releases/cnpg-1.29.2.yaml` | `856312bb13e64c5b03861092eac9045e45f7b7d601aafe75a9016260a762ac8a` |
| Phase 2 PostgreSQL image | `18.4-system-trixie` | OCI index `9287ce030c6f3ce822e383b019ae4aaf1e8370bff3b39f9c51dc10d69dc97219` |
| Phase 3A PostgreSQL + extensions image | `18.4-standard-trixie` | OCI index `4e4ac3fb2c914cfb44f80f0b8be8aa550e83b80bf5220df49c3a8780c1f79bc8` |

`infrastructure/scripts/install-cloudnative-pg.sh` downloads the manifest from
the immutable release tag, checks the committed digest, applies it server-side,
and waits for the controller. It does not install a second database operator or
create application credentials.

The Phase 2 NetworkPolicy permits the operator namespace to reach instance-manager
port 8000. The application Job can reach only DNS and PostgreSQL port 5432.
Phase 3A gives the operator the same instance-manager access and does not open main
PostgreSQL client ingress until a migrated client is explicitly added.

Both current Clusters are single-instance local development resources with PDBs
disabled. Hibernation and logical restore were tested, but replication, failover,
rolling upgrade, Barman Cloud/WAL archival, PITR, backup retention, multi-zone
placement, production storage classes, and recovery objectives remain unaccepted.
