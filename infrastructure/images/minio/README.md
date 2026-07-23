# Local MinIO compatibility image

This image exists only for the laptop Phase 3A compatibility test. The upstream
MinIO server and operator repositories are archived, and the final security fix
(`RELEASE.2025-10-15T17-29-55Z`) was released as source without a community
container image. The Dockerfile therefore builds the exact tag/commit from a
checksum-pinned source archive with the tag's Go 1.24.8 toolchain.

| Input | Pin |
| --- | --- |
| MinIO source tag | `RELEASE.2025-10-15T17-29-55Z` |
| Source commit | `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a` |
| Source archive SHA-256 | `be6d0bd3696c3a13a35f02d3a0280b64319c67918b4501c5c3d87f96d000085c` |
| Go builder OCI index | `sha256:3d78beb141d98f42337f1252ecf2a5f20374109929a4c3f6817f9e4179cc0ae5` |
| Distroless runtime OCI index | `sha256:f7f8f729987ad0fdf6b05eeeae94b26e6a0f613bdf46feea7fc40f7bd72953e6` |
| `mc` release | `RELEASE.2025-08-13T08-35-41Z` |
| `mc` binary SHA-256 | `01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891` |

Build, run exact version checks, and load both local images with:

```bash
infrastructure/scripts/build-phase3a-images.sh
```

This is not the selected production object-storage architecture. A production
environment must use a supported S3 service or an independently maintained
distribution and must add encryption, TLS, identity integration, replication,
backups, and a vendor lifecycle policy.

The local build was version-checked and runtime-tested, but no Trivy, Grype, Syft,
or Docker Scout tool was available. Consequently, no vulnerability-scan, SBOM, or
signature claim is made for these images.
