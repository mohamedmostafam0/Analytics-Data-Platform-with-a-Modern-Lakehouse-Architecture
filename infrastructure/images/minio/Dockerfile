# MinIO stopped publishing community container images after the final security
# release. Build that exact source revision for the local compatibility target.
FROM golang:1.24.8-alpine3.22@sha256:3d78beb141d98f42337f1252ecf2a5f20374109929a4c3f6817f9e4179cc0ae5 AS build

ARG TARGETARCH=amd64
WORKDIR /src
ADD --checksum=sha256:be6d0bd3696c3a13a35f02d3a0280b64319c67918b4501c5c3d87f96d000085c \
  https://github.com/minio/minio/archive/refs/tags/RELEASE.2025-10-15T17-29-55Z.tar.gz \
  /tmp/minio-source.tar.gz
RUN tar -xzf /tmp/minio-source.tar.gz --strip-components=1 -C /src && \
    GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux GOARCH="${TARGETARCH}" \
    go build -mod=readonly -p=2 -tags kqueue -trimpath \
      -ldflags "-s -w \
        -X github.com/minio/minio/cmd.Version=2025-10-15T17:29:55Z \
        -X github.com/minio/minio/cmd.CopyrightYear=2025 \
        -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-10-15T17-29-55Z \
        -X github.com/minio/minio/cmd.CommitID=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a \
        -X github.com/minio/minio/cmd.ShortCommitID=9e49d5e7a648" \
      -o /out/minio .

FROM gcr.io/distroless/static-debian13:nonroot@sha256:f7f8f729987ad0fdf6b05eeeae94b26e6a0f613bdf46feea7fc40f7bd72953e6
LABEL org.opencontainers.image.title="Lakehouse local MinIO compatibility image" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
      org.opencontainers.image.version="RELEASE.2025-10-15T17-29-55Z"
COPY --from=build --chmod=0555 /out/minio /usr/bin/minio
COPY --from=build /src/LICENSE /licenses/LICENSE
USER 65532:65532
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
