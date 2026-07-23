FROM scratch AS download
ADD --checksum=sha256:01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891 \
  https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2025-08-13T08-35-41Z \
  /mc

FROM busybox:1.37.0-musl@sha256:222ad6d973c0d198014546a65cd02c5fdedcc172123c5b4c2bf0af636550bd94
LABEL org.opencontainers.image.title="Lakehouse local MinIO client image" \
      org.opencontainers.image.source="https://github.com/minio/mc" \
      org.opencontainers.image.version="RELEASE.2025-08-13T08-35-41Z"
COPY --from=download --chmod=0555 /mc /usr/bin/mc
USER 65532:65532
ENTRYPOINT ["/usr/bin/mc"]
