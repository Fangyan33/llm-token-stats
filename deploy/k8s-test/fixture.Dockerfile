# Build context is dist/, populated by make build.
FROM alpine:latest
COPY --chmod=0555 fixture /usr/local/bin/fixture
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fixture"]
