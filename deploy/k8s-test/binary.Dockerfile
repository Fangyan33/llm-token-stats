# Precompiled by make build. The build context is dist/.
FROM alpine:latest
ARG COMPONENT
COPY --chmod=0555 ${COMPONENT} /usr/local/bin/service
COPY --chmod=0444 gateway-observer-v1.wasm /usr/share/metering/wasm/gateway-observer-v1.wasm
RUN chmod 0755 /usr/share/metering /usr/share/metering/wasm
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/service"]
