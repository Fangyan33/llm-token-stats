FROM golang:1.25.0-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /observer ./cmd/observer
FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /observer /usr/local/bin/observer
COPY dist/gateway-observer-v1.wasm /usr/share/metering/wasm/gateway-observer-v1.wasm
RUN chmod 0755 /usr/share/metering /usr/share/metering/wasm \
    && chmod 0444 /usr/share/metering/wasm/gateway-observer-v1.wasm
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/observer"]
