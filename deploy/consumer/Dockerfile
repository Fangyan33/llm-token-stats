FROM golang:1.25.0-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /consumer ./cmd/consumer
FROM debian:bookworm-slim
COPY --from=build /consumer /usr/local/bin/consumer
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/consumer"]
