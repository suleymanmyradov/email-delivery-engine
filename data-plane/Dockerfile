# Data plane: Go worker, built as a static binary on a minimal runtime image.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM alpine:3.20
RUN adduser -D -u 10001 worker
USER worker

COPY --from=build /out/worker /usr/local/bin/worker

EXPOSE 9100

ENTRYPOINT ["/usr/local/bin/worker"]
