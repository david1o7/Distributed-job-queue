# -------- build stage --------
FROM golang:1.25-bookworm AS builder

WORKDIR /src

# Cache modules
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary (required for distroless/static)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# -------- runtime stage --------
# static = no libc, no shell; perfect for pure Go
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/server /app/server

# Optional: if you ever need CA certs for outbound HTTPS,
# static-debian12 already includes them.

USER nonroot:nonroot

EXPOSE 8080

# Distroless has no shell — entrypoint is the binary itself
ENTRYPOINT ["/app/server"]