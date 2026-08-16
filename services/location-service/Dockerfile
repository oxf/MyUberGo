FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install git (needed for private/public modules)
RUN apk add --no-cache git

# Copy go mod files from the repo root (build context is repo root)
# and bring in the local `contracts` module used by a replace directive.
COPY services/location-service/go.mod services/location-service/go.sum ./
COPY services/contracts /contracts
COPY services/observability /observability
COPY services/common /common
RUN go mod download

# Copy the command source and internal packages
COPY services/location-service/cmd ./cmd
COPY services/location-service/internal ./internal

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -o app ./cmd

# ---------- runtime stage ----------
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/app /app/app

# Run as non-root (template uses nonroot user)
USER nonroot:nonroot

EXPOSE 8004

ENTRYPOINT ["/app/app"]
