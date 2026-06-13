FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git (needed for private/public modules)
RUN apk add --no-cache git

# Copy go mod files from the repo root (build context is repo root)
# and bring in the local `contracts` module used by a replace directive.
COPY services/auth-service/go.mod services/auth-service/go.sum ./
COPY services/contracts /contracts
RUN go mod download

# Copy the command source
COPY services/auth-service/cmd ./cmd

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -o app ./cmd

# ---------- runtime stage ----------
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/app /app/app

# Run as non-root (template uses nonroot user)
USER nonroot:nonroot

EXPOSE 8000

ENTRYPOINT ["/app/app"]
