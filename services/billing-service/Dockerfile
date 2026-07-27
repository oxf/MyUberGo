FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install git (needed for private/public modules)
RUN apk add --no-cache git

# Copy go mod files from the repo root (build context is repo root)
# and bring in the local `contracts` module used by a replace directive.
COPY services/billing-service/go.mod services/billing-service/go.sum ./
COPY services/contracts /contracts
RUN go mod download

# Copy the billing-service source (cmd and internal packages). Use the whole
# directory, not just ./cmd — a cmd-only COPY is what broke auth-service's
# image build once internal/ packages existed (see CLAUDE.md).
COPY services/billing-service .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -o app ./cmd

# ---------- runtime stage ----------
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/app /app/app

USER nonroot:nonroot

EXPOSE 8005

ENTRYPOINT ["/app/app"]
