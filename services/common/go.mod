module github.com/oxf/MyUber/common

go 1.26.2

require (
	github.com/oxf/MyUber/observability v0.0.0
	github.com/sirupsen/logrus v1.9.4
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/oxf/MyUber/observability => ../observability
