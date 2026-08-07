package otelinit

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestSetup_OTELSDKDisabled_ReturnsZeroProvidersWithoutError(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	// Point exporters at a definitely-unreachable endpoint too, so this
	// test still passes for the right reason (the early return, not a
	// coincidentally-fast local exporter dial) if the disabled check ever
	// regresses.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")

	providers, err := Setup(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if providers.Tracer != nil || providers.Meter != nil || providers.Logger != nil {
		t.Errorf("expected all-nil Providers when OTEL_SDK_DISABLED=true, got %+v", providers)
	}

	// Must be a safe no-op, not a nil-pointer panic.
	if err := providers.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on disabled Providers should be a no-op, got: %v", err)
	}
}

func TestDurationViews_CoversKnownHighVolumeMetrics(t *testing.T) {
	views := durationViews()
	if len(views) == 0 {
		t.Fatal("expected at least one view")
	}
	// Every view must actually match at least one of the metric names this
	// repo emits — a typo in the Instrument.Name pattern would silently
	// produce a view that matches nothing, leaving the default
	// millisecond-shaped buckets in effect (the exact bug this fixes).
	wantMatch := []string{
		"myubergo.command.duration",
		"myubergo.query.duration",
		"myubergo.ride.time_to_match",
		"myubergo.ride.estimated_fare_minor",
		"myubergo.payment.amount_minor",
	}
	for _, name := range wantMatch {
		matched := false
		for _, v := range views {
			// sdkmetric.View doesn't expose its match predicate, so exercise
			// it the same way the SDK does: apply it to a minimal Instrument
			// and see whether the resulting Stream carries our aggregation.
			stream, ok := v(sdkmetric.Instrument{Name: name, Kind: sdkmetric.InstrumentKindHistogram})
			if ok && stream.Aggregation != nil {
				matched = true
			}
		}
		if !matched {
			t.Errorf("no view matched metric %q — it will fall back to the millisecond-shaped default buckets", name)
		}
	}
}
