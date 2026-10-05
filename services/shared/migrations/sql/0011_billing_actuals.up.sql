-- Actual distance/duration recorded from location-service's ride.summary.ready
-- event, alongside the quoted price billing.invoice already carries. Record
-- only — the quoted price at ride request remains binding, this never
-- re-prices an invoice (LOCATION_SPEC.md §2.4). Nullable: a summary may never
-- arrive (location-service down, or the invoice predates this column).
ALTER TABLE billing.invoice ADD COLUMN IF NOT EXISTS actual_distance_m BIGINT;
ALTER TABLE billing.invoice ADD COLUMN IF NOT EXISTS actual_duration_s INTEGER;
