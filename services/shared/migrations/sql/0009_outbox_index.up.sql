-- Supports GetUnprocessedBatch's hot claim query (previously only indexed on the
-- low-cardinality processed boolean). NOW() can't go in a static index predicate.
CREATE INDEX IF NOT EXISTS idx_ride_outbox_unprocessed ON ride.outbox_message(created_at) WHERE processed = false;
CREATE INDEX IF NOT EXISTS idx_driver_outbox_unprocessed ON driver.outbox_message(created_at) WHERE processed = false;
CREATE INDEX IF NOT EXISTS idx_billing_outbox_unprocessed ON billing.outbox_message(created_at) WHERE processed = false;
