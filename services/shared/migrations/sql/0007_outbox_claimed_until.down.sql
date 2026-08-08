ALTER TABLE ride.outbox_message DROP COLUMN IF EXISTS claimed_until;
ALTER TABLE driver.outbox_message DROP COLUMN IF EXISTS claimed_until;
ALTER TABLE billing.outbox_message DROP COLUMN IF EXISTS claimed_until;
