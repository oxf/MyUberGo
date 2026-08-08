-- Outbox workers now claim a batch via a lease (claimed_until) so a crashed
-- worker's in-flight row is recoverable once the lease elapses, instead of
-- relying solely on FOR UPDATE SKIP LOCKED for the life of the process.
-- IF NOT EXISTS: on a fresh database, 0003/0004/0006's CREATE TABLE already
-- declares this column, so this migration is only a real change on a
-- database whose outbox_message tables predate that.
ALTER TABLE ride.outbox_message ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ;
ALTER TABLE driver.outbox_message ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ;
ALTER TABLE billing.outbox_message ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ;
