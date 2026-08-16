-- Adds a 'Failed' ride status so matching-service can tell ride-service it
-- gave up on a ride after exhausting its retry attempts (docs/AUDIT_2026-08-15.md
-- #11) — previously the Postgres row stayed 'Requested' forever with no signal
-- anywhere in the system of record. 0003_ride.up.sql declared the original CHECK
-- constraint inline with no explicit name, so its Postgres-assigned name is
-- looked up dynamically rather than guessed, and dropped/replaced.
DO $$
DECLARE
    con_name text;
BEGIN
    SELECT con.conname INTO con_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = 'ride'
      AND rel.relname = 'ride'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) LIKE '%status%';

    IF con_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE ride.ride DROP CONSTRAINT %I', con_name);
    END IF;
END $$;

ALTER TABLE ride.ride ADD CONSTRAINT ride_status_check
    CHECK (status IN ('Requested', 'Matched', 'InProgress', 'Completed', 'Cancelled', 'Failed'));
