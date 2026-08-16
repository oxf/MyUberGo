-- Reverts to the original 5-status CHECK. Any row already 'Failed' would
-- violate this constraint, so this down migration only succeeds on a
-- database with no 'Failed' rides — acceptable for a dev-only rollback.
ALTER TABLE ride.ride DROP CONSTRAINT IF EXISTS ride_status_check;

ALTER TABLE ride.ride ADD CONSTRAINT ride_status_check
    CHECK (status IN ('Requested', 'Matched', 'InProgress', 'Completed', 'Cancelled'));
