-- ride.ride.driver_id -> driver.driver(id): deferred to its own migration
-- because the driver schema (0004) is created after the ride schema (0003).
ALTER TABLE ride.ride ADD CONSTRAINT fk_ride_driver_id FOREIGN KEY (driver_id) REFERENCES driver.driver(id);
