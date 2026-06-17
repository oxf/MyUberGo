package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

var db *sql.DB
var kafkaBroker string

func main() {
	dsn := getenv("PG_DSN", "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable")
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}

	kafkaBroker = getenv("KAFKA_BROKER", "kafka:29092")

	go startRideRequestedConsumer()

	select {}
}

func startRideRequestedConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "ride.requested",
		GroupID: "matching-service",
	})

	defer reader.Close()

	log.Println("ride-requested consumer started")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("consumer error:", err)
			continue
		}

		var event contractsKafka.RideRequestedEvent

		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf(
			"Ride request received. RideID=%s ClientID=%s Price=%.2f",
			event.RideID,
			event.ClientID,
			event.Price,
		)

		if err := handleRideRequested(event); err != nil {
			log.Println("handle error:", err)
		}
	}
}

func handleRideRequested(event contractsKafka.RideRequestedEvent) error {

	// 1. Get 5 available drivers. First version - by rating TODO add ordering by location
	rows, err := db.Query(`
		SELECT id
		FROM driver.driver_profile
		WHERE status = 'Online'
		ORDER BY rating DESC
		LIMIT 5
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type driver struct {
		id string
	}

	var drivers []driver

	for rows.Next() {
		var d driver
		if err := rows.Scan(&d.id); err != nil {
			return err
		}
		drivers = append(drivers, d)
	}

	if len(drivers) == 0 {
		log.Println("no drivers available for ride:", event.RideID)
		return nil
	}

	// 2. Insert offers into DB
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, d := range drivers {

		_, err := tx.Exec(`
			INSERT INTO matching.ride_offer
			(id, ride_id, driver_id, status, expires_at, offer_rank)
			VALUES
			($1,$2,$3,'Pending',$4,$5)
		`,
			uuid.New().String(),
			event.RideID,
			d.id,
			time.Now().Add(30*time.Second),
			i,
		)

		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("created %d ride offers for ride %s", len(drivers), event.RideID)

	// 3. TODO: send notifications (later via Kafka)

	return nil
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
