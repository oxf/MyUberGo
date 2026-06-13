package main

import (
	"context"
	//"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	// "time"

	// "github.com/golang-jwt/jwt/v5"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	// "github.com/segmentio/kafka-go"

	contracts "github.com/oxf/MyUber/contracts/http"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

var db *sql.DB
var jwtSecret []byte
var kafkaBroker string

func main() {
	dsn := getenv("PG_DSN", "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable")
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}

	jwtSecret = []byte(getenv("JWT_SECRET", "secret_change_me"))
	kafkaBroker = getenv("KAFKA_BROKER", "kafka:29092")

	go startRideRequestOutboxWorker()

	http.HandleFunc("/request-ride", requestRideHandler)
	log.Println("ride-service listening on :8001")
	log.Fatal(http.ListenAndServe(":8001", nil))
}

func requestRideHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}

	var req contracts.CreateRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	uid := r.Header.Get("X-User-Id")

	// 2. Calculate parameters
	var distanceKm = 10.0
	var price = 10.0

	// 3. Create ride request
	var rideID string
	var err1 = db.QueryRow(
		`INSERT INTO ride.ride
    				(client_id,status,pickup_lat,pickup_lng,pickup_address,dest_lat,dest_lng,dest_address,estimated_price,estimated_distance_km) 
				VALUES 
				    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) 
				RETURNING id`,
		uid,
		"Requested",
		req.PickupLat,
		req.PickupLng,
		req.PickupAddress,
		req.DestLat,
		req.DestLng,
		req.DestAddress,
		price,
		distanceKm).Scan(&rideID)
	if err1 != nil {
		http.Error(w, err1.Error(), http.StatusInternalServerError)
		return
	}

	// 4. Create payload
	var rideRequestedEvent = contractsKafka.RideRequestedEvent{
		RideID:     rideID,
		ClientID:   uid,
		DistanceKm: distanceKm,
		Price:      price,
		PickupLocation: contractsKafka.LocationWithAddress{
			Latitude:  req.PickupLat,
			Longitude: req.PickupLng,
			Address:   req.PickupAddress,
		},
		DestinationLocation: contractsKafka.LocationWithAddress{
			Latitude:  req.DestLat,
			Longitude: req.DestLng,
			Address:   req.DestAddress,
		},
	}

	var payload, _ = json.Marshal(rideRequestedEvent)

	// Save event to outbox
	var outboxID string
	err := db.QueryRow(
		`INSERT INTO ride.outbox_message
			(topic,event_type,payload)
		VALUES
			($1,$2,$3::jsonb) 
		RETURNING id`,
		"ride.requested",
		"RideRequested",
		payload,
	).Scan(&outboxID)

	if err != nil {
		log.Println("outbox insert failed:", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	log.Println("outbox inserted id=", outboxID)

	json.NewEncoder(w).Encode(map[string]any{"ride_id": rideID})
}

func startRideRequestOutboxWorker() {
	for {
		err := processOutboxBatch()
		if err != nil {
			log.Println("outbox worker error:", err)
		}

		time.Sleep(2 * time.Second)
	}
}

func processOutboxBatch() error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`
		SELECT id, topic, event_type, payload
		FROM ride.outbox_message
		WHERE processed = false
		ORDER BY created_at
		LIMIT 10
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type OutboxEvent struct {
		ID        string
		Topic     string
		EventType string
		Payload   []byte
	}

	var events []OutboxEvent

	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.Topic, &e.EventType, &e.Payload); err != nil {
			return err
		}
		events = append(events, e)
	}

	// If nothing to do
	if len(events) == 0 {
		return tx.Commit()
	}

	writer := kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, e := range events {
		err := writer.WriteMessages(ctx, kafka.Message{
			Topic: e.Topic,
			Value: e.Payload,
		})
		if err != nil {
			log.Println("kafka publish failed:", e.ID, err)
			continue
		}

		_, err = tx.Exec(`
			UPDATE ride.outbox_message
			SET processed = true
			WHERE id = $1
		`, e.ID)

		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
