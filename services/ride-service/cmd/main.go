package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	contracts "github.com/oxf/MyUber/contracts/http"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
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

	go startRideRequestOutboxWorker()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /request-ride", requestRideHandler)
	mux.HandleFunc("GET /ride", getRideListHandler)
	mux.HandleFunc("GET /ride/{id}", getRideByIdHandler)

	log.Println("ride-service listening on :8001")
	log.Fatal(http.ListenAndServe(":8001", mux))
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

	if uid == "" {
		http.Error(w, "missing X-User-Id header", http.StatusBadRequest)
		return
	}

	// 2. Calculate parameters
	var distanceKm = 10.0
	var price = 10.0

	// 3. Start transaction
	tx, err := db.BeginTx(
		context.Background(),
		&sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
		},
	)

	if err != nil {
		log.Println("failed to begin transaction:", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	defer tx.Rollback()

	//4. Create ride request
	var rideID string
	err = tx.QueryRow(
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

	if err != nil {
		log.Println("failed to insert ride:", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// 5. Create payload
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

	if err != nil {
		log.Println("failed to serialize event:", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// 6. Save event to outbox
	var outboxID string

	err = tx.QueryRow(
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
		log.Println("failed to insert outbox message:", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	log.Println("outbox inserted id=", outboxID)

	// 7. Commit transaction
	if err := tx.Commit(); err != nil {
		log.Println("failed to commit transaction:", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"ride_id": rideID,
	}); err != nil {
		log.Println("failed to write response:", err)
	}
}

func getRideListHandler(w http.ResponseWriter, r *http.Request) {
	page, err := parseIntQuery(r, "page", 1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pageSize, err := parseIntQuery(r, "pageSize", 10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if page < 0 {
		http.Error(w, "page must be >= 0", http.StatusBadRequest)
		return
	}

	if pageSize < 1 {
		http.Error(w, "pageSize must be >= 1", http.StatusBadRequest)
		return
	}

	if pageSize > 100 {
		http.Error(w, "pageSize cannot exceed 100", http.StatusBadRequest)
		return
	}

	offset := page * pageSize

	rows, err := db.Query(`
		SELECT
			id,
			client_id,
			driver_id,
			status,
			pickup_lat,
			pickup_lng,
			pickup_address,
			dest_lat,
			dest_lng,
			dest_address,
			estimated_price,
			estimated_distance_km,
			created_at
		FROM ride.ride
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`, pageSize, offset)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var rides []contracts.RideDto

	for rows.Next() {
		var ride contracts.RideDto

		var (
			pickLat  float64
			pickLng  float64
			pickAddr string

			destLat  float64
			destLng  float64
			destAddr string
		)

		var createdAt time.Time

		err := rows.Scan(
			&ride.ID,
			&ride.ClientID,
			&ride.DriverID,
			&ride.Status,
			&pickLat,
			&pickLng,
			&pickAddr,
			&destLat,
			&destLng,
			&destAddr,
			&ride.EstimatedPrice,
			&ride.EstimatedDistanceKm,
			&createdAt,
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ride.Pickup = contracts.LocationRequest{
			Latitude:  pickLat,
			Longitude: pickLng,
			Address:   pickAddr,
		}

		ride.Destination = contracts.LocationRequest{
			Latitude:  destLat,
			Longitude: destLng,
			Address:   destAddr,
		}

		ride.CreatedAt = createdAt.Format(time.RFC3339)

		rides = append(rides, ride)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(rides); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getRideByIdHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id path", http.StatusBadRequest)
		return
	}

	var ride contracts.RideDto

	var (
		pickLat  float64
		pickLng  float64
		pickAddr string

		destLat  float64
		destLng  float64
		destAddr string

		createdAt time.Time
	)

	err := db.QueryRow(`
		SELECT
			id,
			client_id,
			driver_id,
			status,
			pickup_lat,
			pickup_lng,
			pickup_address,
			dest_lat,
			dest_lng,
			dest_address,
			estimated_price,
			estimated_distance_km,
			created_at
		FROM ride.ride
		WHERE id = $1
	`, id).Scan(
		&ride.ID,
		&ride.ClientID,
		&ride.DriverID,
		&ride.Status,
		&pickLat,
		&pickLng,
		&pickAddr,
		&destLat,
		&destLng,
		&destAddr,
		&ride.EstimatedPrice,
		&ride.EstimatedDistanceKm,
		&createdAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "ride not found", http.StatusNotFound)
			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ride.Pickup = contracts.LocationRequest{
		Latitude:  pickLat,
		Longitude: pickLng,
		Address:   pickAddr,
	}

	ride.Destination = contracts.LocationRequest{
		Latitude:  destLat,
		Longitude: destLng,
		Address:   destAddr,
	}

	ride.CreatedAt = createdAt.Format(time.RFC3339)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(ride); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func parseIntQuery(r *http.Request, key string, defaultValue int) (int, error) {
	valStr := r.URL.Query().Get(key)

	if valStr == "" {
		return defaultValue, nil
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, err
	}

	if val < 0 {
		return 0, fmt.Errorf("%s cannot be negative", key)
	}

	return val, nil
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
