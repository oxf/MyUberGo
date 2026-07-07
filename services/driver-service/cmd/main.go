package main

import (
	"database/sql"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	"driver-service/internal/infrastructure/health"
	"driver-service/internal/infrastructure/metrics"
	"driver-service/internal/infrastructure/shutdown"
	"driver-service/internal/interfaces/http/handler"
	"driver-service/internal/persistence"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
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

	profileRepo := persistence.NewPostgresDriverProfileRepository(db)
	shiftRepo := persistence.NewPostgresShiftRepository(db)

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

	application := app.Application{
		Commands: app.Commands{
			CreateDriverProfile: command.NewCreateDriverProfileHandler(profileRepo, logger, metricsClient),
			UpdateDriverProfile: command.NewUpdateDriverProfileHandler(profileRepo, logger, metricsClient),
			CreateShift:         command.NewCreateShiftHandler(shiftRepo),
			UpdateShift:         command.NewUpdateShiftHandler(shiftRepo, logger, metricsClient),
		},
		Queries: app.Queries{
			GetDriverList: query.NewGetDriverListHandler(profileRepo, logger, metricsClient),
			GetDriverByID: query.NewGetDriverByIDHandler(profileRepo, logger, metricsClient),
			GetShiftList:  query.NewGetShiftListHandler(shiftRepo, logger, metricsClient),
			GetShiftByID:  query.NewGetShiftByIDHandler(shiftRepo, logger, metricsClient),
		},
	}

	profileHandler := handler.NewDriverProfileHandler(application)
	shiftHandler := handler.NewShiftHandler(application)

	// Initialize health checker
	healthChecker := health.NewChecker(db, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// API endpoints
	mux.HandleFunc("POST /driver-profile", profileHandler.Create)
	mux.HandleFunc("PUT /driver-profile/{id}", profileHandler.Update)
	mux.HandleFunc("GET /driver-profile", profileHandler.GetList)
	mux.HandleFunc("GET /driver-profile/{id}", profileHandler.GetByID)
	mux.HandleFunc("POST /driver-shift/create", shiftHandler.Create)
	mux.HandleFunc("PUT /driver-shift/{id}", shiftHandler.Update)
	mux.HandleFunc("GET /driver-shift", shiftHandler.GetList)
	mux.HandleFunc("GET /driver-shift/{id}", shiftHandler.GetByID)

	// Create HTTP server
	server := &http.Server{
		Addr:         ":8003",
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Start server in a goroutine
	go func() {
		log.Println("driver-service listening on :8003")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
		}
	}()

	// Wait for shutdown signal and perform graceful shutdown
	shutdownManager.WaitForShutdown()
}

func requestCreateDriverHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req contracts.CreateDriverProfileDto
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// VALIDATION
	if isUUIDEmpty(req.UserId) {
		writeError(w, "userId is required", http.StatusBadRequest)
		return
	}
	if req.DriverName == "" {
		writeError(w, "driverName is required", http.StatusBadRequest)
		return
	}
	if req.Phone == "" {
		writeError(w, "phone is required", http.StatusBadRequest)
		return
	}

	var driverId string

	err := db.QueryRow(`
		INSERT INTO driver.driver_profile
		(user_id, name, phone, vehicle_type, license_plate)
		VALUES ($1,$2,$3,$4,$5)
		RETURNING id
	`,
		req.UserId,
		req.DriverName,
		req.Phone,
		req.VehicleType,
		req.LicencePlate,
	).Scan(&driverId)

	if err != nil {
		log.Println("failed to insert driver_profile:", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, contracts.CreateDriverProfileResponse{
		Id: driverId,
	})
}

func requestUpdateDriverHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.PathValue("id")
	if isUUIDEmpty(id) {
		writeError(w, "missing id", http.StatusBadRequest)
		return
	}

	var req contracts.UpdateDriverProfileDto
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// simple validation
	if req.DriverName == "" && req.Phone == "" && req.VehicleType == "" && req.LicencePlate == "" {
		writeError(w, "nothing to update", http.StatusBadRequest)
		return
	}

	_, err := db.Exec(`
		UPDATE driver.driver_profile
		SET
			name = COALESCE(NULLIF($1,''), name),
			phone = COALESCE(NULLIF($2,''), phone),
			vehicle_type = COALESCE(NULLIF($3,''), vehicle_type),
			license_plate = COALESCE(NULLIF($4,''), license_plate)
		WHERE id = $5
	`,
		req.DriverName,
		req.Phone,
		req.VehicleType,
		req.LicencePlate,
		id,
	)

	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{"id": id})
}

func requestGetDriverListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	page := 0
	pageSize := 10

	if v := r.URL.Query().Get("page"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 0 {
			writeError(w, "invalid page", http.StatusBadRequest)
			return
		}
		page = p
	}

	if v := r.URL.Query().Get("pageSize"); v != "" {
		ps, err := strconv.Atoi(v)
		if err != nil || ps <= 0 || ps > 100 {
			writeError(w, "invalid pageSize", http.StatusBadRequest)
			return
		}
		pageSize = ps
	}

	offset := page * pageSize

	rows, err := db.Query(`
		SELECT id, user_id, name, phone, rating,
		       vehicle_type, license_plate, status,
		       total_rides_completed, created_at
		FROM driver.driver_profile
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`, pageSize, offset)

	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []contracts.DriverProfileDto

	for rows.Next() {
		var d contracts.DriverProfileDto
		var createdAt time.Time

		err := rows.Scan(
			&d.Id,
			&d.UserId,
			&d.DriverName,
			&d.Phone,
			&d.Rating,
			&d.VehicleType,
			&d.LicencePlate,
			&d.Status,
			&d.TotalRidesCompleted,
			&createdAt,
		)

		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		d.CreatedAt = createdAt.Format(time.RFC3339)
		result = append(result, d)
	}

	writeJSON(w, result)
}

func requestGetDriverByIdHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.PathValue("id")
	if isUUIDEmpty(id) {
		writeError(w, "missing id", http.StatusBadRequest)
		return
	}

	row := db.QueryRow(`
		SELECT
			id,
			user_id,
			name,
			phone,
			rating,
			vehicle_type,
			license_plate,
			status,
			total_rides_completed,
			created_at
		FROM driver.driver_profile
		WHERE id = $1
	`, id)

	var driver contracts.DriverProfileDto
	var createdAt time.Time

	err := row.Scan(
		&driver.Id,
		&driver.UserId,
		&driver.DriverName,
		&driver.Phone,
		&driver.Rating,
		&driver.VehicleType,
		&driver.LicencePlate,
		&driver.Status,
		&driver.TotalRidesCompleted,
		&createdAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, "not found", http.StatusNotFound)
			return
		}
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	driver.CreatedAt = createdAt.Format(time.RFC3339)

	writeJSON(w, driver)
}

func requestCreateShiftHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req contracts.CreateShiftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.DriverId == "" {
		writeError(w, "driverId required", http.StatusBadRequest)
		return
	}

	// prevent multiple active shifts
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM driver.shift
			WHERE driver_id = $1 AND ended_at IS NULL
		)
	`, req.DriverId).Scan(&exists)

	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if exists {
		writeError(w, "active shift already exists", http.StatusConflict)
		return
	}

	var shiftId string

	err = db.QueryRow(`
		INSERT INTO driver.shift (driver_id)
		VALUES ($1)
		RETURNING id
	`, req.DriverId).Scan(&shiftId)

	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, contracts.CreateShiftResponse{Id: shiftId})
}
func requestGetDriverShiftListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query(`
		SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
		FROM driver.shift
		ORDER BY started_at DESC
	`)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []contracts.ShiftDto

	for rows.Next() {
		var s contracts.ShiftDto
		var startedAt, endedAt sql.NullTime

		err := rows.Scan(
			&s.Id,
			&s.DriverId,
			&startedAt,
			&endedAt,
			&s.TotalRides,
			&s.TotalEarnings,
		)

		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.Status = "Finished"
		if !endedAt.Valid {
			s.Status = "Active"
		}

		s.StartedAt = startedAt.Time.Format(time.RFC3339)
		if endedAt.Valid {
			s.EndedAt = endedAt.Time.Format(time.RFC3339)
		}

		result = append(result, s)
	}

	writeJSON(w, result)
}

func requestGetDriverShiftByIdHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeError(w, "missing id", http.StatusBadRequest)
		return
	}

	var s contracts.ShiftDto
	var startedAt, endedAt sql.NullTime

	err := db.QueryRow(`
		SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
		FROM driver.shift
		WHERE id = $1
	`, id).Scan(
		&s.Id,
		&s.DriverId,
		&startedAt,
		&endedAt,
		&s.TotalRides,
		&s.TotalEarnings,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, "not found", http.StatusNotFound)
			return
		}
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.Status = "Finished"
	if !endedAt.Valid {
		s.Status = "Active"
	}

	s.StartedAt = startedAt.Time.Format(time.RFC3339)
	if endedAt.Valid {
		s.EndedAt = endedAt.Time.Format(time.RFC3339)
	}

	writeJSON(w, s)
}

func requestGetDriverShiftListByDriverIdHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	driverId := r.URL.Query().Get("driverId")
	if driverId == "" {
		writeError(w, "driverId required", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(`
		SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
		FROM driver.shift
		WHERE driver_id = $1
		ORDER BY started_at DESC
	`, driverId)

	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []contracts.ShiftDto

	for rows.Next() {
		var s contracts.ShiftDto
		var startedAt, endedAt sql.NullTime

		err := rows.Scan(
			&s.Id,
			&s.DriverId,
			&startedAt,
			&endedAt,
			&s.TotalRides,
			&s.TotalEarnings,
		)

		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.Status = "Finished"
		if !endedAt.Valid {
			s.Status = "Active"
		}

		s.StartedAt = startedAt.Time.Format(time.RFC3339)
		if endedAt.Valid {
			s.EndedAt = endedAt.Time.Format(time.RFC3339)
		}

		result = append(result, s)
	}

	writeJSON(w, result)
}

func requestUpdateShiftHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeError(w, "missing id", http.StatusBadRequest)
		return
	}

	var req contracts.UpdateShiftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// END SHIFT
	if req.Status == "Ended" {
		_, err := db.Exec(`
			UPDATE driver.shift
			SET ended_at = NOW()
			WHERE id = $1 AND ended_at IS NULL
		`, id)

		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, contracts.UpdateShiftResponse{
			Id:     id,
			Status: "Ended",
		})
		return
	}

	writeJSON(w, contracts.UpdateShiftResponse{
		Id:     id,
		Status: "Active",
	})
}

func writeError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func isUUIDEmpty(s string) bool {
	return s == ""
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

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
