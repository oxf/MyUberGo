package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"

	contracts "github.com/oxf/MyUber/contracts/http"
)

var db *sql.DB
var jwtSecret []byte
var authExp time.Duration
var refreshExp time.Duration

type User struct {
	ID    int    `json:"id"`
	Email string `json:"email"`
}

func main() {
	dsn := getenv("PG_DSN", "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable")
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}

	jwtSecret = []byte(getenv("JWT_SECRET", "secret_change_me"))
	aExpMin := atoi(getenv("AUTH_TOKEN_EXP_MIN", "15"))
	rExpHour := atoi(getenv("REFRESH_TOKEN_EXP_HOUR", "24"))
	authExp = time.Duration(aExpMin) * time.Minute
	refreshExp = time.Duration(rExpHour) * time.Hour

	mux := http.NewServeMux()
	mux.HandleFunc("POST /signup", signupHandler)
	mux.HandleFunc("POST /login", loginHandler)
	mux.HandleFunc("POST /refresh", refreshHandler)

	log.Println("auth-service listening on :8000")
	log.Fatal(http.ListenAndServe(":8000", mux))
}

func signupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}

	var req contracts.SignupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 1. Validate input
	if req.Email == "" || req.Password == "" {
		http.Error(w, "invalid credentials", http.StatusBadRequest)
		return
	}

	// 2. Hash password
	hash, _ := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)

	// 3. Insert user
	var id string
	err := db.QueryRow(`
    INSERT INTO auth.user
    (
        email,
        password_hash,
        name,
        phone,
        role
    )
    VALUES
    (
        $1,
        $2,
        $3,
        $4,
        $5
    )
    RETURNING id`, req.Email, string(hash), req.Name, req.Phone, req.Role).Scan(&id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 4. Respond
	resp := contracts.SignupResponse{UserID: id}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}

	var req contracts.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 1. Fetch user
	var id int
	var hash string
	row := db.QueryRow("SELECT id, password_hash FROM \"user\" WHERE email=$1", req.Email)
	if err := row.Scan(&id, &hash); err != nil {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	// 2. Verify password
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)) != nil {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	// 3. Create tokens
	accessToken, err := createToken(id, authExp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	refreshToken, err := createToken(id, refreshExp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 4. Store refresh token
	expiresAt := time.Now().Add(refreshExp)
	_, err = db.Exec("INSERT INTO refresh_token (user_id, token, expires_at) VALUES ($1,$2,$3)", id, refreshToken, expiresAt)
	if err != nil {
		log.Println("failed store refresh", err)
	}

	json.NewEncoder(w).Encode(contracts.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int(authExp.Seconds()),
	})
}

func refreshHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var req contracts.RefreshRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 1. Validate token
	token, err := jwt.Parse(req.RefreshToken, func(t *jwt.Token) (any, error) { return jwtSecret, nil })
	if err != nil || !token.Valid {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	claims := token.Claims.(jwt.MapClaims)
	uidFloat := claims["user_id"].(float64)
	uid := int(uidFloat)

	// 2. Check token exists in DB
	var exists bool
	row := db.QueryRow("SELECT exists(SELECT 1 FROM refresh_token WHERE token=$1 AND user_id=$2 AND expires_at > now())", req.RefreshToken, uid)
	_ = row.Scan(&exists)
	if !exists {
		http.Error(w, "invalid or expired refresh token", http.StatusUnauthorized)
		return
	}

	// 3. Create new auth token
	accessToken, err := createToken(uid, authExp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(accessToken)
}

func createToken(userID int, ttl time.Duration) (string, error) {
	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(ttl).Unix(),
		"iat":     time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func atoi(s string) int { v, _ := strconv.Atoi(s); return v }
