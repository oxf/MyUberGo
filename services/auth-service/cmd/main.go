package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	mux.HandleFunc("GET /users", getUsersHandler)

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
	var id string
	var hash string
	row := db.QueryRow("SELECT id, password_hash FROM auth.\"user\" WHERE email=$1", req.Email)
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
	_, err = db.Exec("INSERT INTO auth.refresh_token (user_id, token, expires_at) VALUES ($1,$2,$3)", id, refreshToken, expiresAt)
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
	uid, ok := claims["user_id"].(string)
	if !ok || uid == "" {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	// 2. Check token exists in DB
	var exists bool
	row := db.QueryRow("SELECT exists(SELECT 1 FROM auth.refresh_token WHERE token=$1 AND user_id=$2 AND expires_at > now())", req.RefreshToken, uid)
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

	json.NewEncoder(w).Encode(contracts.RefreshResponse{
		AccessToken: accessToken,
		ExpiresIn:   int(authExp.Seconds()),
	})
}

func createToken(userID string, ttl time.Duration) (string, error) {
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

var userSortColumns = map[string]string{
	"email":     "email",
	"name":      "name",
	"role":      "role",
	"createdAt": "created_at",
}

// listParams is a validated set of paging/sorting query params. sortCol is a
// SQL column straight from a whitelist map and sortDir is "ASC"/"DESC" — the
// only values ever interpolated into ORDER BY.
type listParams struct {
	page     int
	pageSize int
	sortCol  string
	sortDir  string
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

func parseListParams(r *http.Request, sortColumns map[string]string, defaultSortKey string) (listParams, error) {
	page, err := parseIntQuery(r, "page", 1)
	if err != nil {
		return listParams{}, err
	}
	if page < 1 {
		return listParams{}, fmt.Errorf("page must be >= 1")
	}

	pageSize, err := parseIntQuery(r, "pageSize", 20)
	if err != nil {
		return listParams{}, err
	}
	if pageSize < 1 {
		return listParams{}, fmt.Errorf("pageSize must be >= 1")
	}
	if pageSize > 100 {
		return listParams{}, fmt.Errorf("pageSize cannot exceed 100")
	}

	sortKey := r.URL.Query().Get("sortBy")
	if sortKey == "" {
		sortKey = defaultSortKey
	}
	sortCol, ok := sortColumns[sortKey]
	if !ok {
		return listParams{}, fmt.Errorf("unknown sortBy %q", sortKey)
	}

	sortDir := strings.ToLower(r.URL.Query().Get("sortDir"))
	switch sortDir {
	case "":
		sortDir = "desc"
	case "asc", "desc":
	default:
		return listParams{}, fmt.Errorf("sortDir must be asc or desc")
	}

	return listParams{page: page, pageSize: pageSize, sortCol: sortCol, sortDir: strings.ToUpper(sortDir)}, nil
}

func getUsersHandler(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, userSortColumns, "createdAt")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM auth."user"`).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// password_hash is deliberately never selected here.
	query := fmt.Sprintf(`
		SELECT id, email, name, phone, role, created_at
		FROM auth."user"
		ORDER BY %s %s
		LIMIT $1 OFFSET $2
	`, params.sortCol, params.sortDir)

	rows, err := db.Query(query, params.pageSize, (params.page-1)*params.pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	users := []contracts.UserDto{}
	for rows.Next() {
		var u contracts.UserDto
		var createdAt time.Time
		if err := rows.Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.Role, &createdAt); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		u.CreatedAt = createdAt.Format(time.RFC3339)
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(contracts.PagedResponse[contracts.UserDto]{
		Items:      users,
		Page:       params.page,
		PageSize:   params.pageSize,
		TotalCount: total,
	})
}
