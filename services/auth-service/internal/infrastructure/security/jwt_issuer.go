package security

import (
	"errors"
	"time"

	"auth-service/internal/application/services"

	"github.com/golang-jwt/jwt/v5"
)

// issuer is the "iss" claim Kong's jwt plugin matches against a consumer's
// configured jwt_secrets key (see gateway/kong.yml).
const issuer = "myubergo-auth"

type JWTIssuer struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
}

func NewJWTIssuer(secret []byte, accessTTL, refreshTTL time.Duration) services.TokenIssuer {
	return &JWTIssuer{secret: secret, accessTTL: accessTTL, refreshTTL: refreshTTL}
}

func (j *JWTIssuer) IssueAccess(userID, email, role, clientID string) (string, int, error) {
	token, err := j.createToken(userID, email, role, clientID, j.accessTTL)
	if err != nil {
		return "", 0, err
	}
	return token, int(j.accessTTL.Seconds()), nil
}

func (j *JWTIssuer) IssueRefresh(userID string) (string, time.Time, error) {
	expiresAt := time.Now().Add(j.refreshTTL)
	token, err := j.createToken(userID, "", "", "", j.refreshTTL)
	if err != nil {
		return "", time.Time{}, err
	}
	return token, expiresAt, nil
}

func (j *JWTIssuer) ParseRefresh(tokenStr string) (string, error) {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) { return j.secret, nil },
		jwt.WithValidMethods([]string{"HS256"}))
	if err != nil || !token.Valid {
		return "", errors.New("invalid token")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", errors.New("invalid token")
	}
	uid, ok := claims["user_id"].(string)
	if !ok || uid == "" {
		return "", errors.New("invalid token")
	}
	return uid, nil
}

func (j *JWTIssuer) createToken(userID, email, role, clientID string, ttl time.Duration) (string, error) {
	claims := jwt.MapClaims{
		"user_id": userID,
		"iss":     issuer,
		"exp":     time.Now().Add(ttl).Unix(),
		"iat":     time.Now().Unix(),
	}
	if email != "" {
		claims["email"] = email
	}
	if role != "" {
		claims["role"] = role
	}
	if clientID != "" {
		claims["client_id"] = clientID
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(j.secret)
}
