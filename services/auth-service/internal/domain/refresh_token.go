package domain

import "time"

type RefreshToken struct {
	UserID    string
	Token     string
	ExpiresAt time.Time
}
