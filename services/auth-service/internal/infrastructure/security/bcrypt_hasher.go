package security

import (
	"auth-service/internal/application/services"

	"golang.org/x/crypto/bcrypt"
)

type BcryptHasher struct{}

func NewBcryptHasher() services.PasswordHasher {
	return BcryptHasher{}
}

func (BcryptHasher) Hash(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	return string(hash), err
}

func (BcryptHasher) Compare(hash, plain string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain))
}
