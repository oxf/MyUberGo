package handler

import (
	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

func toUserDto(u *domain.User) contracts.UserDto {
	return contracts.UserDto{
		ID:        u.ID,
		Email:     u.Email,
		Name:      u.Name,
		Phone:     u.Phone,
		Role:      contracts.UserRole(u.Role),
		CreatedAt: u.CreatedAt,
		ClientId:  u.ClientID,
	}
}
