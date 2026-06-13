package contracts

type UserRole string

const (
	RoleClient UserRole = "Client"
	RoleDriver UserRole = "Driver"
)

type SignupRequest struct {
	Email    string   `json:"email"`
	Password string   `json:"password"`
	Name     string   `json:"name"`
	Phone    string   `json:"phone"`
	Role     UserRole `json:"role"`
}

type SignupResponse struct {
	UserID string `json:"userId"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int    `json:"expiresIn"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type RefreshResponse struct {
	AccessToken string `json:"accessToken"`
	ExpiresIn   int    `json:"expiresIn"`
}

type UserProfileResponse struct {
	ID    string   `json:"id"`
	Email string   `json:"email"`
	Name  string   `json:"name"`
	Phone string   `json:"phone"`
	Role  UserRole `json:"role"`
}
