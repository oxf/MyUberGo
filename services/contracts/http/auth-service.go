package contracts

type UserRole string

const (
	RoleClient UserRole = "Client"
	RoleDriver UserRole = "Driver"
	RoleAdmin  UserRole = "Admin"
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

type LogoutRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type UserDto struct {
	ID        string   `json:"id"`
	Email     string   `json:"email"`
	Name      string   `json:"name"`
	Phone     string   `json:"phone"`
	Role      UserRole `json:"role"`
	CreatedAt string   `json:"createdAt"`
	// ClientId is only populated by GET /me for the caller's own profile,
	// so a client can learn its client id without decoding its JWT.
	ClientId *string `json:"clientId,omitempty"`
}
