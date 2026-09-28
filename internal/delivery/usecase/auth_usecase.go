package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/umurinzi/backend/internal/domain"
	"golang.org/x/crypto/bcrypt"
)

//Sentinel errors

var (
	ErrInvalidCredentials = errors.New("invalid phone number or password")
	ErrPhoneAlreadyExists = errors.New("phone number already registered")
)

//JWT claims

// Claims is the strictly-typed JWT payload. Using a concrete struct (not
// map[string]any) ensures the compiler catches field-name typos at compile time.
type Claims struct {
	UserID string      `json:"uid"`
	Role   domain.Role `json:"role"`
	jwt.RegisteredClaims
}

// AuthConfig holds the parameters required to issue and validate JWTs.
// These are injected at construction time from the environment config.
type AuthConfig struct {
	JWTSecret   string
	ExpiryHours int
	BcryptCost  int
}

//AuthUsecase

// AuthUsecase handles registration and authentication. It owns the JWT issuance
// contract — no other layer mints tokens.
type AuthUsecase struct {
	userRepo domain.UserRepository
	cfg      AuthConfig
	log      *slog.Logger
}

// NewAuthUsecase constructs the usecase with required dependencies.
func NewAuthUsecase(userRepo domain.UserRepository, cfg AuthConfig, log *slog.Logger) *AuthUsecase {
	return &AuthUsecase{userRepo: userRepo, cfg: cfg, log: log}
}

//DTOs

// RegisterInput carries validated registration fields from the handler layer.
type RegisterInput struct {
	FullName string
	Phone    string
	Email    string
	Password string
	Role     domain.Role
}

// AuthResult is returned to callers on successful register or login.
// It contains a signed JWT and the sanitised user record (no password hash).
type AuthResult struct {
	Token string
	User  *domain.User
}

//Register

// Register creates a new user account. The plaintext password is hashed with
// bcrypt before any persistence call. The hash cost is injected from config.
//
// RBAC: privileged roles (ADMIN, DISPATCHER) can never be self-registered —
// they are provisioned internally (seed script / by an existing ADMIN via the
// future user-management ops). This closes the privilege-escalation hole
// where anyone could POST role=ADMIN to /auth/register.
func (uc *AuthUsecase) Register(ctx context.Context, in RegisterInput) (*AuthResult, error) {
	if !in.Role.IsValid() {
		return nil, fmt.Errorf("%w: unknown role %q", domain.ErrInvalidInput, in.Role)
	}
	switch in.Role {
	case domain.RoleAdmin, domain.RoleDispatcher:
		uc.log.Warn("blocked privileged self-registration",
			slog.String("phone", in.Phone),
			slog.String("requested_role", string(in.Role)),
		)
		return nil, fmt.Errorf("%w: role %q cannot be self-registered", domain.ErrUnauthorized, in.Role)
	}

	// Check uniqueness before hashing to avoid wasted bcrypt work on conflict.
	_, err := uc.userRepo.GetByPhone(ctx, in.Phone)
	if err == nil {
		// A user was found — phone already registered.
		return nil, ErrPhoneAlreadyExists
	}
	if !errors.Is(err, domain.ErrUserNotFound) {
		// Unexpected infrastructure error.
		return nil, fmt.Errorf("register: phone lookup: %w", err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), uc.cfg.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("register: hash password: %w", err)
	}

	now := time.Now().UTC()
	u := &domain.User{
		ID:           uuid.NewString(),
		FullName:     in.FullName,
		Phone:        in.Phone,
		Email:        in.Email,
		Role:         in.Role,
		PasswordHash: string(hash),
		IsActive:     true,
		IsVerified:   false, // requires phone OTP verification in a subsequent step
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := uc.userRepo.Create(ctx, u); err != nil {
		uc.log.Error("auth.Register: persist user failed",
			slog.String("phone", in.Phone),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("register: persist user: %w", err)
	}

	token, err := uc.issueJWT(u)
	if err != nil {
		// The user was created — we log the JWT failure but still return the
		// user so they can retry a login. A token failure here is an infrastructure
		// issue, not a business rule violation.
		uc.log.Error("auth.Register: JWT issuance failed",
			slog.String("user_id", u.ID),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("register: issue token: %w", err)
	}

	uc.log.Info("new user registered",
		slog.String("user_id", u.ID),
		slog.String("role", string(u.Role)),
	)
	return &AuthResult{Token: token, User: u}, nil
}

func (uc *AuthUsecase) Login(ctx context.Context, phone, password string) (*AuthResult, error) {
	u, err := uc.userRepo.GetByPhone(ctx, phone)
	if err != nil {
		// Always do a dummy bcrypt round to equalize response time.
		_ = bcrypt.CompareHashAndPassword(
			[]byte("$2a$12$dummyhashforatimingguardsothatthisalwaystakestime"),
			[]byte(password),
		)
		uc.log.Warn("auth.Login: unknown phone",
			slog.String("phone", phone),
		)
		return nil, ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		uc.log.Warn("auth.Login: wrong password",
			slog.String("user_id", u.ID),
		)
		return nil, ErrInvalidCredentials
	}

	if !u.IsActive {
		uc.log.Warn("auth.Login: suspended account",
			slog.String("user_id", u.ID),
		)
		return nil, domain.ErrAccountSuspended
	}

	token, err := uc.issueJWT(u)
	if err != nil {
		uc.log.Error("auth.Login: JWT issuance failed",
			slog.String("user_id", u.ID),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("login: issue token: %w", err)
	}

	uc.log.Info("user authenticated",
		slog.String("user_id", u.ID),
		slog.String("role", string(u.Role)),
	)
	return &AuthResult{Token: token, User: u}, nil
}

//JWT issuance(whole codebase Jwt depends on this)

func (uc *AuthUsecase) issueJWT(u *domain.User) (string, error) {
	now := time.Now().UTC()
	claims := Claims{
		UserID: u.ID,
		Role:   u.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "umurinzi",
			Subject:   u.ID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(uc.cfg.ExpiryHours) * time.Hour)),
			ID:        uuid.NewString(), // jti — unique per token, enables future revocation
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(uc.cfg.JWTSecret))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}
