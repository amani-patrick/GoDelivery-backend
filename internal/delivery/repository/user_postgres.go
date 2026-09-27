package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/umurinzi/backend/internal/domain"
)

// ── UserPostgresRepo ──────────────────────────────────────────────────────────

type UserPostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewUserPostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *UserPostgresRepo {
	return &UserPostgresRepo{pool: pool, log: log}
}

func (r *UserPostgresRepo) Create(ctx context.Context, u *domain.User) error {
	const q = `
		INSERT INTO users (
			id, full_name, phone, email, role,
			password_hash, is_active, is_verified,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	_, err := r.pool.Exec(ctx, q,
		u.ID, u.FullName, u.Phone, nullableString(u.Email),
		string(u.Role), u.PasswordHash,
		u.IsActive, u.IsVerified,
		u.CreatedAt, u.UpdatedAt,
	)
	if err != nil {
		r.log.Error("user.repo.Create failed",
			slog.String("user_id", u.ID),
			slog.String("phone", u.Phone),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func (r *UserPostgresRepo) GetByID(ctx context.Context, id string) (*domain.User, error) {
	const q = `
		SELECT id, full_name, phone, COALESCE(email,''), role,
		       password_hash, is_active, is_verified, created_at, updated_at
		FROM users WHERE id = $1`
	u, err := r.scanUser(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("user.repo.GetByID failed",
			slog.String("user_id", id),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("get user by id: %w", err)
	}
	return u, nil
}

func (r *UserPostgresRepo) GetByPhone(ctx context.Context, phone string) (*domain.User, error) {
	const q = `
		SELECT id, full_name, phone, COALESCE(email,''), role,
		       password_hash, is_active, is_verified, created_at, updated_at
		FROM users WHERE phone = $1`
	u, err := r.scanUser(r.pool.QueryRow(ctx, q, phone))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("user.repo.GetByPhone failed",
			slog.String("phone", phone),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("get user by phone: %w", err)
	}
	return u, nil
}

func (r *UserPostgresRepo) Update(ctx context.Context, u *domain.User) error {
	const q = `
		UPDATE users SET
			full_name   = $2,
			email       = $3,
			is_active   = $4,
			is_verified = $5,
			updated_at  = $6
		WHERE id = $1`
	u.UpdatedAt = time.Now().UTC()
	tag, err := r.pool.Exec(ctx, q,
		u.ID, u.FullName, nullableString(u.Email),
		u.IsActive, u.IsVerified, u.UpdatedAt,
	)
	if err != nil {
		r.log.Error("user.repo.Update failed",
			slog.String("user_id", u.ID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update user: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrUserNotFound
	}
	return nil
}

func (r *UserPostgresRepo) scanUser(row pgx.Row) (*domain.User, error) {
	u := &domain.User{}
	err := row.Scan(
		&u.ID, &u.FullName, &u.Phone, &u.Email, &u.Role,
		&u.PasswordHash, &u.IsActive, &u.IsVerified,
		&u.CreatedAt, &u.UpdatedAt,
	)
	return u, err
}

// ── DriverProfilePostgresRepo ─────────────────────────────────────────────────

// DriverProfilePostgresRepo implements domain.DriverProfileRepository.
type DriverProfilePostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewDriverProfilePostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *DriverProfilePostgresRepo {
	return &DriverProfilePostgresRepo{pool: pool, log: log}
}

// Upsert creates or fully replaces a driver profile row.
func (r *DriverProfilePostgresRepo) Upsert(ctx context.Context, p *domain.DriverProfile) error {
	const q = `
		INSERT INTO driver_profiles (
			user_id, status,
			national_id, license_number,
			vehicle_type, plate_number, max_weight_kg,
			is_online, current_lat, current_lng,
			rating, total_deliveries
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (user_id) DO UPDATE SET
			status           = EXCLUDED.status,
			national_id      = EXCLUDED.national_id,
			license_number   = EXCLUDED.license_number,
			vehicle_type     = EXCLUDED.vehicle_type,
			plate_number     = EXCLUDED.plate_number,
			max_weight_kg    = EXCLUDED.max_weight_kg,
			is_online        = EXCLUDED.is_online,
			current_lat      = EXCLUDED.current_lat,
			current_lng      = EXCLUDED.current_lng,
			rating           = EXCLUDED.rating,
			total_deliveries = EXCLUDED.total_deliveries`
	_, err := r.pool.Exec(ctx, q,
		p.UserID, string(p.Status),
		nullableString(p.NationalID), nullableString(p.LicenseNumber),
		string(p.VehicleType), p.PlateNumber, p.MaxWeightKg,
		p.IsOnline, p.CurrentLocation.Lat, p.CurrentLocation.Lng,
		p.Rating, p.TotalDeliveries,
	)
	if err != nil {
		r.log.Error("driver.repo.Upsert failed",
			slog.String("user_id", p.UserID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("upsert driver profile: %w", err)
	}
	return nil
}

// GetByUserID fetches the full driver profile.
func (r *DriverProfilePostgresRepo) GetByUserID(ctx context.Context, userID string) (*domain.DriverProfile, error) {
	const q = `
		SELECT user_id, status,
		       COALESCE(national_id,''), COALESCE(license_number,''),
		       vehicle_type, plate_number, max_weight_kg,
		       is_online, current_lat, current_lng,
		       rating, total_deliveries
		FROM driver_profiles
		WHERE user_id = $1`
	p := &domain.DriverProfile{}
	err := r.pool.QueryRow(ctx, q, userID).Scan(
		&p.UserID, &p.Status,
		&p.NationalID, &p.LicenseNumber,
		&p.VehicleType, &p.PlateNumber, &p.MaxWeightKg,
		&p.IsOnline, &p.CurrentLocation.Lat, &p.CurrentLocation.Lng,
		&p.Rating, &p.TotalDeliveries,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("driver.repo.GetByUserID failed",
			slog.String("user_id", userID),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("get driver profile: %w", err)
	}
	return p, nil
}

// SetOnlineStatus flips is_online. Called when a driver opens/closes the app
// and by TelemetryHandler on WebSocket disconnect (task 3 — phantom online fix).
func (r *DriverProfilePostgresRepo) SetOnlineStatus(ctx context.Context, userID string, online bool) error {
	const q = `UPDATE driver_profiles SET is_online = $2 WHERE user_id = $1`
	tag, err := r.pool.Exec(ctx, q, userID, online)
	if err != nil {
		return fmt.Errorf("set driver online status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrUserNotFound
	}
	return nil
}

// SetStatus transitions the driver's administrative lifecycle status.
func (r *DriverProfilePostgresRepo) SetStatus(ctx context.Context, userID string, status domain.DriverStatus) error {
	const q = `UPDATE driver_profiles SET status = $2 WHERE user_id = $1`
	tag, err := r.pool.Exec(ctx, q, userID, string(status))
	if err != nil {
		r.log.Error("driver.repo.SetStatus failed",
			slog.String("user_id", userID),
			slog.String("status", string(status)),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("set driver status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrUserNotFound
	}
	return nil
}

// SetOnTrip conditionally transitions ACTIVE ↔ ON_TRIP to prevent race conditions.
func (r *DriverProfilePostgresRepo) SetOnTrip(ctx context.Context, userID string, onTrip bool) error {
	var q string
	if onTrip {
		q = `UPDATE driver_profiles SET status = 'ON_TRIP' WHERE user_id = $1 AND status = 'ACTIVE'`
	} else {
		q = `UPDATE driver_profiles SET status = 'ACTIVE' WHERE user_id = $1 AND status = 'ON_TRIP'`
	}
	tag, err := r.pool.Exec(ctx, q, userID)
	if err != nil {
		return fmt.Errorf("set driver on_trip=%v: %w", onTrip, err)
	}
	if tag.RowsAffected() == 0 {
		r.log.Warn("driver.repo.SetOnTrip: precondition not met or driver not found",
			slog.String("user_id", userID),
			slog.Bool("on_trip", onTrip),
		)
	}
	return nil
}

// UpdateRating applies the Bayesian rolling average result (task 6).
func (r *DriverProfilePostgresRepo) UpdateRating(ctx context.Context, userID string, newRating float64, newTotal int) error {
	const q = `UPDATE driver_profiles SET rating = $2, total_deliveries = $3 WHERE user_id = $1`
	tag, err := r.pool.Exec(ctx, q, userID, newRating, newTotal)
	if err != nil {
		r.log.Error("driver.repo.UpdateRating failed",
			slog.String("user_id", userID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update driver rating: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrUserNotFound
	}
	return nil
}

// UpdateTrustScore persists a recalculated trust score and TrustLevel (task 9).
func (r *DriverProfilePostgresRepo) UpdateTrustScore(ctx context.Context, userID string, score float64, level domain.TrustLevel) error {
	const q = `UPDATE driver_profiles SET trust_score = $2, trust_level = $3 WHERE user_id = $1`
	_, err := r.pool.Exec(ctx, q, userID, score, string(level))
	if err != nil {
		return fmt.Errorf("update driver trust score: %w", err)
	}
	return nil
}

// FindEligibleDrivers is the matching engine Postgres-side filter.
// Receives candidateIDs from Redis GEOSEARCH (max 20), applies status +
// capacity + vehicle type filters, orders by rating DESC.
func (r *DriverProfilePostgresRepo) FindEligibleDrivers(
	ctx context.Context,
	candidateIDs []string,
	minWeightKg float64,
	required domain.VehicleType,
) ([]*domain.DriverProfile, error) {
	if len(candidateIDs) == 0 {
		return nil, nil
	}

	var q string
	var args []interface{}

	if required == "" {
		q = `
			SELECT user_id, status,
			       COALESCE(national_id,''), COALESCE(license_number,''),
			       vehicle_type, plate_number, max_weight_kg,
			       is_online, current_lat, current_lng,
			       rating, total_deliveries
			FROM driver_profiles
			WHERE user_id = ANY($1)
			  AND status = 'ACTIVE'
			  AND is_online = TRUE
			  AND max_weight_kg >= $2
			ORDER BY rating DESC`
		args = []interface{}{candidateIDs, minWeightKg}
	} else {
		q = `
			SELECT user_id, status,
			       COALESCE(national_id,''), COALESCE(license_number,''),
			       vehicle_type, plate_number, max_weight_kg,
			       is_online, current_lat, current_lng,
			       rating, total_deliveries
			FROM driver_profiles
			WHERE user_id = ANY($1)
			  AND status = 'ACTIVE'
			  AND is_online = TRUE
			  AND max_weight_kg >= $2
			  AND vehicle_type = $3
			ORDER BY rating DESC`
		args = []interface{}{candidateIDs, minWeightKg, string(required)}
	}

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		r.log.Error("driver.repo.FindEligibleDrivers failed",
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("find eligible drivers: %w", err)
	}
	defer rows.Close()

	var results []*domain.DriverProfile
	for rows.Next() {
		p := &domain.DriverProfile{}
		if err := rows.Scan(
			&p.UserID, &p.Status,
			&p.NationalID, &p.LicenseNumber,
			&p.VehicleType, &p.PlateNumber, &p.MaxWeightKg,
			&p.IsOnline, &p.CurrentLocation.Lat, &p.CurrentLocation.Lng,
			&p.Rating, &p.TotalDeliveries,
		); err != nil {
			return nil, fmt.Errorf("scan eligible driver: %w", err)
		}
		results = append(results, p)
	}
	return results, rows.Err()
}

// ── BusinessProfilePostgresRepo ───────────────────────────────────────────────

type BusinessProfilePostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewBusinessProfilePostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *BusinessProfilePostgresRepo {
	return &BusinessProfilePostgresRepo{pool: pool, log: log}
}

func (r *BusinessProfilePostgresRepo) Upsert(ctx context.Context, p *domain.BusinessProfile) error {
	const q = `
		INSERT INTO business_profiles (
			user_id, company_name, tin_number, contact_name,
			pickup_address, home_lat, home_lng, district, is_verified
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (user_id) DO UPDATE SET
			company_name   = EXCLUDED.company_name,
			tin_number     = EXCLUDED.tin_number,
			contact_name   = EXCLUDED.contact_name,
			pickup_address = EXCLUDED.pickup_address,
			home_lat       = EXCLUDED.home_lat,
			home_lng       = EXCLUDED.home_lng,
			district       = EXCLUDED.district,
			is_verified    = EXCLUDED.is_verified`
	_, err := r.pool.Exec(ctx, q,
		p.UserID, p.CompanyName, nullableString(p.TINNumber), p.ContactName,
		p.PickupAddress, p.Location.Lat, p.Location.Lng,
		p.District, p.IsVerified,
	)
	if err != nil {
		r.log.Error("business.repo.Upsert failed",
			slog.String("user_id", p.UserID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("upsert business profile: %w", err)
	}
	return nil
}

func (r *BusinessProfilePostgresRepo) GetByUserID(ctx context.Context, userID string) (*domain.BusinessProfile, error) {
	const q = `
		SELECT user_id, company_name, COALESCE(tin_number,''), contact_name,
		       pickup_address, home_lat, home_lng, district, is_verified
		FROM business_profiles
		WHERE user_id = $1`
	p := &domain.BusinessProfile{}
	err := r.pool.QueryRow(ctx, q, userID).Scan(
		&p.UserID, &p.CompanyName, &p.TINNumber, &p.ContactName,
		&p.PickupAddress, &p.Location.Lat, &p.Location.Lng,
		&p.District, &p.IsVerified,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("business.repo.GetByUserID failed",
			slog.String("user_id", userID),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("get business profile: %w", err)
	}
	return p, nil
}

func (r *BusinessProfilePostgresRepo) SetVerified(ctx context.Context, userID string, verified bool) error {
	const q = `UPDATE business_profiles SET is_verified = $2 WHERE user_id = $1`
	tag, err := r.pool.Exec(ctx, q, userID, verified)
	if err != nil {
		return fmt.Errorf("set business verified: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrUserNotFound
	}
	return nil
}

func (r *BusinessProfilePostgresRepo) UpdateTrustScore(ctx context.Context, userID string, score float64, level domain.TrustLevel) error {
	const q = `UPDATE business_profiles SET trust_score = $2, trust_level = $3 WHERE user_id = $1`
	_, err := r.pool.Exec(ctx, q, userID, score, string(level))
	if err != nil {
		return fmt.Errorf("update business trust score: %w", err)
	}
	return nil
}

// ── CustomerProfilePostgresRepo ───────────────────────────────────────────────

type CustomerProfilePostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewCustomerProfilePostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *CustomerProfilePostgresRepo {
	return &CustomerProfilePostgresRepo{pool: pool, log: log}
}

func (r *CustomerProfilePostgresRepo) Upsert(ctx context.Context, p *domain.CustomerProfile) error {
	const q = `
		INSERT INTO customer_profiles (user_id, saved_lat, saved_lng)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id) DO UPDATE SET
			saved_lat = EXCLUDED.saved_lat,
			saved_lng = EXCLUDED.saved_lng`
	_, err := r.pool.Exec(ctx, q, p.UserID, p.SavedLocation.Lat, p.SavedLocation.Lng)
	if err != nil {
		r.log.Error("customer.repo.Upsert failed",
			slog.String("user_id", p.UserID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("upsert customer profile: %w", err)
	}
	return nil
}

func (r *CustomerProfilePostgresRepo) GetByUserID(ctx context.Context, userID string) (*domain.CustomerProfile, error) {
	const q = `SELECT user_id, saved_lat, saved_lng FROM customer_profiles WHERE user_id = $1`
	p := &domain.CustomerProfile{}
	err := r.pool.QueryRow(ctx, q, userID).Scan(
		&p.UserID, &p.SavedLocation.Lat, &p.SavedLocation.Lng,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("customer.repo.GetByUserID failed",
			slog.String("user_id", userID),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("get customer profile: %w", err)
	}
	return p, nil
}

func (r *CustomerProfilePostgresRepo) UpdateTrustScore(ctx context.Context, userID string, score float64, level domain.TrustLevel) error {
	const q = `UPDATE customer_profiles SET trust_score = $2, trust_level = $3 WHERE user_id = $1`
	_, err := r.pool.Exec(ctx, q, userID, score, string(level))
	if err != nil {
		return fmt.Errorf("update customer trust score: %w", err)
	}
	return nil
}
