
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/umurinzi/backend/internal/delivery/usecase"
	"github.com/umurinzi/backend/internal/domain"
	"github.com/umurinzi/backend/internal/middleware"
)

// Operation routing: the handler reads operationName from the JSON request body.
// For the two public auth routes (/auth/register, /auth/login) main.go injects
// the operation name via the X-Gql-Operation header so those routes can bypass
// the JWT middleware while still reusing this handler.

type graphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
	OpName    string         `json:"operationName"`
}

type graphQLResponse struct {
	Data   any            `json:"data,omitempty"`
	Errors []graphQLError `json:"errors,omitempty"`
}

type graphQLError struct {
	Message    string         `json:"message"`
	Extensions map[string]any `json:"extensions,omitempty"`
}

//Context keys

type handlerCtxKey string

const ctxIdempotencyKey handlerCtxKey = "idempotency_key"

//Handler

// Handler is the single HTTP entry point for all GraphQL operations.
// It reads usecase interfaces only: no direct repository or Redis access.
type Handler struct {
	deliveryUC *usecase.DeliveryUsecase
	authUC     *usecase.AuthUsecase
	driverUC   *usecase.DriverUsecase
	log        *slog.Logger
}

// NewHandler constructs the GraphQL handler with all usecase dependencies.
func NewHandler(
	deliveryUC *usecase.DeliveryUsecase,
	authUC *usecase.AuthUsecase,
	driverUC *usecase.DriverUsecase,
	log *slog.Logger,
) *Handler {
	return &Handler{
		deliveryUC: deliveryUC,
		authUC:     authUC,
		driverUC:   driverUC,
		log:        log,
	}
}

// ServeHTTP is the single HTTP entry point for all GraphQL operations.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req graphQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// X-Gql-Operation header overrides the body operationName.
	// Used by public auth routes that bypass JWT middleware.
	if hdr := r.Header.Get("X-Gql-Operation"); hdr != "" {
		req.OpName = hdr
	}
	if req.OpName == "" {
		h.writeError(w, http.StatusBadRequest, "operationName is required")
		return
	}

	ctx := r.Context()
	if idemKey := r.Header.Get("X-Idempotency-Key"); idemKey != "" {
		ctx = context.WithValue(ctx, ctxIdempotencyKey, idemKey)
	}

	data, gqlErr := h.route(ctx, req)

	resp := graphQLResponse{}
	if gqlErr != nil {
		resp.Errors = []graphQLError{{
			Message:    gqlErr.Error(),
			Extensions: h.classifyError(gqlErr),
		}}
	} else {
		resp.Data = data
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// route dispatches to the correct resolver by operationName.
func (h *Handler) route(ctx context.Context, req graphQLRequest) (any, error) {
	vars := req.Variables
	if vars == nil {
		vars = make(map[string]any)
	}

	switch req.OpName {

	//Public (no JWT) ───────────────────────────────────────────────────────
	case "Register":
		return h.resolveRegister(ctx, vars)
	case "Login":
		return h.resolveLogin(ctx, vars)

	//Delivery lifecycle ────────────────────────────────────────────────────
	case "CreateDelivery":
		return h.resolveCreateDelivery(ctx, vars)
	case "AcceptOrder":
		return h.resolveAcceptOrder(ctx, vars)
	case "ConfirmPickup":
		return h.resolveConfirmPickup(ctx, vars)
	case "ConfirmDelivery":
		return h.resolveConfirmDelivery(ctx, vars)
	case "RaiseDispute":
		return h.resolveRaiseDispute(ctx, vars)
	case "GetDelivery":
		return h.resolveGetDelivery(ctx, vars)
	case "MyDeliveries":
		return h.resolveMyDeliveries(ctx, vars)
	case "MerchantDeliveries":
		return h.resolveMerchantDeliveries(ctx, vars)

	//Driver registration & admin ───────────────────────────────────────────
	case "RegisterDriver":
		return h.resolveRegisterDriver(ctx, vars)
	case "ApproveDriver":
		return h.resolveApproveDriver(ctx, vars)
	case "SuspendDriver":
		return h.resolveSuspendDriver(ctx, vars)
	case "SetDriverOnline":
		return h.resolveSetDriverOnline(ctx, vars)
	case "SetDriverOffline":
		return h.resolveSetDriverOffline(ctx, vars)
	case "GetDriverProfile":
		return h.resolveGetDriverProfile(ctx, vars)
	case "FindEligibleDrivers":
		return h.resolveFindEligibleDrivers(ctx, vars)

	//Business / merchant profile ───────────────────────────────────────────
	case "UpsertBusinessProfile":
		return h.resolveUpsertBusinessProfile(ctx, vars)
	case "GetBusinessProfile":
		return h.resolveGetBusinessProfile(ctx, vars)

	//Customer profile ──────────────────────────────────────────────────────
	case "UpdateCustomerLocation":
		return h.resolveUpdateCustomerLocation(ctx, vars)

	default:
		return nil, fmt.Errorf("unknown operation: %q", req.OpName)
	}
}

//Auth resolvers ────────────────────────────────────────────────────────────

func (h *Handler) resolveRegister(ctx context.Context, vars map[string]any) (any, error) {
	input, err := requireInputMap(vars)
	if err != nil {
		return nil, err
	}
	result, err := h.authUC.Register(ctx, usecase.RegisterInput{
		FullName: requireString(input, "fullName"),
		Phone:    requireString(input, "phone"),
		Email:    stringOrEmpty(input, "email"),
		Password: requireString(input, "password"),
		Role:     domain.Role(requireString(input, "role")),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"token": result.Token, "user": marshalUser(result.User)}, nil
}

func (h *Handler) resolveLogin(ctx context.Context, vars map[string]any) (any, error) {
	input, err := requireInputMap(vars)
	if err != nil {
		return nil, err
	}
	result, err := h.authUC.Login(ctx,
		requireString(input, "phone"),
		requireString(input, "password"),
	)
	if err != nil {
		return nil, err
	}
	return map[string]any{"token": result.Token, "user": marshalUser(result.User)}, nil
}

//Delivery resolvers ────────────────────────────────────────────────────────

func (h *Handler) resolveCreateDelivery(ctx context.Context, vars map[string]any) (any, error) {
	callerID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	input, err := requireInputMap(vars)
	if err != nil {
		return nil, err
	}

	idempotencyKey, _ := ctx.Value(ctxIdempotencyKey).(string)
	pickup := locationFromMap(input, "pickupLocation")
	dropoff := locationFromMap(input, "dropoffLocation")

	out, err := h.deliveryUC.CreateDelivery(ctx, usecase.CreateDeliveryInput{
		MerchantID:          callerID,
		CustomerID:          requireString(input, "customerId"),
		PickupLat:           pickup[0],
		PickupLng:           pickup[1],
		DropoffLat:          dropoff[0],
		DropoffLng:          dropoff[1],
		Description:         requireString(input, "description"),
		WeightKg:            float64From(input, "weightKg"),
		VehicleTypeRequired: domain.VehicleType(stringOrEmpty(input, "vehicleTypeRequired")),
		PackageCategory:     domain.PackageCategory(stringOrEmpty(input, "packageCategory")),
		IdempotencyKey:      idempotencyKey,
	})
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"delivery":    marshalDelivery(out.Delivery),
		"isDuplicate": out.IsDuplicate,
	}
	if !out.IsDuplicate {
		resp["plaintextQrCode"] = out.PlaintextQRCode
		resp["plaintextPin"] = out.PlaintextPIN
	}
	return resp, nil
}

func (h *Handler) resolveAcceptOrder(ctx context.Context, vars map[string]any) (any, error) {
	driverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	deliveryID, err := requireVar(vars, "deliveryId")
	if err != nil {
		return nil, err
	}
	if err := h.deliveryUC.AcceptOrder(ctx, deliveryID, driverID); err != nil {
		h.log.Warn("AcceptOrder failed",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return nil, err
	}
	return h.fetchAndMarshal(ctx, deliveryID)
}

func (h *Handler) resolveConfirmPickup(ctx context.Context, vars map[string]any) (any, error) {
	driverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	deliveryID, err := requireVar(vars, "deliveryId")
	if err != nil {
		return nil, err
	}
	scannedToken, err := requireVar(vars, "scannedToken")
	if err != nil {
		return nil, err
	}
	// confirmedWeightKg is optional — pass 0 to skip the weight fraud check
	// (used by legacy client versions that don't have a scale).
	confirmedWeightKg := float64From(vars, "confirmedWeightKg")

	if err := h.deliveryUC.ConfirmPickup(ctx, deliveryID, driverID, scannedToken, confirmedWeightKg); err != nil {
		h.log.Warn("ConfirmPickup failed",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return nil, err
	}
	return h.fetchAndMarshal(ctx, deliveryID)
}

func (h *Handler) resolveConfirmDelivery(ctx context.Context, vars map[string]any) (any, error) {
	driverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	deliveryID, err := requireVar(vars, "deliveryId")
	if err != nil {
		return nil, err
	}
	pin, err := requireVar(vars, "customerPin")
	if err != nil {
		return nil, err
	}
	if err := h.deliveryUC.ConfirmDelivery(ctx, deliveryID, driverID, pin); err != nil {
		h.log.Warn("ConfirmDelivery failed",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return nil, err
	}
	d, err := h.deliveryUC.GetDelivery(ctx, deliveryID)
	if err != nil {
		return nil, err
	}
	m := marshalDelivery(d)
	m["piiPurgeRequired"] = true
	return m, nil
}

func (h *Handler) resolveRaiseDispute(ctx context.Context, vars map[string]any) (any, error) {
	actorID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	deliveryID, err := requireVar(vars, "deliveryId")
	if err != nil {
		return nil, err
	}
	reason, err := requireVar(vars, "reason")
	if err != nil {
		return nil, err
	}
	if err := h.deliveryUC.RaiseDispute(ctx, deliveryID, actorID, reason); err != nil {
		return nil, err
	}
	return h.fetchAndMarshal(ctx, deliveryID)
}

func (h *Handler) resolveGetDelivery(ctx context.Context, vars map[string]any) (any, error) {
	if _, err := h.requireAuth(ctx); err != nil {
		return nil, err
	}
	id, err := requireVar(vars, "id")
	if err != nil {
		return nil, err
	}
	return h.fetchAndMarshal(ctx, id)
}

func (h *Handler) resolveMyDeliveries(ctx context.Context, vars map[string]any) (any, error) {
	driverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	states := deliveryStatesFromVars(vars, "states")
	deliveries, err := h.deliveryUC.ListDriverDeliveries(ctx, driverID, states)
	if err != nil {
		return nil, err
	}
	return marshalDeliveries(deliveries), nil
}

func (h *Handler) resolveMerchantDeliveries(ctx context.Context, vars map[string]any) (any, error) {
	merchantID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	states := deliveryStatesFromVars(vars, "states")
	deliveries, err := h.deliveryUC.ListMerchantDeliveries(ctx, merchantID, states)
	if err != nil {
		return nil, err
	}
	return marshalDeliveries(deliveries), nil
}

//Driver resolvers ──────────────────────────────────────────────────────────

func (h *Handler) resolveRegisterDriver(ctx context.Context, vars map[string]any) (any, error) {
	callerID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	input, err := requireInputMap(vars)
	if err != nil {
		return nil, err
	}
	maxWeight := float64From(input, "maxWeightKg")
	profile, err := h.driverUC.RegisterDriver(ctx, usecase.RegisterDriverInput{
		UserID:        callerID,
		NationalID:    requireString(input, "nationalId"),
		LicenseNumber: requireString(input, "licenseNumber"),
		VehicleType:   domain.VehicleType(requireString(input, "vehicleType")),
		PlateNumber:   requireString(input, "plateNumber"),
		MaxWeightKg:   maxWeight,
	})
	if err != nil {
		return nil, err
	}
	return marshalDriverProfile(profile), nil
}

func (h *Handler) resolveApproveDriver(ctx context.Context, vars map[string]any) (any, error) {
	approverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	driverUserID, err := requireVar(vars, "driverUserId")
	if err != nil {
		return nil, err
	}
	if err := h.driverUC.ApproveDriver(ctx, driverUserID, approverID); err != nil {
		return nil, err
	}
	return map[string]any{"driverUserId": driverUserID, "status": string(domain.DriverActive)}, nil
}

func (h *Handler) resolveSuspendDriver(ctx context.Context, vars map[string]any) (any, error) {
	suspenderID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	driverUserID, err := requireVar(vars, "driverUserId")
	if err != nil {
		return nil, err
	}
	reason, err := requireVar(vars, "reason")
	if err != nil {
		return nil, err
	}
	if err := h.driverUC.SuspendDriver(ctx, driverUserID, suspenderID, reason); err != nil {
		return nil, err
	}
	return map[string]any{"driverUserId": driverUserID, "status": string(domain.DriverSuspended)}, nil
}

func (h *Handler) resolveSetDriverOnline(ctx context.Context, vars map[string]any) (any, error) {
	driverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.driverUC.SetOnline(ctx, driverID); err != nil {
		return nil, err
	}
	return map[string]any{"isOnline": true}, nil
}

func (h *Handler) resolveSetDriverOffline(ctx context.Context, vars map[string]any) (any, error) {
	driverID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.driverUC.SetOffline(ctx, driverID); err != nil {
		return nil, err
	}
	return map[string]any{"isOnline": false}, nil
}

func (h *Handler) resolveGetDriverProfile(ctx context.Context, vars map[string]any) (any, error) {
	callerID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	// Allow fetching another driver's public profile if driverUserId is provided;
	// otherwise return the caller's own profile.
	targetID := stringOrEmpty(vars, "driverUserId")
	if targetID == "" {
		targetID = callerID
	}
	profile, err := h.driverUC.GetDriverProfile(ctx, targetID)
	if err != nil {
		return nil, err
	}
	return marshalDriverProfile(profile), nil
}

func (h *Handler) resolveFindEligibleDrivers(ctx context.Context, vars map[string]any) (any, error) {
	if _, err := h.requireAuth(ctx); err != nil {
		return nil, err
	}
	deliveryID, err := requireVar(vars, "deliveryId")
	if err != nil {
		return nil, err
	}

	// Fetch the delivery to get its weight and vehicle requirements.
	d, err := h.deliveryUC.GetDelivery(ctx, deliveryID)
	if err != nil {
		return nil, err
	}

	// NearbyDriverIDs must be provided by the caller from the client-side
	// Redis geo query result (lat/lng → SpatialIndex.FindNearbyDrivers).
	// The handler accepts them as a JSON array variable.
	candidateIDs := stringSliceFromVars(vars, "nearbyDriverIds")

	candidates, err := h.driverUC.FindEligibleDrivers(ctx, usecase.FindEligibleDriversInput{
		NearbyDriverIDs:     candidateIDs,
		MinWeightKg:         d.WeightKg,
		RequiredVehicleType: d.VehicleTypeRequired,
	})
	if err != nil {
		return nil, err
	}

	return marshalDispatchCandidates(candidates), nil
}

//Business profile resolvers ────────────────────────────────────────────────

func (h *Handler) resolveUpsertBusinessProfile(ctx context.Context, vars map[string]any) (any, error) {
	callerID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	input, err := requireInputMap(vars)
	if err != nil {
		return nil, err
	}
	loc := locationFromMap(input, "location")
	profile, err := h.driverUC.UpsertBusinessProfile(ctx, usecase.UpsertBusinessProfileInput{
		UserID:        callerID,
		CompanyName:   requireString(input, "companyName"),
		TINNumber:     stringOrEmpty(input, "tinNumber"),
		ContactName:   stringOrEmpty(input, "contactName"),
		PickupAddress: requireString(input, "pickupAddress"),
		Lat:           loc[0],
		Lng:           loc[1],
		District:      stringOrEmpty(input, "district"),
	})
	if err != nil {
		return nil, err
	}
	return marshalBusinessProfile(profile), nil
}

func (h *Handler) resolveGetBusinessProfile(ctx context.Context, vars map[string]any) (any, error) {
	callerID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	profile, err := h.driverUC.GetBusinessProfile(ctx, callerID)
	if err != nil {
		return nil, err
	}
	return marshalBusinessProfile(profile), nil
}

//Customer profile resolvers ────────────────────────────────────────────────

func (h *Handler) resolveUpdateCustomerLocation(ctx context.Context, vars map[string]any) (any, error) {
	callerID, err := h.requireAuth(ctx)
	if err != nil {
		return nil, err
	}
	lat := float64From(vars, "lat")
	lng := float64From(vars, "lng")
	if err := h.driverUC.UpsertCustomerSavedLocation(ctx, callerID, lat, lng); err != nil {
		return nil, err
	}
	return map[string]any{"lat": lat, "lng": lng}, nil
}

//Auth helper ───────────────────────────────────────────────────────────────

func (h *Handler) requireAuth(ctx context.Context) (string, error) {
	userID, ok := ctx.Value(middleware.CtxUserID).(string)
	if !ok || userID == "" {
		return "", domain.ErrUnauthorized
	}
	return userID, nil
}

//Error classification ──────────────────────────────────────────────────────

func (h *Handler) classifyError(err error) map[string]any {
	ext := map[string]any{}
	switch {
	case errors.Is(err, domain.ErrHandshakeFailed):
		ext["code"] = "HANDSHAKE_FAILED"
	case errors.Is(err, domain.ErrInvalidStateTransition):
		ext["code"] = "INVALID_TRANSITION"
	case errors.Is(err, domain.ErrLockAcquisitionFailed):
		ext["code"] = "ORDER_LOCKED"
	case errors.Is(err, domain.ErrDeliveryNotFound):
		ext["code"] = "NOT_FOUND"
	case errors.Is(err, domain.ErrUserNotFound):
		ext["code"] = "NOT_FOUND"
	case errors.Is(err, domain.ErrUnauthorized):
		ext["code"] = "UNAUTHORIZED"
	case errors.Is(err, domain.ErrAccountSuspended):
		ext["code"] = "ACCOUNT_SUSPENDED"
	case errors.Is(err, domain.ErrDriverNotActive):
		ext["code"] = "DRIVER_NOT_ACTIVE"
	case errors.Is(err, domain.ErrInsufficientCapacity):
		ext["code"] = "INSUFFICIENT_CAPACITY"
	case errors.Is(err, domain.ErrVehicleTypeMismatch):
		ext["code"] = "VEHICLE_TYPE_MISMATCH"
	case errors.Is(err, domain.ErrDriverNotEligible):
		ext["code"] = "DRIVER_NOT_ELIGIBLE"
	case errors.Is(err, domain.ErrWeightMismatch):
		ext["code"] = "WEIGHT_MISMATCH"
	case errors.Is(err, domain.ErrStackNotFeasible):
		ext["code"] = "STACK_NOT_FEASIBLE"
	case errors.Is(err, domain.ErrActorBanned):
		ext["code"] = "ACTOR_BANNED"
	case errors.Is(err, domain.ErrInvalidInput):
		ext["code"] = "INVALID_INPUT"
	default:
		ext["code"] = "INTERNAL_ERROR"
	}
	return ext
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(graphQLResponse{
		Errors: []graphQLError{{Message: msg}},
	})
}

//Marshal helpers ───────────────────────────────────────────────────────────

func (h *Handler) fetchAndMarshal(ctx context.Context, deliveryID string) (any, error) {
	d, err := h.deliveryUC.GetDelivery(ctx, deliveryID)
	if err != nil {
		return nil, err
	}
	return marshalDelivery(d), nil
}

func marshalDelivery(d *domain.Delivery) map[string]any {
	return map[string]any{
		"id":                  d.ID,
		"merchantId":          d.MerchantID,
		"driverId":            d.DriverID,
		"customerId":          d.CustomerID,
		"currentState":        string(d.CurrentState),
		"vehicleTypeRequired": string(d.VehicleTypeRequired),
		"packageCategory":     string(d.PackageCategory),
		"pickupLocation": map[string]any{
			"lat": d.PickupLoc.Lat,
			"lng": d.PickupLoc.Lng,
		},
		"dropoffLocation": map[string]any{
			"lat": d.DropoffLoc.Lat,
			"lng": d.DropoffLoc.Lng,
		},
		"description": d.Description,
		"weightKg":    d.WeightKg,
		"createdAt":   d.CreatedAt.Format(time.RFC3339),
		"updatedAt":   d.UpdatedAt.Format(time.RFC3339),
	}
}

func marshalDeliveries(ds []*domain.Delivery) []map[string]any {
	out := make([]map[string]any, len(ds))
	for i, d := range ds {
		out[i] = marshalDelivery(d)
	}
	return out
}

func marshalUser(u *domain.User) map[string]any {
	return map[string]any{
		"id":         u.ID,
		"fullName":   u.FullName,
		"phone":      u.Phone,
		"role":       string(u.Role),
		"isActive":   u.IsActive,
		"isVerified": u.IsVerified,
		"createdAt":  u.CreatedAt.Format(time.RFC3339),
	}
}

// marshalDriverProfile omits NationalID and LicenseNumber — these are
// administrator-only fields that must never appear in API responses.
func marshalDriverProfile(p *domain.DriverProfile) map[string]any {
	return map[string]any{
		"userId":          p.UserID,
		"status":          string(p.Status),
		"vehicleType":     string(p.VehicleType),
		"plateNumber":     p.PlateNumber,
		"maxWeightKg":     p.MaxWeightKg,
		"isOnline":        p.IsOnline,
		"rating":          p.Rating,
		"totalDeliveries": p.TotalDeliveries,
		"currentLocation": map[string]any{
			"lat": p.CurrentLocation.Lat,
			"lng": p.CurrentLocation.Lng,
		},
	}
}

func marshalBusinessProfile(p *domain.BusinessProfile) map[string]any {
	return map[string]any{
		"userId":        p.UserID,
		"companyName":   p.CompanyName,
		"contactName":   p.ContactName,
		"pickupAddress": p.PickupAddress,
		"location": map[string]any{
			"lat": p.Location.Lat,
			"lng": p.Location.Lng,
		},
		"district":   p.District,
		"isVerified": p.IsVerified,
		// TINNumber deliberately omitted from API responses
	}
}

func marshalDispatchCandidates(cs []*domain.DispatchCandidate) []map[string]any {
	out := make([]map[string]any, len(cs))
	for i, c := range cs {
		out[i] = map[string]any{
			"userId":      c.UserID,
			"fullName":    c.FullName,
			"vehicleType": string(c.VehicleType),
			"plateNumber": c.PlateNumber,
			"maxWeightKg": c.MaxWeightKg,
			"distanceKm":  c.DistanceKm,
			// Phone deliberately omitted from dispatch list responses —
			// only revealed to the merchant after assignment
		}
	}
	return out
}

//Variable extraction helpers ───────────────────────────────────────────────

func requireInputMap(vars map[string]any) (map[string]any, error) {
	v, ok := vars["input"]
	if !ok {
		return nil, fmt.Errorf("%w: missing 'input' field", domain.ErrInvalidInput)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: 'input' must be an object", domain.ErrInvalidInput)
	}
	return m, nil
}

func requireVar(vars map[string]any, key string) (string, error) {
	s := stringOrEmpty(vars, key)
	if strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("%w: missing required variable %q", domain.ErrInvalidInput, key)
	}
	return s, nil
}

func requireString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func stringOrEmpty(m map[string]any, key string) string {
	return requireString(m, key)
}

func float64From(m map[string]any, key string) float64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case float32:
			return float64(n)
		}
	}
	return 0
}

func locationFromMap(m map[string]any, key string) [2]float64 {
	loc, ok := m[key].(map[string]any)
	if !ok {
		return [2]float64{}
	}
	return [2]float64{float64From(loc, "lat"), float64From(loc, "lng")}
}

func deliveryStatesFromVars(vars map[string]any, key string) []domain.DeliveryState {
	raw, ok := vars[key]
	if !ok {
		return nil
	}
	slice, ok := raw.([]any)
	if !ok {
		return nil
	}
	states := make([]domain.DeliveryState, 0, len(slice))
	for _, s := range slice {
		if str, ok := s.(string); ok {
			states = append(states, domain.DeliveryState(str))
		}
	}
	return states
}

func stringSliceFromVars(vars map[string]any, key string) []string {
	raw, ok := vars[key]
	if !ok {
		return nil
	}
	slice, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(slice))
	for _, v := range slice {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
