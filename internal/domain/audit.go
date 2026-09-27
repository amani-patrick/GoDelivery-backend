package domain

import (
	"context"
	"time"
)

// AuditEvent is an immutable domain event appended to the ledger on every
// significant system action. Records are never updated or deleted.
//
// Compliance: this structure satisfies the evidentiary requirements of the
// Rwanda Data Protection Law (Law No. 058/2021). The ledger table in
// PostgreSQL must have INSERT-only grants at the database-role level.
type AuditEvent struct {
	ID         string    `json:"id"`
	EntityID   string    `json:"entity_id"`   
	EntityType string    `json:"entity_type"` 
	ActorID    string    `json:"actor_id"`    
	Action     string    `json:"action"`      
	OldState   string    `json:"old_state,omitempty"`
	NewState   string    `json:"new_state,omitempty"`
	Metadata   string    `json:"metadata,omitempty"` 
	CreatedAt  time.Time `json:"created_at"`
}


// LedgerRepository is the persistence contract for the append-only audit ledger.
// Implementations must guarantee that Append never updates or deletes records.
type LedgerRepository interface {
	Append(ctx context.Context, event *AuditEvent) error
	QueryByEntity(ctx context.Context, entityID string) ([]*AuditEvent, error)
}
