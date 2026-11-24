package principal

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Identity struct {
	UserID    int64
	SessionID uuid.UUID
	Roles     []string
	Source    string
}

// OrderedProof is the credential and owner-incarnation handoff for a strict
// ordered event stream. CredentialID is the exact session/API-key UUID or the
// ticket digest; it never contains the presented secret. ExpiresAt is zero when
// the credential has no persisted expiry.
type OrderedProof struct {
	UserID           int64
	OwnerIncarnation uuid.UUID
	CredentialKind   string
	CredentialID     string
	ValidatedAt      time.Time
	ExpiresAt        time.Time
}

type contextKey struct{}
type orderedProofKey struct{}

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, identity)
}

func FromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(contextKey{}).(Identity)
	return identity, ok && identity.UserID > 0
}

func WithOrderedProof(ctx context.Context, proof OrderedProof) context.Context {
	return context.WithValue(ctx, orderedProofKey{}, proof)
}

func OrderedProofFromContext(ctx context.Context) (OrderedProof, bool) {
	proof, ok := ctx.Value(orderedProofKey{}).(OrderedProof)
	return proof, ok && proof.UserID > 0 && proof.OwnerIncarnation != uuid.Nil
}
