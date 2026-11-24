package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/principal"
)

func bearerProof(credentialID string) principal.OrderedProof {
	return principal.OrderedProof{
		UserID:           1001,
		OwnerIncarnation: uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
		CredentialKind:   "bearer",
		CredentialID:     credentialID,
		ValidatedAt:      time.Now().UTC(),
	}
}

func TestBearerMalformedCredentialIDRejected(t *testing.T) {
	t.Parallel()

	_, err := withOrderedProof(context.Background(), bearerProof("not-a-uuid"))
	if !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("malformed credential ID: want ErrInvalidIdentity, got %v", err)
	}
}

func TestBearerCredentialIDSetsSession(t *testing.T) {
	t.Parallel()

	sessionID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	ctx, err := withOrderedProof(context.Background(), bearerProof(sessionID.String()))
	if err != nil {
		t.Fatalf("valid credential ID: %v", err)
	}
	identity, ok := principal.FromContext(ctx)
	if !ok {
		t.Fatal("identity missing from context")
	}
	if identity.SessionID != sessionID {
		t.Fatalf("session ID: want %v, got %v", sessionID, identity.SessionID)
	}
}
