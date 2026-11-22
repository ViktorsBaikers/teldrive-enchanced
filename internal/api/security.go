package api

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/events"
	"github.com/tgdrive/teldrive/v2/internal/principal"
)

var (
	ErrUnauthenticated = errors.New("authentication failed")
	ErrInvalidIdentity = errors.New("authenticated identity is invalid")
)

type Identity = principal.Identity

type Authenticator interface {
	AuthenticateBearer(context.Context, string) (Identity, error)
	AuthenticateAPIKey(context.Context, string) (Identity, error)
}

type EventTicketAuthenticator interface {
	AuthenticateTicket(context.Context, string) (int64, error)
}

// OrderedCredentialValidator validates a bearer or API key together with the
// owner incarnation on one read-only snapshot.
type OrderedCredentialValidator interface {
	AuthenticateOrderedBearer(context.Context, string) (principal.OrderedProof, error)
	AuthenticateOrderedAPIKey(context.Context, string) (principal.OrderedProof, error)
}

// OrderedEventTicketAuthenticator validates an event ticket together with its
// owner incarnation on one read-only snapshot.
type OrderedEventTicketAuthenticator interface {
	AuthenticateOrderedTicketSnapshot(context.Context, string) (events.TicketIdentity, error)
}

type Security struct {
	authenticator     Authenticator
	eventTickets      EventTicketAuthenticator
	publicFileContent bool
}

func NewSecurity(authenticator Authenticator, eventTickets ...EventTicketAuthenticator) *Security {
	security := &Security{authenticator: authenticator}
	if len(eventTickets) > 0 {
		security.eventTickets = eventTickets[0]
	}
	return security
}

// WithPublicFileContent enables anonymous streaming for the file content endpoints only.
func (s *Security) WithPublicFileContent(enabled bool) *Security {
	s.publicFileContent = enabled
	return s
}

func isFileContentOperation(operation gen.OperationName) bool {
	switch operation {
	case gen.DownloadFileOperation, gen.DownloadFileLegacyOperation, gen.HeadFileOperation, gen.HeadFileLegacyOperation:
		return true
	default:
		return false
	}
}

// A private request context marker prevents clients from forging the internal bypass credential.
type publicFileContentContextKey struct{}

const publicFileContentToken = "teldrive-internal-public-file-content"

func isPublicFileContent(ctx context.Context) bool {
	allowed, _ := ctx.Value(publicFileContentContextKey{}).(bool)
	return allowed
}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	return principal.FromContext(ctx)
}

func UserIDFromContext(ctx context.Context) (int64, error) {
	identity, ok := IdentityFromContext(ctx)
	if !ok {
		return 0, ErrUnauthenticated
	}
	return identity.UserID, nil
}

func HasRole(ctx context.Context, role string) bool {
	identity, ok := IdentityFromContext(ctx)
	if !ok {
		return false
	}
	return slices.Contains(identity.Roles, role)
}

func HasAdminRole(ctx context.Context) bool {
	return HasRole(ctx, "admin") || HasRole(ctx, "owner")
}

func (s *Security) HandleBearerAuth(ctx context.Context, operation gen.OperationName, auth gen.BearerAuth) (context.Context, error) {
	if operation == gen.StreamEventsOperation && isOrderedStreamRequest(ctx) {
		if s == nil {
			return ctx, ErrUnauthenticated
		}
		validator, ok := s.authenticator.(OrderedCredentialValidator)
		if !ok || strings.TrimSpace(auth.Token) == "" {
			return ctx, ErrUnauthenticated
		}
		proof, err := validator.AuthenticateOrderedBearer(ctx, auth.Token)
		if err != nil {
			return ctx, errors.Join(ErrUnauthenticated, err)
		}
		return withOrderedProof(ctx, proof)
	}
	if s != nil && s.publicFileContent && isFileContentOperation(operation) && isPublicFileContent(ctx) && auth.Token == publicFileContentToken {
		return ctx, nil
	}
	if s == nil || s.authenticator == nil || strings.TrimSpace(auth.Token) == "" {
		return ctx, ErrUnauthenticated
	}
	identity, err := s.authenticator.AuthenticateBearer(ctx, auth.Token)
	if err != nil {
		return ctx, errors.Join(ErrUnauthenticated, err)
	}
	if identity.UserID <= 0 {
		return ctx, ErrInvalidIdentity
	}
	auth.Roles = append([]string(nil), identity.Roles...)
	return principal.WithIdentity(ctx, identity), nil
}

func (s *Security) HandleExternalApiKeyAuth(ctx context.Context, operation gen.OperationName, auth gen.ExternalApiKeyAuth) (context.Context, error) {
	if operation == gen.StreamEventsOperation && isOrderedStreamRequest(ctx) {
		if s == nil {
			return ctx, ErrUnauthenticated
		}
		validator, ok := s.authenticator.(OrderedCredentialValidator)
		if !ok || strings.TrimSpace(auth.APIKey) == "" {
			return ctx, ErrUnauthenticated
		}
		proof, err := validator.AuthenticateOrderedAPIKey(ctx, auth.APIKey)
		if err != nil {
			return ctx, errors.Join(ErrUnauthenticated, err)
		}
		return withOrderedProof(ctx, proof)
	}
	if s == nil || s.authenticator == nil || strings.TrimSpace(auth.APIKey) == "" {
		return ctx, ErrUnauthenticated
	}
	identity, err := s.authenticator.AuthenticateAPIKey(ctx, auth.APIKey)
	if err != nil {
		return ctx, errors.Join(ErrUnauthenticated, err)
	}
	if identity.UserID <= 0 {
		return ctx, ErrInvalidIdentity
	}
	auth.Roles = append([]string(nil), identity.Roles...)
	return principal.WithIdentity(ctx, identity), nil
}

func (s *Security) HandleCookieAuth(ctx context.Context, _ gen.OperationName, auth gen.CookieAuth) (context.Context, error) {
	if s == nil || s.authenticator == nil || strings.TrimSpace(auth.APIKey) == "" {
		return ctx, ErrUnauthenticated
	}
	identity, err := s.authenticator.AuthenticateBearer(ctx, auth.APIKey)
	if err != nil {
		return ctx, errors.Join(ErrUnauthenticated, err)
	}
	if identity.UserID <= 0 {
		return ctx, ErrInvalidIdentity
	}
	auth.Roles = append([]string(nil), identity.Roles...)
	return principal.WithIdentity(ctx, identity), nil
}

func (s *Security) HandleEventTicketAuth(ctx context.Context, operation gen.OperationName, auth gen.EventTicketAuth) (context.Context, error) {
	if operation == gen.StreamEventsOperation && isOrderedStreamRequest(ctx) {
		if s == nil {
			return ctx, ErrUnauthenticated
		}
		validator, ok := s.eventTickets.(OrderedEventTicketAuthenticator)
		if !ok || strings.TrimSpace(auth.APIKey) == "" {
			return ctx, ErrUnauthenticated
		}
		identity, err := validator.AuthenticateOrderedTicketSnapshot(ctx, auth.APIKey)
		if err != nil {
			return ctx, errors.Join(ErrUnauthenticated, err)
		}
		return withOrderedProof(ctx, principal.OrderedProof{
			UserID: identity.UserID, OwnerIncarnation: identity.OwnerIncarnation,
			CredentialKind: "event_ticket", CredentialID: identity.Digest,
			ValidatedAt: time.Now().UTC(), ExpiresAt: identity.ExpiresAt,
		})
	}
	if s == nil || s.eventTickets == nil || strings.TrimSpace(auth.APIKey) == "" {
		return ctx, ErrUnauthenticated
	}
	userID, err := s.eventTickets.AuthenticateTicket(ctx, auth.APIKey)
	if err != nil {
		return ctx, errors.Join(ErrUnauthenticated, err)
	}
	if userID <= 0 {
		return ctx, ErrInvalidIdentity
	}
	return principal.WithIdentity(ctx, principal.Identity{UserID: userID, Source: "event_ticket"}), nil
}

func withOrderedProof(ctx context.Context, proof principal.OrderedProof) (context.Context, error) {
	if proof.UserID <= 0 || proof.OwnerIncarnation == uuid.Nil || proof.ValidatedAt.IsZero() {
		return ctx, ErrInvalidIdentity
	}
	identity := principal.Identity{UserID: proof.UserID, Source: proof.CredentialKind}
	if proof.CredentialKind == "bearer" {
		if sessionID, err := uuid.Parse(proof.CredentialID); err == nil {
			identity.SessionID = sessionID
		}
	}
	return principal.WithOrderedProof(principal.WithIdentity(ctx, identity), proof), nil
}

var _ gen.SecurityHandler = (*Security)(nil)
