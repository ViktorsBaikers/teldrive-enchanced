package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	userevents "github.com/tgdrive/teldrive/v2/internal/events"
	"github.com/tgdrive/teldrive/v2/internal/principal"
)

type streamEventEnvelope struct {
	Version      int             `json:"version"`
	OccurredAt   time.Time       `json:"occurredAt"`
	ResourceType string          `json:"resourceType"`
	ResourceID   string          `json:"resourceId,omitempty"`
	Generation   *int64          `json:"generation,omitempty"`
	Payload      json.RawMessage `json:"payload"`
}

type orderedEventLocationSide struct {
	ParentID *string `json:"parentId"`
	Name     string  `json:"name"`
}

type orderedEventEnvelope struct {
	Version      int                       `json:"version"`
	OccurredAt   time.Time                 `json:"occurredAt"`
	ResourceType string                    `json:"resourceType"`
	ResourceID   string                    `json:"resourceId,omitempty"`
	Generation   *int64                    `json:"generation,omitempty"`
	Payload      json.RawMessage           `json:"payload"`
	Before       *orderedEventLocationSide `json:"before,omitempty"`
	After        *orderedEventLocationSide `json:"after,omitempty"`
}

type orderedCursor struct {
	Epoch       uuid.UUID
	Incarnation uuid.UUID
	Position    int64
}

const (
	orderedDBTimeout      = 2 * time.Second
	orderedReplayMaxRows  = 4096
	orderedReplayMaxBytes = 4 << 20
	orderedFrameMaxBytes  = 64 << 10
	orderedAuthorityCap   = 30 * time.Second
	orderedLivePage       = 500
	orderedRenewLead      = 2 * time.Second
	maxAdvertisedBeatMS   = 3600000
)

var orderedEventTypes = []string{
	"file.created", "file.updated", "file.trashed",
	"file.deletion_pending", "file.restored", "file.purged",
}

var errStreamAuthorityExpired = errors.New("ordered stream authority expired")

func (h *RawHandler) StreamEvents(ctx context.Context, params gen.StreamEventsParams, w http.ResponseWriter) error {
	facts := streamQueryContext(ctx)
	if !facts.modePresent {
		return h.streamLegacyEvents(ctx, params, w)
	}
	if !facts.modeValid {
		return problem(http.StatusUnprocessableEntity, "invalid_event_mode", "event stream mode is invalid", nil)
	}
	return h.streamOrderedEvents(ctx, params, w)
}

func (h *RawHandler) streamLegacyEvents(ctx context.Context, params gen.StreamEventsParams, w http.ResponseWriter) error {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return mapServiceError(err)
	}
	if h == nil || h.handler == nil || h.handler.Events == nil {
		return mapServiceError(ErrOperationUnavailable)
	}
	cursor, cursorSet, err := eventCursor(params)
	if err != nil {
		return problem(http.StatusUnprocessableEntity, "invalid_event_cursor", "event cursor is invalid", err)
	}
	eventTypes, err := normalizeEventTypes(params.Types)
	if err != nil {
		return problem(http.StatusUnprocessableEntity, "invalid_event_types", "event types are invalid", err)
	}

	wake, unsubscribe, err := h.handler.Events.Subscribe(userID)
	if err != nil {
		return mapServiceError(err)
	}
	defer unsubscribe()

	if !cursorSet {
		cursor, err = h.handler.Events.CurrentCursor(ctx, userID)
		if err != nil {
			return mapServiceError(err)
		}
	}
	expired, err := h.handler.Events.CursorExpired(ctx, userID, cursor)
	if err != nil {
		return mapServiceError(err)
	}
	initial, err := h.handler.Events.ListAfter(ctx, userID, cursor, eventTypes)
	if err != nil {
		return mapServiceError(err)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	if err := writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprint(w, "retry: 3000\n\n")
		return err
	}); err != nil {
		return nil
	}

	if expired {
		_ = writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
			return writeStreamControl(w, "sync.required", map[string]string{"reason": "cursor_expired"})
		})
		return nil
	}

	heartbeat := time.NewTicker(h.handler.Events.Heartbeat())
	defer heartbeat.Stop()
	pending := initial
	for {
		for len(pending) > 0 {
			if err := writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
				for _, event := range pending {
					if err := writeStreamEvent(w, event); err != nil {
						return err
					}
					cursor = event.ID
				}
				return nil
			}); err != nil {
				return nil
			}
			if len(pending) < int(h.handler.Events.BatchSize()) {
				pending = nil
				break
			}
			pending, err = h.handler.Events.ListAfter(ctx, userID, cursor, eventTypes)
			if err != nil {
				_ = writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
					return writeStreamControl(w, "stream.error", map[string]string{"code": "service_unavailable"})
				})
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-h.handler.Events.Done():
			return nil
		case _, ok := <-wake:
			if !ok {
				return nil
			}
			pending, err = h.handler.Events.ListAfter(ctx, userID, cursor, eventTypes)
			if err != nil {
				_ = writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
					return writeStreamControl(w, "stream.error", map[string]string{"code": "service_unavailable"})
				})
				return nil
			}
		case <-heartbeat.C:
			pending, err = h.handler.Events.ListAfter(ctx, userID, cursor, eventTypes)
			if err != nil {
				_ = writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
					return writeStreamControl(w, "stream.error", map[string]string{"code": "service_unavailable"})
				})
				return nil
			}
			if len(pending) == 0 {
				if err := writeAndFlushStream(w, h.handler.Events.WriteTimeout(), func() error {
					_, err := fmt.Fprint(w, ": keep-alive\n\n")
					return err
				}); err != nil {
					return nil
				}
			}
		}
	}
}

func (h *RawHandler) streamOrderedEvents(ctx context.Context, params gen.StreamEventsParams, w http.ResponseWriter) error {
	if h == nil || h.handler == nil || h.handler.Events == nil {
		return mapServiceError(ErrOperationUnavailable)
	}
	if streamAfterPresent(ctx) {
		return problem(http.StatusUnprocessableEntity, "invalid_event_cursor", "event cursor is invalid", nil)
	}
	proof, ok := principal.OrderedProofFromContext(ctx)
	identity, identityOK := IdentityFromContext(ctx)
	if !ok || !identityOK || identity.UserID != proof.UserID {
		return mapServiceError(ErrUnauthenticated)
	}
	now := time.Now().UTC()
	deadline := proof.ValidatedAt.Add(orderedAuthorityCap)
	if !proof.ExpiresAt.IsZero() && proof.ExpiresAt.Before(deadline) {
		deadline = proof.ExpiresAt
	}
	if !now.Before(deadline) {
		return mapServiceError(ErrUnauthenticated)
	}
	eventTypes, err := orderedEventFilter(params.Types)
	if err != nil {
		return problem(http.StatusUnprocessableEntity, "invalid_event_types", "event types are invalid", err)
	}
	cursor, cursorSet, err := orderedEventCursor(params)
	if err != nil {
		return problem(http.StatusUnprocessableEntity, "invalid_event_cursor", "event cursor is invalid", err)
	}

	wake, unsubscribe, err := h.handler.Events.Subscribe(proof.UserID)
	if err != nil {
		return mapServiceError(err)
	}
	defer unsubscribe()

	state, replay, resetReason, err := h.orderedInitialReplay(ctx, proof, cursor, cursorSet, eventTypes)
	if err != nil {
		if errors.Is(err, ErrInvalidIdentity) {
			return problem(http.StatusForbidden, "forbidden", "ordered stream identity changed", err)
		}
		return mapServiceError(err)
	}

	stream := &orderedStream{
		w: w, writeTimeout: h.handler.Events.WriteTimeout(), deadline: deadline,
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-TelDrive-Events", "ordered")
	w.Header().Set("X-TelDrive-Heartbeat-Ms", strconv.Itoa(advertisedHeartbeatMS(h.handler.Events.Heartbeat())))
	if err := stream.write(func() error {
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprint(w, "retry: 3000\n\n")
		return err
	}); err != nil {
		return nil
	}

	if resetReason != "" {
		_ = stream.write(func() error {
			return writeStreamControl(w, "sync.required", map[string]string{"reason": resetReason})
		})
		return nil
	}
	ready := false
	cursor = orderedCursor{Epoch: state.Epoch, Incarnation: state.OwnerIncarnation}
	if cursorSet {
		cursor.Position = state.High
	}
	if len(replay) > 0 {
		if err := stream.write(func() error {
			for _, event := range replay {
				if _, err := fmt.Fprint(w, event); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil
		}
	}
	mode := "baseline"
	if cursorSet {
		mode = "replay"
	}
	cursor.Position = state.High
	if err := stream.write(func() error {
		return writeStreamControl(w, "sync.ready", orderedReady{Mode: mode, Cursor: cursor.String()})
	}); err != nil {
		return nil
	}
	ready = true
	checkpoint := cursor.Position

	heartbeat := time.NewTicker(h.handler.Events.Heartbeat())
	defer heartbeat.Stop()
	renewRemaining := time.Until(deadline)
	renewLead := orderedRenewLead
	if half := renewRemaining / 2; half < renewLead {
		renewLead = half
	}
	renewTimer := time.NewTimer(renewRemaining - renewLead)
	defer renewTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.handler.Events.Done():
			return nil
		case <-renewTimer.C:
			if !ready {
				return nil
			}
			_ = stream.write(func() error {
				return writeStreamControl(w, "sync.renew", struct{}{})
			})
			return nil
		case _, ok := <-wake:
			if !ok {
				return nil
			}
			advanced, reason, err := h.orderedLive(ctx, stream, proof, &cursor, eventTypes, &checkpoint)
			if err != nil {
				return nil
			}
			if reason != "" {
				_ = stream.write(func() error {
					return writeStreamControl(w, "sync.required", map[string]string{"reason": reason})
				})
				return nil
			}
			if advanced {
				continue
			}
		case <-heartbeat.C:
			advanced, reason, err := h.orderedLive(ctx, stream, proof, &cursor, eventTypes, &checkpoint)
			if err != nil {
				return nil
			}
			if reason != "" {
				_ = stream.write(func() error {
					return writeStreamControl(w, "sync.required", map[string]string{"reason": reason})
				})
				return nil
			}
			if !advanced {
				if err := stream.write(func() error {
					_, err := fmt.Fprint(w, ": keep-alive\n\n")
					return err
				}); err != nil {
					return nil
				}
			}
		}
	}
}

// orderedInitialReplay materializes the bounded replay in one snapshot and
// releases the database before any HTTP output.
func (h *RawHandler) orderedInitialReplay(
	ctx context.Context,
	proof principal.OrderedProof,
	cursor orderedCursor,
	cursorSet bool,
	eventTypes []string,
) (userevents.OrderedState, []string, string, error) {
	snapCtx, cancel := context.WithTimeout(ctx, orderedDBTimeout)
	defer cancel()
	snapshot, err := h.handler.Events.OpenOrderedSnapshot(snapCtx)
	if err != nil {
		return userevents.OrderedState{}, nil, "", err
	}
	defer snapshot.Close()
	state, err := snapshot.State(snapCtx, proof.UserID)
	if err != nil {
		return userevents.OrderedState{}, nil, "", err
	}
	if state.OwnerIncarnation != proof.OwnerIncarnation {
		return userevents.OrderedState{}, nil, "", ErrInvalidIdentity
	}
	if !cursorSet {
		return state, nil, "", nil
	}
	switch {
	case cursor.Epoch != state.Epoch, cursor.Incarnation != state.OwnerIncarnation:
		return state, nil, "epoch_changed", nil
	case cursor.Position < state.Floor:
		return state, nil, "cursor_expired", nil
	case cursor.Position > state.High:
		return state, nil, "replay_limit", nil
	}
	position := cursor.Position
	frames := make([]string, 0, 64)
	rows := 0
	bytes := 0
	for position < state.High {
		events, err := snapshot.List(snapCtx, state, position, eventTypes, orderedLivePage)
		if err != nil {
			return userevents.OrderedState{}, nil, "", err
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			frame, err := orderedEventFrame(state.Epoch, state.OwnerIncarnation, event)
			if err != nil {
				return userevents.OrderedState{}, nil, "", err
			}
			if len(frame) > orderedFrameMaxBytes {
				return state, nil, "replay_limit", nil
			}
			rows++
			bytes += len(frame)
			if rows > orderedReplayMaxRows || bytes > orderedReplayMaxBytes {
				return state, nil, "replay_limit", nil
			}
			frames = append(frames, frame)
			position = event.Position
		}
		if len(events) < orderedLivePage {
			break
		}
	}
	return state, frames, "", nil
}

// orderedLive reads one bounded live page and emits it. It returns whether the
// cursor advanced and any reset reason.
func (h *RawHandler) orderedLive(
	ctx context.Context,
	stream *orderedStream,
	proof principal.OrderedProof,
	cursor *orderedCursor,
	eventTypes []string,
	checkpoint *int64,
) (bool, string, error) {
	advanced := false
	for {
		if err := h.checkOrderedAuthority(ctx, proof); err != nil {
			return false, "", err
		}
		snapCtx, cancel := context.WithTimeout(ctx, orderedDBTimeout)
		snapshot, err := h.handler.Events.OpenOrderedSnapshot(snapCtx)
		if err != nil {
			cancel()
			return false, "", err
		}
		state, stateErr := snapshot.State(snapCtx, proof.UserID)
		if stateErr != nil {
			snapshot.Close()
			cancel()
			return false, "", stateErr
		}
		switch {
		case state.OwnerIncarnation != proof.OwnerIncarnation, cursor.Epoch != state.Epoch:
			snapshot.Close()
			cancel()
			return false, "epoch_changed", nil
		case cursor.Position < state.Floor:
			snapshot.Close()
			cancel()
			return false, "cursor_expired", nil
		case cursor.Position > state.High:
			snapshot.Close()
			cancel()
			return false, "replay_limit", nil
		}
		events, listErr := snapshot.List(snapCtx, state, cursor.Position, eventTypes, orderedLivePage)
		snapshot.Close()
		cancel()
		if listErr != nil {
			return false, "", listErr
		}
		for _, event := range events {
			frame, err := orderedEventFrame(state.Epoch, state.OwnerIncarnation, event)
			if err != nil {
				return false, "", err
			}
			if len(frame) > orderedFrameMaxBytes {
				return false, "replay_limit", nil
			}
			if err := stream.write(func() error {
				_, err := fmt.Fprint(stream.w, frame)
				return err
			}); err != nil {
				return false, "", err
			}
			cursor.Position = event.Position
			advanced = true
		}
		if len(events) == orderedLivePage {
			if err := stream.checkpoint(cursor, checkpoint); err != nil {
				return false, "", err
			}
			continue
		}
		if cursor.Position < state.High {
			cursor.Position = state.High
			advanced = true
		}
		if err := stream.checkpoint(cursor, checkpoint); err != nil {
			return false, "", err
		}
		return advanced, "", nil
	}
}

func (s *orderedStream) checkpoint(cursor *orderedCursor, checkpoint *int64) error {
	if cursor.Position == *checkpoint {
		return nil
	}
	*checkpoint = cursor.Position
	return s.write(func() error {
		return writeStreamControl(s.w, "sync.checkpoint", orderedCheckpoint{Cursor: cursor.String()})
	})
}

func (h *RawHandler) checkOrderedAuthority(ctx context.Context, proof principal.OrderedProof) error {
	checkCtx, cancel := context.WithTimeout(ctx, orderedDBTimeout)
	defer cancel()
	if proof.CredentialKind == "event_ticket" {
		return h.handler.Events.CheckOrderedTicket(checkCtx, proof.CredentialID)
	}
	if h.handler.Auth == nil {
		return nil
	}
	return h.handler.Auth.CheckOrderedAuthority(checkCtx, proof)
}

type orderedStream struct {
	w            http.ResponseWriter
	writeTimeout time.Duration
	deadline     time.Time
}

func (s *orderedStream) write(write func() error) error {
	remaining := time.Until(s.deadline)
	if remaining <= 0 {
		return errStreamAuthorityExpired
	}
	timeout := s.writeTimeout
	if timeout <= 0 || timeout > remaining {
		timeout = remaining
	}
	return writeAndFlushStream(s.w, timeout, write)
}

type orderedReady struct {
	Mode   string `json:"mode"`
	Cursor string `json:"cursor"`
}

type orderedCheckpoint struct {
	Cursor string `json:"cursor"`
}

func (c orderedCursor) String() string {
	return fmt.Sprintf("%s.%s.%d", c.Epoch, c.Incarnation, c.Position)
}

func orderedEventCursor(params gen.StreamEventsParams) (orderedCursor, bool, error) {
	header, ok := params.LastEventID.Get()
	if !ok {
		return orderedCursor{}, false, nil
	}
	cursor, err := parseOrderedCursor(header)
	if err != nil {
		return orderedCursor{}, false, err
	}
	return cursor, true, nil
}

func parseOrderedCursor(value string) (orderedCursor, error) {
	parts := strings.Split(strings.TrimSpace(value), ".")
	if len(parts) != 3 {
		return orderedCursor{}, errors.New("invalid ordered cursor")
	}
	epoch, err := uuid.Parse(parts[0])
	if err != nil {
		return orderedCursor{}, errors.New("invalid ordered cursor epoch")
	}
	incarnation, err := uuid.Parse(parts[1])
	if err != nil {
		return orderedCursor{}, errors.New("invalid ordered cursor incarnation")
	}
	position, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || position < 0 {
		return orderedCursor{}, errors.New("invalid ordered cursor position")
	}
	return orderedCursor{Epoch: epoch, Incarnation: incarnation, Position: position}, nil
}

func orderedEventFilter(values []string) ([]string, error) {
	if len(values) == 0 {
		// Ordered mode is a strict file-event stream: an absent filter means the
		// exact six names, never every event type the account produces.
		return append([]string(nil), orderedEventTypes...), nil
	}
	normalized, err := normalizeEventTypes(values)
	if err != nil {
		return nil, err
	}
	if len(normalized) != len(orderedEventTypes) {
		return nil, errors.New("ordered event types must be the exact strict set")
	}
	for _, value := range normalized {
		found := false
		for _, allowed := range orderedEventTypes {
			if value == allowed {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("unsupported ordered event type")
		}
	}
	return normalized, nil
}

func orderedEventFrame(epoch, incarnation uuid.UUID, event userevents.OrderedEvent) (string, error) {
	if event.Position <= 0 || !validEventName(event.Type) {
		return "", errors.New("invalid ordered event")
	}
	payload := json.RawMessage(event.Payload)
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	before, after, err := orderedEventLocations(event.Location)
	if err != nil {
		return "", err
	}
	envelope, err := json.Marshal(orderedEventEnvelope{
		Version: 1, OccurredAt: event.OccurredAt, ResourceType: event.ResourceType,
		ResourceID: event.ResourceID, Generation: event.Generation, Payload: payload,
		Before: before, After: after,
	})
	if err != nil {
		return "", err
	}
	cursor := orderedCursor{Epoch: epoch, Incarnation: incarnation, Position: event.Position}
	return fmt.Sprintf("id: %s\nevent: %s\ndata: %s\n\n", cursor.String(), event.Type, envelope), nil
}

func orderedEventLocations(raw []byte) (*orderedEventLocationSide, *orderedEventLocationSide, error) {
	if len(raw) == 0 {
		return nil, nil, nil
	}
	var stored struct {
		Old *struct {
			ParentID *string `json:"parentId"`
			Name     *string `json:"name"`
		} `json:"old"`
		New *struct {
			ParentID *string `json:"parentId"`
			Name     *string `json:"name"`
		} `json:"new"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, nil, errors.New("invalid stored event location")
	}
	side := func(value *struct {
		ParentID *string `json:"parentId"`
		Name     *string `json:"name"`
	}) (*orderedEventLocationSide, error) {
		if value == nil {
			return nil, nil
		}
		if value.Name == nil {
			return nil, errors.New("stored event location has no name")
		}
		return &orderedEventLocationSide{ParentID: value.ParentID, Name: *value.Name}, nil
	}
	before, err := side(stored.Old)
	if err != nil {
		return nil, nil, err
	}
	after, err := side(stored.New)
	if err != nil {
		return nil, nil, err
	}
	return before, after, nil
}

func advertisedHeartbeatMS(value time.Duration) int {
	milliseconds := int(value / time.Millisecond)
	if milliseconds < 1 {
		return 1
	}
	if milliseconds > maxAdvertisedBeatMS {
		return maxAdvertisedBeatMS
	}
	return milliseconds
}

func eventCursor(params gen.StreamEventsParams) (cursor int64, set bool, err error) {
	if header, ok := params.LastEventID.Get(); ok {
		value := strings.TrimSpace(header)
		if value == "" {
			return 0, false, errors.New("empty Last-Event-ID")
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			return 0, false, errors.New("invalid Last-Event-ID")
		}
		cursor, set = parsed, true
	}
	if after, ok := params.After.Get(); ok {
		if after < 0 {
			return 0, false, errors.New("negative event cursor")
		}
		if set && cursor != after {
			return 0, false, errors.New("conflicting event cursors")
		}
		cursor, set = after, true
	}
	return cursor, set, nil
}

func normalizeEventTypes(values []string) ([]string, error) {
	if len(values) > 50 {
		return nil, errors.New("too many event types")
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, errors.New("event type is empty")
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func writeStreamEvent(w http.ResponseWriter, event userevents.Event) error {
	if event.ID <= 0 || !validEventName(event.Type) {
		return errors.New("invalid event")
	}
	payload := json.RawMessage(event.Payload)
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	envelope, err := json.Marshal(streamEventEnvelope{
		Version:      1,
		OccurredAt:   event.OccurredAt,
		ResourceType: event.ResourceType,
		ResourceID:   event.ResourceID,
		Generation:   event.Generation,
		Payload:      payload,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.ID, event.Type, envelope)
	return err
}

func writeStreamControl(w http.ResponseWriter, name string, payload any) error {
	if !validEventName(name) {
		return errors.New("invalid control event name")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, encoded)
	return err
}

func validEventName(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func writeAndFlushStream(w http.ResponseWriter, timeout time.Duration, write func() error) error {
	controller := http.NewResponseController(w)
	deadlineSet := false
	if timeout > 0 {
		if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err == nil {
			deadlineSet = true
		} else if !errors.Is(err, http.ErrNotSupported) {
			return err
		}
	}
	if deadlineSet {
		defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	}
	if err := write(); err != nil {
		return err
	}
	return controller.Flush()
}
