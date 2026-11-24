//go:build integration

package api_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/authn"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/events"
	"github.com/tgdrive/teldrive/v2/internal/principal"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
)

type orderedLocation struct {
	ParentID *string `json:"parentId"`
	Name     string  `json:"name"`
}

type orderedEventData struct {
	Version      int              `json:"version"`
	ResourceType string           `json:"resourceType"`
	ResourceID   string           `json:"resourceId"`
	Payload      json.RawMessage  `json:"payload"`
	Before       *orderedLocation `json:"before"`
	After        *orderedLocation `json:"after"`
}

type orderedCursorParts struct {
	Epoch       uuid.UUID
	Incarnation uuid.UUID
	Position    int64
	Raw         string
}

func TestB3OrderedEventTracer(t *testing.T) {
	db := testpostgres.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const userID int64 = 1001
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
		t.Fatal(err)
	}
	auth := newOrderedAuth(t, db)
	eventService := newOrderedEvents(t, db)
	handler := api.NewHandler(catalog.NewService(db.Pool, nil), nil, nil, nil, nil, 0, eventService).
		ConfigureDomains(auth, nil, nil, nil, nil, nil)
	server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
	defer server.Close()

	token := issueBearerToken(t, db, auth, userID, 10*time.Minute)

	baselineResponse, baselineCancel := openEventStream(t, server.URL+"/v1/events?mode=ordered", map[string]string{
		"Authorization": "Bearer " + token,
	})
	defer baselineCancel()
	if got := baselineResponse.Header.Get("X-Teldrive-Events"); got != "ordered" {
		t.Fatalf("ordered header = %q", got)
	}
	beat, err := strconv.Atoi(baselineResponse.Header.Get("X-Teldrive-Heartbeat-Ms"))
	if err != nil || beat <= 0 {
		t.Fatalf("heartbeat header = %q, %v", baselineResponse.Header.Get("X-Teldrive-Heartbeat-Ms"), err)
	}
	baselineReader := bufio.NewReader(baselineResponse.Body)
	if frame := readSSEFrame(t, baselineReader); frame["retry"] != "3000" {
		t.Fatalf("baseline preamble = %#v", frame)
	}
	baselineReady := readSSEFrame(t, baselineReader)
	if baselineReady["event"] != "sync.ready" {
		t.Fatalf("baseline first control = %#v", baselineReady)
	}
	var readyPayload struct {
		Mode   string `json:"mode"`
		Cursor string `json:"cursor"`
	}
	if err := json.Unmarshal([]byte(baselineReady["data"]), &readyPayload); err != nil {
		t.Fatal(err)
	}
	if readyPayload.Mode != "baseline" {
		t.Fatalf("baseline mode = %q", readyPayload.Mode)
	}
	baselineCursor := parseOrderedCursorParts(t, readyPayload.Cursor)
	if got := committedHigh(ctx, t, db, userID); baselineCursor.Position != got {
		t.Fatalf("baseline cursor position = %d, committed high = %d", baselineCursor.Position, got)
	}

	folder, err := catalog.NewService(db.Pool, nil).CreateFolder(ctx, catalog.CreateFolderInput{UserID: userID, Name: "tracer-file"})
	if err != nil {
		t.Fatalf("create folder: %v", err)
	}
	folderID := uuid.UUID(folder.ID.Bytes).String()

	liveFrame := readEventFrame(t, baselineReader, "file.created")
	liveCursor := parseOrderedCursorParts(t, liveFrame["id"])
	if liveCursor.Position <= baselineCursor.Position {
		t.Fatalf("live position = %d, baseline = %d", liveCursor.Position, baselineCursor.Position)
	}
	var liveData orderedEventData
	if err := json.Unmarshal([]byte(liveFrame["data"]), &liveData); err != nil {
		t.Fatal(err)
	}
	if liveData.ResourceID != folderID {
		t.Fatalf("live resource id = %q, want %q", liveData.ResourceID, folderID)
	}
	var committed int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE user_id = $1 AND resource_id = $2 AND event_type = 'file.created'", userID, folderID).Scan(&committed); err != nil {
		t.Fatalf("count committed create: %v", err)
	}
	if committed != 1 {
		t.Fatalf("committed create events = %d", committed)
	}

	baselineCancel()
	_ = baselineResponse.Body.Close()

	filtered, filteredCancel := openEventStream(t, server.URL+"/v1/events?mode=ordered&types=file.created,file.updated,file.trashed,file.deletion_pending,file.restored,file.purged", map[string]string{
		"Authorization": "Bearer " + token,
	})
	filteredReader := bufio.NewReader(filtered.Body)
	_ = readSSEFrame(t, filteredReader)
	if ready := readSSEFrame(t, filteredReader); ready["event"] != "sync.ready" {
		t.Fatalf("strict filter ready = %#v", ready)
	}
	filteredCancel()
	_ = filtered.Body.Close()
	subset := doOrderedRequest(t, http.MethodGet, server.URL+"/v1/events?mode=ordered&types=file.created", map[string]string{
		"Authorization": "Bearer " + token,
	})
	if body := readResponseBody(t, subset); subset.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "invalid_event_types") {
		t.Fatalf("subset filter status = %d body = %s", subset.StatusCode, body)
	}

	replayResponse, replayCancel := openEventStream(t, server.URL+"/v1/events?mode=ordered", map[string]string{
		"Authorization": "Bearer " + token,
		"Last-Event-ID": baselineCursor.Raw,
	})
	defer replayCancel()
	replayReader := bufio.NewReader(replayResponse.Body)
	if frame := readSSEFrame(t, replayReader); frame["retry"] != "3000" {
		t.Fatalf("replay preamble = %#v", frame)
	}
	replayed := readEventFrame(t, replayReader, "file.created")
	if replayed["id"] != liveFrame["id"] {
		t.Fatalf("replayed id = %q, want %q", replayed["id"], liveFrame["id"])
	}
	replayReady := readSSEFrame(t, replayReader)
	if replayReady["event"] != "sync.ready" {
		t.Fatalf("replay ready = %#v", replayReady)
	}
	if err := json.Unmarshal([]byte(replayReady["data"]), &readyPayload); err != nil {
		t.Fatal(err)
	}
	if readyPayload.Mode != "replay" {
		t.Fatalf("replay mode = %q", readyPayload.Mode)
	}
	replayCancel()
	_ = replayResponse.Body.Close()

	legacyResponse, legacyCancel := openEventStream(t, server.URL+"/v1/events?after=0", map[string]string{
		"Authorization": "Bearer " + token,
	})
	defer legacyCancel()
	legacyReader := bufio.NewReader(legacyResponse.Body)
	if frame := readSSEFrame(t, legacyReader); frame["retry"] != "3000" {
		t.Fatalf("legacy preamble = %#v", frame)
	}
	legacyFrame := readEventFrame(t, legacyReader, "file.created")
	legacyID, err := strconv.ParseInt(legacyFrame["id"], 10, 64)
	if err != nil || legacyID <= 0 {
		t.Fatalf("legacy id = %q, %v", legacyFrame["id"], err)
	}
	var legacyData orderedEventData
	if err := json.Unmarshal([]byte(legacyFrame["data"]), &legacyData); err != nil {
		t.Fatal(err)
	}
	if legacyData.ResourceID != folderID {
		t.Fatalf("legacy resource id = %q", legacyData.ResourceID)
	}
}

func TestB3EventHandoff(t *testing.T) {
	db := testpostgres.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Run("ordered rejects raw after presence", func(t *testing.T) {
		const userID int64 = 2001
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
		defer server.Close()
		token := issueBearerToken(t, db, auth, userID, 10*time.Minute)

		for _, query := range []string{
			"mode=ordered&after=1",
			"mode=ordered&after=",
			"mode=ordered&after=1&after=2",
			"mode=ordered&after=%zz",
			"mode=ordered&after=99999999999999999999",
		} {
			response := doOrderedRequest(t, http.MethodGet, server.URL+"/v1/events?"+query, map[string]string{
				"Authorization": "Bearer " + token,
			})
			body := readResponseBody(t, response)
			if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "invalid_event_cursor") {
				t.Fatalf("query %q status = %d body = %s", query, response.StatusCode, body)
			}
		}
		for _, query := range []string{"mode=legacy", "mode=ordered&mode=ordered", "mode=%zz"} {
			response := doOrderedRequest(t, http.MethodGet, server.URL+"/v1/events?"+query, map[string]string{
				"Authorization": "Bearer " + token,
			})
			body := readResponseBody(t, response)
			if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "invalid_event_mode") {
				t.Fatalf("query %q status = %d body = %s", query, response.StatusCode, body)
			}
		}
	})

	t.Run("near-expiry credentials yield no baseline", func(t *testing.T) {
		const userID int64 = 2002
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		delay := 700 * time.Millisecond
		delayedAuthServer := httptest.NewServer(mustServer(t, handler, api.NewSecurity(
			delayedAuth{Service: auth, delay: delay},
			delayedTicketService{Service: eventService, delay: delay},
		)))
		defer delayedAuthServer.Close()

		bearer := issueBearerToken(t, db, auth, userID, 400*time.Millisecond)
		assertNoOrderedBaseline(t, serverRequest(t, delayedAuthServer.URL+"/v1/events?mode=ordered", map[string]string{
			"Authorization": "Bearer " + bearer,
		}))

		expiry := time.Now().Add(400 * time.Millisecond)
		created, err := auth.CreateAPIKey(ctx, userID, "near-expiry", &expiry)
		if err != nil {
			t.Fatal(err)
		}
		assertNoOrderedBaseline(t, serverRequest(t, delayedAuthServer.URL+"/v1/events?mode=ordered", map[string]string{
			"X-Api-Key": created.Secret,
		}))

		ticket, err := eventService.IssueTicket(ctx, userID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, "UPDATE event_stream_tickets SET expires_at = now() + interval '400 milliseconds' WHERE token_hash = $1", sha256Bytes(ticket.Value)); err != nil {
			t.Fatal(err)
		}
		assertNoOrderedBaseline(t, serverRequest(t, delayedAuthServer.URL+"/v1/events?mode=ordered&ticket="+ticket.Value, nil))
	})

	t.Run("incarnation change before snapshot is denied", func(t *testing.T) {
		const userID int64 = 2003
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		racer := mutatingAuth{Service: auth, mutate: func() {
			if _, err := db.Pool.Exec(context.Background(), "DELETE FROM users WHERE user_id = $1", userID); err != nil {
				t.Errorf("retire owner: %v", err)
				return
			}
			if _, err := db.Pool.Exec(context.Background(), "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
				t.Errorf("recreate owner: %v", err)
			}
		}}
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(racer, eventService)))
		defer server.Close()
		token := issueBearerToken(t, db, auth, userID, 10*time.Minute)
		response := doOrderedRequest(t, http.MethodGet, server.URL+"/v1/events?mode=ordered", map[string]string{
			"Authorization": "Bearer " + token,
		})
		body := readResponseBody(t, response)
		if response.StatusCode != http.StatusForbidden || strings.Contains(body, "sync.ready") {
			t.Fatalf("incarnation change status = %d body = %s", response.StatusCode, body)
		}
	})

	t.Run("healthy ready stream renews before authority expiry", func(t *testing.T) {
		const userID int64 = 2008
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
		defer server.Close()
		ticket, err := eventService.IssueTicket(ctx, userID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, "UPDATE event_stream_tickets SET expires_at = now() + interval '1 second' WHERE token_hash = $1", sha256Bytes(ticket.Value)); err != nil {
			t.Fatal(err)
		}
		response := serverRequest(t, server.URL+"/v1/events?mode=ordered&ticket="+ticket.Value, nil)
		if response.StatusCode != http.StatusOK {
			body := readResponseBody(t, response)
			t.Fatalf("renew stream status = %d body = %s", response.StatusCode, body)
		}
		reader := bufio.NewReader(response.Body)
		_ = readSSEFrame(t, reader)
		if ready := readSSEFrame(t, reader); ready["event"] != "sync.ready" {
			t.Fatalf("renew stream ready = %#v", ready)
		}
		renewed := false
		for !renewed {
			frame := readSSEFrame(t, reader)
			if frame["event"] == "sync.renew" {
				if frame["id"] != "" || frame["data"] != "{}" {
					t.Fatalf("renew frame = %#v", frame)
				}
				renewed = true
			}
		}
		_ = response.Body.Close()
	})

	t.Run("retention and reset boundaries", func(t *testing.T) {
		const userID int64 = 2005
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
		defer server.Close()
		token := issueBearerToken(t, db, auth, userID, 10*time.Minute)
		insertOrderedEvents(t, db, userID, 3, "retained")
		epoch, incarnation := orderedStateIdentity(ctx, t, db, userID)
		high := committedHigh(ctx, t, db, userID)
		authHeaders := map[string]string{"Authorization": "Bearer " + token}

		emptyReplay := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(authHeaders, "Last-Event-ID",
			fmt.Sprintf("%s.%s.%d", epoch, incarnation, high)))
		if emptyReplay.StatusCode != http.StatusOK {
			t.Fatalf("empty replay status = %d", emptyReplay.StatusCode)
		}
		emptyReader := bufio.NewReader(emptyReplay.Body)
		if frame := readSSEFrame(t, emptyReader); frame["retry"] != "3000" {
			t.Fatalf("empty replay preamble = %#v", frame)
		}
		emptyReady := readSSEFrame(t, emptyReader)
		if emptyReady["event"] != "sync.ready" || !strings.Contains(emptyReady["data"], `"mode":"replay"`) {
			t.Fatalf("empty replay ready = %#v", emptyReady)
		}
		_ = emptyReplay.Body.Close()

		wrongEpoch := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(authHeaders, "Last-Event-ID",
			fmt.Sprintf("%s.%s.0", uuid.New(), incarnation)))
		wrongReader := bufio.NewReader(wrongEpoch.Body)
		_ = readSSEFrame(t, wrongReader)
		if reason := readRequiredReason(t, wrongReader); reason != "epoch_changed" {
			t.Fatalf("wrong epoch reason = %q", reason)
		}
		_ = wrongEpoch.Body.Close()

		wrongOwner := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(authHeaders, "Last-Event-ID",
			fmt.Sprintf("%s.%s.0", epoch, uuid.New())))
		wrongOwnerReader := bufio.NewReader(wrongOwner.Body)
		_ = readSSEFrame(t, wrongOwnerReader)
		if reason := readRequiredReason(t, wrongOwnerReader); reason != "epoch_changed" {
			t.Fatalf("wrong owner reason = %q", reason)
		}
		_ = wrongOwner.Body.Close()

		beyondHigh := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(authHeaders, "Last-Event-ID",
			fmt.Sprintf("%s.%s.%d", epoch, incarnation, high+1)))
		beyondReader := bufio.NewReader(beyondHigh.Body)
		_ = readSSEFrame(t, beyondReader)
		if reason := readRequiredReason(t, beyondReader); reason != "replay_limit" {
			t.Fatalf("beyond high reason = %q", reason)
		}
		_ = beyondHigh.Body.Close()

		var interior, retainedPosition int64
		if err := db.Pool.QueryRow(ctx, "SELECT strict_position FROM user_events WHERE user_id = $1 AND resource_id = 'retained-2'", userID).Scan(&interior); err != nil {
			t.Fatal(err)
		}
		if err := db.Pool.QueryRow(ctx, "SELECT strict_position FROM user_events WHERE user_id = $1 AND resource_id = 'retained-1'", userID).Scan(&retainedPosition); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, "DELETE FROM user_events WHERE user_id = $1 AND strict_position = $2", userID, interior); err != nil {
			t.Fatal(err)
		}
		retained := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(authHeaders, "Last-Event-ID",
			fmt.Sprintf("%s.%s.%d", epoch, incarnation, retainedPosition)))
		retainedReader := bufio.NewReader(retained.Body)
		_ = readSSEFrame(t, retainedReader)
		if reason := readRequiredReason(t, retainedReader); reason != "cursor_expired" {
			t.Fatalf("interior deletion reason = %q", reason)
		}
		_ = retained.Body.Close()
	})

	t.Run("materialized replay survives concurrent retention", func(t *testing.T) {
		const userID int64 = 2006
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
		defer server.Close()
		token := issueBearerToken(t, db, auth, userID, 10*time.Minute)
		insertOrderedEvents(t, db, userID, 600, "replay")
		epoch, incarnation := orderedStateIdentity(ctx, t, db, userID)
		high := committedHigh(ctx, t, db, userID)

		response := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(
			map[string]string{"Authorization": "Bearer " + token}, "Last-Event-ID",
			fmt.Sprintf("%s.%s.0", epoch, incarnation)))
		defer func() { _ = response.Body.Close() }()
		reader := bufio.NewReader(response.Body)
		if frame := readSSEFrame(t, reader); frame["retry"] != "3000" {
			t.Fatalf("retention preamble = %#v", frame)
		}
		if err := deleteOrderedEventsAbove(t, db, userID, 1, 500); err != nil {
			t.Fatalf("commit retention during replay: %v", err)
		}
		last := int64(0)
		rows := 0
		for {
			frame := readSSEFrame(t, reader)
			if frame["event"] == "sync.ready" {
				if !strings.Contains(frame["data"], `"mode":"replay"`) {
					t.Fatalf("retention ready = %#v", frame)
				}
				break
			}
			if frame["id"] == "" {
				continue
			}
			cursor := parseOrderedCursorParts(t, frame["id"])
			if cursor.Position <= last {
				t.Fatalf("replay order regressed: %d after %d", cursor.Position, last)
			}
			last = cursor.Position
			rows++
		}
		if rows != 600 || last != high {
			t.Fatalf("replayed rows = %d, last = %d, high = %d", rows, last, high)
		}
	})

	t.Run("replay cap resets without readiness", func(t *testing.T) {
		const userID int64 = 2007
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
		defer server.Close()
		token := issueBearerToken(t, db, auth, userID, 10*time.Minute)
		insertOrderedEvents(t, db, userID, 4500, "cap")
		epoch, incarnation := orderedStateIdentity(ctx, t, db, userID)

		response := serverRequest(t, server.URL+"/v1/events?mode=ordered", withHeader(
			map[string]string{"Authorization": "Bearer " + token}, "Last-Event-ID",
			fmt.Sprintf("%s.%s.0", epoch, incarnation)))
		defer func() { _ = response.Body.Close() }()
		reader := bufio.NewReader(response.Body)
		if frame := readSSEFrame(t, reader); frame["retry"] != "3000" {
			t.Fatalf("cap preamble = %#v", frame)
		}
		if reason := readRequiredReason(t, reader); reason != "replay_limit" {
			t.Fatalf("replay cap reason = %q", reason)
		}
	})

	t.Run("revoked blocked stream terminates and reconnect is denied", func(t *testing.T) {
		const userID int64 = 2004
		if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatal(err)
		}
		auth := newOrderedAuth(t, db)
		eventService := newOrderedEvents(t, db)
		handler := api.NewHandler(nil, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
		server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
		defer server.Close()

		for range 8 {
			if _, err := db.Pool.Exec(ctx, `
				INSERT INTO user_events (user_id, event_type, resource_type, resource_id, generation, payload)
				SELECT $1, 'file.created', 'file', 'blocked-' || series::text, 1, '{}'::jsonb
				FROM generate_series(1, 500) AS series
			`, userID); err != nil {
				t.Fatalf("seed blocked events: %v", err)
			}
		}
		epoch, incarnation := orderedStateIdentity(ctx, t, db, userID)
		ticket, err := eventService.IssueTicket(ctx, userID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, "UPDATE event_stream_tickets SET expires_at = now() + interval '1 second' WHERE token_hash = $1", sha256Bytes(ticket.Value)); err != nil {
			t.Fatal(err)
		}
		blockedURL := fmt.Sprintf("%s/v1/events?mode=ordered&ticket=%s", server.URL, ticket.Value)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, blockedURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Last-Event-ID", fmt.Sprintf("%s.%s.0", epoch, incarnation))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("open blocked ordered stream: %v", err)
		}
		if response.StatusCode != http.StatusOK {
			body := readResponseBody(t, response)
			t.Fatalf("blocked stream status = %d body = %s", response.StatusCode, body)
		}
		time.Sleep(150 * time.Millisecond)
		if _, err := db.Pool.Exec(ctx, "UPDATE event_stream_tickets SET expires_at = now() - interval '1 second' WHERE token_hash = $1", sha256Bytes(ticket.Value)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		drainDone := make(chan error, 1)
		go func() {
			_, err := io.Copy(io.Discard, response.Body)
			drainDone <- err
		}()
		select {
		case <-drainDone:
		case <-time.After(5 * time.Second):
			t.Fatal("blocked ordered stream was not bounded by its authority deadline")
		}
		if elapsed := time.Since(start); elapsed > 4*time.Second {
			t.Fatalf("blocked stream took %s after revocation", elapsed)
		}
		_ = response.Body.Close()
		reconnect := doOrderedRequest(t, http.MethodGet, blockedURL, map[string]string{
			"Last-Event-ID": fmt.Sprintf("%s.%s.0", epoch, incarnation),
		})
		body := readResponseBody(t, reconnect)
		if reconnect.StatusCode != http.StatusUnauthorized || strings.Contains(body, "sync.ready") {
			t.Fatalf("revoked reconnect status = %d body = %s", reconnect.StatusCode, body)
		}
	})
}

func TestB3EventLocations(t *testing.T) {
	db := testpostgres.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const userID int64 = 3001
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
		t.Fatal(err)
	}
	auth := newOrderedAuth(t, db)
	eventService := newOrderedEvents(t, db)
	catalogService := catalog.NewService(db.Pool, nil)
	handler := api.NewHandler(catalogService, nil, nil, nil, nil, 0, eventService).ConfigureDomains(auth, nil, nil, nil, nil, nil)
	server := httptest.NewServer(mustServer(t, handler, api.NewSecurity(auth, eventService)))
	defer server.Close()
	token := issueBearerToken(t, db, auth, userID, 10*time.Minute)

	response, responseCancel := openEventStream(t, server.URL+"/v1/events?mode=ordered", map[string]string{
		"Authorization": "Bearer " + token,
	})
	defer responseCancel()
	reader := bufio.NewReader(response.Body)
	if frame := readSSEFrame(t, reader); frame["retry"] != "3000" {
		t.Fatalf("locations preamble = %#v", frame)
	}
	ready := readSSEFrame(t, reader)
	if ready["event"] != "sync.ready" {
		t.Fatalf("locations ready = %#v", ready)
	}

	folder, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: userID, Name: "loc-a"})
	if err != nil {
		t.Fatal(err)
	}
	folderID := uuid.UUID(folder.ID.Bytes)
	createdFrame := readEventFrame(t, reader, "file.created")
	created := decodeOrderedFrame(t, createdFrame)
	if created.ResourceID != folderID.String() || created.After == nil || created.After.Name != "loc-a" || created.After.ParentID != nil || created.Before != nil {
		t.Fatalf("created location = %#v", created)
	}
	if !strings.Contains(createdFrame["data"], `"parentId":null`) {
		t.Fatalf("created root location is not explicit null: %s", createdFrame["data"])
	}

	if _, err := catalogService.Rename(ctx, userID, folderID, nil, "loc-a2"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	renamed := decodeOrderedFrame(t, readEventFrame(t, reader, "file.updated"))
	if renamed.ResourceID != folderID.String() || renamed.Before == nil || renamed.After == nil {
		t.Fatalf("renamed location = %#v", renamed)
	}
	if renamed.Before.Name != "loc-a" || renamed.Before.ParentID != nil || renamed.After.Name != "loc-a2" || renamed.After.ParentID != nil {
		t.Fatalf("rename sides = before %#v after %#v", renamed.Before, renamed.After)
	}

	parent, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: userID, Name: "loc-parent"})
	if err != nil {
		t.Fatal(err)
	}
	parentID := uuid.UUID(parent.ID.Bytes)
	_ = readEventFrame(t, reader, "file.created")
	parentPtr := parentID
	if _, err := catalogService.Move(ctx, userID, folderID, &parentPtr, nil); err != nil {
		t.Fatalf("move: %v", err)
	}
	moved := decodeOrderedFrame(t, readEventFrame(t, reader, "file.updated"))
	if moved.Before == nil || moved.After == nil || moved.Before.ParentID != nil || moved.After.ParentID == nil {
		t.Fatalf("moved location = %#v", moved)
	}
	if *moved.After.ParentID != parentID.String() {
		t.Fatalf("move parent = %v", *moved.After.ParentID)
	}
	if moved.After.Name != "loc-a2" || moved.Before.Name != "loc-a2" {
		t.Fatalf("move names = %#v -> %#v", moved.Before, moved.After)
	}

	if _, err := db.Pool.Exec(ctx, "UPDATE files SET status = 'trashed', deleted_at = now(), generation = generation + 1 WHERE id = $1", folderID); err != nil {
		t.Fatalf("trash: %v", err)
	}
	trashed := decodeOrderedFrame(t, readEventFrame(t, reader, "file.trashed"))
	if trashed.ResourceID != folderID.String() || trashed.Before == nil || trashed.After == nil {
		t.Fatalf("trashed location = %#v", trashed)
	}
	if trashed.Before.ParentID == nil || trashed.After.ParentID == nil || *trashed.After.ParentID != parentID.String() {
		t.Fatalf("trash parents = %#v -> %#v", trashed.Before, trashed.After)
	}

	if _, err := db.Pool.Exec(ctx, "UPDATE files SET status = 'deletion_pending', generation = generation + 1 WHERE id = $1", folderID); err != nil {
		t.Fatalf("mark deletion pending: %v", err)
	}
	pending := decodeOrderedFrame(t, readEventFrame(t, reader, "file.deletion_pending"))
	if pending.ResourceID != folderID.String() || pending.Before == nil || pending.After == nil {
		t.Fatalf("deletion pending location = %#v", pending)
	}
	if pending.Before.Name != "loc-a2" || pending.After.Name != "loc-a2" {
		t.Fatalf("deletion pending names = %#v -> %#v", pending.Before, pending.After)
	}
}

type delayedAuth struct {
	*authn.Service
	delay time.Duration
}

func (d delayedAuth) AuthenticateOrderedBearer(ctx context.Context, raw string) (principal.OrderedProof, error) {
	proof, err := d.Service.AuthenticateOrderedBearer(ctx, raw)
	if err == nil {
		time.Sleep(d.delay)
	}
	return proof, err
}

func (d delayedAuth) AuthenticateOrderedAPIKey(ctx context.Context, raw string) (principal.OrderedProof, error) {
	proof, err := d.Service.AuthenticateOrderedAPIKey(ctx, raw)
	if err == nil {
		time.Sleep(d.delay)
	}
	return proof, err
}

type delayedTicketService struct {
	*events.Service
	delay time.Duration
}

func (d delayedTicketService) AuthenticateOrderedTicketSnapshot(ctx context.Context, value string) (events.TicketIdentity, error) {
	identity, err := d.Service.AuthenticateOrderedTicketSnapshot(ctx, value)
	if err == nil {
		time.Sleep(d.delay)
	}
	return identity, err
}

type mutatingAuth struct {
	*authn.Service
	mutate func()
}

func (m mutatingAuth) AuthenticateOrderedBearer(ctx context.Context, raw string) (principal.OrderedProof, error) {
	proof, err := m.Service.AuthenticateOrderedBearer(ctx, raw)
	if err == nil {
		m.mutate()
	}
	return proof, err
}

func (m mutatingAuth) AuthenticateOrderedAPIKey(ctx context.Context, raw string) (principal.OrderedProof, error) {
	proof, err := m.Service.AuthenticateOrderedAPIKey(ctx, raw)
	if err == nil {
		m.mutate()
	}
	return proof, err
}

type orderedLoginStub struct{}

func (orderedLoginStub) Start(context.Context, string) (authn.LoginStep, error) {
	return authn.LoginStep{}, fmt.Errorf("login unavailable")
}

func (orderedLoginStub) StartQR(context.Context) (authn.LoginStep, error) {
	return authn.LoginStep{}, fmt.Errorf("login unavailable")
}

func (orderedLoginStub) PollQR(context.Context, []byte) (authn.LoginStep, error) {
	return authn.LoginStep{}, fmt.Errorf("login unavailable")
}

func (orderedLoginStub) VerifyCode(context.Context, string, []byte, string) (authn.LoginStep, error) {
	return authn.LoginStep{}, fmt.Errorf("login unavailable")
}

func (orderedLoginStub) VerifyPassword(context.Context, []byte, string) (authn.LoginStep, error) {
	return authn.LoginStep{}, fmt.Errorf("login unavailable")
}

func newOrderedAuth(t *testing.T, db *testpostgres.Database) *authn.Service {
	t.Helper()
	cipher, err := secureblob.NewWithKey(bytes.Repeat([]byte{7}, 32), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	service, err := authn.NewService(db.Pool, cipher, orderedLoginStub{}, authn.Config{
		SigningKey: strings.Repeat("k", 32), Issuer: "b3-ordered",
		AccessTokenTTL: 10 * time.Minute, RefreshTokenTTL: time.Hour, LoginFlowTTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func newOrderedEvents(t *testing.T, db *testpostgres.Database) *events.Service {
	t.Helper()
	service, err := events.NewService(db.Pool, slog.New(slog.NewTextHandler(io.Discard, nil)), events.Config{
		BatchSize: 10, MaxConnectionsPerUser: 30, Heartbeat: 100 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond, TicketTTL: 3 * time.Second, CleanupInterval: time.Hour,
		ConnectTimeout: time.Second, PingInterval: 20 * time.Millisecond,
		ReconnectMin: time.Millisecond, ReconnectMax: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Close(closeCtx)
	})
	return service
}

func mustServer(t *testing.T, handler *api.Handler, security *api.Security) http.Handler {
	t.Helper()
	server, err := api.NewServer(handler, security)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func issueBearerToken(t *testing.T, db *testpostgres.Database, auth *authn.Service, userID int64, expiry time.Duration) string {
	t.Helper()
	refresh := "tdr_" + uuid.NewString()
	hash := sha256.Sum256([]byte(refresh))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO sessions (id, user_id, telegram_session, refresh_token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5)
	`, uuid.New(), userID, []byte{0}, hash[:], time.Now().Add(expiry)); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	renewal, err := auth.RenewAccess(ctx, refresh)
	if err != nil {
		t.Fatalf("renew access: %v", err)
	}
	return renewal.AccessToken
}

func committedHigh(ctx context.Context, t *testing.T, db *testpostgres.Database, userID int64) int64 {
	t.Helper()
	var high int64
	err := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(state.high, 0)
		FROM event_owner_incarnations AS owner
		LEFT JOIN event_owner_stream_state AS state ON state.owner_incarnation = owner.incarnation
		WHERE owner.user_id = $1
	`, userID).Scan(&high)
	if err != nil {
		t.Fatalf("read committed high: %v", err)
	}
	return high
}

func orderedStateIdentity(ctx context.Context, t *testing.T, db *testpostgres.Database, userID int64) (uuid.UUID, uuid.UUID) {
	t.Helper()
	var epoch, incarnation uuid.UUID
	err := db.Pool.QueryRow(ctx, `
		SELECT allocator.stream_epoch, owner.incarnation
		FROM event_stream_allocator AS allocator
		JOIN event_owner_incarnations AS owner ON owner.user_id = $1
		WHERE allocator.id = 1
	`, userID).Scan(&epoch, &incarnation)
	if err != nil {
		t.Fatalf("read ordered identity: %v", err)
	}
	return epoch, incarnation
}

func parseOrderedCursorParts(t *testing.T, value string) orderedCursorParts {
	t.Helper()
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		t.Fatalf("ordered cursor = %q", value)
	}
	epoch, err := uuid.Parse(parts[0])
	if err != nil {
		t.Fatalf("cursor epoch: %v", err)
	}
	incarnation, err := uuid.Parse(parts[1])
	if err != nil {
		t.Fatalf("cursor incarnation: %v", err)
	}
	position, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		t.Fatalf("cursor position: %v", err)
	}
	return orderedCursorParts{Epoch: epoch, Incarnation: incarnation, Position: position, Raw: value}
}

func decodeOrderedFrame(t *testing.T, frame map[string]string) orderedEventData {
	t.Helper()
	var data orderedEventData
	if err := json.Unmarshal([]byte(frame["data"]), &data); err != nil {
		t.Fatalf("decode ordered frame %#v: %v", frame, err)
	}
	return data
}

func serverRequest(t *testing.T, endpoint string, headers map[string]string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		cancel()
		t.Fatalf("request %s: %v", endpoint, err)
	}
	return response
}

func doOrderedRequest(t *testing.T, method, endpoint string, headers map[string]string) *http.Response {
	t.Helper()
	return serverRequest(t, endpoint, headers)
}

func assertNoOrderedBaseline(t *testing.T, response *http.Response) {
	t.Helper()
	body := readResponseBody(t, response)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("credential denial status = %d body = %s", response.StatusCode, body)
	}
	if strings.Contains(body, "sync.ready") || response.Header.Get("X-Teldrive-Events") != "" {
		t.Fatalf("credential denial leaked ordered baseline: headers %v body %s", response.Header, body)
	}
}

func readResponseBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(body)
}

func sha256Bytes(value string) []byte {
	hash := sha256.Sum256([]byte(value))
	return hash[:]
}

func insertOrderedEvents(t *testing.T, db *testpostgres.Database, userID int64, count int, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for remaining := count; remaining > 0; remaining -= 500 {
		batch := min(remaining, 500)
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO user_events (user_id, event_type, resource_type, resource_id, generation, payload)
			SELECT $1, 'file.created', 'file', $2 || '-' || series::text, 1, '{}'::jsonb
			FROM generate_series(1, $3) AS series
		`, userID, prefix, batch); err != nil {
			t.Fatalf("seed ordered events: %v", err)
		}
	}
}

func deleteOrderedEventsAbove(t *testing.T, db *testpostgres.Database, userID, position int64, batch int64) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if _, err := db.Pool.Exec(ctx, `
			DELETE FROM user_events
			WHERE user_id = $1 AND strict_position = ANY (
				SELECT strict_position FROM user_events
				WHERE user_id = $1 AND strict_position > $2
				ORDER BY strict_position
				LIMIT $3
			)
		`, userID, position, batch); err != nil {
			return err
		}
		var remaining int64
		if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE user_id = $1 AND strict_position > $2", userID, position).Scan(&remaining); err != nil {
			return err
		}
		if remaining == 0 {
			return nil
		}
		if batch > remaining {
			batch = remaining
		}
	}
}

func withHeader(headers map[string]string, name, value string) map[string]string {
	result := make(map[string]string, len(headers)+1)
	for key, existing := range headers {
		result[key] = existing
	}
	result[name] = value
	return result
}

func readRequiredReason(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	for {
		frame := readSSEFrame(t, reader)
		if frame["event"] != "sync.required" {
			continue
		}
		var payload struct {
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal([]byte(frame["data"]), &payload); err != nil {
			t.Fatalf("decode required frame: %v", err)
		}
		return payload.Reason
	}
}
