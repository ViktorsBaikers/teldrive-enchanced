//go:build integration

package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type channelHTTPAuth struct{}

func (channelHTTPAuth) AuthenticateBearer(_ context.Context, token string) (api.Identity, error) {
	switch token {
	case "channel-owner":
		return api.Identity{UserID: 1001}, nil
	case "channel-foreign":
		return api.Identity{UserID: 1002}, nil
	}
	return api.Identity{}, api.ErrUnauthenticated
}
func (a channelHTTPAuth) AuthenticateAPIKey(ctx context.Context, token string) (api.Identity, error) {
	return a.AuthenticateBearer(ctx, token)
}

type channelHTTPRemote struct {
	*fixture.Storage
	sent, deleted                atomic.Int32
	reserveEntered, sendRelease  chan struct{}
	deleteEntered, deleteRelease chan struct{}
	failure                      error
	accepted                     bool
}

func (r *channelHTTPRemote) Upload(ctx context.Context, in telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	r.sent.Add(1)
	if r.reserveEntered != nil {
		close(r.reserveEntered)
		select {
		case <-r.sendRelease:
		case <-ctx.Done():
			return telegramstore.StoredPart{}, ctx.Err()
		}
	}
	return r.Storage.Upload(ctx, in)
}
func (r *channelHTTPRemote) DeleteChannel(ctx context.Context, u, c int64) error {
	r.deleted.Add(1)
	if r.deleteEntered != nil {
		close(r.deleteEntered)
		select {
		case <-r.deleteRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.failure != nil && !r.accepted {
		return r.failure
	}
	if err := r.Storage.DeleteChannel(ctx, u, c); err != nil {
		return err
	}
	return r.failure
}

type channelHTTPFixture struct {
	db     *testpostgres.Database
	remote *channelHTTPRemote
	ch     *channels.Service
	up     *uploads.Service
	pipe   *transfer.Pipeline
	server http.Handler
	id     uuid.UUID
	body   []byte
}

func newChannelHTTP(t *testing.T) *channelHTTPFixture {
	t.Helper()
	ctx := context.Background()
	db := testpostgres.New(t)
	if _, e := db.Pool.Exec(ctx, "INSERT INTO users(user_id) VALUES(1001),(1002); INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'selected',true),(9002,1001,'explicit',false)"); e != nil {
		t.Fatal(e)
	}
	r := &channelHTTPRemote{Storage: fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}, {ChannelID: 9002, Actors: []int64{1001, 1002}}})}
	if e := r.Seed(fixture.Message{UserID: 1002, ChannelID: 9001, MessageID: 70, Bytes: []byte("foreign source manifest"), Preserve: true}); e != nil {
		t.Fatal(e)
	}
	if e := r.Seed(fixture.Message{UserID: 1001, ChannelID: 9002, MessageID: 80, Bytes: []byte("explicit unreferenced bytes")}); e != nil {
		t.Fatal(e)
	}
	policy := lifecycle.NewService(db.Pool, r)
	runtime, e := jobs.NewRuntime(db.Pool, r, policy)
	if e != nil {
		t.Fatal(e)
	}
	if e = policy.BindReceiptEnqueuer(runtime); e != nil {
		t.Fatal(e)
	}
	cat := catalog.NewService(db.Pool, nil)
	up := uploads.NewService(db.Pool)
	if e = cat.BindLifecycle(policy); e != nil {
		t.Fatal(e)
	}
	if e = up.BindLifecycle(policy); e != nil {
		t.Fatal(e)
	}
	ch := channels.NewService(db.Pool, channels.TelegramCreator{Storage: r}, channels.Config{PartLimit: 100})
	pipe := transfer.NewPipeline(up, ch, r, nil, transfer.Config{Receipts: policy})
	handler := api.NewHandler(cat, up, pipe, nil, health.NewService("synthetic", db.Pool), 0, nil).ConfigureDomains(nil, nil, ch, nil, nil, nil)
	server, e := api.NewServer(handler, api.NewSecurity(channelHTTPAuth{}))
	if e != nil {
		t.Fatal(e)
	}
	body := []byte("HTTP independently hashed winner\x00\xff")
	session, e := up.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "winner.bin", ExpectedSize: int64(len(body))})
	if e != nil {
		t.Fatal(e)
	}
	return &channelHTTPFixture{db: db, remote: r, ch: ch, up: up, pipe: pipe, server: server, id: uuid.UUID(session.ID.Bytes), body: body}
}
func (f *channelHTTPFixture) request(ctx context.Context, method, path, token string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body)).WithContext(ctx)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost {
		req.Header.Set("Idempotency-Key", uuid.NewString())
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Content-Length", fmt.Sprint(len(body)))
	}
	out := httptest.NewRecorder()
	f.server.ServeHTTP(out, req)
	return out
}
func (f *channelHTTPFixture) put(ctx context.Context) *httptest.ResponseRecorder {
	return f.request(ctx, http.MethodPut, "/v1/uploads/"+f.id.String()+"/parts/1?channelId=9002", "channel-owner", f.body)
}
func (f *channelHTTPFixture) complete(ctx context.Context) *httptest.ResponseRecorder {
	return f.request(ctx, http.MethodPost, "/v1/uploads/"+f.id.String()+"/complete", "channel-owner", []byte(fmt.Sprintf(`{"size":%d}`, len(f.body))))
}
func channelHTTPError(t *testing.T, r *httptest.ResponseRecorder, status int) {
	t.Helper()
	if r.Code != status {
		t.Fatalf("HTTP %d want %d: %s", r.Code, status, r.Body.String())
	}
	if status >= 400 {
		var envelope struct {
			Error struct{ Code, Message string }
		}
		if e := json.Unmarshal(r.Body.Bytes(), &envelope); e != nil || envelope.Error.Code == "" || envelope.Error.Message == "" {
			t.Fatalf("missing generated error envelope: %s %v", r.Body.String(), e)
		}
		if bytes.Contains(r.Body.Bytes(), []byte("synthetic private remote failure")) {
			t.Fatal("private cause exposed")
		}
	}
}
func (f *channelHTTPFixture) snapshot(t *testing.T) string {
	t.Helper()
	var value string
	e := f.db.Pool.QueryRow(context.Background(), `SELECT jsonb_build_array((SELECT to_jsonb(s) FROM upload_sessions s WHERE id=$1),(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY part_no),'[]') FROM upload_parts p WHERE upload_id=$1),(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no),'[]') FROM file_parts p),(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM user_events e))::text`, f.id).Scan(&value)
	if e != nil {
		t.Fatal(e)
	}
	return value
}
func channelHTTPWait(t *testing.T, ctx context.Context, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func TestB2ChannelDeleteHTTP(t *testing.T) {
	t.Run("write_first_pipeline_complete", func(t *testing.T) {
		f := newChannelHTTP(t)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		f.remote.reserveEntered = make(chan struct{})
		f.remote.sendRelease = make(chan struct{})
		uploaded := make(chan *httptest.ResponseRecorder, 1)
		go func() { uploaded <- f.put(ctx) }()
		channelHTTPWait(t, ctx, f.remote.reserveEntered)
		before := f.snapshot(t)
		channelHTTPError(t, f.request(ctx, http.MethodDelete, "/v1/channels/9002", "channel-owner", nil), 409)
		if before != f.snapshot(t) || f.remote.deleted.Load() != 0 {
			t.Fatal("write-first deletion mutated reservation or sent RPC")
		}
		close(f.remote.sendRelease)
		channelHTTPError(t, <-uploaded, 201)
		channelHTTPError(t, f.complete(ctx), 201)
		var message int64
		if e := f.db.Pool.QueryRow(ctx, "SELECT p.message_id FROM upload_sessions s JOIN file_parts p ON p.file_id=s.file_id WHERE s.id=$1", f.id).Scan(&message); e != nil {
			t.Fatal(e)
		}
		reader, e := f.remote.OpenRange(ctx, telegramstore.RangeRequest{UserID: 1001, ChannelID: 9002, MessageID: message, Length: -1})
		if e != nil {
			t.Fatal(e)
		}
		defer reader.Close()
		payload, e := io.ReadAll(reader)
		if e != nil || sha256.Sum256(payload) != sha256.Sum256(f.body) {
			t.Fatal("HTTP winner full SHA-256 changed")
		}
		if e = f.remote.CheckPreserved(); e != nil {
			t.Fatal(e)
		}
		before = f.snapshot(t)
		channelHTTPError(t, f.request(ctx, http.MethodDelete, "/v1/channels/9002", "channel-owner", nil), 409)
		if before != f.snapshot(t) || f.remote.deleted.Load() != 0 {
			t.Fatal("completed winner erased")
		}
	})
	t.Run("delete_first_pipeline_complete", func(t *testing.T) {
		for _, mode := range []string{"success", "unauthenticated", "wrong_owner", "cancelled", "ambiguous", "failed"} {
			t.Run(mode, func(t *testing.T) {
				f := newChannelHTTP(t)
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				defer cancel()
				token := "channel-owner"
				if mode == "unauthenticated" {
					token = ""
				}
				if mode == "wrong_owner" {
					token = "channel-foreign"
				}
				if mode == "unauthenticated" || mode == "wrong_owner" {
					before := f.snapshot(t)
					code := 401
					if mode == "wrong_owner" {
						code = 404
					}
					channelHTTPError(t, f.request(ctx, http.MethodDelete, "/v1/channels/9002", token, nil), code)
					if f.remote.deleted.Load() != 0 || before != f.snapshot(t) {
						t.Fatal("unauthorized channel Delete mutated state")
					}
					channelHTTPError(t, f.put(ctx), 201)
					channelHTTPError(t, f.complete(ctx), 201)
					return
				}
				f.remote.deleteEntered = make(chan struct{})
				f.remote.deleteRelease = make(chan struct{})
				if mode == "failed" || mode == "ambiguous" {
					f.remote.failure = errors.New("synthetic private remote failure")
					f.remote.accepted = mode == "ambiguous"
				}
				requestCtx, stop := context.WithCancel(ctx)
				defer stop()
				deleted := make(chan *httptest.ResponseRecorder, 1)
				clientError := make(chan error, 1)
				handlerDone := make(chan struct{})
				if mode == "cancelled" {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(handlerDone); f.server.ServeHTTP(w, r) }))
					defer server.Close()
					req, e := http.NewRequestWithContext(requestCtx, http.MethodDelete, server.URL+"/v1/channels/9002", nil)
					if e != nil {
						t.Fatal(e)
					}
					req.Header.Set("Authorization", "Bearer "+token)
					go func() {
						response, e := server.Client().Do(req)
						if response != nil {
							response.Body.Close()
						}
						clientError <- e
					}()
				} else {
					go func() { deleted <- f.request(requestCtx, http.MethodDelete, "/v1/channels/9002", token, nil) }()
				}
				channelHTTPWait(t, ctx, f.remote.deleteEntered)
				before := f.snapshot(t)
				channelHTTPError(t, f.put(ctx), 409)
				completion := f.complete(ctx)
				if completion.Code < 400 {
					t.Fatalf("loser published: %d %s", completion.Code, completion.Body.String())
				}
				if f.remote.sent.Load() != 0 || before != f.snapshot(t) {
					t.Fatal("delete-first writer sent or published")
				}
				// Independent acquisition of exact owner/channel locks proves RPC is outside SQL.
				tx, e := f.db.Pool.Begin(ctx)
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Rollback(context.Background())
				if e = lifecycle.LockOwnerTx(ctx, tx, 1001); e != nil {
					t.Fatal(e)
				}
				if e = lifecycle.LockChannelWritesTx(ctx, tx, []int64{9002}); e != nil {
					t.Fatal(e)
				}
				if e = tx.Rollback(ctx); e != nil {
					t.Fatal(e)
				}
				if mode == "cancelled" {
					stop()
					select {
					case e := <-clientError:
						if !errors.Is(e, context.Canceled) {
							t.Fatalf("cancelled Delete client error: %v", e)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					channelHTTPWait(t, ctx, handlerDone)
				} else {
					close(f.remote.deleteRelease)
					out := <-deleted
					if mode == "success" {
						channelHTTPError(t, out, 204)
						if _, ok := f.remote.Bytes(9002, 80); ok {
							t.Fatal("fake Delete did not destroy bytes")
						}
					} else {
						if out.Code < 400 || out.Code == 204 {
							t.Fatalf("failed Delete returned success %d", out.Code)
						}
						channelHTTPError(t, out, out.Code)
					}
				}
				var state string
				if e = f.db.Pool.QueryRow(ctx, "SELECT state FROM channel_deletion_intents WHERE channel_id=9002").Scan(&state); e != nil {
					t.Fatal(e)
				}
				want := "closing"
				if mode == "success" {
					want = "deleted"
				}
				if state != want || f.remote.deleted.Load() != 1 {
					t.Fatalf("gate %s, sends %d", state, f.remote.deleted.Load())
				}
				if e = f.remote.CheckPreserved(); e != nil {
					t.Fatal(e)
				}
			})
		}
	})
}
