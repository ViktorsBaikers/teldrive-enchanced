//go:build integration

package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divyam234/riverpro/riverencrypt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/shares"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type lifecycleAPIFixture struct {
	pool     *pgxpool.Pool
	catalog  *catalog.Service
	uploads  *uploads.Service
	fileops  *fileops.Service
	shares   *shares.Service
	storage  *lifecycleAPIStorage
	server   http.Handler
	folder   uuid.UUID
	private  uuid.UUID
	public   string
	readOnly string
	grant    uuid.UUID
}

func newLifecycleAPI(t *testing.T) *lifecycleAPIFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, `INSERT INTO users(user_id) VALUES(1001),(1002),(1003),(1004); INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'fixture',true)`); err != nil {
		t.Fatal(err)
	}
	f := &lifecycleAPIFixture{pool: db.Pool, catalog: catalog.NewService(db.Pool, nil), uploads: uploads.NewService(db.Pool), storage: &lifecycleAPIStorage{}}
	policy := bindAPIReceiptPolicy(t, db.Pool, f.storage, f.catalog, f.uploads)
	channelService := channels.NewService(db.Pool, nil, channels.Config{})
	var err error
	f.fileops, err = fileops.NewService(db.Pool, f.catalog, channelService, f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.fileops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	f.shares, err = shares.NewService(db.Pool, f.catalog)
	if err != nil {
		t.Fatal(err)
	}
	f.folder = f.newFolder(t, "shared")
	f.private = f.newFolder(t, "private")
	password := "fixture-password"
	public, err := f.shares.Create(ctx, shares.CreateInput{OwnerID: 1001, FileID: f.folder, Password: &password, Permission: sqlcgen.SharePermissionEdit})
	if err != nil {
		t.Fatal(err)
	}
	f.public = public.Token
	readOnly, err := f.shares.Create(ctx, shares.CreateInput{OwnerID: 1001, FileID: f.folder, Permission: sqlcgen.SharePermissionRead})
	if err != nil {
		t.Fatal(err)
	}
	f.readOnly = readOnly.Token
	grant, err := f.shares.CreateGrant(ctx, shares.GrantCreateInput{OwnerID: 1001, FileID: f.folder, GranteeID: 1002, Permission: sqlcgen.SharePermissionEdit})
	if err != nil {
		t.Fatal(err)
	}
	f.grant = uuid.UUID(grant.ID.Bytes)
	if _, err = f.shares.CreateGrant(ctx, shares.GrantCreateInput{OwnerID: 1001, FileID: f.folder, GranteeID: 1003, Permission: sqlcgen.SharePermissionRead}); err != nil {
		t.Fatal(err)
	}
	child, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &f.folder, Name: "destination"})
	if err != nil {
		t.Fatal(err)
	}
	f.folder = uuid.UUID(child.ID.Bytes)
	pipeline := transfer.NewPipeline(f.uploads, channelService, f.storage, nil, transfer.Config{Receipts: policy})
	handler := api.NewHandler(f.catalog, f.uploads, pipeline, transfer.NewDownloader(f.catalog, f.storage, nil), health.NewService("fixture", db.Pool), 0, nil).ConfigureDomains(nil, nil, channelService, f.fileops, f.shares, nil)
	f.server, err = api.NewServer(handler, api.NewSecurity(tokenUserAuthenticator{"owner": 1001, "editor": 1002, "reader": 1003, "foreign": 1004}))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *lifecycleAPIFixture) newFolder(t *testing.T, name string) uuid.UUID {
	t.Helper()
	folder, err := f.catalog.CreateFolder(context.Background(), catalog.CreateFolderInput{UserID: 1001, Name: name, ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return uuid.UUID(folder.ID.Bytes)
}

func (f *lifecycleAPIFixture) route(actor string) (string, map[string]string) {
	headers := map[string]string{"Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
	base := "/v1/uploads"
	if actor == "public" {
		base = "/v1/public/shares/" + f.public + "/uploads"
		headers["X-Share-Password"] = "fixture-password"
	} else {
		headers["Authorization"] = "Bearer " + actor
	}
	return base, headers
}

func (f *lifecycleAPIFixture) create(t *testing.T, actor string, size int64) (uuid.UUID, string, map[string]string) {
	t.Helper()
	base, headers := f.route(actor)
	body := []byte(fmt.Sprintf(`{"parentId":%q,"name":%q,"size":%d,"modTime":"2026-07-01T00:00:00Z","preferredPartSize":1048576}`, f.folder, uuid.NewString(), size))
	response := performRequest(t, f.server, http.MethodPost, base, body, headers)
	if response.Code != 201 {
		t.Fatalf("create: %d %s", response.Code, response.Body.String())
	}
	var session gen.UploadSession
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	if !session.RequiresCompletionManifest {
		t.Fatal("completion capability lost")
	}
	id := uuid.UUID(session.ID)
	return id, base + "/" + id.String(), headers
}

func assertParentUnavailable(t *testing.T, status int, body []byte) {
	t.Helper()
	assertB1APIError(t, status, body, 409, "upload_parent_unavailable")
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		}
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Message != "upload destination folder is unavailable" {
		t.Fatalf("unsafe message: %s", body)
	}
}

func TestB2LifecycleContract(t *testing.T) {
	for _, actor := range []string{"owner", "editor", "public"} {
		for _, size := range []int64{0, 3} {
			t.Run(fmt.Sprintf("%s/size_%d", actor, size), func(t *testing.T) {
				f := newLifecycleAPI(t)
				id, path, headers := f.create(t, actor, size)
				if size > 0 {
					headers["Content-Type"] = "application/octet-stream"
					r := performRequest(t, f.server, http.MethodPut, path+"/parts/1", []byte("abc"), headers)
					if r.Code != 201 {
						t.Fatalf("put: %d %s", r.Code, r.Body.String())
					}
				}
				if _, err := f.catalog.Trash(context.Background(), 1001, f.folder); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Restore(context.Background(), 1001, f.folder); err != nil {
					t.Fatal(err)
				}
				headers["Content-Type"] = "application/json"
				for range 2 {
					r := performRequest(t, f.server, http.MethodPost, path+"/complete", []byte(fmt.Sprintf(`{"size":%d}`, size)), headers)
					assertParentUnavailable(t, r.Code, r.Body.Bytes())
				}
				get := performRequest(t, f.server, http.MethodGet, "/v1/uploads/"+id.String(), nil, map[string]string{"Authorization": "Bearer owner"})
				assertParentUnavailable(t, get.Code, get.Body.Bytes())
				var reason string
				if err := f.pool.QueryRow(context.Background(), `SELECT terminal_failure_code FROM upload_sessions WHERE id=$1`, id).Scan(&reason); err != nil {
					t.Fatal(err)
				}
				if reason != "upload_parent_unavailable" {
					t.Fatalf("reason: %s", reason)
				}
				var count int
				if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM files WHERE kind='file'`).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatal("terminal upload published")
				}
			})
		}
	}
	t.Run("denied_before_terminal", func(t *testing.T) {
		f := newLifecycleAPI(t)
		_, path, headers := f.create(t, "owner", 0)
		if _, err := f.catalog.Trash(context.Background(), 1001, f.folder); err != nil {
			t.Fatal(err)
		}
		for _, actor := range []string{"reader", "foreign"} {
			headers["Authorization"] = "Bearer " + actor
			r := performRequest(t, f.server, http.MethodPost, path+"/complete", []byte(`{"size":0}`), headers)
			if r.Code != 403 && r.Code != 404 {
				t.Fatalf("denied %s: %d %s", actor, r.Code, r.Body.String())
			}
			if bytes.Contains(r.Body.Bytes(), []byte("upload_parent_unavailable")) {
				t.Fatal("terminal disclosed")
			}
		}
	})
	t.Run("completed_put_current_result_access", testLifecycleCompletedPUT)
	t.Run("legacy_unknown_owner", testLifecycleUnknownOwner)
	t.Run("legacy_unknown_editor_public_denied", testLifecycleUnknownDenied)
	t.Run("legacy_completed_current_result", testLifecycleLegacyCompleted)
}

type lifecycleAPIStorage struct {
	apiMemoryStorage
	afterUpload func() error
	afterCopy   func() error
	deleteErr   error
}

func (s *lifecycleAPIStorage) Upload(ctx context.Context, in telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	part, err := s.apiMemoryStorage.Upload(ctx, in)
	if err == nil && s.afterUpload != nil {
		err = s.afterUpload()
	}
	return part, err
}

func (s *lifecycleAPIStorage) CopyPart(ctx context.Context, user, sourceChannel, sourceMessage, destination int64) (telegramstore.StoredPart, error) {
	part, err := s.apiMemoryStorage.CopyPart(ctx, user, sourceChannel, sourceMessage, destination)
	if err == nil && s.afterCopy != nil {
		err = s.afterCopy()
	}
	return part, err
}

func (s *lifecycleAPIStorage) DeleteMessages(ctx context.Context, user, channel int64, ids []int64) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.apiMemoryStorage.DeleteMessages(ctx, user, channel, ids)
}

func (f *lifecycleAPIFixture) reopen(t *testing.T) {
	t.Helper()
	f.catalog = catalog.NewService(f.pool, nil)
	f.uploads = uploads.NewService(f.pool)
	policy := bindAPIReceiptPolicy(t, f.pool, f.storage, f.catalog, f.uploads)
	channelService := channels.NewService(f.pool, nil, channels.Config{})
	var err error
	f.fileops, err = fileops.NewService(f.pool, f.catalog, channelService, f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.fileops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	f.shares, err = shares.NewService(f.pool, f.catalog)
	if err != nil {
		t.Fatal(err)
	}
	pipeline := transfer.NewPipeline(f.uploads, channelService, f.storage, nil, transfer.Config{Receipts: policy})
	handler := api.NewHandler(f.catalog, f.uploads, pipeline, transfer.NewDownloader(f.catalog, f.storage, nil), health.NewService("fixture", f.pool), 0, nil).ConfigureDomains(nil, nil, channelService, f.fileops, f.shares, nil)
	f.server, err = api.NewServer(handler, api.NewSecurity(tokenUserAuthenticator{"owner": 1001, "editor": 1002, "reader": 1003, "foreign": 1004}))
	if err != nil {
		t.Fatal(err)
	}
}

func (f *lifecycleAPIFixture) publish(t *testing.T, actor string) (uuid.UUID, uuid.UUID, string, map[string]string) {
	t.Helper()
	id, path, headers := f.create(t, actor, 3)
	headers["Content-Type"] = "application/octet-stream"
	put := performRequest(t, f.server, http.MethodPut, path+"/parts/1", []byte("abc"), headers)
	if put.Code != 201 {
		t.Fatalf("put: %d %s", put.Code, put.Body.String())
	}
	headers["Content-Type"] = "application/json"
	response := performRequest(t, f.server, http.MethodPost, path+"/complete", []byte(`{"size":3}`), headers)
	if response.Code != 201 {
		t.Fatalf("complete: %d %s", response.Code, response.Body.String())
	}
	var file gen.FileEntry
	if err := json.Unmarshal(response.Body.Bytes(), &file); err != nil {
		t.Fatal(err)
	}
	var aliases int
	if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&aliases); err != nil {
		t.Fatal(err)
	}
	if aliases != 0 {
		t.Fatal("owned completed aliases were not retired")
	}
	return id, uuid.UUID(file.ID), path, headers
}

func (f *lifecycleAPIFixture) snapshot(t *testing.T) string {
	t.Helper()
	var value string
	err := f.pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
 'sessions',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM upload_sessions s),
 'parts',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no),'[]') FROM upload_parts p),
 'files',(SELECT coalesce(jsonb_agg(to_jsonb(f) ORDER BY id),'[]') FROM files f),
 'file_parts',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no),'[]') FROM file_parts p),
 'receipts',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),
 'admissions',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY id),'[]') FROM request_admissions a),
 'batches',(SELECT coalesce(jsonb_agg(to_jsonb(b) ORDER BY user_id,batch_id),'[]') FROM import_batches b),
 'pins',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY user_id,batch_id,destination_ordinal),'[]') FROM import_destination_pins p),
 'attempts',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY attempt_id),'[]') FROM import_job_attempts a),
 'events',(SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM user_events e))::text`).Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type lifecycleEOFReader struct {
	io.Reader
	once  sync.Once
	atEOF func()
}

func (r *lifecycleEOFReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF || n > 0 {
		r.once.Do(r.atEOF)
	}
	return n, err
}

func lifecycleAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle barrier not reached")
	}
}

func testLifecycleCompletedPUT(t *testing.T) {
	for _, actor := range []string{"owner", "editor", "public"} {
		for _, change := range []string{"unchanged", "move", "revoke", "trash", "purge", "inactive_ancestor"} {
			for _, body := range []string{"abc", "xyz", "ab", "abcd"} {
				t.Run(actor+"/"+change+"/"+body, func(t *testing.T) {
					f := newLifecycleAPI(t)
					id, fileID, path, headers := f.publish(t, actor)
					before := f.snapshot(t)
					headers["Content-Type"] = "application/octet-stream"
					reached, release := make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
					reader := &lifecycleEOFReader{Reader: strings.NewReader(body), atEOF: func() {
						close(reached)
						select {
						case <-release:
						case <-time.After(5 * time.Second):
						}
					}}
					req := httptest.NewRequest(http.MethodPut, path+"/parts/1", nil)
					req.Body = io.NopCloser(reader)
					req.ContentLength = 3
					for key, value := range headers {
						req.Header.Set(key, value)
					}
					req.Header.Set("Content-Length", "3")
					out := httptest.NewRecorder()
					done := make(chan struct{})
					go func() { defer close(done); f.server.ServeHTTP(out, req) }()
					lifecycleAwait(t, reached)
					switch change {
					case "move":
						if _, err := f.catalog.MoveWithPolicy(context.Background(), 1001, fileID, &f.private, nil, "fail"); err != nil {
							t.Fatal(err)
						}
					case "revoke":
						if actor == "public" {
							resolved, err := f.shares.Resolve(context.Background(), f.public, "fixture-password")
							if err != nil {
								t.Fatal(err)
							}
							if err = f.shares.Revoke(context.Background(), 1001, uuid.UUID(resolved.Share.ID.Bytes)); err != nil {
								t.Fatal(err)
							}
						} else if actor == "editor" {
							if err := f.shares.RevokeGrant(context.Background(), 1001, f.grant); err != nil {
								t.Fatal(err)
							}
						}
					case "inactive_ancestor":
						// The target stays active; only its historical ancestor is unavailable.
						if _, err := f.pool.Exec(context.Background(), "UPDATE files SET status='trashed',deleted_at=clock_timestamp() WHERE id=$1", f.folder); err != nil {
							t.Fatal(err)
						}
					case "trash", "purge":
						if _, err := f.catalog.Trash(context.Background(), 1001, fileID); err != nil {
							t.Fatal(err)
						}
						if change == "purge" {
							if err := f.fileops.QueuePurge(context.Background(), 1001, fileID); err != nil {
								t.Fatal(err)
							}
							if err := f.fileops.PurgeMany(context.Background(), 1001, []uuid.UUID{fileID}); err != nil {
								t.Fatal(err)
							}
						}
					}
					afterChange := f.snapshot(t)
					releaseOnce.Do(func() { close(release) })
					lifecycleAwait(t, done)
					denied := change == "trash" || change == "purge" || change == "inactive_ancestor" || (actor != "owner" && (change == "move" || change == "revoke"))
					if denied {
						if out.Code != 401 && out.Code != 403 && out.Code != 404 && out.Code != 410 {
							t.Fatalf("current access not enforced: %d %s", out.Code, out.Body.String())
						}
					} else if body == "abc" {
						if out.Code != 200 {
							t.Fatalf("equal replay: %d %s", out.Code, out.Body.String())
						}
					} else if out.Code != 409 && out.Code != 422 {
						t.Fatalf("bad replay acknowledged: %d %s", out.Code, out.Body.String())
					}
					if f.snapshot(t) != afterChange {
						t.Fatal("replay changed durable rows/events")
					}
					if change == "unchanged" && afterChange != before {
						t.Fatal("read-only replay changed state")
					}
					var aliases int
					if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&aliases); err != nil {
						t.Fatal(err)
					}
					if aliases != 0 {
						t.Fatal("replay recreated alias")
					}
					if change != "purge" && change != "inactive_ancestor" {
						download := performRequest(t, f.server, http.MethodGet, "/v1/files/"+fileID.String()+"/content", nil, map[string]string{"Authorization": "Bearer owner"})
						if change != "trash" && (download.Code != 200 || sha256.Sum256(download.Body.Bytes()) != sha256.Sum256([]byte("abc"))) {
							t.Fatalf("winner bytes: %d %s", download.Code, download.Body.String())
						}
					}
				})
			}
		}
	}
	for _, body := range []string{"abc", "xyz", "ab", "abcd"} {
		t.Run("waiter_"+body, func(t *testing.T) { testLifecycleWaiter(t, body) })
	}
}

func legacyUnknownUpload(t *testing.T, f *lifecycleAPIFixture, size int64) (uuid.UUID, string, map[string]string) {
	t.Helper()
	id, path, headers := f.create(t, "owner", size)
	if size > 0 {
		headers["Content-Type"] = "application/octet-stream"
		r := performRequest(t, f.server, http.MethodPut, path+"/parts/1", []byte("abc"), headers)
		if r.Code != 201 {
			t.Fatalf("legacy put: %d %s", r.Code, r.Body.String())
		}
	}
	// This API-local cutover fixture is not the separate old-driver dump/restore proof.
	if _, err := f.pool.Exec(context.Background(), "UPDATE upload_sessions SET parent_id=NULL,terminal_failure_code='upload_destination_unknown' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	headers["Content-Type"] = "application/json"
	return id, path, headers
}

func testLifecycleUnknownOwner(t *testing.T) {
	for _, size := range []int64{0, 3} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			f := newLifecycleAPI(t)
			id, path, headers := legacyUnknownUpload(t, f, size)
			before := f.snapshot(t)
			for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPost} {
				route := path
				body := []byte(nil)
				switch method {
				case http.MethodPut:
					route += "/parts/1"
					body = []byte("abc")
					headers["Content-Type"] = "application/octet-stream"
				case http.MethodPost:
					route += "/complete"
					body = []byte(fmt.Sprintf(`{"size":%d}`, size))
					headers["Content-Type"] = "application/json"
				}
				r := performRequest(t, f.server, method, route, body, headers)
				assertParentUnavailable(t, r.Code, r.Body.Bytes())
				if bytes.Contains(r.Body.Bytes(), []byte("upload_destination_unknown")) || bytes.Contains(r.Body.Bytes(), []byte(f.folder.String())) {
					t.Fatal("legacy internal detail leaked")
				}
			}
			if _, err := f.uploads.Create(context.Background(), uploads.CreateInput{ID: id, UserID: 1001}); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("existing ID: %v", err)
			}
			if _, err := f.uploads.ClaimPart(context.Background(), uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: 3}); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("claim: %v", err)
			}
			if f.snapshot(t) != before {
				t.Fatal("unknown operation changed evidence")
			}
			f.storage.mu.Lock()
			defer f.storage.mu.Unlock()
			if size > 0 && sha256.Sum256(f.storage.messages[1]) != sha256.Sum256([]byte("abc")) {
				t.Fatal("unknown bytes changed")
			}
		})
	}
}

func testLifecycleUnknownDenied(t *testing.T) {
	for _, size := range []int64{0, 3} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			f := newLifecycleAPI(t)
			id, _, _ := legacyUnknownUpload(t, f, size)
			before := f.snapshot(t)
			for _, actor := range []string{"editor", "reader", "foreign", "public"} {
				base, headers := f.route(actor)
				path := base + "/" + id.String()
				for _, suffix := range []string{"/parts/1", "/complete"} {
					method := http.MethodPost
					body := []byte(fmt.Sprintf(`{"size":%d}`, size))
					headers["Content-Type"] = "application/json"
					if suffix == "/parts/1" {
						method = http.MethodPut
						body = []byte("abc")
						headers["Content-Type"] = "application/octet-stream"
					}
					r := performRequest(t, f.server, method, path+suffix, body, headers)
					if r.Code != 403 {
						t.Fatalf("%s unknown: %d %s", actor, r.Code, r.Body.String())
					}
					if bytes.Contains(r.Body.Bytes(), []byte("upload_parent_unavailable")) {
						t.Fatal("denied legacy reason leaked")
					}
				}
			}
			for _, denied := range []struct {
				token, password string
				status          int
			}{{f.public, "wrong", 401}, {f.readOnly, "", 403}} {
				r := performRequest(t, f.server, http.MethodPost, "/v1/public/shares/"+denied.token+"/uploads/"+id.String()+"/complete", []byte(fmt.Sprintf(`{"size":%d}`, size)), map[string]string{"Content-Type": "application/json", "Idempotency-Key": uuid.NewString(), "X-Share-Password": denied.password})
				if r.Code != denied.status || bytes.Contains(r.Body.Bytes(), []byte("upload_parent_unavailable")) {
					t.Fatalf("legacy credential denial: %d %s", r.Code, r.Body.String())
				}
			}
			if f.snapshot(t) != before {
				t.Fatal("denied legacy mutated")
			}
		})
	}
}

func testLifecycleLegacyCompleted(t *testing.T) {
	f := newLifecycleAPI(t)
	_, fileID, _, _ := f.publish(t, "owner")
	id, path, headers := f.create(t, "owner", 3)
	// API-local historical metadata has no receipt for this legacy origin.
	if _, err := f.pool.Exec(context.Background(), "UPDATE upload_sessions SET state='completed',file_id=$2,completed_at=clock_timestamp() WHERE id=$1", id, fileID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(context.Background(), `INSERT INTO upload_parts(upload_id,part_no,state,plain_size,stored_size,channel_id,message_id,checksum,block_hashes,created_at,updated_at)
SELECT $1,part_no,'stored',plain_size,stored_size,channel_id,message_id,checksum,block_hashes,clock_timestamp(),clock_timestamp() FROM file_parts WHERE file_id=$2`, id, fileID); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot(t)
	r := performRequest(t, f.server, http.MethodPost, path+"/complete", []byte(`{"size":3}`), headers)
	if r.Code != 201 {
		t.Fatalf("legacy completion: %d %s", r.Code, r.Body.String())
	}
	if f.snapshot(t) != before {
		t.Fatal("legacy replay rebound references")
	}
	if _, err := f.catalog.MoveWithPolicy(context.Background(), 1001, fileID, &f.private, nil, "fail"); err != nil {
		t.Fatal(err)
	}
	base, denied := f.route("public")
	r = performRequest(t, f.server, http.MethodPost, base+"/"+id.String()+"/complete", []byte(`{"size":3}`), denied)
	if r.Code != 404 {
		t.Fatalf("legacy current result: %d %s", r.Code, r.Body.String())
	}
	if _, err := f.catalog.Trash(context.Background(), 1001, fileID); err != nil {
		t.Fatal(err)
	}
	r = performRequest(t, f.server, http.MethodPost, path+"/complete", []byte(`{"size":3}`), headers)
	if r.Code != 404 {
		t.Fatalf("legacy trash replay: %d %s", r.Code, r.Body.String())
	}
	if err := f.fileops.QueuePurge(context.Background(), 1001, fileID); err != nil {
		t.Fatal(err)
	}
	if err := f.fileops.PurgeMany(context.Background(), 1001, []uuid.UUID{fileID}); !errors.Is(err, fileops.ErrPurgeIncomplete) {
		t.Fatalf("legacy alias must retain physical evidence: %v", err)
	}
	r = performRequest(t, f.server, http.MethodPost, path+"/complete", []byte(`{"size":3}`), headers)
	if r.Code != 404 {
		t.Fatalf("legacy purge replay: %d %s", r.Code, r.Body.String())
	}
	f.storage.mu.Lock()
	defer f.storage.mu.Unlock()
	if sha256.Sum256(f.storage.messages[1]) != sha256.Sum256([]byte("abc")) {
		t.Fatal("legacy alias bytes were deleted")
	}
}

// Response loss happens after the real generated handler and SQL commit.
func lifecycleRetryHTTP(t *testing.T, f *lifecycleAPIFixture, path string, body []byte, headers map[string]string, truncated bool, afterLoss func()) (int, []byte, http.Header) {
	t.Helper()
	var calls atomic.Int32
	lost := make(chan struct{})
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			f.server.ServeHTTP(w, r)
			return
		}
		recorded := httptest.NewRecorder()
		f.server.ServeHTTP(recorded, r)
		if afterLoss != nil {
			afterLoss()
		}
		close(lost)
		if truncated {
			for key, values := range recorded.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.Header().Set("Content-Length", fmt.Sprint(recorded.Body.Len()+32))
			w.WriteHeader(recorded.Code)
			_, _ = w.Write(recorded.Body.Bytes()[:recorded.Body.Len()/2])
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer host.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	for attempt := range 2 {
		if attempt > 0 {
			lifecycleAwait(t, lost)
		}
		req, err := http.NewRequest(http.MethodPost, host.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		response, err := client.Do(req)
		if err != nil {
			if attempt == 0 {
				continue
			}
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			if attempt == 0 {
				continue
			}
			t.Fatal(readErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if calls.Load() != 2 {
			t.Fatalf("response-loss retry calls=%d", calls.Load())
		}
		return response.StatusCode, data, response.Header
	}
	t.Fatal("bounded replay did not return")
	return 0, nil, nil
}

func TestB2UploadAdmissionHTTP(t *testing.T) {
	for _, name := range []string{"lost_create_ack", "truncated_terminal_reply", "fingerprint_mismatch", "owner_editor_public_scope", "current_result_retention"} {
		t.Run(name, func(t *testing.T) {
			f := newLifecycleAPI(t)
			body := []byte(fmt.Sprintf(`{"parentId":%q,"name":"admitted.bin","size":0,"modTime":"2026-07-01T00:00:00Z"}`, f.folder))
			base, headers := f.route("owner")
			switch name {
			case "lost_create_ack":
				var before string
				status, data, _ := lifecycleRetryHTTP(t, f, base, body, headers, false, func() { before = f.snapshot(t); f.reopen(t) })
				if status != 201 {
					t.Fatalf("lost create: %d %s", status, data)
				}
				if f.snapshot(t) != before {
					t.Fatal("create replay changed rows/events")
				}
			case "truncated_terminal_reply":
				created := performRequest(t, f.server, http.MethodPost, base, body, headers)
				if created.Code != 201 {
					t.Fatalf("initial: %d %s", created.Code, created.Body.String())
				}
				if _, err := f.catalog.Trash(context.Background(), 1001, f.folder); err != nil {
					t.Fatal(err)
				}
				var before string
				status, data, _ := lifecycleRetryHTTP(t, f, base, body, headers, true, func() {
					if _, err := f.catalog.Restore(context.Background(), 1001, f.folder); err != nil {
						t.Error(err)
					}
					before = f.snapshot(t)
					f.reopen(t)
				})
				assertParentUnavailable(t, status, data)
				if f.snapshot(t) != before {
					t.Fatal("terminal replay allocated or mutated")
				}
			case "fingerprint_mismatch":
				r := performRequest(t, f.server, http.MethodPost, base, body, headers)
				if r.Code != 201 {
					t.Fatalf("initial: %d %s", r.Code, r.Body.String())
				}
				before := f.snapshot(t)
				changed := bytes.Replace(body, []byte("admitted.bin"), []byte("changed.bin"), 1)
				r = performRequest(t, f.server, http.MethodPost, base, changed, headers)
				assertB1APIError(t, r.Code, r.Body.Bytes(), 409, "conflict")
				if f.snapshot(t) != before {
					t.Fatal("mismatch changed admission")
				}
				for _, field := range []struct {
					name  string
					value any
				}{
					{"parentId", f.private.String()}, {"size", int64(3)}, {"mimeType", "application/octet-stream"},
					{"hash", map[string]string{"algorithm": "blake3", "value": strings.Repeat("a", 64)}},
					{"modTime", "2026-07-01T00:00:01Z"}, {"encryption", true}, {"conflictPolicy", "rename"}, {"preferredPartSize", int64(16 << 20)},
				} {
					t.Run(field.name, func(t *testing.T) {
						var request map[string]any
						if err := json.Unmarshal(body, &request); err != nil {
							t.Fatal(err)
						}
						request[field.name] = field.value
						encoded, err := json.Marshal(request)
						if err != nil {
							t.Fatal(err)
						}
						out := performRequest(t, f.server, http.MethodPost, base, encoded, headers)
						assertB1APIError(t, out.Code, out.Body.Bytes(), 409, "conflict")
						if f.snapshot(t) != before {
							t.Fatal("changed caller field mutated admission")
						}
					})
				}
				var defaults map[string]any
				if err := json.Unmarshal(body, &defaults); err != nil {
					t.Fatal(err)
				}
				defaults["encryption"] = false
				defaults["conflictPolicy"] = "fail"
				defaults["modTime"] = "2026-07-01T01:00:00+01:00"
				encoded, err := json.Marshal(defaults)
				if err != nil {
					t.Fatal(err)
				}
				out := performRequest(t, f.server, http.MethodPost, base, encoded, headers)
				if out.Code != 201 || f.snapshot(t) != before {
					t.Fatalf("normalized defaults/UTC changed identity: %d %s", out.Code, out.Body.String())
				}
			case "owner_editor_public_scope":
				key := headers["Idempotency-Key"]
				ids := map[uuid.UUID]bool{}
				for _, actor := range []string{"owner", "editor", "public"} {
					base, actorHeaders := f.route(actor)
					actorHeaders["Idempotency-Key"] = key
					r := performRequest(t, f.server, http.MethodPost, base, body, actorHeaders)
					if r.Code != 201 {
						t.Fatalf("%s create: %d %s", actor, r.Code, r.Body.String())
					}
					var session gen.UploadSession
					if err := json.Unmarshal(r.Body.Bytes(), &session); err != nil {
						t.Fatal(err)
					}
					if ids[uuid.UUID(session.ID)] {
						t.Fatal("actor/share scope collapsed")
					}
					ids[uuid.UUID(session.ID)] = true
					again := performRequest(t, f.server, http.MethodPost, base, body, actorHeaders)
					if again.Code != 201 || !bytes.Equal(r.Body.Bytes(), again.Body.Bytes()) {
						t.Fatalf("%s scope replay: %d %s", actor, again.Code, again.Body.String())
					}
				}
				for _, actor := range []string{"reader", "foreign"} {
					base, denied := f.route(actor)
					denied["Idempotency-Key"] = key
					r := performRequest(t, f.server, http.MethodPost, base, body, denied)
					if r.Code != 403 {
						t.Fatalf("scope denied %s: %d %s", actor, r.Code, r.Body.String())
					}
				}
				public, publicHeaders := f.route("public")
				publicHeaders["Idempotency-Key"] = key
				publicHeaders["X-Share-Password"] = "wrong"
				r := performRequest(t, f.server, http.MethodPost, public, body, publicHeaders)
				if r.Code != 401 {
					t.Fatalf("password: %d %s", r.Code, r.Body.String())
				}
			case "current_result_retention":
				base, headers = f.route("editor")
				r := performRequest(t, f.server, http.MethodPost, base, body, headers)
				if r.Code != 201 {
					t.Fatalf("create: %d %s", r.Code, r.Body.String())
				}
				var session gen.UploadSession
				if err := json.Unmarshal(r.Body.Bytes(), &session); err != nil {
					t.Fatal(err)
				}
				completed := performRequest(t, f.server, http.MethodPost, base+"/"+uuid.UUID(session.ID).String()+"/complete", []byte(`{"size":0}`), headers)
				if completed.Code != 201 {
					t.Fatalf("complete: %d %s", completed.Code, completed.Body.String())
				}
				var file gen.FileEntry
				if err := json.Unmarshal(completed.Body.Bytes(), &file); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.MoveWithPolicy(context.Background(), 1001, uuid.UUID(file.ID), &f.private, nil, "fail"); err != nil {
					t.Fatal(err)
				}
				f.reopen(t)
				before := f.snapshot(t)
				for _, request := range [][]byte{body, bytes.Replace(body, []byte("admitted.bin"), []byte("changed.bin"), 1)} {
					r = performRequest(t, f.server, http.MethodPost, base, request, headers)
					if r.Code != 403 {
						t.Fatalf("current result before mismatch: %d %s", r.Code, r.Body.String())
					}
				}
				if f.snapshot(t) != before {
					t.Fatal("denied replay changed original")
				}
			}
			var count int
			if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM request_admissions").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 1
			if name == "owner_editor_public_scope" {
				want = 3
			}
			if count != want {
				t.Fatalf("admissions=%d want %d", count, want)
			}
		})
	}
}

func TestB2CopyAdmissionHTTP(t *testing.T) {
	for _, name := range []string{"terminal_reply_lost", "success_reply_lost", "fingerprint_mismatch", "current_result_access", "concurrent_restart"} {
		t.Run(name, func(t *testing.T) {
			f := newLifecycleAPI(t)
			_, source, _, _ := f.publish(t, "owner")
			destination := f.newFolder(t, "copy-destination")
			body := []byte(fmt.Sprintf(`{"parentId":%q,"name":"copy.bin"}`, destination))
			path := "/v1/files/" + source.String() + "/copy"
			headers := map[string]string{"Authorization": "Bearer owner", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
			if name == "terminal_reply_lost" {
				f.storage.afterCopy = func() error {
					if _, err := f.catalog.Trash(context.Background(), 1001, destination); err != nil {
						return err
					}
					_, err := f.catalog.Restore(context.Background(), 1001, destination)
					return err
				}
			}
			var status int
			var data []byte
			var responseHeaders http.Header
			switch name {
			case "terminal_reply_lost", "success_reply_lost":
				var before string
				status, data, responseHeaders = lifecycleRetryHTTP(t, f, path, body, headers, name == "terminal_reply_lost", func() { before = f.snapshot(t); f.reopen(t) })
				if f.snapshot(t) != before {
					t.Fatal("copy retry changed operation/result/event")
				}
			case "concurrent_restart":
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				t.Cleanup(func() { once.Do(func() { close(release) }) })
				f.storage.afterCopy = func() error {
					close(entered)
					select {
					case <-release:
						return nil
					case <-time.After(5 * time.Second):
						return context.DeadlineExceeded
					}
				}
				server := f.server
				first := make(chan *httptest.ResponseRecorder, 1)
				go func() { first <- performRequest(t, server, http.MethodPost, path, body, headers) }()
				lifecycleAwait(t, entered)
				f.reopen(t)
				r := performRequest(t, f.server, http.MethodPost, path, body, headers)
				assertB1APIError(t, r.Code, r.Body.Bytes(), 409, "conflict")
				once.Do(func() { close(release) })
				select {
				case r = <-first:
				case <-time.After(5 * time.Second):
					t.Fatal("first copy did not finish")
				}
				if r.Code != 201 {
					t.Fatalf("first copy: %d %s", r.Code, r.Body.String())
				}
				r = performRequest(t, f.server, http.MethodPost, path, body, headers)
				status, data, responseHeaders = r.Code, r.Body.Bytes(), r.Header()
			default:
				r := performRequest(t, f.server, http.MethodPost, path, body, headers)
				if r.Code != 201 {
					t.Fatalf("copy: %d %s", r.Code, r.Body.String())
				}
				var file gen.FileEntry
				if err := json.Unmarshal(r.Body.Bytes(), &file); err != nil {
					t.Fatal(err)
				}
				if name == "current_result_access" {
					if _, err := f.catalog.MoveWithPolicy(context.Background(), 1001, uuid.UUID(file.ID), &f.private, nil, "fail"); err != nil {
						t.Fatal(err)
					}
					// Original source/destination are no longer needed for an authorized result replay.
					if _, err := f.catalog.Trash(context.Background(), 1001, source); err != nil {
						t.Fatal(err)
					}
				}
				before := f.snapshot(t)
				request := body
				if name == "fingerprint_mismatch" {
					request = bytes.Replace(body, []byte("copy.bin"), []byte("other.bin"), 1)
				}
				r = performRequest(t, f.server, http.MethodPost, path, request, headers)
				status, data, responseHeaders = r.Code, r.Body.Bytes(), r.Header()
				if f.snapshot(t) != before {
					t.Fatal("copy replay mutated state")
				}
			}
			if name == "terminal_reply_lost" {
				assertParentUnavailable(t, status, data)
			} else if name == "fingerprint_mismatch" {
				assertB1APIError(t, status, data, 409, "conflict")
			} else {
				if status != 201 || responseHeaders.Get("Etag") == "" || responseHeaders.Get("Location") == "" {
					t.Fatalf("copy replay: %d %s", status, data)
				}
				var file gen.FileEntry
				if err := json.Unmarshal(data, &file); err != nil {
					t.Fatal(err)
				}
				download := performRequest(t, f.server, http.MethodGet, "/v1/files/"+uuid.UUID(file.ID).String()+"/content", nil, headers)
				if download.Code != 200 || sha256.Sum256(download.Body.Bytes()) != sha256.Sum256([]byte("abc")) {
					t.Fatal("copied bytes differ")
				}
			}
			var operations int
			if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM remote_write_receipts WHERE row_kind='copy_operation'").Scan(&operations); err != nil {
				t.Fatal(err)
			}
			if operations != 1 {
				t.Fatalf("operations=%d", operations)
			}
			if name == "current_result_access" {
				for _, mode := range []string{"moved", "revoked", "inactive_ancestor"} {
					t.Run(mode, func(t *testing.T) { testLifecycleCopyResultDenied(t, mode) })
				}
			}
			f.storage.mu.Lock()
			sent := f.storage.nextID
			sourceBytes := append([]byte(nil), f.storage.messages[1]...)
			f.storage.mu.Unlock()
			if sent != 2 || sha256.Sum256(sourceBytes) != sha256.Sum256([]byte("abc")) {
				t.Fatalf("duplicate RPC/source loss: sent=%d", sent)
			}
		})
	}
}

func (f *lifecycleAPIFixture) runtime(t *testing.T, client *http.Client) (*jobs.Runtime, *lifecycle.Service, *secureblob.Cipher) {
	t.Helper()
	policy := lifecycle.NewService(f.pool, f.storage)
	f.catalog = catalog.NewService(f.pool, nil)
	f.uploads = uploads.NewService(f.pool)
	if err := f.catalog.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	if err := f.uploads.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	channelService := channels.NewService(f.pool, nil, channels.Config{})
	var err error
	f.fileops, err = fileops.NewService(f.pool, f.catalog, channelService, f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.fileops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	f.shares, err = shares.NewService(f.pool, f.catalog)
	if err != nil {
		t.Fatal(err)
	}
	pipeline := transfer.NewPipeline(f.uploads, channelService, f.storage, nil, transfer.Config{Receipts: policy})
	cipher, err := secureblob.NewWithKey(bytes.Repeat([]byte{7}, 32), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := jobs.NewRuntimeWithServices(f.pool, f.storage, "teldrive", nil, cipher, time.Hour, jobs.UploaderServices{Catalog: f.catalog, Uploads: f.uploads, Pipeline: pipeline, HTTPClient: client}, policy, f.fileops)
	if err != nil {
		t.Fatal(err)
	}
	if err = policy.BindReceiptEnqueuer(runtime); err != nil {
		t.Fatal(err)
	}
	handler := api.NewHandler(f.catalog, f.uploads, pipeline, transfer.NewDownloader(f.catalog, f.storage, nil), health.NewService("fixture", f.pool), 0, nil).ConfigureDomains(nil, nil, channelService, f.fileops, f.shares, nil).ConfigureJobs(runtime)
	f.server, err = api.NewServer(handler, api.NewSecurity(lifecycleAPIAuthenticator{}))
	if err != nil {
		t.Fatal(err)
	}
	return runtime, policy, cipher
}

type lifecycleAPIAuthenticator struct{}

func (lifecycleAPIAuthenticator) AuthenticateBearer(_ context.Context, token string) (api.Identity, error) {
	switch token {
	case "owner":
		return api.Identity{UserID: 1001}, nil
	case "editor":
		return api.Identity{UserID: 1002}, nil
	case "reader":
		return api.Identity{UserID: 1003}, nil
	case "foreign":
		return api.Identity{UserID: 1004}, nil
	case "admin":
		return api.Identity{UserID: 1001, Roles: []string{"admin"}}, nil
	}
	return api.Identity{}, api.ErrUnauthenticated
}
func (a lifecycleAPIAuthenticator) AuthenticateAPIKey(ctx context.Context, key string) (api.Identity, error) {
	return a.AuthenticateBearer(ctx, key)
}

func TestB2CleanupErrorVisibility(t *testing.T) {
	for _, secondary := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(secondary.Error(), func(t *testing.T) {
			f := newLifecycleAPI(t)
			runtime, policy, _ := f.runtime(t, nil)
			_, source, _, _ := f.publish(t, "owner")
			destination := f.newFolder(t, "cleanup-destination")
			f.storage.deleteErr = secondary
			f.storage.afterCopy = func() error {
				if _, err := f.catalog.Trash(context.Background(), 1001, destination); err != nil {
					return err
				}
				_, err := f.catalog.Restore(context.Background(), 1001, destination)
				return err
			}
			path := "/v1/files/" + source.String() + "/copy"
			headers := map[string]string{"Authorization": "Bearer owner", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
			r := performRequest(t, f.server, http.MethodPost, path, []byte(fmt.Sprintf(`{"parentId":%q,"name":"loser.bin"}`, destination)), headers)
			assertParentUnavailable(t, r.Code, r.Body.Bytes())
			var jobID int64
			if err := f.pool.QueryRow(context.Background(), "SELECT job_id FROM remote_write_receipts WHERE row_kind='message_attempt' AND origin_kind='copy'").Scan(&jobID); err != nil {
				t.Fatal(err)
			}
			if jobID <= 0 {
				t.Fatal("missing actual River report ID")
			}
			if _, err := runtime.GetForUser(context.Background(), jobID, 1001); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.GetForUser(context.Background(), jobID, 1004); err == nil {
				t.Fatal("foreign owner read cleanup report")
			}
			var receiptID uuid.UUID
			if err := f.pool.QueryRow(context.Background(), "SELECT id FROM remote_write_receipts WHERE row_kind='message_attempt' AND origin_kind='copy'").Scan(&receiptID); err != nil {
				t.Fatal(err)
			}
			for range 4 {
				var attempts int
				var state string
				if err := f.pool.QueryRow(context.Background(), "SELECT attempts,state FROM remote_write_receipts WHERE id=$1", receiptID).Scan(&attempts, &state); err != nil {
					t.Fatal(err)
				}
				if state == "exhausted" {
					break
				}
				if _, err := f.pool.Exec(context.Background(), "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", receiptID); err != nil {
					t.Fatal(err)
				}
				if err := policy.Recover(context.Background(), 1001, []uuid.UUID{receiptID}); err == nil {
					t.Fatal("failed cleanup reported success")
				}
			}
			var exhausted bool
			if err := f.pool.QueryRow(context.Background(), "SELECT state='exhausted' AND attempts=4 FROM remote_write_receipts WHERE id=$1", receiptID).Scan(&exhausted); err != nil || !exhausted {
				t.Fatalf("actual retry exhaustion=%v error=%v", exhausted, err)
			}
			for _, retained := range []bool{true, false} {
				if !retained {
					if _, err := f.pool.Exec(context.Background(), "DELETE FROM river_job WHERE id=$1", jobID); err != nil {
						t.Fatal(err)
					}
				}
				for _, actor := range []string{"owner", "foreign"} {
					before := f.snapshot(t)
					r = performRequest(t, f.server, http.MethodPost, fmt.Sprintf("/v1/jobs/%d/retry", jobID), nil, map[string]string{"Authorization": "Bearer " + actor})
					if actor == "owner" {
						assertB1APIError(t, r.Code, r.Body.Bytes(), 409, "conflict")
					} else if r.Code != 404 {
						t.Fatalf("foreign retry: %d %s", r.Code, r.Body.String())
					}
					if f.snapshot(t) != before {
						t.Fatal("retry reopened receipt work")
					}
				}
				visible := performRequest(t, f.server, http.MethodGet, fmt.Sprintf("/v1/jobs/%d", jobID), nil, map[string]string{"Authorization": "Bearer owner"})
				if visible.Code != 200 {
					t.Fatalf("retained report: %d %s", visible.Code, visible.Body.String())
				}
			}
		})
	}
	t.Run("disconnected_no_response", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := httptest.NewRecorder()
		api.ErrorHandler(ctx, out, httptest.NewRequest(http.MethodPost, "/v1/uploads", nil), &api.Problem{Status: 409, Code: "upload_parent_unavailable", Message: "upload destination folder is unavailable", Cause: errors.Join(uploads.ErrParentUnavailable, context.DeadlineExceeded)})
		if out.Body.Len() != 0 || len(out.Header()) != 0 {
			t.Fatal("disconnected request received second response")
		}
		f := newLifecycleAPI(t)
		_, _, _ = f.runtime(t, nil)
		_, source, _, _ := f.publish(t, "owner")
		destination := f.newFolder(t, "disconnected-destination")
		live, cancelRequest := context.WithCancel(context.Background())
		f.storage.deleteErr = context.DeadlineExceeded
		f.storage.afterCopy = func() error {
			if _, err := f.catalog.Trash(context.Background(), 1001, destination); err != nil {
				return err
			}
			cancelRequest()
			return context.Canceled
		}
		request := httptest.NewRequest(http.MethodPost, "/v1/files/"+source.String()+"/copy", strings.NewReader(fmt.Sprintf(`{"parentId":%q,"name":"cancelled.bin"}`, destination))).WithContext(live)
		request.Header.Set("Authorization", "Bearer owner")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", uuid.NewString())
		recorded := httptest.NewRecorder()
		f.server.ServeHTTP(recorded, request)
		if recorded.Body.Len() != 0 || len(recorded.Header()) != 0 {
			t.Fatal("generated disconnected request wrote a response")
		}
		var retained int
		if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM remote_write_receipts WHERE origin_kind='copy' AND job_id>0").Scan(&retained); err != nil {
			t.Fatal(err)
		}
		if retained == 0 {
			t.Fatal("disconnect lost durable owner report")
		}
	})
}

func TestB2LifecycleAdapters(t *testing.T) {
	for _, adapter := range []string{"single", "bulk", "public", "purge", "clean_trash"} {
		t.Run(adapter, func(t *testing.T) {
			f := newLifecycleAPI(t)
			id, _, _ := f.create(t, "owner", 0)
			headers := map[string]string{"Authorization": "Bearer owner", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
			var out *httptest.ResponseRecorder
			switch adapter {
			case "single":
				out = performRequest(t, f.server, http.MethodDelete, "/v1/files/"+f.folder.String(), nil, headers)
			case "bulk":
				out = performRequest(t, f.server, http.MethodPost, "/v1/files/bulk/trash", []byte(fmt.Sprintf(`{"fileIds":[%q]}`, f.folder)), headers)
			case "public":
				out = performRequest(t, f.server, http.MethodDelete, "/v1/public/shares/"+f.public+"/files/"+f.folder.String(), nil, map[string]string{"X-Share-Password": "fixture-password"})
			case "purge", "clean_trash":
				if _, err := f.catalog.Trash(context.Background(), 1001, f.folder); err != nil {
					t.Fatal(err)
				}
				path := "/v1/files/" + f.folder.String() + "/purge"
				if adapter == "clean_trash" {
					path = "/v1/files/trash"
				}
				out = performRequest(t, f.server, http.MethodDelete, path, nil, headers)
			}
			want := 204
			if adapter == "bulk" {
				want = 200
			}
			if out.Code != want {
				t.Fatalf("%s adapter: %d %s", adapter, out.Code, out.Body.String())
			}
			get := performRequest(t, f.server, http.MethodGet, "/v1/uploads/"+id.String(), nil, headers)
			assertParentUnavailable(t, get.Code, get.Body.Bytes())
			var reason string
			if err := f.pool.QueryRow(context.Background(), "SELECT terminal_failure_code FROM upload_sessions WHERE id=$1", id).Scan(&reason); err != nil {
				t.Fatal(err)
			}
			if reason != "upload_parent_unavailable" {
				t.Fatal("adapter bypassed terminal fence")
			}
		})
	}
	t.Run("enqueue_failure_rolls_back", func(t *testing.T) {
		f := newLifecycleAPI(t)
		_, path, headers := f.create(t, "owner", 3)
		headers["Content-Type"] = "application/octet-stream"
		put := performRequest(t, f.server, http.MethodPut, path+"/parts/1", []byte("abc"), headers)
		if put.Code != 201 {
			t.Fatalf("put: %d %s", put.Code, put.Body.String())
		}
		before := f.snapshot(t)
		if _, err := f.pool.Exec(context.Background(), "ALTER TABLE river_job ADD CONSTRAINT api_refuse_cleanup CHECK(kind <> 'api_receipt_fixture')"); err != nil {
			t.Fatal(err)
		}
		out := performRequest(t, f.server, http.MethodDelete, "/v1/files/"+f.folder.String(), nil, map[string]string{"Authorization": "Bearer owner"})
		if out.Code < 400 {
			t.Fatalf("false durable scheduling: %d %s", out.Code, out.Body.String())
		}
		if f.snapshot(t) != before {
			t.Fatal("failed enqueue committed hierarchy/terminal/event")
		}
	})
}

type lifecycleSourceTransport struct {
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	requests atomic.Int32
	fail     bool
}

func (s *lifecycleSourceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.requests.Add(1)
	if req.URL.Host != "source.invalid" {
		return nil, errors.New("unexpected source host")
	}
	if req.Method == http.MethodHead {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		if s.fail {
			return nil, errors.New("synthetic source preflight failure")
		}
	}
	body := ""
	if req.Method == http.MethodGet {
		body = "abc"
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Length": {"3"}, "Content-Type": {"application/octet-stream"}, "Etag": {`"fixture-v1"`}, "Last-Modified": {"Wed, 01 Jul 2026 00:00:00 GMT"}}, Body: io.NopCloser(strings.NewReader(body)), ContentLength: 3, Request: req}, nil
}

func lifecycleWaitJob(t *testing.T, pool *pgxpool.Pool, id int64, states ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var state string
		if err := pool.QueryRow(ctx, "SELECT state FROM river_job WHERE id=$1", id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		for _, wanted := range states {
			if state == wanted {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("job %d state=%s want=%v", id, state, states)
		case <-ticker.C:
		}
	}
}

func TestB2ImportAdmissionHTTP(t *testing.T) {
	for _, name := range []string{"uuid_trash_restore_before_a", "purge_before_a", "absolute_nearest_anchor", "relative_source_anchor", "preflight_failure_restart", "blocked_preflight_resume", "root_and_rollback", "deletion_rollback", "denied_and_generic_bypass"} {
		t.Run(name, func(t *testing.T) {
			f := newLifecycleAPI(t)
			transport := &lifecycleSourceTransport{entered: make(chan struct{}), release: make(chan struct{}), fail: name == "preflight_failure_restart"}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(transport.release) }) })
			runtime, _, cipher := f.runtime(t, &http.Client{Transport: transport, Timeout: 5 * time.Second})
			destination := f.folder.String()
			sourcePath := "source.bin"
			anchor := f.folder
			switch name {
			case "absolute_nearest_anchor":
				destination = "/shared/destination/missing/nested"
			case "relative_source_anchor":
				prefix, err := f.catalog.CreateFolder(context.Background(), catalog.CreateFolderInput{UserID: 1001, ParentID: &f.folder, Name: "prefix"})
				if err != nil {
					t.Fatal(err)
				}
				anchor = uuid.UUID(prefix.ID.Bytes)
				sourcePath = "prefix/missing/source.bin"
			case "root_and_rollback":
				destination = "/"
			}
			headers := map[string]string{"Authorization": "Bearer owner", "Content-Type": "application/json"}
			body := []byte(fmt.Sprintf(`{"destination":%q,"sources":[{"type":"http","url":"https://source.invalid/source.bin","destinationPath":%q}]}`, destination, sourcePath))
			if name == "denied_and_generic_bypass" {
				before := f.snapshot(t)
				for _, actor := range []string{"foreign", ""} {
					headers["Authorization"] = "Bearer " + actor
					r := performRequest(t, f.server, http.MethodPost, "/v1/uploads/imports", body, headers)
					if actor == "" {
						if r.Code != 401 {
							t.Fatalf("unauthenticated import: %d %s", r.Code, r.Body.String())
						}
					} else if r.Code < 400 {
						t.Fatal("wrong owner admitted")
					}
				}
				headers["Authorization"] = "Bearer owner"
				r := performRequest(t, f.server, http.MethodPost, "/v1/uploads/imports", []byte(`{"destination":"invalid-uuid","sources":[{"type":"http","url":"https://source.invalid/source.bin"}]}`), headers)
				if r.Code != 422 {
					t.Fatalf("invalid UUID: %d %s", r.Code, r.Body.String())
				}
				headers["Authorization"] = "Bearer admin"
				for _, kind := range []string{jobs.UploadBatchKind, jobs.UploadSourceKind} {
					r = performRequest(t, f.server, http.MethodPost, "/v1/jobs", []byte(fmt.Sprintf(`{"type":%q,"args":{}}`, kind)), headers)
					if r.Code != 400 {
						t.Fatalf("generic bypass %s: %d %s", kind, r.Code, r.Body.String())
					}
					encoded, err := json.Marshal(periodicHTTPBody("raw-"+kind, kind))
					if err != nil {
						t.Fatal(err)
					}
					r = performRequest(t, f.server, http.MethodPost, "/v1/periodic-jobs", encoded, headers)
					if r.Code < 400 {
						t.Fatal("periodic raw import bypass")
					}
				}
				if f.snapshot(t) != before || transport.requests.Load() != 0 {
					t.Fatal("denied admission mutated or accessed source")
				}
				return
			}
			if name == "root_and_rollback" {
				if _, err := f.pool.Exec(context.Background(), "ALTER TABLE import_batches ADD CONSTRAINT api_q_rollback CHECK(false) NOT VALID"); err != nil {
					t.Fatal(err)
				}
				r := performRequest(t, f.server, http.MethodPost, "/v1/uploads/imports", body, headers)
				if r.Code < 400 {
					t.Fatal("failed Q claimed admission")
				}
				var count int
				if err := f.pool.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM import_batches)+(SELECT count(*) FROM import_destination_pins)+(SELECT count(*) FROM river_job WHERE kind=$1)", jobs.UploadBatchKind).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 || transport.requests.Load() != 0 {
					t.Fatal("Q rollback leaked job/pin/source access")
				}
				if _, err := f.pool.Exec(context.Background(), "ALTER TABLE import_batches DROP CONSTRAINT api_q_rollback"); err != nil {
					t.Fatal(err)
				}
			}
			out := performRequest(t, f.server, http.MethodPost, "/v1/uploads/imports", body, headers)
			if out.Code != 202 {
				t.Fatalf("Q route: %d %s", out.Code, out.Body.String())
			}
			var response gen.Job
			if err := json.Unmarshal(out.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			jobID, err := strconv.ParseInt(response.ID, 10, 64)
			if err != nil || jobID <= 0 {
				t.Fatalf("actual job ID: %q %v", response.ID, err)
			}
			var encrypted []byte
			if err := f.pool.QueryRow(context.Background(), "SELECT args FROM river_job WHERE id=$1", jobID).Scan(&encrypted); err != nil {
				t.Fatal(err)
			}
			plain, err := riverencrypt.DecryptArgs(cipher, encrypted)
			if err != nil {
				t.Fatal(err)
			}
			var args jobs.UploadBatchArgs
			if err = json.Unmarshal(plain, &args); err != nil {
				t.Fatal(err)
			}
			if args.BatchID == "" || args.UserID != 1001 {
				t.Fatal("missing persisted admitted identity")
			}
			var pins int
			if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM import_destination_pins WHERE user_id=1001 AND batch_id=$1", args.BatchID).Scan(&pins); err != nil {
				t.Fatal(err)
			}
			if pins != 2 || transport.requests.Load() != 0 {
				t.Fatal("source access preceded concrete Q pins")
			}
			var initialFolders int
			if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM files WHERE kind='folder'").Scan(&initialFolders); err != nil {
				t.Fatal(err)
			}
			if name != "root_and_rollback" {
				ordinal := int32(-1)
				if name == "relative_source_anchor" {
					ordinal = 0
				}
				var pinned uuid.UUID
				if err := f.pool.QueryRow(context.Background(), "SELECT anchor_id FROM import_destination_pins WHERE batch_id=$1 AND destination_ordinal=$2", args.BatchID, ordinal).Scan(&pinned); err != nil {
					t.Fatal(err)
				}
				if pinned != anchor {
					t.Fatalf("Q anchor=%s want %s", pinned, anchor)
				}
			}
			if err := runtime.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			var stop sync.Once
			stopRuntime := func() {
				stop.Do(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := runtime.Stop(ctx); err != nil {
						t.Error(err)
					}
				})
			}
			t.Cleanup(stopRuntime)
			lifecycleAwait(t, transport.entered)
			if name == "preflight_failure_restart" {
				release.Do(func() { close(transport.release) })
				lifecycleWaitJob(t, f.pool, jobID, "retryable", "scheduled")
				stopRuntime()
			}
			if name == "deletion_rollback" {
				before := f.snapshot(t)
				if _, err := f.pool.Exec(context.Background(), "ALTER TABLE files ADD CONSTRAINT api_trash_rollback CHECK(status <> 'trashed') NOT VALID"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Trash(context.Background(), 1001, anchor); err == nil {
					t.Fatal("trash rollback was not exercised")
				}
				if f.snapshot(t) != before {
					t.Fatal("deletion rollback changed Q admission")
				}
				if _, err := f.pool.Exec(context.Background(), "ALTER TABLE files DROP CONSTRAINT api_trash_rollback"); err != nil {
					t.Fatal(err)
				}
			} else if name != "root_and_rollback" {
				if _, err := f.catalog.Trash(context.Background(), 1001, anchor); err != nil {
					t.Fatal(err)
				}
				if name == "purge_before_a" {
					if err := f.fileops.QueuePurge(context.Background(), 1001, anchor); err != nil {
						t.Fatal(err)
					}
					if err := f.fileops.PurgeMany(context.Background(), 1001, []uuid.UUID{anchor}); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := f.catalog.Restore(context.Background(), 1001, anchor); err != nil {
						t.Fatal(err)
					}
				}
			}
			release.Do(func() { close(transport.release) })
			control := name == "root_and_rollback" || name == "deletion_rollback"
			if name != "preflight_failure_restart" {
				if control {
					lifecycleWaitJob(t, f.pool, jobID, "completed")
				} else {
					lifecycleWaitJob(t, f.pool, jobID, "cancelled")
				}
			}
			if control {
				var dispatched bool
				if err := f.pool.QueryRow(context.Background(), "SELECT dispatched FROM import_batches WHERE batch_id=$1", args.BatchID).Scan(&dispatched); err != nil {
					t.Fatal(err)
				}
				if !dispatched {
					t.Fatal("root sentinel failed dispatch")
				}
				var childID int64
				if err := f.pool.QueryRow(context.Background(), "SELECT job_id FROM import_job_attempts WHERE batch_id=$1 AND source_index=0", args.BatchID).Scan(&childID); err != nil {
					t.Fatal(err)
				}
				lifecycleWaitJob(t, f.pool, childID, "completed")
				stopRuntime()
				var fileID uuid.UUID
				if err := f.pool.QueryRow(context.Background(), "SELECT id FROM files WHERE kind='file'").Scan(&fileID); err != nil {
					t.Fatal(err)
				}
				download := performRequest(t, f.server, http.MethodGet, "/v1/files/"+fileID.String()+"/content", nil, headers)
				if download.Code != 200 || sha256.Sum256(download.Body.Bytes()) != sha256.Sum256([]byte("abc")) {
					t.Fatal("root/rollback import bytes differ")
				}
				return
			}
			stopRuntime()
			requests := transport.requests.Load()
			before := f.snapshot(t)
			fresh, _, _ := f.runtime(t, &http.Client{Transport: transport, Timeout: 5 * time.Second})
			if _, err := fresh.InsertUploadBatch(context.Background(), args); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("fresh Q replay: %v", err)
			}
			job := &river.Job[jobs.UploadBatchArgs]{JobRow: &rivertype.JobRow{ID: jobID}, Args: args}
			worker := jobs.NewUploadBatchWorker(f.pool, &http.Client{Transport: transport, Timeout: 5 * time.Second}, f.catalog, f.uploads)
			if err := worker.Work(context.Background(), job); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("fresh actual Work: %v", err)
			}
			if transport.requests.Load() != requests || f.snapshot(t) != before {
				t.Fatal("terminal replay accessed source or changed rows")
			}
			var count int
			if err := f.pool.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM import_job_attempts WHERE batch_id=$1)+(SELECT count(*) FROM upload_sessions)+(SELECT count(*) FROM files WHERE kind='file')", args.BatchID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("pre-A loss created children/upload/result")
			}
			var folders int
			if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM files WHERE kind='folder'").Scan(&folders); err != nil {
				t.Fatal(err)
			}
			wantFolders := initialFolders
			if name == "purge_before_a" {
				wantFolders--
			}
			if folders != wantFolders {
				t.Fatalf("pre-A loss recreated directories: %d want %d", folders, wantFolders)
			}
			f.storage.mu.Lock()
			sends := f.storage.nextID
			f.storage.mu.Unlock()
			if sends != 0 {
				t.Fatal("pre-A loss published remote bytes")
			}
		})
	}
}

type lifecycleBusyCatalog struct {
	*uploads.Service
	busy chan struct{}
	once sync.Once
}

func (c *lifecycleBusyCatalog) ClaimPart(ctx context.Context, in uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	result, err := c.Service.ClaimPart(ctx, in)
	if errors.Is(err, uploads.ErrPartBusy) {
		c.once.Do(func() { close(c.busy) })
	}
	return result, err
}

func testLifecycleWaiter(t *testing.T, body string) {
	f := newLifecycleAPI(t)
	_, policy, _ := f.runtime(t, nil)
	observer := &lifecycleBusyCatalog{Service: f.uploads, busy: make(chan struct{})}
	channelService := channels.NewService(f.pool, nil, channels.Config{})
	pipeline := transfer.NewPipeline(observer, channelService, f.storage, nil, transfer.Config{Receipts: policy})
	handler := api.NewHandler(f.catalog, f.uploads, pipeline, nil, health.NewService("fixture", f.pool), 0, nil).ConfigureDomains(nil, nil, channelService, f.fileops, f.shares, nil)
	server, err := api.NewServer(handler, api.NewSecurity(lifecycleAPIAuthenticator{}))
	if err != nil {
		t.Fatal(err)
	}
	f.server = server
	id, path, headers := f.create(t, "owner", 3)
	headers["Content-Type"] = "application/octet-stream"
	remote, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(released) }) })
	f.storage.afterUpload = func() error {
		close(remote)
		select {
		case <-released:
			return nil
		case <-time.After(5 * time.Second):
			return context.DeadlineExceeded
		}
	}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- performRequest(t, server, http.MethodPut, path+"/parts/1", []byte("abc"), headers) }()
	lifecycleAwait(t, remote)
	secondBase, secondHeaders := f.route("public")
	secondHeaders["Content-Type"] = "application/octet-stream"
	bodyReached, bodyRelease := make(chan struct{}), make(chan struct{})
	var bodyOnce sync.Once
	t.Cleanup(func() { bodyOnce.Do(func() { close(bodyRelease) }) })
	reader := &lifecycleEOFReader{Reader: strings.NewReader(body), atEOF: func() {
		close(bodyReached)
		select {
		case <-bodyRelease:
		case <-time.After(5 * time.Second):
		}
	}}
	request := httptest.NewRequest(http.MethodPut, secondBase+"/"+id.String()+"/parts/1", nil)
	request.Body = io.NopCloser(reader)
	request.ContentLength = 3
	for key, value := range secondHeaders {
		request.Header.Set(key, value)
	}
	request.Header.Set("Content-Length", "3")
	second := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); server.ServeHTTP(second, request) }()
	lifecycleAwait(t, observer.busy)
	once.Do(func() { close(released) })
	select {
	case r := <-first:
		if r.Code != 201 {
			t.Fatalf("first upload: %d %s", r.Code, r.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first upload stuck")
	}
	headers["Content-Type"] = "application/json"
	completed := performRequest(t, server, http.MethodPost, path+"/complete", []byte(`{"size":3}`), headers)
	if completed.Code != 201 {
		t.Fatalf("completion: %d %s", completed.Code, completed.Body.String())
	}
	var file gen.FileEntry
	if err := json.Unmarshal(completed.Body.Bytes(), &file); err != nil {
		t.Fatal(err)
	}
	lifecycleAwait(t, bodyReached)
	fileID := uuid.UUID(file.ID)
	if _, err := f.catalog.MoveWithPolicy(context.Background(), 1001, fileID, &f.private, nil, "fail"); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot(t)
	bodyOnce.Do(func() { close(bodyRelease) })
	lifecycleAwait(t, done)
	if second.Code != 404 {
		t.Fatalf("waiter disclosed moved result/proof: %d %s", second.Code, second.Body.String())
	}
	if f.snapshot(t) != before {
		t.Fatal("completed waiter changed rows/events")
	}
	f.storage.mu.Lock()
	sent := f.storage.nextID
	payload := append([]byte(nil), f.storage.messages[1]...)
	f.storage.mu.Unlock()
	if sent != 1 || sha256.Sum256(payload) != sha256.Sum256([]byte("abc")) {
		t.Fatal("waiter sent again or changed winner bytes")
	}
	parts := performRequest(t, server, http.MethodGet, path+"/parts?limit=1", nil, headers)
	if parts.Code != 200 {
		t.Fatalf("completed list projection: %d %s", parts.Code, parts.Body.String())
	}
	var page gen.ListUploadPartsOK
	if err := json.Unmarshal(parts.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].PartNo != 1 {
		t.Fatal("retired alias missing from paged projection")
	}
}

func testLifecycleCopyResultDenied(t *testing.T, mode string) {
	f := newLifecycleAPI(t)
	_, source, _, _ := f.publish(t, "owner")
	actor := "editor"
	want := 403
	if mode == "inactive_ancestor" {
		actor = "owner"
		want = 404
	}
	headers := map[string]string{"Authorization": "Bearer " + actor, "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
	path := "/v1/files/" + source.String() + "/copy"
	body := []byte(fmt.Sprintf(`{"parentId":%q,"name":"current.bin"}`, f.folder))
	out := performRequest(t, f.server, http.MethodPost, path, body, headers)
	if out.Code != 201 {
		t.Fatalf("copy setup: %d %s", out.Code, out.Body.String())
	}
	var file gen.FileEntry
	if err := json.Unmarshal(out.Body.Bytes(), &file); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "moved":
		if _, err := f.catalog.MoveWithPolicy(context.Background(), 1001, uuid.UUID(file.ID), &f.private, nil, "fail"); err != nil {
			t.Fatal(err)
		}
	case "revoked":
		if err := f.shares.RevokeGrant(context.Background(), 1001, f.grant); err != nil {
			t.Fatal(err)
		}
	case "inactive_ancestor":
		// Deliberately inconsistent historical ancestry: target remains active.
		if _, err := f.pool.Exec(context.Background(), "UPDATE files SET status='trashed',deleted_at=clock_timestamp() WHERE id=$1", f.folder); err != nil {
			t.Fatal(err)
		}
		var active bool
		if err := f.pool.QueryRow(context.Background(), "SELECT status='active' FROM files WHERE id=$1", uuid.UUID(file.ID)).Scan(&active); err != nil || !active {
			t.Fatalf("active target prerequisite: %v %v", active, err)
		}
	}
	before := f.snapshot(t)
	f.reopen(t)
	for _, request := range [][]byte{body, bytes.Replace(body, []byte("current.bin"), []byte("changed.bin"), 1)} {
		out = performRequest(t, f.server, http.MethodPost, path, request, headers)
		if out.Code != want || bytes.Contains(out.Body.Bytes(), []byte("upload_parent_unavailable")) {
			t.Fatalf("current result before mismatch: %d %s", out.Code, out.Body.String())
		}
	}
	if f.snapshot(t) != before {
		t.Fatal("denied copy mutated retained identity")
	}
	f.storage.mu.Lock()
	defer f.storage.mu.Unlock()
	if f.storage.nextID != 2 {
		t.Fatal("denied copy sent again")
	}
	for _, id := range []int64{1, 2} {
		if sha256.Sum256(f.storage.messages[id]) != sha256.Sum256([]byte("abc")) {
			t.Fatal("denied copy lost source/result bytes")
		}
	}
}
