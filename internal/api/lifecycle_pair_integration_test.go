//go:build integration

package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/db/migrations"
	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/authn"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/database"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/shares"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

var pairModTime = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

// The trace observes real requests and outcomes; it never supplies a session,
// admission, result, copy operation or successful generated response.
type pairRequest struct {
	Method, Path, Query, Key string
	Body                     json.RawMessage
	Status                   int
	Lost                     bool
}

type pairStorage struct {
	*fixture.Storage
	mu              sync.Mutex
	uploads, copies int
}

func (s *pairStorage) Upload(ctx context.Context, in telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	s.uploads++
	s.mu.Unlock()
	return s.Storage.Upload(ctx, in)
}
func (s *pairStorage) CopyPart(ctx context.Context, user, channel, message, destination int64) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	s.copies++
	s.mu.Unlock()
	return s.Storage.CopyPart(ctx, user, channel, message, destination)
}
func (s *pairStorage) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploads, s.copies
}

type pairFixture struct {
	t                                    *testing.T
	ctx                                  context.Context
	pool                                 *pgxpool.Pool
	catalog                              *catalog.Service
	ops                                  *fileops.Service
	shares                               *shares.Service
	storage                              *pairStorage
	generated                            http.Handler
	server                               *httptest.Server
	binary, directory, config, namespace string
	owner                                int64
	parent                               uuid.UUID
	mu                                   sync.Mutex
	trace                                []pairRequest
	gate                                 *fixture.Barrier
	gateMode                             string
	gateUsed                             bool
	truncate                             bool
	loseCreate                           int
	loseComplete                         int
	loseCopySuccess                      int
	transient                            int
	listingTransient                     int
	discoveryTransient                   int
	mixedCreate                          chan struct{}
	transientName                        string
	terminalAt                           int
	cancelCommand                        context.CancelFunc
}

func pairPrivatePath(t *testing.T, path string, directory bool) {
	t.Helper()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("explicit canonical private path required")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			t.Fatal("private path missing or symlinked")
		}
		if p == path || !directory && p == filepath.Dir(path) {
			mode := os.FileMode(0600)
			if directory || p != path {
				mode = 0700
			}
			v := reflect.ValueOf(info.Sys())
			if v.Kind() != reflect.Pointer || v.IsNil() {
				t.Fatal("private path ownership unavailable")
			}
			uid := v.Elem().FieldByName("Uid")
			if !uid.IsValid() || !uid.CanUint() || uid.Uint() != uint64(os.Getuid()) || info.Mode().Perm() != mode {
				t.Fatal("private path ownership or permissions rejected")
			}
		}
		if p == filepath.Dir(p) {
			break
		}
	}
}

func pairCandidate(t *testing.T) string {
	if os.Getenv("B2_ISOLATION_APPROVAL") == "" || os.Getenv("B2_PROOF_DIR") == "" || os.Getenv("B2_PAIRED_RESTORE_INPUT") == "" {
		t.Skip("private B2 paired authority not supplied; paired proof NOT-RUN")
	}
	t.Helper()
	binary, expected := os.Getenv("B2_RCLONE_BINARY"), os.Getenv("B2_RCLONE_SHA256")
	if !filepath.IsAbs(binary) || filepath.Clean(binary) != binary || len(expected) != 64 {
		t.Fatal("fresh B2_RCLONE_BINARY/B2_RCLONE_SHA256 required; paired proof NOT-RUN")
	}
	info, err := os.Lstat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatal("owned candidate executable required")
	}
	resolved, err := filepath.EvalSymlinks(binary)
	if err != nil || resolved != binary {
		t.Fatal("candidate executable path is not canonical")
	}
	owner := reflect.ValueOf(info.Sys())
	if owner.Kind() != reflect.Pointer || owner.IsNil() {
		t.Fatal("candidate ownership unavailable")
	}
	uid := owner.Elem().FieldByName("Uid")
	if !uid.IsValid() || !uid.CanUint() || uid.Uint() != uint64(os.Getuid()) || info.Mode().Perm()&0022 != 0 {
		t.Fatal("candidate executable is not exclusively owner-writable")
	}
	f, err := os.Open(binary)
	if err != nil {
		t.Fatal("candidate executable unreadable")
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal("candidate executable hash failed")
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		t.Fatal("candidate executable checksum mismatch")
	}
	if os.Getenv("B2_ISOLATION_APPROVAL") == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Fatal("fresh private isolation prerequisites required; paired proof NOT-RUN")
	}
	pairPrivatePath(t, os.Getenv("B2_PROOF_DIR"), true)
	return binary
}

func newPair(t *testing.T, binary string) *pairFixture {
	if os.Getenv("B2_ISOLATION_APPROVAL") == "" || os.Getenv("B2_PROOF_DIR") == "" {
		t.Skip("private B2 paired authority not supplied; paired proof NOT-RUN")
	}
	t.Helper()
	db := testpostgres.New(t)
	// Leave cleanup headroom within the root's 90-second per-leaf bound.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	t.Cleanup(cancel)
	if _, err := db.Pool.Exec(ctx, `INSERT INTO teldrive.users(user_id) VALUES(1001),(1002); INSERT INTO teldrive.channels(channel_id,user_id,name,selected) VALUES(9001,1001,'synthetic',true)`); err != nil {
		t.Fatal("paired fixture seed failed")
	}
	f := composePair(t, ctx, binary, db.Pool, 1001, nil)
	f.parent = f.folder(f.namespace, nil)
	return f
}

func composePair(t *testing.T, ctx context.Context, binary string, pool *pgxpool.Pool, owner int64, remote *pairStorage) *pairFixture {
	t.Helper()
	directory, err := os.MkdirTemp(os.Getenv("B2_PROOF_DIR"), "__teldrive_audit_")
	if err != nil {
		t.Fatal("owned private source directory creation failed")
	}
	f := &pairFixture{t: t, ctx: ctx, pool: pool, binary: binary, directory: directory, namespace: "__teldrive_audit_" + strings.ReplaceAll(uuid.NewString(), "-", ""), owner: owner}
	if remote == nil {
		remote = &pairStorage{Storage: fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{owner, 1002}}, {ChannelID: 9002, Actors: []int64{owner, 1002}}})}
		if err := remote.Seed(fixture.Message{UserID: 1002, ChannelID: 9002, MessageID: 700, Bytes: []byte("foreign retained payload"), Preserve: true}); err != nil {
			t.Fatal(err)
		}
	}
	f.storage = remote
	f.catalog = catalog.NewService(pool, nil)
	// Reuse the existing real InsertTx producer, including actual positive Job.ID.
	up := uploads.NewService(pool)
	policy := bindAPIReceiptPolicy(t, pool, remote, f.catalog, up)
	ch := channels.NewService(pool, nil, channels.Config{PartLimit: 10000})
	f.ops, err = fileops.NewService(pool, f.catalog, ch, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	f.shares, err = shares.NewService(pool, f.catalog)
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := secureblob.New("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := authn.NewService(pool, cipher, unusedTelegramLogin{}, authn.Config{SigningKey: "0123456789abcdef0123456789abcdef", Issuer: "test", AccessTokenTTL: time.Hour, RefreshTokenTTL: time.Hour, LoginFlowTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	pipe := transfer.NewPipeline(up, ch, remote, nil, transfer.Config{Receipts: policy})
	handler := api.NewHandler(f.catalog, up, pipe, transfer.NewDownloader(f.catalog, remote, nil), health.NewService("fixture", pool), 0, nil).ConfigureDomains(auth, nil, ch, f.ops, f.shares, nil)
	f.generated, err = api.NewServer(handler, api.NewSecurity(pairAuthenticator{owner: owner}))
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() { f.server.CloseClientConnections(); f.server.Close() })
	f.config = filepath.Join(directory, "rclone.conf")
	config := fmt.Sprintf("[fixture]\ntype = teldrive\napi_host = %s\napi_key = test-key\nchunk_size = 64Mi\nupload_concurrency = 1\n", f.server.URL)
	if err = os.WriteFile(f.config, []byte(config), 0600); err != nil {
		t.Fatal("owned candidate config write failed")
	}
	t.Cleanup(func() {
		// Keep full private source, request and row evidence, including failures.
		f.mu.Lock()
		trace := append([]pairRequest(nil), f.trace...)
		f.mu.Unlock()
		b, err := json.Marshal(trace)
		if err == nil {
			err = os.WriteFile(filepath.Join(directory, "requests.json"), b, 0600)
		}
		if err != nil {
			t.Error("private request evidence write failed")
		}
	})
	return f
}

type pairAuthenticator struct{ owner int64 }

func (a pairAuthenticator) AuthenticateBearer(_ context.Context, token string) (api.Identity, error) {
	if token == "foreign-token" {
		owner := int64(1002)
		if a.owner == 1 {
			owner = 2
		}
		return api.Identity{UserID: owner, Roles: []string{"user"}}, nil
	}
	if token != "test-token" {
		return api.Identity{}, errors.New("invalid synthetic token")
	}
	return api.Identity{UserID: a.owner, Roles: []string{"user"}}, nil
}
func (a pairAuthenticator) AuthenticateAPIKey(_ context.Context, key string) (api.Identity, error) {
	if key != "test-key" {
		return api.Identity{}, errors.New("invalid synthetic key")
	}
	return api.Identity{UserID: a.owner, Roles: []string{"user"}}, nil
}

func (f *pairFixture) folder(name string, parent *uuid.UUID) uuid.UUID {
	f.t.Helper()
	row, err := f.catalog.CreateFolder(f.ctx, catalog.CreateFolderInput{UserID: f.owner, ParentID: parent, Name: name, ModTime: pairModTime})
	if err != nil {
		f.t.Fatal("paired folder creation failed")
	}
	return uuid.UUID(row.ID.Bytes)
}

func (f *pairFixture) download(id uuid.UUID, want []byte) {
	f.t.Helper()
	r := performRequest(f.t, f.generated, http.MethodGet, "/v1/files/"+id.String()+"/content", nil, map[string]string{"Authorization": "Bearer test-token"})
	if r.Code != 200 || len(r.Body.Bytes()) != len(want) || sha256.Sum256(r.Body.Bytes()) != sha256.Sum256(want) {
		f.t.Fatal("independent complete download size/SHA-256 differs")
	}
}

func (f *pairFixture) publish(parent *uuid.UUID, name string, data []byte) uuid.UUID {
	f.t.Helper()
	body, _ := json.Marshal(map[string]any{"parentId": parent, "name": name, "size": len(data), "modTime": pairModTime, "preferredPartSize": 16 << 20})
	headers := map[string]string{"Authorization": "Bearer test-token", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
	r := performRequest(f.t, f.generated, "POST", "/v1/uploads", body, headers)
	if r.Code != 201 {
		f.t.Fatalf("fixture real create: %d", r.Code)
	}
	var session struct{ ID uuid.UUID }
	if json.Unmarshal(r.Body.Bytes(), &session) != nil || session.ID == uuid.Nil {
		f.t.Fatal("fixture session missing")
	}
	for offset, part := 0, 1; offset < len(data); offset, part = offset+(16<<20), part+1 {
		headers["Content-Type"] = "application/octet-stream"
		r = performRequest(f.t, f.generated, "PUT", fmt.Sprintf("/v1/uploads/%s/parts/%d", session.ID, part), data[offset:min(offset+(16<<20), len(data))], headers)
		if r.Code != 201 {
			f.t.Fatalf("fixture real part: %d", r.Code)
		}
	}
	headers["Content-Type"] = "application/json"
	body, _ = json.Marshal(map[string]any{"size": len(data)})
	r = performRequest(f.t, f.generated, "POST", "/v1/uploads/"+session.ID.String()+"/complete", body, headers)
	if r.Code != 201 {
		f.t.Fatalf("fixture real complete: %d", r.Code)
	}
	var file struct{ ID uuid.UUID }
	if json.Unmarshal(r.Body.Bytes(), &file) != nil || file.ID == uuid.Nil {
		f.t.Fatal("fixture result missing")
	}
	f.download(file.ID, data)
	return file.ID
}

func (f *pairFixture) source(name string, data []byte) string {
	f.t.Helper()
	path := filepath.Join(f.directory, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chtimes(path, pairModTime, pairModTime); err != nil {
		f.t.Fatal(err)
	}
	manifest, _ := json.Marshal(struct {
		Size   int
		SHA256 [32]byte
	}{len(data), sha256.Sum256(data)})
	if err := os.WriteFile(filepath.Join(f.directory, fmt.Sprintf("source-%x.manifest.json", sha256.Sum256([]byte(name)))), manifest, 0600); err != nil {
		f.t.Fatal(err)
	}
	return path
}

func pairSource(t *testing.T, path string, want []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || len(b) != len(want) || sha256.Sum256(b) != sha256.Sum256(want) {
		t.Fatal("failed move lost or changed original source bytes")
	}
}

func (f *pairFixture) rows() string {
	f.t.Helper()
	var value string
	err := f.pool.QueryRow(f.ctx, `SELECT jsonb_build_object(
 'sessions',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM teldrive.upload_sessions s),
 'upload_parts',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no),'[]') FROM teldrive.upload_parts p),
 'files',(SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM teldrive.files x),
 'file_parts',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no),'[]') FROM teldrive.file_parts p),
 'shares',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM teldrive.file_shares s),
 'receipts',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM teldrive.remote_write_receipts r),
 'admissions',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY id),'[]') FROM teldrive.request_admissions a),
 'events',(SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM teldrive.user_events e))::text`).Scan(&value)
	if err != nil {
		f.t.Fatal("paired exact row/part/share/event snapshot failed")
	}
	if err = os.WriteFile(filepath.Join(f.directory, "rows.json"), []byte(value), 0600); err != nil {
		f.t.Fatal("private row evidence write failed")
	}
	return value
}

func (f *pairFixture) requests() []pairRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pairRequest(nil), f.trace...)
}

func (f *pairFixture) serve(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api")
	var body []byte
	if r.Method == "POST" || r.Method == "PATCH" {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "synthetic body read", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	request := pairRequest{Method: r.Method, Path: path, Query: r.URL.RawQuery, Key: r.Header.Get("Idempotency-Key"), Body: body}
	f.mu.Lock()
	index := len(f.trace)
	f.trace = append(f.trace, request)
	shouldGate := !f.gateUsed && f.gate != nil && ((f.gateMode == "complete" || f.gateMode == "published") && strings.HasSuffix(path, "/complete") || f.gateMode == "create" && r.Method == "POST" && path == "/v1/uploads")
	if shouldGate {
		f.gateUsed = true
	}
	transient := false
	if r.Method == "POST" && path == "/v1/uploads" {
		var in struct{ Name string }
		_ = json.Unmarshal(body, &in)
		if f.transientName != "" && in.Name == f.transientName {
			transient = true
		} else if f.transientName == "" && f.transient > 0 {
			f.transient--
			transient = true
		}
	}
	if r.Method == "GET" && path == "/v1/files" && r.URL.Query().Get("parentId") == f.parent.String() && f.listingTransient > 0 {
		f.listingTransient--
		transient = true
	}
	if r.Method == "GET" && path == "/v1/uploads" && r.URL.Query().Get("state") == "open" && f.discoveryTransient > 0 {
		f.discoveryTransient--
		transient = true
	}
	f.mu.Unlock()
	if transient {
		f.mu.Lock()
		f.trace[index].Status = 503
		f.mu.Unlock()
		http.Error(w, "synthetic pre-admission transient", 503)
		return
	}
	// Complete gates before publication, create gates after actual admission COMMIT.
	if shouldGate && f.gateMode == "complete" {
		if err := f.gate.Pause(r.Context()); err != nil {
			http.Error(w, "bounded fixture gate", 503)
			return
		}
	}
	r.URL.Path = path
	response := httptest.NewRecorder()
	f.generated.ServeHTTP(response, r)
	if shouldGate && (f.gateMode == "create" || f.gateMode == "published") {
		if err := f.gate.Pause(r.Context()); err != nil {
			http.Error(w, "bounded fixture gate", 503)
			return
		}
	}
	f.mu.Lock()
	lost := f.truncate && response.Code == 409
	if path == "/v1/uploads" && r.Method == "POST" && response.Code == 201 && f.loseCreate > 0 {
		f.loseCreate--
		lost = true
	}
	if strings.HasSuffix(path, "/complete") && response.Code == 201 && f.loseComplete > 0 {
		f.loseComplete--
		lost = true
	}
	if strings.HasSuffix(path, "/copy") && response.Code == 201 && f.loseCopySuccess > 0 {
		f.loseCopySuccess--
		lost = true
	}
	f.trace[index].Status = response.Code
	f.trace[index].Lost = lost
	if f.mixedCreate != nil && r.Method == "POST" && path == "/v1/uploads" && response.Code == 201 {
		close(f.mixedCreate)
		f.mixedCreate = nil
	}
	f.mu.Unlock()
	for key, values := range response.Header() {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if lost {
		// Keep every terminal body undecodable, including selected-session GET.
		w.Header().Set("Content-Length", strconv.Itoa(response.Body.Len()+1))
		w.WriteHeader(response.Code)
		_, _ = w.Write([]byte("{"))
		if flush, ok := w.(http.Flusher); ok {
			flush.Flush()
		}
		if hijack, ok := w.(http.Hijacker); ok {
			conn, _, err := hijack.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}
		return
	}
	w.WriteHeader(response.Code)
	_, _ = w.Write(response.Body.Bytes())
}

type pairCommandResult struct {
	Err    error
	Output []byte
}

func (f *pairFixture) start(stdin []byte, args ...string) <-chan pairCommandResult {
	f.t.Helper()
	// Hash again immediately before every launch, never discover an installed CLI.
	if pairCandidate(f.t) != f.binary {
		f.t.Fatal("candidate changed during paired proof")
	}
	base := []string{"--config", f.config, "--cache-dir", filepath.Join(f.directory, "cache"), "--transfers", "1", "--checkers", "1", "--multi-thread-streams", "1", "--retries", "3", "--low-level-retries", "3", "--retries-sleep", "0s", "--stats", "0", "-vv"}
	ctx, cancel := context.WithCancel(f.ctx)
	f.cancelCommand = cancel
	command := exec.CommandContext(ctx, f.binary, append(args, base...)...)
	command.Env = []string{"HOME=" + f.directory, "TMPDIR=" + f.directory, "XDG_CONFIG_HOME=" + f.directory, "XDG_CACHE_HOME=" + filepath.Join(f.directory, "cache"), "PATH=/usr/bin:/bin"}
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	result := make(chan pairCommandResult, 1)
	exited := make(chan struct{})
	f.t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			f.t.Error("candidate cleanup failed to reap child")
		}
	})
	go func() {
		defer close(exited)
		output, err := command.CombinedOutput()
		if writeErr := os.WriteFile(filepath.Join(f.directory, "candidate.log"), output, 0600); writeErr != nil {
			err = errors.Join(err, errors.New("private candidate evidence write failed"))
		}
		result <- pairCommandResult{Err: err, Output: output}
	}()
	return result
}
func (f *pairFixture) finish(done <-chan pairCommandResult, success bool) pairCommandResult {
	f.t.Helper()
	var result pairCommandResult
	select {
	case result = <-done:
	case <-f.ctx.Done():
		f.t.Fatal("bounded candidate command did not exit")
	}
	if (result.Err == nil) != success {
		f.t.Fatalf("candidate success=%v, wanted %v; inspect private candidate evidence", result.Err == nil, success)
	}
	if f.ctx.Err() != nil {
		f.t.Fatal("candidate used fixture deadline instead of reaching bounded outcome")
	}
	return result
}

type pairLoser struct {
	directory, mixed, outer, truncated, lostCreate    bool
	comparison, firstPresent, transformed, raw, spool bool
	compareLocal, checkFirst, fastList, checked       bool
}

func (f *pairFixture) target(name string) string { return "fixture:" + f.namespace + "/" + name }

func (f *pairFixture) protected(ids []uuid.UUID) string {
	f.t.Helper()
	var result string
	err := f.pool.QueryRow(f.ctx, `SELECT jsonb_build_object(
 'files',(SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM teldrive.files x WHERE id=ANY($1::uuid[])),
 'parts',(SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY file_id,part_no),'[]') FROM teldrive.file_parts x WHERE file_id=ANY($1::uuid[])),
 'shares',(SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM teldrive.file_shares x WHERE file_id=ANY($1::uuid[])))::text`, ids).Scan(&result)
	if err != nil {
		f.t.Fatal("independent protected row oracle failed")
	}
	return result
}

func (f *pairFixture) admission() (uuid.UUID, uuid.UUID, string) {
	f.t.Helper()
	var id, key uuid.UUID
	var row string
	if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(upload_id,copy_operation_id),request_key,to_jsonb(a)::text FROM teldrive.request_admissions a WHERE user_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1`, f.owner).Scan(&id, &key, &row); err != nil {
		f.t.Fatal("real original SQL admission absent")
	}
	return id, key, row
}

func (f *pairFixture) terminal(id uuid.UUID, copy bool) string {
	f.t.Helper()
	var row string
	query := `SELECT to_jsonb(s)::text FROM teldrive.upload_sessions s WHERE id=$1 AND terminal_failure_code='upload_parent_unavailable' AND state='aborted'`
	if copy {
		query = `SELECT jsonb_build_object('id',id,'operation',operation_id,'result',result_file_id,'destination',destination_parent_id,'root',destination_is_root,'token',lease_token,'terminal',terminal_failure_code,'epoch',terminal_at,'published',published_at)::text FROM teldrive.remote_write_receipts WHERE id=$1 AND row_kind='copy_operation' AND terminal_failure_code='copy_destination_unavailable' AND terminal_at IS NOT NULL AND published_at IS NULL`
	}
	if err := f.pool.QueryRow(f.ctx, query, id).Scan(&row); err != nil {
		f.t.Fatal("original admitted operation lacks durable terminal epoch")
	}
	return row
}

func (f *pairFixture) eventSnapshot() string {
	f.t.Helper()
	var result string
	if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY id),'[]')::text FROM teldrive.user_events e`).Scan(&result); err != nil {
		f.t.Fatal("exact event snapshot failed")
	}
	return result
}

func (f *pairFixture) loseParent(restoreIDs []uuid.UUID) {
	f.t.Helper()
	if _, err := f.catalog.Trash(f.ctx, f.owner, f.parent); err != nil {
		f.t.Fatal("real parent trash failed")
	}
	if _, err := f.catalog.Restore(f.ctx, f.owner, f.parent); err != nil {
		f.t.Fatal("real parent restore failed")
	}
	for _, id := range restoreIDs {
		var status string
		if err := f.pool.QueryRow(f.ctx, "SELECT status::text FROM teldrive.files WHERE id=$1", id).Scan(&status); err != nil {
			f.t.Fatal(err)
		}
		if status == "trashed" {
			if _, err := f.catalog.Restore(f.ctx, f.owner, id); err != nil {
				f.t.Fatal("real old target restore failed")
			}
		} else if status != "active" {
			f.t.Fatal("old target unavailable after parent restore")
		}
	}
	f.mu.Lock()
	f.terminalAt = len(f.trace)
	f.mu.Unlock()
}

func pairPayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

func runPairLoser(t *testing.T, binary string, spec pairLoser, size int) {
	t.Helper()
	f := newPair(t, binary)
	data := pairPayload(size)
	name := "z-target"
	if spec.transformed {
		name = "b"
	}
	effective := name
	if spec.transformed {
		effective = "B"
	}
	old := bytes.Repeat([]byte{0xc3}, min(size, 4096))
	oldID := f.publish(&f.parent, effective, old)
	if spec.checked {
		f.setTime(oldID, pairModTime.Add(-time.Hour))
	}
	var err error
	// First pass must not see the equality trap, except first-present CopyDest.
	if !spec.checked {
		if _, err = f.catalog.Trash(f.ctx, f.owner, oldID); err != nil {
			t.Fatal(err)
		}
	}
	restore := []uuid.UUID{oldID}
	if spec.transformed {
		lower := f.publish(&f.parent, name, old)
		if _, err = f.catalog.Trash(f.ctx, f.owner, lower); err != nil {
			t.Fatal(err)
		}
		restore = append(restore, lower)
	}
	sourceName := "source/original"
	if spec.directory || spec.comparison {
		sourceName = "source/" + name
	}
	source := f.source(sourceName, data)
	args := []string{"moveto", source, f.target(name)}
	if spec.directory {
		args = []string{"move", filepath.Dir(source), f.target("")}
		if spec.checkFirst {
			args = append(args, "--check-first", "--order-by", "name")
		}
		if spec.fastList {
			args = append(args, "--fast-list")
		}
	}
	if spec.transformed {
		args = append(args, "--name-transform", "uppercase")
	}
	if spec.checked {
		args = append(args, "--ignore-times")
	}
	var comparisonID uuid.UUID
	var comparisonPath string
	comparisonBytes := data
	if !spec.firstPresent {
		comparisonBytes = old
	}
	if spec.comparison {
		if !spec.checked {
			args = append(args, "--no-check-dest")
		}
		if spec.compareLocal {
			comparisonPath = filepath.Join(f.directory, "comparison")
			if err = os.Mkdir(comparisonPath, 0700); err != nil {
				t.Fatal(err)
			}
			args = append(args, "--compare-dest", comparisonPath)
		} else {
			comparisonParent := f.folder(f.namespace+"_comparison", nil)
			comparisonID = f.publish(&comparisonParent, name, comparisonBytes)
			if !spec.firstPresent {
				if _, err = f.catalog.Trash(f.ctx, f.owner, comparisonID); err != nil {
					t.Fatal(err)
				}
			}
			args = append(args, "--copy-dest", "fixture:"+f.namespace+"_comparison")
		}
	}
	if spec.mixed {
		f.source("source/a-transient", []byte("separate retryable source"))
		// Exhaust discovery before any create request can become uncertain intent.
		f.discoveryTransient = 9
		f.transientName = "a-transient"
		f.mixedCreate = make(chan struct{})
		args = append(args, "--order-by", "name")
	}
	if spec.raw {
		args = []string{"rcat", f.target(name), "--streaming-upload-cutoff", "32Mi"}
		if spec.spool {
			args = append(args, "--streaming-upload-cutoff", "1", "--disable", "PutStream")
		}
	}
	if spec.firstPresent {
		f.gate = fixture.NewBarrier(t, nil)
		f.gateMode = "copy"
		f.storage.SetSendFaults(fixture.SendFaults{AfterCopy: f.gate})
	} else {
		f.gate = fixture.NewBarrier(t, nil)
		f.gateMode = "complete"
		if spec.lostCreate {
			f.gateMode = "create"
			f.loseCreate = 1
		}
	}
	f.truncate = spec.truncated
	_, copiesBefore := f.storage.counts()
	beforeAdmissions := 0
	if err = f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.request_admissions").Scan(&beforeAdmissions); err != nil {
		t.Fatal(err)
	}
	startTrace := len(f.requests())
	var stdin []byte
	if spec.raw {
		stdin = data
	}
	mixedCreate := f.mixedCreate
	done := f.start(stdin, args...)
	if mixedCreate != nil {
		select {
		case <-mixedCreate:
		case <-f.ctx.Done():
			t.Fatal("unrelated discovery failure prevented actual target admission")
		}
	}
	if err = f.gate.Wait(f.ctx); err != nil {
		t.Fatal("actual candidate did not reach committed admission/remote barrier")
	}
	id, key, admission := f.admission()
	if size > 64<<20 {
		var parts int
		if err = f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.upload_parts WHERE upload_id=$1 AND state='stored'", id).Scan(&parts); err != nil || parts != 2 {
			t.Fatal("multipart variant did not store both actual candidate parts")
		}
	}
	if spec.mixed {
		count := 0
		for _, r := range f.requests()[startTrace:] {
			if r.Status == 503 {
				if r.Method != "GET" || r.Path != "/v1/uploads" || !strings.Contains(r.Query, "state=open") {
					t.Fatal("mixed failure did not precede upload-create admission")
				}
				count++
			}
		}
		if count != 9 {
			t.Fatal("mixed fixture did not observe earlier exhausted retryable requests")
		}
	}
	if spec.firstPresent {
		var transport uuid.UUID
		var fingerprint []byte
		if err = f.pool.QueryRow(f.ctx, `SELECT request_fingerprint FROM teldrive.request_admissions WHERE copy_operation_id=$1`, id).Scan(&fingerprint); err != nil || len(fingerprint) != 32 {
			t.Fatal("copy fingerprint absent")
		}
		found := false
		for _, r := range f.requests()[startTrace:] {
			if strings.HasSuffix(r.Path, "/copy") {
				transport = uuid.MustParse(strings.TrimSuffix(strings.TrimPrefix(r.Path, "/v1/files/"), "/copy"))
				found = transport == comparisonID && r.Key == key.String()
			}
		}
		if !found {
			t.Fatal("actual comparison transport FileID/key did not own original copy admission")
		}
	}
	f.loseParent(restore)
	share, err := f.shares.Create(f.ctx, shares.CreateInput{OwnerID: f.owner, FileID: oldID, Permission: sqlcgen.SharePermissionRead})
	if err != nil {
		t.Fatal(err)
	}
	if spec.checked {
		f.setTime(oldID, pairModTime)
	}
	f.denials(id, share.Token, size)
	if spec.comparison && !spec.firstPresent {
		if spec.compareLocal {
			path := filepath.Join(comparisonPath, name)
			if err = os.WriteFile(path, old, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Chtimes(path, pairModTime, pairModTime); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err = f.catalog.Restore(f.ctx, f.owner, comparisonID); err != nil {
				t.Fatal(err)
			}
		}
	}
	terminal := f.terminal(id, spec.firstPresent)
	events := f.eventSnapshot()
	protected := f.protected(restore)
	f.rows()
	f.gate.Release()
	result := f.finish(done, false)
	requests := f.requests()[startTrace:]
	if spec.mixed {
		// These post-command fault probes are not counted as candidate RPCs.
		beforeProbe := f.rows()
		body, _ := json.Marshal(map[string]any{"parentId": f.parent, "name": "a-transient", "size": 25, "modTime": pairModTime})
		for range 2 {
			response := performRequest(t, http.HandlerFunc(f.serve), "POST", "/v1/uploads", body,
				map[string]string{"Authorization": "Bearer test-token", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()})
			if response.Code != 503 || f.rows() != beforeProbe {
				t.Fatal("later transient create escaped persistent pre-admission rejection")
			}
		}
	}
	pairSource(t, source, data)
	if spec.mixed {
		pairSource(t, filepath.Join(f.directory, "source/a-transient"), []byte("separate retryable source"))
	}
	if f.terminal(id, spec.firstPresent) != terminal {
		t.Fatal("retry changed exact original terminal session/operation")
	}
	afterID, afterKey, afterAdmission := f.admission()
	if afterID != id || afterKey != key || afterAdmission != admission {
		t.Fatal("retry changed immutable original ID/key/scope/request fingerprint")
	}
	var admissions int
	if err = f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.request_admissions").Scan(&admissions); err != nil || admissions != beforeAdmissions+1 {
		t.Fatal("loser created replacement/adopted another admission")
	}
	if f.eventSnapshot() != events || f.protected(restore) != protected {
		t.Fatal("loser changed old destination/share/parts/events or published replacement")
	}
	for _, oldFile := range restore {
		f.download(oldFile, old)
	}
	shared := performRequest(t, f.generated, "GET", "/v1/public/shares/"+share.Token+"/content", nil, nil)
	if shared.Code != 200 || sha256.Sum256(shared.Body.Bytes()) != sha256.Sum256(old) {
		t.Fatal("old target share bytes changed")
	}
	if spec.comparison && !spec.compareLocal {
		f.download(comparisonID, comparisonBytes)
	}
	if spec.compareLocal {
		pairSource(t, filepath.Join(comparisonPath, name), old)
	}
	if err = f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
	_, copiesAfter := f.storage.counts()
	if copiesAfter-copiesBefore != boolInt(spec.firstPresent) {
		t.Fatal("losing upload/copy performed extra CopyPart")
	}
	exactGets, completeCalls, creates, copies, terminalResponses := 0, 0, 0, 0, 0
	var frozenUpload, frozenCopy string
	f.mu.Lock()
	afterLoss := f.terminalAt - startTrace
	f.mu.Unlock()
	for index, r := range requests {
		if r.Method == "POST" && r.Path == "/v1/uploads" {
			var in struct {
				Name     string
				ParentID uuid.UUID `json:"parentId"`
			}
			if json.Unmarshal(r.Body, &in) != nil {
				t.Fatal("actual frozen request is not valid JSON")
			}
			if spec.mixed && in.Name == "a-transient" {
				if creates == 0 || r.Status != 503 {
					t.Fatal("mixed transient source reached create before discovery exhaustion or escaped rejection")
				}
				continue
			}
			creates++
			body := pairCanonical(r.Body)
			if frozenUpload == "" {
				frozenUpload = body
			}
			if r.Key != key.String() || body != frozenUpload || in.ParentID != f.parent || in.Name != effective {
				t.Fatal("upload retry changed frozen key/request/parent/effective name")
			}
		}
		if strings.HasSuffix(r.Path, "/copy") {
			copies++
			body := pairCanonical(r.Body)
			if frozenCopy == "" {
				frozenCopy = body
			}
			if r.Key != key.String() || body != frozenCopy {
				t.Fatal("copy retry changed frozen key/request")
			}
		}
		if r.Method == "GET" && r.Path == "/v1/uploads/"+id.String() {
			exactGets++
		}
		if strings.HasSuffix(r.Path, "/complete") {
			completeCalls++
		}
		if r.Status == 409 {
			terminalResponses++
			if spec.truncated && !r.Lost {
				t.Fatal("decoded terminal response rescued truncated fixture")
			}
		}
		if r.Method == "POST" && r.Path == "/v1/folders" {
			t.Fatal("automatic retry recreated destination path")
		}
		// Mixed siblings share parent-list transport; source-labelled proof guards z-target.
		if !spec.mixed && index >= afterLoss && r.Method == "GET" && r.Path == "/v1/files" && strings.Contains(r.Query, f.parent.String()) {
			t.Fatal("retained retry reached primary metadata before refusal")
		}
	}
	if terminalResponses == 0 {
		t.Fatal("candidate never received real terminal generated response")
	}
	if spec.firstPresent && (creates != 0 || completeCalls != 0 || copies < 1) {
		t.Fatal("admitted alternate copy reached same-pass upload fallback")
	}
	if !spec.firstPresent && creates < 1 {
		t.Fatal("actual candidate upload admission not observed")
	}
	if spec.lostCreate && creates < 2 {
		t.Fatal("lost create acknowledgement was not replayed through actual pacer")
	}
	if spec.truncated && !spec.outer && !spec.raw && completeCalls+exactGets < 2 {
		t.Fatal("truncated terminal low-level retry was not observed")
	}
	if spec.outer {
		// The request sequence supplements actual cmd.Run attempt output: the
		// first pacer and post-failure guard exhaust before a fresh guard pass.
		if spec.firstPresent || spec.lostCreate {
			if copies+creates < 3 {
				t.Fatal("original frozen request did not exhaust actual pacer ambiguity")
			}
		} else if spec.mixed {
			if completeCalls+exactGets < 3 {
				t.Fatal("mixed terminal pacer ambiguity was not exhausted")
			}
		} else if exactGets < 6 {
			t.Fatal("no repeated exact discovery evidence after exhausted low-level ambiguity")
		}
		if !pairSecondPass(result.Output) {
			t.Fatal("actual second cmd.Run pass not observed")
		}
	} else if !spec.raw && !spec.truncated && pairSecondPass(result.Output) {
		t.Fatal("decoded terminal cause failed to stop actual cmd.Run")
	}
	if spec.raw {
		if bytes.Contains(result.Output, []byte("Attempt 2/3")) {
			t.Fatal("raw rcat falsely gained outer retry")
		}
		if exactGets == 0 {
			t.Fatal("real inner Put retry did not reconcile original selection")
		}
	}
	f.rows()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestB2PairedLifecycleRoundTrip(t *testing.T) {
	binary := pairCandidate(t)
	losers := []struct {
		name     string
		spec     pairLoser
		variants bool
	}{
		{"terminal_single_outer_retry", pairLoser{}, true},
		{"terminal_mixed_outer_retry", pairLoser{directory: true, mixed: true}, true},
		{"lost_create_ack_low_level_retry", pairLoser{lostCreate: true}, false},
		{"lost_create_ack_outer_retry", pairLoser{lostCreate: true, truncated: true, outer: true}, false},
		{"truncated_terminal_low_level_retry", pairLoser{truncated: true}, false},
		{"truncated_terminal_mixed_outer_retry", pairLoser{directory: true, mixed: true, truncated: true, outer: true}, false},
		{"skip_equal_restored_single_outer_retry", pairLoser{truncated: true, outer: true}, false},
		{"skip_equal_restored_directory_outer_retry", pairLoser{directory: true, truncated: true, outer: true}, false},
		{"skip_equal_restored_fast_list_outer_retry", pairLoser{directory: true, checkFirst: true, fastList: true, truncated: true, outer: true}, false},
		{"no_check_dest_compare_dest_outer_retry", pairLoser{comparison: true, compareLocal: true, truncated: true, outer: true}, false},
		{"no_check_dest_copy_dest_outer_retry", pairLoser{comparison: true, truncated: true, outer: true}, false},
		{"copy_dest_first_present_single_parent_loss", pairLoser{comparison: true, firstPresent: true}, false},
		{"copy_dest_first_present_single_truncated_outer_retry", pairLoser{comparison: true, firstPresent: true, truncated: true, outer: true}, false},
		{"copy_dest_first_present_directory_parent_loss", pairLoser{directory: true, comparison: true, firstPresent: true, checkFirst: true}, false},
		{"copy_dest_first_present_directory_truncated_outer_retry", pairLoser{directory: true, comparison: true, firstPresent: true, truncated: true, outer: true, checkFirst: true}, false},
		{"ordinary_transformed_upload_truncated_outer_retry", pairLoser{transformed: true, truncated: true, outer: true}, false},
		{"raw_rcat_buffered_terminal_inner_retry", pairLoser{raw: true, truncated: true}, false},
	}
	for _, row := range losers {
		t.Run(row.name, func(t *testing.T) {
			if row.variants {
				for _, variant := range []struct {
					name string
					size int
				}{{"sequential", 4096}, {"multipart", (64 << 20) + 113}, {"zero", 0}} {
					t.Run(variant.name, func(t *testing.T) { runPairLoser(t, binary, row.spec, variant.size) })
				}
			} else if row.spec.raw {
				for _, spool := range []bool{false, true} {
					name := "buffered"
					if spool {
						name = "spooled"
					}
					t.Run(name, func(t *testing.T) { spec := row.spec; spec.spool = spool; runPairLoser(t, binary, spec, 4096) })
				}
			} else if row.spec.directory && row.spec.firstPresent {
				for _, checked := range []bool{false, true} {
					name := "no_check_dest"
					if checked {
						name = "checked_pair"
					}
					t.Run(name, func(t *testing.T) { spec := row.spec; spec.checked = checked; runPairLoser(t, binary, spec, 4096) })
				}
			} else {
				runPairLoser(t, binary, row.spec, 4096)
			}
		})
	}
	t.Run("compare_dest_no_admission_control", func(t *testing.T) { runPairComparisonControl(t, binary, false) })
	t.Run("copy_dest_no_admission_control", func(t *testing.T) { runPairComparisonControl(t, binary, true) })
	t.Run("copy_success_reply_replay", func(t *testing.T) { runPairCopyReplay(t, binary, true) })
	t.Run("copy_terminal_reply_replay", func(t *testing.T) { runPairCopyReplay(t, binary, false) })
	t.Run("dirmove_terminal_outer_retry", func(t *testing.T) { runPairDirMove(t, binary) })
	t.Run("explicit_new_invocation", func(t *testing.T) { runPairExplicit(t, binary) })
	t.Run("legacy_detached_empty_move_refused", func(t *testing.T) { runPairLegacy(t, binary) })
	t.Run("ordinary_stream_manual_fallback_control", func(t *testing.T) {
		for _, branch := range []string{"stream", "buffered", "spooled"} {
			t.Run(branch, func(t *testing.T) { runPairStream(t, binary, branch) })
		}
	})
	t.Run("ordinary_transformed_manual_fallback_control", func(t *testing.T) { runPairUploadControl(t, binary, true, false, false) })
	t.Run("ordinary_transformed_upload_transient_control", func(t *testing.T) { runPairUploadControl(t, binary, true, true, false) })
	t.Run("raw_rcat_transient_inner_retry_control", func(t *testing.T) {
		for _, spool := range []bool{false, true} {
			name := "buffered"
			if spool {
				name = "spooled"
			}
			t.Run(name, func(t *testing.T) { runPairRawControl(t, binary, spool) })
		}
	})
	t.Run("transient_outer_retry_control", func(t *testing.T) { runPairUploadControl(t, binary, false, true, true) })
	t.Run("unchanged_move_success_control", func(t *testing.T) {
		t.Run("equal_without_admission", func(t *testing.T) { runPairUnchanged(t, binary) })
		t.Run("publication_first", func(t *testing.T) { runPairPublicationFirst(t, binary, false) })
		t.Run("commit_ack_ambiguity_unavailable_database", func(t *testing.T) { runPairPublicationFirst(t, binary, true) })
		t.Run("hash_mismatch_preserves_previous_result", func(t *testing.T) { runPairHashControl(t, binary) })
		t.Run("observation_helpers", testPairObservationHelpers)
	})
}

func (f *pairFixture) active(name string) uuid.UUID {
	f.t.Helper()
	var id uuid.UUID
	if err := f.pool.QueryRow(f.ctx, "SELECT id FROM teldrive.files WHERE user_id=$1 AND parent_id=$2 AND name=$3 AND status='active'", f.owner, f.parent, name).Scan(&id); err != nil {
		f.t.Fatal("actual result absent")
	}
	return id
}

func (f *pairFixture) assertOneRequest(kind string) uuid.UUID {
	f.t.Helper()
	var id uuid.UUID
	var count int
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.request_admissions WHERE request_kind=$1", kind).Scan(&count); err != nil || count != 1 {
		f.t.Fatal("control did not retain exactly one actual admission")
	}
	if err := f.pool.QueryRow(f.ctx, "SELECT coalesce(upload_id,copy_operation_id) FROM teldrive.request_admissions WHERE request_kind=$1", kind).Scan(&id); err != nil {
		f.t.Fatal(err)
	}
	var key string
	var body string
	calls := 0
	for _, r := range f.requests() {
		if r.Method != "POST" || (kind == "upload" && r.Path != "/v1/uploads") || (kind == "copy" && !strings.HasSuffix(r.Path, "/copy")) {
			continue
		}
		if key == "" {
			key = r.Key
			body = string(r.Body)
		}
		if key != r.Key || body != string(r.Body) {
			f.t.Fatal("automatic retry changed frozen key/request")
		}
		calls++
	}
	if calls == 0 || key == "" {
		f.t.Fatal("actual keyed request not observed")
	}
	return id
}

func runPairUploadControl(t *testing.T, binary string, transformed, transient, outer bool) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	if outer {
		runPairOuterMove(t, f, data)
		return
	}
	source := f.source("source/original", data)
	name := "z-target"
	if transformed {
		name = "b"
	}
	args := []string{"moveto", source, f.target(name)}
	if transformed {
		args = append(args, "--name-transform", "uppercase")
	}
	if transient {
		f.transient = 2
	}
	f.finish(f.start(nil, args...), true)
	effective := name
	if transformed {
		effective = "B"
	}
	f.download(f.active(effective), data)
	f.assertOneRequest("upload")
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("successful move retained its owned source")
	}
	_, copies := f.storage.counts()
	if copies != 0 {
		t.Fatal("local ordinary manual path unexpectedly server-copied")
	}
	for _, r := range f.requests() {
		if r.Path == "/v1/uploads" && r.Method == "POST" {
			var in struct {
				Name     string
				ParentID uuid.UUID `json:"parentId"`
			}
			if json.Unmarshal(r.Body, &in) != nil || in.Name != effective || in.ParentID != f.parent {
				t.Fatal("manual upload lost requested/effective destination mapping")
			}
		}
	}
	if transient {
		n := 0
		for _, r := range f.requests() {
			if r.Status == 503 {
				n++
			}
		}
		if n != 2 {
			t.Fatal("ordinary transient control never retried real transport")
		}
	}
	f.rows()
}

func runPairRawControl(t *testing.T, binary string, spool bool) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	f.source("stdin", data)
	f.transient = 3
	args := []string{"rcat", f.target("z-target"), "--streaming-upload-cutoff", "32Mi"}
	if spool {
		args = append(args, "--streaming-upload-cutoff", "1", "--disable", "PutStream")
	}
	result := f.finish(f.start(data, args...), true)
	f.assertOneRequest("upload")
	f.download(f.active("z-target"), data)
	calls := 0
	for _, r := range f.requests() {
		if r.Method == "POST" && r.Path == "/v1/uploads" {
			calls++
		}
	}
	if calls < 4 || bytes.Contains(result.Output, []byte("Attempt 2/3")) {
		t.Fatal("raw transient control did not use real inner Retry without outer retry")
	}
	needle := []byte("File to upload is small")
	if spool {
		needle = []byte("creating temporary local FS to spool file")
	}
	if !bytes.Contains(result.Output, needle) {
		t.Fatal("actual raw buffered/spooled branch not observed")
	}
	pairSource(t, filepath.Join(f.directory, "stdin"), data)
	f.rows()
}

func runPairUnchanged(t *testing.T, binary string) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	id := f.publish(&f.parent, "z-target", data)
	source := f.source("source", data)
	before := f.rows()
	f.finish(f.start(nil, "moveto", source, f.target("z-target")), true)
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("ordinary equality move did not remove only its source")
	}
	if f.rows() != before {
		t.Fatal("unchanged control unexpectedly admitted or changed remote rows")
	}
	f.download(id, data)
}

func runPairComparisonControl(t *testing.T, binary string, copyDest bool) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	source := f.source("source/z-target", data)
	args := []string{"moveto", source, f.target("z-target"), "--no-check-dest"}
	var comparison uuid.UUID
	if copyDest {
		parent := f.folder(f.namespace+"_comparison", nil)
		comparison = f.publish(&parent, "z-target", data)
		args = append(args, "--copy-dest", "fixture:"+f.namespace+"_comparison")
	} else {
		path := filepath.Join(f.directory, "comparison")
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(path, "z-target")
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(file, pairModTime, pairModTime); err != nil {
			t.Fatal(err)
		}
		args = append(args, "--compare-dest", path)
	}
	uploadsBefore, copiesBefore := f.storage.counts()
	before := f.rows()
	f.finish(f.start(nil, args...), true)
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("single-file comparison control failed ordinary source-removal semantics")
	}
	uploadsAfter, copiesAfter := f.storage.counts()
	if uploadsAfter != uploadsBefore {
		t.Fatal("comparison control silently used fallback upload")
	}
	if copyDest {
		if copiesAfter != copiesBefore+1 {
			t.Fatal("copy-dest control did not execute actual CopyPart")
		}
		f.download(comparison, data)
		f.download(f.active("z-target"), data)
	} else if copiesAfter != copiesBefore || f.rows() != before {
		t.Fatal("compare-dest skip mutated destination")
	}
	f.rows()
}

func runPairStream(t *testing.T, binary, branch string) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	f.source("http-source", data)
	var mu sync.Mutex
	heads, reads := 0, 0
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Method == "HEAD" {
			heads++
		}
		if r.URL.Path == "/b" {
			reads++
		}
		mu.Unlock()
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<a href="b">b</a>`)
			return
		}
		if r.URL.Path != "/b" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = w.Write(data)
	}))
	defer source.Close()
	config, err := os.OpenFile(f.config, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(config, "\n[source]\ntype = http\nurl = %s\n", source.URL)
	closeErr := config.Close()
	if err != nil || closeErr != nil {
		t.Fatal("owned HTTP source config write failed")
	}
	args := []string{"copyto", "source:b", f.target("b"), "--http-no-head", "--name-transform", "uppercase", "--streaming-upload-cutoff", "1"}
	if branch == "buffered" {
		args = append(args, "--streaming-upload-cutoff", "32Mi")
	}
	if branch == "spooled" {
		args = append(args, "--disable", "PutStream")
	}
	result := f.finish(f.start(nil, args...), true)
	f.download(f.active("B"), data)
	f.assertOneRequest("upload")
	mu.Lock()
	headCount, readCount := heads, reads
	mu.Unlock()
	if headCount != 0 || readCount == 0 {
		t.Fatal("HTTP source did not use actual --http-no-head GET")
	}
	expectedSize := int64(len(data))
	if branch == "stream" {
		expectedSize = -1
	}
	found := false
	for _, r := range f.requests() {
		if r.Path == "/v1/uploads" && r.Method == "POST" {
			var in struct {
				Name string
				Size int64
			}
			if json.Unmarshal(r.Body, &in) != nil || in.Name != "B" || in.Size != expectedSize {
				t.Fatal("actual PutStream/buffered upload wire metadata differs")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("nonlocal manual path never admitted actual upload")
	}
	if branch == "buffered" && !bytes.Contains(result.Output, []byte("File to upload is small")) {
		t.Fatal("real buffered Rcat branch absent")
	}
	if branch == "spooled" && !bytes.Contains(result.Output, []byte("creating temporary local FS to spool file")) {
		t.Fatal("real spooled Rcat branch absent")
	}
	pairSource(t, filepath.Join(f.directory, "http-source"), data)
	f.rows()
}

func runPairOuterMove(t *testing.T, f *pairFixture, data []byte) {
	source := f.source("source/z-target", data)
	f.listingTransient = 3
	result := f.finish(f.start(nil, "move", filepath.Dir(source), f.target("")), true)
	listings, failures, successes := 0, 0, 0
	for _, request := range f.requests() {
		if request.Method != "GET" || request.Path != "/v1/files" || !strings.Contains(request.Query, f.parent.String()) {
			continue
		}
		listings++
		if request.Status == 503 {
			failures++
		}
		if request.Status == 200 {
			successes++
		}
	}
	if listings < 4 || failures != 3 || successes == 0 || !pairSecondPass(result.Output) {
		t.Fatal("transient-only move did not exhaust first listing then succeed on real outer pass")
	}
	f.download(f.active("z-target"), data)
	f.assertOneRequest("upload")
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("successful transient outer move did not remove only owned source")
	}
	f.rows()
}

func (f *pairFixture) rawFile(id uuid.UUID, want []byte) {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, "SELECT channel_id,message_id FROM teldrive.file_parts WHERE file_id=$1 ORDER BY part_no", id)
	if err != nil {
		f.t.Fatal("independent old part query failed")
	}
	defer rows.Close()
	h := sha256.New()
	size := 0
	for rows.Next() {
		var channel, message int64
		if rows.Scan(&channel, &message) != nil {
			f.t.Fatal("old part identity decode failed")
		}
		b, ok := f.storage.Bytes(channel, message)
		if !ok {
			f.t.Fatal("old physical part disappeared")
		}
		_, _ = h.Write(b)
		size += len(b)
	}
	if rows.Err() != nil || size != len(want) || !bytes.Equal(h.Sum(nil), sumPair(want)) {
		f.t.Fatal("full independent old part size/SHA-256 differs")
	}
}
func sumPair(data []byte) []byte { sum := sha256.Sum256(data); return sum[:] }

func runPairCopyReplay(t *testing.T, binary string, success bool) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	old := bytes.Repeat([]byte{0xc3}, len(data))
	srcParent := f.folder(f.namespace+"_source", nil)
	src := f.publish(&srcParent, "original", data)
	oldID := f.publish(&f.parent, "z-target", old)
	share, err := f.shares.Create(f.ctx, shares.CreateInput{OwnerID: f.owner, FileID: oldID, Permission: sqlcgen.SharePermissionRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.catalog.Trash(f.ctx, f.owner, oldID); err != nil {
		t.Fatal(err)
	}
	f.source("independent-source", data)
	start := len(f.requests())
	uploadsBefore, copiesBefore := f.storage.counts()
	protected := f.protected([]uuid.UUID{oldID, src})
	events := f.eventSnapshot()
	var admissionsBefore int
	if err = f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.request_admissions").Scan(&admissionsBefore); err != nil {
		t.Fatal(err)
	}
	var terminal, admission string
	var original, key uuid.UUID
	var done <-chan pairCommandResult
	if success {
		f.loseCopySuccess = 1
		done = f.start(nil, "copyto", "fixture:"+f.namespace+"_source/original", f.target("z-target"))
	} else {
		f.gate = fixture.NewBarrier(t, nil)
		f.storage.SetSendFaults(fixture.SendFaults{AfterCopy: f.gate})
		f.truncate = true
		done = f.start(nil, "copyto", "fixture:"+f.namespace+"_source/original", f.target("z-target"))
		if err = f.gate.Wait(f.ctx); err != nil {
			t.Fatal("actual CopyPart reservation/remote barrier absent")
		}
		original, key, admission = f.admission()
		f.loseParent([]uuid.UUID{oldID})
		share, err = f.shares.Create(f.ctx, shares.CreateInput{OwnerID: f.owner, FileID: oldID, Permission: sqlcgen.SharePermissionRead})
		if err != nil {
			t.Fatal(err)
		}
		terminal = f.terminal(original, true)
		protected = f.protected([]uuid.UUID{oldID, src})
		events = f.eventSnapshot()
		f.gate.Release()
	}
	f.finish(done, success)
	f.download(src, data)
	f.rawFile(oldID, old)
	pairSource(t, filepath.Join(f.directory, "independent-source"), data)
	uploadsAfter, copiesAfter := f.storage.counts()
	if copiesAfter != copiesBefore+1 || uploadsAfter != uploadsBefore {
		t.Fatal("copy reply replay ran extra CopyPart or fallback upload")
	}
	if f.protected([]uuid.UUID{oldID, src}) != protected {
		t.Fatal("copy reply replay changed independent source/previous target/share/parts")
	}
	var admissionsAfter int
	if err = f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.request_admissions").Scan(&admissionsAfter); err != nil || admissionsAfter != admissionsBefore+1 {
		t.Fatal("copy replay admitted replacement identity")
	}
	calls := 0
	var frozenBody string
	var frozenKey string
	for _, r := range f.requests()[start:] {
		if r.Method == "POST" && (r.Path == "/v1/uploads" || r.Path == "/v1/folders") {
			t.Fatal("copy replay entered fallback upload or path recreation")
		}
		if !strings.HasSuffix(r.Path, "/copy") {
			continue
		}
		calls++
		if frozenKey == "" {
			frozenKey = r.Key
			frozenBody = string(r.Body)
		}
		if r.Key != frozenKey || string(r.Body) != frozenBody {
			t.Fatal("copy reply replay changed actual transport fingerprint/key")
		}
		if !success && r.Status == 409 && !r.Lost {
			t.Fatal("decoded terminal body rescued copy replay fixture")
		}
	}
	if calls < 2 {
		t.Fatal("real copy pacer never replayed lost response")
	}
	if success {
		id := f.active("z-target")
		f.download(id, data)
		var count int
		if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM teldrive.remote_write_receipts WHERE row_kind='copy_operation' AND result_file_id=$1 AND published_at IS NOT NULL`, id).Scan(&count); err != nil || count != 1 {
			t.Fatal("success replay changed original operation/result")
		}
		if strings.Count(f.eventSnapshot(), `"file.created"`)-strings.Count(events, `"file.created"`) != 1 {
			t.Fatal("successful copy replay published more than the one original file")
		}
	} else {
		id, k, row := f.admission()
		if id != original || k != key || row != admission || f.terminal(id, true) != terminal {
			t.Fatal("terminal copy replay changed original identity/epoch")
		}
		f.download(oldID, old)
		if f.eventSnapshot() != events {
			t.Fatal("terminal copy replay changed publication/events")
		}
	}
	shared := performRequest(t, f.generated, "GET", "/v1/public/shares/"+share.Token+"/content", nil, nil)
	if !success && (shared.Code != 200 || sha256.Sum256(shared.Body.Bytes()) != sha256.Sum256(old)) {
		t.Fatal("copy failure changed old share")
	}
	f.rows()
}

func runPairExplicit(t *testing.T, binary string) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	source := f.source("source", data)
	f.gate = fixture.NewBarrier(t, nil)
	f.gateMode = "complete"
	done := f.start(nil, "moveto", source, f.target("z-target"), "--ignore-times")
	if err := f.gate.Wait(f.ctx); err != nil {
		t.Fatal("first explicit invocation never admitted")
	}
	id, key, row := f.admission()
	f.loseParent(nil)
	terminal := f.terminal(id, false)
	f.gate.Release()
	f.finish(done, false)
	pairSource(t, source, data)
	f.mu.Lock()
	f.gate = nil
	f.mu.Unlock()
	start := len(f.requests())
	f.finish(f.start(nil, "moveto", source, f.target("z-target"), "--ignore-times"), true)
	next, nextKey, _ := f.admission()
	if next == id || nextKey == key || f.terminal(id, false) != terminal {
		t.Fatal("new explicit invocation changed old terminal intent or reused key")
	}
	var oldRow string
	if err := f.pool.QueryRow(f.ctx, "SELECT to_jsonb(a)::text FROM teldrive.request_admissions a WHERE upload_id=$1", id).Scan(&oldRow); err != nil || oldRow != row {
		t.Fatal("new invocation rewrote original admission")
	}
	found := false
	for _, r := range f.requests()[start:] {
		if r.Path == "/v1/uploads" && r.Method == "POST" {
			found = true
			if r.Key != nextKey.String() {
				t.Fatal("explicit invocation did not use new key")
			}
		}
	}
	if !found {
		t.Fatal("new invocation skipped real admission")
	}
	f.download(f.active("z-target"), data)
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("successful explicit move failed to remove only owned source")
	}
	f.rows()
}

func runPairPublicationFirst(t *testing.T, binary string, unavailable bool) {
	if unavailable {
		runPairCommitACK(t, binary)
		return
	}
	f := newPair(t, binary)
	data := pairPayload(4096)
	source := f.source("source", data)
	f.gate = fixture.NewBarrier(t, nil)
	f.gateMode = "published"
	f.loseComplete = 1
	done := f.start(nil, "moveto", source, f.target("z-target"), "--ignore-times")
	if err := f.gate.Wait(f.ctx); err != nil {
		t.Fatal("actual publication COMMIT barrier absent")
	}
	id := f.active("z-target")
	f.download(id, data)
	session, _, _ := f.admission()
	var fileID uuid.UUID
	if err := f.pool.QueryRow(f.ctx, "SELECT file_id FROM teldrive.upload_sessions WHERE id=$1 AND state='completed'", session).Scan(&fileID); err != nil || fileID != id {
		t.Fatal("publication first did not commit original winner")
	}
	before := f.rows()
	uploadsBefore, copiesBefore := f.storage.counts()
	f.gate.Release()
	f.finish(done, true)
	if f.rows() != before {
		t.Fatal("completion lost-ACK replay changed publication/alias/share/events")
	}
	uploadsAfter, copiesAfter := f.storage.counts()
	if uploadsAfter != uploadsBefore || copiesAfter != copiesBefore {
		t.Fatal("completion replay sent another remote write")
	}
	f.download(id, data)
	var messages []int64
	rows, err := f.pool.Query(f.ctx, "SELECT message_id FROM teldrive.file_parts WHERE file_id=$1 ORDER BY part_no", id)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var message int64
		if rows.Scan(&message) != nil {
			t.Fatal(err)
		}
		messages = append(messages, message)
	}
	scanErr := rows.Err()
	rows.Close()
	if scanErr != nil || len(messages) != 1 {
		t.Fatal("winner actual part identity missing")
	}
	if _, err = f.catalog.Trash(f.ctx, f.owner, f.parent); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = f.pool.QueryRow(f.ctx, "SELECT status::text FROM teldrive.files WHERE id=$1", id).Scan(&status); err != nil || status != "trashed" {
		t.Fatal("ordinary later hierarchy deletion failed to discover committed child")
	}
	if err = f.ops.QueuePurge(f.ctx, f.owner, f.parent); err != nil {
		t.Fatal(err)
	}
	if err = f.ops.PurgeMany(f.ctx, f.owner, []uuid.UUID{f.parent}); err != nil {
		t.Fatal("ordinary owned winner purge failed")
	}
	for _, message := range messages {
		if _, ok := f.storage.Bytes(9001, message); ok {
			t.Fatal("ordinary purge did not include actual winner bytes")
		}
	}
	if err = f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
	f.rows()
}

func runPairHashControl(t *testing.T, binary string) {
	f := newPair(t, binary)
	data := pairPayload(4096)
	old := []byte("independent previous destination")
	oldID := f.publish(&f.parent, "z-target", old)
	source := f.source("source", data)
	before := f.protected([]uuid.UUID{oldID})
	f.gate = fixture.NewBarrier(t, nil)
	f.gateMode = "complete"
	done := f.start(nil, "moveto", source, f.target("z-target"), "--ignore-times")
	if err := f.gate.Wait(f.ctx); err != nil {
		t.Fatal("actual hash-control upload did not reach complete")
	}
	id, _, _ := f.admission()
	snapshot := f.rows()
	bad := []byte(fmt.Sprintf(`{"size":%d,"hash":{"algorithm":"blake3","value":%q}}`, len(data), strings.Repeat("0", 64)))
	headers := map[string]string{"Authorization": "Bearer test-token", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
	r := performRequest(t, f.generated, "POST", "/v1/uploads/"+id.String()+"/complete", bad, headers)
	assertB1APIError(t, r.Code, r.Body.Bytes(), 422, "hash_mismatch")
	if f.rows() != snapshot || f.protected([]uuid.UUID{oldID}) != before {
		t.Fatal("bad manifest changed previous destination/session/parts/events")
	}
	// Stop the real child before releasing the request; a 503 alone permits retry.
	f.cancelCommand()
	f.gate.Close()
	f.finish(done, false)
	pairSource(t, source, data)
	f.download(oldID, old)
	f.rows()
}

func runPairDirMove(t *testing.T, binary string) {
	f := newPair(t, binary)
	sourceParent := f.folder(f.namespace+"_source", nil)
	sub := f.folder("sub", &sourceParent)
	payloads := map[uuid.UUID][]byte{}
	payloads[f.publish(&sourceParent, "a-file", pairPayload(4096))] = pairPayload(4096)
	payloads[f.publish(&sub, "b-file", []byte("nested independent source"))] = []byte("nested independent source")
	old := []byte("independent old destination")
	oldID := f.publish(&f.parent, "old", old)
	sourceBefore := pairRemoteManifest(t, f, sourceParent)
	f.gate = fixture.NewBarrier(t, nil)
	f.storage.SetSendFaults(fixture.SendFaults{AfterCopy: f.gate})
	f.truncate = true
	start := len(f.requests())
	done := f.start(nil, "move", "fixture:"+f.namespace+"_source", f.target(""), "--disable", "Move")
	if err := f.gate.Wait(f.ctx); err != nil {
		t.Fatal("existing target did not force real DirMove ErrorDirExists through March/Copy")
	}
	id, key, admission := f.admission()
	f.loseParent(nil)
	terminal := f.terminal(id, true)
	current, err := f.catalog.Get(f.ctx, f.owner, f.parent)
	if err != nil {
		t.Fatal(err)
	}
	retained := f.namespace + "_retained"
	if _, err = f.catalog.Update(f.ctx, catalog.UpdateInput{UserID: f.owner, FileID: f.parent, ExpectedGeneration: &current.Generation, Name: &retained}); err != nil {
		t.Fatal("real target removal by retained rename failed")
	}
	var originalPath int
	if err = f.pool.QueryRow(f.ctx, "SELECT count(*) FROM teldrive.files WHERE parent_id IS NULL AND name=$1 AND status='active'", f.namespace).Scan(&originalPath); err != nil || originalPath != 0 {
		t.Fatal("fresh shortcut target is not actually absent")
	}
	protected := f.protected([]uuid.UUID{oldID})
	events := f.eventSnapshot()
	afterLoss := len(f.requests())
	f.gate.Release()
	result := f.finish(done, false)
	if !pairSecondPass(result.Output) {
		t.Fatal("actual directory outer command pass absent")
	}
	if pairRemoteManifest(t, f, sourceParent) != sourceBefore || f.protected([]uuid.UUID{oldID}) != protected {
		t.Fatal("DirMove retry changed full source paths/bytes or old destination")
	}
	for file, data := range payloads {
		f.download(file, data)
	}
	f.download(oldID, old)
	next, k, row := f.admission()
	if next != id || k != key || row != admission || f.terminal(id, true) != terminal || f.eventSnapshot() != events {
		t.Fatal("DirMove retry replaced admission/publication/epoch")
	}
	copies := 0
	for i, r := range f.requests()[start:] {
		if strings.HasSuffix(r.Path, "/copy") {
			copies++
			if r.Key != key.String() {
				t.Fatal("DirMove retry rotated copy key")
			}
		}
		if r.Method == "DELETE" || strings.HasSuffix(r.Path, "/move") || r.Method == "PATCH" {
			t.Fatal("retained DirMove emitted rename/move/delete RPC")
		}
		if start+i >= afterLoss && r.Method == "POST" && (r.Path == "/v1/folders" || r.Path == "/v1/uploads") {
			t.Fatal("DirMove retry prepared replacement path")
		}
		if r.Status == 409 && !r.Lost {
			t.Fatal("decoded fatal rescued DirMove fixture")
		}
	}
	if copies != 3 {
		t.Fatal("initial copy pacer ambiguity was not exhausted without replacement")
	}
	f.rows()
}

func pairRemoteManifest(t *testing.T, f *pairFixture, root uuid.UUID) string {
	t.Helper()
	rows, err := f.pool.Query(f.ctx, `WITH RECURSIVE tree AS (
 SELECT id,parent_id,name::text path,kind,size,status FROM teldrive.files WHERE id=$1
 UNION ALL SELECT x.id,x.parent_id,tree.path||'/'||x.name,x.kind,x.size,x.status FROM teldrive.files x JOIN tree ON x.parent_id=tree.id
 ) SELECT id,path,kind::text,coalesce(size,0),status::text FROM tree ORDER BY path`, root)
	if err != nil {
		t.Fatal("full source path manifest query failed")
	}
	type entry struct {
		ID         uuid.UUID
		Path, Kind string
		Size       int64
		Status     string
		SHA256     [32]byte
	}
	var entries []entry
	for rows.Next() {
		var x entry
		if rows.Scan(&x.ID, &x.Path, &x.Kind, &x.Size, &x.Status) != nil {
			t.Fatal("source manifest decode failed")
		}
		entries = append(entries, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal("source manifest scan failed")
	}
	for i := range entries {
		if entries[i].Kind != "file" {
			continue
		}
		r := performRequest(t, f.generated, "GET", "/v1/files/"+entries[i].ID.String()+"/content", nil, map[string]string{"Authorization": "Bearer test-token"})
		if r.Code != 200 || int64(r.Body.Len()) != entries[i].Size {
			t.Fatal("full source manifest download failed")
		}
		entries[i].SHA256 = sha256.Sum256(r.Body.Bytes())
	}
	b, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

type pairLegacyScenario struct {
	UploadID                                 uuid.UUID
	ParentID                                 *uuid.UUID
	FileID                                   *uuid.UUID
	Size                                     int64
	Hash                                     string
	SHA256                                   [32]byte
	Plain                                    []byte
	Encrypted                                bool
	SessionBefore, PartsBefore, SessionAfter json.RawMessage
}
type pairLegacyEvidence struct {
	Version                            int
	RunID, Database                    string
	OID                                int64
	Provenance, Mode                   string
	TemplatePath, TemplateSHA256       string
	FixtureSource, FixtureSourceSHA256 string
	FixtureBinary, FixtureBinarySHA256 string
	MigrationSHA256, Key               string
	Scenarios                          map[string]pairLegacyScenario
	Messages                           []struct {
		UserID, ChannelID, MessageID int64
		Bytes                        []byte
		SHA256                       [32]byte
	}
	Rows              map[string]json.RawMessage
	Triggers          map[string]string
	Roles, Extensions json.RawMessage
	MigrationFiles    []struct {
		Path   string
		Size   int64
		SHA256 string
	}
	Tools  testpostgres.ToolContext
	Config map[string]any
}

func pairUniqueJSON(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] {
				return errors.New("duplicate private JSON key")
			}
			seen[name] = true
		}
		if err := pairUniqueJSON(d); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}
func pairJSON(t *testing.T, path string, out any) []byte {
	t.Helper()
	pairPrivatePath(t, path, false)
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 1<<20 {
		t.Fatal("private paired restore evidence unreadable")
	}
	if pairUniqueJSON(json.NewDecoder(bytes.NewReader(b))) != nil {
		t.Fatal("private paired restore evidence is not unique JSON")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		t.Fatal("private paired restore evidence schema rejected")
	}
	return b
}
func pairCanonical(raw []byte) string {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return "invalid JSON"
	}
	b, _ := json.Marshal(value)
	return string(b)
}
func pairHashFile(t *testing.T, path, expected string) {
	t.Helper()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("canonical modeled fixture path required")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || p == path && !info.Mode().IsRegular() {
			t.Fatal("pinned modeled fixture input missing or symlinked")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	digest, err := hex.DecodeString(expected)
	if err != nil || len(digest) != sha256.Size || strings.ToLower(expected) != expected {
		t.Fatal("pinned modeled fixture digest rejected")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("pinned modeled fixture input unreadable")
	}
	defer file.Close()
	h := sha256.New()
	if _, err = io.Copy(h, file); err != nil || hex.EncodeToString(h.Sum(nil)) != expected {
		t.Fatal("pinned modeled fixture identity changed")
	}
}

func pairModeledProvenance(old pairLegacyEvidence, template testpostgres.ModeledLegacyTemplate, templateBytes []byte, directory, output, run string) error {
	digest := sha256.Sum256(templateBytes)
	if old.Version != 2 || template.Version != 2 || old.Provenance != "modeled_legacy" || template.Provenance != old.Provenance ||
		run == "" || old.RunID != run || template.RunID != run || old.Mode != "all" || template.Mode != old.Mode ||
		old.Key == "" || template.Key != old.Key || template.Output != output ||
		old.TemplateSHA256 != hex.EncodeToString(digest[:]) ||
		old.FixtureSource != template.FixtureSource || old.FixtureSourceSHA256 != template.FixtureSourceSHA256 ||
		old.FixtureBinary != template.FixtureBinary || old.FixtureBinarySHA256 != template.FixtureBinarySHA256 ||
		old.MigrationSHA256 != template.MigrationSHA256 {
		return errors.New("modeled paired template/evidence identity rejected")
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return errors.New("modeled paired directory rejected")
	}
	for _, path := range []string{old.TemplatePath, output} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != directory {
			return errors.New("modeled paired template/output scope rejected")
		}
	}
	if old.TemplatePath == output {
		return errors.New("modeled paired template/output must differ")
	}
	for _, path := range []string{old.FixtureSource, old.FixtureBinary} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("modeled paired fixture path rejected")
		}
	}
	for _, hash := range []string{old.FixtureSourceSHA256, old.FixtureBinarySHA256, old.MigrationSHA256} {
		decoded, err := hex.DecodeString(hash)
		if err != nil || len(decoded) != sha256.Size || strings.ToLower(hash) != hash {
			return errors.New("modeled paired fixture digest rejected")
		}
	}
	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		return errors.New("modeled paired embedded migrations unavailable")
	}
	var manifest strings.Builder
	count := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= "000015" {
			continue
		}
		if count >= len(old.MigrationFiles) || !strings.HasPrefix(entry.Name(), fmt.Sprintf("%06d_", count+1)) {
			return errors.New("modeled paired migration sequence rejected")
		}
		file := old.MigrationFiles[count]
		data, err := migrations.Files.ReadFile(entry.Name())
		hash := sha256.Sum256(data)
		if err != nil || file.Path != entry.Name() || file.Size != int64(len(data)) || file.SHA256 != hex.EncodeToString(hash[:]) {
			return errors.New("modeled paired embedded migration manifest differs")
		}
		manifest.WriteString(file.SHA256 + "  " + file.Path + "\n")
		count++
	}
	digest = sha256.Sum256([]byte(manifest.String()))
	if count != 14 || len(old.MigrationFiles) != count || old.MigrationSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("modeled paired migration aggregate rejected")
	}
	return nil
}

func TestB2PairedModeledProvenance(t *testing.T) {
	directory := filepath.Join(string(filepath.Separator), "modeled-proof")
	output := filepath.Join(directory, "evidence.json")
	template := testpostgres.ModeledLegacyTemplate{
		Version: 2, RunID: "modeled-run", Provenance: "modeled_legacy", Mode: "all", Key: "key",
		Output: output, FixtureSource: filepath.Join(directory, "fixture.go"), FixtureSourceSHA256: strings.Repeat("a", 64),
		FixtureBinary: filepath.Join(directory, "migrations.test"), FixtureBinarySHA256: strings.Repeat("b", 64),
	}
	old := pairLegacyEvidence{
		Version: 2, RunID: template.RunID, Provenance: template.Provenance, Mode: template.Mode, Key: template.Key,
		TemplatePath:  filepath.Join(directory, "template.json"),
		FixtureSource: template.FixtureSource, FixtureSourceSHA256: template.FixtureSourceSHA256,
		FixtureBinary: template.FixtureBinary, FixtureBinarySHA256: template.FixtureBinarySHA256,
	}
	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var manifest strings.Builder
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= "000015" {
			continue
		}
		data, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(data)
		file := struct {
			Path   string
			Size   int64
			SHA256 string
		}{entry.Name(), int64(len(data)), hex.EncodeToString(hash[:])}
		old.MigrationFiles = append(old.MigrationFiles, file)
		manifest.WriteString(file.SHA256 + "  " + file.Path + "\n")
	}
	hash := sha256.Sum256([]byte(manifest.String()))
	old.MigrationSHA256 = hex.EncodeToString(hash[:])
	template.MigrationSHA256 = old.MigrationSHA256
	raw, err := json.Marshal(template)
	if err != nil {
		t.Fatal(err)
	}
	hash = sha256.Sum256(raw)
	old.TemplateSHA256 = hex.EncodeToString(hash[:])
	if err := pairModeledProvenance(old, template, raw, directory, output, template.RunID); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"version", "provenance", "mode", "run", "key", "output", "template_path", "template_bytes", "source_pin", "binary_pin", "malformed_pin", "migration_path", "migration_size", "migration_hash", "migration_count", "migration_aggregate"} {
		t.Run(name, func(t *testing.T) {
			evidence := old
			evidence.MigrationFiles = append(old.MigrationFiles[:0:0], old.MigrationFiles...)
			pinned := template
			data := raw
			switch name {
			case "version":
				evidence.Version = 1
			case "provenance":
				evidence.Provenance = "historical"
			case "mode":
				pinned.Mode = "empty"
			case "run":
				pinned.RunID = "other-run"
			case "key":
				pinned.Key = "other-key"
			case "output":
				pinned.Output = filepath.Join(directory, "other.json")
			case "template_path":
				evidence.TemplatePath = filepath.Join(directory, "outside", "template.json")
			case "template_bytes":
				data = append(append([]byte(nil), raw...), '\n')
			case "source_pin":
				pinned.FixtureSourceSHA256 = strings.Repeat("c", 64)
			case "binary_pin":
				pinned.FixtureBinarySHA256 = strings.Repeat("c", 64)
			case "malformed_pin":
				evidence.FixtureBinarySHA256 = strings.Repeat("B", 64)
				pinned.FixtureBinarySHA256 = evidence.FixtureBinarySHA256
			case "migration_path":
				evidence.MigrationFiles[0].Path = "./" + evidence.MigrationFiles[0].Path
			case "migration_size":
				evidence.MigrationFiles[0].Size++
			case "migration_hash":
				evidence.MigrationFiles[0].SHA256 = strings.Repeat("c", 64)
			case "migration_count":
				evidence.MigrationFiles = evidence.MigrationFiles[:13]
			case "migration_aggregate":
				evidence.MigrationSHA256 = strings.Repeat("c", 64)
				pinned.MigrationSHA256 = evidence.MigrationSHA256
			}
			if err := pairModeledProvenance(evidence, pinned, data, directory, output, template.RunID); err == nil {
				t.Fatal("invalid modeled provenance accepted")
			}
		})
	}
}
func pairLegacyRows(t *testing.T, pool *pgxpool.Pool, withoutReason bool) map[string]json.RawMessage {
	t.Helper()
	out := map[string]json.RawMessage{}
	for _, name := range []string{"users", "channels", "files", "file_parts", "upload_sessions", "upload_parts", "file_shares", "user_events"} {
		expression := "to_jsonb(x)"
		if name == "upload_sessions" && withoutReason {
			expression += "-'terminal_failure_code'"
		}
		var raw []byte
		if err := pool.QueryRow(context.Background(), "SELECT coalesce(jsonb_agg(j ORDER BY j::text),'[]'::jsonb) FROM (SELECT "+expression+" j FROM teldrive."+name+" x) s").Scan(&raw); err != nil {
			t.Fatal("retained genuine legacy row oracle failed")
		}
		out[name] = raw
	}
	return out
}
func pairLegacyEqual(t *testing.T, a, b map[string]json.RawMessage) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatal("legacy row manifest shape changed")
	}
	for name, want := range a {
		if pairCanonical(want) != pairCanonical(b[name]) {
			t.Fatal("genuine legacy rows/parts/events changed")
		}
	}
}

func pairLegacyTriggers(t *testing.T, pool *pgxpool.Pool, retained map[string]string) {
	t.Helper()
	actual := map[string]string{}
	rows, err := pool.Query(context.Background(), `SELECT c.relname||'.'||t.tgname,pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='teldrive' AND NOT t.tgisinternal AND t.tgname NOT IN ('upload_terminal_evidence','remote_write_receipts_evidence','request_admissions_evidence','import_batches_evidence','import_destination_pins_evidence','import_job_attempts_evidence','channel_deletion_intents_evidence') ORDER BY 1`)
	if err != nil {
		t.Fatal("installed trigger snapshot failed")
	}
	defer rows.Close()
	for rows.Next() {
		var key, definition string
		if rows.Scan(&key, &definition) != nil {
			t.Fatal("installed trigger decode failed")
		}
		actual[key] = definition
	}
	if rows.Err() != nil {
		t.Fatal("installed trigger scan failed")
	}
	var body string
	if pool.QueryRow(context.Background(), `SELECT pg_get_functiondef('teldrive.emit_upload_event()'::regprocedure)`).Scan(&body) != nil {
		t.Fatal("installed upload event function missing")
	}
	actual["emit_upload_event"] = body
	if retained == nil || len(retained) != len(actual) {
		t.Fatal("retained trigger manifest differs")
	}
	for key, definition := range actual {
		if retained[key] != definition {
			t.Fatal("retained trigger definition changed")
		}
	}
}

func runPairLegacy(t *testing.T, binary string) {
	if os.Getenv("B2_ISOLATION_APPROVAL") == "" || os.Getenv("B2_PAIRED_RESTORE_INPUT") == "" {
		t.Skip("private B2 paired restore authority not supplied; paired legacy proof NOT-RUN")
	}
	approvalPath, inputPath, rawURL := os.Getenv("B2_ISOLATION_APPROVAL"), os.Getenv("B2_PAIRED_RESTORE_INPUT"), os.Getenv("TEST_DATABASE_URL")
	snapshotPath, evidencePath := os.Getenv("B2_PAIRED_LEGACY_SNAPSHOT"), os.Getenv("B2_PAIRED_LEGACY_EVIDENCE")
	if inputPath == "" || snapshotPath == "" || evidencePath == "" {
		t.Fatal("exact root_restore_handoff inputs required; paired legacy proof NOT-RUN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	approval, err := testpostgres.LoadApproval(approvalPath)
	if err != nil {
		t.Fatal("fresh paired legacy approval missing")
	}
	input, err := testpostgres.LoadRestoreInput(inputPath, *approval, rawURL, time.Now().UTC())
	if err != nil || len(input.TargetDatabases) != 1 {
		t.Fatal("exact distinct paired restore target rejected")
	}
	var snapshot testpostgres.Snapshot
	pairJSON(t, snapshotPath, &snapshot)
	var old pairLegacyEvidence
	pairJSON(t, evidencePath, &old)
	if snapshot.Version != 1 || snapshot.MigrationVersion != 14 || snapshot.SourceDatabase != input.SourceDatabase || snapshot.SourceOID != old.OID || old.OID <= 0 || snapshot.RunID != old.RunID || old.RunID != approval.RunID || old.Database != input.SourceDatabase || old.Tools != approval.Tools || snapshot.Tools != old.Tools {
		t.Fatal("modeled G11 source/dump/evidence identity mismatch")
	}
	if old.Config["schema"] != "teldrive" || old.Config["storage"] != "synthetic" || old.Config["workers_disabled"] != true || old.Config["content_key_version"] != float64(1) {
		t.Fatal("modeled source/config manifest missing")
	}
	var template testpostgres.ModeledLegacyTemplate
	templateBytes := pairJSON(t, old.TemplatePath, &template)
	if err := pairModeledProvenance(old, template, templateBytes, input.Directory, evidencePath, approval.RunID); err != nil {
		t.Fatal(err)
	}
	_, fixtureSource, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("modeled fixture source identity unavailable")
	}
	fixtureSource, err = filepath.Abs(filepath.Join(filepath.Dir(fixtureSource), "../../db/migrations/lifecycle_restore_integration_test.go"))
	if err != nil || old.FixtureSource != fixtureSource {
		t.Fatal("modeled fixture source identity mismatch")
	}
	pairHashFile(t, old.FixtureSource, old.FixtureSourceSHA256)
	// The retained migrations test artifact seeded the source, not this API executable.
	pairHashFile(t, old.FixtureBinary, old.FixtureBinarySHA256)
	root, err := testpostgres.OpenRestoreFixture(ctx, approvalPath, inputPath, rawURL)
	if err != nil {
		t.Fatal("observed paired restore tool scope rejected")
	}
	defer root.Close()
	db, err := root.CloneSnapshot(ctx, snapshotPath, input.TargetDatabases[0])
	if err != nil {
		t.Fatal("actual retained G11 dump clone failed")
	}
	t.Cleanup(func() {
		db.Close()
		if t.Failed() {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := db.Drop(cleanup); err != nil {
			t.Error("successful paired target guarded cleanup failed")
		}
	})
	pairLegacyEqual(t, old.Rows, pairLegacyRows(t, db.Pool, false))
	pairLegacyTriggers(t, db.Pool, old.Triggers)
	if err = db.Mutate(ctx, func(ctx context.Context, _ *pgxpool.Pool) error {
		return database.Migrate(ctx, database.Config{URL: db.URL, Schema: "teldrive"})
	}); err != nil {
		t.Fatal("actual guarded candidate migration failed")
	}
	pairLegacyEqual(t, old.Rows, pairLegacyRows(t, db.Pool, true))
	pairLegacyTriggers(t, db.Pool, old.Triggers)
	scenario, ok := old.Scenarios["detached_cli_empty"]
	if !ok || scenario.UploadID == uuid.Nil || scenario.ParentID == nil || scenario.Size != 0 || len(scenario.Plain) != 0 || scenario.SHA256 != sha256.Sum256(nil) || len(scenario.Hash) != 64 {
		t.Fatal("modeled detached_cli_empty source/valid original manifest absent")
	}
	var beforeSession, afterSession map[string]any
	if json.Unmarshal(scenario.SessionBefore, &beforeSession) != nil || json.Unmarshal(scenario.SessionAfter, &afterSession) != nil || beforeSession["parent_id"] != scenario.ParentID.String() || afterSession["parent_id"] != nil || afterSession["state"] != "open" {
		t.Fatal("modeled ambiguous-parent transition snapshots missing")
	}
	if algorithm, present := beforeSession["expected_hash_algorithm"]; !present || algorithm != nil {
		t.Fatal("old CLI creation expected hash algorithm was not NULL")
	}
	if value, present := beforeSession["expected_hash_value"]; !present || value != nil {
		t.Fatal("old CLI creation expected hash value was not NULL")
	}
	if beforeSession["mime_type"] != "application/octet-stream" {
		t.Fatal("old CLI creation explicit MIME missing")
	}
	if _, present := afterSession["parent_id"]; !present {
		t.Fatal("old detach omitted NULL parent")
	}
	beforeUpdatedText, beforeUpdatedOK := beforeSession["updated_at"].(string)
	afterUpdatedText, afterUpdatedOK := afterSession["updated_at"].(string)
	beforeUpdated, beforeUpdatedErr := time.Parse(time.RFC3339Nano, beforeUpdatedText)
	afterUpdated, afterUpdatedErr := time.Parse(time.RFC3339Nano, afterUpdatedText)
	if !beforeUpdatedOK || !afterUpdatedOK || beforeUpdatedErr != nil || afterUpdatedErr != nil || beforeUpdated.IsZero() || afterUpdated.IsZero() || afterUpdated.Before(beforeUpdated) {
		t.Fatal("old detach updated_at was invalid, zero, or moved backward")
	}
	delete(beforeSession, "parent_id")
	delete(afterSession, "parent_id")
	delete(beforeSession, "updated_at")
	delete(afterSession, "updated_at")
	a, _ := json.Marshal(beforeSession)
	b, _ := json.Marshal(afterSession)
	if !bytes.Equal(a, b) {
		t.Fatal("old detach changed more than original parent and updated_at")
	}
	var session struct {
		Name         string    `json:"name"`
		ModTime      time.Time `json:"mod_time"`
		ExpectedSize int64     `json:"expected_size"`
	}
	if json.Unmarshal(scenario.SessionAfter, &session) != nil || session.Name != "detached_empty" || session.ExpectedSize != 0 || session.ModTime.IsZero() {
		t.Fatal("original source name/size/modtime manifest absent")
	}
	remote := &pairStorage{Storage: fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1, 2}}, {ChannelID: 9002, Actors: []int64{1, 2}}})}
	for _, message := range old.Messages {
		if sha256.Sum256(message.Bytes) != message.SHA256 {
			t.Fatal("old independent message SHA-256 differs")
		}
		if err = remote.Seed(fixture.Message{UserID: message.UserID, ChannelID: message.ChannelID, MessageID: message.MessageID, Bytes: message.Bytes, Preserve: true}); err != nil {
			t.Fatal(err)
		}
	}
	f := composePair(t, ctx, binary, db.Pool, 1, remote)
	source := f.source(session.Name, nil)
	if err = os.Chtimes(source, session.ModTime, session.ModTime); err != nil {
		t.Fatal(err)
	}
	var exact string
	if err = db.Pool.QueryRow(ctx, `SELECT to_jsonb(s)::text FROM teldrive.upload_sessions s WHERE id=$1 AND parent_id IS NULL AND state='open' AND terminal_failure_code='upload_destination_unknown'`, scenario.UploadID).Scan(&exact); err != nil {
		t.Fatal("restored original unknown row missing")
	}
	// Independently submit the valid modeled pre-candidate empty manifest.
	headers := map[string]string{"Authorization": "Bearer test-token", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
	originalScenario, ok := old.Scenarios["detached_empty"]
	if !ok || originalScenario.UploadID == uuid.Nil || len(originalScenario.Hash) != 64 {
		t.Fatal("original hashful G11 control absent")
	}
	original := []byte(fmt.Sprintf(`{"size":0,"hash":{"algorithm":"blake3","value":%q}}`, originalScenario.Hash))
	unchanged := f.rows()
	r := performRequest(t, f.generated, "POST", "/v1/uploads/"+originalScenario.UploadID.String()+"/complete", original, headers)
	assertParentUnavailable(t, r.Code, r.Body.Bytes())
	if f.rows() != unchanged {
		t.Fatal("original legacy manifest changed restored evidence")
	}
	target := old.Scenarios["root_replacement_target"]
	if target.FileID == nil {
		t.Fatal("independent same-name old root target absent")
	}
	f.rawFile(*target.FileID, nil)
	for _, truncated := range []bool{false, true} {
		name := "decoded"
		if truncated {
			name = "truncated"
		}
		t.Run(name, func(t *testing.T) {
			f.t = t
			f.mu.Lock()
			f.truncate = truncated
			f.mu.Unlock()
			start := len(f.requests())
			result := f.finish(f.start(nil, "moveto", source, "fixture:"+session.Name, "--ignore-times"), false)
			pairSource(t, source, nil)
			if f.rows() != unchanged {
				t.Fatal("legacy automatic consumer created/replaced/published or changed exact session/share/events")
			}
			var current string
			if err := db.Pool.QueryRow(ctx, "SELECT to_jsonb(s)::text FROM teldrive.upload_sessions s WHERE id=$1", scenario.UploadID).Scan(&current); err != nil || current != exact {
				t.Fatal("legacy consumer changed original unknown session")
			}
			gets := 0
			for _, request := range f.requests()[start:] {
				if request.Method == "POST" || request.Method == "PUT" || request.Method == "PATCH" || request.Method == "DELETE" {
					t.Fatal("legacy automatic attempt mutated instead of retaining exact candidate")
				}
				if request.Path == "/v1/uploads/"+scenario.UploadID.String() {
					gets++
					if request.Status != 409 || truncated && !request.Lost {
						t.Fatal("legacy exact GET did not retain unavailable outcome")
					}
				}
			}
			if gets == 0 {
				t.Fatal("ordinary automatic resume filtered genuine old candidate before exact GET")
			}
			if truncated && (gets < 6 || !pairSecondPass(result.Output)) {
				t.Fatal("truncated legacy exact-selection retry did not reach real outer pass")
			}
			if !truncated && pairSecondPass(result.Output) {
				t.Fatal("decoded legacy terminal response did not stop command")
			}
			if err := remote.CheckPreserved(); err != nil {
				t.Fatal(err)
			}
		})
	}
	f.t = t
}

func (f *pairFixture) setTime(id uuid.UUID, modTime time.Time) {
	f.t.Helper()
	row, err := f.catalog.Get(f.ctx, f.owner, id)
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err = f.catalog.Update(f.ctx, catalog.UpdateInput{UserID: f.owner, FileID: id, ExpectedGeneration: &row.Generation, ModTime: &modTime}); err != nil {
		f.t.Fatal("fixture metadata equality boundary failed")
	}
}

func (f *pairFixture) denials(id uuid.UUID, token string, size int) {
	f.t.Helper()
	before := f.rows()
	for _, headers := range []map[string]string{nil, {"Authorization": "Bearer foreign-token"}} {
		r := performRequest(f.t, f.generated, "GET", "/v1/uploads/"+id.String(), nil, headers)
		if r.Code != 401 && r.Code != 403 && r.Code != 404 {
			f.t.Fatal("generated owner boundary did not deny unrelated actor")
		}
		if bytes.Contains(r.Body.Bytes(), []byte("upload_parent_unavailable")) {
			f.t.Fatal("terminal destination disclosed to unrelated actor")
		}
	}
	headers := map[string]string{"Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
	r := performRequest(f.t, f.generated, "POST", "/v1/public/shares/"+token+"/uploads/"+id.String()+"/complete", []byte(fmt.Sprintf(`{"size":%d}`, size)), headers)
	if r.Code != 403 && r.Code != 404 && r.Code != 410 {
		f.t.Fatal("real read-only public share gained terminal completion authority")
	}
	if bytes.Contains(r.Body.Bytes(), []byte("upload_parent_unavailable")) || f.rows() != before {
		f.t.Fatal("public denial disclosed or changed terminal evidence")
	}
}

// Reuse the allocation fixture's real pgx tracer/socket seam: consume the
// server's COMMIT response, prove durability independently, then withhold ACK.
type pairACKWire struct {
	ctx               context.Context
	mu                sync.Mutex
	completing        map[uint32]bool
	used              bool
	received, release chan struct{}
}
type pairACKSocket struct {
	net.Conn
	wire  *pairACKWire
	armed atomic.Bool
}

func (s *pairACKSocket) Read(p []byte) (int, error) {
	n, err := s.Conn.Read(p)
	if n > 0 && s.armed.Swap(false) {
		close(s.wire.received)
		select {
		case <-s.wire.release:
		case <-s.wire.ctx.Done():
		}
		_ = s.Conn.Close()
		return 0, fixture.ErrResponseLost
	}
	return n, err
}
func (w *pairACKWire) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	pid := conn.PgConn().PID()
	if strings.Contains(data.SQL, "-- name: CompleteUploadSession ") {
		w.completing[pid] = true
	}
	if strings.EqualFold(strings.TrimSpace(data.SQL), "commit") && w.completing[pid] && !w.used {
		w.used = true
		conn.PgConn().Conn().(*pairACKSocket).armed.Store(true)
	}
	return ctx
}
func (*pairACKWire) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func runPairCommitACK(t *testing.T, binary string) {
	base := newPair(t, binary)
	wire := &pairACKWire{ctx: base.ctx, completing: map[uint32]bool{}, received: make(chan struct{}), release: make(chan struct{})}
	config := base.pool.Config()
	config.ConnConfig.Tracer = wire
	config.ConnConfig.AfterNetConnect = func(_ context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
		return &pairACKSocket{Conn: conn, wire: wire}, nil
	}
	pool, err := pgxpool.NewWithConfig(base.ctx, config)
	if err != nil {
		t.Fatal("approved publication ACK fault pool failed")
	}
	t.Cleanup(func() {
		select {
		case <-wire.release:
		default:
			close(wire.release)
		}
		pool.Close()
	})
	f := composePair(t, base.ctx, binary, pool, base.owner, base.storage)
	f.parent = base.parent
	f.namespace = base.namespace
	data := pairPayload(4096)
	source := f.source("source", data)
	f.gate = fixture.NewBarrier(t, nil)
	f.gateMode = "complete"
	done := f.start(nil, "moveto", source, f.target("z-target"), "--ignore-times")
	if err = f.gate.Wait(f.ctx); err != nil {
		t.Fatal("actual candidate did not reach pre-COMMIT boundary")
	}
	id, key, admission := f.admission()
	f.gate.Release()
	select {
	case <-wire.received:
	case <-f.ctx.Done():
		t.Fatal("actual PostgreSQL COMMIT response was not intercepted")
	}
	var result uuid.UUID
	if err = base.pool.QueryRow(f.ctx, "SELECT file_id FROM teldrive.upload_sessions WHERE id=$1 AND state='completed'", id).Scan(&result); err != nil {
		t.Fatal("independent connection cannot prove actual committed winner")
	}
	base.download(result, data)
	before := base.rows()
	// Close only this fixture's service pool. Its blocked COMMIT still owns a
	// connection; Acquire establishes closing before the ACK is released.
	closed := make(chan struct{})
	go func() { pool.Close(); close(closed) }()
	for {
		if f.ctx.Err() != nil {
			t.Fatal("bounded actual unavailable-readback barrier failed")
		}
		conn, acquireErr := pool.Acquire(f.ctx)
		if acquireErr != nil {
			break
		}
		conn.Release()
	}
	close(wire.release)
	f.finish(done, false)
	select {
	case <-closed:
	case <-f.ctx.Done():
		t.Fatal("ACK fault pool did not close")
	}
	pairSource(t, source, data)
	base.download(result, data)
	if base.rows() != before || len(f.storage.Deletes()) != 0 {
		t.Fatal("unavailable SQL COMMIT outcome compensated or changed committed winner")
	}
	var row string
	if err = base.pool.QueryRow(f.ctx, "SELECT to_jsonb(a)::text FROM teldrive.request_admissions a WHERE upload_id=$1 AND request_key=$2", id, key).Scan(&row); err != nil || row != admission {
		t.Fatal("ambiguous SQL acknowledgement replaced original admission")
	}
}

func pairSecondPass(output []byte) bool {
	// cmd.Run logs Attempt1 only after f() finishes and chooses another pass.
	// A nonretryable second f() can stop before its own Attempt2 marker.
	first := bytes.Index(output, []byte("Attempt 1/3 failed"))
	if first < 0 {
		return false
	}
	tail := output[first+len("Attempt 1/3 failed"):]
	return bytes.Contains(tail, []byte("Attempt 2/3")) ||
		bytes.Contains(tail, []byte("Can't retry any of the errors - not attempting retries")) ||
		bytes.Contains(tail, []byte("Fatal error received - not attempting retries"))
}

func testPairObservationHelpers(t *testing.T) {
	for _, row := range []struct {
		output string
		second bool
	}{
		{"Fatal error received - not attempting retries", false},
		{"Can't retry any of the errors - not attempting retries", false},
		{"Attempt 1/3 failed\nAttempt 2/3 succeeded", true},
		{"Attempt 1/3 failed\nCan't retry any of the errors - not attempting retries", true},
		{"Attempt 1/3 failed\nFatal error received - not attempting retries", true},
		{"Attempt 1/3 failed", false},
	} {
		if pairSecondPass([]byte(row.output)) != row.second {
			t.Fatal("actual cmd.Run observation shape misclassified")
		}
	}
	for _, row := range []struct {
		body  string
		valid bool
	}{
		{`{"a":[{"b":1},{"b":2}]}`, true},
		{`{"a":1,"a":2}`, false},
		{`{"a":[{"b":1,"b":2}]}`, false},
		{`{"a":[`, false},
	} {
		if (pairUniqueJSON(json.NewDecoder(strings.NewReader(row.body))) == nil) != row.valid {
			t.Fatal("private evidence duplicate/malformed JSON accepted")
		}
	}
}
