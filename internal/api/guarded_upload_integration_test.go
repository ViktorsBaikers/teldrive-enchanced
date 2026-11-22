//go:build integration

package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"

	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestGuardedUploadHTTP(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, `
INSERT INTO users (user_id) VALUES (1001), (1002);
INSERT INTO channels (channel_id, user_id, name, selected) VALUES (9001, 1001, 'storage', true)`); err != nil {
		t.Fatal(err)
	}

	catalogService := catalog.NewService(db.Pool, nil)
	uploadService := uploads.NewService(db.Pool)
	storage := &guardedHTTPStorage{inner: &apiMemoryStorage{}}
	receipts := bindAPIReceiptPolicy(t, db.Pool, storage, catalogService, uploadService)
	pipeline := transfer.NewPipeline(uploadService, apiFixedResolver(9001), storage, nil, transfer.Config{Receipts: receipts})
	downloader := transfer.NewDownloader(catalogService, storage, nil)
	handler := api.NewHandler(catalogService, uploadService, pipeline, downloader, health.NewService("test", db.Pool), 0, nil)
	server, err := api.NewServer(handler, api.NewSecurity(guardedHTTPAuthenticator{}))
	if err != nil {
		t.Fatal(err)
	}

	discovery := performRequest(t, server, http.MethodGet, "/v1/uploads/guarded", nil, guardedHTTPHeaders("owner-token", nil))
	if discovery.Code != http.StatusOK {
		t.Fatalf("discovery status = %d, body=%s", discovery.Code, discovery.Body.String())
	}
	incarnation := discovery.Header().Get("X-TelDrive-Recovery-Incarnation")
	if _, err := uuid.Parse(incarnation); err != nil {
		t.Fatalf("discovery incarnation = %q", incarnation)
	}
	if discovery.Body.String() != "{}" {
		t.Fatalf("discovery body = %s", discovery.Body.String())
	}

	source := bytes.Repeat([]byte("guarded-http"), 512)
	name := uuid.NewString() + ".bin"
	session := guardedHTTPCreate(t, server, "owner-token", name, int64(len(source)), incarnation)
	uploadID := uuid.UUID(session.ID).String()

	part := performRequest(t, server, http.MethodPut, "/v1/uploads/guarded/"+uploadID+"/parts/1", source, guardedHTTPHeaders("owner-token", map[string]string{
		"Content-Type":                    "application/octet-stream",
		"X-TelDrive-Recovery-Incarnation": incarnation,
	}))
	if part.Code != http.StatusCreated {
		t.Fatalf("guarded part status = %d, body=%s", part.Code, part.Body.String())
	}

	digest := guardedHTTPDigest(source)
	completeHeaders := guardedHTTPHeaders("owner-token", map[string]string{
		"Content-Type":                    "application/json",
		"Idempotency-Key":                 uuid.NewString(),
		"X-TelDrive-Recovery-Incarnation": incarnation,
	})
	completeBody := []byte(fmt.Sprintf(`{"size":%d,"hash":{"algorithm":"blake3","value":%q}}`, len(source), digest))
	completed := performRequest(t, server, http.MethodPost, "/v1/uploads/guarded/"+uploadID+"/complete", completeBody, completeHeaders)
	if completed.Code != http.StatusOK {
		t.Fatalf("guarded complete status = %d, body=%s", completed.Code, completed.Body.String())
	}
	var file gen.GuardedFileEntry
	if err := json.Unmarshal(completed.Body.Bytes(), &file); err != nil {
		t.Fatal(err)
	}
	fileID := uuid.UUID(file.ID)
	if file.Name != name || file.Generation != 1 || file.ParentSegments == nil || len(file.ParentSegments) != 0 {
		t.Fatalf("guarded result = %#v", file)
	}
	if completed.Header().Get("Etag") != `"1"` || completed.Header().Get("Location") != "/files/"+fileID.String() {
		t.Fatalf("complete headers = Etag %q Location %q", completed.Header().Get("Etag"), completed.Header().Get("Location"))
	}

	statusRes := performRequest(t, server, http.MethodGet, "/v1/uploads/guarded/"+uploadID, nil, guardedHTTPHeaders("owner-token", map[string]string{
		"X-TelDrive-Recovery-Incarnation": incarnation,
	}))
	if statusRes.Code != http.StatusOK {
		t.Fatalf("guarded status = %d, body=%s", statusRes.Code, statusRes.Body.String())
	}
	if got := statusRes.Header().Get("X-TelDrive-Upload-Guard"); got != "1" {
		t.Fatalf("guarded status marker = %q", got)
	}
	var status gen.GuardedUploadSession
	if err := json.Unmarshal(statusRes.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	receipt, ok := status.CompletedReceipt.Get()
	if !ok || receipt.UploadId != status.ID || uuid.UUID(receipt.Result.ID) != fileID || receipt.Size != int64(len(source)) || string(receipt.Hash.Value) != digest {
		t.Fatalf("guarded receipt = %#v", status.CompletedReceipt)
	}
	if receipt.Result.ParentSegments == nil || len(receipt.Result.ParentSegments) != 0 {
		t.Fatalf("receipt segments = %#v", receipt.Result.ParentSegments)
	}
	// The receipt names the guarded create admission: that is the only key a
	// crashed client still holds. The completing request's own admission stays
	// publication evidence in the result row, never durable client state.
	var createKey, resultKey string
	if err := db.Pool.QueryRow(ctx, `SELECT b.request_key::text, a.request_key::text
FROM guarded_upload_bindings b
JOIN guarded_upload_results r ON r.upload_id = b.upload_id
JOIN request_admissions a ON a.id = r.request_admission_id
WHERE b.upload_id = $1`, uploadID).Scan(&createKey, &resultKey); err != nil {
		t.Fatal(err)
	}
	if createKey == resultKey || receipt.AdmissionKey != createKey {
		t.Fatalf("receipt admissionKey = %q, create = %q, result = %q", receipt.AdmissionKey, createKey, resultKey)
	}

	download := performRequest(t, server, http.MethodGet, "/v1/files/"+fileID.String()+"/content/"+name+"?download=1", nil, map[string]string{"Authorization": "Bearer owner-token"})
	if download.Code != http.StatusOK || !bytes.Equal(download.Body.Bytes(), source) {
		t.Fatalf("independent download = %d, %d bytes", download.Code, download.Body.Len())
	}
	if got := guardedHTTPSHA256(download.Body.Bytes()); got != guardedHTTPSHA256(source) {
		t.Fatalf("download SHA-256 = %s", got)
	}

	// The same complete acknowledgement is replayable without a second publication.
	replayed := performRequest(t, server, http.MethodPost, "/v1/uploads/guarded/"+uploadID+"/complete", completeBody, completeHeaders)
	if replayed.Code != http.StatusOK {
		t.Fatalf("complete replay = %d, body=%s", replayed.Code, replayed.Body.String())
	}
	var replayedFile gen.GuardedFileEntry
	if err := json.Unmarshal(replayed.Body.Bytes(), &replayedFile); err != nil || uuid.UUID(replayedFile.ID) != fileID {
		t.Fatalf("complete replay file = %#v, %v", replayedFile, err)
	}
	var published int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE user_id=1001 AND parent_id IS NULL AND name=$1 AND status='active'", name).Scan(&published); err != nil || published != 1 {
		t.Fatalf("published files = %d, %v", published, err)
	}
	changedBody := []byte(fmt.Sprintf(`{"size":%d,"hash":{"algorithm":"blake3","value":%q}}`, len(source), guardedHTTPDigest([]byte("other"))))
	changed := performRequest(t, server, http.MethodPost, "/v1/uploads/guarded/"+uploadID+"/complete", changedBody, completeHeaders)
	if changed.Code != http.StatusConflict {
		t.Fatalf("changed complete replay = %d, body=%s", changed.Code, changed.Body.String())
	}

	// A legacy part route cannot mutate a guarded session or read its body.
	open := guardedHTTPCreate(t, server, "owner-token", uuid.NewString()+".bin", int64(len(source)), incarnation)
	openID := uuid.UUID(open.ID).String()
	before := storage.uploads()
	legacyBody := &guardedHTTPCountingReader{data: source}
	legacy := guardedHTTPRequest(server, http.MethodPut, "/v1/uploads/"+openID+"/parts/1", legacyBody, guardedHTTPHeaders("owner-token", map[string]string{
		"Content-Type": "application/octet-stream",
	}))
	if legacy.Code != http.StatusConflict || legacyBody.read != 0 || storage.uploads() != before {
		t.Fatalf("legacy guarded mutation = %d, body=%s, body bytes %d, storage %d->%d", legacy.Code, legacy.Body.String(), legacyBody.read, before, storage.uploads())
	}

	// A foreign actor cannot read or mutate the guarded session.
	foreignBody := &guardedHTTPCountingReader{data: source}
	foreign := guardedHTTPRequest(server, http.MethodPut, "/v1/uploads/guarded/"+openID+"/parts/1", foreignBody, guardedHTTPHeaders("other-token", map[string]string{
		"Content-Type":                    "application/octet-stream",
		"X-TelDrive-Recovery-Incarnation": incarnation,
	}))
	if foreign.Code != http.StatusForbidden || foreignBody.read != 0 || storage.uploads() != before {
		t.Fatalf("foreign guarded mutation = %d, body bytes %d, storage %d->%d", foreign.Code, foreignBody.read, before, storage.uploads())
	}
	foreignStatus := performRequest(t, server, http.MethodGet, "/v1/uploads/guarded/"+openID, nil, guardedHTTPHeaders("other-token", map[string]string{
		"X-TelDrive-Recovery-Incarnation": incarnation,
	}))
	if foreignStatus.Code != http.StatusForbidden {
		t.Fatalf("foreign guarded status = %d, body=%s", foreignStatus.Code, foreignStatus.Body.String())
	}

	// A changed external destination refuses before body I/O and leaves its bytes untouched.
	conflictName := uuid.NewString() + ".bin"
	conflicted := guardedHTTPCreate(t, server, "owner-token", conflictName, int64(len(source)), incarnation)
	conflictedID := uuid.UUID(conflicted.ID).String()
	externalID := uuid.New()
	if _, err := db.Pool.Exec(ctx, `INSERT INTO files (id,user_id,parent_id,name,kind,size,status,mod_time)
VALUES ($1,1001,NULL,$2,'file',$3,'active',now())`, externalID, conflictName, int64(len(source))); err != nil {
		t.Fatal(err)
	}
	before = storage.uploads()
	conflictBody := &guardedHTTPCountingReader{data: source}
	conflict := guardedHTTPRequest(server, http.MethodPut, "/v1/uploads/guarded/"+conflictedID+"/parts/1", conflictBody, guardedHTTPHeaders("owner-token", map[string]string{
		"Content-Type":                    "application/octet-stream",
		"X-TelDrive-Recovery-Incarnation": incarnation,
	}))
	if conflict.Code != http.StatusPreconditionFailed || conflictBody.read != 0 || storage.uploads() != before {
		t.Fatalf("conflicted part = %d, body bytes %d, storage %d->%d", conflict.Code, conflictBody.read, before, storage.uploads())
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(conflict.Body.Bytes(), &envelope); err != nil || envelope.Error.Code != "upload_destination_changed" {
		t.Fatalf("conflict envelope = %s, %v", conflict.Body.String(), err)
	}
	var externalSize int64
	var externalStatus string
	if err := db.Pool.QueryRow(ctx, "SELECT size,status FROM files WHERE id=$1", externalID).Scan(&externalSize, &externalStatus); err != nil || externalSize != int64(len(source)) || externalStatus != "active" {
		t.Fatalf("external file = %d %q, %v", externalSize, externalStatus, err)
	}

	// Ordinary unguarded upload keeps its published contract.
	plainName := uuid.NewString() + ".bin"
	plain := performRequest(t, server, http.MethodPost, "/v1/uploads", []byte(fmt.Sprintf(`{"name":%q,"size":%d,"modTime":"2026-07-01T00:00:00Z","preferredPartSize":16777216}`, plainName, len(source))), guardedHTTPHeaders("owner-token", map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": uuid.NewString(),
	}))
	if plain.Code != http.StatusCreated {
		t.Fatalf("ordinary create = %d, body=%s", plain.Code, plain.Body.String())
	}
	var plainSession gen.UploadSession
	if err := json.Unmarshal(plain.Body.Bytes(), &plainSession); err != nil {
		t.Fatal(err)
	}
	plainID := uuid.UUID(plainSession.ID).String()
	plainPart := performRequest(t, server, http.MethodPut, "/v1/uploads/"+plainID+"/parts/1", source, map[string]string{
		"Authorization": "Bearer owner-token", "Content-Type": "application/octet-stream",
	})
	if plainPart.Code != http.StatusCreated {
		t.Fatalf("ordinary part = %d, body=%s", plainPart.Code, plainPart.Body.String())
	}
	plainComplete := performRequest(t, server, http.MethodPost, "/v1/uploads/"+plainID+"/complete", []byte(fmt.Sprintf(`{"size":%d}`, len(source))), map[string]string{
		"Authorization": "Bearer owner-token", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString(),
	})
	if plainComplete.Code != http.StatusCreated {
		t.Fatalf("ordinary complete = %d, body=%s", plainComplete.Code, plainComplete.Body.String())
	}
}

func guardedHTTPCreate(t *testing.T, server http.Handler, token, name string, size int64, incarnation string) gen.UploadSession {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"destination":{"mountRoot":null,"parentId":null,"expected":{"kind":"absent"}},"name":%q,"size":%d,"modTime":"2026-07-01T00:00:00Z","preferredPartSize":16777216}`, name, size))
	response := performRequest(t, server, http.MethodPost, "/v1/uploads/guarded", body, guardedHTTPHeaders(token, map[string]string{
		"Content-Type":                    "application/json",
		"Idempotency-Key":                 uuid.NewString(),
		"X-TelDrive-Recovery-Incarnation": incarnation,
	}))
	if response.Code != http.StatusCreated {
		t.Fatalf("guarded create = %d, body=%s", response.Code, response.Body.String())
	}
	var session gen.UploadSession
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	return session
}

func guardedHTTPHeaders(token string, extra map[string]string) map[string]string {
	headers := map[string]string{"Authorization": "Bearer " + token}
	for key, value := range extra {
		headers[key] = value
	}
	return headers
}

func guardedHTTPRequest(handler http.Handler, method, path string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, body)
	if reader, ok := body.(*guardedHTTPCountingReader); ok {
		request.ContentLength = int64(len(reader.data))
		request.Header.Set("Content-Length", fmt.Sprint(len(reader.data)))
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func guardedHTTPDigest(data []byte) string {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(data)
	return treehash.SumToHex(treehash.ComputeTreeHash(hasher.Sum()))
}

func guardedHTTPSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type guardedHTTPAuthenticator struct{}

func (guardedHTTPAuthenticator) AuthenticateBearer(_ context.Context, token string) (api.Identity, error) {
	return guardedHTTPIdentity(token)
}

func (guardedHTTPAuthenticator) AuthenticateAPIKey(_ context.Context, token string) (api.Identity, error) {
	return guardedHTTPIdentity(token)
}

func guardedHTTPIdentity(token string) (api.Identity, error) {
	switch token {
	case "owner-token":
		return api.Identity{UserID: 1001, Roles: []string{"user"}}, nil
	case "other-token":
		return api.Identity{UserID: 1002, Roles: []string{"user"}}, nil
	default:
		return api.Identity{}, api.ErrUnauthenticated
	}
}

type guardedHTTPStorage struct {
	inner *apiMemoryStorage
	mu    sync.Mutex
	count int
}

func (s *guardedHTTPStorage) Upload(ctx context.Context, request telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
	return s.inner.Upload(ctx, request)
}

func (s *guardedHTTPStorage) uploads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *guardedHTTPStorage) OpenRange(ctx context.Context, request telegramstore.RangeRequest) (io.ReadCloser, error) {
	return s.inner.OpenRange(ctx, request)
}

func (s *guardedHTTPStorage) DeleteMessages(ctx context.Context, userID, channelID int64, ids []int64) error {
	return s.inner.DeleteMessages(ctx, userID, channelID, ids)
}

func (s *guardedHTTPStorage) CopyPart(ctx context.Context, userID, sourceChannelID, sourceMessageID, destinationChannelID int64) (telegramstore.StoredPart, error) {
	return s.inner.CopyPart(ctx, userID, sourceChannelID, sourceMessageID, destinationChannelID)
}

func (s *guardedHTTPStorage) CreateChannel(ctx context.Context, userID int64, name string) (telegramstore.Channel, error) {
	return s.inner.CreateChannel(ctx, userID, name)
}

func (s *guardedHTTPStorage) DeleteChannel(ctx context.Context, userID, channelID int64) error {
	return s.inner.DeleteChannel(ctx, userID, channelID)
}

type guardedHTTPCountingReader struct {
	data []byte
	read int
}

func (r *guardedHTTPCountingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.read += n
	r.data = r.data[n:]
	return n, nil
}
