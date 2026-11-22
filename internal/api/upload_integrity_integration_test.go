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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zeebo/blake3"

	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/authn"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/shares"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestB1UploadContract(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, `INSERT INTO users (user_id) VALUES (1001),(1002),(1003); INSERT INTO channels (channel_id,user_id,name,selected) VALUES (9001,1001,'storage',true)`); err != nil {
		t.Fatal(err)
	}
	catalogService := catalog.NewService(db.Pool, nil)
	uploadService := uploads.NewService(db.Pool)
	storage := &apiMemoryStorage{}
	receipts := bindAPIReceiptPolicy(t, db.Pool, storage, catalogService, uploadService)
	shareService, err := shares.NewService(db.Pool, catalogService)
	if err != nil {
		t.Fatal(err)
	}
	folder, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "shared", ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	folderID, ok := dbtypes.GoogleUUID(folder.ID)
	if !ok {
		t.Fatal("invalid folder UUID")
	}
	privateFolder, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "private", ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	privateFolderID := uuid.UUID(privateFolder.ID.Bytes)
	password := "fixture-password"
	editable, err := shareService.Create(ctx, shares.CreateInput{OwnerID: 1001, FileID: folderID, Password: &password, Permission: sqlcgen.SharePermissionEdit})
	if err != nil {
		t.Fatal(err)
	}
	readOnly, err := shareService.Create(ctx, shares.CreateInput{OwnerID: 1001, FileID: folderID, Permission: sqlcgen.SharePermissionRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shareService.CreateGrant(ctx, shares.GrantCreateInput{OwnerID: 1001, FileID: folderID, GranteeID: 1002, Permission: sqlcgen.SharePermissionEdit}); err != nil {
		t.Fatal(err)
	}
	if _, err := shareService.CreateGrant(ctx, shares.GrantCreateInput{OwnerID: 1001, FileID: folderID, GranteeID: 1003, Permission: sqlcgen.SharePermissionRead}); err != nil {
		t.Fatal(err)
	}
	pipeline := transfer.NewPipeline(uploadService, apiFixedResolver(9001), storage, nil, transfer.Config{Receipts: receipts})
	handler := api.NewHandler(catalogService, uploadService, pipeline, transfer.NewDownloader(catalogService, storage, nil), health.NewService("test", db.Pool), 0, nil).ConfigureDomains(nil, nil, nil, nil, shareService, nil)
	server, err := api.NewServer(handler, api.NewSecurity(tokenUserAuthenticator{"owner": 1001, "editor": 1002, "reader": 1003}))
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"owner", "editor", "public"} {
		for _, geometry := range []struct{ expected, actual int64 }{{0, 0}, {3, 3}, {-1, 0}, {-1, 3}} {
			size := geometry.expected
			t.Run(fmt.Sprintf("%s/size=%d/actual=%d", route, size, geometry.actual), func(t *testing.T) {
				headers := map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + route, "Idempotency-Key": uuid.NewString()}
				base := "/v1/uploads"
				if route == "public" {
					base = "/v1/public/shares/" + editable.Token + "/uploads"
					delete(headers, "Authorization")
					headers["X-Share-Password"] = password
				}
				created := performRequest(t, server, http.MethodPost, base, []byte(fmt.Sprintf(`{"parentId":%q,"name":%q,"size":%d,"modTime":"2026-07-01T00:00:00Z","preferredPartSize":1048576}`, folderID.String(), uuid.NewString(), size)), headers)
				if created.Code != http.StatusCreated {
					t.Fatalf("create: %d %s", created.Code, created.Body.String())
				}
				var session gen.UploadSession
				if err := json.Unmarshal(created.Body.Bytes(), &session); err != nil {
					t.Fatal(err)
				}
				if !session.RequiresCompletionManifest || session.PartSize != 16<<20 {
					t.Fatalf("session: %#v", session)
				}
				path := base + "/" + uuid.UUID(session.ID).String()
				for _, body := range []string{"", "{", "{}", `{"size":-1}`, `{"size":0,"hash":{"algorithm":"sha256","value":"` + strings.Repeat("a", 64) + `"}}`} {
					response := performRequest(t, server, http.MethodPost, path+"/complete", []byte(body), headers)
					assertB1APIError(t, response.Code, response.Body.Bytes(), 400, "invalid_request")
				}
				invalidDigest := performRequest(t, server, http.MethodPost, path+"/complete", []byte(`{"size":0,"hash":{"algorithm":"blake3","value":"invalid"}}`), headers)
				assertB1APIError(t, invalidDigest.Code, invalidDigest.Body.Bytes(), 422, "invalid_request")
				impossible := performRequest(t, server, http.MethodPost, path+"/complete", []byte(`{"size":9223372036854775807}`), headers)
				assertB1APIError(t, impossible.Code, impossible.Body.Bytes(), 422, "invalid_request")
				if size != 0 {
					incomplete := performRequest(t, server, http.MethodPost, path+"/complete", []byte(`{"size":3}`), headers)
					assertB1APIError(t, incomplete.Code, incomplete.Body.Bytes(), 409, "upload_incomplete")
				}
				payload := []byte("abc")
				if geometry.actual == 0 {
					payload = nil
				}
				if len(payload) != 0 {
					headers["Content-Type"] = "application/octet-stream"
					if route == "public" {
						missingType := map[string]string{"X-Share-Password": password}
						response := performRequest(t, server, http.MethodPut, path+"/parts/1", payload, missingType)
						if response.Code != http.StatusBadRequest {
							t.Fatalf("missing binary content type: %d %s", response.Code, response.Body.String())
						}
					}
					assertTruncated := func(t *testing.T) {
						t.Helper()
						request := httptest.NewRequest(http.MethodPut, path+"/parts/1", nil)
						request.Body = io.NopCloser(b1TruncatedBody{strings.NewReader("xy")})
						request.ContentLength = int64(len(payload))
						request.Header.Set("Content-Length", fmt.Sprint(len(payload)))
						for key, value := range headers {
							request.Header.Set(key, value)
						}
						response := httptest.NewRecorder()
						server.ServeHTTP(response, request)
						assertB1APIError(t, response.Code, response.Body.Bytes(), 422, "invalid_request")
					}
					t.Run("truncated-stream-before-store", assertTruncated)
					unpublished := performRequest(t, server, http.MethodGet, "/v1/files?parentId="+folderID.String(), nil, map[string]string{"Authorization": "Bearer owner"})
					if unpublished.Code != http.StatusOK || bytes.Contains(unpublished.Body.Bytes(), []byte(session.Name)) {
						t.Fatalf("truncated stream published file: %d %s", unpublished.Code, unpublished.Body.String())
					}
					put := performRequest(t, server, http.MethodPut, path+"/parts/1", payload, headers)
					if put.Code != http.StatusCreated {
						t.Fatalf("put: %d %s", put.Code, put.Body.String())
					}
					t.Run("truncated-stream-preserves-winner", assertTruncated)
					headers["Content-Type"] = "application/json"
				}
				wrong := performRequest(t, server, http.MethodPost, path+"/complete", []byte(fmt.Sprintf(`{"size":%d,"hash":{"algorithm":"blake3","value":"%s"}}`, len(payload), strings.Repeat("0", 64))), headers)
				assertB1APIError(t, wrong.Code, wrong.Body.Bytes(), 422, "hash_mismatch")
				root := blake3.New()
				if len(payload) != 0 {
					block := blake3.Sum256(payload)
					_, _ = root.Write(block[:])
				}
				manifest := []byte(fmt.Sprintf(`{"size":%d,"hash":{"algorithm":"blake3","value":"%s"}}`, len(payload), hex.EncodeToString(root.Sum(nil))))
				completed := performRequest(t, server, http.MethodPost, path+"/complete", manifest, headers)
				if completed.Code != http.StatusCreated {
					t.Fatalf("complete: %d %s", completed.Code, completed.Body.String())
				}
				if completed.Header().Get("Etag") == "" || completed.Header().Get("Location") == "" {
					t.Fatal("missing completion headers")
				}
				// Discard the first response, as if the connection dropped after commit.
				replayed := performRequest(t, server, http.MethodPost, path+"/complete", manifest, headers)
				if replayed.Code != http.StatusCreated || replayed.Body.String() != completed.Body.String() || replayed.Header().Get("Etag") != completed.Header().Get("Etag") {
					t.Fatalf("replay: %d %s", replayed.Code, replayed.Body.String())
				}
				var file gen.FileEntry
				if err := json.Unmarshal(replayed.Body.Bytes(), &file); err != nil {
					t.Fatal(err)
				}
				downloadPath := "/v1/files/" + uuid.UUID(file.ID).String() + "/content"
				if route == "public" {
					downloadPath = "/v1/public/shares/" + editable.Token + "/files/" + uuid.UUID(file.ID).String() + "/content"
				}
				download := performRequest(t, server, http.MethodGet, downloadPath, nil, headers)
				if download.Code != http.StatusOK || download.Body.String() != string(payload) {
					t.Fatalf("download: %d %q", download.Code, download.Body.String())
				}
				headers["Idempotency-Key"] = uuid.NewString()
				replacement := performRequest(t, server, http.MethodPost, base, []byte(fmt.Sprintf(`{"parentId":%q,"name":%q,"size":%d,"modTime":"2026-07-01T00:00:00Z","conflictPolicy":"replace"}`, folderID.String(), file.Name, len(payload))), headers)
				if replacement.Code != http.StatusCreated {
					t.Fatalf("replacement create: %d %s", replacement.Code, replacement.Body.String())
				}
				var replacementSession gen.UploadSession
				if err := json.Unmarshal(replacement.Body.Bytes(), &replacementSession); err != nil {
					t.Fatal(err)
				}
				replacementPath := base + "/" + uuid.UUID(replacementSession.ID).String()
				if len(payload) != 0 {
					headers["Content-Type"] = "application/octet-stream"
					put := performRequest(t, server, http.MethodPut, replacementPath+"/parts/1", []byte("xyz"), headers)
					if put.Code != http.StatusCreated {
						t.Fatalf("replacement put: %d %s", put.Code, put.Body.String())
					}
					headers["Content-Type"] = "application/json"
				}
				rejected := performRequest(t, server, http.MethodPost, replacementPath+"/complete", []byte(fmt.Sprintf(`{"size":%d,"hash":{"algorithm":"blake3","value":"%s"}}`, len(payload), strings.Repeat("0", 64))), headers)
				assertB1APIError(t, rejected.Code, rejected.Body.Bytes(), 422, "hash_mismatch")
				preserved := performRequest(t, server, http.MethodGet, downloadPath, nil, headers)
				if preserved.Code != http.StatusOK || preserved.Body.String() != string(payload) || preserved.Header().Get("Etag") != download.Header().Get("Etag") {
					t.Fatalf("failed replacement changed old file: %d %s", preserved.Code, preserved.Body.String())
				}
				if route == "owner" {
					for _, denied := range []struct {
						path, token, password string
						status                int
					}{
						{path, "reader", "", 403},
						{"/v1/public/shares/" + readOnly.Token + "/uploads/" + uuid.UUID(session.ID).String(), "", "", 403},
						{"/v1/public/shares/" + editable.Token + "/uploads/" + uuid.UUID(session.ID).String(), "", "wrong", 401},
						// Unknown tokens retain the existing indistinguishable expired-share response.
						{"/v1/public/shares/invalid/uploads/" + uuid.UUID(session.ID).String(), "", "", 410},
					} {
						response := performRequest(t, server, http.MethodPost, denied.path+"/complete", manifest, map[string]string{"Content-Type": "application/json", "Idempotency-Key": uuid.NewString(), "Authorization": "Bearer " + denied.token, "X-Share-Password": denied.password})
						if response.Code != denied.status {
							t.Fatalf("denied: %d %s", response.Code, response.Body.String())
						}
					}
				}
				t.Run("completed-replay-current-file-access", func(t *testing.T) {
					fileID := uuid.UUID(file.ID)
					generation := int64(file.Generation)
					moved, err := catalogService.MoveWithPolicy(ctx, 1001, fileID, &privateFolderID, &generation, "fail")
					if err != nil {
						t.Fatal(err)
					}
					privateName := "private-" + uuid.NewString()
					if _, err := catalogService.Update(ctx, catalog.UpdateInput{UserID: 1001, FileID: fileID, ExpectedGeneration: &moved.Generation, Name: &privateName}); err != nil {
						t.Fatal(err)
					}
					sizeOnly := []byte(fmt.Sprintf(`{"size":%d}`, len(payload)))
					for _, actor := range []string{"editor", "public", "owner"} {
						replayPath := "/v1/uploads/" + uuid.UUID(session.ID).String() + "/complete"
						replayHeaders := map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + actor, "Idempotency-Key": uuid.NewString()}
						if actor == "public" {
							replayPath = "/v1/public/shares/" + editable.Token + "/uploads/" + uuid.UUID(session.ID).String() + "/complete"
							delete(replayHeaders, "Authorization")
							replayHeaders["X-Share-Password"] = password
						}
						response := performRequest(t, server, http.MethodPost, replayPath, sizeOnly, replayHeaders)
						if actor == "owner" {
							if response.Code != http.StatusCreated || !bytes.Contains(response.Body.Bytes(), []byte(privateName)) || !bytes.Contains(response.Body.Bytes(), []byte(privateFolderID.String())) {
								t.Fatalf("owner lost completed result: %d %s", response.Code, response.Body.String())
							}
						} else {
							wantStatus := http.StatusForbidden
							if actor == "public" {
								wantStatus = http.StatusNotFound
							}
							if response.Code != wantStatus || bytes.Contains(response.Body.Bytes(), []byte(privateName)) || bytes.Contains(response.Body.Bytes(), []byte(privateFolderID.String())) {
								t.Fatalf("%s accessed moved result: %d %s", actor, response.Code, response.Body.String())
							}
						}
					}
				})
			})
		}
	}
}

func assertB1APIError(t *testing.T, status int, body []byte, wantStatus int, wantCode string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || envelope.Error.Code != wantCode {
		t.Fatalf("error: %d %s; want %d %s", status, body, wantStatus, wantCode)
	}
}

type b1TruncatedBody struct {
	io.Reader
}

func (r b1TruncatedBody) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// TestB1PairedRcloneResumeRoundTrip requires an explicitly supplied offline candidate.
// It never discovers an installed rclone or connects to Telegram.
func TestB1PairedRcloneResumeRoundTrip(t *testing.T) {
	binary := os.Getenv("B1_RCLONE_BINARY")
	expectedChecksum := os.Getenv("B1_RCLONE_SHA256")
	if binary == "" || expectedChecksum == "" {
		t.Fatal("B1_RCLONE_BINARY and B1_RCLONE_SHA256 are required; paired proof is NOT-RUN")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("B1_RCLONE_BINARY must be an absolute owned candidate path")
	}
	executable, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	checksum := sha256.Sum256(executable)
	if !strings.EqualFold(hex.EncodeToString(checksum[:]), expectedChecksum) {
		t.Fatal("candidate executable SHA-256 does not match B1_RCLONE_SHA256")
	}
	if os.Getenv("B1_ISOLATED_DB_APPROVED") != "1" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Fatal("B1_ISOLATED_DB_APPROVED=1 and disposable TEST_DATABASE_URL are required")
	}
	t.Logf("candidate SHA-256: %x", checksum)
	for _, scenario := range []string{"fresh", "zero", "same-byte-resume", "different-byte-open-resume"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			if _, err := db.Pool.Exec(ctx, `INSERT INTO users (user_id) VALUES (1001); INSERT INTO channels (channel_id,user_id,name,selected) VALUES (9001,1001,'fixture',true)`); err != nil {
				t.Fatal(err)
			}
			catalogService := catalog.NewService(db.Pool, nil)
			uploadService := uploads.NewService(db.Pool)
			shareService, err := shares.NewService(db.Pool, catalogService)
			if err != nil {
				t.Fatal(err)
			}
			cipher, err := secureblob.New("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
			if err != nil {
				t.Fatal(err)
			}
			authService, err := authn.NewService(db.Pool, cipher, unusedTelegramLogin{}, authn.Config{
				SigningKey: "0123456789abcdef0123456789abcdef", Issuer: "test",
				AccessTokenTTL: time.Hour, RefreshTokenTTL: time.Hour, LoginFlowTTL: time.Minute,
			})
			if err != nil {
				t.Fatal(err)
			}
			storage := &apiMemoryStorage{}
			receipts := bindAPIReceiptPolicy(t, db.Pool, storage, catalogService, uploadService)
			pipeline := transfer.NewPipeline(uploadService, apiFixedResolver(9001), storage, nil, transfer.Config{Receipts: receipts})
			handler := api.NewHandler(catalogService, uploadService, pipeline, transfer.NewDownloader(catalogService, storage, nil), health.NewService("fixture", db.Pool), 0, nil).
				ConfigureDomains(authService, nil, nil, nil, shareService, nil)
			generated, err := api.NewServer(handler, api.NewSecurity(apiAuthenticator{}))
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var trace []string
			var cancelTransfer context.CancelFunc
			interrupt := false
			var completedBody []byte
			var completedPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := strings.TrimPrefix(r.URL.Path, "/api")
				mu.Lock()
				trace = append(trace, r.Method+" "+path)
				if interrupt && r.Method == http.MethodPut && strings.HasSuffix(path, "/parts/2") {
					interrupt = false
					cancel := cancelTransfer
					mu.Unlock()
					// Part one has committed; stop the actual process before publication.
					cancel()
					http.Error(w, "fixture interruption", http.StatusServiceUnavailable)
					return
				}
				mu.Unlock()
				if r.Method == http.MethodPost && strings.HasSuffix(path, "/complete") {
					body, readErr := io.ReadAll(r.Body)
					if readErr != nil {
						http.Error(w, "fixture body read", 500)
						return
					}
					mu.Lock()
					completedBody, completedPath = append([]byte(nil), body...), path
					mu.Unlock()
					r.Body = io.NopCloser(bytes.NewReader(body))
				}
				r.URL.Path = path
				generated.ServeHTTP(w, r)
			}))
			defer server.Close()
			dir := t.TempDir()
			source := filepath.Join(dir, "source.bin")
			payload := bytes.Repeat([]byte{0x37}, (64<<20)+113)
			if scenario == "zero" {
				payload = nil
			}
			modTime := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
			writeSource := func(data []byte) {
				t.Helper()
				if err := os.WriteFile(source, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(source, modTime, modTime); err != nil {
					t.Fatal(err)
				}
			}
			writeSource(payload)
			sourceSHA := sha256.Sum256(payload)
			sourceSize := len(payload)
			tree := blake3.New()
			for offset := 0; offset < len(payload); offset += 16 << 20 {
				end := min(offset+(16<<20), len(payload))
				block := blake3.Sum256(payload[offset:end])
				_, _ = tree.Write(block[:])
			}
			sourceTree := hex.EncodeToString(tree.Sum(nil))
			config := filepath.Join(dir, "rclone.conf")
			if err := os.WriteFile(config, []byte(fmt.Sprintf("[fixture]\ntype = teldrive\napi_host = %s\napi_key = test-key\nchunk_size = 64Mi\nupload_concurrency = 1\n", server.URL)), 0600); err != nil {
				t.Fatal(err)
			}
			run := func(stop bool) (string, error) {
				t.Helper()
				commandCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
				defer cancel()
				mu.Lock()
				interrupt, cancelTransfer = stop, cancel
				mu.Unlock()
				command := exec.CommandContext(commandCtx, binary, "copyto", source, "fixture:paired.bin",
					"--config", config, "--cache-dir", filepath.Join(dir, "cache"), "--ignore-times",
					"--transfers", "1", "--multi-thread-streams", "1", "--retries", "1",
					"--low-level-retries", "1", "--stats", "0", "-vv")
				// No inherited remote config, credentials, proxies or production cache.
				command.Env = []string{"HOME=" + dir, "TMPDIR=" + dir, "PATH=/usr/bin:/bin"}
				output, runErr := command.CombinedOutput()
				return string(output), runErr
			}
			download := func(id string) []byte {
				t.Helper()
				response := performRequest(t, generated, http.MethodGet, "/v1/files/"+id+"/content", nil, map[string]string{"Authorization": "Bearer test-token"})
				if response.Code != http.StatusOK {
					t.Fatalf("download: %d %s", response.Code, response.Body.String())
				}
				return response.Body.Bytes()
			}
			fileID := func() string {
				t.Helper()
				var id string
				if err := db.Pool.QueryRow(ctx, `SELECT id::text FROM files WHERE user_id=1001 AND name='paired.bin' AND status='active'`).Scan(&id); err != nil {
					t.Fatal(err)
				}
				return id
			}
			negative := scenario == "different-byte-open-resume"
			var priorID, shareToken string
			if negative {
				writeSource([]byte("previous published destination"))
				if output, err := run(false); err != nil {
					t.Fatalf("prior publication: %v\n%s", err, output)
				}
				priorID = fileID()
				share, err := shareService.Create(ctx, shares.CreateInput{OwnerID: 1001, FileID: uuid.MustParse(priorID), Permission: sqlcgen.SharePermissionRead})
				if err != nil {
					t.Fatal(err)
				}
				shareToken = share.Token
				writeSource(payload)
			}
			resume := negative || scenario == "same-byte-resume"
			var seededID string
			if resume {
				if output, err := run(true); err == nil {
					t.Fatalf("interrupted transfer succeeded:\n%s", output)
				}
				if err := db.Pool.QueryRow(ctx, `SELECT id::text FROM upload_sessions WHERE user_id=1001 AND name='paired.bin' AND state='open' AND expires_at>now()`).Scan(&seededID); err != nil {
					t.Fatal(err)
				}
				var stored int
				if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM upload_parts WHERE upload_id=$1 AND state='stored' AND part_no=1 AND plain_size=67108864 AND checksum IS NOT NULL`, seededID).Scan(&stored); err != nil || stored != 1 {
					t.Fatalf("interruption did not retain verified first part: %d %v", stored, err)
				}
			}
			snapshot := func() string {
				t.Helper()
				var result string
				err := db.Pool.QueryRow(ctx, `SELECT jsonb_build_object(
					'files',(SELECT jsonb_agg(to_jsonb(f) ORDER BY id) FROM files f WHERE user_id=1001),
					'parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no) FROM file_parts p WHERE file_id IN (SELECT id FROM files WHERE user_id=1001)),
					'shares',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM file_shares s WHERE file_id IN (SELECT id FROM files WHERE user_id=1001)),
					'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM user_events e WHERE user_id=1001)
				)::text`).Scan(&result)
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			before := ""
			var priorSHA [32]byte
			if negative {
				before = snapshot()
				priorSHA = sha256.Sum256(download(priorID))
				changed := append([]byte(nil), payload...)
				changed[7] ^= 0xff
				writeSource(changed)
			}
			storage.mu.Lock()
			writesBefore := storage.nextID
			storage.mu.Unlock()
			mu.Lock()
			trace = nil
			completedBody, completedPath = nil, ""
			mu.Unlock()
			output, runErr := run(false)
			mu.Lock()
			requests := append([]string(nil), trace...)
			manifest, completePath := append([]byte(nil), completedBody...), completedPath
			mu.Unlock()
			if resume {
				found := false
				for _, request := range requests {
					found = found || request == "GET /v1/uploads/"+seededID+"/parts"
					if request == "POST /v1/uploads" {
						t.Fatal("resume created an alternative upload session")
					}
				}
				if !found {
					t.Fatalf("candidate never inspected seeded session %s: %v\n%s", seededID, requests, output)
				}
			}
			if negative {
				if runErr == nil || !strings.Contains(output, "stored upload part 1 does not match source") || !strings.Contains(output, seededID) {
					t.Fatalf("expected exact-session current-byte rejection, got %v\n%s", runErr, output)
				}
				for _, request := range requests {
					if strings.HasSuffix(request, "/complete") {
						t.Fatalf("different bytes attempted completion: %s", request)
					}
				}
				if after := snapshot(); after != before {
					t.Fatal("failed resume changed destination/part references/share/events")
				}
				if sha256.Sum256(download(priorID)) != priorSHA || fileID() != priorID {
					t.Fatal("failed resume changed prior published bytes or ID")
				}
				shared := performRequest(t, generated, http.MethodGet, "/v1/public/shares/"+shareToken+"/content", nil, nil)
				if shared.Code != http.StatusOK || sha256.Sum256(shared.Body.Bytes()) != priorSHA {
					t.Fatalf("prior share access changed: %d", shared.Code)
				}
				var state string
				if err := db.Pool.QueryRow(ctx, `SELECT state::text FROM upload_sessions WHERE id=$1`, seededID).Scan(&state); err != nil || state != "open" {
					t.Fatalf("seed session changed state: %q %v", state, err)
				}
				return
			}
			if runErr != nil {
				t.Fatalf("candidate transfer: %v\n%s", runErr, output)
			}
			id := fileID()
			actual := download(id)
			if len(actual) != sourceSize || sha256.Sum256(actual) != sourceSHA {
				t.Fatal("independent full download SHA-256/size mismatch")
			}
			var storedTree string
			if err := db.Pool.QueryRow(ctx, `SELECT hash_value FROM files WHERE id=$1`, id).Scan(&storedTree); err != nil || storedTree != sourceTree {
				t.Fatalf("global-block tree oracle mismatch: %q %v", storedTree, err)
			}
			storage.mu.Lock()
			writes := storage.nextID - writesBefore
			storage.mu.Unlock()
			wantWrites := int64(2)
			if resume {
				wantWrites = 1
			} else if sourceSize == 0 {
				wantWrites = 0
			}
			if writes != wantWrites {
				t.Fatalf("Telegram writes=%d, want %d", writes, wantWrites)
			}
			if completePath == "" || len(manifest) == 0 {
				t.Fatal("candidate did not send completion manifest")
			}
			// Drop a real completion replay response after the generated server commits.
			published := snapshot()
			headers := map[string]string{"Authorization": "Bearer test-token", "Content-Type": "application/json", "Idempotency-Key": uuid.NewString()}
			lostStatus := make(chan int, 1)
			lostServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response := httptest.NewRecorder()
				generated.ServeHTTP(response, r)
				lostStatus <- response.Code
				connection, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
			}))
			lostRequest, err := http.NewRequest(http.MethodPost, lostServer.URL+completePath, bytes.NewReader(manifest))
			if err != nil {
				lostServer.Close()
				t.Fatal(err)
			}
			for key, value := range headers {
				lostRequest.Header.Set(key, value)
			}
			lostClient := &http.Client{Timeout: 10 * time.Second}
			lostResponse, lostErr := lostClient.Do(lostRequest)
			if lostResponse != nil {
				_ = lostResponse.Body.Close()
			}
			lostServer.Close()
			if lostErr == nil {
				t.Fatal("completion response loss was not observed by HTTP client")
			}
			select {
			case status := <-lostStatus:
				if status != http.StatusCreated {
					t.Fatalf("lost completion response status: %d", status)
				}
			case <-time.After(time.Second):
				t.Fatal("lost-response request never reached generated completion route")
			}
			replay := performRequest(t, generated, http.MethodPost, completePath, manifest, headers)
			if replay.Code != http.StatusCreated || snapshot() != published {
				t.Fatalf("same-result completion replay: %d %s", replay.Code, replay.Body.String())
			}
			var replayed gen.FileEntry
			if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil || uuid.UUID(replayed.ID).String() != id {
				t.Fatalf("replay changed published ID: %v", err)
			}
			different := performRequest(t, generated, http.MethodPost, completePath, []byte(fmt.Sprintf(`{"size":%d}`, sourceSize+1)), headers)
			assertB1APIError(t, different.Code, different.Body.Bytes(), http.StatusConflict, "upload_invalid_state")
			if snapshot() != published {
				t.Fatal("different completed manifest changed publication/events")
			}
		})
	}
}
