//go:build integration

package jobs_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/divyam234/riverpro/riverencrypt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestUploadSourceWorkerPublishesAndSkipsMatchingLocalFile(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	root := t.TempDir()
	filePath := filepath.Join(root, "hello.txt")
	if err := os.WriteFile(filePath, []byte("hello uploader"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatal(err)
	}
	storage := &uploadWorkerStorage{}
	uploadService := uploads.NewService(db.Pool)
	catalogService := catalog.NewService(db.Pool, nil)
	channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
	policy := bindUploadPolicy(t, db.Pool, storage, catalogService, uploadService)
	pipeline := transfer.NewPipeline(uploadService, channelService, storage, nil, transfer.Config{Receipts: policy})
	admission := newSourceAdmission(t, db.Pool, storage, catalogService, uploadService, pipeline, policy, nil, []string{root})
	worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, uploadService, pipeline, nil, 0, false, []string{root})
	args := jobs.UploadSourceArgs{
		BatchID: "9ba4ddef-ea18-4a67-acd4-277f63d813ce", SourceIndex: 0, UserID: 1001, PartConcurrency: 2,
		Source: jobs.UploadFileSource{Type: "local", Location: filePath, DestinationPath: "folder/hello.txt", Size: info.Size(), ModTime: info.ModTime(), HasModTime: true},
	}
	job := admission.job(t, args)
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("first Work() error = %v", err)
	}
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("second Work() error = %v", err)
	}
	if storage.uploads != 1 {
		t.Fatalf("Telegram uploads = %d, want 1", storage.uploads)
	}
	var count int
	var mimeType string
	if err := db.Pool.QueryRow(ctx, `SELECT count(*), min(f.mime_type) FROM files f JOIN files p ON p.id = f.parent_id WHERE f.name = 'hello.txt' AND p.name = 'folder' AND f.status = 'active'`).Scan(&count, &mimeType); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("published matching files = %d, want 1", count)
	}
	if mimeType != "text/plain" {
		t.Fatalf("published MIME type = %q, want text/plain", mimeType)
	}
}

type uploadWorkerStorage struct {
	mu          sync.Mutex
	uploads     int
	lastPayload []byte
	payloads    map[int64][]byte
}

func (s *uploadWorkerStorage) Upload(_ context.Context, request telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	payload, err := io.ReadAll(request.Reader)
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	if int64(len(payload)) != request.Size {
		return telegramstore.StoredPart{}, fmt.Errorf("payload size = %d, want %d", len(payload), request.Size)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads++
	s.lastPayload = payload
	if s.payloads == nil {
		s.payloads = make(map[int64][]byte)
	}
	s.payloads[int64(s.uploads)] = payload
	return telegramstore.StoredPart{ChannelID: request.ChannelID, MessageID: int64(s.uploads), Size: request.Size}, nil
}

func (s *uploadWorkerStorage) OpenRange(_ context.Context, request telegramstore.RangeRequest) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.payloads[request.MessageID]
	if !ok || request.Offset < 0 || request.Offset > int64(len(payload)) {
		return nil, fmt.Errorf("invalid message range")
	}
	end := int64(len(payload))
	if request.Length >= 0 {
		end = min(end, request.Offset+request.Length)
	}
	return io.NopCloser(bytes.NewReader(payload[request.Offset:end])), nil
}
func (*uploadWorkerStorage) DeleteMessages(context.Context, int64, int64, []int64) error { return nil }
func (*uploadWorkerStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, nil
}
func (*uploadWorkerStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, nil
}
func (*uploadWorkerStorage) DeleteChannel(context.Context, int64, int64) error { return nil }

func TestUploadSourceEncryptionRequired(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	payload := []byte("secret import")
	filePath := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(filePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "secret.txt", time.Time{}, bytes.NewReader(payload))
	}))
	defer server.Close()
	storage := &uploadWorkerStorage{}
	uploadService := uploads.NewService(db.Pool)
	catalogService := catalog.NewService(db.Pool, nil)
	channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
	keys := transfer.StaticKeyProvider{1: "import-encryption-policy-test-key"}
	policy := bindUploadPolicy(t, db.Pool, storage, catalogService, uploadService)
	pipeline := transfer.NewPipeline(uploadService, channelService, storage, keys, transfer.Config{Receipts: policy})
	admission := newSourceAdmission(t, db.Pool, storage, catalogService, uploadService, pipeline, policy, server.Client(), []string{filepath.Dir(filePath)})
	for _, required := range []bool{false, true} {
		for _, requested := range []bool{false, true} {
			for _, sourceType := range []string{"local", "http"} {
				t.Run(fmt.Sprintf("required=%t/requested=%t/source=%s", required, requested, sourceType), func(t *testing.T) {
					name := uuid.NewString() + ".txt"
					location := filePath
					if sourceType == "http" {
						location = server.URL
					}
					// A plaintext session queued before policy activation must not be resumed
					// when encryption is now required.
					if required && !requested {
						mimeType := "text/plain"
						if _, err := uploadService.Create(ctx, uploads.CreateInput{UserID: 1001, Name: name, ExpectedSize: int64(len(payload)), MIMEType: &mimeType, ModTime: info.ModTime()}); err != nil {
							t.Fatal(err)
						}
					}
					worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, uploadService, pipeline, server.Client(), 1, required, []string{filepath.Dir(filePath)})
					job := admission.job(t, jobs.UploadSourceArgs{
						BatchID: uuid.NewString(), UserID: 1001, Encryption: requested,
						Source: jobs.UploadFileSource{Type: sourceType, Location: location, DestinationPath: name, Size: int64(len(payload)), ModTime: info.ModTime(), HasModTime: true},
					})
					if err := worker.Work(ctx, job); err != nil {
						t.Fatalf("Work() = %v", err)
					}
					wantEncrypted := required || requested
					var encrypted bool
					var keyVersion int32
					if err := db.Pool.QueryRow(ctx, `SELECT encryption, COALESCE(encryption_key_version, 0) FROM files WHERE user_id=1001 AND name=$1 AND status='active'`, name).Scan(&encrypted, &keyVersion); err != nil {
						t.Fatal(err)
					}
					if encrypted != wantEncrypted || wantEncrypted && keyVersion != 1 || !wantEncrypted && keyVersion != 0 {
						t.Fatalf("file encryption=%t, key=%d, want encryption=%t", encrypted, keyVersion, wantEncrypted)
					}
					storage.mu.Lock()
					plaintext := bytes.Equal(storage.lastPayload, payload)
					storage.mu.Unlock()
					if plaintext == wantEncrypted {
						t.Fatalf("Telegram payload is plaintext=%t, want encryption=%t", plaintext, wantEncrypted)
					}
				})
			}
		}
	}
}

func TestB1LocalMatchingPlaintextRequiresEncryptedReplacement(t *testing.T) {
	for _, policy := range []struct {
		name      string
		requested bool
		required  bool
	}{
		{name: "requested", requested: true},
		{name: "required", required: true},
	} {
		t.Run(policy.name, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			seedCleanupOwner(t, db.Pool)
			root := t.TempDir()
			location := filepath.Join(root, "secret.txt")
			payload := []byte("matching plaintext must become ciphertext")
			if err := os.WriteFile(location, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			storage := &uploadWorkerStorage{}
			service := uploads.NewService(db.Pool)
			catalogService := catalog.NewService(db.Pool, nil)
			channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
			keys := transfer.StaticKeyProvider{1: "import-encryption-policy-test-key"}
			receiptPolicy := bindUploadPolicy(t, db.Pool, storage, catalogService, service)
			pipeline := transfer.NewPipeline(service, channelService, storage, keys, transfer.Config{Receipts: receiptPolicy})
			admission := newSourceAdmission(t, db.Pool, storage, catalogService, service, pipeline, receiptPolicy, nil, []string{root})
			name := uuid.NewString() + ".txt"
			args := jobs.UploadSourceArgs{UserID: 1001, Source: jobs.UploadFileSource{Type: "local", Location: location, DestinationPath: name}}
			job := admission.job(t, args)
			plaintextWorker := jobs.NewUploadSourceWorker(db.Pool, catalogService, service, pipeline, nil, 1, false, []string{root})
			if err := plaintextWorker.Work(ctx, job); err != nil {
				t.Fatal(err)
			}
			var oldID uuid.UUID
			var oldEncrypted bool
			if err := db.Pool.QueryRow(ctx, `SELECT id,encryption FROM files WHERE user_id=1001 AND name=$1 AND status='active'`, name).Scan(&oldID, &oldEncrypted); err != nil {
				t.Fatal(err)
			}
			if oldEncrypted || storage.uploads != 1 || !bytes.Equal(storage.lastPayload, payload) {
				t.Fatal("fixture did not publish matching plaintext")
			}
			args.Encryption = policy.requested
			job = admission.job(t, args)
			worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, service, pipeline, nil, 1, policy.required, []string{root})
			if err := worker.Work(ctx, job); err != nil {
				t.Fatal(err)
			}
			var activeID uuid.UUID
			var encrypted bool
			var keyVersion int32
			if err := db.Pool.QueryRow(ctx, `SELECT id,encryption,COALESCE(encryption_key_version,0) FROM files WHERE user_id=1001 AND name=$1 AND status='active'`, name).Scan(&activeID, &encrypted, &keyVersion); err != nil {
				t.Fatal(err)
			}
			if activeID == oldID || !encrypted || keyVersion != 1 {
				t.Fatalf("replacement id=%s encryption=%t key=%d", activeID, encrypted, keyVersion)
			}
			if storage.uploads != 2 || bytes.Equal(storage.lastPayload, payload) || len(storage.lastPayload) <= len(payload) {
				t.Fatal("replacement did not store ciphertext")
			}
			var messageID, storedSize, plainSize int64
			if err := db.Pool.QueryRow(ctx, `SELECT message_id,stored_size,plain_size FROM file_parts WHERE file_id=$1`, activeID).Scan(&messageID, &storedSize, &plainSize); err != nil {
				t.Fatal(err)
			}
			if messageID != 2 || storedSize != int64(len(storage.lastPayload)) || plainSize != int64(len(payload)) {
				t.Fatal("published part metadata does not describe encrypted replacement")
			}
			if err := worker.Work(ctx, job); err != nil {
				t.Fatal(err)
			}
			if storage.uploads != 2 {
				t.Fatal("matching encrypted destination was not skipped")
			}
		})
	}
}

func TestB1DeferredLocalImportRejectsSymlinkEscapeBeforePublication(t *testing.T) {
	for _, replacement := range []string{"leaf", "ancestor"} {
		t.Run(replacement, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			seedCleanupOwner(t, db.Pool)
			root := t.TempDir()
			directory := filepath.Join(root, "nested")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			location := filepath.Join(directory, "source.txt")
			if err := os.WriteFile(location, []byte("queued source"), 0o600); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(location)
			if err != nil {
				t.Fatal(err)
			}
			name := uuid.NewString() + ".txt"
			// Capture the durable source-job fields before replacing the expanded path.
			args := jobs.UploadSourceArgs{UserID: 1001, Source: jobs.UploadFileSource{Type: "local", Location: location, DestinationPath: name, Size: info.Size(), ModTime: info.ModTime(), HasModTime: true}}
			storage := &uploadWorkerStorage{}
			service := uploads.NewService(db.Pool)
			catalogService := catalog.NewService(db.Pool, nil)
			channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
			policy := bindUploadPolicy(t, db.Pool, storage, catalogService, service)
			pipeline := transfer.NewPipeline(service, channelService, storage, nil, transfer.Config{Receipts: policy})
			admission := newSourceAdmission(t, db.Pool, storage, catalogService, service, pipeline, policy, nil, []string{root})
			job := admission.job(t, args)
			outside := t.TempDir()
			external := filepath.Join(outside, "source.txt")
			if err := os.WriteFile(external, []byte("private bytes outside configured roots"), 0o600); err != nil {
				t.Fatal(err)
			}
			if replacement == "leaf" {
				if err := os.Remove(location); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, location); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(directory, directory+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, directory); err != nil {
					t.Fatal(err)
				}
			}
			worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, service, pipeline, nil, 0, false, []string{root})
			if err := worker.Work(ctx, job); err == nil {
				t.Fatal("escaped source succeeded")
			}
			if storage.uploads != 0 || len(storage.payloads) != 0 {
				t.Fatal("escaped source reached Telegram storage")
			}
			var published, sessions int
			if err := db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM files WHERE name=$1),(SELECT count(*) FROM upload_sessions WHERE name=$1)`, name).Scan(&published, &sessions); err != nil {
				t.Fatal(err)
			}
			if published != 0 || sessions != 0 {
				t.Fatalf("escaped source published files=%d sessions=%d", published, sessions)
			}
		})
	}
}

type sourceAdmission struct {
	pool    *pgxpool.Pool
	runtime *jobs.Runtime
	client  *river.Client[pgx.Tx]
	cipher  *secureblob.Cipher
	catalog *catalog.Service
	uploads *uploads.Service
	http    *http.Client
	roots   []string
}

func bindUploadPolicy(t *testing.T, pool *pgxpool.Pool, storage telegramstore.Storage, cat *catalog.Service, service *uploads.Service) *lifecycle.Service {
	t.Helper()
	policy := lifecycle.NewService(pool, storage)
	if err := cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	if err := service.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	return policy
}
func newSourceAdmission(t *testing.T, pool *pgxpool.Pool, storage telegramstore.Storage, cat *catalog.Service, service *uploads.Service, pipeline *transfer.Pipeline, policy *lifecycle.Service, httpClient *http.Client, roots []string) *sourceAdmission {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := secureblob.NewWithKey(key, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := jobs.NewRuntimeWithServices(pool, storage, "teldrive", nil, cipher, time.Hour, jobs.UploaderServices{Catalog: cat, Uploads: service, Pipeline: pipeline, HTTPClient: httpClient, ActiveKeyVersion: 1, LocalImportRoots: roots}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err = policy.BindReceiptEnqueuer(runtime); err != nil {
		t.Fatal(err)
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, jobs.NewUploadSourceWorker(pool, cat, service, pipeline, httpClient, 1, false, roots))
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: "teldrive", Workers: workers, Hooks: []rivertype.Hook{riverencrypt.NewEncryptHookConfig(&riverencrypt.EncryptHookConfig{Encryptor: cipher, JobKindsInclude: []string{jobs.UploadSourceKind}})}})
	if err != nil {
		t.Fatal(err)
	}
	return &sourceAdmission{pool: pool, runtime: runtime, client: client, cipher: cipher, catalog: cat, uploads: service, http: httpClient, roots: roots}
}
func (a *sourceAdmission) job(t *testing.T, args jobs.UploadSourceArgs) *river.Job[jobs.UploadSourceArgs] {
	t.Helper()
	ctx := context.Background()
	source := jobs.UploadSource{Type: args.Source.Type, DestinationPath: args.Source.DestinationPath, Headers: args.Source.Headers}
	if source.Type == "local" {
		source.Path = args.Source.Location
	} else {
		source.URL = args.Source.Location
	}
	batch := jobs.UploadBatchArgs{BatchID: uuid.NewString(), UserID: args.UserID, ParentID: args.ParentID, Sources: []jobs.UploadSource{source}, Encryption: args.Encryption, ChunkSize: args.ChunkSize, PartConcurrency: args.PartConcurrency}
	parent, err := a.runtime.InsertUploadBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	row, err := a.client.JobGet(ctx, parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(row.EncodedArgs, []byte(`"river_encrypted_args"`)) {
		t.Fatal("Runtime Q inserted plaintext batch args")
	}
	worker := jobs.NewUploadBatchWorker(a.pool, a.http, a.catalog, a.uploads, a.roots)
	if err = worker.Work(rivertest.WorkContext(ctx, a.client), &river.Job[jobs.UploadBatchArgs]{JobRow: row, Args: batch}); err != nil {
		t.Fatal(err)
	}
	var childID int64
	if err = a.pool.QueryRow(ctx, "SELECT job_id FROM import_job_attempts WHERE user_id=$1 AND batch_id=$2 AND source_index=0", args.UserID, batch.BatchID).Scan(&childID); err != nil {
		t.Fatal(err)
	}
	child, err := a.client.JobGet(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(child.EncodedArgs, []byte(`"river_encrypted_args"`)) {
		t.Fatal("atomic A inserted plaintext source args")
	}
	encoded, err := riverencrypt.DecryptArgs(a.cipher, child.EncodedArgs)
	if err != nil {
		t.Fatal(err)
	}
	var actual jobs.UploadSourceArgs
	if err = json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	return &river.Job[jobs.UploadSourceArgs]{JobRow: child, Args: actual}
}
