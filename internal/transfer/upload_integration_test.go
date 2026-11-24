//go:build integration

package transfer_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/zeebo/blake3"

	"github.com/tgdrive/teldrive/v2/internal/contentcrypto"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestUploadPipelinePlaintextAndRetry(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	body := []byte("plain Telegram upload")
	session, err := catalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "plain.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	uploadID, ok := dbtypes.GoogleUUID(session.ID)
	if !ok {
		t.Fatal("missing upload id")
	}
	checksum := checksumFor(body)
	resolver := &fixedResolver{channelID: 9001}
	pipeline := transfer.NewPipeline(catalog, resolver, storage, nil, transfer.Config{Receipts: policy})

	result, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &checksum, Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if result.Existing || result.Part.State != sqlcgen.UploadPartStateStored {
		t.Fatalf("UploadPart() result = %#v", result)
	}
	if got := storage.payload(0); !bytes.Equal(got, body) {
		t.Fatalf("stored payload = %q", got)
	}
	if !result.Part.Checksum.Valid || result.Part.Checksum.String != checksum || len(result.Part.BlockHashes) != treehash.DigestSize {
		t.Fatalf("stored metadata = %#v", result.Part)
	}

	retryBody := bytes.NewReader(body)
	retry, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: retryBody,
	})
	if err != nil || !retry.Existing {
		t.Fatalf("retry = %#v, %v", retry, err)
	}
	if retryBody.Len() != 0 || retry.Part.MessageID != result.Part.MessageID {
		t.Fatalf("retry did not consume equal bytes or reuse the message: unread=%d part=%+v", retryBody.Len(), retry.Part)
	}
	if storage.uploadCount() != 1 || resolver.calls != 1 {
		t.Fatalf("retry performed side effects: uploads=%d resolves=%d", storage.uploadCount(), resolver.calls)
	}

	file, err := catalog.Complete(context.Background(), b1TransferManifest(uploadID, body))
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	expectedFileHash := b1TransferSourceRoot(body)
	if !file.HashValue.Valid || file.HashValue.String != expectedFileHash {
		t.Fatalf("file hash = %#v, want %s", file.HashValue, expectedFileHash)
	}
}

type observedBusyCatalog struct {
	*uploads.Service
	busy   chan struct{}
	once   sync.Once
	resume chan struct{}
}

func (c *observedBusyCatalog) ClaimPart(ctx context.Context, in uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	claim, err := c.Service.ClaimPart(ctx, in)
	if errors.Is(err, uploads.ErrPartBusy) {
		c.once.Do(func() { close(c.busy) })
		select {
		case <-c.resume:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	return claim, err
}

func TestUploadPipelineOverlappingRetry(t *testing.T) {
	for _, scenario := range []string{"stored and completed", "equal body", "different body", "misleading checksum", "checksum conflict", "failed attempt", "expired lease", "cancelled waiter", "aborted session", "expired session"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			seedTransferOwner(t, db.Pool, 1001, 9001)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			storage := &memoryStorage{}
			service, policy := transferFixtureUploads(t, db.Pool, storage)
			catalog := &observedBusyCatalog{Service: service, busy: make(chan struct{}), resume: make(chan struct{})}
			body := []byte("data")
			session, err := catalog.Create(ctx, uploads.CreateInput{
				UserID: 1001, Name: "overlap.bin", ExpectedSize: 4, PartSize: 4,
			})
			if err != nil {
				t.Fatal(err)
			}
			uploadID, _ := dbtypes.GoogleUUID(session.ID)
			original, err := catalog.Service.ClaimPart(ctx, uploads.ClaimPartInput{
				UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 4,
			})
			if err != nil {
				t.Fatal(err)
			}
			checksum := checksumFor(body)
			var originalReceipt, staleReceipt uuid.UUID
			switch scenario {
			case "expired lease":
				staleReceipt = transferFixtureReceipt(t, ctx, policy, storage, body, uploadID, original, 999)
			case "cancelled waiter", "failed attempt":
			default:
				originalReceipt = transferFixtureReceipt(t, ctx, policy, storage, body, uploadID, original, 101)
			}
			var requestChecksum *string
			retryBytes := body
			switch scenario {
			case "different body", "misleading checksum":
				retryBytes = []byte("else")
				if scenario == "misleading checksum" {
					requestChecksum = &checksum
				}
			case "checksum conflict":
				wrong := checksumFor([]byte("else"))
				requestChecksum = &wrong
			}
			retryBody := bytes.NewReader(retryBytes)
			pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
			type outcome struct {
				result *transfer.UploadPartResult
				err    error
			}
			done := make(chan outcome, 1)
			waitCtx, stopWait := context.WithCancel(ctx)
			defer stopWait()
			go func() {
				var reader io.Reader = retryBody
				if scenario == "cancelled waiter" {
					reader = panicReader{}
				}
				result, err := pipeline.UploadPart(waitCtx, transfer.UploadPartRequest{
					UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: 4, Checksum: requestChecksum, Body: reader,
				})
				done <- outcome{result, err}
			}()
			select {
			case <-catalog.busy:
			case <-ctx.Done():
				t.Fatal("retry never observed the active lease")
			}
			switch scenario {
			case "cancelled waiter":
				stopWait()
			case "failed attempt":
				_, err = catalog.FailPart(ctx, uploads.FailPartInput{
					UploadID: uploadID, PartNo: 1, LeaseToken: original.LeaseToken, ErrorCode: "telegram_upload_failed",
				})
			case "expired lease":
				_, err = db.Pool.Exec(ctx, "UPDATE upload_parts SET lease_expires_at = now() - interval '1 second' WHERE upload_id = $1 AND part_no = 1", uploadID)
			default:
				hasher := treehash.NewBlockHasher()
				_, _ = hasher.Write(body)
				_, err = catalog.StorePart(ctx, uploads.StorePartInput{
					UploadID: uploadID, PartNo: 1, LeaseToken: original.LeaseToken, MessageID: 101, ReceiptID: originalReceipt,
					StoredSize: 4, Checksum: checksum, BlockHashes: hasher.Sum(),
				})
				if err == nil && scenario == "stored and completed" {
					_, err = catalog.Complete(ctx, b1TransferManifest(uploadID, body))
				}
				if err == nil && scenario == "aborted session" {
					_, err = db.Pool.Exec(ctx, "UPDATE upload_sessions SET state = 'aborted' WHERE id = $1", uploadID)
				}
				if err == nil && scenario == "expired session" {
					_, err = db.Pool.Exec(ctx, "UPDATE upload_sessions SET expires_at = now() - interval '1 second' WHERE id = $1", uploadID)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			close(catalog.resume)
			var got outcome
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal("retry did not finish")
			}
			switch scenario {
			case "different body", "misleading checksum", "checksum conflict":
				if !errors.Is(got.err, uploads.ErrPartConflict) {
					t.Fatalf("retry error = %v, want immutable body conflict", got.err)
				}
			case "aborted session":
				if !errors.Is(got.err, uploads.ErrInvalidState) {
					t.Fatalf("retry error = %v, want aborted session rejection", got.err)
				}
			case "expired session":
				if !errors.Is(got.err, uploads.ErrExpired) {
					t.Fatalf("retry error = %v, want expired session rejection", got.err)
				}
			case "cancelled waiter":
				if !errors.Is(got.err, context.Canceled) {
					t.Fatalf("retry error = %v, want cancellation", got.err)
				}
				part, err := catalog.GetPart(ctx, 1001, uploadID, 1)
				if err != nil || part.State != sqlcgen.UploadPartStateUploading || part.LeaseToken != dbtypes.UUID(original.LeaseToken) {
					t.Fatalf("waiter disturbed active lease: %+v, %v", part, err)
				}
			default:
				wantExisting := scenario != "failed attempt" && scenario != "expired lease"
				if got.err != nil || got.result == nil || got.result.Existing != wantExisting || retryBody.Len() != 0 {
					t.Fatalf("retry = %+v, %v", got.result, got.err)
				}
			}
			wantUploads := 0
			if scenario == "failed attempt" || scenario == "expired lease" {
				wantUploads = 1
			}
			if storage.uploadCount() != wantUploads {
				t.Fatalf("Telegram uploads = %d, want %d", storage.uploadCount(), wantUploads)
			}
			if scenario == "expired lease" {
				if err := catalog.RenewPart(ctx, uploads.RenewPartInput{UploadID: uploadID, PartNo: 1, LeaseToken: original.LeaseToken}); !errors.Is(err, uploads.ErrLeaseLost) {
					t.Fatalf("stale owner renewed winner: %v", err)
				}
				if _, err := catalog.FailPart(ctx, uploads.FailPartInput{UploadID: uploadID, PartNo: 1, LeaseToken: original.LeaseToken, ErrorCode: "stale"}); !errors.Is(err, uploads.ErrLeaseLost) {
					t.Fatalf("stale owner failed winner: %v", err)
				}
				if _, err := catalog.StorePart(ctx, uploads.StorePartInput{UploadID: uploadID, PartNo: 1, LeaseToken: original.LeaseToken, ReceiptID: staleReceipt, MessageID: 999, StoredSize: 4, Checksum: checksum, BlockHashes: got.result.Part.BlockHashes}); !errors.Is(err, uploads.ErrLeaseLost) {
					t.Fatalf("stale owner stored over winner: %v", err)
				}
				part, err := catalog.GetPart(ctx, 1001, uploadID, 1)
				if err != nil || !reflect.DeepEqual(part, got.result.Part) || !bytes.Equal(storage.payload(0), body) {
					t.Fatalf("stale owner disturbed winning bytes: %+v, %v", part, err)
				}
			}
		})
	}
}

func TestUploadPipelineHashingDisabled(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	body := []byte("upload without hashing")
	session, err := catalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "unhashed.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{DisableHashing: true, Receipts: policy})
	result, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if result.Part.Checksum.Valid || len(result.Part.BlockHashes) != 0 {
		t.Fatalf("stored hash metadata = (%#v, %x), want empty", result.Part.Checksum, result.Part.BlockHashes)
	}
	file, err := catalog.Complete(context.Background(), uploads.CompleteInput{UserID: 1001, UploadID: uploadID, Size: int64(len(body))})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if file.HashAlgorithm.Valid || file.HashValue.Valid {
		t.Fatalf("file hash = (%#v, %#v), want empty", file.HashAlgorithm, file.HashValue)
	}
}

func TestUploadPipelineEncryptedCompatibility(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	body := bytes.Repeat([]byte("encrypted-block-"), 6000)
	version := int32(7)
	session, err := catalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "encrypted.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
		Encryption: true, EncryptionKeyVersion: &version,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, transfer.StaticKeyProvider{7: "same-key-format-as-original"}, transfer.Config{
		Receipts: policy,
		Random:   bytes.NewReader(bytes.Repeat([]byte{9}, 32)),
	})
	result, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	ciphertext := storage.payload(0)
	if int64(len(ciphertext)) != contentcrypto.EncryptedSize(int64(len(body))) || bytes.Equal(ciphertext, body) {
		t.Fatalf("ciphertext size/content invalid: %d", len(ciphertext))
	}
	if !result.Part.Salt.Valid || result.Part.Salt.String == "" {
		t.Fatalf("encrypted part salt = %#v", result.Part.Salt)
	}
	cipher, err := contentcrypto.NewCipher("same-key-format-as-original", result.Part.Salt.String)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := cipher.DecryptData(io.NopCloser(bytes.NewReader(ciphertext)))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(decrypted)
	if err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("decrypt = %d bytes, %v", len(plain), err)
	}
	if _, err := catalog.Complete(context.Background(), b1TransferManifest(uploadID, body)); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
}

func TestUploadPipelineChecksumMismatchCompensates(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	body := []byte("checksum body")
	session, err := catalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "bad.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	wrong := checksumFor([]byte("different body"))
	pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
	_, err = pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &wrong, Body: bytes.NewReader(body),
	})
	if !errors.Is(err, transfer.ErrChecksumMismatch) {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if len(storage.deleted) != 1 || storage.deleted[0] != 1 {
		t.Fatalf("deleted messages = %#v", storage.deleted)
	}
	part, err := catalog.GetPart(context.Background(), 1001, uploadID, 1)
	if err != nil {
		t.Fatalf("GetPart() error = %v", err)
	}
	if part.State != sqlcgen.UploadPartStateFailed || !part.LastErrorCode.Valid || part.LastErrorCode.String != "checksum_mismatch" {
		t.Fatalf("failed part = %#v", part)
	}
}

func TestUploadPipelineRejectsLongBodyAndCompensates(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	session, err := catalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "long.bin", ExpectedSize: 4, PartSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
	_, err = pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: 4, Body: bytes.NewBufferString("abcde"),
	})
	if !errors.Is(err, transfer.ErrBodyTooLong) {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if len(storage.deleted) != 1 {
		t.Fatalf("expected compensation, deleted=%#v", storage.deleted)
	}
}

func seedTransferOwner(t testing.TB, pool *pgxpool.Pool, userID, channelID int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO channels (channel_id, user_id, name, selected) VALUES ($1, $2, 'storage', true)", channelID, userID); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
}

func checksumFor(data []byte) string {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(data)
	return treehash.SumToHex(treehash.ComputeTreeHash(hasher.Sum()))
}

func b1TransferSourceRoot(source []byte) string {
	root := blake3.New()
	for offset := 0; offset < len(source); offset += treehash.BlockSize {
		digest := blake3.Sum256(source[offset:min(offset+treehash.BlockSize, len(source))])
		_, _ = root.Write(digest[:])
	}
	return hex.EncodeToString(root.Sum(nil))
}

func b1TransferManifest(uploadID uuid.UUID, source []byte) uploads.CompleteInput {
	algorithm, value := "blake3", b1TransferSourceRoot(source)
	return uploads.CompleteInput{UserID: 1001, UploadID: uploadID, Size: int64(len(source)), HashAlgorithm: &algorithm, HashValue: &value}
}

type fixedResolver struct {
	channelID int64
	calls     int
}

func (r *fixedResolver) Resolve(_ context.Context, _ int64, requested int64) (int64, error) {
	r.calls++
	if requested != 0 {
		return requested, nil
	}
	return r.channelID, nil
}

type memoryStorage struct {
	mu            sync.Mutex
	uploads       [][]byte
	messages      map[int64][]byte
	deleted       []int64
	rangeRequests []telegramstore.RangeRequest
	nextID        int64
	sessionOpens  int
	sessionCloses int
}

func (s *memoryStorage) OpenDownloadSession(context.Context, int64) (telegramstore.DownloadSession, error) {
	s.mu.Lock()
	s.sessionOpens++
	s.mu.Unlock()
	return &memoryDownloadSession{storage: s}, nil
}

type memoryDownloadSession struct {
	storage *memoryStorage
	closed  bool
}

func (s *memoryDownloadSession) Metadata(_ context.Context, request telegramstore.MetadataRequest) (telegramstore.StoredPart, error) {
	s.storage.mu.Lock()
	defer s.storage.mu.Unlock()
	payload, ok := s.storage.messages[request.MessageID]
	if !ok {
		return telegramstore.StoredPart{}, telegramstore.ErrMessageNotFound
	}
	return telegramstore.StoredPart{ChannelID: request.ChannelID, MessageID: request.MessageID, Size: int64(len(payload))}, nil
}

func (s *memoryDownloadSession) OpenRange(ctx context.Context, request telegramstore.RangeRequest) (io.ReadCloser, error) {
	return s.storage.OpenRange(ctx, request)
}

func (s *memoryDownloadSession) Close() error {
	s.storage.mu.Lock()
	defer s.storage.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.storage.sessionCloses++
	}
	return nil
}

func (s *memoryStorage) Upload(_ context.Context, request telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	payload, err := io.ReadAll(request.Reader)
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	copyPayload := append([]byte(nil), payload...)
	s.uploads = append(s.uploads, copyPayload)
	if s.messages == nil {
		s.messages = make(map[int64][]byte)
	}
	s.messages[s.nextID] = copyPayload
	return telegramstore.StoredPart{ChannelID: request.ChannelID, MessageID: s.nextID, Size: int64(len(payload))}, nil
}

func (s *memoryStorage) OpenRange(_ context.Context, request telegramstore.RangeRequest) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.messages[request.MessageID]
	if !ok {
		return nil, telegramstore.ErrMessageNotFound
	}
	if request.Offset < 0 || request.Offset > int64(len(payload)) || request.Length < -1 {
		return nil, telegramstore.ErrInvalidRequest
	}
	end := int64(len(payload))
	if request.Length >= 0 && request.Offset+request.Length < end {
		end = request.Offset + request.Length
	}
	s.rangeRequests = append(s.rangeRequests, request)
	return io.NopCloser(bytes.NewReader(append([]byte(nil), payload[request.Offset:end]...))), nil
}

func (s *memoryStorage) DeleteMessages(_ context.Context, _ int64, _ int64, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, ids...)
	for _, id := range ids {
		delete(s.messages, id)
	}
	return nil
}

func (s *memoryStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("not implemented")
}
func (s *memoryStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, errors.New("not implemented")
}

func (s *memoryStorage) DeleteChannel(context.Context, int64, int64) error { return nil }

func (s *memoryStorage) payload(index int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.uploads[index]...)
}

func (s *memoryStorage) uploadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uploads)
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("retry body was read") }

type hiddenStoredCatalog struct {
	*uploads.Service
	hide bool
}

func (c *hiddenStoredCatalog) GetPart(ctx context.Context, userID int64, uploadID uuid.UUID, partNo int32) (*sqlcgen.UploadPart, error) {
	if c.hide {
		c.hide = false
		return nil, uploads.ErrNotFound
	}
	return c.Service.GetPart(ctx, userID, uploadID, partNo)
}

type acknowledgedBody struct {
	io.Reader
	bytes int
	eof   bool
}

func (r *acknowledgedBody) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	r.eof = r.eof || errors.Is(err, io.EOF)
	return n, err
}

func TestB1ReplayBodyIdentity(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	ctx := context.Background()
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	original := []byte("stored replay bytes")
	checksum := b1TransferSourceRoot(original)
	for _, route := range []string{"initial", "claim"} {
		for _, tc := range []struct {
			name  string
			body  []byte
			hash  *string
			want  error
			proof string
		}{
			{name: "equal", body: original},
			{name: "equal asserted", body: original, hash: &checksum},
			{name: "different", body: bytes.Repeat([]byte("x"), len(original)), want: uploads.ErrPartConflict},
			{name: "misleading checksum", body: bytes.Repeat([]byte("x"), len(original)), hash: &checksum, want: uploads.ErrPartConflict},
			{name: "short", body: original[:len(original)-1], want: transfer.ErrBodyTooShort},
			{name: "long", body: append(append([]byte(nil), original...), '!'), want: transfer.ErrBodyTooLong},
			{name: "missing checksum", body: original, proof: "missing checksum", want: uploads.ErrPartConflict},
			{name: "corrupt checksum", body: original, proof: "corrupt checksum", want: uploads.ErrPartConflict},
			{name: "missing blocks", body: original, proof: "missing blocks", want: uploads.ErrPartConflict},
			{name: "corrupt blocks", body: original, proof: "corrupt blocks", want: uploads.ErrPartConflict},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				session, err := catalog.Create(ctx, uploads.CreateInput{
					UserID: 1001, Name: uuid.NewString() + ".bin", ExpectedSize: int64(len(original)),
				})
				if err != nil {
					t.Fatal(err)
				}
				id, _ := dbtypes.GoogleUUID(session.ID)
				startCount := storage.uploadCount()
				initial := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
				winner, err := initial.UploadPart(ctx, transfer.UploadPartRequest{
					UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(original)), Body: bytes.NewReader(original),
				})
				if err != nil {
					t.Fatal(err)
				}
				if tc.proof != "" {
					var storedChecksum any = checksum
					blocks := winner.Part.BlockHashes
					switch tc.proof {
					case "missing checksum":
						storedChecksum = nil
					case "corrupt checksum":
						storedChecksum = "bad"
					case "missing blocks":
						blocks = nil
					case "corrupt blocks":
						blocks = make([]byte, treehash.DigestSize)
					}
					if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET checksum = $2, block_hashes = $3 WHERE upload_id = $1 AND part_no = 1", id, storedChecksum, blocks); err != nil {
						t.Fatal(err)
					}
				}
				before, err := catalog.GetPart(ctx, 1001, id, 1)
				if err != nil {
					t.Fatal(err)
				}
				body := &acknowledgedBody{Reader: bytes.NewReader(tc.body)}
				replay := transfer.NewPipeline(&hiddenStoredCatalog{Service: catalog, hide: route == "claim"}, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})
				got, err := replay.UploadPart(ctx, transfer.UploadPartRequest{
					UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(original)), Checksum: tc.hash, Body: body,
				})
				if !errors.Is(err, tc.want) {
					t.Fatalf("replay = %+v, %v, want %v", got, err, tc.want)
				}
				if tc.want == nil && (got == nil || !got.Existing || got.Part.MessageID != winner.Part.MessageID || body.bytes != len(original) || !body.eof) {
					t.Fatalf("replay did not prove exact body: result=%+v body=%+v", got, body)
				}
				if tc.want != nil && got != nil {
					t.Fatalf("failed replay acknowledged part: %+v", got)
				}
				part, err := catalog.GetPart(ctx, 1001, id, 1)
				if err != nil || !reflect.DeepEqual(part, before) || storage.uploadCount() != startCount+1 || len(storage.deleted) != 0 || !bytes.Equal(storage.payload(startCount), original) {
					t.Fatalf("replay changed stored part: %+v, %v, uploads=%d deletes=%v", part, err, storage.uploadCount(), storage.deleted)
				}
				session, err = catalog.Get(ctx, 1001, id)
				if err != nil || session.State != sqlcgen.UploadStateOpen || session.FileID.Valid {
					t.Fatalf("replay published a file: %+v, %v", session, err)
				}
			})
		}
	}
}

func TestB1HashDisabledProof(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	ctx := context.Background()
	storage := &memoryStorage{}
	catalog, policy := transferFixtureUploads(t, db.Pool, storage)
	body := []byte("proof remains mandatory")
	checksum := b1TransferSourceRoot(body)
	for _, mode := range []string{"unhashed", "asserted part", "expected file", "existing hashed"} {
		t.Run(mode, func(t *testing.T) {
			input := uploads.CreateInput{UserID: 1001, Name: uuid.NewString() + ".bin", ExpectedSize: int64(len(body))}
			if mode == "expected file" {
				algorithm := "blake3"
				input.ExpectedHashAlgorithm, input.ExpectedHashValue = &algorithm, &checksum
			}
			session, err := catalog.Create(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			id, _ := dbtypes.GoogleUUID(session.ID)
			startCount := storage.uploadCount()
			pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{DisableHashing: mode != "existing hashed", Receipts: policy})
			var asserted *string
			if mode == "asserted part" {
				asserted = &checksum
			}
			winner, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{
				UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(body)), Checksum: asserted, Body: bytes.NewReader(body),
			})
			if err != nil {
				t.Fatal(err)
			}
			wantProof := mode != "unhashed"
			if winner.Part.Checksum.Valid != wantProof || (len(winner.Part.BlockHashes) > 0) != wantProof {
				t.Fatalf("fresh hash policy = %+v, want proof=%t", winner.Part, wantProof)
			}
			replay := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{DisableHashing: true})
			for _, hash := range []*string{nil, &checksum} {
				current := &acknowledgedBody{Reader: bytes.NewReader(body)}
				got, err := replay.UploadPart(ctx, transfer.UploadPartRequest{
					UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(body)), Checksum: hash, Body: current,
				})
				if wantProof {
					if err != nil || got == nil || !got.Existing || current.bytes != len(body) || !current.eof {
						t.Fatalf("hashed replay = %+v, %v body=%+v", got, err, current)
					}
				} else if !errors.Is(err, uploads.ErrPartConflict) || got != nil {
					t.Fatalf("unhashed replay accepted without proof: %+v, %v", got, err)
				}
			}
			part, err := catalog.GetPart(ctx, 1001, id, 1)
			if err != nil || !reflect.DeepEqual(part, winner.Part) || storage.uploadCount() != startCount+1 || len(storage.deleted) != 0 {
				t.Fatalf("hash-disabled replay changed stored part: %+v, %v", part, err)
			}
			manifest := uploads.CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(body))}
			if wantProof {
				manifest = b1TransferManifest(id, body)
			}
			file, err := catalog.Complete(ctx, manifest)
			if err != nil || file.HashValue.Valid != wantProof {
				t.Fatalf("fresh completion = %+v, %v", file, err)
			}
			if !bytes.Equal(storage.payload(startCount), body) {
				t.Fatalf("hash-disabled mode %s changed original bytes", mode)
			}
		})
	}
}

type heldUploadStorage struct {
	*memoryStorage
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *heldUploadStorage) Upload(ctx context.Context, request telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return s.memoryStorage.Upload(ctx, request)
	case <-ctx.Done():
		return telegramstore.StoredPart{}, context.Cause(ctx)
	}
}

type releasedReader struct {
	io.Reader
	ready <-chan struct{}
}

func (r releasedReader) Read(p []byte) (int, error) {
	select {
	case <-r.ready:
		return r.Reader.Read(p)
	default:
		return 0, errors.New("waiter body read before its claim settled")
	}
}

func TestB1ReplayBodyIdentityConcurrent(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	memory := &memoryStorage{}
	for _, scenario := range []string{"equal", "different", "misleading checksum", "cancelled waiter"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			service, policy := transferFixtureUploads(t, db.Pool, memory)
			catalog := &observedBusyCatalog{Service: service, busy: make(chan struct{}), resume: make(chan struct{})}
			original := []byte("data")
			checksum := b1TransferSourceRoot(original)
			session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: uuid.NewString() + ".bin", ExpectedSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			id, _ := dbtypes.GoogleUUID(session.ID)
			release := make(chan struct{})
			storage := &heldUploadStorage{memoryStorage: memory, started: make(chan struct{}), release: release}
			pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
			type outcome struct {
				result *transfer.UploadPartResult
				err    error
			}
			startCount := memory.uploadCount()
			winnerDone := make(chan outcome, 1)
			go func() {
				result, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{
					UserID: 1001, UploadID: id, PartNo: 1, PlainSize: 4, Body: bytes.NewReader(original),
				})
				winnerDone <- outcome{result, err}
			}()
			select {
			case <-storage.started:
			case <-ctx.Done():
				t.Fatal("winner never reached its upload")
			}
			winnerLease, err := catalog.GetPart(ctx, 1001, id, 1)
			if err != nil || !winnerLease.LeaseToken.Valid {
				t.Fatalf("missing winning lease: %+v, %v", winnerLease, err)
			}
			current := original
			var asserted *string
			if scenario == "different" || scenario == "misleading checksum" {
				current = []byte("else")
			}
			if scenario == "misleading checksum" {
				asserted = &checksum
			}
			waiterBody := &acknowledgedBody{Reader: bytes.NewReader(current)}
			waitCtx, stopWait := context.WithCancel(ctx)
			defer stopWait()
			waiterDone := make(chan outcome, 1)
			go func() {
				result, err := pipeline.UploadPart(waitCtx, transfer.UploadPartRequest{
					UserID: 1001, UploadID: id, PartNo: 1, PlainSize: 4, Checksum: asserted,
					Body: releasedReader{Reader: waiterBody, ready: catalog.resume},
				})
				waiterDone <- outcome{result, err}
			}()
			select {
			case <-catalog.busy:
			case <-ctx.Done():
				t.Fatal("waiter never reached the winning lease")
			}
			var waiter outcome
			if scenario == "cancelled waiter" {
				stopWait()
				select {
				case waiter = <-waiterDone:
				case <-ctx.Done():
					t.Fatal("cancelled waiter did not finish")
				}
				part, err := catalog.GetPart(ctx, 1001, id, 1)
				if !errors.Is(waiter.err, context.Canceled) || waiter.result != nil || waiterBody.bytes != 0 || !reflect.DeepEqual(part, winnerLease) || err != nil {
					t.Fatalf("cancelled waiter disturbed winner: result=%+v error=%v part=%+v lookup=%v", waiter.result, waiter.err, part, err)
				}
			}
			close(release)
			var winner outcome
			select {
			case winner = <-winnerDone:
			case <-ctx.Done():
				t.Fatal("winner did not finish")
			}
			if winner.err != nil || winner.result == nil || winner.result.Existing {
				t.Fatalf("winner = %+v, %v", winner.result, winner.err)
			}
			close(catalog.resume)
			if scenario != "cancelled waiter" {
				select {
				case waiter = <-waiterDone:
				case <-ctx.Done():
					t.Fatal("waiter did not finish after the winner")
				}
				if scenario == "equal" {
					if waiter.err != nil || waiter.result == nil || !waiter.result.Existing || waiter.result.Part.MessageID != winner.result.Part.MessageID {
						t.Fatalf("equal waiter = %+v, %v", waiter.result, waiter.err)
					}
				} else if !errors.Is(waiter.err, uploads.ErrPartConflict) || waiter.result != nil {
					t.Fatalf("different waiter = %+v, %v", waiter.result, waiter.err)
				}
				if waiterBody.bytes != len(current) || !waiterBody.eof {
					t.Fatalf("waiter body not proved: %+v", waiterBody)
				}
			}
			part, err := catalog.GetPart(ctx, 1001, id, 1)
			if err != nil || !reflect.DeepEqual(part, winner.result.Part) || memory.uploadCount() != startCount+1 || len(memory.deleted) != 0 || !bytes.Equal(memory.payload(startCount), original) {
				t.Fatalf("waiter changed winner bytes: part=%+v error=%v uploads=%d deletes=%v", part, err, memory.uploadCount(), memory.deleted)
			}
		})
	}
}

type transferFixtureReceiptArgs struct {
	UserID     int64       `json:"user_id"`
	ReceiptIDs []uuid.UUID `json:"receipt_ids"`
}

func (transferFixtureReceiptArgs) Kind() string { return "transfer_fixture_receipt_cleanup" }

type transferFixtureReceiptQueue struct{ client *river.Client[pgx.Tx] }

func (q transferFixtureReceiptQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	result, err := q.client.InsertTx(ctx, tx, transferFixtureReceiptArgs{UserID: in.UserID, ReceiptIDs: in.ReceiptIDs}, &river.InsertOpts{
		MaxAttempts: 3, Metadata: []byte(fmt.Sprintf(`{"user_id":%d}`, in.UserID)),
	})
	if err != nil {
		return 0, err
	}
	if result == nil || result.Job == nil || result.Job.ID <= 0 {
		return 0, errors.New("missing actual River job ID")
	}
	return result.Job.ID, nil
}

func transferFixtureUploads(t testing.TB, pool *pgxpool.Pool, storage telegramstore.Storage) (*uploads.Service, *lifecycle.Service) {
	t.Helper()
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: "teldrive", SkipUnknownJobCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	policy := lifecycle.NewService(pool, storage)
	if err := policy.BindReceiptEnqueuer(transferFixtureReceiptQueue{client: client}); err != nil {
		t.Fatal(err)
	}
	service := uploads.NewService(pool)
	if err := service.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	return service, policy
}

func transferFixtureReceipt(t testing.TB, ctx context.Context, policy *lifecycle.Service, storage *memoryStorage, body []byte, uploadID uuid.UUID, claim *uploads.ClaimPartResult, messageID int64) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := policy.Reserve(ctx, lifecycle.ReserveInput{
		ID: id, UserID: 1001, ChannelID: claim.Part.ChannelID, OriginKind: "upload", UploadID: &uploadID,
		PartNo: claim.Part.PartNo, LeaseToken: claim.LeaseToken, LeaseExpiresAt: claim.Part.LeaseExpiresAt.Time,
	}); err != nil {
		t.Fatal(err)
	}
	storage.mu.Lock()
	if storage.messages == nil {
		storage.messages = make(map[int64][]byte)
	}
	storage.messages[messageID] = append([]byte(nil), body...)
	storage.mu.Unlock()
	if _, err := policy.Observe(ctx, lifecycle.ObserveInput{
		ReceiptID: id, UserID: 1001, ChannelID: claim.Part.ChannelID, MessageID: messageID,
		UploadID: &uploadID, PartNo: claim.Part.PartNo, LeaseToken: claim.LeaseToken,
	}); err != nil {
		t.Fatal(err)
	}
	return id
}
