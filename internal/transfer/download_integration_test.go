//go:build integration

package transfer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestDownloaderReadsAcrossTelegramParts(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	fileCatalog := catalog.NewService(db.Pool, nil)
	storage := &memoryStorage{}
	uploadCatalog, policy := transferFixtureUploads(t, db.Pool, storage)
	if err := fileCatalog.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	pipeline := transfer.NewPipeline(uploadCatalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
	source := append(bytes.Repeat([]byte("abcdefgh"), treehash.BlockSize/4), []byte("ij")...)
	sourceSHA256 := sha256.Sum256(source)

	session, err := uploadCatalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "parts.bin", ExpectedSize: int64(len(source)), PartSize: treehash.BlockSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	for partNo, body := range [][]byte{source[:treehash.BlockSize], source[treehash.BlockSize : 2*treehash.BlockSize], source[2*treehash.BlockSize:]} {
		if _, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
			UserID: 1001, UploadID: uploadID, PartNo: int32(partNo + 1), PlainSize: int64(len(body)), Body: bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("upload part %d: %v", partNo+1, err)
		}
	}
	file, err := uploadCatalog.Complete(context.Background(), b1TransferManifest(uploadID, source))
	if err != nil {
		t.Fatal(err)
	}
	fileID, _ := dbtypes.GoogleUUID(file.ID)

	downloader := transfer.NewDownloader(fileCatalog, storage, nil)
	offset, length := int64(treehash.BlockSize-1), int64(treehash.BlockSize+2)
	download, err := downloader.Open(context.Background(), transfer.DownloadRequest{
		UserID: 1001, FileID: fileID, Offset: offset, Length: length,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer download.Reader.Close()
	got, err := io.ReadAll(download.Reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, source[offset:offset+length]) {
		t.Fatalf("cross-part range mismatch: got %d bytes", len(got))
	}
	if download.Offset != offset || download.Length != length || download.TotalSize != int64(len(source)) {
		t.Fatalf("download metadata = %#v", download)
	}

	storage.mu.Lock()
	requests := append([]telegramstore.RangeRequest(nil), storage.rangeRequests...)
	storage.mu.Unlock()
	if len(requests) != 3 || requests[0].Offset != offset || requests[0].Length != 1 || requests[1].Offset != 0 || requests[1].Length != treehash.BlockSize || requests[2].Offset != 0 || requests[2].Length != 1 {
		t.Fatalf("Telegram range requests = %#v", requests)
	}
	full, err := downloader.Open(context.Background(), transfer.DownloadRequest{UserID: 1001, FileID: fileID, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer full.Reader.Close()
	reconstructed, err := io.ReadAll(full.Reader)
	if err != nil || len(reconstructed) != len(source) || sha256.Sum256(reconstructed) != sourceSHA256 {
		t.Fatalf("full reconstruction = %d bytes, %v", len(reconstructed), err)
	}
}

func TestDownloaderDecryptsRangeAcrossCipherBlocks(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	fileCatalog := catalog.NewService(db.Pool, nil)
	storage := &memoryStorage{}
	uploadCatalog, policy := transferFixtureUploads(t, db.Pool, storage)
	if err := fileCatalog.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	keyVersion := int32(3)
	keys := transfer.StaticKeyProvider{3: "download-compatible-key"}
	pipeline := transfer.NewPipeline(uploadCatalog, &fixedResolver{channelID: 9001}, storage, keys, transfer.Config{
		Receipts: policy,
		Random:   bytes.NewReader(bytes.Repeat([]byte{4}, 32)),
	})
	body := bytes.Repeat([]byte("0123456789abcdef"), 9000)
	session, err := uploadCatalog.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "encrypted-range.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
		Encryption: true, EncryptionKeyVersion: &keyVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	if _, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	file, err := uploadCatalog.Complete(context.Background(), b1TransferManifest(uploadID, body))
	if err != nil {
		t.Fatal(err)
	}
	fileID, _ := dbtypes.GoogleUUID(file.ID)

	offset := int64(65536 - 17)
	length := int64(100)
	downloader := transfer.NewDownloader(fileCatalog, storage, keys)
	download, err := downloader.Open(context.Background(), transfer.DownloadRequest{
		UserID: 1001, FileID: fileID, Offset: offset, Length: length,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer download.Reader.Close()
	got, err := io.ReadAll(download.Reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, body[offset:offset+length]) {
		t.Fatalf("decrypted range mismatch: got %d bytes", len(got))
	}
	if err := download.Reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	storage.mu.Lock()
	requests := append([]telegramstore.RangeRequest(nil), storage.rangeRequests...)
	ciphertextSize := len(storage.uploads[0])
	sessionOpens, sessionCloses := storage.sessionOpens, storage.sessionCloses
	storage.mu.Unlock()
	if sessionOpens != 1 || sessionCloses != 1 {
		t.Fatalf("encrypted download sessions = opened %d, closed %d, want 1/1", sessionOpens, sessionCloses)
	}
	if len(requests) == 0 {
		t.Fatal("expected Telegram range request")
	}
	for _, request := range requests {
		if request.Length >= int64(ciphertextSize) {
			t.Fatalf("encrypted range fetched whole object: %#v", request)
		}
	}
}

func TestB1CompletionAtomicityPayload(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedTransferOwner(t, db.Pool, 1001, 9001)
	fileCatalog := catalog.NewService(db.Pool, nil)
	storage := &memoryStorage{}
	uploadCatalog, policy := transferFixtureUploads(t, db.Pool, storage)
	if err := fileCatalog.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	pipeline := transfer.NewPipeline(uploadCatalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{Receipts: policy})
	oldBytes := []byte("old destination bytes remain readable")
	oldSHA256 := sha256.Sum256(oldBytes)
	session, err := uploadCatalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "atomic.bin", ExpectedSize: int64(len(oldBytes))})
	if err != nil {
		t.Fatal(err)
	}
	oldUploadID, _ := dbtypes.GoogleUUID(session.ID)
	oldPart, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: oldUploadID, PartNo: 1, PlainSize: int64(len(oldBytes)), Body: bytes.NewReader(oldBytes)})
	if err != nil {
		t.Fatal(err)
	}
	old, err := uploadCatalog.Complete(ctx, b1TransferManifest(oldUploadID, oldBytes))
	if err != nil {
		t.Fatal(err)
	}
	fileID, _ := dbtypes.GoogleUUID(old.ID)
	shareID := uuid.New()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO file_shares(id,file_id,owner_id,token_prefix,token_hash) VALUES($1,$2,1001,'b1',$3)", shareID, fileID, []byte(shareID.String())); err != nil {
		t.Fatal(err)
	}
	newBytes := []byte("different replacement bytes")
	replacement, err := uploadCatalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "atomic.bin", ExpectedSize: int64(len(newBytes)), ConflictPolicy: sqlcgen.NameConflictPolicyReplace})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := dbtypes.GoogleUUID(replacement.ID)
	if _, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(newBytes)), Body: bytes.NewReader(newBytes)}); err != nil {
		t.Fatal(err)
	}
	manifest := b1TransferManifest(id, newBytes)
	manifest.HashValue = new(strings.Repeat("ab", 32))
	eventCount := func() int {
		t.Helper()
		var count int
		if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE user_id=1001 AND resource_id IN ($1,$2,$3)", fileID.String(), shareID.String(), id.String()).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	before := eventCount()
	if _, err := uploadCatalog.Complete(ctx, manifest); !errors.Is(err, uploads.ErrHashMismatch) {
		t.Fatalf("bad replacement error = %v", err)
	}
	after := eventCount()
	current, err := fileCatalog.Get(ctx, 1001, fileID)
	if err != nil || current.Generation != old.Generation || current.Status != sqlcgen.FileStatusActive {
		t.Fatalf("old destination = %+v, %v", current, err)
	}
	var active bool
	if err := db.Pool.QueryRow(ctx, "SELECT revoked_at IS NULL FROM file_shares WHERE id=$1", shareID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if !active || before != after {
		t.Fatalf("replacement changed share/events: active=%v events=%d/%d", active, before, after)
	}
	parts, err := sqlcgen.New(db.Pool).ListFileParts(ctx, old.ID)
	if err != nil || len(parts) != 1 || parts[0].MessageID != oldPart.Part.MessageID.Int64 || parts[0].ChannelID != oldPart.Part.ChannelID {
		t.Fatalf("old message references = %+v, %v", parts, err)
	}
	download, err := transfer.NewDownloader(fileCatalog, storage, nil).Open(ctx, transfer.DownloadRequest{UserID: 1001, FileID: fileID, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer download.Reader.Close()
	body, err := io.ReadAll(download.Reader)
	if err != nil || len(body) != len(oldBytes) || sha256.Sum256(body) != oldSHA256 {
		t.Fatalf("old download changed: %d bytes, %v", len(body), err)
	}
	storage.mu.Lock()
	deleted := len(storage.deleted)
	storage.mu.Unlock()
	if deleted != 0 {
		t.Fatalf("rejected completion deleted %d messages", deleted)
	}
}
