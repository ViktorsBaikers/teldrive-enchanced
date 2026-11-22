//go:build integration

package uploads_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestUploadPreservesExactName(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	name := "  Cafe\u0301  " + strings.Repeat("x", 300)

	svc, _ := newUploadFixtureService(t, db.Pool)
	session, err := svc.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: name, ExpectedSize: 0,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if session.Name != name {
		t.Fatalf("session name = %q, want exact %q", session.Name, name)
	}
}

func TestUploadLifecycleAgainstRealPostgres(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, policy := newUploadFixtureService(t, db.Pool)

	source := append(bytes.Repeat([]byte("abcdefgh"), treehash.BlockSize/4), []byte("ij")...)
	firstHashes, hash := hashMetadata(source[:treehash.BlockSize])
	session, err := svc.Create(ctx, uploads.CreateInput{
		UserID:         1001,
		Name:           "video.bin",
		ExpectedSize:   int64(len(source)),
		PartSize:       treehash.BlockSize,
		ConflictPolicy: sqlcgen.NameConflictPolicyFail,
	})
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}
	uploadID := mustUploadUUID(t, session.ID)

	first, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: treehash.BlockSize, Checksum: &hash,
	})
	if err != nil {
		t.Fatalf("claim first part: %v", err)
	}
	if first.Existing || first.LeaseToken == uuid.Nil {
		t.Fatalf("first claim = %#v", first)
	}
	if err := svc.RenewPart(ctx, uploads.RenewPartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: uuid.New(),
	}); !errors.Is(err, uploads.ErrLeaseLost) {
		t.Fatalf("wrong lease renewal error = %v, want ErrLeaseLost", err)
	}
	if err := svc.RenewPart(ctx, uploads.RenewPartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: first.LeaseToken,
	}); err != nil {
		t.Fatalf("renew first part: %v", err)
	}
	if _, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: treehash.BlockSize, Checksum: &hash,
	}); !errors.Is(err, uploads.ErrPartBusy) {
		t.Fatalf("active lease error = %v, want ErrPartBusy", err)
	}
	receiptID := observeUploadFixturePart(t, ctx, policy, uploadID, 1, first.LeaseToken, 101)
	if _, err := svc.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, ReceiptID: receiptID, PartNo: 1, LeaseToken: uuid.New(), MessageID: 101, StoredSize: treehash.BlockSize,
		Checksum: hash, BlockHashes: firstHashes,
	}); !errors.Is(err, uploads.ErrLeaseLost) {
		t.Fatalf("wrong lease error = %v, want ErrLeaseLost", err)
	}
	if _, err := svc.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, ReceiptID: receiptID, PartNo: 1, LeaseToken: first.LeaseToken, MessageID: 101, StoredSize: treehash.BlockSize,
		Checksum: hash, BlockHashes: firstHashes,
	}); err != nil {
		t.Fatalf("store first part: %v", err)
	}

	retry, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: treehash.BlockSize, Checksum: &hash,
	})
	if err != nil || !retry.Existing || retry.LeaseToken != uuid.Nil {
		t.Fatalf("exact retry = %#v, %v", retry, err)
	}
	_, otherHash := hashMetadata([]byte("wxyz"))
	if _, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: treehash.BlockSize, Checksum: &otherHash,
	}); !errors.Is(err, uploads.ErrPartConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrPartConflict", err)
	}

	if _, err := svc.Complete(ctx, b1Manifest(uploadID, source)); !errors.Is(err, uploads.ErrIncomplete) {
		t.Fatalf("incomplete completion error = %v, want ErrIncomplete", err)
	}
	stillOpen, err := svc.Get(ctx, 1001, uploadID)
	if err != nil || stillOpen.State != sqlcgen.UploadStateOpen {
		t.Fatalf("session after rolled-back completion = %#v, %v", stillOpen, err)
	}

	claimAndStore(t, ctx, svc, uploadID, 2, source[treehash.BlockSize:2*treehash.BlockSize], 102, policy)
	claimAndStore(t, ctx, svc, uploadID, 3, source[2*treehash.BlockSize:], 103, policy)
	file, err := svc.Complete(ctx, b1Manifest(uploadID, source))
	if err != nil {
		t.Fatalf("complete upload: %v", err)
	}
	fileID := mustUploadUUID(t, file.ID)
	if file.Name != "video.bin" || !file.Size.Valid || file.Size.Int64 != int64(len(source)) {
		t.Fatalf("completed file = %#v", file)
	}
	expectedFileHash := b1SourceRoot(source)
	if !file.HashAlgorithm.Valid || file.HashAlgorithm.String != string(treehash.TypeBlake3) || !file.HashValue.Valid || file.HashValue.String != expectedFileHash {
		t.Fatalf("completed file hash = (%#v, %#v), want blake3:%s", file.HashAlgorithm, file.HashValue, expectedFileHash)
	}

	var partCount int
	var totalSize int64
	if err := db.Pool.QueryRow(ctx, "SELECT count(*), sum(plain_size) FROM file_parts WHERE file_id = $1", fileID).Scan(&partCount, &totalSize); err != nil {
		t.Fatalf("read finalized parts: %v", err)
	}
	if partCount != 3 || totalSize != int64(len(source)) {
		t.Fatalf("finalized parts = count %d, size %d", partCount, totalSize)
	}
	var persistedFirstHashes []byte
	if err := db.Pool.QueryRow(ctx, "SELECT block_hashes FROM file_parts WHERE file_id = $1 AND part_no = 1", fileID).Scan(&persistedFirstHashes); err != nil {
		t.Fatalf("read finalized block hashes: %v", err)
	}
	if !bytes.Equal(persistedFirstHashes, firstHashes) {
		t.Fatalf("persisted block hashes changed: %x", persistedFirstHashes)
	}

	again, err := svc.Complete(ctx, b1Manifest(uploadID, source))
	if err != nil || mustUploadUUID(t, again.ID) != fileID {
		t.Fatalf("idempotent completion = %#v, %v", again, err)
	}
	if _, err := svc.Abort(ctx, 1001, uploadID); !errors.Is(err, uploads.ErrInvalidState) {
		t.Fatalf("abort completed upload error = %v", err)
	}
}

func TestUnknownSizeUploadDerivesSizeOnCompletion(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, policy := newUploadFixtureService(t, db.Pool)
	source := append(bytes.Repeat([]byte("abcd"), treehash.BlockSize/4), []byte("ef")...)

	session, err := svc.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: "stream.bin", ExpectedSize: -1, PartSize: treehash.BlockSize,
	})
	if err != nil {
		t.Fatalf("create unknown-size upload: %v", err)
	}
	uploadID := mustUploadUUID(t, session.ID)
	claimAndStore(t, ctx, svc, uploadID, 1, source[:treehash.BlockSize], 201, policy)
	claimAndStore(t, ctx, svc, uploadID, 2, source[treehash.BlockSize:], 202, policy)

	file, err := svc.Complete(ctx, b1Manifest(uploadID, source))
	if err != nil {
		t.Fatalf("complete unknown-size upload: %v", err)
	}
	if !file.Size.Valid || file.Size.Int64 != int64(len(source)) {
		t.Fatalf("completed file size = %#v, want %d", file.Size, len(source))
	}
	completed, err := svc.Get(ctx, 1001, uploadID)
	if err != nil {
		t.Fatalf("get completed upload: %v", err)
	}
	if completed.ExpectedSize != int64(len(source)) {
		t.Fatalf("completed upload expected size = %d, want %d", completed.ExpectedSize, len(source))
	}

	invalid, err := svc.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: "invalid-stream.bin", ExpectedSize: -1, PartSize: treehash.BlockSize,
	})
	if err != nil {
		t.Fatalf("create invalid unknown-size upload: %v", err)
	}
	invalidID := mustUploadUUID(t, invalid.ID)
	claimAndStore(t, ctx, svc, invalidID, 1, []byte("ab"), 203, policy)
	claimAndStore(t, ctx, svc, invalidID, 2, bytes.Repeat([]byte("cdef"), treehash.BlockSize/4), 204, policy)
	if _, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: invalidID, Size: treehash.BlockSize + 2}); !errors.Is(err, uploads.ErrIncomplete) {
		t.Fatalf("non-final short part error = %v, want ErrIncomplete", err)
	}
}

func TestUploadAbortZeroByteAndNameConflict(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, _ := newUploadFixtureService(t, db.Pool)

	abortedSession, err := svc.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "aborted", ExpectedSize: 1, PartSize: 1})
	if err != nil {
		t.Fatalf("create aborted upload: %v", err)
	}
	abortedID := mustUploadUUID(t, abortedSession.ID)
	aborted, err := svc.Abort(ctx, 1001, abortedID)
	if err != nil || aborted.State != sqlcgen.UploadStateAborted {
		t.Fatalf("abort upload = %#v, %v", aborted, err)
	}
	if repeated, err := svc.Abort(ctx, 1001, abortedID); err != nil || repeated.State != sqlcgen.UploadStateAborted {
		t.Fatalf("repeat abort = %#v, %v", repeated, err)
	}

	zero, err := svc.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "empty.txt", ExpectedSize: 0})
	if err != nil {
		t.Fatalf("create zero-byte upload: %v", err)
	}
	zeroID := mustUploadUUID(t, zero.ID)
	if _, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: zeroID, PartNo: 1, ChannelID: 9001}); !errors.Is(err, uploads.ErrInvalidInput) {
		t.Fatalf("zero-byte part error = %v", err)
	}
	if _, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: zeroID, Size: 0}); err != nil {
		t.Fatalf("complete zero-byte upload: %v", err)
	}

	caseDistinct, err := svc.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "EMPTY.TXT", ExpectedSize: 0})
	if err != nil {
		t.Fatalf("create case-distinct session: %v", err)
	}
	if completed, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: mustUploadUUID(t, caseDistinct.ID), Size: 0}); err != nil || completed.Name != "EMPTY.TXT" {
		t.Fatalf("complete case-distinct upload = %#v, %v", completed, err)
	}

	conflict, err := svc.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "empty.txt", ExpectedSize: 0})
	if err != nil {
		t.Fatalf("create exact conflicting session: %v", err)
	}
	conflictID := mustUploadUUID(t, conflict.ID)
	if _, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: conflictID, Size: 0}); !errors.Is(err, uploads.ErrNameConflict) {
		t.Fatalf("exact name conflict error = %v, want ErrNameConflict", err)
	}
	open, err := svc.Get(ctx, 1001, conflictID)
	if err != nil || open.State != sqlcgen.UploadStateOpen {
		t.Fatalf("conflicting session should remain open: %#v, %v", open, err)
	}
}

func TestUploadConflictPoliciesAgainstRealPostgres(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, _ := newUploadFixtureService(t, db.Pool)

	originalSession, err := svc.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: "report.txt", ExpectedSize: 0,
	})
	if err != nil {
		t.Fatalf("create original upload: %v", err)
	}
	original, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: mustUploadUUID(t, originalSession.ID), Size: 0})
	if err != nil {
		t.Fatalf("complete original upload: %v", err)
	}
	originalID := mustUploadUUID(t, original.ID)

	renameSession, err := svc.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: "report.txt", ExpectedSize: 0,
		ConflictPolicy: sqlcgen.NameConflictPolicyRename,
	})
	if err != nil {
		t.Fatalf("create rename upload: %v", err)
	}
	renamed, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: mustUploadUUID(t, renameSession.ID), Size: 0})
	if err != nil {
		t.Fatalf("complete rename upload: %v", err)
	}
	if renamed.Name != "report (1).txt" {
		t.Fatalf("renamed file name = %q, want report (1).txt", renamed.Name)
	}

	replaceSession, err := svc.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: "report.txt", ExpectedSize: 0,
		ConflictPolicy: sqlcgen.NameConflictPolicyReplace,
	})
	if err != nil {
		t.Fatalf("create replace upload: %v", err)
	}
	replacement, err := svc.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: mustUploadUUID(t, replaceSession.ID), Size: 0})
	if err != nil {
		t.Fatalf("complete replace upload: %v", err)
	}
	if replacement.Name != "report.txt" || mustUploadUUID(t, replacement.ID) == originalID {
		t.Fatalf("replacement file = %#v", replacement)
	}

	var originalStatus sqlcgen.FileStatus
	if err := db.Pool.QueryRow(ctx, "SELECT status FROM files WHERE id=$1", originalID).Scan(&originalStatus); err != nil {
		t.Fatalf("read replaced file status: %v", err)
	}
	if originalStatus != sqlcgen.FileStatusDeletionPending {
		t.Fatalf("original status = %q, want deletion_pending", originalStatus)
	}
	var activeCount int
	if err := db.Pool.QueryRow(ctx, `
SELECT count(*)
FROM files
WHERE user_id=1001 AND parent_id IS NULL AND status='active'`).Scan(&activeCount); err != nil {
		t.Fatalf("count active files: %v", err)
	}
	if activeCount != 2 {
		t.Fatalf("active file count = %d, want 2", activeCount)
	}
}

func claimAndStore(t testing.TB, ctx context.Context, svc *uploads.Service, uploadID uuid.UUID, partNo int32, data []byte, messageID int64, policy *lifecycle.Service) []byte {
	t.Helper()
	blockHashes, checksum := hashMetadata(data)
	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: partNo, ChannelID: 9001, PlainSize: int64(len(data)), Checksum: &checksum,
	})
	if err != nil {
		t.Fatalf("claim part %d: %v", partNo, err)
	}
	receiptID := observeUploadFixturePart(t, ctx, policy, uploadID, partNo, claim.LeaseToken, messageID)
	if _, err := svc.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, ReceiptID: receiptID, PartNo: partNo, LeaseToken: claim.LeaseToken, MessageID: messageID, StoredSize: int64(len(data)),
		Checksum: checksum, BlockHashes: blockHashes,
	}); err != nil {
		t.Fatalf("store part %d: %v", partNo, err)
	}
	return blockHashes
}

func hashMetadata(data []byte) ([]byte, string) {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(data)
	blockHashes := hasher.Sum()
	return blockHashes, treehash.SumToHex(treehash.ComputeTreeHash(blockHashes))
}

func seedUploadOwner(t testing.TB, db *pgxpool.Pool, userID, channelID int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
		t.Fatalf("seed upload user: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO channels (channel_id, user_id, name, selected) VALUES ($1, $2, 'storage', true)", channelID, userID); err != nil {
		t.Fatalf("seed upload channel: %v", err)
	}
}

func mustUploadUUID(t testing.TB, value pgtype.UUID) uuid.UUID {
	t.Helper()
	id, ok := dbtypes.GoogleUUID(value)
	if !ok {
		t.Fatal("expected UUID value")
	}
	return id
}

type uploadFixtureCleanupArgs struct {
	UserID     int64       `json:"user_id"`
	ReceiptIDs []uuid.UUID `json:"receipt_ids"`
}

func (uploadFixtureCleanupArgs) Kind() string { return "upload_metadata_fixture_cleanup" }

type uploadFixtureQueue struct{ client *river.Client[pgx.Tx] }

func (q uploadFixtureQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	result, err := q.client.InsertTx(ctx, tx, uploadFixtureCleanupArgs{UserID: in.UserID, ReceiptIDs: in.ReceiptIDs}, &river.InsertOpts{
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

func newUploadFixtureService(t testing.TB, pool *pgxpool.Pool) (*uploads.Service, *lifecycle.Service) {
	t.Helper()
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: "teldrive", SkipUnknownJobCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	policy := lifecycle.NewService(pool, nil)
	if err := policy.BindReceiptEnqueuer(uploadFixtureQueue{client: client}); err != nil {
		t.Fatal(err)
	}
	svc := uploads.NewService(pool)
	if err := svc.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	return svc, policy
}

// These are positively known synthetic metadata fixtures, not historical ownership or Pipeline byte proof.
func observeUploadFixturePart(t testing.TB, ctx context.Context, policy *lifecycle.Service, uploadID uuid.UUID, partNo int32, token uuid.UUID, messageID int64) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := policy.Reserve(ctx, lifecycle.ReserveInput{
		ID: id, UserID: 1001, ChannelID: 9001, OriginKind: "upload", UploadID: &uploadID,
		PartNo: partNo, LeaseToken: token, LeaseExpiresAt: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatalf("reserve synthetic upload receipt: %v", err)
	}
	if _, err := policy.Observe(ctx, lifecycle.ObserveInput{
		ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: messageID,
		UploadID: &uploadID, PartNo: partNo, LeaseToken: token,
	}); err != nil {
		t.Fatalf("observe synthetic upload receipt: %v", err)
	}
	return id
}
