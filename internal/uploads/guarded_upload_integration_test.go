//go:build integration

package uploads_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestB3GuardedChangedSlotRejectedAfterPreflight(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, policy := newUploadFixtureService(t, db.Pool)
	incarnation := recoveryIncarnation(t, db.Pool)
	source := guardedSource("guarded conflict")

	// External winner commits between a successful preflight and Complete.
	name := uuid.NewString() + ".bin"
	session := guardedCreate(t, svc, nil, nil, name, int64(len(source)), incarnation)
	uploadID := mustUploadUUID(t, session.ID)
	guardedClaimAndStore(t, ctx, svc, policy, uploadID, source, 501, incarnation)
	origin, err := svc.VerifyGuardedPart(ctx, 1001, uploadID, incarnation)
	if err != nil || len(origin.ParentSegments) != 0 {
		t.Fatalf("preflight = %+v, %v", origin, err)
	}
	winnerID := insertExternalFile(t, db.Pool, nil, name, int64(len(source)))
	completeAdmission := completeAdmissionKey()
	if _, err := svc.Complete(ctx, guardedManifest(uploadID, source, completeAdmission, incarnation)); !errors.Is(err, uploads.ErrGuardConflict) {
		t.Fatalf("changed slot completion = %v, want ErrGuardConflict", err)
	}
	assertExternalFile(t, db.Pool, winnerID, name, int64(len(source)))
	stored, err := sqlcgen.New(db.Pool).GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UploadID: session.ID, UserID: 1001})
	if err != nil || stored.State != sqlcgen.UploadStateOpen || stored.GuardConflictCode.String != "destination_changed" {
		t.Fatalf("held session = %+v, %v", stored, err)
	}
	// A once-held generation stays held even if the original empty state returns.
	if _, err := db.Pool.Exec(ctx, "DELETE FROM files WHERE id=$1", winnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Complete(ctx, guardedManifest(uploadID, source, completeAdmission, incarnation)); !errors.Is(err, uploads.ErrGuardConflict) {
		t.Fatalf("sticky conflict retry = %v, want ErrGuardConflict", err)
	}

	// Guarded winner commits first; a later external create cannot replace it.
	name = uuid.NewString() + ".bin"
	session = guardedCreate(t, svc, nil, nil, name, int64(len(source)), incarnation)
	uploadID = mustUploadUUID(t, session.ID)
	guardedClaimAndStore(t, ctx, svc, policy, uploadID, source, 502, incarnation)
	if _, err := svc.VerifyGuardedPart(ctx, 1001, uploadID, incarnation); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	completeAdmission = completeAdmissionKey()
	file, err := svc.Complete(ctx, guardedManifest(uploadID, source, completeAdmission, incarnation))
	if err != nil {
		t.Fatalf("guarded completion: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO files (id,user_id,parent_id,name,kind,size,status,mod_time)
VALUES ($1,1001,NULL,$2,'file',$3,'active',now())`, uuid.New(), name, int64(len(source))); err == nil {
		t.Fatal("external create replaced a guarded result")
	}
	assertExternalFile(t, db.Pool, mustUploadUUID(t, file.ID), name, int64(len(source)))
	if count := countActiveSlot(t, db.Pool, nil, name); count != 1 {
		t.Fatalf("active slot count = %d, want the guarded result", count)
	}
}

func TestB3GuardedFolderIdentity(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, policy := newUploadFixtureService(t, db.Pool)
	catalogService := catalog.NewService(db.Pool, nil)
	incarnation := recoveryIncarnation(t, db.Pool)
	source := guardedSource("guarded folder identity")

	root, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "mount-root"})
	if err != nil {
		t.Fatal(err)
	}
	rootID := mustUploadUUID(t, root.ID)
	parent, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &rootID, Name: "incoming"})
	if err != nil {
		t.Fatal(err)
	}
	parentID := mustUploadUUID(t, parent.ID)
	name := uuid.NewString() + ".bin"
	session := guardedCreate(t, svc, &parentID, &rootID, name, int64(len(source)), incarnation)
	sessionID := mustUploadUUID(t, session.ID)
	guardedClaimAndStore(t, ctx, svc, policy, sessionID, source, 601, incarnation)

	// The same folder UUID may move or rename inside the pinned root.
	if _, err := catalogService.Rename(ctx, 1001, parentID, nil, "renamed-incoming"); err != nil {
		t.Fatal(err)
	}
	completeAdmission := completeAdmissionKey()
	file, err := svc.Complete(ctx, guardedManifest(sessionID, source, completeAdmission, incarnation))
	if err != nil {
		t.Fatalf("in-root moved folder completion: %v", err)
	}
	if file.ParentID != dbtypes.UUID(parentID) {
		t.Fatalf("result parent = %v, want the admitted folder UUID", file.ParentID)
	}

	// Leaving the pinned root invalidates the admitted ancestry. A new folder
	// with the same path name cannot receive the pending write.
	otherRoot, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "outside-root"})
	if err != nil {
		t.Fatal(err)
	}
	otherRootID := mustUploadUUID(t, otherRoot.ID)
	name = uuid.NewString() + ".bin"
	session = guardedCreate(t, svc, &parentID, &rootID, name, int64(len(source)), incarnation)
	uploadID := mustUploadUUID(t, session.ID)
	guardedClaimAndStore(t, ctx, svc, policy, uploadID, source, 602, incarnation)
	if _, err := catalogService.Move(ctx, 1001, parentID, &otherRootID, nil); err != nil {
		t.Fatal(err)
	}
	decoy, err := catalogService.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &rootID, Name: "renamed-incoming"})
	if err != nil {
		t.Fatal(err)
	}
	decoyID := mustUploadUUID(t, decoy.ID)
	if _, err := svc.VerifyGuardedPart(ctx, 1001, uploadID, incarnation); !errors.Is(err, uploads.ErrGuardConflict) {
		t.Fatalf("out-of-root preflight = %v, want ErrGuardConflict", err)
	}
	if _, err := svc.Complete(ctx, guardedManifest(uploadID, source, completeAdmissionKey(), incarnation)); !errors.Is(err, uploads.ErrGuardConflict) {
		t.Fatalf("out-of-root completion = %v, want ErrGuardConflict", err)
	}
	if count := countActiveSlot(t, db.Pool, &parentID, name); count != 0 {
		t.Fatalf("out-of-root session published %d files", count)
	}
	if count := countActiveSlot(t, db.Pool, &decoyID, name); count != 0 {
		t.Fatalf("path-name decoy received the pending write")
	}
}

func TestB3GuardedDomainReplayAndLegacyLatch(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, policy := newUploadFixtureService(t, db.Pool)
	incarnation := recoveryIncarnation(t, db.Pool)
	source := guardedSource("guarded replay")

	name := uuid.NewString() + ".bin"
	input, _ := guardedInput(name, int64(len(source)), incarnation)
	session, err := svc.Create(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := mustUploadUUID(t, session.ID)
	guardedClaimAndStore(t, ctx, svc, policy, uploadID, source, 701, incarnation)
	completeAdmission := completeAdmissionKey()
	completed, err := svc.Complete(ctx, guardedManifestWithAdmission(uploadID, source, completeAdmission, incarnation))
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Create(ctx, input)
	if err != nil || mustUploadUUID(t, again.ID) != uploadID {
		t.Fatalf("create replay = %+v, %v", again, err)
	}
	replayed, err := svc.Complete(ctx, guardedManifestWithAdmission(uploadID, source, completeAdmission, incarnation))
	if err != nil || mustUploadUUID(t, replayed.ID) != mustUploadUUID(t, completed.ID) {
		t.Fatalf("complete replay = %+v, %v", replayed, err)
	}
	if !replayed.Size.Valid || replayed.Size.Int64 != int64(len(source)) || !replayed.HashValue.Valid || replayed.HashValue.String != b1SourceRoot(source) {
		t.Fatalf("replay proof = size %v hash %v", replayed.Size, replayed.HashValue)
	}
	var files int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE user_id=1001 AND parent_id IS NULL AND name=$1", name).Scan(&files); err != nil || files != 1 {
		t.Fatalf("result files = %d, %v", files, err)
	}

	// A replay with a changed guard under the same admission key is refused.
	expected := mustUploadUUID(t, replayed.ID)
	changed := *input.Guard
	changed.ExpectedFileID = &expected
	generation := int64(1)
	changed.ExpectedGeneration = &generation
	bad := input
	bad.Guard = &changed
	if _, err := svc.Create(ctx, bad); err == nil {
		t.Fatal("changed guard replayed the admitted session")
	}

	// Direct legacy-shaped SQL cannot publish or clear a guarded session.
	openName := uuid.NewString() + ".bin"
	open, _ := guardedInput(openName, int64(len(source)), incarnation)
	openSession, err := svc.Create(ctx, open)
	if err != nil {
		t.Fatal(err)
	}
	openID := mustUploadUUID(t, openSession.ID)
	guardedClaimAndStore(t, ctx, svc, policy, openID, source, 702, incarnation)
	if _, err := db.Pool.Exec(ctx, "UPDATE upload_sessions SET state='completed', file_id=$2, completed_at=now() WHERE id=$1", openID, replayed.ID); err == nil {
		t.Fatal("legacy-shaped SQL published a guarded session")
	}
	if _, err := db.Pool.Exec(ctx, "UPDATE upload_sessions SET guarded=false WHERE id=$1", openID); err == nil {
		t.Fatal("legacy-shaped SQL removed the guard")
	}
	if _, err := db.Pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", openID); err == nil {
		t.Fatal("guarded session was deleted")
	}

	// Expired open sessions never yield a positive GET.
	if _, err := db.Pool.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", openID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, 1001, openID); !errors.Is(err, uploads.ErrExpired) {
		t.Fatalf("expired get = %v, want ErrExpired", err)
	}
	if _, err := svc.GetGuarded(ctx, 1001, openID, incarnation); !errors.Is(err, uploads.ErrExpired) {
		t.Fatalf("expired guarded get = %v, want ErrExpired", err)
	}

	// Ordinary unguarded publication keeps its existing path.
	plainName := uuid.NewString() + ".bin"
	plain, err := svc.Create(ctx, uploads.CreateInput{UserID: 1001, Name: plainName, ExpectedSize: int64(len(source)), PartSize: treehash.BlockSize})
	if err != nil {
		t.Fatal(err)
	}
	plainID := mustUploadUUID(t, plain.ID)
	claimAndStore(t, ctx, svc, plainID, 1, source, 703, policy)
	if _, err := svc.Complete(ctx, b1Manifest(plainID, source)); err != nil {
		t.Fatalf("ordinary completion: %v", err)
	}
}

func TestB3GuardedPartOriginBeforeBody(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, _ := newUploadFixtureService(t, db.Pool)
	incarnation := recoveryIncarnation(t, db.Pool)
	source := guardedSource("guarded part origin")

	name := uuid.NewString() + ".bin"
	session := guardedCreate(t, svc, nil, nil, name, int64(len(source)), incarnation)
	uploadID := mustUploadUUID(t, session.ID)
	pipeline := transfer.NewPipeline(&guardedPipelineCatalog{Service: svc}, guardedChannels{}, unusedStorage{}, nil, transfer.Config{})
	for _, tc := range []struct {
		name    string
		request transfer.UploadPartRequest
		hook    bool
		want    error
	}{
		{name: "legacy route", request: transfer.UploadPartRequest{UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(source))}, want: uploads.ErrGuardedRouteRequired},
		{name: "wrong incarnation", request: transfer.UploadPartRequest{UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(source)), Guarded: true, RecoveryIncarnation: uuid.New()}, want: uploads.ErrRecoveryChanged},
		{name: "hook veto", request: transfer.UploadPartRequest{UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(source)), Guarded: true, RecoveryIncarnation: incarnation}, hook: true, want: errGuardedVeto},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &countingBody{}
			tc.request.Body = body
			if tc.hook {
				tc.request.BeforeParts = func(path []string) error {
					if len(path) != 0 {
						t.Fatalf("verified path = %v, want root", path)
					}
					return errGuardedVeto
				}
			}
			if _, err := pipeline.UploadPart(ctx, tc.request); !errors.Is(err, tc.want) {
				t.Fatalf("upload = %v, want %v", err, tc.want)
			}
			if body.read != 0 {
				t.Fatalf("guard failure consumed %d body bytes", body.read)
			}
		})
	}
}

var errGuardedVeto = errors.New("guarded hook veto")

type guardedPipelineCatalog struct{ *uploads.Service }

type guardedChannels struct{}

func (guardedChannels) Resolve(context.Context, int64, int64) (int64, error) { return 9001, nil }

type unusedStorage struct{}

func (unusedStorage) Upload(context.Context, telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("unused storage")
}
func (unusedStorage) OpenRange(context.Context, telegramstore.RangeRequest) (io.ReadCloser, error) {
	return nil, errors.New("unused storage")
}
func (unusedStorage) DeleteMessages(context.Context, int64, int64, []int64) error {
	return errors.New("unused storage")
}
func (unusedStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("unused storage")
}
func (unusedStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, errors.New("unused storage")
}
func (unusedStorage) DeleteChannel(context.Context, int64, int64) error {
	return errors.New("unused storage")
}

type countingBody struct{ read int }

func (b *countingBody) Read(p []byte) (int, error) {
	b.read += len(p)
	return 0, io.EOF
}

func guardedSource(label string) []byte {
	return bytes.Repeat([]byte(label), treehash.BlockSize/len(label)+1)[:treehash.BlockSize]
}

func recoveryIncarnation(t testing.TB, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	var value pgtype.UUID
	if err := pool.QueryRow(context.Background(), "SELECT incarnation FROM recovery_incarnations").Scan(&value); err != nil {
		t.Fatal(err)
	}
	return mustUploadUUID(t, value)
}

func guardedAdmission(key uuid.UUID) *lifecycle.RequestAdmissionInput {
	actor := int64(1001)
	return &lifecycle.RequestAdmissionInput{
		Scope:              lifecycle.RequestScope{ActorUserID: &actor},
		RequestKey:         key,
		RequestFingerprint: sha256.Sum256([]byte(key.String())),
	}
}

func guardedInput(name string, size int64, incarnation uuid.UUID) (uploads.CreateInput, uuid.UUID) {
	key := uuid.New()
	return uploads.CreateInput{
		UserID:         1001,
		Name:           name,
		ExpectedSize:   size,
		PartSize:       treehash.BlockSize,
		ConflictPolicy: sqlcgen.NameConflictPolicyFail,
		Admission:      guardedAdmission(key),
		Guard:          &uploads.GuardInput{RecoveryIncarnation: incarnation},
	}, key
}

func guardedCreate(t testing.TB, svc *uploads.Service, parent, mountRoot *uuid.UUID, name string, size int64, incarnation uuid.UUID) *sqlcgen.UploadSession {
	t.Helper()
	input, _ := guardedInput(name, size, incarnation)
	input.ParentID = parent
	input.Guard.MountRoot = mountRoot
	session, err := svc.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("create guarded upload: %v", err)
	}
	return session
}

func guardedClaimAndStore(t testing.TB, ctx context.Context, svc *uploads.Service, policy *lifecycle.Service, uploadID uuid.UUID, data []byte, messageID int64, incarnation uuid.UUID) {
	t.Helper()
	blockHashes, checksum := hashMetadata(data)
	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(data)), Checksum: &checksum,
		Guarded: true, RecoveryIncarnation: incarnation,
	})
	if err != nil {
		t.Fatalf("claim guarded part: %v", err)
	}
	receiptID := observeUploadFixturePart(t, ctx, policy, uploadID, 1, claim.LeaseToken, messageID)
	if _, err := svc.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, ReceiptID: receiptID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: messageID,
		StoredSize: int64(len(data)), Checksum: checksum, BlockHashes: blockHashes,
		Guarded: true, RecoveryIncarnation: incarnation,
	}); err != nil {
		t.Fatalf("store guarded part: %v", err)
	}
}

func guardedManifest(uploadID uuid.UUID, source []byte, admission *lifecycle.RequestAdmissionInput, incarnation uuid.UUID) uploads.CompleteInput {
	return guardedManifestWithAdmission(uploadID, source, admission, incarnation)
}

func guardedManifestWithAdmission(uploadID uuid.UUID, source []byte, admission *lifecycle.RequestAdmissionInput, incarnation uuid.UUID) uploads.CompleteInput {
	algorithm, value := string(treehash.TypeBlake3), b1SourceRoot(source)
	return uploads.CompleteInput{
		UserID: 1001, UploadID: uploadID, Size: int64(len(source)),
		HashAlgorithm: &algorithm, HashValue: &value,
		Guarded: true, Admission: admission, RecoveryIncarnation: incarnation,
	}
}

func completeAdmissionKey() *lifecycle.RequestAdmissionInput {
	return guardedAdmission(uuid.New())
}

func insertExternalFile(t testing.TB, pool *pgxpool.Pool, parent *uuid.UUID, name string, size int64) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(context.Background(), `INSERT INTO files (id,user_id,parent_id,name,kind,size,status,mod_time)
VALUES ($1,1001,$2,$3,'file',$4,'active',now())`, id, parent, name, size); err != nil {
		t.Fatal(err)
	}
	return id
}

func assertExternalFile(t testing.TB, pool *pgxpool.Pool, id uuid.UUID, name string, size int64) {
	t.Helper()
	var gotName string
	var gotSize int64
	var status sqlcgen.FileStatus
	var generation int64
	if err := pool.QueryRow(context.Background(), "SELECT name,size,status,generation FROM files WHERE id=$1", id).Scan(&gotName, &gotSize, &status, &generation); err != nil {
		t.Fatal(err)
	}
	if gotName != name || gotSize != size || status != sqlcgen.FileStatusActive || generation != 1 {
		t.Fatalf("external winner = (%q,%d,%q,%d)", gotName, gotSize, status, generation)
	}
}

func countActiveSlot(t testing.TB, pool *pgxpool.Pool, parent *uuid.UUID, name string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM files WHERE user_id=1001 AND parent_id IS NOT DISTINCT FROM $1 AND name=$2 AND status='active'", parent, name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
