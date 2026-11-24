//go:build integration

package uploads_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zeebo/blake3"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

// b1SourceRoot uses global source offsets, never product part aggregation.
func b1SourceRoot(source []byte) string {
	root := blake3.New()
	for offset := 0; offset < len(source); offset += treehash.BlockSize {
		digest := blake3.Sum256(source[offset:min(offset+treehash.BlockSize, len(source))])
		_, _ = root.Write(digest[:])
	}
	return hex.EncodeToString(root.Sum(nil))
}

func b1Manifest(uploadID uuid.UUID, source []byte) uploads.CompleteInput {
	algorithm, value := "blake3", b1SourceRoot(source)
	return uploads.CompleteInput{UserID: 1001, UploadID: uploadID, Size: int64(len(source)), HashAlgorithm: &algorithm, HashValue: &value}
}

func b1Session(t testing.TB, pool *pgxpool.Pool, size int64, name string) (*uploads.Service, *lifecycle.Service, uuid.UUID) {
	t.Helper()
	svc, policy := newUploadFixtureService(t, pool)
	session, err := svc.Create(context.Background(), uploads.CreateInput{UserID: 1001, Name: name, ExpectedSize: size, PartSize: treehash.BlockSize})
	if err != nil {
		t.Fatal(err)
	}
	return svc, policy, mustUploadUUID(t, session.ID)
}

func b1CleanupCutoff(t testing.TB, ctx context.Context, pool *pgxpool.Pool) pgtype.Timestamptz {
	t.Helper()
	cutoff, err := sqlcgen.New(pool).GetCleanupSweepCutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return cutoff
}

func TestB1TreeHashOracle(t *testing.T) {
	for _, size := range []int{0, 1, treehash.BlockSize - 1, treehash.BlockSize, treehash.BlockSize + 1, 2*treehash.BlockSize + 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db := testpostgres.New(t)
			seedUploadOwner(t, db.Pool, 1001, 9001)
			source := make([]byte, size)
			for i := range source {
				source[i] = byte(i*31 + i/treehash.BlockSize)
			}
			svc, policy, id := b1Session(t, db.Pool, int64(size), "oracle.bin")
			for offset, part := 0, int32(1); offset < size; offset, part = offset+treehash.BlockSize, part+1 {
				claimAndStore(t, context.Background(), svc, id, part, source[offset:min(offset+treehash.BlockSize, size)], int64(part), policy)
			}
			file, err := svc.Complete(context.Background(), b1Manifest(id, source))
			if err != nil || !file.HashValue.Valid || file.HashValue.String != b1SourceRoot(source) {
				t.Fatalf("oracle completion = %+v, %v", file, err)
			}
			parts, err := sqlcgen.New(db.Pool).ListFileParts(context.Background(), file.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(parts) != (size+treehash.BlockSize-1)/treehash.BlockSize {
				t.Fatalf("parts = %d", len(parts))
			}
		})
	}
}

func TestB1EmptyCompletion(t *testing.T) {
	for _, size := range []int64{0, -1} {
		for _, assertion := range []string{"absent", "canonical", "wrong create", "wrong complete"} {
			t.Run(fmt.Sprintf("%d/%s", size, assertion), func(t *testing.T) {
				db := testpostgres.New(t)
				seedUploadOwner(t, db.Pool, 1001, 9001)
				svc, _ := newUploadFixtureService(t, db.Pool)
				algorithm, expected := "blake3", b1SourceRoot(nil)
				in := uploads.CreateInput{UserID: 1001, Name: "empty", ExpectedSize: size}
				if assertion == "canonical" || assertion == "wrong create" {
					if assertion == "wrong create" {
						expected = strings.Repeat("01", 32)
					}
					in.ExpectedHashAlgorithm, in.ExpectedHashValue = &algorithm, &expected
				}
				session, err := svc.Create(context.Background(), in)
				if err != nil {
					t.Fatal(err)
				}
				id := mustUploadUUID(t, session.ID)
				manifest := uploads.CompleteInput{UserID: 1001, UploadID: id, Size: 0}
				if assertion == "wrong complete" {
					manifest.HashAlgorithm, manifest.HashValue = &algorithm, new(strings.Repeat("02", 32))
				}
				file, err := svc.Complete(context.Background(), manifest)
				if strings.HasPrefix(assertion, "wrong") {
					if !errors.Is(err, uploads.ErrHashMismatch) {
						t.Fatalf("wrong empty hash = %v", err)
					}
					return
				}
				if err != nil || !file.Size.Valid || file.Size.Int64 != 0 || !file.HashValue.Valid || file.HashValue.String != b1SourceRoot(nil) {
					t.Fatalf("empty result = %+v, %v", file, err)
				}
				again, err := svc.Complete(context.Background(), manifest)
				if err != nil || again.ID != file.ID || again.Generation != file.Generation {
					t.Fatalf("empty replay = %+v, %v", again, err)
				}
				var count int
				if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM file_parts WHERE file_id=$1", file.ID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("empty parts = %d, %v", count, err)
				}
			})
		}
	}
}

func TestB1CompletionReplay(t *testing.T) {
	db := testpostgres.New(t)
	seedUploadOwner(t, db.Pool, 1001, 9001)
	source := []byte("immutable publication")
	svc, policy, id := b1Session(t, db.Pool, int64(len(source)), "replay")
	claimAndStore(t, context.Background(), svc, id, 1, source, 1, policy)
	manifest := b1Manifest(id, source)
	file, err := svc.Complete(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	before := b1Snapshot(t, db.Pool, id, mustUploadUUID(t, file.ID), uuid.Nil)
	for _, test := range []struct {
		name string
		size int64
		hash *string
		want error
	}{
		{"lost response", manifest.Size, manifest.HashValue, nil},
		{"optional omitted", manifest.Size, nil, nil},
		{"different size", manifest.Size + 1, nil, uploads.ErrInvalidState},
		{"different hash", manifest.Size, new(strings.Repeat("aa", 32)), uploads.ErrInvalidState},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := uploads.CompleteInput{UserID: 1001, UploadID: id, Size: test.size, HashValue: test.hash}
			if test.hash != nil {
				in.HashAlgorithm = manifest.HashAlgorithm
			}
			again, err := svc.Complete(context.Background(), in)
			if !errors.Is(err, test.want) {
				t.Fatalf("replay error = %v; want %v", err, test.want)
			}
			if err == nil && (again.ID != file.ID || again.Generation != file.Generation) {
				t.Fatalf("replay changed file: %+v", again)
			}
			if after := b1Snapshot(t, db.Pool, id, mustUploadUUID(t, file.ID), uuid.Nil); after != before {
				t.Fatal("replay changed committed state/events")
			}
		})
	}
	if _, err := db.Pool.Exec(context.Background(), "UPDATE files SET status='deletion_pending', deleted_at=now() WHERE id=$1", file.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Complete(context.Background(), manifest); !errors.Is(err, uploads.ErrNotFound) {
		t.Fatalf("missing published file = %v", err)
	}
	if _, err := db.Pool.Exec(context.Background(), "DELETE FROM file_parts WHERE file_id=$1", file.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(context.Background(), "DELETE FROM files WHERE id=$1", file.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Complete(context.Background(), manifest); !errors.Is(err, uploads.ErrNotFound) {
		t.Fatalf("deleted published file = %v", err)
	}
}

func b1Snapshot(t testing.TB, pool *pgxpool.Pool, uploadID, fileID, shareID uuid.UUID) string {
	t.Helper()
	var result string
	err := pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
		'file', (SELECT to_jsonb(f) FROM files f WHERE id=$1),
		'parts', (SELECT jsonb_agg(to_jsonb(p) ORDER BY part_no) FROM file_parts p WHERE file_id=$1),
		'share', (SELECT to_jsonb(s) FROM file_shares s WHERE id=$2),
		'upload', (SELECT to_jsonb(u) FROM upload_sessions u WHERE id=$3),
		'events', (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM user_events e WHERE user_id=1001 AND (resource_id IN ($1::text,$2::text,$3::text) OR payload->>'fileId'=$1::text))
	)::text`, fileID, shareID, uploadID).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestB1CompletionAtomicity(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	oldBytes := []byte("old nonempty destination")
	svc, policy, oldID := b1Session(t, db.Pool, int64(len(oldBytes)), "replace")
	claimAndStore(t, ctx, svc, oldID, 1, oldBytes, 1, policy)
	old, err := svc.Complete(ctx, b1Manifest(oldID, oldBytes))
	if err != nil {
		t.Fatal(err)
	}
	fileID, shareID := mustUploadUUID(t, old.ID), uuid.New()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO file_shares(id,file_id,owner_id,token_prefix,token_hash) VALUES($1,$2,1001,'b1',$3)", shareID, fileID, []byte(shareID.String())); err != nil {
		t.Fatal(err)
	}
	newBytes := []byte("new nonempty destination")
	for index, scenario := range []string{"bad size", "bad complete hash", "bad create hash", "missing part", "bad digest count", "bad checksum", "missing proof", "legacy geometry"} {
		t.Run(scenario, func(t *testing.T) {
			algorithm, wrong := "blake3", strings.Repeat("ab", 32)
			in := uploads.CreateInput{UserID: 1001, Name: "replace", ExpectedSize: -1, PartSize: treehash.BlockSize, ConflictPolicy: sqlcgen.NameConflictPolicyReplace}
			if scenario == "bad create hash" {
				in.ExpectedHashAlgorithm, in.ExpectedHashValue = &algorithm, &wrong
			}
			session, err := svc.Create(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			id := mustUploadUUID(t, session.ID)
			if scenario != "missing part" {
				claimAndStore(t, ctx, svc, id, 1, newBytes, 100+int64(index), policy)
			}
			manifest := b1Manifest(id, newBytes)
			want := uploads.ErrIncomplete
			switch scenario {
			case "bad size":
				manifest.Size++
			case "bad complete hash":
				manifest.HashValue = &wrong
				want = uploads.ErrHashMismatch
			case "bad create hash":
				want = uploads.ErrHashMismatch
			case "bad digest count":
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET block_hashes=block_hashes||block_hashes WHERE upload_id=$1", id); err != nil {
					t.Fatal(err)
				}
			case "bad checksum":
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET checksum=$2 WHERE upload_id=$1", id, wrong); err != nil {
					t.Fatal(err)
				}
			case "missing proof":
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET checksum=NULL, block_hashes=NULL WHERE upload_id=$1", id); err != nil {
					t.Fatal(err)
				}
			case "legacy geometry":
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_sessions SET part_size=4 WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				want = uploads.ErrInvalidInput
			}
			before := b1Snapshot(t, db.Pool, id, fileID, shareID)
			if _, err := svc.Complete(ctx, manifest); !errors.Is(err, want) {
				t.Fatalf("completion error = %v, want %v", err, want)
			}
			if after := b1Snapshot(t, db.Pool, id, fileID, shareID); after != before {
				t.Fatal("rejected completion changed destination/share/parts/generation/session/events")
			}
		})
	}
	fresh, err := svc.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "replace", ExpectedSize: int64(len(newBytes)), ConflictPolicy: sqlcgen.NameConflictPolicyReplace})
	if err != nil {
		t.Fatal(err)
	}
	id := mustUploadUUID(t, fresh.ID)
	claimAndStore(t, ctx, svc, id, 1, newBytes, 999, policy)
	replacement, err := svc.Complete(ctx, b1Manifest(id, newBytes))
	if err != nil {
		t.Fatal(err)
	}
	var status string
	var generation int64
	var revoked bool
	if err := db.Pool.QueryRow(ctx, "SELECT status,generation FROM files WHERE id=$1", fileID).Scan(&status, &generation); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, "SELECT revoked_at IS NOT NULL FROM file_shares WHERE id=$1", shareID).Scan(&revoked); err != nil {
		t.Fatal(err)
	}
	if status != "deletion_pending" || generation != old.Generation+1 || !revoked || replacement.ID == old.ID {
		t.Fatalf("replacement status=%s generation=%d revoked=%v", status, generation, revoked)
	}
	var events []string
	rows, err := db.Pool.Query(ctx, "SELECT event_type FROM user_events WHERE user_id=1001 AND resource_id=$1 AND event_type IN ('upload.completing','upload.completed') ORDER BY id", id.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var event string
		if err := rows.Scan(&event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(events, ",") != "upload.completing,upload.completed" {
		t.Fatalf("publication events = %v", events)
	}
	if sha256.Sum256(oldBytes) == sha256.Sum256(newBytes) {
		t.Fatal("fixture bytes must differ")
	}
	for _, event := range []struct {
		name     string
		resource uuid.UUID
	}{
		{"file.created", mustUploadUUID(t, replacement.ID)},
		{"file.deletion_pending", fileID},
		{"share.revoked", shareID},
	} {
		var count int
		if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE user_id=1001 AND resource_id=$1 AND event_type=$2", event.resource.String(), event.name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s committed %d times", event.name, count)
		}
	}
}

// Wait for a real PostgreSQL lock dependency, not a scheduler guess or sleep.
func b1WaitForBlocked(t testing.TB, ctx context.Context, pool *pgxpool.Pool, blocker uint32) uint32 {
	t.Helper()
	for {
		var waiter uint32
		err := pool.QueryRow(ctx, "SELECT pid FROM pg_stat_activity WHERE $1::integer = ANY(pg_blocking_pids(pid)) LIMIT 1", int32(blocker)).Scan(&waiter)
		if err == nil {
			return waiter
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("operation never waited for backend %d: %v", blocker, ctx.Err())
		default:
			runtime.Gosched()
		}
	}
}

func TestB1UploadSessionSerialization(t *testing.T) {
	for _, scenario := range []string{"claim after complete", "store after complete", "stored tail first", "uploading tail first", "failed tail first", "claim after expiry", "store after expiry", "renew after expiry", "fail after expiry", "complete after expiry", "abort after expiry", "stolen lease", "expired lease", "abort before store"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			seedUploadOwner(t, db.Pool, 1001, 9001)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			source := bytes.Repeat([]byte{17}, treehash.BlockSize)
			svc, policy, id := b1Session(t, db.Pool, -1, "serialization")
			claimAndStore(t, ctx, svc, id, 1, source, 1, policy)
			tail := []byte("tail")
			hashes, checksum := hashMetadata(tail)
			var claim *uploads.ClaimPartResult
			var err error
			if scenario != "claim after complete" && scenario != "store after complete" && scenario != "claim after expiry" && scenario != "complete after expiry" && scenario != "abort after expiry" {
				claim, err = svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 2, ChannelID: 9001, PlainSize: int64(len(tail))})
				if err != nil {
					t.Fatal(err)
				}
			}
			var tailReceipt uuid.UUID
			if claim != nil {
				tailReceipt = observeUploadFixturePart(t, ctx, policy, id, 2, claim.LeaseToken, 2)
			}
			store := func(token uuid.UUID) error {
				_, err := svc.StorePart(ctx, uploads.StorePartInput{ReceiptID: tailReceipt, UploadID: id, PartNo: 2, LeaseToken: token, MessageID: 2, StoredSize: int64(len(tail)), Checksum: checksum, BlockHashes: hashes})
				return err
			}
			if strings.HasSuffix(scenario, "tail first") {
				if scenario == "stored tail first" {
					if err := store(claim.LeaseToken); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "failed tail first" {
					if _, err := svc.FailPart(ctx, uploads.FailPartInput{UploadID: id, PartNo: 2, LeaseToken: claim.LeaseToken, ErrorCode: "test"}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := svc.Complete(ctx, b1Manifest(id, source)); !errors.Is(err, uploads.ErrIncomplete) {
					t.Fatalf("late tail published: %v", err)
				}
				return
			}
			if scenario == "stolen lease" || scenario == "expired lease" {
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE upload_id=$1 AND part_no=2", id); err != nil {
					t.Fatal(err)
				}
				if scenario == "stolen lease" {
					if _, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 2, ChannelID: 9001, PlainSize: int64(len(tail))}); err != nil {
						t.Fatal(err)
					}
				}
				if err := store(claim.LeaseToken); !errors.Is(err, uploads.ErrLeaseLost) {
					t.Fatalf("stale store = %v", err)
				}
				if err := svc.RenewPart(ctx, uploads.RenewPartInput{UploadID: id, PartNo: 2, LeaseToken: claim.LeaseToken}); !errors.Is(err, uploads.ErrLeaseLost) {
					t.Fatalf("stale renew = %v", err)
				}
				if _, err := svc.FailPart(ctx, uploads.FailPartInput{UploadID: id, PartNo: 2, LeaseToken: claim.LeaseToken, ErrorCode: "stale"}); !errors.Is(err, uploads.ErrLeaseLost) {
					t.Fatalf("stale fail = %v", err)
				}
				return
			}
			if scenario == "abort before store" {
				if _, err := svc.Abort(ctx, 1001, id); err != nil {
					t.Fatal(err)
				}
				if err := store(claim.LeaseToken); !errors.Is(err, uploads.ErrInvalidState) {
					t.Fatalf("store after abort = %v", err)
				}
				return
			}
			gate, err := db.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Rollback(context.Background())
			var blocker uint32
			if err := gate.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blocker); err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(scenario, "after complete") {
				// Complete holds the owner, hierarchy and receipt locks before the destination gate.
				if _, err := gate.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", b1DestinationLockID(1001)); err != nil {
					t.Fatal(err)
				}
				completed := make(chan error, 1)
				go func() { _, err := svc.Complete(ctx, b1Manifest(id, source)); completed <- err }()
				completionPID := b1WaitForBlocked(t, ctx, db.Pool, blocker)
				late := make(chan error, 1)
				go func() {
					if scenario == "claim after complete" {
						_, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 2, ChannelID: 9001, PlainSize: 4})
						late <- err
					} else {
						late <- store(uuid.New())
					}
				}()
				b1WaitForBlocked(t, ctx, db.Pool, completionPID)
				if err := gate.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				if err := <-completed; err != nil {
					t.Fatal(err)
				}
				want := uploads.ErrInvalidState
				if scenario == "claim after complete" {
					// Completed claims project published parts; the absent tail has no proof.
					want = uploads.ErrNotFound
				}
				if err := <-late; !errors.Is(err, want) {
					t.Fatalf("late mutator = %v; want %v", err, want)
				}
				session, err := svc.Get(ctx, 1001, id)
				if err != nil || session.State != sqlcgen.UploadStateCompleted || !session.FileID.Valid {
					t.Fatalf("publication session = %+v, %v", session, err)
				}
				fileID := uuid.UUID(session.FileID.Bytes)
				before := b1Snapshot(t, db.Pool, id, fileID, uuid.Nil)
				file, err := svc.Complete(ctx, b1Manifest(id, source))
				root := b1SourceRoot(source)
				if err != nil || file.ID != session.FileID || !file.Size.Valid || file.Size.Int64 != int64(len(source)) || !file.HashValue.Valid || file.HashValue.String != root {
					t.Fatalf("published source replay = %+v, %v", file, err)
				}
				existing, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(source)), Checksum: &root})
				if err != nil {
					t.Fatal(err)
				}
				digest := blake3.Sum256(source)
				if !existing.Existing || existing.LeaseToken != uuid.Nil || existing.Part.ChannelID != 9001 || !existing.Part.MessageID.Valid || existing.Part.MessageID.Int64 != 1 || !existing.Part.StoredSize.Valid || existing.Part.StoredSize.Int64 != int64(len(source)) || !bytes.Equal(existing.Part.BlockHashes, digest[:]) {
					t.Fatalf("completed source proof = %+v", existing)
				}
				if _, err := svc.GetPart(ctx, 1001, id, 2); !errors.Is(err, uploads.ErrNotFound) {
					t.Fatalf("absent completed tail = %v", err)
				}
				var aliases int
				if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&aliases); err != nil || aliases != 0 {
					t.Fatalf("completed aliases = %d, %v", aliases, err)
				}
				if after := b1Snapshot(t, db.Pool, id, fileID, uuid.Nil); after != before {
					t.Fatal("completed replay/proof changed publication/aliases/events")
				}
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				cutoff := b1CleanupCutoff(t, ctx, db.Pool)
				if _, err := sqlcgen.New(db.Pool).ExpireUploadSessions(ctx, cutoff); err != nil {
					t.Fatal(err)
				}
				cleanup, err := sqlcgen.New(db.Pool).ListUploadSessionsPendingCleanup(ctx, sqlcgen.ListUploadSessionsPendingCleanupParams{PassCutoff: cutoff})
				if err != nil || len(cleanup) != 0 {
					t.Fatalf("completed cleanup candidates = %+v, %v", cleanup, err)
				}
				return
			}
			// Complete validates its proof before locking; expire only after it reaches the owner fence.
			if err := lifecycle.LockOwnerTx(ctx, gate, 1001); err != nil {
				t.Fatal(err)
			}
			if _, err := gate.Exec(ctx, "SELECT id FROM upload_sessions WHERE id=$1 FOR UPDATE", id); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				switch scenario {
				case "claim after expiry":
					_, err = svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 2, ChannelID: 9001, PlainSize: 4})
				case "store after expiry":
					err = store(claim.LeaseToken)
				case "renew after expiry":
					err = svc.RenewPart(ctx, uploads.RenewPartInput{UploadID: id, PartNo: 2, LeaseToken: claim.LeaseToken})
				case "fail after expiry":
					_, err = svc.FailPart(ctx, uploads.FailPartInput{UploadID: id, PartNo: 2, LeaseToken: claim.LeaseToken, ErrorCode: "late"})
				case "complete after expiry":
					_, err = svc.Complete(ctx, b1Manifest(id, source))
				case "abort after expiry":
					_, err = svc.Abort(ctx, 1001, id)
				}
				done <- err
			}()
			b1WaitForBlocked(t, ctx, db.Pool, blocker)
			if _, err := gate.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
			if expired, err := sqlcgen.New(db.Pool).ExpireUploadSessions(ctx, b1CleanupCutoff(t, ctx, db.Pool)); err != nil || len(expired) != 0 {
				t.Fatalf("locked session was not skipped: %+v, %v", expired, err)
			}
			if err := gate.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, uploads.ErrExpired) {
				t.Fatalf("expired mutator = %v", err)
			}
			if _, err := sqlcgen.New(db.Pool).ExpireUploadSessions(ctx, b1CleanupCutoff(t, ctx, db.Pool)); err != nil {
				t.Fatal(err)
			}
			session, err := svc.Get(ctx, 1001, id)
			if err != nil || session.State != sqlcgen.UploadStateExpired {
				t.Fatalf("expiry result = %+v, %v", session, err)
			}
		})
	}
}

func b1DestinationLockID(userID int64) int64 {
	// Match the destination namespace only to hold a deterministic SQL barrier.
	input := append([]byte("teldrive/upload-destination/"), make([]byte, 24)...)
	for i := range 8 {
		input[len("teldrive/upload-destination/")+i] = byte(uint64(userID) >> (56 - 8*i))
	}
	digest := sha256.Sum256(input)
	var lock uint64
	for _, value := range digest[:8] {
		lock = lock<<8 | uint64(value)
	}
	return int64(lock)
}

func TestB1UnknownRetryCoverage(t *testing.T) {
	for _, scenario := range []string{"empty retry", "one full part retry", "short final retry", "sparse stored rows"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			seedUploadOwner(t, db.Pool, 1001, 9001)
			source := append(bytes.Repeat([]byte{23}, 2*treehash.BlockSize), []byte("old final tail")...)
			svc, policy, id := b1Session(t, db.Pool, -1, "unknown")
			for offset, part := 0, int32(1); offset < len(source); offset, part = offset+treehash.BlockSize, part+1 {
				if scenario == "sparse stored rows" && part == 2 {
					continue
				}
				claimAndStore(t, context.Background(), svc, id, part, source[offset:min(offset+treehash.BlockSize, len(source))], int64(part), policy)
			}
			size := 0
			switch scenario {
			case "one full part retry":
				size = treehash.BlockSize
			case "short final retry":
				size = treehash.BlockSize + 3
			case "sparse stored rows":
				size = len(source)
			}
			before := b1Snapshot(t, db.Pool, id, uuid.Nil, uuid.Nil)
			if _, err := svc.Complete(context.Background(), b1Manifest(id, source[:size])); !errors.Is(err, uploads.ErrIncomplete) {
				t.Fatalf("stale coverage completed: %v", err)
			}
			if after := b1Snapshot(t, db.Pool, id, uuid.Nil, uuid.Nil); after != before {
				t.Fatal("rejected EOF changed expected size or publication events")
			}
		})
	}
}

func TestB1HashDisabledProof(t *testing.T) {
	for _, scenario := range []string{"fresh unhashed", "current hash required", "create hash required", "hashed then unhashed", "unhashed then hashed"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			seedUploadOwner(t, db.Pool, 1001, 9001)
			source := append(bytes.Repeat([]byte{7}, treehash.BlockSize), byte(9))
			algorithm, root := "blake3", b1SourceRoot(source)
			svc, policy := newUploadFixtureService(t, db.Pool)
			in := uploads.CreateInput{UserID: 1001, Name: "proof", ExpectedSize: int64(len(source)), PartSize: treehash.BlockSize}
			if scenario == "create hash required" {
				in.ExpectedHashAlgorithm, in.ExpectedHashValue = &algorithm, &root
			}
			session, err := svc.Create(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			id := mustUploadUUID(t, session.ID)
			for index, data := range [][]byte{source[:treehash.BlockSize], source[treehash.BlockSize:]} {
				partNo := int32(index + 1)
				hashed := (scenario == "hashed then unhashed" && index == 0) || (scenario == "unhashed then hashed" && index == 1)
				if hashed {
					claimAndStore(t, ctx, svc, id, partNo, data, int64(partNo), policy)
					continue
				}
				claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: partNo, ChannelID: 9001, PlainSize: int64(len(data))})
				if err != nil {
					t.Fatal(err)
				}
				receiptID := observeUploadFixturePart(t, ctx, policy, id, partNo, claim.LeaseToken, int64(partNo))
				if _, err := svc.StorePart(ctx, uploads.StorePartInput{ReceiptID: receiptID, UploadID: id, PartNo: partNo, LeaseToken: claim.LeaseToken, MessageID: int64(partNo), StoredSize: int64(len(data))}); err != nil {
					t.Fatal(err)
				}
				if _, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: partNo, ChannelID: 9001, PlainSize: int64(len(data))}); !errors.Is(err, uploads.ErrPartConflict) {
					t.Fatalf("unhashed stored part authorized reuse: %v", err)
				}
			}
			manifest := uploads.CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(source))}
			if scenario == "current hash required" {
				manifest.HashAlgorithm, manifest.HashValue = &algorithm, &root
			}
			file, err := svc.Complete(ctx, manifest)
			if scenario != "fresh unhashed" {
				if !errors.Is(err, uploads.ErrIncomplete) {
					t.Fatalf("unverified publication = %+v, %v", file, err)
				}
			} else if err != nil || file.HashAlgorithm.Valid || file.HashValue.Valid {
				t.Fatalf("fresh unhashed publication = %+v, %v", file, err)
			}
		})
	}
}

type b1CompletionPreflightGate struct {
	ready   chan struct{}
	release chan struct{}
}

func (g *b1CompletionPreflightGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(g) == g && data.SQL == "begin" {
		// BEGIN follows proof validation, before Complete takes any lifecycle lock.
		close(g.ready)
		select {
		case <-g.release:
		case <-ctx.Done():
		}
	}
	return ctx
}

func (*b1CompletionPreflightGate) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestB1PartMutatorWinsPublicationLock(t *testing.T) {
	for _, operation := range []string{"claim", "store"} {
		t.Run(operation, func(t *testing.T) {
			db := testpostgres.New(t)
			seedUploadOwner(t, db.Pool, 1001, 9001)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			preflight := &b1CompletionPreflightGate{ready: make(chan struct{}), release: make(chan struct{})}
			config := db.Pool.Config()
			config.ConnConfig.Tracer = preflight
			pool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			source := bytes.Repeat([]byte{3}, treehash.BlockSize)
			svc, policy, id := b1Session(t, pool, -1, "mutator-first")
			claimAndStore(t, ctx, svc, id, 1, source, 1, policy)
			completed := make(chan error, 1)
			go func() {
				completionCtx := context.WithValue(ctx, preflight, preflight)
				_, err := svc.Complete(completionCtx, b1Manifest(id, source))
				completed <- err
			}()
			select {
			case <-preflight.ready:
			case <-ctx.Done():
				t.Fatal("completion never validated its preflight proof")
			}
			// Add the tail only after Complete has proved the original source snapshot.
			claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 2, ChannelID: 9001, PlainSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			var tailReceipt uuid.UUID
			if operation == "store" {
				tailReceipt = observeUploadFixturePart(t, ctx, policy, id, 2, claim.LeaseToken, 2)
			}
			if operation == "claim" {
				if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE upload_id=$1 AND part_no=2", id); err != nil {
					t.Fatal(err)
				}
			}
			gate, err := db.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Rollback(context.Background())
			var blocker uint32
			if err := gate.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blocker); err != nil {
				t.Fatal(err)
			}
			if operation == "store" {
				// BindTx locks the observed receipt before storing its upload alias.
				if _, err := gate.Exec(ctx, "SELECT id FROM remote_write_receipts WHERE id=$1 FOR UPDATE", tailReceipt); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := gate.Exec(ctx, "SELECT part_no FROM upload_parts WHERE upload_id=$1 AND part_no=2 FOR UPDATE", id); err != nil {
					t.Fatal(err)
				}
			}
			mutated := make(chan error, 1)
			go func() {
				if operation == "claim" {
					_, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 2, ChannelID: 9001, PlainSize: 4})
					mutated <- err
				} else {
					hashes, checksum := hashMetadata([]byte("tail"))
					_, err := svc.StorePart(ctx, uploads.StorePartInput{ReceiptID: tailReceipt, UploadID: id, PartNo: 2, LeaseToken: claim.LeaseToken, MessageID: 2, StoredSize: 4, Checksum: checksum, BlockHashes: hashes})
					mutated <- err
				}
			}()
			mutatorPID := b1WaitForBlocked(t, ctx, db.Pool, blocker)
			close(preflight.release)
			b1WaitForBlocked(t, ctx, db.Pool, mutatorPID)
			if err := gate.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-mutated; err != nil {
				t.Fatal(err)
			}
			if err := <-completed; !errors.Is(err, uploads.ErrIncomplete) {
				t.Fatalf("publication ignored winning tail: %v", err)
			}
			session, err := svc.Get(ctx, 1001, id)
			if err != nil || session.State != sqlcgen.UploadStateOpen || session.ExpectedSize != -1 {
				t.Fatalf("rejected publication changed session: %+v, %v", session, err)
			}
			part, err := svc.GetPart(ctx, 1001, id, 2)
			wantState := sqlcgen.UploadPartStateUploading
			if operation == "store" {
				wantState = sqlcgen.UploadPartStateStored
			}
			if err != nil || part.State != wantState || part.ChannelID != 9001 || part.PlainSize != 4 {
				t.Fatalf("winning tail = %+v, %v", part, err)
			}
			if operation == "store" {
				hashes, checksum := hashMetadata([]byte("tail"))
				if !part.MessageID.Valid || part.MessageID.Int64 != 2 || !part.StoredSize.Valid || part.StoredSize.Int64 != 4 || !part.Checksum.Valid || part.Checksum.String != checksum || !bytes.Equal(part.BlockHashes, hashes) {
					t.Fatalf("winning stored proof = %+v", part)
				}
			} else if !part.LeaseToken.Valid || uuid.UUID(part.LeaseToken.Bytes) == claim.LeaseToken {
				t.Fatalf("winning claim did not replace the expired lease: %+v", part)
			}
			var files, events int
			if err := db.Pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM files WHERE user_id=1001 AND name='mutator-first'),
				(SELECT count(*) FROM user_events WHERE user_id=1001 AND resource_id=$1 AND event_type IN ('upload.completing','upload.completed'))`, id.String()).Scan(&files, &events); err != nil {
				t.Fatal(err)
			}
			if files != 0 || events != 0 {
				t.Fatalf("rejected publication leaked files=%d events=%d", files, events)
			}
			// Once expiry wins, neither the stored nor leased tail can become a publication.
			if _, err := db.Pool.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
			if _, err := sqlcgen.New(db.Pool).ExpireUploadSessions(ctx, b1CleanupCutoff(t, ctx, db.Pool)); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.Complete(ctx, b1Manifest(id, append(source, []byte("tail")...))); !errors.Is(err, uploads.ErrInvalidState) {
				t.Fatalf("expired publication = %v", err)
			}
		})
	}
}

func TestB1StoredPartIntegrity(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc, policy, id := b1Session(t, db.Pool, 4, "stored-proof")
	hashes, checksum := hashMetadata([]byte("data"))
	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: 4, Checksum: &checksum})
	if err != nil {
		t.Fatal(err)
	}
	receiptID := observeUploadFixturePart(t, ctx, policy, id, 1, claim.LeaseToken, 1)
	snapshot := func() string {
		t.Helper()
		var state string
		if err := db.Pool.QueryRow(ctx, "SELECT to_jsonb(p)::text FROM upload_parts p WHERE upload_id=$1 AND part_no=1", id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	valid := uploads.StorePartInput{ReceiptID: receiptID, UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 1, StoredSize: 4, Checksum: checksum, BlockHashes: hashes}
	before := snapshot()
	for _, scenario := range []string{"wrong digest count", "wrong checksum", "wrong stored size", "wrong lease"} {
		t.Run(scenario, func(t *testing.T) {
			in := valid
			want := uploads.ErrPartConflict
			switch scenario {
			case "wrong digest count":
				in.BlockHashes = append(append([]byte(nil), hashes...), hashes...)
			case "wrong checksum":
				in.Checksum = strings.Repeat("ff", 32)
			case "wrong stored size":
				in.StoredSize++
				want = uploads.ErrInvalidInput
			case "wrong lease":
				in.LeaseToken = uuid.New()
				want = uploads.ErrLeaseLost
			}
			if _, err := svc.StorePart(ctx, in); !errors.Is(err, want) {
				t.Fatalf("store error = %v, want %v", err, want)
			}
			if snapshot() != before {
				t.Fatal("rejected metadata changed live part")
			}
		})
	}
	if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET plain_size=5 WHERE upload_id=$1", id); err != nil {
		t.Fatal(err)
	}
	corrupt := snapshot()
	if err := svc.RenewPart(ctx, uploads.RenewPartInput{UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken}); !errors.Is(err, uploads.ErrInvalidInput) {
		t.Fatalf("renew corrupt geometry = %v", err)
	}
	if _, err := svc.FailPart(ctx, uploads.FailPartInput{UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken, ErrorCode: "failure"}); !errors.Is(err, uploads.ErrInvalidInput) {
		t.Fatalf("fail corrupt geometry = %v", err)
	}
	if snapshot() != corrupt {
		t.Fatal("invalid part geometry changed during renewal/failure")
	}
	if _, err := db.Pool.Exec(ctx, "UPDATE upload_parts SET plain_size=4 WHERE upload_id=$1", id); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.StorePart(ctx, valid); err != nil {
		t.Fatal(err)
	}
	stored := snapshot()
	if _, err := svc.StorePart(ctx, valid); !errors.Is(err, uploads.ErrLeaseLost) {
		t.Fatalf("stored overwrite = %v", err)
	}
	if err := svc.RenewPart(ctx, uploads.RenewPartInput{UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken}); !errors.Is(err, uploads.ErrLeaseLost) {
		t.Fatalf("stored renewal = %v", err)
	}
	if _, err := svc.FailPart(ctx, uploads.FailPartInput{UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken, ErrorCode: "stale"}); !errors.Is(err, uploads.ErrLeaseLost) {
		t.Fatalf("stored failure = %v", err)
	}
	if snapshot() != stored {
		t.Fatal("stored metadata changed after stale mutations")
	}
}

func TestB1CancelledSessionWaitKeepsWinner(t *testing.T) {
	db := testpostgres.New(t)
	seedUploadOwner(t, db.Pool, 1001, 9001)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	svc, _, id := b1Session(t, db.Pool, 4, "cancel")
	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(context.Background())
	var blocker uint32
	if err := gate.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Exec(ctx, "SELECT id FROM upload_sessions WHERE id=$1 FOR UPDATE", id); err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := svc.ClaimPart(waitCtx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: 4})
		done <- err
	}()
	b1WaitForBlocked(t, ctx, db.Pool, blocker)
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled session wait = %v", err)
	}
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	part, err := svc.GetPart(ctx, 1001, id, 1)
	if err != nil || part.State != sqlcgen.UploadPartStateUploading || part.LeaseToken.Bytes != [16]byte(claim.LeaseToken) {
		t.Fatalf("waiter changed winner: %+v, %v", part, err)
	}
}
