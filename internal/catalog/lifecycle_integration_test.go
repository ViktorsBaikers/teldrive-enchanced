//go:build integration

package catalog_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type hierarchyQueue struct{ failure error }

func (q *hierarchyQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	if q.failure != nil {
		return 0, q.failure
	}
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO hierarchy_jobs(user_id) VALUES($1) RETURNING id", in.UserID).Scan(&id)
	return id, err
}
func TestB2AncestorDeletion(t *testing.T) {
	for _, mode := range []string{"direct", "ancestor", "ancestor_purge", "bulk_overlap", "rollback", "q_uuid", "q_absolute", "q_relative", "copy_destination", "replace", "restore"} {
		t.Run(mode, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			for _, sql := range []string{"INSERT INTO users(user_id) VALUES(1001)", "CREATE TABLE hierarchy_jobs(id bigint GENERATED ALWAYS AS IDENTITY,user_id bigint)"} {
				if _, err := db.Pool.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001}}})
			sourceBody := []byte("independent source and previous destination")
			if err := storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: 80, Bytes: sourceBody, Preserve: true}); err != nil {
				t.Fatal(err)
			}
			sourceID, shareID := uuid.New(), uuid.New()
			if _, err := db.Pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,status,size,mod_time) VALUES($1,1001,'preserved-source','file','active',$2,now())", sourceID, len(sourceBody)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) VALUES($1,1,9001,80,$2,$2)", sourceID, len(sourceBody)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Pool.Exec(ctx, "INSERT INTO file_shares(id,file_id,owner_id,token_prefix,token_hash) VALUES($1,$2,1001,'b2',$3)", shareID, sourceID, []byte(shareID.String())); err != nil {
				t.Fatal(err)
			}
			policy := lifecycle.NewService(db.Pool, storage)
			queue := &hierarchyQueue{}
			if err := policy.BindReceiptEnqueuer(queue); err != nil {
				t.Fatal(err)
			}
			service := catalog.NewService(db.Pool, nil)
			if err := service.BindLifecycle(policy); err != nil {
				t.Fatal(err)
			}
			root, err := service.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "root"})
			if err != nil {
				t.Fatal(err)
			}
			rootID := uuid.UUID(root.ID.Bytes)
			child, err := service.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &rootID, Name: "child"})
			if err != nil {
				t.Fatal(err)
			}
			childID := uuid.UUID(child.ID.Bytes)
			uploader := uploads.NewService(db.Pool)
			if err = uploader.BindLifecycle(policy); err != nil {
				t.Fatal(err)
			}
			session, err := uploader.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &childID, Name: "pending", ExpectedSize: 0})
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			operation := uuid.New()
			_, _, err = policy.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: operation, RootResultFileID: uuid.New(), LeaseToken: uuid.New(), DestinationParentID: &childID, LeaseExpiresAt: time.Now().Add(time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			batch := uuid.New()
			tx, err = db.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			destination := childID.String()
			if mode == "q_absolute" {
				destination = "/root/child/missing"
			}
			if mode == "q_relative" {
				destination = "/root"
			}
			pinInput := uploads.PinImportDestinationsInput{UserID: 1001, BatchID: batch, Destination: destination, SourceDestinations: []string{"child/missing/file"}}
			pins, err := uploader.PinImportDestinationsTx(ctx, tx, pinInput)
			if err != nil {
				t.Fatal(err)
			}
			if err = uploader.InsertImportBatchAdmissionTx(ctx, tx, uploads.InsertImportBatchAdmissionInput{UserID: 1001, BatchID: batch, OriginJobID: 101, RequestFingerprint: sha256.Sum256([]byte("args")), Pins: pins}); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			snapshot := func() string {
				t.Helper()
				var value string
				e := db.Pool.QueryRow(ctx, `SELECT jsonb_build_array(
   (SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY id),'[]') FROM files f),
   (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM upload_sessions s),
   (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM file_shares s),
   (SELECT COALESCE(jsonb_agg(to_jsonb(b) ORDER BY batch_id),'[]') FROM import_batches b),
   (SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY batch_id,destination_ordinal),'[]') FROM import_destination_pins p),
   (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),
   (SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM user_events e),
   (SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY id),'[]') FROM hierarchy_jobs j))::text`).Scan(&value)
				if e != nil {
					t.Fatal(e)
				}
				return value
			}
			before := snapshot()
			if mode == "rollback" {
				queue.failure = fixture.ErrProcessLost
			}
			target := rootID
			if mode == "direct" {
				target = childID
			}
			if mode == "replace" {
				outside, err := service.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "outside"})
				if err != nil {
					t.Fatal(err)
				}
				outsideID := uuid.UUID(outside.ID.Bytes)
				replacement, err := service.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &outsideID, Name: "root"})
				if err != nil {
					t.Fatal(err)
				}
				_, err = service.MoveWithPolicy(ctx, 1001, uuid.UUID(replacement.ID.Bytes), nil, nil, "replace")
			} else if mode == "bulk_overlap" {
				_, err = service.BulkTrash(ctx, 1001, []uuid.UUID{rootID, childID})
			} else {
				_, err = service.Trash(ctx, 1001, target)
			}
			if mode == "rollback" {
				if !errors.Is(err, fixture.ErrProcessLost) {
					t.Fatalf("enqueue rollback: %v", err)
				}
				if before != snapshot() {
					t.Fatal("deletion rollback leaked hierarchy/share/session/import/receipt/event/job changes")
				}
				if e := storage.CheckPreserved(); e != nil {
					t.Fatal(e)
				}
				raw, e := uploader.Get(ctx, 1001, uuid.UUID(session.ID.Bytes))
				if e != nil || raw.TerminalFailureCode.Valid {
					t.Fatalf("rollback leaked terminal session %v", e)
				}
				var n int
				if e = db.Pool.QueryRow(ctx, "SELECT count(*) FROM hierarchy_jobs").Scan(&n); e != nil || n != 0 {
					t.Fatalf("rollback leaked job %d %v", n, e)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "ancestor_purge" {
				operations, e := fileops.NewService(db.Pool, service, channels.NewService(db.Pool, nil, channels.Config{}), storage)
				if e != nil {
					t.Fatal(e)
				}
				if e = operations.BindLifecycle(policy); e != nil {
					t.Fatal(e)
				}
				if e = operations.QueuePurge(ctx, 1001, target); e != nil {
					t.Fatal(e)
				}
				if e = operations.PurgeMany(ctx, 1001, []uuid.UUID{target}); e != nil {
					t.Fatal(e)
				}
				var remaining int
				if e = db.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE id=ANY($1)", []uuid.UUID{rootID, childID}).Scan(&remaining); e != nil || remaining != 0 {
					t.Fatalf("actual ancestor purge did not finalize %d %v", remaining, e)
				}
			}
			var terminal string
			if err = db.Pool.QueryRow(ctx, "SELECT terminal_failure_code FROM remote_write_receipts WHERE id=$1", operation).Scan(&terminal); err != nil || terminal != "copy_destination_unavailable" {
				t.Fatalf("copy fence: %s %v", terminal, err)
			}
			if _, err = uploader.Get(ctx, 1001, uuid.UUID(session.ID.Bytes)); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("session survived ancestor loss %v", err)
			}
			b, err := uploader.GetImportBatch(ctx, 1001, batch, 101)
			if err != nil || !b.TerminalFailureCode.Valid {
				t.Fatalf("Q pin escaped loss %+v %v", b, err)
			}
			if mode != "replace" && mode != "ancestor_purge" {
				if _, err = service.Restore(ctx, 1001, target); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = uploader.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: uuid.UUID(session.ID.Bytes), Size: 0}); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("restore revived loser %v", err)
			}
			if err := storage.CheckPreserved(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestB2HierarchyMutationOrder(t *testing.T) {
	for _, mutation := range []string{"create", "rename", "move", "bulk_move", "trash", "bulk_trash", "restore"} {
		t.Run(mutation, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := db.Pool.Exec(ctx, "INSERT INTO users(user_id) VALUES(1001)"); err != nil {
				t.Fatal(err)
			}
			service := catalog.NewService(db.Pool, nil)
			storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001}}})
			ledger := lifecycle.NewService(db.Pool, storage)
			if err := ledger.BindReceiptEnqueuer(&hierarchyQueue{}); err != nil {
				t.Fatal(err)
			}
			if err := service.BindLifecycle(ledger); err != nil {
				t.Fatal(err)
			}
			file, err := service.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "entry"})
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.UUID(file.ID.Bytes)
			if mutation == "restore" {
				if _, err = service.Trash(ctx, 1001, id); err != nil {
					t.Fatal(err)
				}
			}
			blocker, err := db.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(ctx)
			if err = lifecycle.LockOwnerTx(ctx, blocker, 1001); err != nil {
				t.Fatal(err)
			}
			var blockerPID uint32
			if err = blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				switch mutation {
				case "create":
					_, err = service.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "new"})
				case "rename":
					_, err = service.Rename(ctx, 1001, id, nil, "renamed")
				case "move":
					_, err = service.Move(ctx, 1001, id, nil, nil)
				case "bulk_move":
					_, err = service.BulkMove(ctx, 1001, []uuid.UUID{id}, nil, "fail")
				case "trash":
					_, err = service.Trash(ctx, 1001, id)
				case "bulk_trash":
					_, err = service.BulkTrash(ctx, 1001, []uuid.UUID{id})
				case "restore":
					_, err = service.Restore(ctx, 1001, id)
				}
				done <- err
			}()
			// Observe the actual SQL waiter from an independent pool connection.
			var waiter uint32
			for waiter == 0 {
				if err = ctx.Err(); err != nil {
					t.Fatal(err)
				}
				if err = db.Pool.QueryRow(ctx, "SELECT COALESCE((SELECT pid FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid)) LIMIT 1),0)", blockerPID).Scan(&waiter); err != nil {
					t.Fatal(err)
				}
			}
			if err = fixture.WaitForBlocking(ctx, db.Pool, waiter, blockerPID); err != nil {
				t.Fatal(err)
			}
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
