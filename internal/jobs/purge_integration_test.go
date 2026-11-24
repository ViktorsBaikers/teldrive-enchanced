//go:build integration

package jobs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestPendingFilePurgeWorkerProcessesDeletionPendingRoots(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES (1001)"); err != nil {
		t.Fatal(err)
	}
	rootID := uuid.New()
	childID := uuid.New()
	secondRootID := uuid.New()
	activeID := uuid.New()
	if _, err := db.Pool.Exec(ctx, `
INSERT INTO files (id, user_id, parent_id, name, kind, size, status, mod_time, deleted_at)
VALUES
    ($1, 1001, NULL, 'root', 'folder', NULL, 'deletion_pending', now(), now()),
    ($2, 1001, $1, 'child', 'file', 0, 'deletion_pending', now(), now()),
    ($3, 1001, NULL, 'second-root', 'file', 0, 'deletion_pending', now(), now()),
    ($4, 1001, NULL, 'active', 'file', 0, 'active', now(), NULL)
`, rootID, childID, secondRootID, activeID); err != nil {
		t.Fatal(err)
	}

	service := &recordingPurgeService{after: func(ctx context.Context, userID int64, fileID uuid.UUID) error {
		if _, err := db.Pool.Exec(ctx, "DELETE FROM files WHERE user_id = $1 AND parent_id = $2", userID, fileID); err != nil {
			return err
		}
		_, err := db.Pool.Exec(ctx, "DELETE FROM files WHERE user_id = $1 AND id = $2", userID, fileID)
		return err
	}}
	worker := jobs.NewPendingFilePurgeWorker(db.Pool, service)
	job := &river.Job[jobs.PurgeSweepArgs]{Args: jobs.PurgeSweepArgs{}}
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	calls := service.callsSnapshot()
	if len(calls) != 2 || calls[0].userID != 1001 || calls[1].userID != 1001 {
		t.Fatalf("purge calls = %#v", calls)
	}
	calledIDs := map[uuid.UUID]bool{calls[0].fileID: true, calls[1].fileID: true}
	if !calledIDs[rootID] || !calledIDs[secondRootID] {
		t.Fatalf("purge calls = %#v", calls)
	}
	if batches := service.batchesSnapshot(); len(batches) != 1 || batches[0] != 2 {
		t.Fatalf("purge batches = %v, want [2]", batches)
	}

	if got := (jobs.PurgeSweepArgs{}).Kind(); got != jobs.PurgeSweepKind {
		t.Fatalf("Kind() = %q", got)
	}
	opts := (jobs.PurgeSweepArgs{}).InsertOpts()
	if opts.Queue != jobs.PurgeQueue || opts.MaxAttempts != 3 || opts.Priority != 1 {
		t.Fatalf("InsertOpts() = %#v", opts)
	}
	if got := worker.Timeout(job); got != 2*time.Hour {
		t.Fatalf("Timeout() = %s", got)
	}

	if err := (*jobs.PendingFilePurgeWorker)(nil).Work(ctx, job); !errors.Is(err, jobs.ErrPurgeNotConfigured) {
		t.Fatalf("nil worker error = %v", err)
	}
	if err := jobs.NewPendingFilePurgeWorker(db.Pool, nil).Work(ctx, job); !errors.Is(err, jobs.ErrPurgeNotConfigured) {
		t.Fatalf("nil service error = %v", err)
	}
}

func TestPendingFilePurgeWorkerDrainsMultiplePages(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES (1001)"); err != nil {
		t.Fatal(err)
	}
	// The fixture stands in for an already-populated library; the ordinary 512
	// append bound is proven by TestB3BulkCatalogLane.
	seedTx, seedErr := db.Pool.Begin(ctx)
	if seedErr != nil {
		t.Fatal(seedErr)
	}
	defer seedTx.Rollback(ctx)
	if _, seedErr = seedTx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); seedErr != nil {
		t.Fatal(seedErr)
	}
	if _, seedErr = seedTx.Exec(ctx, `
INSERT INTO files (user_id, name, kind, size, status, mod_time, deleted_at)
SELECT 1001, 'pending-' || value, 'file', 0, 'deletion_pending', now(), now()
FROM generate_series(1, 1001) AS value
`); seedErr != nil {
		t.Fatal(seedErr)
	}
	if seedErr = seedTx.Commit(ctx); seedErr != nil {
		t.Fatal(seedErr)
	}

	service := &recordingPurgeService{after: func(ctx context.Context, userID int64, fileID uuid.UUID) error {
		_, err := db.Pool.Exec(ctx, "DELETE FROM files WHERE user_id = $1 AND id = $2", userID, fileID)
		return err
	}}
	worker := jobs.NewPendingFilePurgeWorker(db.Pool, service)
	if err := worker.Work(ctx, &river.Job[jobs.PurgeSweepArgs]{Args: jobs.PurgeSweepArgs{}}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if calls := service.callsSnapshot(); len(calls) != 1001 {
		t.Fatalf("purge calls = %d, want 1001", len(calls))
	}
	if batches := service.batchesSnapshot(); len(batches) != 2 || batches[0] != 1000 || batches[1] != 1 {
		t.Fatalf("purge batches = %v, want [1000 1]", batches)
	}
	var remaining int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE status = 'deletion_pending'").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("deletion-pending files = %d, want 0", remaining)
	}
}

func TestPendingFilePurgeWorkerReturnsServiceFailure(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES (1001)"); err != nil {
		t.Fatal(err)
	}
	fileID := uuid.New()
	if _, err := db.Pool.Exec(ctx, `
INSERT INTO files (id, user_id, name, kind, size, status, mod_time, deleted_at)
VALUES ($1, 1001, 'pending', 'file', 0, 'deletion_pending', now(), now())
`, fileID); err != nil {
		t.Fatal(err)
	}
	serviceErr := errors.New("purge failed")
	worker := jobs.NewPendingFilePurgeWorker(db.Pool, &recordingPurgeService{err: serviceErr})
	if err := worker.Work(ctx, &river.Job[jobs.PurgeSweepArgs]{Args: jobs.PurgeSweepArgs{}}); !errors.Is(err, serviceErr) {
		t.Fatalf("Work() error = %v", err)
	}
}

type purgeCall struct {
	userID int64
	fileID uuid.UUID
}

type recordingPurgeService struct {
	mu      sync.Mutex
	calls   []purgeCall
	batches []int
	err     error
	after   func(context.Context, int64, uuid.UUID) error
}

func (s *recordingPurgeService) PurgeMany(ctx context.Context, userID int64, fileIDs []uuid.UUID) error {
	s.mu.Lock()
	s.batches = append(s.batches, len(fileIDs))
	s.mu.Unlock()
	for _, fileID := range fileIDs {
		if err := s.Purge(ctx, userID, fileID); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingPurgeService) Purge(ctx context.Context, userID int64, fileID uuid.UUID) error {
	s.mu.Lock()
	s.calls = append(s.calls, purgeCall{userID: userID, fileID: fileID})
	err, after := s.err, s.after
	s.mu.Unlock()
	if err != nil || after == nil {
		return err
	}
	return after(ctx, userID, fileID)
}

func (s *recordingPurgeService) callsSnapshot() []purgeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]purgeCall(nil), s.calls...)
}

func (s *recordingPurgeService) batchesSnapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.batches...)
}

func TestB2PurgeSweepOrdinaryTerminalParents(t *testing.T) {
	for _, state := range []string{"aborted", "expired"} {
		t.Run(state, func(t *testing.T) {
			f := newCleanupFixture(t)
			ctx := context.Background()
			c := catalog.NewService(f.pool, nil)
			if err := c.BindLifecycle(f.policy); err != nil {
				t.Fatal(err)
			}
			service, err := fileops.NewService(f.pool, c, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
			if err != nil {
				t.Fatal(err)
			}
			if err = service.BindLifecycle(f.policy); err != nil {
				t.Fatal(err)
			}
			folder, err := c.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "terminal-parent"})
			if err != nil {
				t.Fatal(err)
			}
			parent := uuid.UUID(folder.ID.Bytes)
			session, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &parent, Name: "terminal-with-part", ExpectedSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.UUID(session.ID.Bytes)
			if _, err = f.uploads.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: 4}); err != nil {
				t.Fatal(err)
			}
			if state == "aborted" {
				if _, err = f.uploads.Abort(ctx, 1001, id); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				if _, err = sqlcgen.New(f.pool).ExpireUploadSessions(ctx, dbtypes.Time(time.Now())); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := func() string {
				t.Helper()
				var value string
				if err := f.pool.QueryRow(ctx, `SELECT jsonb_build_array(
 (SELECT to_jsonb(s)-'parent_id'-'updated_at' FROM upload_sessions s WHERE id=$1),
 (SELECT jsonb_agg(to_jsonb(p) ORDER BY part_no) FROM upload_parts p WHERE upload_id=$1))::text`, id).Scan(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := snapshot()
			if _, err = c.Trash(ctx, 1001, parent); err != nil {
				t.Fatal(err)
			}
			if err = service.QueuePurge(ctx, 1001, parent); err != nil {
				t.Fatal(err)
			}
			if _, err = recordedCleanup(t, f.pool, jobs.NewPendingFilePurgeWorker(f.pool, service), jobs.PurgeSweepArgs{}); err != nil {
				t.Fatal(err)
			}
			var removed, detached, terminal bool
			var parts int
			if err = f.pool.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM files WHERE id=$1),
 parent_id IS NULL,state=$3::upload_state AND terminal_failure_code IS NULL,
 (SELECT count(*) FROM upload_parts WHERE upload_id=$2) FROM upload_sessions WHERE id=$2`, parent, id, state).Scan(&removed, &detached, &terminal, &parts); err != nil {
				t.Fatal(err)
			}
			if !removed || !detached || !terminal || parts != 1 || snapshot() != before || len(f.storage.Deletes()) != 0 {
				t.Fatalf("terminal sweep changed evidence: removed=%v detached=%v terminal=%v parts=%d", removed, detached, terminal, parts)
			}
		})
	}
}

func TestB2PurgeSweepContinuation(t *testing.T) {
	testRetainedCatalogSweep(t, false, false)
}

func TestB2SweepFence(t *testing.T) {
	t.Run("pending_then_upload", func(t *testing.T) { testRetainedCatalogSweep(t, false, true) })
	t.Run("trash_then_pending_then_upload", func(t *testing.T) { testRetainedCatalogSweep(t, true, true) })
}

func testRetainedCatalogSweep(t *testing.T, trash, includeUpload bool) {
	t.Helper()
	f := newCleanupFixture(t)
	ctx := context.Background()
	c := catalog.NewService(f.pool, nil)
	if err := c.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
	service, err := fileops.NewService(f.pool, c, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
	var emptyRaw json.RawMessage
	if trash {
		emptyRaw, err = recordedCleanup(t, f.pool, jobs.NewTrashCleanupWorker(f.pool, service), jobs.TrashCleanupSweepArgs{Retention: "720h"})
	} else {
		emptyRaw, err = recordedCleanup(t, f.pool, jobs.NewPendingFilePurgeWorker(f.pool, service), jobs.PurgeSweepArgs{})
	}
	if err != nil {
		t.Fatal(err)
	}
	var empty jobs.PurgeSweepOutput
	if err = json.Unmarshal(emptyRaw, &empty); err != nil {
		t.Fatal(err)
	}
	if empty.Pages != 1 || empty.Roots != 0 || empty.Incomplete {
		t.Fatalf("empty pass=%s", emptyRaw)
	}
	status := "deletion_pending"
	if trash {
		status = "trashed"
	}
	// Unknown legacy references are deliberately not attested as candidate ownership.
	// The fixture stands in for an already-populated library; the ordinary 512
	// append bound is proven by TestB3BulkCatalogLane.
	seedTx, seedErr := f.pool.Begin(ctx)
	if seedErr != nil {
		t.Fatal(seedErr)
	}
	defer seedTx.Rollback(ctx)
	if _, seedErr = seedTx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); seedErr != nil {
		t.Fatal(seedErr)
	}
	if _, seedErr = seedTx.Exec(ctx, `WITH inserted AS (
 INSERT INTO files(id,user_id,name,kind,size,status,mod_time,deleted_at,created_at)
 SELECT ('00000000-0000-0000-0001-'||lpad(n::text,12,'0'))::uuid,1001,'retained-'||n,'file',1,$1::file_status,
 '2000-01-01','2000-01-01','2000-01-01' FROM generate_series(1,1000)n RETURNING id)
 INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size)
 SELECT id,1,9001,10000+row_number() OVER(ORDER BY id),1,1 FROM inserted`, status); seedErr != nil {
		t.Fatal(seedErr)
	}
	if seedErr = seedTx.Commit(ctx); seedErr != nil {
		t.Fatal(seedErr)
	}
	var retained []uuid.UUID
	for n := 1; n <= 1000; n++ {
		retained = append(retained, uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0001-%012d", n)))
		if err = f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: int64(10000 + n), Bytes: []byte("x"), Preserve: true}); err != nil {
			t.Fatal(err)
		}
	}
	physical := make(map[int64]int64)
	origins := make(map[int64]uuid.UUID)
	complete := func(owner, channel, message int64) uuid.UUID {
		t.Helper()
		body := []byte(fmt.Sprintf("independent synthetic payload %d/%d", channel, message))
		session, e := f.uploads.Create(ctx, uploads.CreateInput{UserID: owner, Name: fmt.Sprintf("pipeline-%d", message), ExpectedSize: int64(len(body)), PartSize: treehash.BlockSize})
		if e != nil {
			t.Fatal(e)
		}
		id := uuid.UUID(session.ID.Bytes)
		pipeline := transfer.NewPipeline(f.uploads, channels.NewService(f.pool, nil, channels.Config{}), f.storage, nil, transfer.Config{Receipts: f.policy})
		result, e := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: owner, UploadID: id, PartNo: 1, RequestedChannelID: channel, PlainSize: int64(len(body)), Body: bytes.NewReader(body)})
		if e != nil {
			t.Fatal(e)
		}
		assertCleanupBytes(t, f.storage, channel, result.Part.MessageID.Int64, body)
		physical[message] = result.Part.MessageID.Int64
		origins[message] = id
		file, e := f.uploads.Complete(ctx, uploads.CompleteInput{UserID: owner, UploadID: id, Size: int64(len(body))})
		if e != nil {
			t.Fatal(e)
		}
		fileID := uuid.UUID(file.ID.Bytes)
		var aliases int
		if e = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&aliases); e != nil || aliases != 0 {
			t.Fatalf("completed aliases=%d: %v", aliases, e)
		}
		if _, e = c.Trash(ctx, owner, fileID); e != nil {
			t.Fatal(e)
		}
		if !trash {
			if e = service.QueuePurge(ctx, owner, fileID); e != nil {
				t.Fatal(e)
			}
		}
		if _, e = f.pool.Exec(ctx, "UPDATE files SET created_at='2000-01-02',deleted_at='2000-01-01' WHERE id=$1", fileID); e != nil {
			t.Fatal(e)
		}
		return fileID
	}
	failed := complete(1002, 9002, 3001)
	eligible := complete(1003, 9003, 3002)
	protected := complete(1001, 9001, 2001)
	exhausted := complete(1001, 9001, 2002)
	// An unrelated all-state alias vetoes deletion even for an owned completed origin.
	if _, err = f.pool.Exec(ctx, `WITH s AS (
 INSERT INTO upload_sessions(user_id,name,expected_size,mod_time,part_size,expires_at)
 VALUES(1002,'independent protected alias',1,now(),16777216,now()+interval '1 day') RETURNING id)
 INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,state)
 SELECT id,1,9001,$1,1,1,'stored' FROM s`, physical[2001]); err != nil {
		t.Fatal(err)
	}
	f.storage.fail[9001] = errors.New("synthetic lifetime exhaustion")
	for range 4 {
		if err = service.Purge(ctx, 1001, exhausted); err == nil {
			t.Fatal("exhaustion fixture reported success")
		}
		if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE upload_id=$1", origins[2002]); err != nil {
			t.Fatal(err)
		}
	}
	delete(f.storage.fail, 9001)
	var attempts int
	if err = f.pool.QueryRow(ctx, "SELECT attempts FROM remote_write_receipts WHERE upload_id=$1", origins[2002]).Scan(&attempts); err != nil || attempts != 4 {
		t.Fatalf("exhausted lifetime=%d: %v", attempts, err)
	}
	exhaustedIDs := []uuid.UUID{exhausted, origins[2002]}
	exhaustedBefore := cleanupSnapshot(t, f.pool, exhaustedIDs)
	f.storage.fail[9002] = errors.New("synthetic inaccessible owner")
	var uploadRetained []uuid.UUID
	var uploadBefore string
	if includeUpload {
		// The fixture stands in for an already-populated library; the ordinary
		// 512 append bound is proven by TestB3BulkCatalogLane.
		seedTx, seedErr := f.pool.Begin(ctx)
		if seedErr != nil {
			t.Fatal(seedErr)
		}
		defer seedTx.Rollback(ctx)
		if _, seedErr = seedTx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); seedErr != nil {
			t.Fatal(seedErr)
		}
		if _, seedErr = seedTx.Exec(ctx, `WITH s AS (
 INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,expires_at,state,created_at)
 SELECT ('00000000-0000-0000-0002-'||lpad(n::text,12,'0'))::uuid,1001,'retained-upload-'||n,1,'2000-01-01',16777216,'2000-01-02','aborted','2000-01-01'
 FROM generate_series(1,1000)n RETURNING id)
 INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,state)
 SELECT id,1,9001,20000+row_number() OVER(ORDER BY id),1,1,'stored' FROM s`); seedErr != nil {
			t.Fatal(seedErr)
		}
		if seedErr = seedTx.Commit(ctx); seedErr != nil {
			t.Fatal(seedErr)
		}
		for n := 1; n <= 1000; n++ {
			uploadRetained = append(uploadRetained, uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0002-%012d", n)))
			if err = f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: int64(20000 + n), Bytes: []byte("u"), Preserve: true}); err != nil {
				t.Fatal(err)
			}
		}
		protectedUpload, _ := f.owned(t, 1001, 9001, 5001, true)
		exhaustedUpload, exhaustedReceipt := f.owned(t, 1001, 9001, 5002, true)
		if _, err = f.pool.Exec(ctx, `WITH f AS (
 INSERT INTO files(user_id,name,kind,size,mod_time)
 SELECT 1002,'independent terminal-upload alias','file',expected_size,now()
 FROM upload_sessions WHERE id=$1 RETURNING id,size)
 INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size)
 SELECT id,1,9001,5001,size,size FROM f`, protectedUpload); err != nil {
			t.Fatal(err)
		}
		f.storage.fail[9001] = errors.New("synthetic terminal-upload exhaustion")
		tx, e := f.pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		e = f.policy.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{exhaustedReceipt}, PrimaryCode: "upload_cleanup"})
		if e == nil {
			e = tx.Commit(ctx)
		}
		_ = tx.Rollback(ctx)
		if e != nil {
			t.Fatal(e)
		}
		for range 4 {
			if err = f.policy.Recover(ctx, 1001, []uuid.UUID{exhaustedReceipt}); err == nil {
				t.Fatal("terminal-upload exhaustion reported success")
			}
			if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", exhaustedReceipt); err != nil {
				t.Fatal(err)
			}
		}
		delete(f.storage.fail, 9001)
		uploadRetained = append(uploadRetained, protectedUpload, exhaustedUpload)
		f.owned(t, 1002, 9002, 5003, true)
		uploadBefore = cleanupSnapshot(t, f.pool, uploadRetained)
	}
	before := cleanupSnapshot(t, f.pool, retained)
	for pass := range 3 {
		if trash && pass > 0 {
			pages := f.trace.Count("ListTrashedRootsBefore")
			raw, e := recordedCleanup(t, f.pool, jobs.NewTrashCleanupWorker(f.pool, service), jobs.TrashCleanupSweepArgs{Retention: "720h"})
			var empty jobs.PurgeSweepOutput
			if e != nil {
				t.Fatal(e)
			}
			if e = json.Unmarshal(raw, &empty); e != nil {
				t.Fatal(e)
			}
			if empty.Pages != 1 || empty.Roots != 0 || empty.Incomplete || f.trace.Count("ListTrashedRootsBefore")-pages != 1 {
				t.Fatalf("empty repeated trash pass=%s", raw)
			}
		}
		query := "ListDeletionPendingRoots"
		if trash && pass == 0 {
			query = "ListTrashedRootsBefore"
		}
		selectedStatus := "deletion_pending"
		if trash && pass == 0 {
			selectedStatus = "trashed"
		}
		var lastSelected uuid.UUID
		if err = f.pool.QueryRow(ctx, `SELECT id FROM (
 SELECT id,created_at FROM files WHERE status=$1::file_status AND parent_id IS NULL
 ORDER BY created_at,id LIMIT 1000) page ORDER BY created_at DESC,id DESC LIMIT 1`, selectedStatus).Scan(&lastSelected); err != nil {
			t.Fatal(err)
		}
		pages, cutoffs := f.trace.Count(query), f.trace.Count("GetCleanupSweepCutoff")
		var raw json.RawMessage
		var workErr error
		if trash && pass == 0 {
			raw, workErr = recordedCleanup(t, f.pool, jobs.NewTrashCleanupWorker(f.pool, service), jobs.TrashCleanupSweepArgs{Retention: "720h"})
		} else {
			raw, workErr = recordedCleanup(t, f.pool, jobs.NewPendingFilePurgeWorker(f.pool, service), jobs.PurgeSweepArgs{})
		}
		if workErr == nil {
			t.Fatal("retained physical work reported success")
		}
		var output jobs.PurgeSweepOutput
		if err = json.Unmarshal(raw, &output); err != nil {
			t.Fatal(err)
		}
		wantRoots := 1003
		if pass == 0 && !trash {
			wantRoots++
		}
		if pass == 1 {
			wantRoots++
		}
		if output.Pages != 2 || output.Roots != wantRoots || !output.Incomplete || output.FailedBatches < 1 {
			t.Fatalf("false sweep progress: %s", raw)
		}
		if got := f.trace.Count(query) - pages; got != 2 {
			t.Fatalf("candidate pages=%d, want 2", got)
		}
		assertSweepSecondCursor(t, f.trace, query, pages, lastSelected)
		if got := f.trace.Count("GetCleanupSweepCutoff") - cutoffs; got != 1 {
			t.Fatalf("cutoff reads=%d", got)
		}
		// Trash changes status once; snapshot after that transition, then require exact stability.
		if trash && pass == 0 {
			before = cleanupSnapshot(t, f.pool, retained)
		}
		if got := cleanupSnapshot(t, f.pool, retained); got != before {
			t.Fatal("retained evidence changed")
		}
		if got := cleanupSnapshot(t, f.pool, exhaustedIDs); got != exhaustedBefore {
			t.Fatal("exhausted receipt, budget or catalog changed")
		}
		for _, spec := range []struct {
			id      uuid.UUID
			message int64
		}{{protected, 2001}, {exhausted, 2002}} {
			var n int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE id=$1", spec.id).Scan(&n); err != nil || n != 1 {
				t.Fatalf("retained owned root=%d: %v", n, err)
			}
			assertCleanupBytes(t, f.storage, 9001, physical[spec.message], []byte(fmt.Sprintf("independent synthetic payload %d/%d", 9001, spec.message)))
		}
		if pass == 0 {
			behind := complete(1003, 9003, 3003)
			if err = service.QueuePurge(ctx, 1003, behind); err != nil {
				t.Fatal(err)
			}
			if _, err = f.pool.Exec(ctx, "UPDATE files SET created_at='1999-01-01' WHERE id=$1", behind); err != nil {
				t.Fatal(err)
			}
		}
		var present int
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE id=$1", eligible).Scan(&present); err != nil || present != 0 {
			t.Fatalf("later eligible catalog retained=%d: %v", present, err)
		}
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE id=$1", failed).Scan(&present); err != nil || present != 1 {
			t.Fatalf("failed owner evidence lost=%d: %v", present, err)
		}
		assertCleanupBytes(t, f.storage, 9002, physical[3001], []byte("independent synthetic payload 9002/3001"))
		var purged int
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE event_type='file.purged' AND resource_id=$1", eligible.String()).Scan(&purged); err != nil || purged != 1 {
			t.Fatalf("ACK-backed purge events=%d: %v", purged, err)
		}
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE event_type='file.purged' AND resource_id=ANY($1::text[])", []string{protected.String(), exhausted.String(), failed.String()}).Scan(&purged); err != nil || purged != 0 {
			t.Fatalf("false physical purge events=%d: %v", purged, err)
		}
		if includeUpload {
			id, _ := f.owned(t, 1003, 9003, int64(4000+pass), true)
			pages := f.trace.Count("ListUploadSessionsPendingCleanup")
			if err = f.run(t); !errors.Is(err, jobs.ErrUploadCleanupIncomplete) {
				t.Fatalf("upload aggregate=%v", err)
			}
			if got := f.trace.Count("ListUploadSessionsPendingCleanup") - pages; got != 2 {
				t.Fatalf("upload retained pages=%d", got)
			}
			assertSweepSecondCursor(t, f.trace, "ListUploadSessionsPendingCleanup", pages, uploadRetained[999])
			if after := cleanupSnapshot(t, f.pool, uploadRetained); after != uploadBefore {
				t.Fatal("upload retained evidence changed")
			}
			for _, message := range []int64{5001, 5002} {
				assertCleanupBytes(t, f.storage, 9001, message, []byte(fmt.Sprintf("independent synthetic payload %d/%d", 9001, message)))
			}
			var parts int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&parts); err != nil || parts != 0 {
				t.Fatalf("upload sweep ACK parts=%d: %v", parts, err)
			}
		}
	}
	want := []string{fmt.Sprintf("1003/9003/%d", physical[3002]), fmt.Sprintf("1003/9003/%d", physical[3003])}
	if includeUpload {
		want = append(want, "1003/9003/4000", "1003/9003/4001", "1003/9003/4002")
	}
	cleanupDeleted(t, f.storage, want...)
	for _, message := range []int64{physical[2002], 5002} {
		if message == 5002 && !includeUpload {
			continue
		}
		calls := 0
		for _, call := range f.storage.Deletes() {
			if call.ChannelID != 9001 {
				continue
			}
			for _, id := range call.MessageIDs {
				if id == message {
					calls++
				}
			}
		}
		if calls != 4 {
			t.Fatalf("lifetime invocation count for %d=%d, want 4", message, calls)
		}
	}
}

// TestB3BulkCatalogLifecycle proves the catalog-wide bulk lane: a folder with
// more than 512 descendants is trashed, marked deletion-pending and purged,
// each operation in one transaction, without the ordinary row-count bound.
func TestB3BulkCatalogLifecycle(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	c := catalog.NewService(f.pool, nil)
	if err := c.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
	service, err := fileops.NewService(f.pool, c, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}

	var parentID uuid.UUID
	if err := f.pool.QueryRow(ctx, `
		INSERT INTO files (user_id, name, kind, status, mod_time)
		VALUES (1001, 'bulk-parent', 'folder', 'active', now())
		RETURNING id`).Scan(&parentID); err != nil {
		t.Fatalf("seed bulk parent: %v", err)
	}
	const children = 1003
	for start := 1; start <= children; start += 400 {
		end := start + 399
		if end > children {
			end = children
		}
		if _, err := f.pool.Exec(ctx, `
			INSERT INTO files (id, user_id, parent_id, name, kind, size, status, mod_time)
			SELECT ('00000000-0000-0000-0002-'||lpad(n::text,12,'0'))::uuid,1001,$1,'bulk-'||n,'file',0,'active',now()
			FROM generate_series($2::int,$3::int) n`, parentID, start, end); err != nil {
			t.Fatalf("seed bulk children %d-%d: %v", start, end, err)
		}
	}

	// Each of these is one transaction over the whole 1004-row subtree.
	if _, err := c.Trash(ctx, 1001, parentID); err != nil {
		t.Fatalf("bulk trash of 1004 rows: %v", err)
	}
	var trashed int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM files
		WHERE user_id = 1001 AND (id = $1 OR parent_id = $1) AND status = 'trashed'`,
		parentID).Scan(&trashed); err != nil {
		t.Fatalf("count trashed bulk tree: %v", err)
	}
	if trashed != children+1 {
		t.Fatalf("trashed rows = %d, want %d", trashed, children+1)
	}
	if err := service.QueuePurge(ctx, 1001, parentID); err != nil {
		t.Fatalf("bulk mark deletion-pending: %v", err)
	}
	var pending int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM files
		WHERE user_id = 1001 AND (id = $1 OR parent_id = $1) AND status = 'deletion_pending'`,
		parentID).Scan(&pending); err != nil {
		t.Fatalf("count pending bulk tree: %v", err)
	}
	if pending != children+1 {
		t.Fatalf("deletion-pending rows = %d, want %d", pending, children+1)
	}
	if err := service.Purge(ctx, 1001, parentID); err != nil {
		t.Fatalf("bulk purge of 1004 rows: %v", err)
	}
	var remaining int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM files WHERE user_id = 1001 AND (id = $1 OR parent_id = $1)`,
		parentID).Scan(&remaining); err != nil {
		t.Fatalf("count remaining bulk tree: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("bulk purge left %d rows", remaining)
	}
	var events, unpositioned int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE strict_position IS NULL)
		FROM user_events WHERE user_id = 1001 AND event_type IN ('file.trashed','file.deletion_pending','file.purged')`).
		Scan(&events, &unpositioned); err != nil {
		t.Fatalf("inspect bulk events: %v", err)
	}
	if events < 3*(children+1) || unpositioned != 0 {
		t.Fatalf("bulk events = %d unpositioned = %d", events, unpositioned)
	}
}
