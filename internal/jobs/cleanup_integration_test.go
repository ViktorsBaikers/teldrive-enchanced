//go:build integration

package jobs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/testutil/querytrace"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

// This producer proves caller-Tx identity/rollback only, not Runtime scheduling.
type cleanupQueue struct{}

func (cleanupQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO cleanup_receipt_jobs(user_id) VALUES($1) RETURNING id", in.UserID).Scan(&id)
	return id, err
}

type cleanupStorage struct {
	*fixture.Storage
	fail      map[int64]error
	pages     map[int64][]telegramstore.DocumentMessagePage
	listError map[int64]error
	requests  []telegramstore.ListDocumentMessagesRequest
}

func (s *cleanupStorage) DeleteMessages(ctx context.Context, owner, channel int64, ids []int64) error {
	if err := s.fail[channel]; err != nil {
		s.Storage.SetDeleteFaults(fixture.DeleteFaults{Error: err})
	} else {
		s.Storage.SetDeleteFaults(fixture.DeleteFaults{})
	}
	return s.Storage.DeleteMessages(ctx, owner, channel, ids)
}

func (s *cleanupStorage) ListDocumentMessages(ctx context.Context, in telegramstore.ListDocumentMessagesRequest) (telegramstore.DocumentMessagePage, error) {
	s.requests = append(s.requests, in)
	if err := ctx.Err(); err != nil {
		return telegramstore.DocumentMessagePage{}, err
	}
	if err := s.listError[in.ChannelID]; err != nil {
		return telegramstore.DocumentMessagePage{}, err
	}
	pages := s.pages[in.ChannelID]
	for i, page := range pages {
		if (i == 0 && in.BeforeID == 0) || (i > 0 && in.BeforeID == pages[i-1].BeforeID) {
			return page, nil
		}
	}
	return telegramstore.DocumentMessagePage{Exhausted: true}, nil
}

// Keep exact cursor arguments alongside counts without changing the production query seam.
type cleanupSweepTrace struct {
	querytrace.Counter
	mu   sync.Mutex
	args map[string][][]any
}

func (c *cleanupSweepTrace) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = c.Counter.TraceQueryStart(ctx, conn, data)
	for _, name := range []string{"ListDeletionPendingRoots", "ListTrashedRootsBefore", "ListUploadSessionsPendingCleanup"} {
		if strings.HasPrefix(data.SQL, "-- name: "+name+" ") {
			c.mu.Lock()
			c.args[name] = append(c.args[name], append([]any(nil), data.Args...))
			c.mu.Unlock()
		}
	}
	return ctx
}

func (c *cleanupSweepTrace) arguments(name string) [][]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]any(nil), c.args[name]...)
}

func assertSweepSecondCursor(t testing.TB, trace *cleanupSweepTrace, name string, start int, id uuid.UUID) {
	t.Helper()
	args := trace.arguments(name)[start:]
	if len(args) != 2 {
		t.Fatalf("%s cursor pages=%d, want 2", name, len(args))
	}
	var fixed []time.Time
	for _, arg := range args[0] {
		if value, ok := arg.(pgtype.Timestamptz); ok && value.Valid {
			fixed = append(fixed, value.Time)
		}
	}
	for _, want := range fixed {
		found := false
		for _, arg := range args[1] {
			if value, ok := arg.(pgtype.Timestamptz); ok && value.Valid && value.Time.Equal(want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s changed fixed pass/retention cutoff", name)
		}
	}
	for page, row := range args {
		var cursor pgtype.UUID
		var found bool
		pairedTime := false
		for _, arg := range row {
			if value, ok := arg.(pgtype.Timestamptz); ok {
				if page == 0 && !value.Valid {
					pairedTime = true
				}
				if page == 1 && value.Valid && value.Time.Equal(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) {
					pairedTime = true
				}
			}
		}
		if !pairedTime {
			t.Fatalf("%s page%d lost paired immutable timestamp", name, page)
		}
		for _, arg := range row {
			if value, ok := arg.(pgtype.UUID); ok {
				cursor, found = value, true
			}
		}
		if !found || (page == 0 && cursor.Valid) || (page == 1 && (!cursor.Valid || cursor.Bytes != [16]byte(id))) {
			t.Fatalf("%s page%d did not use last-selected immutable ID: %v", name, page, row)
		}
	}
}

type cleanupFixture struct {
	pool    *pgxpool.Pool
	storage *cleanupStorage
	policy  *lifecycle.Service
	uploads *uploads.Service
	trace   *cleanupSweepTrace
}

func newCleanupFixture(t *testing.T) *cleanupFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	for _, query := range []string{
		"INSERT INTO users(user_id) VALUES(1002),(1003)",
		"INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9002,1002,'second',true),(9003,1003,'third',true)",
		"CREATE TABLE cleanup_receipt_jobs(id bigint GENERATED ALWAYS AS IDENTITY,user_id bigint NOT NULL)",
	} {
		if _, err := db.Pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	trace := &cleanupSweepTrace{args: make(map[string][][]any)}
	config, err := pgxpool.ParseConfig(db.URL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	storage := &cleanupStorage{
		Storage: fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}, {ChannelID: 9002, Actors: []int64{1002}}, {ChannelID: 9003, Actors: []int64{1003}}}),
		fail:    make(map[int64]error), pages: make(map[int64][]telegramstore.DocumentMessagePage), listError: make(map[int64]error),
	}
	policy := lifecycle.NewService(pool, storage)
	if err := policy.BindReceiptEnqueuer(cleanupQueue{}); err != nil {
		t.Fatal(err)
	}
	service := uploads.NewService(pool)
	if err := service.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	return &cleanupFixture{pool: pool, storage: storage, policy: policy, uploads: service, trace: trace}
}

func (f *cleanupFixture) owned(t testing.TB, owner, channel, message int64, terminal bool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	body := []byte(fmt.Sprintf("independent synthetic payload %d/%d", channel, message))
	session, err := f.uploads.Create(ctx, uploads.CreateInput{ID: uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", message)), UserID: owner, Name: fmt.Sprintf("owned-%d-%d", channel, message), ExpectedSize: int64(len(body)), PartSize: treehash.BlockSize})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.UUID(session.ID.Bytes)
	blocks, checksum := cleanupHash(body)
	claim, err := f.uploads.ClaimPart(ctx, uploads.ClaimPartInput{UserID: owner, UploadID: id, PartNo: 1, ChannelID: channel, PlainSize: int64(len(body)), Checksum: &checksum})
	if err != nil {
		t.Fatal(err)
	}
	receiptID := uuid.New()
	if _, err = f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: receiptID, UserID: owner, ChannelID: channel, OriginKind: "upload", UploadID: &id, PartNo: 1, LeaseToken: claim.LeaseToken, LeaseExpiresAt: claim.Part.LeaseExpiresAt.Time}); err != nil {
		t.Fatal(err)
	}
	if err = f.storage.Seed(fixture.Message{UserID: owner, ChannelID: channel, MessageID: message, Bytes: body}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.policy.Observe(ctx, lifecycle.ObserveInput{ReceiptID: receiptID, UserID: owner, ChannelID: channel, MessageID: message, UploadID: &id, PartNo: 1, LeaseToken: claim.LeaseToken}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.uploads.StorePart(ctx, uploads.StorePartInput{ReceiptID: receiptID, UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: message, StoredSize: int64(len(body)), Checksum: checksum, BlockHashes: blocks}); err != nil {
		t.Fatal(err)
	}
	if terminal {
		if _, err = f.uploads.Abort(ctx, owner, id); err != nil {
			t.Fatal(err)
		}
	}
	return id, receiptID
}

func (f *cleanupFixture) run(t testing.TB) error {
	t.Helper()
	return jobs.NewUploadCleanupWorker(f.pool, f.policy).Work(context.Background(), &river.Job[jobs.UploadCleanupSweepArgs]{})
}

func recordedCleanup[T river.JobArgs](t testing.TB, pool *pgxpool.Pool, worker river.Worker[T], args T) (json.RawMessage, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	tester := rivertest.NewWorker(t, riverpgxv5.New(pool), &river.Config{SkipUnknownJobCheck: true}, worker)
	result, workErr := tester.Work(ctx, t, tx, args, nil)
	if result == nil || result.Job == nil {
		t.Fatalf("missing actual worker result: %v", workErr)
	}
	var metadata struct {
		Output json.RawMessage `json:"output"`
	}
	if err = json.Unmarshal(result.Job.Metadata, &metadata); err != nil || len(metadata.Output) == 0 {
		t.Fatalf("lost partial worker output: %s %v", result.Job.Metadata, err)
	}
	return metadata.Output, workErr
}

func cleanupSnapshot(t testing.TB, pool *pgxpool.Pool, ids []uuid.UUID) string {
	t.Helper()
	var value string
	if err := pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
 'sessions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM upload_sessions s WHERE s.id=ANY($1::uuid[])),
 'parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.upload_id,p.part_no) FROM upload_parts p WHERE p.upload_id=ANY($1::uuid[])),
 'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM remote_write_receipts r WHERE r.upload_id=ANY($1::uuid[])),
 'files',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM files f WHERE f.id=ANY($1::uuid[])),
 'file_parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.file_id,p.part_no) FROM file_parts p WHERE p.file_id=ANY($1::uuid[])),
 'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM user_events e WHERE e.resource_id=ANY(SELECT u::text FROM unnest($1::uuid[])u))
 )::text`, ids).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func cleanupDeleted(t testing.TB, storage *cleanupStorage, want ...string) {
	t.Helper()
	var got []string
	for _, call := range storage.Deletes() {
		for _, id := range call.DeletedIDs {
			got = append(got, fmt.Sprintf("%d/%d/%d", call.UserID, call.ChannelID, id))
		}
		if len(call.PreservedIDs) != 0 {
			t.Fatalf("deleted independently protected bytes: %+v", call)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("exact acknowledged byte-deletion set=%v, want %v", got, want)
	}
	if err := storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupSweepExpiresAndDeletesTelegramParts(t *testing.T) {
	f := newCleanupFixture(t)
	id, _ := f.owned(t, 1001, 9001, 77, false)
	if _, err := f.pool.Exec(context.Background(), "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 minute' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}
	var state string
	var count int
	if err := f.pool.QueryRow(context.Background(), "SELECT state,(SELECT count(*) FROM upload_parts WHERE upload_id=$1) FROM upload_sessions WHERE id=$1", id).Scan(&state, &count); err != nil {
		t.Fatal(err)
	}
	if state != "expired" || count != 0 {
		t.Fatalf("state=%s remaining parts=%d", state, count)
	}
	cleanupDeleted(t, f.storage, "1001/9001/77")
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}
	cleanupDeleted(t, f.storage, "1001/9001/77")
}

func TestCleanupSweepRetainsReferencesWhenTelegramFails(t *testing.T) {
	f := newCleanupFixture(t)
	id, receipt := f.owned(t, 1001, 9001, 88, true)
	fault := errors.New("synthetic permission failure")
	f.storage.fail[9001] = fault
	if err := f.run(t); !errors.Is(err, fault) {
		t.Fatalf("lost cleanup cause: %v", err)
	}
	var count, attempts int
	if err := f.pool.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM upload_parts WHERE upload_id=$1),attempts FROM remote_write_receipts WHERE id=$2", id, receipt).Scan(&count, &attempts); err != nil {
		t.Fatal(err)
	}
	if count != 1 || attempts != 1 {
		t.Fatalf("retained parts=%d attempts=%d", count, attempts)
	}
	delete(f.storage.fail, 9001)
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}
	cleanupDeleted(t, f.storage, "1001/9001/88")
}

func TestCleanupSweepDrainsMultiplePages(t *testing.T) {
	f := newCleanupFixture(t)
	for i := int64(1); i <= 1001; i++ {
		f.owned(t, 1001, 9001, i, true)
	}
	before := f.trace.Count("ListUploadSessionsPendingCleanup")
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}
	if got := f.trace.Count("ListUploadSessionsPendingCleanup") - before; got != 2 {
		t.Fatalf("candidate pages=%d, want 2", got)
	}
	var want []string
	for i := int64(1); i <= 1001; i++ {
		want = append(want, fmt.Sprintf("1001/9001/%d", i))
	}
	cleanupDeleted(t, f.storage, want...)
}

func TestB2UploadSweepContinuation(t *testing.T) {
	t.Run("retained_page_and_failed_owner", func(t *testing.T) {
		f := newCleanupFixture(t)
		ctx := context.Background()
		protected, _ := f.owned(t, 1001, 9001, 2001, true)
		exhausted, exhaustedReceipt := f.owned(t, 1001, 9001, 2002, true)
		f.storage.fail[9001] = errors.New("synthetic exhausted failure")
		for range 4 {
			tx, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = f.policy.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{exhaustedReceipt}, PrimaryCode: "upload_cleanup"})
			if err == nil {
				err = tx.Commit(ctx)
			}
			_ = tx.Rollback(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.policy.Recover(ctx, 1001, []uuid.UUID{exhaustedReceipt}); err == nil {
				t.Fatal("expected synthetic delete failure")
			}
			if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", exhaustedReceipt); err != nil {
				t.Fatal(err)
			}
		}
		delete(f.storage.fail, 9001)
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
 SELECT ('00000000-0000-0000-0000-'||lpad(n::text,12,'0'))::uuid,1001,'retained-'||n,1,now(),16777216,now()-interval '1 hour','aborted','2000-01-01' FROM generate_series(1,998) n RETURNING id)
 INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,state)
 SELECT id,1,9001,10000+row_number() OVER(),1,1,'stored' FROM s`); seedErr != nil {
			t.Fatal(seedErr)
		}
		if seedErr = seedTx.Commit(ctx); seedErr != nil {
			t.Fatal(seedErr)
		}
		for n := int64(10001); n <= 10998; n++ {
			if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: n, Bytes: []byte("x"), Preserve: true}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.pool.Exec(ctx, `WITH f AS (
 INSERT INTO files(user_id,name,kind,size,mod_time)
 SELECT 1002,'independent alias','file',expected_size,now() FROM upload_sessions WHERE id=$1 RETURNING id,size)
 INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size)
 SELECT id,1,9001,2001,size,size FROM f`, protected); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(ctx, "UPDATE upload_sessions SET created_at='2000-01-01' WHERE id=ANY($1::uuid[])", []uuid.UUID{protected, exhausted}); err != nil {
			t.Fatal(err)
		}
		failed, failedReceipt := f.owned(t, 1002, 9002, 3001, true)
		later, _ := f.owned(t, 1003, 9003, 3002, false)
		if _, err := f.pool.Exec(ctx, "UPDATE upload_sessions SET created_at='2000-01-01',expires_at=clock_timestamp()-interval '1 minute' WHERE id=ANY($1::uuid[])", []uuid.UUID{failed, later}); err != nil {
			t.Fatal(err)
		}
		var retained []uuid.UUID
		rows, err := f.pool.Query(ctx, "SELECT id FROM upload_sessions WHERE created_at='2000-01-01' AND id<$1 ORDER BY id", failed)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id uuid.UUID
			if err = rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			retained = append(retained, id)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if len(retained) != 1000 {
			t.Fatalf("retained first page=%d", len(retained))
		}
		before := cleanupSnapshot(t, f.pool, retained)
		fault := errors.New("synthetic other-owner permission denied")
		f.storage.fail[9002] = fault
		for pass := range 2 {
			pages, expiry, cutoffs := f.trace.Count("ListUploadSessionsPendingCleanup"), f.trace.Count("ExpireUploadSessions"), f.trace.Count("GetCleanupSweepCutoff")
			dispositions, partQueries, claims := f.trace.Count("ListCleanupMessageDispositions"), f.trace.Count("ListUploadPartsForCleanupMany"), f.trace.Count("ClaimReceiptDeletion")
			raw, err := recordedCleanup(t, f.pool, jobs.NewUploadCleanupWorker(f.pool, f.policy), jobs.UploadCleanupSweepArgs{})
			if !errors.Is(err, fault) || !errors.Is(err, jobs.ErrUploadCleanupIncomplete) {
				t.Fatalf("aggregate outcome=%v", err)
			}
			var output jobs.UploadCleanupOutput
			if err = json.Unmarshal(raw, &output); err != nil {
				t.Fatal(err)
			}
			if !output.Incomplete || output.Unknown != 998 || output.Protected != 1 || output.Exhausted != 1 || output.Failed != 1 || output.Pages != 2 {
				t.Fatalf("partial report=%s", raw)
			}
			if output.Deleted != 1-pass || output.RemovedParts != int64(1-pass) {
				t.Fatalf("false progress report=%s", raw)
			}
			if got := f.trace.Count("ListCleanupMessageDispositions") - dispositions; got != 5-pass*2 {
				t.Fatalf("bounded disposition queries=%d", got)
			}
			if got := f.trace.Count("ListUploadPartsForCleanupMany") - partQueries; got != 2 {
				t.Fatalf("bounded part queries=%d", got)
			}
			if got := f.trace.Count("ClaimReceiptDeletion") - claims; got != 2-pass {
				t.Fatalf("bounded claim queries=%d", got)
			}
			if got := f.trace.Count("ListUploadSessionsPendingCleanup") - pages; got != 2 {
				t.Fatalf("pass%d pages=%d; full retained page must advance", pass, got)
			}
			if got := f.trace.Count("GetCleanupSweepCutoff") - cutoffs; got != 1 {
				t.Fatalf("cutoff reads=%d", got)
			}
			if got := f.trace.Count("ExpireUploadSessions") - expiry; got != 1 {
				t.Fatalf("expiry batches=%d; only one eligible open row", got)
			}
			if got := cleanupSnapshot(t, f.pool, retained); got != before {
				t.Fatal("retained evidence or consumed budget changed")
			}
		}
		var attempts int
		if err := f.pool.QueryRow(ctx, "SELECT attempts FROM remote_write_receipts WHERE id=$1", failedReceipt).Scan(&attempts); err != nil || attempts != 2 {
			t.Fatalf("failed budget=%d %v", attempts, err)
		}
		cleanupDeleted(t, f.storage, "1003/9003/3002")
	})
	t.Run("legacy_unknown_evidence_retained", func(t *testing.T) {
		f := newCleanupFixture(t)
		ctx := context.Background()
		var ids []uuid.UUID
		for _, state := range []string{"open", "aborted", "expired"} {
			for _, nonempty := range []bool{false, true} {
				id := uuid.New()
				ids = append(ids, id)
				if _, err := f.pool.Exec(ctx, `INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,state,expires_at,created_at,terminal_failure_code)
 VALUES($1,1001,$2,$3,'2000-01-01',16777216,$4,clock_timestamp()-interval '1 day','2000-01-01','upload_destination_unknown')`, id, fmt.Sprintf("unknown-%s-%t", state, nonempty), map[bool]int64{false: 0, true: 4}[nonempty], state); err != nil {
					t.Fatal(err)
				}
				if nonempty {
					message := int64(4000 + len(ids))
					body := []byte("kept")
					blocks, checksum := cleanupHash(body)
					if _, err := f.pool.Exec(ctx, `INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,checksum,block_hashes,salt,state,lease_token,lease_expires_at,created_at,updated_at)
 VALUES($1,1,9001,$2,4,4,$3,$4,'legacy-salt','stored',$5,'2000-01-02','2000-01-01','2000-01-01')`, id, message, checksum, blocks, uuid.New()); err != nil {
						t.Fatal(err)
					}
					if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: message, Bytes: body, Preserve: true}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		before := cleanupSnapshot(t, f.pool, ids)
		f.owned(t, 1003, 9003, 4999, true)
		f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{{ID: 4002, CreatedAt: time.Unix(0, 0)}, {ID: 4004, CreatedAt: time.Unix(0, 0)}, {ID: 4006, CreatedAt: time.Unix(0, 0)}}, Exhausted: true}}
		for pass := range 2 {
			cutoff, err := sqlcgen.New(f.pool).GetCleanupSweepCutoff(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := sqlcgen.New(f.pool).ExpireUploadSessions(ctx, cutoff); err != nil || len(rows) != 0 {
				t.Fatalf("unknown expiry mutated rows: %v %v", rows, err)
			}
			if err = f.run(t); !errors.Is(err, jobs.ErrUploadCleanupIncomplete) {
				t.Fatalf("unknown cleanup outcome=%v", err)
			}
			if err = jobs.NewOrphanedTelegramPartsCleanupWorker(f.pool, f.policy, f.storage, 0).Work(ctx, &river.Job[jobs.OrphanCleanupArgs]{}); err != nil {
				t.Fatal(err)
			}
			records := []map[string]any{
				{"user_id": int64(1001), "upload_id": ids[1], "part_no": 1, "channel_id": int64(9001), "message_id": int64(4002)},
				{"user_id": int64(1001), "upload_id": ids[3], "part_no": 1, "channel_id": int64(9001), "message_id": int64(4004)},
				{"user_id": int64(1001), "upload_id": ids[5], "part_no": 1, "channel_id": int64(9001), "message_id": int64(4006)},
			}
			raw, e := json.Marshal(records)
			if e != nil {
				t.Fatal(e)
			}
			if n, e := sqlcgen.New(f.pool).DeleteUploadPartsForCleanup(ctx, raw); e != nil || n != 0 {
				t.Fatalf("direct unknown ACK erased evidence: %d %v", n, e)
			}
			if after := cleanupSnapshot(t, f.pool, ids); after != before {
				t.Fatal("unknown reason/session/part/lease/proof/timestamps changed")
			}
			if got := f.trace.Count("ListUploadSessionsPendingCleanup"); got != pass+1 {
				t.Fatalf("diagnostic pages=%d", got)
			}
		}
		cleanupDeleted(t, f.storage, "1003/9003/4999")
	})
	t.Run("expiry_batches_and_locked_row", func(t *testing.T) {
		f := newCleanupFixture(t)
		ctx := context.Background()
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
		if _, seedErr = seedTx.Exec(ctx, `INSERT INTO upload_sessions(user_id,name,expected_size,mod_time,part_size,expires_at,created_at)
 SELECT 1001,'expiry-'||n,0,now(),16777216,clock_timestamp()-interval '1 day','2000-01-01' FROM generate_series(1,1001)n`); seedErr != nil {
			t.Fatal(seedErr)
		}
		if seedErr = seedTx.Commit(ctx); seedErr != nil {
			t.Fatal(seedErr)
		}
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		var locked pgtype.UUID
		if err = tx.QueryRow(ctx, "SELECT id FROM upload_sessions ORDER BY id LIMIT 1 FOR UPDATE").Scan(&locked); err != nil {
			t.Fatal(err)
		}
		expiry := f.trace.Count("ExpireUploadSessions")
		if err = f.run(t); err != nil {
			t.Fatal(err)
		}
		if got := f.trace.Count("ExpireUploadSessions") - expiry; got != 2 {
			t.Fatalf("expiry queries=%d, want full1000 plus empty", got)
		}
		var open int
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_sessions WHERE state='open'").Scan(&open); err != nil || open != 1 {
			t.Fatalf("locked expiry=%d %v", open, err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = f.run(t); err != nil {
			t.Fatal(err)
		}
	})
}

func seedCleanupOwner(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "INSERT INTO users(user_id) VALUES(1001); INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'storage',true)"); err != nil {
		t.Fatal(err)
	}
}

func cleanupHash(data []byte) ([]byte, string) {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(data)
	blocks := hasher.Sum()
	return blocks, treehash.SumToHex(treehash.ComputeTreeHash(blocks))
}

func assertCleanupBytes(t testing.TB, s *cleanupStorage, channel, message int64, want []byte) {
	t.Helper()
	got, ok := s.Bytes(channel, message)
	if !ok || !bytes.Equal(got, want) || sha256.Sum256(got) != sha256.Sum256(want) {
		t.Fatalf("independent bytes changed for %d/%d", channel, message)
	}
}
