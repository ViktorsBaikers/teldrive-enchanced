//go:build integration

package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

const periodicHTTPSystemID = "receipt_cleanup_sweep"

// Only the authenticator assigns roles. Request bodies/context cannot choose one.
type periodicIssuedAuthenticator struct{}

func (periodicIssuedAuthenticator) AuthenticateBearer(_ context.Context, token string) (api.Identity, error) {
	switch token {
	case "issued-member":
		return api.Identity{UserID: 1001}, nil
	case "issued-admin":
		return api.Identity{UserID: 1001, Roles: []string{"admin"}}, nil
	case "issued-owner":
		return api.Identity{UserID: 1001, Roles: []string{"owner"}}, nil
	}
	return api.Identity{}, api.ErrUnauthenticated
}
func (a periodicIssuedAuthenticator) AuthenticateAPIKey(ctx context.Context, token string) (api.Identity, error) {
	return a.AuthenticateBearer(ctx, token)
}

type periodicHTTPFixture struct {
	db             *testpostgres.Database
	storage        *fixture.Storage
	policy         *lifecycle.Service
	runtime        *jobs.Runtime
	server         http.Handler
	source, parent uuid.UUID
}

func newPeriodicHTTPFixture(t *testing.T) *periodicHTTPFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users(user_id) VALUES(1001),(1002); INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'synthetic',true)"); err != nil {
		t.Fatal(err)
	}
	storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}})
	for _, m := range []fixture.Message{{UserID: 1001, ChannelID: 9001, MessageID: 10, Bytes: []byte("source bytes"), Preserve: true}, {UserID: 1002, ChannelID: 9001, MessageID: 20, Bytes: []byte("foreign bytes"), Preserve: true}, {UserID: 1001, ChannelID: 9001, MessageID: 30, Bytes: []byte("winner bytes"), Preserve: true}} {
		if err := storage.Seed(m); err != nil {
			t.Fatal(err)
		}
	}
	policy := lifecycle.NewService(db.Pool, storage)
	r, err := jobs.NewRuntime(db.Pool, storage, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err = policy.BindReceiptEnqueuer(r); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewService(db.Pool, nil)
	up := uploads.NewService(db.Pool)
	if err = cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	if err = up.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	handler := api.NewHandler(cat, up, nil, nil, health.NewService("synthetic", db.Pool), 0, nil).ConfigureJobs(r)
	server, err := api.NewServer(handler, api.NewSecurity(periodicIssuedAuthenticator{}))
	if err != nil {
		t.Fatal(err)
	}
	f := &periodicHTTPFixture{db: db, storage: storage, policy: policy, runtime: r, server: server, source: uuid.New(), parent: uuid.New()}
	if _, err = db.Pool.Exec(ctx, `INSERT INTO files(id,user_id,name,kind,status,size,mod_time) VALUES($1,1001,'source.bin','file','active',12,clock_timestamp()),($2,1001,'destination','folder','active',0,clock_timestamp())`, f.source, f.parent); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) VALUES($1,1,9001,10,12,12)", f.source); err != nil {
		t.Fatal(err)
	}
	if err = r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Stop(stop); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f *periodicHTTPFixture) request(t *testing.T, method, path, token string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	if method == http.MethodPut {
		updated := make(map[string]any, len(body))
		for key, value := range body {
			if key != "id" {
				updated[key] = value
			}
		}
		body = updated
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	out := httptest.NewRecorder()
	f.server.ServeHTTP(out, req)
	return out
}
func periodicHTTPBody(id, kind string) map[string]any {
	return map[string]any{"id": id, "kind": kind, "args": map[string]any{"retention": "48h"}, "queue": jobs.CleanupQueue, "priority": 2, "maxAttempts": 3, "tags": []string{}, "cronExpression": "@every 12h", "cronTimezone": "UTC", "paused": false}
}
func periodicHTTPSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(context.Background(), "SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY id),'[]')::text FROM river_periodic_job p").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func periodicHTTPAssert(t *testing.T, out *httptest.ResponseRecorder, status int, code, message string) {
	t.Helper()
	if out.Code != status {
		t.Fatalf("HTTP %d want %d: %s", out.Code, status, out.Body.String())
	}
	if code != "" {
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Body.Bytes(), &envelope); err != nil || envelope.Error.Code != code || envelope.Error.Message != message {
			t.Fatalf("wrong shared error envelope: %s (%v)", out.Body.String(), err)
		}
	}
}
func periodicHTTPMutations(id string) [][2]string {
	return [][2]string{{http.MethodPost, "/v1/periodic-jobs"}, {http.MethodPut, "/v1/periodic-jobs/" + id}, {http.MethodDelete, "/v1/periodic-jobs/" + id}, {http.MethodPost, "/v1/periodic-jobs/" + id + "/pause"}, {http.MethodPost, "/v1/periodic-jobs/" + id + "/resume"}, {http.MethodPost, "/v1/periodic-jobs/reset"}}
}

func TestB2PeriodicScheduleOwnershipHTTP(t *testing.T) {
	t.Run("unauthenticated_denied", func(t *testing.T) {
		f := newPeriodicHTTPFixture(t)
		for _, route := range periodicHTTPMutations("ordinary") {
			before := periodicHTTPSnapshot(t, f.db.Pool)
			out := f.request(t, route[0], route[1], "", periodicHTTPBody("ordinary", jobs.EventCleanupKind))
			periodicHTTPAssert(t, out, 401, "unauthorized", "authentication is required")
			if periodicHTTPSnapshot(t, f.db.Pool) != before {
				t.Fatal("unauthenticated mutation wrote schedule")
			}
		}
	})
	t.Run("nonadmin_mutations_denied", func(t *testing.T) {
		f := newPeriodicHTTPFixture(t)
		for _, id := range []string{"ordinary", periodicHTTPSystemID} {
			for _, route := range periodicHTTPMutations(id) {
				before := periodicHTTPSnapshot(t, f.db.Pool)
				out := f.request(t, route[0], route[1], "issued-member", periodicHTTPBody(id, jobs.EventCleanupKind))
				periodicHTTPAssert(t, out, 403, "forbidden", "administrator access is required")
				if periodicHTTPSnapshot(t, f.db.Pool) != before {
					t.Fatal("valid nonadmin token mutated catalog")
				}
			}
		}
	})
	t.Run("admin_system_mutations_conflict", func(t *testing.T) {
		f := newPeriodicHTTPFixture(t)
		periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", "issued-admin", periodicHTTPBody("ordinary-alias-target", jobs.EventCleanupKind)), 201, "", "")
		for _, token := range []string{"issued-admin", "issued-owner"} {
			for _, variant := range []string{"canonical", "wrong_kind", "alias"} {
				t.Run(token+"/"+variant, func(t *testing.T) {
					id := periodicHTTPSystemID
					if variant == "wrong_kind" {
						if _, err := f.db.Pool.Exec(context.Background(), "UPDATE river_periodic_job SET kind=$1 WHERE id=$2", jobs.EventCleanupKind, id); err != nil {
							t.Fatal(err)
						}
					}
					if variant == "alias" {
						id = "reserved-kind-alias"
						if _, err := f.db.Pool.Exec(context.Background(), `INSERT INTO river_periodic_job(id,kind,args,queue,priority,max_attempts,tags,cron_expression,cron_timezone,next_run_at) VALUES($1,$2,'{}','maintenance',2,3,'{}','@every 12h','UTC',clock_timestamp()+interval '12 hours') ON CONFLICT(id) DO NOTHING`, id, jobs.ReceiptDiscoverySweepKind); err != nil {
							t.Fatal(err)
						}
					}
					for _, route := range periodicHTTPMutations(id)[:5] {
						before := periodicHTTPSnapshot(t, f.db.Pool)
						body := periodicHTTPBody(id, jobs.EventCleanupKind)
						if route[0] == http.MethodPost && route[1] == "/v1/periodic-jobs" {
							body["kind"] = jobs.ReceiptDiscoverySweepKind
						}
						out := f.request(t, route[0], route[1], token, body)
						periodicHTTPAssert(t, out, 409, "conflict", "operation conflicts with current state")
						if periodicHTTPSnapshot(t, f.db.Pool) != before {
							t.Fatal("unsafe admin mutation partially changed persisted row/due")
						}
					}
					before := periodicHTTPSnapshot(t, f.db.Pool)
					out := f.request(t, http.MethodPost, "/v1/periodic-jobs", token, periodicHTTPBody("new-alias", jobs.ReceiptDiscoverySweepKind))
					periodicHTTPAssert(t, out, 409, "conflict", "operation conflicts with current state")
					if periodicHTTPSnapshot(t, f.db.Pool) != before {
						t.Fatal("reserved kind gained alias")
					}
					before = periodicHTTPSnapshot(t, f.db.Pool)
					out = f.request(t, http.MethodPut, "/v1/periodic-jobs/ordinary-alias-target", token, periodicHTTPBody("ordinary-alias-target", jobs.ReceiptDiscoverySweepKind))
					periodicHTTPAssert(t, out, 409, "conflict", "operation conflicts with current state")
					if periodicHTTPSnapshot(t, f.db.Pool) != before {
						t.Fatal("proposed reserved-kind replacement changed ordinary schedule")
					}
					if _, err := f.db.Pool.Exec(context.Background(), "UPDATE river_periodic_job SET kind=$1 WHERE id=$2", jobs.ReceiptDiscoverySweepKind, periodicHTTPSystemID); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	})
	t.Run("admin_other_schedule_and_reset_control", func(t *testing.T) {
		for _, token := range []string{"issued-admin", "issued-owner"} {
			t.Run(token, func(t *testing.T) {
				f := newPeriodicHTTPFixture(t)
				id := "ordinary-control"
				body := periodicHTTPBody(id, jobs.EventCleanupKind)
				periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", token, body), 201, "", "")
				body["priority"] = 3
				periodicHTTPAssert(t, f.request(t, http.MethodPut, "/v1/periodic-jobs/"+id, token, body), 200, "", "")
				periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs/"+id+"/pause", token, nil), 200, "", "")
				periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs/"+id+"/resume", token, nil), 200, "", "")
				periodicHTTPAssert(t, f.request(t, http.MethodDelete, "/v1/periodic-jobs/"+id, token, nil), 204, "", "")
				periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", token, body), 201, "", "")
				periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs/reset", token, nil), 200, "", "")
				var custom, enabled int
				if err := f.db.Pool.QueryRow(context.Background(), "SELECT count(*) FILTER(WHERE id=$1),count(*) FILTER(WHERE id=$2 AND kind=$3 AND paused_at IS NULL) FROM river_periodic_job", id, periodicHTTPSystemID, jobs.ReceiptDiscoverySweepKind).Scan(&custom, &enabled); err != nil || custom != 0 || enabled != 1 {
					t.Fatalf("actual full reset failed: custom=%d mandatory=%d %v", custom, enabled, err)
				}
				f.observeCrash(t)
				f.discover(t)
				t.Run("no_receipt", func(t *testing.T) { f.noReceipt(t) })
			})
		}
	})
}

type periodicObserveCrash struct {
	mu       sync.Mutex
	observed map[uint32]bool
	armed    atomic.Bool
	hooks    fixture.Hooks
}
type periodicTraceSQL struct{}

func (c *periodicObserveCrash) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, periodicTraceSQL{}, d.SQL)
}
func (c *periodicObserveCrash) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, d pgx.TraceQueryEndData) {
	if d.Err != nil || !c.armed.Load() {
		return
	}
	sql, _ := ctx.Value(periodicTraceSQL{}).(string)
	pid := conn.PgConn().PID()
	c.mu.Lock()
	if strings.Contains(sql, "-- name: ObserveMessageReceipt") {
		c.observed[pid] = true
	}
	crash := strings.EqualFold(strings.TrimSpace(sql), "commit") && c.observed[pid]
	if crash {
		delete(c.observed, pid)
		c.armed.Store(false)
	}
	c.mu.Unlock()
	if crash {
		if err := c.hooks.ObserveCommittedBeforeStage.Pause(ctx); !errors.Is(err, fixture.ErrProcessLost) {
			panic(fmt.Errorf("wrong actual commit fault: %w", err))
		}
		panic(fixture.ErrProcessLost)
	}
}
func (f *periodicHTTPFixture) observeCrash(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	gate := fixture.NewBarrier(t, fixture.ErrProcessLost)
	gate.Release()
	trace := &periodicObserveCrash{observed: make(map[uint32]bool), hooks: fixture.Hooks{ObserveCommittedBeforeStage: gate}}
	trace.armed.Store(true)
	cfg, err := pgxpool.ParseConfig(f.db.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	policy := lifecycle.NewService(pool, f.storage)
	if err = policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewService(pool, nil)
	if err = cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	ops, err := fileops.NewService(pool, cat, channels.NewService(pool, nil, channels.Config{PartLimit: 10000}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	var lost any
	func() {
		defer func() { lost = recover() }()
		name := "reset-new-debt"
		_, err = ops.Copy(ctx, fileops.CopyInput{UserID: 1001, FileID: f.source, ParentID: &f.parent, Name: &name})
	}()
	if lost != fixture.ErrProcessLost {
		t.Fatalf("actual Observe commit process loss missing: %v %v", lost, err)
	}
	var jobCount, refs int
	if err = f.db.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM river_job WHERE kind=$1),(SELECT count(*) FROM file_parts p JOIN remote_write_receipts r ON r.channel_id=p.channel_id AND r.message_id=p.message_id)", jobs.ReceiptCleanupKind).Scan(&jobCount, &refs); err != nil || jobCount+refs != 0 {
		t.Fatalf("not new ledger-only debt: jobs=%d refs=%d %v", jobCount, refs, err)
	}
	if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE published_at IS NULL AND state IN ('reserved','observed')"); err != nil {
		t.Fatal(err)
	}
}
func periodicHTTPEventually(t *testing.T, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for !check() {
		if ctx.Err() != nil {
			t.Fatal("same-runtime persisted scheduler did not recover new debt")
		}
		runtime.Gosched()
	}
}
func (f *periodicHTTPFixture) due(t *testing.T) {
	t.Helper()
	if _, err := f.db.Pool.Exec(context.Background(), "UPDATE river_periodic_job SET next_run_at=clock_timestamp()-interval '1 second' WHERE id=$1", periodicHTTPSystemID); err != nil {
		t.Fatal(err)
	}
}
func (f *periodicHTTPFixture) discover(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	f.due(t)
	periodicHTTPEventually(t, func() bool {
		var pending int
		err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM remote_write_receipts WHERE row_kind='message_attempt' AND state<>'deleted'").Scan(&pending)
		return err == nil && pending == 0
	})
	var message, first, current, owner int64
	if err := f.db.Pool.QueryRow(ctx, "SELECT message_id,job_id,wakeup_job_id,user_id FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&message, &first, &current, &owner); err != nil {
		t.Fatal(err)
	}
	if first <= 0 || current <= 0 || owner != 1001 {
		t.Fatal("scheduler missing actual first/current owner IDs")
	}
	if _, ok := f.storage.Bytes(9001, message); ok {
		t.Fatal("observed destination bytes not deleted")
	}
	if _, err := f.runtime.GetForUser(ctx, first, 1001); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.GetForUser(ctx, first, 1002); err == nil {
		t.Fatal("foreign owner report leaked")
	}
	items, _, err := f.runtime.List(ctx, jobs.ListInput{UserID: 1001, Kind: jobs.ReceiptCleanupKind, Limit: 200})
	if err != nil || len(items) == 0 {
		t.Fatalf("authenticated reset recovery missing owner List: %v", err)
	}
	stats, err := f.runtime.StatisticsForUser(ctx, 1001)
	if err != nil || stats.Completed+stats.Discarded == 0 {
		t.Fatalf("authenticated reset owner Statistics: %+v %v", stats, err)
	}
	periodicHTTPEventually(t, func() bool {
		var active int
		return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE kind=$1 AND state NOT IN ('completed','discarded','cancelled')", jobs.ReceiptCleanupKind).Scan(&active) == nil && active == 0
	})
	if _, err := f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE kind=$1", jobs.ReceiptCleanupKind); err != nil {
		t.Fatal(err)
	}
	if item, err := f.runtime.GetForUser(ctx, first, 1001); err != nil || item.State != "completed" {
		t.Fatalf("retained owner report: %+v %v", item, err)
	}
	for id, want := range map[int64]string{10: "source bytes", 20: "foreign bytes", 30: "winner bytes"} {
		got, ok := f.storage.Bytes(9001, id)
		if !ok || sha256.Sum256(got) != sha256.Sum256([]byte(want)) {
			t.Fatalf("independent source/foreign/winner SHA changed: %d", id)
		}
	}
}
func (f *periodicHTTPFixture) noReceipt(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	id, result, token := uuid.New(), uuid.New(), uuid.New()
	tx, err := f.db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.policy.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: id, RootResultFileID: result, LeaseToken: token, DestinationParentID: &f.parent, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()})
	if err == nil {
		err = tx.Commit(ctx)
	}
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	before := len(f.storage.Deletes())
	f.due(t)
	var first, current, owner int64
	periodicHTTPEventually(t, func() bool {
		err := f.db.Pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id,user_id FROM remote_write_receipts WHERE id=$1 AND state='unresolved'", id).Scan(&first, &current, &owner)
		return err == nil && first > 0 && current > 0 && owner == 1001
	})
	if report, e := f.runtime.GetForUser(ctx, first, 1001); e != nil || report.State != "discarded" {
		t.Fatalf("same-runtime no-message first owner report: %+v %v", report, e)
	}
	var metadataOwner int64
	if err = f.db.Pool.QueryRow(ctx, "SELECT (metadata->>'user_id')::bigint FROM river_job WHERE id=$1", current).Scan(&metadataOwner); err != nil || metadataOwner != 1001 {
		t.Fatalf("no-message insert-time owner metadata: %v", err)
	}
	if len(f.storage.Deletes()) != before {
		t.Fatal("report-only debt sent destructive RPC")
	}
}

func TestB2PeriodicAdmissionHTTP(t *testing.T) {
	t.Run("priority_create_update", func(t *testing.T) {
		f := newPeriodicHTTPFixture(t)
		body := periodicHTTPBody("priority-rejected", jobs.EventCleanupKind)
		body["priority"] = 5
		before := periodicHTTPSnapshot(t, f.db.Pool)
		periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", "issued-admin", body), 400, "invalid_request", "request is invalid")
		if periodicHTTPSnapshot(t, f.db.Pool) != before {
			t.Fatal("priority 5 Create wrote a definition")
		}
		for _, priority := range []int{-9, 0, 1, 4} {
			id := fmt.Sprintf("priority-%d", priority)
			body = periodicHTTPBody(id, jobs.EventCleanupKind)
			body["priority"] = priority
			out := f.request(t, http.MethodPost, "/v1/periodic-jobs", "issued-admin", body)
			periodicHTTPAssert(t, out, 201, "", "")
			want := priority
			if priority <= 0 {
				want = 1
			}
			var created struct {
				Priority int `json:"priority"`
			}
			if err := json.Unmarshal(out.Body.Bytes(), &created); err != nil || created.Priority != want {
				t.Fatalf("Create priority %d response: %s (%v)", priority, out.Body.String(), err)
			}
			body["priority"] = 5
			body["paused"] = true
			body["cronExpression"] = "@every 24h"
			before = periodicHTTPSnapshot(t, f.db.Pool)
			periodicHTTPAssert(t, f.request(t, http.MethodPut, "/v1/periodic-jobs/"+id, "issued-admin", body), 400, "invalid_request", "request is invalid")
			if periodicHTTPSnapshot(t, f.db.Pool) != before {
				t.Fatal("priority 5 Update changed definition, pause state, or due time")
			}
			body["priority"] = priority
			out = f.request(t, http.MethodPut, "/v1/periodic-jobs/"+id, "issued-admin", body)
			periodicHTTPAssert(t, out, 200, "", "")
			var updated struct {
				Priority int  `json:"priority"`
				Paused   bool `json:"paused"`
			}
			if err := json.Unmarshal(out.Body.Bytes(), &updated); err != nil || updated.Priority != want || !updated.Paused {
				t.Fatalf("Update priority %d response: %s (%v)", priority, out.Body.String(), err)
			}
		}
	})
	t.Run("capacity_counts_paused_definitions", func(t *testing.T) {
		f := newPeriodicHTTPFixture(t)
		ctx := context.Background()
		if _, err := f.db.Pool.Exec(ctx, `
INSERT INTO river_periodic_job
    (id,kind,args,queue,priority,max_attempts,tags,cron_expression,cron_timezone,next_run_at,paused_at)
SELECT 'a-http-capacity-'||lpad(i::text,5,'0'),$1,'{"retention":"48h"}'::jsonb,$2,2,3,'{}',
       '@every 12h','UTC',clock_timestamp()+interval '12 hours',clock_timestamp()
FROM generate_series(1,(SELECT 9999-count(*)::integer FROM river_periodic_job)) i`, jobs.EventCleanupKind, jobs.CleanupQueue); err != nil {
			t.Fatal(err)
		}
		body := periodicHTTPBody("a-http-last-supported", jobs.EventCleanupKind)
		periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", "issued-admin", body), 201, "", "")
		before := periodicHTTPSnapshot(t, f.db.Pool)
		body["id"] = "a-http-over-capacity"
		periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", "issued-admin", body), 409, "conflict", "operation conflicts with current state")
		if periodicHTTPSnapshot(t, f.db.Pool) != before {
			t.Fatal("capacity refusal changed definitions")
		}
		body["id"] = "a-http-last-supported"
		periodicHTTPAssert(t, f.request(t, http.MethodPost, "/v1/periodic-jobs", "issued-admin", body), 409, "already_exists", "periodic job already exists")
		body["priority"] = 4
		periodicHTTPAssert(t, f.request(t, http.MethodPut, "/v1/periodic-jobs/a-http-last-supported", "issued-admin", body), 200, "", "")
		var total, required int
		if err := f.db.Pool.QueryRow(ctx, "SELECT count(*),count(*) FILTER(WHERE id=$1 AND paused_at IS NULL) FROM river_periodic_job", periodicHTTPSystemID).Scan(&total, &required); err != nil || total != 10000 || required != 1 {
			t.Fatalf("capacity boundary total=%d mandatory=%d %v", total, required, err)
		}
		f.observeCrash(t)
		f.discover(t)
	})
}
