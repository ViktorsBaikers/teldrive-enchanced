//go:build integration

package channels_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/localtelegram"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

var allocationACKLost = errors.New("synthetic channel COMMIT response lost")

type allocationRemote struct {
	*fixture.Storage
	id            int64
	createErr     error
	deleteErr     error
	deleteEntered chan struct{}
	deleteRelease chan struct{}
	deletes       atomic.Int32
	uploads       atomic.Int32
	copies        atomic.Int32
	beforeUpload  func(context.Context) error
	beforeCopy    func(context.Context) error
	create        func(context.Context) (telegramstore.Channel, error)
}

func (r *allocationRemote) CreateChannel(ctx context.Context, _ int64, name string) (telegramstore.Channel, error) {
	if r.create != nil {
		return r.create(ctx)
	}
	return telegramstore.Channel{ID: r.id, Name: name}, r.createErr
}
func (r *allocationRemote) DeleteChannel(ctx context.Context, u, c int64) error {
	r.deletes.Add(1)
	if r.deleteEntered != nil {
		select {
		case r.deleteEntered <- struct{}{}:
		default:
		}
	}
	if r.deleteRelease != nil {
		select {
		case <-r.deleteRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.deleteErr != nil {
		return r.deleteErr
	}
	return r.Storage.DeleteChannel(ctx, u, c)
}
func (r *allocationRemote) Upload(ctx context.Context, in telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	r.uploads.Add(1)
	if r.beforeUpload != nil {
		if err := r.beforeUpload(ctx); err != nil {
			return telegramstore.StoredPart{}, err
		}
	}
	return r.Storage.Upload(ctx, in)
}
func (r *allocationRemote) CopyPart(ctx context.Context, u, s, m, d int64) (telegramstore.StoredPart, error) {
	r.copies.Add(1)
	if r.beforeCopy != nil {
		if err := r.beforeCopy(ctx); err != nil {
			return telegramstore.StoredPart{}, err
		}
	}
	return r.Storage.CopyPart(ctx, u, s, m, d)
}

type allocationFixture struct {
	db      *testpostgres.Database
	remote  *allocationRemote
	policy  *lifecycle.Service
	runtime *jobs.Runtime
	cat     *catalog.Service
	up      *uploads.Service
	ch      *channels.Service
	pipe    *transfer.Pipeline
	ops     *fileops.Service
	body    []byte
}

func newAllocation(t *testing.T) *allocationFixture {
	t.Helper()
	db := testpostgres.New(t)
	seedChannelOwner(t, db.Pool, 1001)
	storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 2002}}, {ChannelID: 9002, Actors: []int64{1001, 2002}}, {ChannelID: 9003, Actors: []int64{1001, 2002}}})
	r := &allocationRemote{Storage: storage, id: 9002}
	if err := storage.Seed(fixture.Message{UserID: 2002, ChannelID: 9003, MessageID: 70, Bytes: []byte("independent foreign manifest bytes"), Preserve: true}); err != nil {
		t.Fatal(err)
	}
	policy := lifecycle.NewService(db.Pool, r)
	runtime, err := jobs.NewRuntime(db.Pool, r, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err = policy.BindReceiptEnqueuer(runtime); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewService(db.Pool, nil)
	if err = cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	up := uploads.NewService(db.Pool)
	if err = up.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	ch := channels.NewService(db.Pool, channels.TelegramCreator{Storage: r}, channels.Config{PartLimit: 1, AutoCreate: true})
	ops, err := fileops.NewService(db.Pool, cat, ch, r)
	if err != nil {
		t.Fatal(err)
	}
	if err = ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	return &allocationFixture{db: db, remote: r, policy: policy, runtime: runtime, cat: cat, up: up, ch: ch, ops: ops, pipe: transfer.NewPipeline(up, ch, r, nil, transfer.Config{Receipts: policy}), body: []byte("independent full winner payload\x00\xff")}
}
func (f *allocationFixture) session(t *testing.T, name string, u int64) uuid.UUID {
	t.Helper()
	s, e := f.up.Create(context.Background(), uploads.CreateInput{UserID: u, Name: name, ExpectedSize: int64(len(f.body))})
	if e != nil {
		t.Fatal(e)
	}
	return uuid.UUID(s.ID.Bytes)
}
func (f *allocationFixture) put(ctx context.Context, id uuid.UUID, u, c int64) (*transfer.UploadPartResult, error) {
	return f.pipe.UploadPart(ctx, transfer.UploadPartRequest{UserID: u, UploadID: id, PartNo: 1, RequestedChannelID: c, PlainSize: int64(len(f.body)), Body: bytes.NewReader(f.body)})
}
func (f *allocationFixture) complete(t *testing.T, id uuid.UUID, u int64) *sqlcgen.File {
	t.Helper()
	v, e := f.up.Complete(context.Background(), uploads.CompleteInput{UserID: u, UploadID: id, Size: int64(len(f.body))})
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func (f *allocationFixture) download(t *testing.T, part *sqlcgen.UploadPart) {
	t.Helper()
	r, e := f.remote.OpenRange(context.Background(), telegramstore.RangeRequest{UserID: 1001, ChannelID: part.ChannelID, MessageID: part.MessageID.Int64, Length: -1})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	got, e := io.ReadAll(r)
	if e != nil || sha256.Sum256(got) != sha256.Sum256(f.body) {
		t.Fatalf("independent downloaded SHA-256: %v", e)
	}
	if e = f.remote.CheckPreserved(); e != nil {
		t.Fatal(e)
	}
}
func (f *allocationFixture) snapshot(t *testing.T) string {
	t.Helper()
	var s string
	e := f.db.Pool.QueryRow(context.Background(), `SELECT jsonb_build_array((SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY id),'[]') FROM files f),(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM upload_sessions s),(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no),'[]') FROM file_parts p),(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no),'[]') FROM upload_parts p),(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM user_events e),(SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY user_id,channel_id),'[]') FROM channels c))::text`).Scan(&s)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func allocationWait(t *testing.T, ctx context.Context, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// The socket consumes the real response, but never delivers it to pgx. An
// independent connection proves durability before the competing writer starts.
type allocationSocket struct {
	net.Conn
	armed atomic.Bool
	fault *allocationWire
}

func (s *allocationSocket) Read(p []byte) (int, error) {
	n, e := s.Conn.Read(p)
	if n > 0 && s.armed.Swap(false) {
		s.fault.once.Do(func() { close(s.fault.received) })
		select {
		case <-s.fault.release:
		case <-s.fault.ctx.Done():
		}
		_ = s.Conn.Close()
		return 0, allocationACKLost
	}
	return n, e
}

type allocationWire struct {
	ctx               context.Context
	received, release chan struct{}
	once              sync.Once
	mu                sync.Mutex
	creating          map[uint32]bool
	used              bool
	match             string
}

func (w *allocationWire) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	pid := c.PgConn().PID()
	if strings.Contains(d.SQL, w.match) {
		w.creating[pid] = true
	}
	if strings.EqualFold(strings.TrimSpace(d.SQL), "commit") && w.creating[pid] && !w.used {
		w.used = true
		c.PgConn().Conn().(*allocationSocket).armed.Store(true)
	}
	return ctx
}
func (*allocationWire) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func allocationFaultPool(t *testing.T, f *allocationFixture, ctx context.Context) (*pgxpool.Pool, *allocationWire) {
	return allocationFaultPoolFor(t, f, ctx, "-- name: CreateChannel ")
}
func allocationFaultPoolFor(t *testing.T, f *allocationFixture, ctx context.Context, match string) (*pgxpool.Pool, *allocationWire) {
	t.Helper()
	w := &allocationWire{ctx: ctx, received: make(chan struct{}), release: make(chan struct{}), creating: make(map[uint32]bool), match: match}
	cfg, e := pgxpool.ParseConfig(f.db.URL)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.Tracer = w
	cfg.ConnConfig.AfterNetConnect = func(_ context.Context, _ *pgconn.Config, c net.Conn) (net.Conn, error) {
		return &allocationSocket{Conn: c, fault: w}, nil
	}
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
		pool.Close()
	})
	return pool, w
}
func allocationReport(t *testing.T, err error, id int64) {
	t.Helper()
	var report *channels.AllocationFailure
	if err == nil || !errors.As(err, &report) || report.ChannelID != id || report.UserID != 1001 || report.Disposition != "cleanup_not_attempted_retained" || report.Cause == nil {
		t.Fatalf("untruthful retained report: %#v %v", report, err)
	}
}

func allocationClosePool(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (error, <-chan struct{}) {
	t.Helper()
	closed := make(chan struct{})
	go func() { pool.Close(); close(closed) }()
	for {
		conn, err := pool.Acquire(ctx)
		if err == nil {
			conn.Release()
			continue
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		if errors.Is(err, puddle.ErrClosedPool) {
			return err, closed
		}
		// Close can cancel an in-flight constructor before a later acquire sees the closed pool.
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("observe closed allocation pool: %v", err)
		}
	}
}

func TestB2ChannelAllocationSafety(t *testing.T) {
	for _, mode := range []string{"rollover_commit_lost_ack_winner", "resolve_many_commit_lost_ack_winner", "admin_selected_commit_lost_ack_winner", "admin_unselected_commit_lost_ack_exposed"} {
		t.Run(mode, func(t *testing.T) {
			f := newAllocation(t)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			if mode == "resolve_many_commit_lost_ack_winner" {
				insertChannel(t, f.db.Pool, 1001, 9001, true)
				old := f.session(t, "capacity.bin", 1001)
				p, e := f.put(ctx, old, 1001, 9001)
				if e != nil {
					t.Fatal(e)
				}
				f.complete(t, old, 1001)
				f.download(t, p.Part)
			}
			pool, wire := allocationFaultPool(t, f, ctx)
			allocator := channels.NewService(pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{PartLimit: 1, AutoCreate: true})
			failed := make(chan error, 1)
			go func() {
				var e error
				switch mode {
				case "rollover_commit_lost_ack_winner":
					_, e = allocator.Resolve(ctx, 1001, 0)
				case "resolve_many_commit_lost_ack_winner":
					_, e = allocator.ResolveMany(ctx, 1001, 1)
				default:
					_, e = allocator.Create(ctx, 1001, "retained", mode != "admin_unselected_commit_lost_ack_exposed")
				}
				failed <- e
			}()
			allocationWait(t, ctx, wire.received)
			var durable bool
			if e := f.db.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM channels WHERE user_id=1001 AND channel_id=9002)").Scan(&durable); e != nil || !durable {
				t.Fatalf("no independent durable COMMIT: %v", e)
			}
			requested := int64(0)
			if mode == "admin_unselected_commit_lost_ack_exposed" {
				requested = 9002
			}
			resolver := channels.NewService(f.db.Pool, nil, channels.Config{PartLimit: 100})
			if got, e := resolver.Resolve(ctx, 1001, requested); e != nil || got != 9002 {
				t.Fatalf("actual independent Resolve: %d %v", got, e)
			}
			id := f.session(t, "winner.bin", 1001)
			part, e := f.put(ctx, id, 1001, requested)
			if e != nil {
				t.Fatal(e)
			}
			f.complete(t, id, 1001)
			f.download(t, part.Part)
			before := f.snapshot(t)
			close(wire.release)
			select {
			case e = <-failed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			allocationReport(t, e, 9002)
			if f.remote.deletes.Load() != 0 || len(f.remote.Deletes()) != 0 || before != f.snapshot(t) {
				t.Fatal("failed allocator changed winner, selection, receipts or events")
			}
			f.download(t, part.Part)
		})
	}
	for _, mode := range []string{"definitive_rollback_retained", "unavailable_readback_retained", "existing_channel_conflict_retained"} {
		t.Run(mode, func(t *testing.T) {
			f := newAllocation(t)
			insertChannel(t, f.db.Pool, 1001, 9001, true)
			ctx := context.Background()
			if mode == "existing_channel_conflict_retained" {
				insertChannel(t, f.db.Pool, 1001, 9002, false)
			} else if mode == "definitive_rollback_retained" {
				if _, e := f.db.Pool.Exec(ctx, "ALTER TABLE channels ADD CONSTRAINT reject_allocation CHECK(channel_id<>9002)"); e != nil {
					t.Fatal(e)
				}
			}
			svc := f.ch
			if mode == "unavailable_readback_retained" {
				cfg, e := pgxpool.ParseConfig(f.db.URL)
				if e != nil {
					t.Fatal(e)
				}
				pool, e := pgxpool.NewWithConfig(ctx, cfg)
				if e != nil {
					t.Fatal(e)
				}
				pool.Close()
				svc = channels.NewService(pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{})
			}
			before := f.snapshot(t)
			_, e := svc.Create(ctx, 1001, "retain", true)
			allocationReport(t, e, 9002)
			if before != f.snapshot(t) || f.remote.deletes.Load() != 0 {
				t.Fatal("retention failure changed current SQL state")
			}
			if mode == "definitive_rollback_retained" {
				t.Run("statement_boundaries", allocationStatementFailures)
			}
			if mode == "unavailable_readback_retained" {
				t.Run("commit_then_unavailable_readback", allocationUnavailableAfterCommit)
			}
		})
	}
	t.Run("provision_creator_error_enrollment", func(t *testing.T) {
		for _, caller := range []string{"rollover", "resolve_many", "admin"} {
			for _, known := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/known_%t", caller, known), func(t *testing.T) {
					f := newAllocation(t)
					cause := errors.New("provisioning primary")
					f.remote.createErr = &telegramstore.ChannelCreationError{UserID: 1001, ChannelID: 9002, Err: cause}
					if !known {
						f.remote.id = 0
						f.remote.createErr = cause
					}
					before := f.snapshot(t)
					var e error
					switch caller {
					case "rollover":
						_, e = f.ch.Resolve(context.Background(), 1001, 0)
					case "resolve_many":
						_, e = f.ch.ResolveMany(context.Background(), 1001, 1)
					default:
						_, e = f.ch.Create(context.Background(), 1001, "retained", true)
					}
					allocationReport(t, e, f.remote.id)
					if !errors.Is(e, cause) || before != f.snapshot(t) || f.remote.deletes.Load() != 0 {
						t.Fatal("creator failure lost cause or mutated state")
					}
				})
			}
		}
		t.Run("cancel_readback_selection", allocationCreatorFailureControls)
	})
	for _, mode := range []string{"provision_bot_lookup_discovered_copy_winner", "provision_bot_admin_discovered_copy_winner"} {
		t.Run(mode, func(t *testing.T) { allocationProvisionWinner(t, mode) })
	}
}

func TestB2ChannelAdminControls(t *testing.T) {
	t.Run("explicit_delete_success", func(t *testing.T) {
		t.Run("multi_owner_registrations_and_exact_token", allocationDeleteRegistrations)
		f := newAllocation(t)
		insertChannel(t, f.db.Pool, 1001, 9002, false)
		if e := f.remote.Seed(fixture.Message{UserID: 1001, ChannelID: 9002, MessageID: 10, Bytes: []byte("explicit unknown content")}); e != nil {
			t.Fatal(e)
		}
		if e := f.ch.Delete(context.Background(), 1001, 9002); e != nil {
			t.Fatal(e)
		}
		if _, ok := f.remote.Bytes(9002, 10); ok || f.remote.deletes.Load() != 1 {
			t.Fatal("explicit Delete did not destroy real bytes once")
		}
		var state string
		if e := f.db.Pool.QueryRow(context.Background(), "SELECT state FROM channel_deletion_intents WHERE channel_id=9002").Scan(&state); e != nil || state != "deleted" {
			t.Fatalf("missing durable tombstone %s %v", state, e)
		}
		if e := f.remote.CheckPreserved(); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("selected_and_referenced_refusal", func(t *testing.T) {
		f := newAllocation(t)
		insertChannel(t, f.db.Pool, 1001, 9002, true)
		if e := f.ch.Delete(context.Background(), 1001, 9002); !errors.Is(e, channels.ErrSelectedChannel) {
			t.Fatal(e)
		}
		if _, e := f.ch.Create(context.Background(), 1001, "control", false); e == nil {
			t.Fatal("duplicate create accepted")
		}
		id := f.session(t, "actual.bin", 1001)
		p, e := f.put(context.Background(), id, 1001, 9002)
		if e != nil {
			t.Fatal(e)
		}
		insertChannel(t, f.db.Pool, 1001, 9001, false)
		if _, e = f.ch.Select(context.Background(), 1001, 9001); e != nil {
			t.Fatal(e)
		}
		if e = f.ch.Delete(context.Background(), 1001, 9002); !errors.Is(e, channels.ErrChannelInUse) {
			t.Fatal(e)
		}
		if e = f.ch.Delete(context.Background(), 2002, 9002); !errors.Is(e, channels.ErrInvalidChannel) {
			t.Fatal(e)
		}
		f.download(t, p.Part)
		if f.remote.deletes.Load() != 0 {
			t.Fatal("protected channel deleted")
		}
	})
	for _, name := range []string{"explicit_delete_error_truthful", "delete_failed_retains_gate", "delete_cancelled_or_ambiguous_ack"} {
		t.Run(name, func(t *testing.T) {
			for _, mode := range []string{"permission", "transport", "inaccessible", "cancelled", "accepted_ack_lost"} {
				t.Run(mode, func(t *testing.T) {
					f := newAllocation(t)
					insertChannel(t, f.db.Pool, 1001, 9002, false)
					cause := errors.New("remote failure")
					if mode == "inaccessible" {
						cause = telegramstore.ErrInvalidChannel
					}
					f.remote.deleteErr = cause
					if mode == "accepted_ack_lost" {
						f.remote.deleteErr = nil
						f.remote.deleteRelease = make(chan struct{})
						f.remote.deleteEntered = make(chan struct{}, 1)
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if mode == "cancelled" {
						f.remote.deleteRelease = make(chan struct{})
						f.remote.deleteEntered = make(chan struct{}, 1)
					}
					done := make(chan error, 1)
					go func() { done <- f.ch.Delete(ctx, 1001, 9002) }()
					if mode == "cancelled" || mode == "accepted_ack_lost" {
						waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
						defer stop()
						allocationWait(t, waitCtx, f.remote.deleteEntered)
						if mode == "accepted_ack_lost" {
							if e := f.remote.Storage.DeleteChannel(waitCtx, 1001, 9002); e != nil {
								t.Fatal(e)
							}
							f.remote.deleteErr = cause
							close(f.remote.deleteRelease)
						} else {
							cancel()
							cause = context.Canceled
						}
					}
					e := <-done
					if !errors.Is(e, cause) {
						t.Fatalf("lost primary cause %v", e)
					}
					if _, e = f.ch.Resolve(context.Background(), 1001, 9002); !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
						t.Fatalf("failed Delete reopened gate: %v", e)
					}
					if e = f.ch.Delete(context.Background(), 1001, 9002); e == nil || f.remote.deletes.Load() != 1 {
						t.Fatal("failed Delete retried whole-channel RPC")
					}
				})
			}
			if name == "delete_cancelled_or_ambiguous_ack" {
				for _, stage := range []string{"InsertChannelDeletionIntent", "FinalizeChannelDeletionIntent"} {
					t.Run(stage+"_commit_lost_ack", func(t *testing.T) { allocationDeleteACKLoss(t, stage) })
				}
			}
		})
	}
	for _, name := range []string{"write_first_reserved_upload", "write_first_stored_upload", "write_first_completed_upload", "cross_owner_receipt_inflight_refusal"} {
		t.Run(name, func(t *testing.T) {
			f := newAllocation(t)
			insertChannel(t, f.db.Pool, 1001, 9002, false)
			u := int64(1001)
			if name == "cross_owner_receipt_inflight_refusal" {
				u = 2002
				insertChannel(t, f.db.Pool, u, 9002, false)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			id := f.session(t, "winner.bin", u)
			entered, release := make(chan struct{}), make(chan struct{})
			reserved := strings.Contains(name, "reserved") || u == 2002
			if reserved {
				f.remote.beforeUpload = func(ctx context.Context) error {
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			type result struct {
				p *transfer.UploadPartResult
				e error
			}
			done := make(chan result, 1)
			go func() { p, e := f.put(ctx, id, u, 9002); done <- result{p, e} }()
			var got result
			if reserved {
				allocationWait(t, ctx, entered)
			} else {
				got = <-done
				if got.e != nil {
					t.Fatal(got.e)
				}
				if name == "write_first_completed_upload" {
					f.complete(t, id, u)
				}
			}
			before := f.snapshot(t)
			if e := f.ch.Delete(ctx, 1001, 9002); !errors.Is(e, channels.ErrChannelInUse) {
				t.Fatalf("write-first Delete: %v", e)
			}
			if before != f.snapshot(t) || f.remote.deletes.Load() != 0 {
				t.Fatal("Delete changed protected writer")
			}
			if reserved {
				close(release)
				got = <-done
				if got.e != nil {
					t.Fatal(got.e)
				}
			}
			if name != "write_first_completed_upload" {
				f.complete(t, id, u)
			}
			f.download(t, got.p.Part)
			if u == 2002 {
				t.Run("receipt_without_reference", func(t *testing.T) { allocationCopyReservationFirstFor(t, 2002) })
			}
		})
	}
	t.Run("delete_first_explicit_upload", func(t *testing.T) {
		t.Run("original_empty_check_gap", func(t *testing.T) { allocationEmptyCheckGap(t, false) })
		t.Run("cancel_before_intent", func(t *testing.T) { allocationEmptyCheckGap(t, true) })
		f := newAllocation(t)
		insertChannel(t, f.db.Pool, 1001, 9002, false)
		id := f.session(t, "loser.bin", 1001)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := f.ch.Resolve(ctx, 1001, 9002); e != nil {
			t.Fatal(e)
		}
		f.remote.deleteEntered = make(chan struct{}, 1)
		f.remote.deleteRelease = make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- f.ch.Delete(ctx, 1001, 9002) }()
		allocationWait(t, ctx, f.remote.deleteEntered)
		before := f.snapshot(t)
		if _, e := f.put(ctx, id, 1001, 9002); !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
			t.Fatalf("closing channel admitted upload %v", e)
		}
		if _, e := f.up.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(f.body))}); e == nil {
			t.Fatal("loser completed")
		}
		if f.remote.uploads.Load() != 0 || before != f.snapshot(t) {
			t.Fatal("closing upload sent or published")
		}
		// Taking the same locks while remote is paused proves no SQL lock spans RPC.
		tx, e := f.db.Pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if e = lifecycle.LockOwnerTx(ctx, tx, 1001); e != nil {
			t.Fatal(e)
		}
		if e = lifecycle.LockChannelWritesTx(ctx, tx, []int64{9002}); e != nil {
			t.Fatal(e)
		}
		if e = tx.Rollback(ctx); e != nil {
			t.Fatal(e)
		}
		close(f.remote.deleteRelease)
		if e = <-done; e != nil {
			t.Fatal(e)
		}
	})
	t.Run("delete_first_copy_bind", func(t *testing.T) {
		t.Run("copy_reservation_first", func(t *testing.T) { allocationCopyReservationFirst(t) })
		f := newAllocation(t)
		insertChannel(t, f.db.Pool, 1001, 9001, true)
		id := f.session(t, "source.bin", 1001)
		part, e := f.put(context.Background(), id, 1001, 9001)
		if e != nil {
			t.Fatal(e)
		}
		source := f.complete(t, id, 1001)
		insertChannel(t, f.db.Pool, 1001, 9002, false)
		if e = f.ch.Delete(context.Background(), 1001, 9002); e != nil {
			t.Fatal(e)
		}
		// Reused creator identity must fail before CopyPart, even after registration loss.
		name := "copy.bin"
		_, e = f.ops.Copy(context.Background(), fileops.CopyInput{UserID: 1001, FileID: uuid.UUID(source.ID.Bytes), Name: &name})
		if !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
			t.Fatalf("copy reused deleted channel %v", e)
		}
		if f.remote.copies.Load() != 0 {
			t.Fatal("closing copy sent")
		}
		f.download(t, part.Part)
	})
	t.Run("closing_channel_discovery_selection_refused", func(t *testing.T) {
		f := newAllocation(t)
		insertChannel(t, f.db.Pool, 1001, 9002, false)
		f.remote.deleteErr = errors.New("retain closing")
		if e := f.ch.Delete(context.Background(), 1001, 9002); e == nil {
			t.Fatal("false delete success")
		}
		for range 2 {
			svc := channels.NewService(f.db.Pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{AutoCreate: true})
			remote := []channels.RemoteChannel{{ID: 9002, Name: "closed"}, {ID: 9001, Name: "fresh"}}
			filtered, e := svc.FilterDiscoverable(context.Background(), 1001, remote)
			if e != nil || len(filtered) != 1 || filtered[0].ID != 9001 {
				t.Fatalf("discovery gate %+v %v", filtered, e)
			}
			before := f.snapshot(t)
			if _, e = svc.Sync(context.Background(), 1001, remote); !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
				t.Fatal(e)
			}
			if _, e = svc.Select(context.Background(), 1001, 9002); !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
				t.Fatal(e)
			}
			if _, e = svc.Create(context.Background(), 1001, "reuse", true); !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
				t.Fatal(e)
			}
			if _, e = svc.Resolve(context.Background(), 1001, 9002); !errors.Is(e, lifecycle.ErrChannelWriteUnavailable) {
				t.Fatal(e)
			}
			if before != f.snapshot(t) {
				t.Fatal("closed channel resurrected")
			}
		}
	})
}

type allocationRunner struct{ api *tg.Client }

func (r allocationRunner) Run(ctx context.Context, _ int64, _ telegramstore.Operation, fn func(context.Context, *tg.Client) error) error {
	return fn(ctx, r.api)
}

type allocationBots struct {
	lookup           bool
	entered, release chan struct{}
	cause            error
}

func (b *allocationBots) ChannelBots(ctx context.Context, _ int64, _ *tg.Client) ([]tg.InputUserClass, error) {
	if b.lookup {
		close(b.entered)
		select {
		case <-b.release:
			return nil, b.cause
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return []tg.InputUserClass{&tg.InputUser{UserID: 11, AccessHash: 12}}, nil
}

type allocationProvisionInvoker struct {
	server  *localtelegram.Server
	bots    *allocationBots
	deletes atomic.Int32
}

func (i *allocationProvisionInvoker) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	switch in.(type) {
	case *tg.ChannelsEditAdminRequest:
		close(i.bots.entered)
		select {
		case <-i.bots.release:
			return i.bots.cause
		case <-ctx.Done():
			return ctx.Err()
		}
	case *tg.ChannelsDeleteChannelRequest, *tg.ChannelsDeleteMessagesRequest:
		i.deletes.Add(1)
	}
	return i.server.Invoke(ctx, in, out)
}
func allocationProvisionWinner(t *testing.T, mode string) {
	t.Helper()
	db := testpostgres.New(t)
	seedChannelOwner(t, db.Pool, 1001)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	emulator, e := localtelegram.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	cause := errors.New("provisioning primary")
	bots := &allocationBots{lookup: strings.Contains(mode, "lookup"), entered: make(chan struct{}), release: make(chan struct{}), cause: cause}
	invoker := &allocationProvisionInvoker{server: emulator, bots: bots}
	runner := allocationRunner{tg.NewClient(invoker)}
	storage := telegramstore.NewGotdStorage(runner, nil)
	policy := lifecycle.NewService(db.Pool, storage)
	runtime, e := jobs.NewRuntime(db.Pool, storage, policy)
	if e != nil {
		t.Fatal(e)
	}
	if e = policy.BindReceiptEnqueuer(runtime); e != nil {
		t.Fatal(e)
	}
	cat := catalog.NewService(db.Pool, nil)
	if e = cat.BindLifecycle(policy); e != nil {
		t.Fatal(e)
	}
	up := uploads.NewService(db.Pool)
	if e = up.BindLifecycle(policy); e != nil {
		t.Fatal(e)
	}
	ch := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 100, AutoCreate: true})
	sourceChannel, e := ch.Create(ctx, 1001, "source", true)
	if e != nil {
		t.Fatal(e)
	}
	pipe := transfer.NewPipeline(up, ch, storage, nil, transfer.Config{Receipts: policy})
	body := []byte("pre-provision source full payload\x00\xff")
	foreign := []byte("independent foreign full payload")
	upload := func(owner int64, name string, data []byte) (*sqlcgen.File, *sqlcgen.UploadPart) {
		t.Helper()
		s, e := up.Create(ctx, uploads.CreateInput{UserID: owner, Name: name, ExpectedSize: int64(len(data))})
		if e != nil {
			t.Fatal(e)
		}
		p, e := pipe.UploadPart(ctx, transfer.UploadPartRequest{UserID: owner, UploadID: uuid.UUID(s.ID.Bytes), PartNo: 1, RequestedChannelID: sourceChannel.ChannelID, PlainSize: int64(len(data)), Body: bytes.NewReader(data)})
		if e != nil {
			t.Fatal(e)
		}
		f, e := up.Complete(ctx, uploads.CompleteInput{UserID: owner, UploadID: uuid.UUID(s.ID.Bytes), Size: int64(len(data))})
		if e != nil {
			t.Fatal(e)
		}
		return f, p.Part
	}
	source, sourcePart := upload(1001, "source.bin", body)
	if _, e = ch.Sync(ctx, 2002, []channels.RemoteChannel{{ID: sourceChannel.ChannelID, Name: "shared source"}}); e != nil {
		t.Fatal(e)
	}
	_, foreignPart := upload(2002, "foreign.bin", foreign)
	creator := channels.NewService(db.Pool, channels.TelegramCreator{Storage: telegramstore.NewGotdStorage(runner, nil, telegramstore.WithBotProvider(bots))}, channels.Config{})
	failed := make(chan error, 1)
	go func() { _, err := creator.Create(ctx, 1001, "exposed", true); failed <- err }()
	allocationWait(t, ctx, bots.entered)
	account, e := telegramstore.NewGotdAccount(runner)
	if e != nil {
		t.Fatal(e)
	}
	discovered, e := account.DiscoverChannels(ctx, 1001)
	if e != nil {
		t.Fatal(e)
	}
	remote := make([]channels.RemoteChannel, 0, len(discovered))
	var destination int64
	for _, c := range discovered {
		remote = append(remote, channels.RemoteChannel{ID: c.ID, Name: c.Name})
		if c.Name == "exposed" {
			destination = c.ID
		}
	}
	if destination == 0 {
		t.Fatal("actual Discover missed provisioning channel")
	}
	if _, e = ch.Sync(ctx, 1001, remote); e != nil {
		t.Fatal(e)
	}
	if _, e = ch.Select(ctx, 1001, destination); e != nil {
		t.Fatal(e)
	}
	ops, e := fileops.NewService(db.Pool, cat, ch, storage)
	if e != nil {
		t.Fatal(e)
	}
	if e = ops.BindLifecycle(policy); e != nil {
		t.Fatal(e)
	}
	name := "copy-winner.bin"
	winner, e := ops.Copy(ctx, fileops.CopyInput{UserID: 1001, FileID: uuid.UUID(source.ID.Bytes), Name: &name})
	if e != nil {
		t.Fatal(e)
	}
	var winnerMessage int64
	if e = db.Pool.QueryRow(ctx, "SELECT message_id FROM file_parts WHERE file_id=$1 AND channel_id=$2", uuid.UUID(winner.ID.Bytes), destination).Scan(&winnerMessage); e != nil {
		t.Fatal(e)
	}
	download := func(owner, c, m int64, want []byte) {
		t.Helper()
		r, e := storage.OpenRange(ctx, telegramstore.RangeRequest{UserID: owner, ChannelID: c, MessageID: m, Length: -1})
		if e != nil {
			t.Fatal(e)
		}
		got, e := io.ReadAll(r)
		_ = r.Close()
		if e != nil || sha256.Sum256(got) != sha256.Sum256(want) {
			t.Fatalf("independent full GotdStorage download changed: %v", e)
		}
	}
	download(1001, destination, winnerMessage, body)
	snapshot := func() string {
		t.Helper()
		var v string
		e := db.Pool.QueryRow(ctx, `SELECT jsonb_build_array((SELECT jsonb_agg(to_jsonb(c) ORDER BY user_id,channel_id) FROM channels c),(SELECT jsonb_agg(to_jsonb(f) ORDER BY id) FROM files f),(SELECT jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no) FROM file_parts p),(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM remote_write_receipts r),(SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM user_events e))::text`).Scan(&v)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	before := snapshot()
	close(bots.release)
	err := <-failed
	allocationReport(t, err, destination)
	var creation *telegramstore.ChannelCreationError
	if !errors.Is(err, cause) || !errors.As(err, &creation) || creation.ChannelID != destination || invoker.deletes.Load() != 0 || before != snapshot() {
		t.Fatal("provisioning failure lost winner, typed cause, known channel or selection")
	}
	download(1001, destination, winnerMessage, body)
	download(1001, sourcePart.ChannelID, sourcePart.MessageID.Int64, body)
	download(2002, foreignPart.ChannelID, foreignPart.MessageID.Int64, foreign)
}

func allocationDeleteACKLoss(t *testing.T, stage string) {
	t.Helper()
	f := newAllocation(t)
	insertChannel(t, f.db.Pool, 1001, 9002, false)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	pool, wire := allocationFaultPoolFor(t, f, ctx, "-- name: "+stage+" ")
	svc := channels.NewService(pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{})
	done := make(chan error, 1)
	go func() { done <- svc.Delete(ctx, 1001, 9002) }()
	allocationWait(t, ctx, wire.received)
	var state string
	if e := f.db.Pool.QueryRow(ctx, "SELECT state FROM channel_deletion_intents WHERE channel_id=9002").Scan(&state); e != nil {
		t.Fatal(e)
	}
	want := "closing"
	calls := int32(0)
	if stage == "FinalizeChannelDeletionIntent" {
		want = "deleted"
		calls = 1
	}
	if state != want || f.remote.deletes.Load() != calls {
		t.Fatalf("independent committed intent %s RPC %d", state, f.remote.deletes.Load())
	}
	close(wire.release)
	if e := <-done; e == nil {
		t.Fatal("lost SQL ACK became successful Delete")
	}
	if e := f.ch.Delete(ctx, 1001, 9002); e == nil || f.remote.deletes.Load() != calls {
		t.Fatal("uncertain Delete retried RPC")
	}
	if e := f.remote.CheckPreserved(); e != nil {
		t.Fatal(e)
	}
}

func allocationBlockedPID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blocker uint32) uint32 {
	t.Helper()
	var pid uint32
	for pid == 0 {
		if e := ctx.Err(); e != nil {
			t.Fatal(e)
		}
		if e := pool.QueryRow(ctx, "SELECT COALESCE((SELECT pid FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid)) LIMIT 1),0)", blocker).Scan(&pid); e != nil {
			t.Fatal(e)
		}
	}
	if e := fixture.WaitForBlocking(ctx, pool, pid, blocker); e != nil {
		t.Fatal(e)
	}
	return pid
}

func allocationEmptyCheckGap(t *testing.T, cancelBefore bool) {
	t.Helper()
	f := newAllocation(t)
	insertChannel(t, f.db.Pool, 1001, 9002, false)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	id := f.session(t, "gap.bin", 1001)
	// Trigger stops actual insertion after the real protection SELECT, not a copied check.
	if _, e := f.db.Pool.Exec(ctx, `CREATE FUNCTION block_channel_intent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(732094); RETURN NEW; END $$; CREATE TRIGGER block_channel_intent BEFORE INSERT ON channel_deletion_intents FOR EACH ROW EXECUTE FUNCTION block_channel_intent()`); e != nil {
		t.Fatal(e)
	}
	blocker, e := f.db.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer blocker.Rollback(ctx)
	if _, e = blocker.Exec(ctx, "SELECT pg_advisory_xact_lock(732094)"); e != nil {
		t.Fatal(e)
	}
	var blockerPID uint32
	if e = blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); e != nil {
		t.Fatal(e)
	}
	deleteCtx, stop := context.WithCancel(ctx)
	defer stop()
	deleted := make(chan error, 1)
	go func() { deleted <- f.ch.Delete(deleteCtx, 1001, 9002) }()
	deletePID := allocationBlockedPID(t, ctx, f.db.Pool, blockerPID)
	type result struct {
		p *transfer.UploadPartResult
		e error
	}
	written := make(chan result, 1)
	go func() { p, e := f.put(ctx, id, 1001, 9002); written <- result{p, e} }()
	allocationBlockedPID(t, ctx, f.db.Pool, deletePID)
	if cancelBefore {
		stop()
		if e = <-deleted; !errors.Is(e, context.Canceled) {
			t.Fatalf("pre-intent cancellation %v", e)
		}
		if e = blocker.Rollback(ctx); e != nil {
			t.Fatal(e)
		}
		got := <-written
		if got.e != nil {
			t.Fatal(got.e)
		}
		f.complete(t, id, 1001)
		f.download(t, got.p.Part)
		var n int
		if e = f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM channel_deletion_intents").Scan(&n); e != nil || n != 0 || f.remote.deletes.Load() != 0 {
			t.Fatal("cancelled pre-intent deletion closed channel")
		}
		return
	}
	if e = blocker.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-deleted; e != nil {
		t.Fatal(e)
	}
	got := <-written
	if !errors.Is(got.e, lifecycle.ErrChannelWriteUnavailable) || f.remote.uploads.Load() != 0 || f.remote.deletes.Load() != 1 {
		t.Fatalf("empty-check gap admitted writer: %v", got.e)
	}
}

func allocationCopyReservationFirst(t *testing.T) {
	allocationCopyReservationFirstFor(t, 1001)
}
func allocationCopyReservationFirstFor(t *testing.T, owner int64) {
	t.Helper()
	f := newAllocation(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	insertChannel(t, f.db.Pool, owner, 9001, true)
	id := f.session(t, "source.bin", owner)
	sourcePart, e := f.put(ctx, id, owner, 9001)
	if e != nil {
		t.Fatal(e)
	}
	source := f.complete(t, id, owner)
	insertChannel(t, f.db.Pool, owner, 9002, false)
	if owner != 1001 {
		insertChannel(t, f.db.Pool, 1001, 9002, false)
	}
	if _, e = f.ch.Select(ctx, owner, 9002); e != nil {
		t.Fatal(e)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	f.remote.beforeCopy = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		name := "copy.bin"
		_, err := f.ops.Copy(ctx, fileops.CopyInput{UserID: owner, FileID: uuid.UUID(source.ID.Bytes), Name: &name})
		done <- err
	}()
	allocationWait(t, ctx, entered)
	if _, e = f.ch.Select(ctx, owner, 9001); e != nil {
		t.Fatal(e)
	}
	before := f.snapshot(t)
	if e = f.ch.Delete(ctx, 1001, 9002); !errors.Is(e, channels.ErrChannelInUse) {
		t.Fatalf("no-reference copy reservation ignored: %v", e)
	}
	if before != f.snapshot(t) || f.remote.deletes.Load() != 0 {
		t.Fatal("Delete changed copy reservation")
	}
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	f.download(t, sourcePart.Part)
	var message int64
	if e = f.db.Pool.QueryRow(ctx, "SELECT message_id FROM file_parts WHERE channel_id=9002").Scan(&message); e != nil {
		t.Fatal(e)
	}
	reader, e := f.remote.OpenRange(ctx, telegramstore.RangeRequest{UserID: 1001, ChannelID: 9002, MessageID: message, Length: -1})
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(reader)
	_ = reader.Close()
	if e != nil || sha256.Sum256(got) != sha256.Sum256(f.body) {
		t.Fatal("copy winner SHA-256 changed")
	}
}

type allocationFailureTrace struct {
	match string
	fired atomic.Bool
}

func (t *allocationFailureTrace) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.Contains(strings.ToLower(d.SQL), strings.ToLower(t.match)) && t.fired.CompareAndSwap(false, true) {
		_ = c.PgConn().Conn().Close()
	}
	return ctx
}
func (*allocationFailureTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func allocationStatementFailures(t *testing.T) {
	t.Helper()
	for _, caller := range []string{"rollover", "resolve_many", "admin"} {
		for _, boundary := range []string{"begin", "-- name: ClearSelectedChannel ", "-- name: CreateChannel ", "-- name: SelectChannel ", "commit"} {
			t.Run(caller+"/"+strings.TrimSpace(boundary), func(t *testing.T) {
				f := newAllocation(t)
				insertChannel(t, f.db.Pool, 1001, 9001, true)
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				defer cancel()
				id := f.session(t, "capacity.bin", 1001)
				p, e := f.put(ctx, id, 1001, 9001)
				if e != nil {
					t.Fatal(e)
				}
				f.complete(t, id, 1001)
				f.download(t, p.Part)
				trace := &allocationFailureTrace{match: boundary}
				cfg, e := pgxpool.ParseConfig(f.db.URL)
				if e != nil {
					t.Fatal(e)
				}
				cfg.ConnConfig.Tracer = trace
				pool, e := pgxpool.NewWithConfig(ctx, cfg)
				if e != nil {
					t.Fatal(e)
				}
				defer pool.Close()
				svc := channels.NewService(pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{PartLimit: 1, AutoCreate: true})
				before := f.snapshot(t)
				switch caller {
				case "rollover":
					_, e = svc.Resolve(ctx, 1001, 0)
				case "resolve_many":
					_, e = svc.ResolveMany(ctx, 1001, 1)
				default:
					_, e = svc.Create(ctx, 1001, "retained", true)
				}
				allocationReport(t, e, 9002)
				if !trace.fired.Load() || f.remote.deletes.Load() != 0 || before != f.snapshot(t) {
					t.Fatal("statement failure changed selection or deleted remote channel")
				}
				var report *channels.AllocationFailure
				if !errors.As(e, &report) || report.Cause == nil {
					t.Fatal("primary SQL cause missing")
				}
			})
		}
	}
}

func allocationUnavailableAfterCommit(t *testing.T) {
	t.Helper()
	f := newAllocation(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	pool, wire := allocationFaultPool(t, f, ctx)
	allocator := channels.NewService(pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{})
	done := make(chan error, 1)
	go func() { _, e := allocator.Create(ctx, 1001, "retained", true); done <- e }()
	allocationWait(t, ctx, wire.received)
	var durable bool
	if e := f.db.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM channels WHERE user_id=1001 AND channel_id=9002)").Scan(&durable); e != nil || !durable {
		t.Fatal("allocation did not really commit")
	}
	id := f.session(t, "actual-winner.bin", 1001)
	p, e := f.put(ctx, id, 1001, 9002)
	if e != nil {
		t.Fatal(e)
	}
	f.complete(t, id, 1001)
	before := f.snapshot(t)
	unavailable, closed := allocationClosePool(t, ctx, pool)
	close(wire.release)
	e = <-done
	allocationReport(t, e, 9002)
	var report *channels.AllocationFailure
	if !errors.As(e, &report) {
		t.Fatalf("missing retained allocation report: %v", e)
	}
	if report.Cause == nil || !errors.Is(e, report.Cause) {
		t.Fatalf("missing actual COMMIT primary: %v", report.Cause)
	}
	if !errors.Is(report.Secondary, unavailable) || !errors.Is(e, unavailable) {
		t.Fatalf("missing actual readback secondary: primary=%v secondary=%v want=%v", report.Cause, report.Secondary, unavailable)
	}
	allocationWait(t, ctx, closed)
	if before != f.snapshot(t) || f.remote.deletes.Load() != 0 {
		t.Fatal("unavailable reconciliation damaged committed winner")
	}
	f.download(t, p.Part)
}

func allocationCreatorFailureControls(t *testing.T) {
	t.Helper()
	for _, caller := range []string{"rollover", "resolve_many", "admin"} {
		for _, mode := range []string{"cancelled_context", "readback_unavailable", "concurrent_select"} {
			for _, known := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/known_%t", caller, mode, known), func(t *testing.T) {
					f := newAllocation(t)
					insertChannel(t, f.db.Pool, 1001, 9001, true)
					insertChannel(t, f.db.Pool, 1001, 9003, false)
					ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
					defer cancel()
					requestCtx, stop := context.WithCancel(ctx)
					defer stop()
					cfg, e := pgxpool.ParseConfig(f.db.URL)
					if e != nil {
						t.Fatal(e)
					}
					pool, e := pgxpool.NewWithConfig(ctx, cfg)
					if e != nil {
						t.Fatal(e)
					}
					defer pool.Close()
					svc := channels.NewService(pool, channels.TelegramCreator{Storage: f.remote}, channels.Config{PartLimit: 1, AutoCreate: true})
					// Fill selected channel through actual candidate publication, not a fabricated reference.
					id := f.session(t, "capacity.bin", 1001)
					p, e := f.put(ctx, id, 1001, 9001)
					if e != nil {
						t.Fatal(e)
					}
					f.complete(t, id, 1001)
					primary := errors.New("creator primary")
					var unavailable error
					want := f.snapshot(t)
					var closed <-chan struct{}
					f.remote.create = func(context.Context) (telegramstore.Channel, error) {
						switch mode {
						case "cancelled_context":
							stop()
						case "readback_unavailable":
							unavailable, closed = allocationClosePool(t, ctx, pool)
						case "concurrent_select":
							if _, e = f.ch.Select(ctx, 1001, 9003); e != nil {
								return telegramstore.Channel{}, e
							}
							want = f.snapshot(t)
						}
						if !known {
							return telegramstore.Channel{}, primary
						}
						return telegramstore.Channel{ID: 9002, Name: "retained"}, &telegramstore.ChannelCreationError{UserID: 1001, ChannelID: 9002, Err: primary}
					}
					switch caller {
					case "rollover":
						_, e = svc.Resolve(requestCtx, 1001, 0)
					case "resolve_many":
						_, e = svc.ResolveMany(requestCtx, 1001, 1)
					default:
						_, e = svc.Create(requestCtx, 1001, "retained", true)
					}
					expectedID := int64(0)
					if known {
						expectedID = 9002
					}
					allocationReport(t, e, expectedID)
					var report *channels.AllocationFailure
					var creation *telegramstore.ChannelCreationError
					if !errors.Is(e, primary) || !errors.As(e, &report) || want != f.snapshot(t) || f.remote.deletes.Load() != 0 {
						t.Fatal("creator failure changed legitimate state or lost primary")
					}
					if known && (!errors.As(e, &creation) || creation.ChannelID != 9002) {
						t.Fatal("known creation identity lost")
					}
					if mode == "readback_unavailable" {
						if !errors.Is(report.Secondary, unavailable) || !errors.Is(e, unavailable) {
							t.Fatalf("readback failure secondary lost: primary=%v secondary=%v want=%v", report.Cause, report.Secondary, unavailable)
						}
						allocationWait(t, ctx, closed)
					}
					f.download(t, p.Part)
				})
			}
		}
	}
}

func allocationDeleteRegistrations(t *testing.T) {
	t.Helper()
	f := newAllocation(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	insertChannel(t, f.db.Pool, 1001, 9001, true)
	insertChannel(t, f.db.Pool, 1001, 9002, false)
	insertChannel(t, f.db.Pool, 2002, 9002, false)
	if e := f.remote.Seed(fixture.Message{UserID: 1001, ChannelID: 9002, MessageID: 10, Bytes: []byte("explicit physical channel contents")}); e != nil {
		t.Fatal(e)
	}
	before := f.snapshot(t)
	tx, e := f.db.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if e = lifecycle.LockOwnerTx(ctx, tx, 1001); e != nil {
		t.Fatal(e)
	}
	if e = lifecycle.LockChannelWritesTx(ctx, tx, []int64{9002}); e != nil {
		t.Fatal(e)
	}
	q := sqlcgen.New(tx)
	token, wrong := dbtypes.UUID(uuid.New()), dbtypes.UUID(uuid.New())
	if _, e = q.InsertChannelDeletionIntent(ctx, sqlcgen.InsertChannelDeletionIntentParams{ChannelID: 9002, IntentID: token, RequestedBy: 1001}); e != nil {
		t.Fatal(e)
	}
	changed, e := q.FinalizeChannelDeletionIntent(ctx, sqlcgen.FinalizeChannelDeletionIntentParams{ChannelID: 9002, IntentID: wrong})
	if e != nil || changed != 0 {
		t.Fatalf("wrong token finalized intent: %d %v", changed, e)
	}
	removed, e := q.DeleteChannelRegistrationsForIntent(ctx, sqlcgen.DeleteChannelRegistrationsForIntentParams{ChannelID: 9002, IntentID: wrong})
	if e != nil || removed != 0 {
		t.Fatalf("wrong token removed registrations: %d %v", removed, e)
	}
	removed, e = q.DeleteChannelRegistrationsForIntent(ctx, sqlcgen.DeleteChannelRegistrationsForIntentParams{ChannelID: 9002, IntentID: token})
	if e != nil || removed != 0 {
		t.Fatalf("unfinalized intent removed registrations: %d %v", removed, e)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	if before != f.snapshot(t) || f.remote.deletes.Load() != 0 {
		t.Fatal("zero-CAS/token refusal changed registrations or remote bytes")
	}
	if e = f.ch.Delete(ctx, 1001, 9002); e != nil {
		t.Fatal(e)
	}
	var remaining, unrelated int
	if e = f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM channels WHERE channel_id=9002").Scan(&remaining); e != nil || remaining != 0 {
		t.Fatalf("physical registrations remain across owners: %d %v", remaining, e)
	}
	if e = f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM channels WHERE user_id=1001 AND channel_id=9001 AND selected").Scan(&unrelated); e != nil || unrelated != 1 {
		t.Fatalf("unrelated registration changed: %d %v", unrelated, e)
	}
	intent, e := sqlcgen.New(f.db.Pool).GetChannelDeletionIntent(ctx, 9002)
	if e != nil || intent.State != "deleted" {
		t.Fatalf("physical tombstone lost: %+v %v", intent, e)
	}
	tx, e = f.db.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if e = lifecycle.LockOwnerTx(ctx, tx, 1001); e != nil {
		t.Fatal(e)
	}
	if e = lifecycle.LockChannelWritesTx(ctx, tx, []int64{9002}); e != nil {
		t.Fatal(e)
	}
	q = sqlcgen.New(tx)
	changed, e = q.FinalizeChannelDeletionIntent(ctx, sqlcgen.FinalizeChannelDeletionIntentParams{ChannelID: 9002, IntentID: intent.IntentID})
	if e != nil || changed != 0 {
		t.Fatalf("deleted intent reopened or finalized twice: %d %v", changed, e)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	if _, ok := f.remote.Bytes(9002, 10); ok || f.remote.deletes.Load() != 1 {
		t.Fatal("actual physical deletion did not destroy bytes exactly once")
	}
	if e = f.remote.CheckPreserved(); e != nil {
		t.Fatal(e)
	}
}
