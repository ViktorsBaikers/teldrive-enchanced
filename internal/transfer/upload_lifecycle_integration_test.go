//go:build integration

package transfer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/contentcrypto"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type b2UploadQueue struct{ fail error }

func (q *b2UploadQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	if q.fail != nil {
		return 0, q.fail
	}
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO b2_upload_jobs(user_id,receipt_ids) VALUES ($1,$2) RETURNING id", in.UserID, in.ReceiptIDs).Scan(&id)
	return id, err
}

type b2UploadStorage struct {
	*fixture.Storage
	mu     sync.Mutex
	sends  int
	after  func()
	change func(telegramstore.StoredPart) telegramstore.StoredPart
}

func (s *b2UploadStorage) Upload(ctx context.Context, in telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	s.sends++
	s.mu.Unlock()
	part, err := s.Storage.Upload(ctx, in)
	if s.after != nil {
		s.after()
	}
	if s.change != nil {
		part = s.change(part)
	}
	return part, err
}
func (s *b2UploadStorage) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.sends }

type b2UploadFixture struct {
	pool     *pgxpool.Pool
	service  *uploads.Service
	ledger   *lifecycle.Service
	queue    *b2UploadQueue
	storage  *b2UploadStorage
	pipeline *transfer.Pipeline
	id       uuid.UUID
	body     []byte
}

func newB2Upload(t *testing.T, encrypted, unhashed bool) *b2UploadFixture {
	t.Helper()
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	if _, err := db.Pool.Exec(context.Background(), "CREATE TABLE b2_upload_jobs(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,user_id bigint NOT NULL,receipt_ids uuid[] NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	storage := &b2UploadStorage{Storage: fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}, {ChannelID: 9002, Actors: []int64{1001}}})}
	for _, message := range []fixture.Message{{UserID: 1002, ChannelID: 9001, MessageID: 90, Bytes: []byte("foreign source and share"), Preserve: true}, {UserID: 1001, ChannelID: 9002, MessageID: 91, Bytes: []byte("old destination"), Preserve: true}} {
		if err := storage.Seed(message); err != nil {
			t.Fatal(err)
		}
	}
	queue := &b2UploadQueue{}
	ledger := lifecycle.NewService(db.Pool, storage)
	if err := ledger.BindReceiptEnqueuer(queue); err != nil {
		t.Fatal(err)
	}
	service := uploads.NewService(db.Pool)
	if err := service.BindLifecycle(ledger); err != nil {
		t.Fatal(err)
	}
	body := []byte("independent upload body")
	in := uploads.CreateInput{UserID: 1001, Name: "b2.bin", ExpectedSize: int64(len(body)), PartSize: treehash.BlockSize, Encryption: encrypted}
	var keys transfer.StaticKeyProvider
	if encrypted {
		version := int32(7)
		in.EncryptionKeyVersion = &version
		keys = transfer.StaticKeyProvider{7: "synthetic-content-key"}
	}
	session, err := service.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := dbtypes.GoogleUUID(session.ID)
	pipeline := transfer.NewPipeline(service, channels.NewService(db.Pool, nil, channels.Config{}), storage, keys, transfer.Config{Receipts: ledger, DisableHashing: unhashed})
	return &b2UploadFixture{db.Pool, service, ledger, queue, storage, pipeline, id, body}
}
func (f *b2UploadFixture) put(ctx context.Context, body []byte) (*transfer.UploadPartResult, error) {
	return f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(f.body)), Body: bytes.NewReader(body)})
}
func (f *b2UploadFixture) snapshot(t *testing.T) string {
	t.Helper()
	var snapshot string
	err := f.pool.QueryRow(context.Background(), `SELECT jsonb_build_array(
	(SELECT to_jsonb(s) FROM upload_sessions s WHERE id=$1),
	(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY part_no),'[]') FROM upload_parts p WHERE upload_id=$1),
	(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY p.file_id,p.part_no),'[]') FROM file_parts p),
	(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),
	(SELECT count(*) FROM user_events))::text`, f.id).Scan(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func (f *b2UploadFixture) independentDownload(t *testing.T, part *sqlcgen.UploadPart) {
	t.Helper()
	// Read actual remote bytes, independently of the PUT reader/hash response.
	reader, err := f.storage.OpenRange(context.Background(), telegramstore.RangeRequest{UserID: 1001, ChannelID: part.ChannelID, MessageID: part.MessageID.Int64, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	if part.Salt.Valid {
		cipher, e := contentcrypto.NewCipher("synthetic-content-key", part.Salt.String)
		if e != nil {
			reader.Close()
			t.Fatal(e)
		}
		decrypted, e := cipher.DecryptData(reader)
		if e != nil {
			reader.Close()
			t.Fatal(e)
		}
		reader = decrypted
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil || sha256.Sum256(got) != sha256.Sum256(f.body) {
		t.Fatalf("independent download bytes changed: %v", err)
	}
	if err := f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}

func TestB2CancellationCompensation(t *testing.T) {
	for _, outcome := range []string{"stored_winner", "receipt_with_error", "cleanup_failure"} {
		t.Run(outcome, func(t *testing.T) {
			f := newB2Upload(t, false, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.storage.after = cancel
			if outcome != "stored_winner" {
				f.storage.SetSendFaults(fixture.SendFaults{UploadError: fixture.ErrResponseLost})
			}
			if outcome == "cleanup_failure" {
				f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: fixture.ErrProcessLost})
			}
			result, err := f.put(ctx, f.body)
			if outcome == "stored_winner" {
				if err != nil || result == nil {
					t.Fatalf("canceled successful send not finalized: %+v %v", result, err)
				}
				f.independentDownload(t, result.Part)
				if len(f.storage.Deletes()) != 0 {
					t.Fatal("stored winner compensated")
				}
			} else {
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatalf("primary failure lost: %v", err)
				}
				if outcome == "cleanup_failure" && !errors.Is(err, fixture.ErrProcessLost) {
					t.Fatalf("cleanup cause lost: %v", err)
				}
				var state string
				var job int64
				if e := f.pool.QueryRow(context.Background(), "SELECT state,job_id FROM remote_write_receipts WHERE upload_id=$1", f.id).Scan(&state, &job); e != nil || job <= 0 {
					t.Fatalf("missing durable debt: %s %d %v", state, job, e)
				}
				if outcome == "receipt_with_error" && state != "deleted" {
					t.Fatalf("owned compensation state=%s", state)
				}
				if len(f.storage.Deletes()) != 1 {
					t.Fatal("detached exact cleanup did not run once")
				}
			}
		})
	}
}

func TestB2UploadReceiptFailures(t *testing.T) {
	for _, fault := range []string{"reserve", "observe", "observe_with_primary", "store", "enqueue", "lease", "receipt_error", "wrong_channel", "nonpositive", "unknown", "unknown_destination", "size", "body_long", "checksum", "legacy_failed_retarget", "legacy_expired_retarget"} {
		t.Run(fault, func(t *testing.T) {
			f := newB2Upload(t, false, false)
			ctx := context.Background()
			switch fault {
			case "reserve":
				_, err := f.pool.Exec(ctx, "INSERT INTO channel_deletion_intents(channel_id,intent_id,requested_by) VALUES (9001,$1,1001)", uuid.New())
				if err != nil {
					t.Fatal(err)
				}
			case "observe", "observe_with_primary":
				_, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT b2_observe_fault CHECK (message_id IS NULL)")
				if err != nil {
					t.Fatal(err)
				}
				if fault == "observe_with_primary" {
					f.storage.SetSendFaults(fixture.SendFaults{UploadError: fixture.ErrResponseLost})
				}
			case "store":
				_, err := f.pool.Exec(ctx, "ALTER TABLE upload_parts ADD CONSTRAINT b2_store_fault CHECK (state <> 'stored')")
				if err != nil {
					t.Fatal(err)
				}
			case "enqueue":
				f.queue.fail = fixture.ErrProcessLost
				f.storage.SetSendFaults(fixture.SendFaults{UploadError: fixture.ErrResponseLost})
			case "lease":
				f.storage.after = func() {
					if _, err := f.pool.Exec(ctx, "UPDATE upload_parts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE upload_id=$1", f.id); err != nil {
						t.Error(err)
					}
				}
			case "receipt_error":
				f.storage.SetSendFaults(fixture.SendFaults{UploadError: fixture.ErrResponseLost})
			case "wrong_channel":
				f.storage.change = func(p telegramstore.StoredPart) telegramstore.StoredPart { p.ChannelID = 9002; return p }
			case "nonpositive":
				f.storage.change = func(p telegramstore.StoredPart) telegramstore.StoredPart { p.MessageID = 0; return p }
			case "unknown":
				f.storage.SetSendFaults(fixture.SendFaults{UploadWithoutReceipt: true})
			case "unknown_destination":
				if _, err := f.pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", f.id); err != nil {
					t.Fatal(err)
				}
			case "size":
				f.storage.change = func(p telegramstore.StoredPart) telegramstore.StoredPart { p.Size++; return p }
			case "legacy_failed_retarget", "legacy_expired_retarget":
				state := "failed"
				if fault == "legacy_expired_retarget" {
					state = "uploading"
				}
				_, err := f.pool.Exec(ctx, "INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,state,lease_token,lease_expires_at) VALUES ($1,1,9002,91,$2,$3,$4,clock_timestamp()-interval '1 second')", f.id, len(f.body), state, uuid.New())
				if err != nil {
					t.Fatal(err)
				}
				before := f.snapshot(t)
				_, err = f.service.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: f.id, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(f.body))})
				if !errors.Is(err, uploads.ErrPartConflict) || before != f.snapshot(t) {
					t.Fatalf("legacy physical identity retargeted: %v", err)
				}
				return
			}
			before := f.snapshot(t)
			body := f.body
			if fault == "body_long" {
				body = append(bytes.Clone(body), 'x')
			}
			var err error
			if fault == "checksum" {
				wrong := checksumFor([]byte("different checksum"))
				_, err = f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(body)), Checksum: &wrong, Body: bytes.NewReader(body)})
			} else {
				_, err = f.put(ctx, body)
			}
			if err == nil {
				t.Fatal("fault accepted")
			}
			if fault == "unknown_destination" && (!errors.Is(err, uploads.ErrParentUnavailable) || f.storage.count() != 0 || f.snapshot(t) != before) {
				t.Fatalf("unknown destination admitted or mutated: %v", err)
			}
			if fault == "reserve" && f.storage.count() != 0 {
				t.Fatal("Resolve hint authorized send past intent")
			}
			if fault == "observe" || fault == "observe_with_primary" || fault == "enqueue" || fault == "wrong_channel" || fault == "nonpositive" || fault == "unknown" {
				if len(f.storage.Deletes()) != 0 {
					t.Fatal("unproved identity/persistence authorized delete")
				}
			}
			if fault == "enqueue" && (!errors.Is(err, fixture.ErrProcessLost) || !errors.Is(err, fixture.ErrResponseLost)) {
				t.Fatalf("lost both outcomes: %v", err)
			}
			if fault == "observe_with_primary" && !errors.Is(err, fixture.ErrResponseLost) {
				t.Fatalf("observation failure hid primary: %v", err)
			}
			if fault == "store" || fault == "lease" || fault == "receipt_error" || fault == "size" || fault == "body_long" || fault == "checksum" {
				var state string
				var attempts int
				if e := f.pool.QueryRow(ctx, "SELECT state,attempts FROM remote_write_receipts WHERE upload_id=$1 AND channel_id=9001 AND message_id=91 AND job_id>0", f.id).Scan(&state, &attempts); e != nil || state != "deleted" || attempts != 1 {
					t.Fatalf("exact owned cleanup debt=%s attempts=%d %v", state, attempts, e)
				}
				calls := f.storage.Deletes()
				if len(calls) != 1 || calls[0].ChannelID != 9001 || !reflect.DeepEqual(calls[0].MessageIDs, []int64{91}) {
					t.Fatalf("wrong physical compensation: %+v", calls)
				}
			}
			var stored int
			if e := f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1 AND state='stored'", f.id).Scan(&stored); e != nil || stored != 0 {
				t.Fatalf("partial bind: %d %v", stored, e)
			}
			if e := f.storage.CheckPreserved(); e != nil {
				t.Fatal(e)
			}
		})
	}
	for _, unavailable := range []bool{false, true} {
		name := "store_lost_ack"
		if unavailable {
			name = "store_lost_ack_unavailable_readback"
		}
		t.Run(name, func(t *testing.T) {
			f := newB2Upload(t, false, false)
			catalog := &b2StoreAckCatalog{Service: f.service}
			if unavailable {
				oldPool := f.pool
				observer, e := pgxpool.NewWithConfig(context.Background(), oldPool.Config())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(observer.Close)
				catalog.afterStore = oldPool.Close
				f.pool = observer
				f.service = uploads.NewService(observer)
				f.ledger = lifecycle.NewService(observer, f.storage)
				if e = f.ledger.BindReceiptEnqueuer(f.queue); e != nil {
					t.Fatal(e)
				}
			}
			f.pipeline = transfer.NewPipeline(catalog, channels.NewService(f.pool, nil, channels.Config{}), f.storage, nil, transfer.Config{Receipts: f.ledger})
			_, err := f.put(context.Background(), f.body)
			if !errors.Is(err, fixture.ErrResponseLost) {
				t.Fatalf("lost commit response cause: %v", err)
			}
			if unavailable && catalog.readErr == nil {
				t.Fatal("closed service pool did not fail real independent readback")
			}
			current, e := f.service.GetPart(context.Background(), 1001, f.id, 1)
			if e != nil || current.State != sqlcgen.UploadPartStateStored {
				t.Fatalf("actual SQL winner lost: %+v %v", current, e)
			}
			before := f.snapshot(t)
			var receiptID uuid.UUID
			if e = f.pool.QueryRow(context.Background(), "SELECT id FROM remote_write_receipts WHERE upload_id=$1", f.id).Scan(&receiptID); e != nil {
				t.Fatal(e)
			}
			if e = f.ledger.Recover(context.Background(), 1001, []uuid.UUID{receiptID}); e != nil {
				t.Fatal(e)
			}
			if f.snapshot(t) != before || len(f.storage.Deletes()) != 0 {
				t.Fatal("ambiguous commit compensated durable winner")
			}
			f.independentDownload(t, current)
		})
	}
}

// The real claim stays leased until Pipeline has observed Busy. Completion and
// exact alias retirement commit before releasing the waiting request.
type b2ReplayCatalog struct {
	*uploads.Service
	busy        chan struct{}
	release     chan struct{}
	once        sync.Once
	hideInitial bool
	reads       int
}

func (c *b2ReplayCatalog) GetPart(ctx context.Context, u int64, id uuid.UUID, n int32) (*sqlcgen.UploadPart, error) {
	c.reads++
	if c.hideInitial && c.reads == 1 {
		return nil, uploads.ErrNotFound
	}
	return c.Service.GetPart(ctx, u, id, n)
}
func (c *b2ReplayCatalog) ClaimPart(ctx context.Context, in uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	part, err := c.Service.ClaimPart(ctx, in)
	if errors.Is(err, uploads.ErrPartBusy) {
		c.once.Do(func() { close(c.busy) })
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return part, err
}

func TestB2CompletedPartReplay(t *testing.T) {
	for _, name := range []string{"direct_equal", "direct_different", "direct_short", "direct_long", "claim_completed", "waiter_equal", "waiter_different", "waiter_short", "waiter_long", "list_completed", "result_trash_purge", "replay_current_result_access"} {
		t.Run(name, func(t *testing.T) {
			for _, mode := range []string{"plain", "encrypted", "hash_disabled"} {
				t.Run(mode, func(t *testing.T) {
					f := newB2Upload(t, mode == "encrypted", mode == "hash_disabled")
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					var uploaded *transfer.UploadPartResult
					var waiter <-chan error
					var waiting *b2ReplayCatalog
					body := bytes.Clone(f.body)
					if strings.HasSuffix(name, "different") {
						body[0] ^= 1
					}
					if strings.HasSuffix(name, "short") {
						body = body[:len(body)-1]
					}
					if strings.HasSuffix(name, "long") {
						body = append(body, 'x')
					}
					if strings.HasPrefix(name, "waiter_") {
						barrier := fixture.NewBarrier(t, nil)
						f.storage.SetSendFaults(fixture.SendFaults{AfterUpload: barrier})
						type outcome struct {
							result *transfer.UploadPartResult
							err    error
						}
						first := make(chan outcome, 1)
						go func() { r, e := f.put(ctx, f.body); first <- outcome{r, e} }()
						if err := barrier.Wait(ctx); err != nil {
							t.Fatal(err)
						}
						waiting = &b2ReplayCatalog{Service: f.service, busy: make(chan struct{}), release: make(chan struct{})}
						pipeline := transfer.NewPipeline(waiting, &fixedResolver{channelID: 9001}, f.storage, nil, transfer.Config{Receipts: f.ledger})
						done := make(chan error, 1)
						waiter = done
						go func() {
							_, e := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(f.body)), Body: bytes.NewReader(body)})
							done <- e
						}()
						select {
						case <-waiting.busy:
						case <-ctx.Done():
							t.Fatal("waiter never held by real lease")
						}
						barrier.Release()
						select {
						case got := <-first:
							if got.err != nil {
								t.Fatal(got.err)
							}
							uploaded = got.result
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					} else {
						var err error
						uploaded, err = f.put(ctx, f.body)
						if err != nil {
							t.Fatal(err)
						}
					}
					file, err := f.service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: f.id, Size: int64(len(f.body))})
					if err != nil {
						t.Fatal(err)
					}
					var aliases int
					if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", f.id).Scan(&aliases); err != nil || aliases != 0 {
						t.Fatalf("actual completion did not retire exact aliases: %d %v", aliases, err)
					}
					before := f.snapshot(t)
					sends := f.storage.count()
					want := error(nil)
					if mode == "hash_disabled" {
						want = uploads.ErrPartConflict
					} else if strings.HasSuffix(name, "different") {
						want = uploads.ErrPartConflict
					} else if strings.HasSuffix(name, "short") {
						want = transfer.ErrBodyTooShort
					} else if strings.HasSuffix(name, "long") {
						want = transfer.ErrBodyTooLong
					}
					switch {
					case waiter != nil:
						close(waiting.release)
						select {
						case err = <-waiter:
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					case name == "claim_completed":
						claim, e := f.service.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(f.body)), ChannelID: 9002})
						err = e
						if err == nil && (!claim.Existing || claim.LeaseToken != uuid.Nil || !reflect.DeepEqual(claim.Part, uploaded.Part)) {
							t.Fatal("completed claim changed proof or acquired lease")
						}
						if err == nil {
							claimRace := &b2ReplayCatalog{Service: f.service, hideInitial: true}
							pipeline := transfer.NewPipeline(claimRace, &fixedResolver{channelID: 9001}, f.storage, nil, transfer.Config{Receipts: f.ledger})
							result, e := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(f.body)), Body: bytes.NewReader(body)})
							err = e
							if err == nil && (result == nil || !result.Existing) {
								t.Fatal("claim race did not route actual completed body proof")
							}
						}
					case name == "list_completed":
						parts, e := f.service.ListParts(ctx, uploads.ListPartsInput{UserID: 1001, UploadID: f.id, Limit: 1})
						err = e
						want = nil
						if err == nil && (len(parts) != 1 || !reflect.DeepEqual(parts[0], uploaded.Part)) {
							t.Fatalf("completed projection differs: %+v", parts)
						}
						cursor := int32(1)
						next, e := f.service.ListParts(ctx, uploads.ListPartsInput{UserID: 1001, UploadID: f.id, AfterPartNo: &cursor, Limit: 1})
						if e != nil || len(next) != 0 {
							t.Fatalf("cursor replay: %+v %v", next, e)
						}
					case name == "result_trash_purge":
						f.independentDownload(t, uploaded.Part)
						hierarchy := catalog.NewService(f.pool, nil)
						if e := hierarchy.BindLifecycle(f.ledger); e != nil {
							t.Fatal(e)
						}
						operations, e := fileops.NewService(f.pool, hierarchy, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
						if e != nil {
							t.Fatal(e)
						}
						if e = operations.BindLifecycle(f.ledger); e != nil {
							t.Fatal(e)
						}
						resultID := uuid.UUID(file.ID.Bytes)
						if _, e = hierarchy.Trash(ctx, 1001, resultID); e != nil {
							t.Fatal(e)
						}
						if _, e = f.put(ctx, f.body); !errors.Is(e, uploads.ErrNotFound) {
							t.Fatalf("trashed result replay: %v", e)
						}
						if e = operations.QueuePurge(ctx, 1001, resultID); e != nil {
							t.Fatal(e)
						}
						if _, e = f.put(ctx, f.body); !errors.Is(e, uploads.ErrNotFound) {
							t.Fatalf("pending result replay: %v", e)
						}
						if e = operations.PurgeMany(ctx, 1001, []uuid.UUID{resultID}); e != nil {
							t.Fatal(e)
						}
						if _, ok := f.storage.Bytes(uploaded.Part.ChannelID, uploaded.Part.MessageID.Int64); ok {
							t.Fatal("owned bytes remain after real purge ACK")
						}
						if len(f.storage.Deletes()) != 1 {
							t.Fatal("real purge did not delete exactly once")
						}
						var purged int
						if e = f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE resource_id=$1 AND event_type='file.purged'", resultID.String()).Scan(&purged); e != nil || purged != 1 {
							t.Fatalf("purge event=%d %v", purged, e)
						}
						_, err = f.put(ctx, f.body)
						want = uploads.ErrNotFound
						before = f.snapshot(t)
					case name == "replay_current_result_access":
						hierarchy := catalog.NewService(f.pool, nil)
						if e := hierarchy.BindLifecycle(f.ledger); e != nil {
							t.Fatal(e)
						}
						reader := &b2AfterBody{Reader: bytes.NewReader(f.body), after: func() {
							if _, e := hierarchy.Trash(ctx, 1001, uuid.UUID(file.ID.Bytes)); e != nil {
								t.Error(e)
							}
						}}
						_, err = f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(f.body)), Body: reader})
						if mode != "hash_disabled" {
							want = uploads.ErrNotFound
						}
						before = f.snapshot(t)
					default:
						_, err = f.put(ctx, body)
					}
					if !errors.Is(err, want) {
						t.Fatalf("replay=%v, want %v", err, want)
					}
					wantDeletes := 0
					if name == "result_trash_purge" {
						wantDeletes = 1
					}
					if f.snapshot(t) != before || f.storage.count() != sends || len(f.storage.Deletes()) != wantDeletes {
						t.Fatal("read-only replay mutated durable identity/events/remote bytes")
					}
					if name != "result_trash_purge" {
						f.independentDownload(t, uploaded.Part)
					}
					if e := f.storage.CheckPreserved(); e != nil {
						t.Fatal(e)
					}
				})
			}
		})
	}
}

type b2AfterBody struct {
	*bytes.Reader
	once  sync.Once
	after func()
}

func (r *b2AfterBody) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	if e == io.EOF {
		r.once.Do(r.after)
	}
	return n, e
}

// The decorator returns a lost response only after the real SQL Store commits.
// Domain tests separately inject at the real transaction Commit seam.
type b2StoreAckCatalog struct {
	*uploads.Service
	returned   bool
	afterStore func()
	readErr    error
}

func (c *b2StoreAckCatalog) StorePart(ctx context.Context, in uploads.StorePartInput) (*sqlcgen.UploadPart, error) {
	part, err := c.Service.StorePart(ctx, in)
	if err != nil {
		return part, err
	}
	c.returned = true
	if c.afterStore != nil {
		c.afterStore()
	}
	return part, fixture.ErrResponseLost
}
func (c *b2StoreAckCatalog) GetPart(ctx context.Context, userID int64, id uuid.UUID, n int32) (*sqlcgen.UploadPart, error) {
	part, err := c.Service.GetPart(ctx, userID, id, n)
	if c.returned {
		c.readErr = err
	}
	return part, err
}
