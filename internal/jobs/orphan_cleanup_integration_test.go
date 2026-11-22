//go:build integration

package jobs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestOrphanCleanupDeletesOnlyExpiredUnreferencedDocuments(t *testing.T) {
	// Age and absence alone never authorize the historical orphan expectation.
	f := newCleanupFixture(t)
	f.owned(t, 1001, 9001, 10, true)
	for _, id := range []int64{11, 12} {
		if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: id, Bytes: []byte(fmt.Sprint(id)), Preserve: true}); err != nil {
			t.Fatal(err)
		}
	}
	f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{
		{ID: 12, CreatedAt: time.Unix(0, 0)}, {ID: 11, CreatedAt: time.Now()}, {ID: 10, CreatedAt: time.Unix(0, 0)},
	}, Exhausted: true}}
	worker := jobs.NewOrphanedTelegramPartsCleanupWorker(f.pool, f.policy, f.storage, 7*24*time.Hour)
	if worker.Timeout(nil) != 4*time.Hour {
		t.Fatal("changed maintenance timeout")
	}
	if err := worker.Work(context.Background(), &river.Job[jobs.OrphanCleanupArgs]{}); err != nil {
		t.Fatal(err)
	}
	cleanupDeleted(t, f.storage, "1001/9001/10")
}

func TestB2MixedChannelOwnership(t *testing.T) {
	for _, shape := range []string{"empty", "singleton", "duplicates", "multipage", "nonadvancing", "partial_failure"} {
		t.Run(shape, func(t *testing.T) {
			f := newCleanupFixture(t)
			ctx := context.Background()
			eligible, _ := f.owned(t, 1001, 9001, 10, true)
			_, _ = f.owned(t, 1001, 9001, 20, false)
			protected, _ := f.owned(t, 1001, 9001, 30, true)
			if _, err := f.pool.Exec(ctx, "INSERT INTO channels(channel_id,user_id,name) VALUES(9001,1002,'shared fixture')"); err != nil {
				t.Fatal(err)
			}
			_, _ = f.owned(t, 1002, 9001, 40, true)
			// The shared registration ends before scanning, leaving a foreign receipt in owner 1001's listing.
			if _, err := f.pool.Exec(ctx, "DELETE FROM channels WHERE channel_id=9001 AND user_id=1002"); err != nil {
				t.Fatal(err)
			}
			recent, _ := f.owned(t, 1001, 9001, 60, true)
			alias := uuid.New()
			if _, err := f.pool.Exec(ctx, `WITH f AS (
 INSERT INTO files(id,user_id,name,kind,size,status,mod_time,deleted_at)
 SELECT $1,1002,'all-owner terminal alias','file',expected_size,'deletion_pending',now(),now()
 FROM upload_sessions WHERE id=$2 RETURNING id,size)
 INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size)
 SELECT id,1,9001,30,size,size FROM f`, alias, protected); err != nil {
				t.Fatal(err)
			}
			if err := f.storage.Seed(fixture.Message{UserID: 1002, ChannelID: 9001, MessageID: 50, Bytes: []byte("foreign unknown exact bytes"), Preserve: true}); err != nil {
				t.Fatal(err)
			}
			missing := uuid.New()
			if _, err := f.pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,size,mod_time) VALUES($1,1001,'private broken file','file',5,now());", missing); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id) VALUES($1,1,9001,99)", missing); err != nil {
				t.Fatal(err)
			}
			old := time.Unix(0, 0)
			messages := []telegramstore.DocumentMessage{{ID: 60, CreatedAt: time.Now()}, {ID: 50, CreatedAt: old}, {ID: 40, CreatedAt: old}, {ID: 30, CreatedAt: old}, {ID: 20, CreatedAt: old}, {ID: 10, CreatedAt: old}}
			switch shape {
			case "empty":
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Exhausted: true}}
			case "singleton":
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: messages[5:], Exhausted: true}}
			case "duplicates":
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: messages, BeforeID: 10}, {Messages: messages[5:], Exhausted: true}}
			case "multipage":
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: messages[:3], BeforeID: 40}, {Messages: messages[3:], BeforeID: 10}, {Exhausted: true}}
			case "nonadvancing":
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: messages, BeforeID: 10}, {BeforeID: 10}}
			case "partial_failure":
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: messages, Exhausted: true}}
				f.owned(t, 1002, 9002, 70, true)
				f.storage.pages[9002] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{{ID: 70, CreatedAt: old}}, Exhausted: true}}
				f.storage.fail[9002] = errors.New("synthetic channel permission denied")
			}
			retained := []uuid.UUID{protected, recent, alias}
			before := cleanupSnapshot(t, f.pool, retained)
			var protectedBytes = make(map[int64][]byte)
			for _, id := range []int64{20, 30, 40, 50, 60} {
				body, ok := f.storage.Bytes(9001, id)
				if !ok {
					t.Fatal("missing initial bytes")
				}
				protectedBytes[id] = body
			}
			for pass := range 2 {
				start := len(f.storage.requests)
				raw, err := recordedCleanup(t, f.pool, jobs.NewOrphanedTelegramPartsCleanupWorker(f.pool, f.policy, f.storage, 24*time.Hour), jobs.OrphanCleanupArgs{})
				var report jobs.OrphanCleanupOutput
				if e := json.Unmarshal(raw, &report); e != nil {
					t.Fatal(e)
				}
				if report.Incomplete != (err != nil) || len(raw) > 16<<20 {
					t.Fatalf("false or unbounded report=%s", raw)
				}
				for _, secret := range []string{"private broken file", missing.String(), "messageIds", "missingMessageIds", "channelId"} {
					if bytes.Contains(raw, []byte(secret)) {
						t.Fatalf("global output leaked private diagnosis: %s", raw)
					}
				}
				if shape != "empty" && report.Deleted != 1-pass {
					t.Fatalf("false acknowledged count=%s", raw)
				}
				if shape != "nonadvancing" && (!report.BrokenTruncated || report.BrokenTotal != 1 || len(report.BrokenFiles) != 0) {
					t.Fatalf("broken diagnostics lost or leaked=%s", raw)
				}
				if shape == "partial_failure" && (report.FailedChannels != 1 || report.Ownership.Failed != 1 || report.Ownership.Unknown != 1 || report.Ownership.Foreign != 1 || report.Ownership.Protected != 2) {
					t.Fatalf("partial ownership report=%s", raw)
				}
				if (shape == "nonadvancing" || shape == "partial_failure") != (err != nil) {
					t.Fatalf("shape=%s error=%v", shape, err)
				}
				if shape == "partial_failure" && !errors.Is(err, f.storage.fail[9002]) {
					t.Fatalf("lost primary channel error: %v", err)
				}
				limit := map[string]int{"empty": 3, "singleton": 3, "duplicates": 4, "multipage": 5, "nonadvancing": 4, "partial_failure": 3}[shape]
				if got := len(f.storage.requests) - start; got != limit {
					t.Fatalf("pass%d listing calls=%d want%d", pass, got, limit)
				}
				for _, request := range f.storage.requests[start:] {
					if request.Limit != 100 {
						t.Fatalf("unbounded listing: %+v", request)
					}
				}
				for id, body := range protectedBytes {
					assertCleanupBytes(t, f.storage, 9001, id, body)
				}
				if got := cleanupSnapshot(t, f.pool, retained); got != before {
					t.Fatal("scan changed protected/recent evidence")
				}
			}
			if shape == "empty" {
				cleanupDeleted(t, f.storage)
			} else {
				cleanupDeleted(t, f.storage, "1001/9001/10")
			}
			var parts int
			if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", eligible).Scan(&parts); err != nil || parts != 1 {
				t.Fatalf("orphan scan stole target SQL finalization: %d %v", parts, err)
			}
		})
	}
}

func TestB2ChannelAuthorization(t *testing.T) {
	for _, failure := range []error{telegramstore.ErrInvalidChannel, errors.New("synthetic authentication failed"), errors.New("synthetic permission denied")} {
		t.Run(failure.Error(), func(t *testing.T) {
			f := newCleanupFixture(t)
			ctx := context.Background()
			id, receipt := f.owned(t, 1001, 9001, 80, true)
			f.owned(t, 1003, 9003, 81, true)
			body, _ := f.storage.Bytes(9001, 80)
			f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{{ID: 80, CreatedAt: time.Unix(0, 0)}}, Exhausted: true}}
			f.storage.pages[9003] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{{ID: 81, CreatedAt: time.Unix(0, 0)}}, Exhausted: true}}
			f.storage.fail[9001] = failure
			worker := jobs.NewOrphanedTelegramPartsCleanupWorker(f.pool, f.policy, f.storage, 0)
			if err := worker.Work(ctx, &river.Job[jobs.OrphanCleanupArgs]{}); !errors.Is(err, failure) {
				t.Fatalf("channel failure became absence: %v", err)
			}
			assertCleanupBytes(t, f.storage, 9001, 80, body)
			var state string
			var count, attempts int
			if err := f.pool.QueryRow(ctx, "SELECT state,attempts,(SELECT count(*) FROM upload_parts WHERE upload_id=$2) FROM remote_write_receipts WHERE id=$1", receipt, id).Scan(&state, &attempts, &count); err != nil {
				t.Fatal(err)
			}
			if state != "cleanup_pending" || attempts != 1 || count != 1 {
				t.Fatalf("failed evidence=%s/%d/%d", state, attempts, count)
			}
			before := cleanupSnapshot(t, f.pool, []uuid.UUID{id})
			raw, err := json.Marshal([]map[string]any{{"user_id": int64(1001), "upload_id": id, "part_no": 1, "channel_id": int64(9001), "message_id": int64(80)}})
			if err != nil {
				t.Fatal(err)
			}
			if n, err := sqlcgen.New(f.pool).DeleteUploadPartsForCleanup(ctx, raw); err != nil || n != 0 {
				t.Fatalf("direct failed ACK erased target: %d %v", n, err)
			}
			if after := cleanupSnapshot(t, f.pool, []uuid.UUID{id}); after != before {
				t.Fatal("failed direct ACK changed exact evidence")
			}
			cleanupDeleted(t, f.storage, "1003/9003/81")
		})
	}
	t.Run("wrong_actor_and_listing_failure", func(t *testing.T) {
		f := newCleanupFixture(t)
		ctx := context.Background()
		if _, err := f.pool.Exec(ctx, "INSERT INTO channels(channel_id,user_id,name) VALUES(9001,1002,'shared fixture')"); err != nil {
			t.Fatal(err)
		}
		id, _ := f.owned(t, 1002, 9001, 90, true)
		if _, err := f.pool.Exec(ctx, "DELETE FROM channels WHERE channel_id=9001 AND user_id=1002"); err != nil {
			t.Fatal(err)
		}
		before := cleanupSnapshot(t, f.pool, []uuid.UUID{id})
		result, err := f.policy.CleanupMessagesWithinBudget(ctx, 1001, 9001, []int64{90})
		if err != nil || result.Foreign != 1 || result.Deleted != 0 {
			t.Fatalf("wrong actor result=%+v %v", result, err)
		}
		if after := cleanupSnapshot(t, f.pool, []uuid.UUID{id}); after != before {
			t.Fatal("wrong actor changed foreign evidence")
		}
		f.storage.listError[9001] = telegramstore.ErrInvalidChannel
		if err = jobs.NewOrphanedTelegramPartsCleanupWorker(f.pool, f.policy, f.storage, 0).Work(ctx, &river.Job[jobs.OrphanCleanupArgs]{}); !errors.Is(err, telegramstore.ErrInvalidChannel) {
			t.Fatalf("listing absence lie=%v", err)
		}
		cleanupDeleted(t, f.storage)
	})
	t.Run("ack_failure_retains_exact_target", func(t *testing.T) {
		f := newCleanupFixture(t)
		ctx := context.Background()
		id, receipt := f.owned(t, 1001, 9001, 91, true)
		if _, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT cleanup_ack_fault CHECK(state<>'deleted')"); err != nil {
			t.Fatal(err)
		}
		if err := f.run(t); err == nil {
			t.Fatal("ACK failure reported success")
		}
		var count int
		if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("failed ACK erased target: %d %v", count, err)
		}
		if _, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts DROP CONSTRAINT cleanup_ack_fault"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", receipt); err != nil {
			t.Fatal(err)
		}
		if err := f.run(t); err != nil {
			t.Fatal(err)
		}
		if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&count); err != nil || count != 0 {
			t.Fatalf("exact ACK recovery failed: %d %v", count, err)
		}
		cleanupDeleted(t, f.storage, "1001/9001/91")
	})
	t.Run("all_owner_all_state_reference_veto", func(t *testing.T) {
		for _, kind := range []string{"open", "aborted", "expired", "completed", "active", "trashed", "deletion_pending"} {
			t.Run(kind, func(t *testing.T) {
				f := newCleanupFixture(t)
				ctx := context.Background()
				target, receipt := f.owned(t, 1001, 9001, 92, false)
				f.owned(t, 1003, 9003, 93, true)
				alias := uuid.New()
				retained := []uuid.UUID{target, alias}
				body, _ := f.storage.Bytes(9001, 92)
				switch kind {
				case "open", "aborted", "expired", "completed":
					file, err := f.uploads.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: target, Size: int64(len(body))})
					if err != nil {
						t.Fatal(err)
					}
					fileID := uuid.UUID(file.ID.Bytes)
					retained = append(retained, fileID)
					if _, err = f.pool.Exec(ctx, "UPDATE files SET status='deletion_pending',deleted_at=now() WHERE id=$1", fileID); err != nil {
						t.Fatal(err)
					}
					if _, err = f.pool.Exec(ctx, `WITH completed_file AS (
 INSERT INTO files(id,user_id,name,kind,size,mod_time)
 SELECT $1,1002,'independent completed result','file',$3,now() WHERE $2::upload_state='completed' RETURNING id)
 INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,state,expires_at,completed_at,file_id)
 VALUES($1,1002,'independent upload reference',$3,now(),16777216,$2::upload_state,now()+interval '1 day',
 CASE WHEN $2::upload_state='completed' THEN now() END,(SELECT id FROM completed_file))`, alias, kind, int64(len(body))); err != nil {
						t.Fatal(err)
					}
					if _, err = f.pool.Exec(ctx, "INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,state) VALUES($1,1,9001,92,$2,$2,'stored')", alias, int64(len(body))); err != nil {
						t.Fatal(err)
					}
				default:
					if _, err := f.uploads.Abort(ctx, 1001, target); err != nil {
						t.Fatal(err)
					}
					if _, err := f.pool.Exec(ctx, `INSERT INTO files(id,user_id,name,kind,size,status,mod_time,deleted_at)
 VALUES($1,1002,'independent file reference','file',$3,$2::file_status,now(),
 CASE WHEN $2::file_status='active' THEN NULL ELSE now() END)`, alias, kind, int64(len(body))); err != nil {
						t.Fatal(err)
					}
					if _, err := f.pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) VALUES($1,1,9001,92,$2,$2)", alias, int64(len(body))); err != nil {
						t.Fatal(err)
					}
				}
				var eligible, referenced bool
				if err := f.pool.QueryRow(ctx, "SELECT origin_eligible,has_references FROM lifecycle_receipt_status WHERE id=$1", receipt).Scan(&eligible, &referenced); err != nil || !eligible || !referenced {
					t.Fatalf("reference veto fixture eligible=%t referenced=%t: %v", eligible, referenced, err)
				}
				before := cleanupSnapshot(t, f.pool, retained)
				f.storage.pages[9001] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{{ID: 92, CreatedAt: time.Unix(0, 0)}}, Exhausted: true}}
				f.storage.pages[9003] = []telegramstore.DocumentMessagePage{{Messages: []telegramstore.DocumentMessage{{ID: 93, CreatedAt: time.Unix(0, 0)}}, Exhausted: true}}
				for range 2 {
					if err := jobs.NewOrphanedTelegramPartsCleanupWorker(f.pool, f.policy, f.storage, 0).Work(ctx, &river.Job[jobs.OrphanCleanupArgs]{}); err != nil {
						t.Fatal(err)
					}
					if after := cleanupSnapshot(t, f.pool, retained); after != before {
						t.Fatal("independent reference or target evidence changed")
					}
					assertCleanupBytes(t, f.storage, 9001, 92, body)
				}
				cleanupDeleted(t, f.storage, "1003/9003/93")
			})
		}
	})
}
