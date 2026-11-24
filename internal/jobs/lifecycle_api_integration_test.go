//go:build integration

package jobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

func TestB2CleanupJobVisibility(t *testing.T) {
	for _, outcome := range []string{"pending", "completed", "unresolved", "exhausted"} {
		t.Run(outcome, func(t *testing.T) {
			f := newReceiptRuntimeFixture(t)
			ctx := context.Background()
			var first int64
			var receipt uuid.UUID
			if outcome != "unresolved" {
				f.copyCrash(t)
				if err := f.db.Pool.QueryRow(ctx, "SELECT id FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&receipt); err != nil {
					t.Fatal(err)
				}
				if err := f.policy.DiscoverRecoveryCandidate(ctx, receipt, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				if err := f.db.Pool.QueryRow(ctx, "SELECT job_id FROM remote_write_receipts WHERE id=$1", receipt).Scan(&first); err != nil {
					t.Fatal(err)
				}
				if first <= 0 {
					t.Fatal("report did not use actual positive owner River ID")
				}
				if outcome == "completed" {
					if err := f.policy.Recover(ctx, 1001, []uuid.UUID{receipt}); err != nil {
						t.Fatal(err)
					}
				}
				if outcome == "exhausted" {
					// Claims, not fixture state rewrites, consume all four reservations.
					for range 4 {
						claims, err := f.policy.ClaimDelete(ctx, 1001, []uuid.UUID{receipt})
						if err != nil || len(claims) != 1 {
							t.Fatalf("durable claim: %v count=%d", err, len(claims))
						}
						if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", receipt); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := f.policy.ClaimDelete(ctx, 1001, []uuid.UUID{receipt}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if outcome == "unresolved" {
				f.start(t)
				f.noReceipt(t)
				if err := f.db.Pool.QueryRow(ctx, "SELECT id,job_id FROM remote_write_receipts WHERE row_kind='copy_operation' AND state='unresolved' ORDER BY created_at DESC LIMIT 1").Scan(&receipt, &first); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.runtime.GetForUser(ctx, first, 1001)
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "unresolved" || outcome == "exhausted" {
				if before.State != "discarded" {
					t.Fatalf("terminal ledger outcome hidden by live row: %+v", before)
				}
				if _, err = f.runtime.RetryForUser(ctx, first, 1001); !errors.Is(err, ErrReceiptRetryUnavailable) {
					t.Fatalf("terminal retry was not refused: %v", err)
				}
			}
			if _, err = f.runtime.GetForUser(ctx, first, 1002); !errors.Is(err, river.ErrNotFound) {
				t.Fatalf("other owner sees exact identity: %v", err)
			}
			foreign, _, err := f.runtime.List(ctx, ListInput{UserID: 1002, Kind: ReceiptCleanupKind, Limit: 200})
			if err != nil || len(foreign) != 0 {
				t.Fatalf("foreign list leak: %+v %v", foreign, err)
			}
			// Disconnect/retention cannot erase original primary or safe cleanup codes.
			if _, err = f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE kind=$1", ReceiptCleanupKind); err != nil {
				t.Fatal(err)
			}
			retained, err := f.runtime.GetForUser(ctx, first, 1001)
			if err != nil || retained.ID != first {
				t.Fatalf("retained original ID: %+v %v", retained, err)
			}
			if len(retained.Output) == 0 || strings.Contains(string(retained.Output), receipt.String()) {
				t.Fatal("missing safe report or leaked internal receipt UUID")
			}
			if outcome == "unresolved" || outcome == "exhausted" {
				if retained.State != "discarded" || retained.LastError == "" {
					t.Fatal("terminal safe classification missing")
				}
				if _, err = f.runtime.Retry(ctx, first); !errors.Is(err, ErrReceiptRetryUnavailable) {
					t.Fatalf("retained exhausted retry accepted: %v", err)
				}
			}
			items, next, err := f.runtime.List(ctx, ListInput{UserID: 1001, Kind: ReceiptCleanupKind, Queue: CleanupQueue, Limit: 1})
			if err != nil || len(items) != 1 {
				t.Fatalf("retained filtered page: %+v %v", items, err)
			}
			seen := map[int64]bool{items[0].ID: true}
			for next != "" {
				items, next, err = f.runtime.List(ctx, ListInput{UserID: 1001, Kind: ReceiptCleanupKind, Queue: CleanupQueue, Limit: 1, Cursor: next})
				if err != nil {
					t.Fatal(err)
				}
				for _, item := range items {
					if seen[item.ID] {
						t.Fatal("live/retained duplicate across int64 cursor")
					}
					seen[item.ID] = true
				}
			}
			if !seen[first] {
				t.Fatal("first report omitted from merged pages")
			}
			stats, err := f.runtime.StatisticsForUser(ctx, 1001)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Pending+stats.Retryable+stats.Discarded+stats.Completed == 0 {
				t.Fatal("retention removed owner statistics")
			}
			if _, err = f.runtime.GetForUser(ctx, first, 1002); !errors.Is(err, river.ErrNotFound) {
				t.Fatalf("retained other-owner detail leak: %v", err)
			}
			f.preserved(t)
		})
	}
}
