//go:build integration

package jobs_test

import (
	"context"
	"testing"

	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
)

func TestRuntimeInsertStartAndStopAgainstRealPostgres(t *testing.T) {
	db := testpostgres.New(t)
	storage := &cleanupStorage{
		Storage: fixture.NewStorage(t, []fixture.ChannelScope{
			{ChannelID: 9001, Actors: []int64{1001}},
		}),
	}
	policy := lifecycle.NewService(db.Pool, storage)
	runtime, err := jobs.NewRuntime(db.Pool, storage, policy)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if err := policy.BindReceiptEnqueuer(runtime); err != nil {
		t.Fatalf("BindReceiptEnqueuer() error = %v", err)
	}
	ctx := context.Background()
	if err := runtime.InsertCleanup(ctx); err != nil {
		t.Fatalf("InsertCleanup() error = %v", err)
	}
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE kind = $1", jobs.CleanupSweepKind).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("cleanup jobs = %d, want 1", count)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	if err := runtime.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := runtime.Stop(ctx); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
}
