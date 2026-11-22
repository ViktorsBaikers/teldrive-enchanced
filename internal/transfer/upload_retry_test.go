package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type busyPartCatalog struct {
	*leaseCatalog
	claims int
	busy   int
	err    error
}

func (c *busyPartCatalog) ClaimPart(context.Context, uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	c.claims++
	if c.claims <= c.busy {
		return nil, uploads.ErrPartBusy
	}
	return &uploads.ClaimPartResult{Part: &sqlcgen.UploadPart{}, LeaseToken: c.leaseToken}, c.err
}

func TestClaimPartWaitsWithCappedBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := &busyPartCatalog{leaseCatalog: newLeaseCatalog(), busy: 6}
		pipeline := &Pipeline{catalog: catalog}
		started := time.Now()
		claim, err := pipeline.claimPart(context.Background(), uploads.ClaimPartInput{})
		if err != nil || claim.LeaseToken != catalog.leaseToken || claim.Existing {
			t.Fatalf("claimPart() = %+v, %v", claim, err)
		}
		// 250ms + 500ms + 1s + 2s + 2s + 2s.
		if elapsed := time.Since(started); elapsed != 7750*time.Millisecond || catalog.claims != 7 {
			t.Fatalf("elapsed = %v, claims = %d", elapsed, catalog.claims)
		}
	})
}

func TestClaimPartStopsOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := &busyPartCatalog{leaseCatalog: newLeaseCatalog(), busy: 10}
		pipeline := &Pipeline{catalog: catalog}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		started := time.Now()
		if _, err := pipeline.claimPart(ctx, uploads.ClaimPartInput{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("claimPart() = %v, want deadline exceeded", err)
		}
		if elapsed := time.Since(started); elapsed != 100*time.Millisecond || catalog.claims != 1 {
			t.Fatalf("elapsed = %v, claims = %d", elapsed, catalog.claims)
		}
	})
}

func TestClaimPartDoesNotRetryPermanentErrors(t *testing.T) {
	for _, want := range []error{uploads.ErrInvalidState, uploads.ErrPartConflict, uploads.ErrExpired} {
		t.Run(want.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				catalog := &busyPartCatalog{leaseCatalog: newLeaseCatalog(), err: want}
				pipeline := &Pipeline{catalog: catalog}
				started := time.Now()
				if _, err := pipeline.claimPart(context.Background(), uploads.ClaimPartInput{}); !errors.Is(err, want) {
					t.Fatalf("claimPart() = %v, want %v", err, want)
				}
				if time.Since(started) != 0 || catalog.claims != 1 {
					t.Fatalf("retried permanent error: calls = %d", catalog.claims)
				}
			})
		})
	}
}

func TestB1ReplayBodyIdentityAfterWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want error
	}{
		{name: "equal", body: "data"},
		{name: "different", body: "else", want: uploads.ErrPartConflict},
		{name: "short", body: "dat", want: ErrBodyTooShort},
		{name: "long", body: "data!", want: ErrBodyTooLong},
	} {
		for _, disableHashing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hash-disabled=%t", tc.name, disableHashing), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					hasher := treehash.NewBlockHasher()
					_, _ = hasher.Write([]byte("data"))
					blocks := hasher.Sum()
					lease := newLeaseCatalog()
					catalog := &replayCatalog{
						leaseCatalog: lease, route: "wait",
						session: &sqlcgen.UploadSession{State: sqlcgen.UploadStateCompleted},
						part: &sqlcgen.UploadPart{State: sqlcgen.UploadPartStateStored, PlainSize: 4,
							Checksum: dbtypes.Text(treehash.SumToHex(treehash.ComputeTreeHash(blocks))), BlockHashes: blocks,
							MessageID: dbtypes.Int8(7), LeaseToken: dbtypes.UUID(lease.leaseToken)},
					}
					body := &replayBodyReader{Reader: bytes.NewBufferString(tc.body)}
					pipeline := NewPipeline(catalog, fixedChannelResolver(9), &deleteStorage{}, nil, Config{DisableHashing: disableHashing})
					started := time.Now()
					got, err := pipeline.UploadPart(context.Background(), UploadPartRequest{
						UserID: 1, UploadID: lease.uploadID, PartNo: 1, PlainSize: 4, Body: body,
					})
					if !errors.Is(err, tc.want) {
						t.Fatalf("UploadPart() = %+v, %v, want %v", got, err, tc.want)
					}
					if tc.want == nil && (got == nil || !got.Existing || got.Part.MessageID.Int64 != 7 || body.bytes != 4 || !body.eof) {
						t.Fatalf("waiter did not prove exact body: result=%+v body=%+v", got, body)
					}
					if time.Since(started) != 250*time.Millisecond || catalog.claims != 1 || catalog.stores != 0 || catalog.fails != 0 || catalog.part.LeaseToken != dbtypes.UUID(lease.leaseToken) {
						t.Fatalf("waiter modified winner or retried: %+v", catalog)
					}
				})
			})
		}
	}
}

func TestB1ReplayBodyIdentityCancelledWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lease := newLeaseCatalog()
		catalog := &replayCatalog{
			leaseCatalog: lease, route: "wait",
			session: &sqlcgen.UploadSession{State: sqlcgen.UploadStateOpen, ExpiresAt: dbtypes.Time(time.Now().Add(time.Hour))},
			part:    &sqlcgen.UploadPart{State: sqlcgen.UploadPartStateUploading, PlainSize: 4, LeaseToken: dbtypes.UUID(lease.leaseToken)},
		}
		body := &replayBodyReader{Reader: bytes.NewBufferString("else")}
		pipeline := NewPipeline(catalog, fixedChannelResolver(9), &deleteStorage{}, nil, Config{})
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := pipeline.UploadPart(ctx, UploadPartRequest{
			UserID: 1, UploadID: lease.uploadID, PartNo: 1, PlainSize: 4, Body: body,
		})
		if !errors.Is(err, context.DeadlineExceeded) || body.bytes != 0 || body.eof {
			t.Fatalf("cancelled waiter = %v, body=%+v", err, body)
		}
		if catalog.fails != 0 || catalog.stores != 0 || catalog.part.State != sqlcgen.UploadPartStateUploading || catalog.part.LeaseToken != dbtypes.UUID(lease.leaseToken) {
			t.Fatalf("cancelled waiter disturbed winner: %+v", catalog)
		}
	})
}
