package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestExactReader(t *testing.T) {
	t.Parallel()

	t.Run("exact", func(t *testing.T) {
		r := newExactReader(context.Background(), bytes.NewBufferString("abcd"), 4)
		got, err := io.ReadAll(r)
		if err != nil || string(got) != "abcd" {
			t.Fatalf("ReadAll() = %q, %v", got, err)
		}
		if err := r.Verify(); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("short", func(t *testing.T) {
		r := newExactReader(context.Background(), bytes.NewBufferString("abc"), 4)
		_, err := io.ReadAll(r)
		if !errors.Is(err, ErrBodyTooShort) {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if err := r.Verify(); !errors.Is(err, ErrBodyTooShort) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("truncated transport", func(t *testing.T) {
		source := io.MultiReader(strings.NewReader("abc"), failingReader{io.ErrUnexpectedEOF})
		r := newExactReader(context.Background(), source, 4)
		got, err := io.ReadAll(r)
		if string(got) != "abc" || !errors.Is(err, ErrBodyTooShort) {
			t.Fatalf("ReadAll() = %q, %v, want preserved bytes and short-body error", got, err)
		}
		if err := r.Verify(); !errors.Is(err, ErrBodyTooShort) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("long", func(t *testing.T) {
		r := newExactReader(context.Background(), bytes.NewBufferString("abcde"), 4)
		got, err := io.ReadAll(r)
		if err != nil || string(got) != "abcd" {
			t.Fatalf("ReadAll() = %q, %v", got, err)
		}
		if err := r.Verify(); !errors.Is(err, ErrBodyTooLong) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := newExactReader(ctx, bytes.NewBufferString("abcd"), 4)
		var b [1]byte
		if _, err := r.Read(b[:]); !errors.Is(err, context.Canceled) {
			t.Fatalf("Read() error = %v", err)
		}
	})

	t.Run("unproven EOF", func(t *testing.T) {
		source := io.MultiReader(strings.NewReader("abcd"), noProgressReader{})
		r := newExactReader(context.Background(), source, 4)
		if _, err := io.Copy(io.Discard, r); err != nil {
			t.Fatal(err)
		}
		if err := r.Verify(); !errors.Is(err, io.ErrNoProgress) {
			t.Fatalf("Verify() = %v, want unproven EOF rejection", err)
		}
	})

	t.Run("error before EOF", func(t *testing.T) {
		want := errors.New("body failed at EOF")
		r := newExactReader(context.Background(), io.MultiReader(strings.NewReader("abcd"), failingReader{want}), 4)
		if _, err := io.Copy(io.Discard, r); err != nil {
			t.Fatal(err)
		}
		if err := r.Verify(); !errors.Is(err, want) {
			t.Fatalf("Verify() = %v, want source error", err)
		}
	})
}

func TestGenerateSaltDeterministic(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{1}, 32)
	first, err := generateSalt(bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	second, err := generateSalt(bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first == "" {
		t.Fatalf("salt values = %q and %q", first, second)
	}
	if _, err := generateSalt(nil); err == nil {
		t.Fatal("expected nil random source error")
	}
}

func TestPartName(t *testing.T) {
	t.Parallel()
	pipeline := &Pipeline{}
	multi := &sqlcgen.UploadSession{Name: "movie.mkv", ExpectedSize: 10, PartSize: 4}
	single := &sqlcgen.UploadSession{Name: "movie.mkv", ExpectedSize: 4, PartSize: 4}
	unknown := &sqlcgen.UploadSession{Name: "movie.mkv", ExpectedSize: -1, PartSize: 4}
	cases := []struct {
		name    string
		session *sqlcgen.UploadSession
		partNo  int32
		want    string
	}{
		{name: "single part keeps name", session: single, partNo: 1, want: "movie.mkv"},
		{name: "unknown size keeps suffix", session: unknown, partNo: 1, want: "movie.mkv.001"},
		{name: "multi part first", session: multi, partNo: 1, want: "movie.mkv.001"},
		{name: "multi part second", session: multi, partNo: 2, want: "movie.mkv.002"},
		{name: "three digits", session: multi, partNo: 999, want: "movie.mkv.999"},
		{name: "beyond three digits", session: multi, partNo: 1000, want: "movie.mkv.1000"},
	}
	for _, tc := range cases {
		if got := pipeline.partName(tc.session, tc.partNo); got != tc.want {
			t.Fatalf("%s: partName(%d) = %q, want %q", tc.name, tc.partNo, got, tc.want)
		}
	}
}

func TestStaticKeyProvider(t *testing.T) {
	t.Parallel()
	provider := StaticKeyProvider{1: "secret"}
	if key, err := provider.Key(context.Background(), 1, 1); err != nil || key != "secret" {
		t.Fatalf("Key() = %q, %v", key, err)
	}
	if _, err := provider.Key(context.Background(), 1, 2); !errors.Is(err, ErrEncryptionKey) {
		t.Fatalf("missing key error = %v", err)
	}
}

func TestChecksumMatches(t *testing.T) {
	t.Parallel()
	value := "abc"
	if !checksumMatches("", false, nil) {
		t.Fatal("nil assertion should match")
	}
	if !checksumMatches(value, true, &value) {
		t.Fatal("equal checksum should match")
	}
	if checksumMatches(value, false, &value) {
		t.Fatal("invalid stored checksum should not match")
	}
	other := "def"
	if checksumMatches(value, true, &other) {
		t.Fatal("different checksum should not match")
	}
}

func TestDeleteUploadedCompensation(t *testing.T) {
	t.Parallel()
	catalog := &cleanupCatalog{}
	ledger := &unitUploadReceipts{recoverErr: errors.New("delete failed")}
	pipeline := &Pipeline{catalog: catalog, config: Config{Receipts: ledger}}
	budget, cancel := partCleanupContext(context.Background())
	defer cancel()
	primary := errors.New("upload failed")
	err := pipeline.failReceipt(budget, UploadPartRequest{UserID: 1, UploadID: uuid.New(), PartNo: 1}, uuid.New(), uuid.New(), "telegram_upload_failed", primary)
	if !errors.Is(err, primary) || !errors.Is(err, ledger.recoverErr) || ledger.recoveries != 1 {
		t.Fatalf("receipt compensation lost outcomes: %v", err)
	}
}

func TestFailPartDetachesCleanupFromCanceledRequest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	catalog := &cleanupCatalog{}
	pipeline := &Pipeline{catalog: catalog}
	cause := context.Canceled

	err := pipeline.failPart(ctx, UploadPartRequest{UploadID: uuid.New(), PartNo: 1}, uuid.New(), "request_canceled", cause)

	if !errors.Is(err, cause) {
		t.Fatalf("failPart() error = %v, want %v", err, cause)
	}
	if catalog.cleanupErr != nil {
		t.Fatalf("FailPart() context error = %v", catalog.cleanupErr)
	}
	if !catalog.hadDeadline {
		t.Fatal("FailPart() cleanup context has no deadline")
	}
}

func TestUploadPartRenewsLeaseDuringTransfer(t *testing.T) {
	t.Parallel()
	catalog := newLeaseCatalog()
	storage := &leaseStorage{waitForRenewal: catalog.renewed}
	pipeline := NewPipeline(catalog, fixedChannelResolver(9), storage, nil, Config{LeaseRenewInterval: time.Millisecond, Receipts: &unitUploadReceipts{}})

	result, err := pipeline.UploadPart(context.Background(), UploadPartRequest{
		UserID: 1, UploadID: catalog.uploadID, PartNo: 1, PlainSize: 4, Body: bytes.NewBufferString("data"),
	})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if result.Part.State != sqlcgen.UploadPartStateStored || catalog.renewCount() == 0 {
		t.Fatalf("result = %#v, renewals = %d", result, catalog.renewCount())
	}
}

func TestUploadPartFinalizesPublishedMessageAfterRequestCancellation(t *testing.T) {
	t.Parallel()
	catalog := newLeaseCatalog()
	ctx, cancel := context.WithCancel(context.Background())
	storage := &leaseStorage{cancelAfterUpload: cancel}
	pipeline := NewPipeline(catalog, fixedChannelResolver(9), storage, nil, Config{Receipts: &unitUploadReceipts{}})

	result, err := pipeline.UploadPart(ctx, UploadPartRequest{
		UserID: 1, UploadID: catalog.uploadID, PartNo: 1, PlainSize: 4, Body: bytes.NewBufferString("data"),
	})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if result.Part.State != sqlcgen.UploadPartStateStored || catalog.storeContextErr != nil || !catalog.storeHadDeadline {
		t.Fatalf("result = %#v, store context error = %v, bounded=%t", result, catalog.storeContextErr, catalog.storeHadDeadline)
	}
}

type fixedChannelResolver int64

func (r fixedChannelResolver) Resolve(context.Context, int64, int64) (int64, error) {
	return int64(r), nil
}

type leaseCatalog struct {
	mu               sync.Mutex
	uploadID         uuid.UUID
	leaseToken       uuid.UUID
	renewed          chan struct{}
	renewOnce        sync.Once
	renewals         int
	storeContextErr  error
	storeHadDeadline bool
}

func newLeaseCatalog() *leaseCatalog {
	return &leaseCatalog{uploadID: uuid.New(), leaseToken: uuid.New(), renewed: make(chan struct{})}
}

func (c *leaseCatalog) Get(context.Context, int64, uuid.UUID) (*sqlcgen.UploadSession, error) {
	return &sqlcgen.UploadSession{State: sqlcgen.UploadStateOpen, ExpiresAt: dbtypes.Time(time.Now().Add(time.Hour))}, nil
}
func (c *leaseCatalog) GetPart(context.Context, int64, uuid.UUID, int32) (*sqlcgen.UploadPart, error) {
	return nil, uploads.ErrNotFound
}
func (c *leaseCatalog) ClaimPart(context.Context, uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	return &uploads.ClaimPartResult{Part: &sqlcgen.UploadPart{LeaseExpiresAt: dbtypes.Time(time.Now().Add(time.Minute))}, LeaseToken: c.leaseToken}, nil
}
func (c *leaseCatalog) RenewPart(context.Context, uploads.RenewPartInput) error {
	c.mu.Lock()
	c.renewals++
	c.mu.Unlock()
	c.renewOnce.Do(func() { close(c.renewed) })
	return nil
}
func (c *leaseCatalog) StorePart(ctx context.Context, _ uploads.StorePartInput) (*sqlcgen.UploadPart, error) {
	c.storeContextErr = ctx.Err()
	_, c.storeHadDeadline = ctx.Deadline()
	return &sqlcgen.UploadPart{State: sqlcgen.UploadPartStateStored}, nil
}
func (*leaseCatalog) FailPart(context.Context, uploads.FailPartInput) (*sqlcgen.UploadPart, error) {
	return &sqlcgen.UploadPart{}, nil
}
func (c *leaseCatalog) renewCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.renewals
}

type leaseStorage struct {
	waitForRenewal    <-chan struct{}
	cancelAfterUpload context.CancelFunc
	uploadError       error
}

func (s *leaseStorage) Upload(ctx context.Context, request telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	if s.waitForRenewal != nil {
		select {
		case <-s.waitForRenewal:
		case <-ctx.Done():
			return telegramstore.StoredPart{}, context.Cause(ctx)
		}
	}
	data, err := io.ReadAll(request.Reader)
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	if s.cancelAfterUpload != nil {
		s.cancelAfterUpload()
	}
	return telegramstore.StoredPart{ChannelID: request.ChannelID, MessageID: 7, Size: int64(len(data))}, s.uploadError
}
func (*leaseStorage) OpenRange(context.Context, telegramstore.RangeRequest) (io.ReadCloser, error) {
	return nil, errors.New("not used")
}
func (*leaseStorage) DeleteMessages(context.Context, int64, int64, []int64) error { return nil }
func (*leaseStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("not used")
}
func (*leaseStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, errors.New("not used")
}
func (*leaseStorage) DeleteChannel(context.Context, int64, int64) error { return nil }

type cleanupCatalog struct {
	cleanupErr  error
	hadDeadline bool
}

func (*cleanupCatalog) Get(context.Context, int64, uuid.UUID) (*sqlcgen.UploadSession, error) {
	return nil, errors.New("not used")
}
func (*cleanupCatalog) GetPart(context.Context, int64, uuid.UUID, int32) (*sqlcgen.UploadPart, error) {
	return nil, errors.New("not used")
}
func (*cleanupCatalog) ClaimPart(context.Context, uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	return nil, errors.New("not used")
}
func (*cleanupCatalog) RenewPart(context.Context, uploads.RenewPartInput) error {
	return errors.New("not used")
}
func (*cleanupCatalog) StorePart(context.Context, uploads.StorePartInput) (*sqlcgen.UploadPart, error) {
	return nil, errors.New("not used")
}
func (c *cleanupCatalog) FailPart(ctx context.Context, _ uploads.FailPartInput) (*sqlcgen.UploadPart, error) {
	c.cleanupErr = ctx.Err()
	_, c.hadDeadline = ctx.Deadline()
	return &sqlcgen.UploadPart{}, nil
}

type deleteStorage struct {
	deleted []int64
	err     error
}

func (*deleteStorage) Upload(context.Context, telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("not used")
}
func (*deleteStorage) OpenRange(context.Context, telegramstore.RangeRequest) (io.ReadCloser, error) {
	return nil, errors.New("not used")
}
func (s *deleteStorage) DeleteMessages(_ context.Context, _ int64, _ int64, ids []int64) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, ids...)
	return nil
}
func (*deleteStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("not implemented")
}
func (*deleteStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, errors.New("not used")
}
func (*deleteStorage) DeleteChannel(context.Context, int64, int64) error { return nil }

func TestNormalizeOptionalChecksum(t *testing.T) {
	t.Parallel()
	if value, err := normalizeOptionalChecksum(nil); err != nil || value != nil {
		t.Fatalf("nil checksum = %#v, %v", value, err)
	}
	valid := strings.Repeat("AB", treehash.DigestSize)
	normalized, err := normalizeOptionalChecksum(&valid)
	if err != nil || normalized == nil || *normalized != strings.ToLower(valid) {
		t.Fatalf("valid checksum = %#v, %v", normalized, err)
	}
	invalid := "xyz"
	if _, err := normalizeOptionalChecksum(&invalid); !errors.Is(err, ErrInvalidUpload) {
		t.Fatalf("invalid checksum error = %v", err)
	}
}

func TestRandomizedPartName(t *testing.T) {
	t.Parallel()
	pipeline := &Pipeline{config: Config{RandomizePartNames: true}}
	session := &sqlcgen.UploadSession{Name: "movie.mkv", ExpectedSize: 4, PartSize: 4}
	first := pipeline.partName(session, 1)
	second := pipeline.partName(session, 1)
	if first == second || len(first) != 64 || len(second) != 64 {
		t.Fatalf("randomized names = %q, %q", first, second)
	}
}

type noProgressReader struct{}

func (noProgressReader) Read([]byte) (int, error) { return 0, nil }

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

type replayBodyReader struct {
	io.Reader
	bytes     int
	eof       bool
	afterEOF  func()
	afterRead func()
}

func (r *replayBodyReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	if r.afterRead != nil {
		r.afterRead()
	}
	r.eof = r.eof || errors.Is(err, io.EOF)
	if errors.Is(err, io.EOF) && r.afterEOF != nil {
		r.afterEOF()
	}
	return n, err
}

type replayCatalog struct {
	*leaseCatalog
	part    *sqlcgen.UploadPart
	session *sqlcgen.UploadSession
	route   string
	claims  int
	stores  int
	fails   int
}

func (c *replayCatalog) Get(context.Context, int64, uuid.UUID) (*sqlcgen.UploadSession, error) {
	return c.session, nil
}

func (c *replayCatalog) GetPart(context.Context, int64, uuid.UUID, int32) (*sqlcgen.UploadPart, error) {
	if c.route == "initial" || c.claims > 0 {
		return c.part, nil
	}
	return nil, uploads.ErrNotFound
}

func (c *replayCatalog) ClaimPart(context.Context, uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	c.claims++
	if c.route == "wait" {
		return nil, uploads.ErrPartBusy
	}
	return &uploads.ClaimPartResult{Part: c.part, Existing: true}, nil
}

func (c *replayCatalog) StorePart(context.Context, uploads.StorePartInput) (*sqlcgen.UploadPart, error) {
	c.stores++
	return nil, errors.New("replay must not store")
}

func (c *replayCatalog) FailPart(context.Context, uploads.FailPartInput) (*sqlcgen.UploadPart, error) {
	c.fails++
	return nil, errors.New("replay must not fail the winner")
}

func TestB1ReplayBodyIdentityPaths(t *testing.T) {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write([]byte("data"))
	blockHashes := hasher.Sum()
	checksum := treehash.SumToHex(treehash.ComputeTreeHash(blockHashes))
	otherChecksum := strings.Repeat("0", 64)
	readErr := errors.New("source failed")
	cases := []struct {
		name     string
		body     string
		asserted *string
		mutate   func(*sqlcgen.UploadPart)
		readErr  error
		want     error
	}{
		{name: "equal", body: "data"},
		{name: "equal asserted", body: "data", asserted: &checksum},
		{name: "different", body: "else", want: uploads.ErrPartConflict},
		{name: "misleading assertion", body: "else", asserted: &checksum, want: uploads.ErrPartConflict},
		{name: "wrong assertion", body: "data", asserted: &otherChecksum, want: uploads.ErrPartConflict},
		{name: "short", body: "dat", want: ErrBodyTooShort},
		{name: "long", body: "data!", want: ErrBodyTooLong},
		{name: "reader error", body: "data", readErr: readErr, want: readErr},
		{name: "missing checksum", body: "data", mutate: func(p *sqlcgen.UploadPart) { p.Checksum.Valid = false }, want: uploads.ErrPartConflict},
		{name: "corrupt checksum", body: "data", mutate: func(p *sqlcgen.UploadPart) { p.Checksum.String = "bad" }, want: uploads.ErrPartConflict},
		{name: "missing blocks", body: "data", mutate: func(p *sqlcgen.UploadPart) { p.BlockHashes = nil }, want: uploads.ErrPartConflict},
		{name: "corrupt blocks", body: "data", mutate: func(p *sqlcgen.UploadPart) { p.BlockHashes = make([]byte, treehash.DigestSize) }, want: uploads.ErrPartConflict},
		{name: "size conflict", body: "data", mutate: func(p *sqlcgen.UploadPart) { p.PlainSize++ }, want: uploads.ErrPartConflict},
	}
	for _, route := range []string{"initial", "claim"} {
		for _, disableHashing := range []bool{false, true} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%s/hash-disabled=%t/%s", route, disableHashing, tc.name), func(t *testing.T) {
					lease := newLeaseCatalog()
					catalog := &replayCatalog{
						leaseCatalog: lease, route: route,
						session: &sqlcgen.UploadSession{State: sqlcgen.UploadStateOpen, ExpiresAt: dbtypes.Time(time.Now().Add(time.Hour))},
						part: &sqlcgen.UploadPart{State: sqlcgen.UploadPartStateStored, PlainSize: 4,
							Checksum: dbtypes.Text(checksum), BlockHashes: blockHashes, MessageID: dbtypes.Int8(7), LeaseToken: dbtypes.UUID(lease.leaseToken)},
					}
					if tc.mutate != nil {
						tc.mutate(catalog.part)
					}
					source := io.Reader(strings.NewReader(tc.body))
					if tc.readErr != nil {
						source = io.MultiReader(source, failingReader{tc.readErr})
					}
					body := &replayBodyReader{Reader: source}
					storage := &deleteStorage{}
					pipeline := NewPipeline(catalog, fixedChannelResolver(9), storage, nil, Config{DisableHashing: disableHashing})
					got, err := pipeline.UploadPart(context.Background(), UploadPartRequest{
						UserID: 1, UploadID: lease.uploadID, PartNo: 1, PlainSize: 4, Checksum: tc.asserted, Body: body,
					})
					if !errors.Is(err, tc.want) {
						t.Fatalf("UploadPart() = %+v, %v, want %v", got, err, tc.want)
					}
					if tc.want == nil && (got == nil || !got.Existing || got.Part != catalog.part || body.bytes != 4 || !body.eof) {
						t.Fatalf("replay did not prove the exact current body: result=%+v body=%+v", got, body)
					}
					if tc.want != nil && got != nil {
						t.Fatalf("failed replay acknowledged part: %+v", got)
					}
					if catalog.stores != 0 || catalog.fails != 0 || len(storage.deleted) != 0 || catalog.part.MessageID.Int64 != 7 || catalog.part.LeaseToken != dbtypes.UUID(lease.leaseToken) {
						t.Fatalf("replay modified winner: %+v, stores=%d fails=%d deletes=%v", catalog.part, catalog.stores, catalog.fails, storage.deleted)
					}
				})
			}
		}
	}
}

func TestB1ReplayBodyIdentitySessionState(t *testing.T) {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write([]byte("data"))
	blocks := hasher.Sum()
	for _, tc := range []struct {
		name       string
		state      sqlcgen.UploadState
		expired    bool
		change     bool
		cancel     bool
		cancelRead bool
		want       error
	}{
		{name: "completed", state: sqlcgen.UploadStateCompleted},
		{name: "aborted", state: sqlcgen.UploadStateAborted, want: uploads.ErrInvalidState},
		{name: "expired state", state: sqlcgen.UploadStateExpired, want: uploads.ErrInvalidState},
		{name: "open expired", state: sqlcgen.UploadStateOpen, expired: true, want: uploads.ErrExpired},
		{name: "aborted during body", state: sqlcgen.UploadStateAborted, change: true, want: uploads.ErrInvalidState},
		{name: "expired during body", state: sqlcgen.UploadStateOpen, expired: true, change: true, want: uploads.ErrExpired},
		{name: "cancelled at EOF", state: sqlcgen.UploadStateOpen, cancel: true, want: context.Canceled},
		{name: "cancelled during body", state: sqlcgen.UploadStateOpen, cancelRead: true, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lease := newLeaseCatalog()
			session := &sqlcgen.UploadSession{State: sqlcgen.UploadStateOpen, ExpiresAt: dbtypes.Time(time.Now().Add(time.Hour))}
			endState := func() {
				session.State = tc.state
				if tc.expired {
					session.ExpiresAt = dbtypes.Time(time.Now().Add(-time.Hour))
				}
			}
			if !tc.change {
				endState()
			}
			catalog := &replayCatalog{
				leaseCatalog: lease, route: "initial", session: session,
				part: &sqlcgen.UploadPart{State: sqlcgen.UploadPartStateStored, PlainSize: 4,
					Checksum: dbtypes.Text(treehash.SumToHex(treehash.ComputeTreeHash(blocks))), BlockHashes: blocks,
					MessageID: dbtypes.Int8(7), LeaseToken: dbtypes.UUID(lease.leaseToken)},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &replayBodyReader{Reader: strings.NewReader("data")}
			body.afterEOF = func() {
				if tc.change {
					endState()
				}
				if tc.cancel {
					cancel()
				}
			}
			if tc.cancelRead {
				body.afterRead = cancel
			}
			pipeline := NewPipeline(catalog, fixedChannelResolver(9), &deleteStorage{}, nil, Config{})
			got, err := pipeline.UploadPart(ctx, UploadPartRequest{
				UserID: 1, UploadID: lease.uploadID, PartNo: 1, PlainSize: 4, Body: body,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("UploadPart() = %+v, %v, want %v", got, err, tc.want)
			}
			if tc.want == nil && (got == nil || !got.Existing || body.bytes != 4 || !body.eof) {
				t.Fatalf("completed replay did not consume exact body: %+v body=%+v", got, body)
			}
			if tc.want != nil && got != nil {
				t.Fatalf("terminal/cancelled replay acknowledged part: %+v", got)
			}
			if catalog.stores != 0 || catalog.fails != 0 || catalog.part.MessageID.Int64 != 7 || catalog.part.LeaseToken != dbtypes.UUID(lease.leaseToken) {
				t.Fatalf("replay mutated winner: %+v", catalog)
			}
		})
	}
}

func TestB2CompensationDeadline(t *testing.T) {
	for _, elapsed := range []time.Duration{5*time.Second - time.Nanosecond, 5 * time.Second, 5*time.Second + time.Nanosecond} {
		t.Run(elapsed.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				catalog := newLeaseCatalog()
				parent, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				primary := errors.New("post-publication failure")
				ledger := &unitUploadReceipts{observeDelay: 4 * time.Second, recoverDelay: elapsed - 4*time.Second}
				pipeline := NewPipeline(catalog, fixedChannelResolver(9), &leaseStorage{cancelAfterUpload: cancel, uploadError: primary}, nil, Config{Receipts: ledger})
				_, err := pipeline.UploadPart(parent, UploadPartRequest{UserID: 1, UploadID: catalog.uploadID, PartNo: 1, PlainSize: 4, Body: bytes.NewBufferString("data")})
				if !errors.Is(err, primary) {
					t.Fatalf("primary failure lost: %v", err)
				}
				if ledger.observationErr != nil {
					t.Fatalf("parent canceled detached observation: %v", ledger.observationErr)
				}
				if ledger.recoveries != 1 || ledger.remaining != time.Second {
					t.Fatalf("remaining budget renewed: %v, calls=%d", ledger.remaining, ledger.recoveries)
				}
				if errors.Is(ledger.finalErr, context.DeadlineExceeded) != (elapsed >= 5*time.Second) {
					t.Fatalf("elapsed=%v, budget=%v", elapsed, ledger.finalErr)
				}
				if !ledger.recoveryDeadline.Equal(ledger.observationDeadline) {
					t.Fatal("recovery changed shared deadline")
				}
			})
		})
	}
}

type unitUploadReceipts struct {
	recoverErr                            error
	recoveries                            int
	observeDelay, recoverDelay            time.Duration
	observationErr, finalErr              error
	observationDeadline, recoveryDeadline time.Time
	remaining                             time.Duration
}

func (*unitUploadReceipts) Reserve(context.Context, lifecycle.ReserveInput) (*sqlcgen.RemoteWriteReceipt, error) {
	return &sqlcgen.RemoteWriteReceipt{}, nil
}
func (r *unitUploadReceipts) Observe(ctx context.Context, _ lifecycle.ObserveInput) (*sqlcgen.RemoteWriteReceipt, error) {
	r.observationErr = ctx.Err()
	r.observationDeadline, _ = ctx.Deadline()
	if r.observeDelay > 0 {
		time.Sleep(r.observeDelay)
	}
	return &sqlcgen.RemoteWriteReceipt{}, ctx.Err()
}
func (r *unitUploadReceipts) RecoverWithinBudget(ctx context.Context, _ int64, _ []uuid.UUID) error {
	r.recoveries++
	r.recoveryDeadline, _ = ctx.Deadline()
	r.remaining = time.Until(r.recoveryDeadline)
	if r.recoverDelay > 0 {
		time.Sleep(r.recoverDelay)
		synctest.Wait()
	}
	r.finalErr = ctx.Err()
	return errors.Join(r.recoverErr, r.finalErr)
}
