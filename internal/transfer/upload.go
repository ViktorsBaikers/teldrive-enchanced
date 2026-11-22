package transfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/contentcrypto"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

var (
	ErrInvalidUpload       = errors.New("invalid upload request")
	ErrBodyTooShort        = errors.New("upload body is shorter than Content-Length")
	ErrBodyTooLong         = errors.New("upload body is longer than Content-Length")
	ErrChecksumMismatch    = errors.New("plaintext part checksum mismatch")
	ErrEncryptionKey       = errors.New("encryption key is unavailable")
	ErrStoredSizeMismatch  = errors.New("stored Telegram part size mismatch")
	ErrUploadNotConfigured = errors.New("upload pipeline is not configured")
)

const partCleanupTimeout = 5 * time.Second
const defaultLeaseRenewInterval = 20 * time.Second

// UploadCatalog is the durable upload-session boundary required by Pipeline.
type UploadCatalog interface {
	Get(context.Context, int64, uuid.UUID) (*sqlcgen.UploadSession, error)
	GetPart(context.Context, int64, uuid.UUID, int32) (*sqlcgen.UploadPart, error)
	ClaimPart(context.Context, uploads.ClaimPartInput) (*uploads.ClaimPartResult, error)
	RenewPart(context.Context, uploads.RenewPartInput) error
	StorePart(context.Context, uploads.StorePartInput) (*sqlcgen.UploadPart, error)
	FailPart(context.Context, uploads.FailPartInput) (*sqlcgen.UploadPart, error)
}

// ChannelResolver chooses an owned channel and performs rollover when needed.
type ChannelResolver interface {
	Resolve(context.Context, int64, int64) (int64, error)
}

// KeyProvider resolves versioned server-managed content-encryption keys.
type KeyProvider interface {
	Key(context.Context, int64, int32) (string, error)
}

// UploadReceipts is the shared durable policy, not deletion authority from an RPC result.
type UploadReceipts interface {
	Reserve(context.Context, lifecycle.ReserveInput) (*sqlcgen.RemoteWriteReceipt, error)
	Observe(context.Context, lifecycle.ObserveInput) (*sqlcgen.RemoteWriteReceipt, error)
	RecoverWithinBudget(context.Context, int64, []uuid.UUID) error
}

type Config struct {
	UploadThreads      int
	RandomizePartNames bool
	DisableHashing     bool
	Random             io.Reader
	LeaseRenewInterval time.Duration
	Receipts           UploadReceipts
}

type Pipeline struct {
	catalog  UploadCatalog
	channels ChannelResolver
	storage  telegramstore.Storage
	keys     KeyProvider
	config   Config
}

func NewPipeline(catalog UploadCatalog, channels ChannelResolver, storage telegramstore.Storage, keys KeyProvider, cfg Config) *Pipeline {
	if cfg.Random == nil {
		cfg.Random = rand.Reader
	}
	if cfg.LeaseRenewInterval <= 0 {
		cfg.LeaseRenewInterval = defaultLeaseRenewInterval
	}
	return &Pipeline{catalog: catalog, channels: channels, storage: storage, keys: keys, config: cfg}
}

// ValidateUploadKey checks availability without exposing key material or mutating an upload.
func (p *Pipeline) ValidateUploadKey(ctx context.Context, userID int64, keyVersion int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.keys == nil {
		return ErrEncryptionKey
	}
	key, err := p.keys.Key(ctx, userID, keyVersion)
	if err != nil {
		return errors.Join(ErrEncryptionKey, err)
	}
	if key == "" {
		return ErrEncryptionKey
	}
	return nil
}

type UploadPartRequest struct {
	UserID              int64
	UploadID            uuid.UUID
	PartNo              int32
	RequestedChannelID  int64
	PlainSize           int64
	Checksum            *string
	Body                io.Reader
	Guarded             bool
	RecoveryIncarnation uuid.UUID
	// BeforeParts runs only after the admitted ancestry and slot are proven and
	// before any body byte is read. An error vetoes the transfer.
	BeforeParts func(verifiedParentPath []string) error
}

type UploadPartResult struct {
	Part     *sqlcgen.UploadPart
	Existing bool
}

func (p *Pipeline) UploadPart(ctx context.Context, request UploadPartRequest) (*UploadPartResult, error) {
	if p.catalog == nil || p.channels == nil || p.storage == nil {
		return nil, ErrUploadNotConfigured
	}
	if request.UserID <= 0 || request.UploadID == uuid.Nil || request.PartNo <= 0 || request.PlainSize <= 0 || request.Body == nil {
		return nil, ErrInvalidUpload
	}
	checksum, err := normalizeOptionalChecksum(request.Checksum)
	if err != nil {
		return nil, err
	}
	request.Checksum = checksum

	session, err := p.catalog.Get(ctx, request.UserID, request.UploadID)
	if err != nil {
		return nil, err
	}
	if err := p.verifyGuardedPart(ctx, request, session); err != nil {
		return nil, err
	}
	if existing, err := p.catalog.GetPart(ctx, request.UserID, request.UploadID, request.PartNo); err == nil {
		if existing.State == sqlcgen.UploadPartStateStored {
			return p.replayPart(ctx, request, existing)
		}
	} else if !errors.Is(err, uploads.ErrNotFound) {
		return nil, err
	}
	slog.DebugContext(ctx, "upload part started", "upload_id", request.UploadID, "part_no", request.PartNo, "plain_size", request.PlainSize, "requested_channel_id", request.RequestedChannelID)

	channelID, err := p.channels.Resolve(ctx, request.UserID, request.RequestedChannelID)
	if err != nil {
		return nil, err
	}
	claim, err := p.claimPart(ctx, uploads.ClaimPartInput{
		UserID:              request.UserID,
		UploadID:            request.UploadID,
		PartNo:              request.PartNo,
		ChannelID:           channelID,
		PlainSize:           request.PlainSize,
		Checksum:            request.Checksum,
		Guarded:             request.Guarded,
		RecoveryIncarnation: request.RecoveryIncarnation,
	})
	if err != nil {
		return nil, err
	}
	if claim.Existing {
		return p.replayPart(ctx, request, claim.Part)
	}
	uploadCtx, cancelUpload := context.WithCancelCause(ctx)
	defer cancelUpload(nil)
	renewErrors := p.renewPartLease(uploadCtx, cancelUpload, request, claim.LeaseToken)

	exact := newExactReader(uploadCtx, request.Body, request.PlainSize)
	shouldHash := !p.config.DisableHashing || request.Checksum != nil || session.ExpectedHashAlgorithm.Valid
	var hasher *treehash.BlockHasher
	plainReader := io.Reader(exact)
	if shouldHash {
		hasher = treehash.NewBlockHasher()
		plainReader = io.TeeReader(exact, hasher)
	}
	storedReader := plainReader
	storedSize := request.PlainSize
	var salt *string

	if session.Encryption {
		if p.keys == nil || !session.EncryptionKeyVersion.Valid {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "encryption_key_missing", ErrEncryptionKey)
		}
		key, keyErr := p.keys.Key(ctx, request.UserID, session.EncryptionKeyVersion.Int32)
		if keyErr != nil || key == "" {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "encryption_key_missing", errors.Join(ErrEncryptionKey, keyErr))
		}
		generatedSalt, saltErr := generateSalt(p.config.Random)
		if saltErr != nil {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "salt_generation_failed", saltErr)
		}
		cipher, cipherErr := contentcrypto.NewCipher(key, generatedSalt)
		if cipherErr != nil {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "cipher_initialization_failed", cipherErr)
		}
		encrypted, cipherErr := cipher.EncryptData(plainReader)
		if cipherErr != nil {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "cipher_initialization_failed", cipherErr)
		}
		storedReader = encrypted
		storedSize = contentcrypto.EncryptedSize(request.PlainSize)
		salt = &generatedSalt
	}

	if p.config.Receipts == nil {
		return nil, p.failPart(ctx, request, claim.LeaseToken, "lifecycle_unavailable", ErrUploadNotConfigured)
	}
	receiptID := uuid.New()
	if _, err := p.config.Receipts.Reserve(uploadCtx, lifecycle.ReserveInput{
		ID: receiptID, UserID: request.UserID, ChannelID: channelID, OriginKind: "upload",
		UploadID: &request.UploadID, PartNo: request.PartNo, LeaseToken: claim.LeaseToken, LeaseExpiresAt: claim.Part.LeaseExpiresAt.Time,
	}); err != nil {
		return nil, p.failPart(ctx, request, claim.LeaseToken, "receipt_reservation_failed", err)
	}
	stored, err := p.storage.Upload(uploadCtx, telegramstore.UploadRequest{
		UserID:    request.UserID,
		ChannelID: channelID,
		Name:      p.partName(session, request.PartNo),
		Reader:    storedReader,
		Size:      storedSize,
		Threads:   p.config.UploadThreads,
	})
	slog.DebugContext(ctx, "uploading part to telegram", "upload_id", request.UploadID, "part_no", request.PartNo, "channel_id", channelID, "stored_size", storedSize, "threads", p.config.UploadThreads)
	// One detached budget covers observation, finalization and all compensation.
	budget, cancelBudget := partCleanupContext(ctx)
	defer cancelBudget()
	if stored.ChannelID == channelID && stored.MessageID > 0 {
		if _, observeErr := p.config.Receipts.Observe(budget, lifecycle.ObserveInput{
			ReceiptID: receiptID, UserID: request.UserID, ChannelID: channelID, MessageID: stored.MessageID,
			LeaseToken: claim.LeaseToken, UploadID: &request.UploadID, PartNo: request.PartNo,
		}); observeErr != nil {
			return nil, p.failPart(budget, request, claim.LeaseToken, "receipt_observation_failed", errors.Join(err, observeErr))
		}
	} else if err == nil {
		err = ErrStoredSizeMismatch
	}
	if renewErr := pendingRenewError(renewErrors); renewErr != nil {
		err = errors.Join(err, renewErr)
	}
	if err != nil {
		return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "telegram_upload_failed", err)
	}
	if stored.Size != storedSize {
		return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "telegram_size_mismatch", ErrStoredSizeMismatch)
	}
	if err := exact.Verify(); err != nil {
		return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "body_size_mismatch", err)
	}

	var (
		actualChecksum *string
		blockHashes    []byte
	)
	if shouldHash {
		blockHashes = hasher.Sum()
		if len(blockHashes) == 0 || len(blockHashes)%treehash.DigestSize != 0 {
			return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "hash_generation_failed", ErrInvalidUpload)
		}
		value := treehash.SumToHex(treehash.ComputeTreeHash(blockHashes))
		actualChecksum = &value
		if request.Checksum != nil && !strings.EqualFold(*request.Checksum, value) {
			return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "checksum_mismatch", ErrChecksumMismatch)
		}
	}

	part, err := p.catalog.StorePart(budget, uploads.StorePartInput{
		UploadID:            request.UploadID,
		ReceiptID:           receiptID,
		PartNo:              request.PartNo,
		LeaseToken:          claim.LeaseToken,
		MessageID:           stored.MessageID,
		StoredSize:          stored.Size,
		Checksum:            valueOrEmpty(actualChecksum),
		Salt:                salt,
		BlockHashes:         blockHashes,
		Guarded:             request.Guarded,
		RecoveryIncarnation: request.RecoveryIncarnation,
	})
	if err != nil {
		// A returned error is not rollback proof. Independently read the durable
		// slot before releasing it; unavailable/missing proof preserves uncertainty.
		current, readErr := p.catalog.GetPart(budget, request.UserID, request.UploadID, request.PartNo)
		if errors.Is(readErr, uploads.ErrParentUnavailable) {
			return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "part_storage_failed", errors.Join(err, readErr))
		}
		if readErr != nil {
			return nil, errors.Join(err, readErr)
		}
		if current.State == sqlcgen.UploadPartStateStored {
			return nil, err
		}
		return nil, p.failReceipt(budget, request, claim.LeaseToken, receiptID, "part_storage_failed", err)
	}
	slog.DebugContext(ctx, "upload part completed", "upload_id", request.UploadID, "part_no", request.PartNo, "channel_id", stored.ChannelID, "message_id", stored.MessageID, "stored_size", stored.Size)
	return &UploadPartResult{Part: part}, nil
}

func (p *Pipeline) replayPart(ctx context.Context, request UploadPartRequest, part *sqlcgen.UploadPart) (*UploadPartResult, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if part == nil || part.State != sqlcgen.UploadPartStateStored || part.PlainSize != request.PlainSize || !part.Checksum.Valid {
		return nil, uploads.ErrPartConflict
	}
	checksum, err := normalizeOptionalChecksum(&part.Checksum.String)
	if err != nil || len(part.BlockHashes) == 0 || !checksumMatches(*checksum, true, request.Checksum) {
		return nil, uploads.ErrPartConflict
	}
	before, err := p.catalog.Get(ctx, request.UserID, request.UploadID)
	if err != nil {
		return nil, err
	}
	exact := newExactReader(ctx, request.Body, request.PlainSize)
	hasher := treehash.NewBlockHasher()
	if _, err := io.Copy(hasher, exact); err != nil {
		return nil, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := exact.Verify(); err != nil {
		return nil, err
	}
	blockHashes := hasher.Sum()
	if !bytes.Equal(blockHashes, part.BlockHashes) || treehash.SumToHex(treehash.ComputeTreeHash(blockHashes)) != *checksum {
		return nil, uploads.ErrPartConflict
	}
	// A replay never owns a lease; recheck terminal state without mutating the winner.
	session, err := p.catalog.Get(ctx, request.UserID, request.UploadID)
	if err != nil {
		return nil, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	switch session.State {
	case sqlcgen.UploadStateOpen:
		if !session.ExpiresAt.Time.After(time.Now()) {
			return nil, uploads.ErrExpired
		}
	case sqlcgen.UploadStateCompleted:
	default:
		return nil, uploads.ErrInvalidState
	}
	current, err := p.catalog.GetPart(ctx, request.UserID, request.UploadID, request.PartNo)
	if err != nil {
		return nil, err
	}
	if before.State == sqlcgen.UploadStateCompleted && session.FileID != before.FileID {
		return nil, uploads.ErrPartConflict
	}
	if !samePartProof(part, current) {
		return nil, uploads.ErrPartConflict
	}
	return &UploadPartResult{Part: part, Existing: true}, nil
}

type guardedPartCatalog interface {
	VerifyGuardedPart(context.Context, int64, uuid.UUID, uuid.UUID) (*uploads.GuardedPartOrigin, error)
}

// verifyGuardedPart enforces route mode and the admitted destination before any
// body byte is consumed, for both fresh claims and stored-part replay.
func (p *Pipeline) verifyGuardedPart(ctx context.Context, request UploadPartRequest, session *sqlcgen.UploadSession) error {
	if !session.Guarded {
		if request.Guarded {
			return uploads.ErrGuardedRouteRequired
		}
		return nil
	}
	if !request.Guarded || request.RecoveryIncarnation == uuid.Nil {
		return uploads.ErrGuardedRouteRequired
	}
	verifier, ok := p.catalog.(guardedPartCatalog)
	if !ok {
		return uploads.ErrGuardedRouteRequired
	}
	origin, err := verifier.VerifyGuardedPart(ctx, request.UserID, request.UploadID, request.RecoveryIncarnation)
	if err != nil {
		return err
	}
	if request.BeforeParts != nil {
		return request.BeforeParts(origin.ParentSegments)
	}
	return nil
}

// claimPart waits for an overlapping request instead of rejecting an otherwise
// valid retry. The database lease remains authoritative across server instances:
// never steal it, read the body, or upload bytes until the part is owned or stored.
func (p *Pipeline) claimPart(ctx context.Context, in uploads.ClaimPartInput) (*uploads.ClaimPartResult, error) {
	delay := 250 * time.Millisecond
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		claim, err := p.catalog.ClaimPart(ctx, in)
		if !errors.Is(err, uploads.ErrPartBusy) {
			return claim, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
		// Completion may also have closed the session while we waited. Check
		// stored metadata before trying to claim against the session state.
		part, err := p.catalog.GetPart(ctx, in.UserID, in.UploadID, in.PartNo)
		if err != nil && !errors.Is(err, uploads.ErrNotFound) {
			return nil, err
		}
		if err == nil && part.State == sqlcgen.UploadPartStateStored {
			if part.PlainSize != in.PlainSize {
				return nil, uploads.ErrPartConflict
			}
			return &uploads.ClaimPartResult{Part: part, Existing: true}, nil
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func (p *Pipeline) failPart(ctx context.Context, request UploadPartRequest, leaseToken uuid.UUID, code string, cause error) error {
	cleanupCtx, cancel := partCleanupContext(ctx)
	defer cancel()
	_, failErr := p.catalog.FailPart(cleanupCtx, uploads.FailPartInput{
		UploadID:            request.UploadID,
		PartNo:              request.PartNo,
		LeaseToken:          leaseToken,
		ErrorCode:           code,
		Guarded:             request.Guarded,
		RecoveryIncarnation: request.RecoveryIncarnation,
	})
	if failErr != nil && !errors.Is(failErr, uploads.ErrLeaseLost) {
		return errors.Join(cause, failErr)
	}
	return cause
}

func (p *Pipeline) failReceipt(ctx context.Context, request UploadPartRequest, token, receiptID uuid.UUID, code string, cause error) error {
	_, failErr := p.catalog.FailPart(ctx, uploads.FailPartInput{UploadID: request.UploadID, PartNo: request.PartNo, LeaseToken: token, ReceiptID: receiptID, ErrorCode: code, Guarded: request.Guarded, RecoveryIncarnation: request.RecoveryIncarnation})
	if failErr != nil && !errors.Is(failErr, uploads.ErrLeaseLost) {
		return errors.Join(cause, failErr)
	}
	return errors.Join(cause, p.config.Receipts.RecoverWithinBudget(ctx, request.UserID, []uuid.UUID{receiptID}))
}

func samePartProof(left, right *sqlcgen.UploadPart) bool {
	if left == nil || right == nil || !bytes.Equal(left.BlockHashes, right.BlockHashes) {
		return false
	}
	return left.UploadID == right.UploadID && left.PartNo == right.PartNo && left.ChannelID == right.ChannelID && left.MessageID == right.MessageID &&
		left.PlainSize == right.PlainSize && left.StoredSize == right.StoredSize && left.Checksum == right.Checksum && left.Salt == right.Salt &&
		left.State == right.State && left.LeaseToken == right.LeaseToken && left.LeaseExpiresAt == right.LeaseExpiresAt && left.LastErrorCode == right.LastErrorCode &&
		left.CreatedAt == right.CreatedAt && left.UpdatedAt == right.UpdatedAt
}

func (p *Pipeline) renewPartLease(ctx context.Context, cancel context.CancelCauseFunc, request UploadPartRequest, leaseToken uuid.UUID) <-chan error {
	errorsCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(p.config.LeaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := partCleanupContext(ctx)
				err := p.catalog.RenewPart(renewCtx, uploads.RenewPartInput{
					UploadID: request.UploadID, PartNo: request.PartNo, LeaseToken: leaseToken,
					Guarded: request.Guarded, RecoveryIncarnation: request.RecoveryIncarnation,
				})
				stop()
				if err != nil {
					errorsCh <- err
					cancel(err)
					return
				}
			}
		}
	}()
	return errorsCh
}

func pendingRenewError(errorsCh <-chan error) error {
	select {
	case err := <-errorsCh:
		return err
	default:
		return nil
	}
}

type partCleanupBudgetKey struct{}

func partCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Value(partCleanupBudgetKey{}) != nil {
		return context.WithCancel(ctx)
	}
	budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), partCleanupTimeout)
	return context.WithValue(budget, partCleanupBudgetKey{}, true), cancel
}

// partName returns the Telegram document name for a part. Uploads whose known
// size fits in a single part keep the original file name; unknown-size uploads
// cannot know the final part count, so they always use the numbered suffix.
func (p *Pipeline) partName(session *sqlcgen.UploadSession, partNo int32) string {
	if !p.config.RandomizePartNames {
		if partNo == 1 && session.ExpectedSize > 0 && session.ExpectedSize <= session.PartSize {
			return session.Name
		}
		return fmt.Sprintf("%s.%03d", session.Name, partNo)
	}
	digest := sha256.Sum256([]byte(uuid.NewString()))
	return hex.EncodeToString(digest[:])
}
func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func generateSalt(random io.Reader) (string, error) {
	if random == nil {
		return "", errors.New("random source is required")
	}
	seed := make([]byte, 32)
	if _, err := io.ReadFull(random, seed); err != nil {
		return "", fmt.Errorf("read encryption salt entropy: %w", err)
	}
	digest := sha256.Sum256(seed)
	return base64.URLEncoding.EncodeToString(digest[:]), nil
}

func normalizeOptionalChecksum(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	normalized := strings.ToLower(strings.TrimSpace(*value))
	digest, err := hex.DecodeString(normalized)
	if err != nil || len(digest) != treehash.DigestSize {
		return nil, ErrInvalidUpload
	}
	return &normalized, nil
}

func checksumMatches(stored string, valid bool, expected *string) bool {
	if expected == nil {
		return true
	}
	return valid && strings.EqualFold(stored, *expected)
}

type exactReader struct {
	ctx       context.Context
	source    io.Reader
	remaining int64
}

func newExactReader(ctx context.Context, source io.Reader, size int64) *exactReader {
	return &exactReader{ctx: ctx, source: source, remaining: size}
}

func (r *exactReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.source.Read(p)
	r.remaining -= int64(n)
	if r.remaining > 0 && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return n, ErrBodyTooShort
	}
	if n == 0 && err == nil {
		return 0, io.ErrNoProgress
	}
	return n, err
}

func (r *exactReader) Verify() error {
	if r.remaining != 0 {
		return ErrBodyTooShort
	}
	var probe [1]byte
	n, err := r.source.Read(probe[:])
	if n > 0 {
		return ErrBodyTooLong
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return io.ErrNoProgress
}

// StaticKeyProvider is suitable for configuration-backed key rotation. New
// uploads reference a version while old files remain decryptable.
type StaticKeyProvider map[int32]string

func (p StaticKeyProvider) Key(_ context.Context, _ int64, version int32) (string, error) {
	key, ok := p[version]
	if !ok || key == "" {
		return "", ErrEncryptionKey
	}
	return key, nil
}
