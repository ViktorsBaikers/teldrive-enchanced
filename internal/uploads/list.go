package uploads

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
)

type ListInput struct {
	UserID         int64
	State          *sqlcgen.UploadState
	AfterCreatedAt *time.Time
	AfterID        *uuid.UUID
	Limit          int32
}

func (s *Service) List(ctx context.Context, in ListInput) ([]*sqlcgen.UploadSession, error) {
	if in.UserID <= 0 {
		return nil, ErrInvalidInput
	}
	if in.Limit <= 0 {
		in.Limit = 100
	}
	if in.Limit > 200 {
		in.Limit = 200
	}
	var state sqlcgen.NullUploadState
	if in.State != nil {
		state = sqlcgen.NullUploadState{UploadState: *in.State, Valid: true}
	}
	items, err := s.queries.ListUploadSessions(ctx, sqlcgen.ListUploadSessionsParams{
		UserID:         in.UserID,
		State:          state,
		AfterCreatedAt: dbtypes.OptionalTime(in.AfterCreatedAt),
		AfterID:        dbtypes.OptionalUUID(in.AfterID),
		PageSize:       in.Limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list uploads: %w", err)
	}
	return items, nil
}

type ListPartsInput struct {
	UserID      int64
	UploadID    uuid.UUID
	AfterPartNo *int32
	Limit       int32
}

func (s *Service) ListParts(ctx context.Context, in ListPartsInput) ([]*sqlcgen.UploadPart, error) {
	if in.UserID <= 0 || in.UploadID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin part inspection: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	session, err := q.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: in.UserID, UploadID: dbtypes.UUID(in.UploadID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := s.validateDestination(ctx, q, session); err != nil {
		return nil, err
	}
	if in.Limit <= 0 {
		in.Limit = 100
	}
	if in.Limit > 200 {
		in.Limit = 200
	}
	var after pgtype.Int4
	if in.AfterPartNo != nil {
		after = dbtypes.Int4(*in.AfterPartNo)
	}
	if session.State == sqlcgen.UploadStateCompleted {
		proofs, err := q.ListCompletedUploadPartProofs(ctx, sqlcgen.ListCompletedUploadPartProofsParams{UserID: in.UserID, UploadID: dbtypes.UUID(in.UploadID), AfterPartNo: after, PageSize: in.Limit})
		if err != nil {
			return nil, fmt.Errorf("list completed parts: %w", err)
		}
		parts := make([]*sqlcgen.UploadPart, len(proofs))
		for i, proof := range proofs {
			parts[i] = projectCompletedPart(*proof)
		}
		return parts, nil
	}
	parts, err := q.ListUploadParts(ctx, sqlcgen.ListUploadPartsParams{
		UploadID:    dbtypes.UUID(in.UploadID),
		AfterPartNo: after,
		PageSize:    in.Limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list upload parts: %w", err)
	}
	return parts, nil
}
