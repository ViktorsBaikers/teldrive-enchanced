package catalog

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
)

type UpdateInput struct {
	UserID             int64
	FileID             uuid.UUID
	ExpectedGeneration *int64
	Name               *string
	ModTime            *time.Time
}

func (s *Service) Update(ctx context.Context, in UpdateInput) (*sqlcgen.File, error) {
	if in.UserID <= 0 {
		return nil, ErrInvalidOwner
	}
	if in.FileID == uuid.Nil || (in.Name == nil && in.ModTime == nil) {
		return nil, ErrInvalidName
	}
	var name pgtype.Text
	if in.Name != nil {
		name = dbtypes.Text(*in.Name)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err = lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	current, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{FileID: dbtypes.UUID(in.FileID), UserID: in.UserID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var parentID *uuid.UUID
	if current.ParentID.Valid {
		parent := uuid.UUID(current.ParentID.Bytes)
		parentID = &parent
	}
	if err = checkActiveParent(ctx, q, in.UserID, parentID); err != nil {
		return nil, err
	}
	file, err := q.UpdateFileMetadata(ctx, sqlcgen.UpdateFileMetadataParams{
		Name:               name,
		ModTime:            dbtypes.OptionalTime(in.ModTime),
		FileID:             dbtypes.UUID(in.FileID),
		UserID:             in.UserID,
		ExpectedGeneration: dbtypes.OptionalInt8(in.ExpectedGeneration),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		if in.ExpectedGeneration != nil {
			return nil, ErrPrecondition
		}
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, classifyWriteError("update file", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.invalidateFile(ctx, in.UserID, in.FileID)
	return file, nil
}
