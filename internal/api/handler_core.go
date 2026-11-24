package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/shares"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func (h *Handler) HealthLive(ctx context.Context) (*gen.HealthStatus, error) {
	if h.Health == nil {
		return nil, problem(503, "service_unavailable", "health service is unavailable", ErrOperationUnavailable)
	}
	status := h.Health.Live()
	return &gen.HealthStatus{Status: gen.HealthStatusStatus(status.State), Version: status.Version}, nil
}

func (h *Handler) HealthReady(ctx context.Context) (gen.HealthReadyRes, error) {
	if h.Health == nil {
		return nil, problem(503, "service_unavailable", "health service is unavailable", ErrOperationUnavailable)
	}
	status, err := h.Health.Ready(ctx)
	if err != nil {
		return nil, problem(503, "not_ready", "service is not ready", err)
	}
	return &gen.HealthStatus{Status: gen.HealthStatusStatus(status.State), Version: status.Version}, nil
}

func (h *Handler) CreateFolder(ctx context.Context, req *gen.FolderCreateRequest, params gen.CreateFolderParams) (gen.CreateFolderRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Catalog == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	if policy, ok := req.ConflictPolicy.Get(); ok && string(policy) != "fail" {
		return nil, mapServiceError(uploads.ErrUnsupportedConflictPolicy)
	}
	var modTime time.Time
	if value, ok := req.ModTime.Get(); ok {
		modTime = value
	}
	parentID := optionalGoogleUUID(req.ParentId)
	ownerID := userID
	if parentID != nil {
		access, err := h.resolveAuthenticatedFileAccess(ctx, *parentID, true)
		if err != nil {
			return nil, mapServiceError(err)
		}
		ownerID = access.OwnerID
	}
	file, err := h.Catalog.CreateFolder(ctx, catalog.CreateFolderInput{
		UserID: ownerID, ParentID: parentID, Name: req.Name, ModTime: modTime,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	entry, err := fileEntry(file)
	if err != nil {
		return nil, mapServiceError(err)
	}
	fileID := googleUUID(entry.ID)
	return &gen.CopyFileCreatedHeaders{
		Etag: generationETag(file.Generation), Location: gen.URI(url.URL{Path: "/files/" + fileID.String()}),
		Response: entry,
	}, nil
}

func (h *Handler) GetFile(ctx context.Context, params gen.GetFileParams) (gen.GetFileRes, error) {
	if h.Catalog == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	access, err := h.resolveAuthenticatedFileAccess(ctx, googleUUID(params.FileId), false)
	if err != nil {
		return nil, mapServiceError(err)
	}
	file, err := h.Catalog.Get(ctx, access.OwnerID, googleUUID(params.FileId))
	if err != nil {
		return nil, mapServiceError(err)
	}
	entry, err := fileEntry(file)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.FileEntryHeaders{Etag: generationETag(file.Generation), Response: entry}, nil
}

func (h *Handler) ListFiles(ctx context.Context, params gen.ListFilesParams) (gen.ListFilesRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Catalog == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	var cursor fileCursor
	if err := decodeCursor(params.Cursor, &cursor); err != nil {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	limit := params.Limit.Or(100)
	scope := string(params.Scope.Or(gen.FileListQueryScopeFolder))
	bookmarked := params.Bookmarked.Or(false)
	if bookmarked {
		if params.Status.Or(gen.FileListQueryStatusActive) != gen.FileListQueryStatusActive {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
		scope = "drive"
	}
	sortBy, order, searchType := "name", "asc", "text"
	if value, ok := params.Sort.Get(); ok {
		sortBy = string(value)
	}
	if value, ok := params.Order.Get(); ok {
		order = string(value)
	}
	if value, ok := params.SearchType.Get(); ok {
		searchType = string(value)
	}
	if cursor.Sort != "" && (cursor.Sort != sortBy || cursor.Order != order) {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	parentID := optionalGoogleUUID(params.ParentId)
	ownerID := userID
	if cursor.ID != uuid.Nil && cursor.Scope != "" && cursor.Scope != scope {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	if scope != "folder" {
		folderCursor := ""
		if parentID != nil {
			folderCursor = parentID.String()
		}
		fingerprintBytes, _ := json.Marshal(struct {
			Scope, Folder, Search, SearchType, Sort, Order, Kind, Status string
			Categories                                                   []gen.FileCategory
			After, Before                                                string
			Bookmarked                                                   bool
		}{scope, folderCursor, params.Search.Or(""), searchType, sortBy, order, string(params.Kind.Or("")), string(params.Status.Or("")), params.Category, params.UpdatedAfter.Or(time.Time{}).Format(time.RFC3339Nano), params.UpdatedBefore.Or(time.Time{}).Format(time.RFC3339Nano), bookmarked})
		fingerprintSum := sha256.Sum256(fingerprintBytes)
		fingerprint := hex.EncodeToString(fingerprintSum[:])
		if cursor.ID != uuid.Nil && (cursor.Scope != scope || cursor.FolderID != folderCursor || cursor.Fingerprint != fingerprint) {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
		cursor.Fingerprint = fingerprint
		if strings.TrimSpace(params.Path.Or("")) != "" {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
		if scope == "drive" && parentID != nil {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
	} else if parentID != nil {
		access, err := h.resolveAuthenticatedFileAccess(ctx, *parentID, false)
		if err != nil {
			return nil, mapServiceError(err)
		}
		ownerID = access.OwnerID
	}
	input := catalog.ListInput{
		UserID: ownerID, Scope: scope, ParentID: parentID, Path: params.Path.Or(""),
		Search: params.Search.Or(""), SearchType: searchType,
		Sort: sortBy, Order: order, Limit: limit, Bookmarked: bookmarked,
	}
	if scope == "recursive" {
		input.ScopeFolderID = parentID
		input.ParentID = nil
	}
	if value, ok := params.Kind.Get(); ok {
		kind := sqlcgen.FileKind(value)
		input.Kind = &kind
	}
	if value, ok := params.Status.Get(); ok {
		input.Status = sqlcgen.FileStatus(value)
	}
	for _, category := range params.Category {
		input.Categories = append(input.Categories, string(category))
	}
	if value, ok := params.UpdatedAfter.Get(); ok {
		input.UpdatedAfter = &value
	}
	if value, ok := params.UpdatedBefore.Get(); ok {
		input.UpdatedBefore = &value
	}
	if cursor.ID != uuid.Nil {
		input.AfterID = &cursor.ID
		input.AfterName = cursor.Name
		input.AfterValue = cursor.Value
	}
	files, err := h.Catalog.List(ctx, input)
	if err != nil {
		return nil, mapServiceError(err)
	}
	paths := map[uuid.UUID]string{}
	if scope != "folder" {
		ids := make([]uuid.UUID, 0, len(files))
		for _, file := range files {
			if id, ok := dbtypes.GoogleUUID(file.ID); ok {
				ids = append(ids, id)
			}
		}
		paths, err = h.Catalog.ParentPaths(ctx, userID, ids)
		if err != nil {
			return nil, mapServiceError(err)
		}
	}
	items := make([]gen.FileEntry, 0, len(files))
	for _, file := range files {
		entry, err := fileEntry(file)
		if err != nil {
			return nil, mapServiceError(err)
		}
		if scope != "folder" {
			if id, ok := dbtypes.GoogleUUID(file.ID); ok {
				entry.ParentPath = gen.NewOptString(paths[id])
			}
		}
		items = append(items, entry)
	}
	response := gen.ListFilesOK{Items: items}
	if len(files) == int(limit) && len(files) > 0 {
		last := files[len(files)-1]
		lastID, _ := dbtypes.GoogleUUID(last.ID)
		response.NextCursor = encodeCursor(fileCursor{
			Name: last.Name, Sort: sortBy, Order: order,
			Value: catalog.FileCursorValue(last, sortBy), ID: lastID, Scope: scope,
			FolderID: func() string {
				if parentID != nil {
					return parentID.String()
				}
				return ""
			}(),
			Fingerprint: cursor.Fingerprint,
		})
	}
	return &response, nil
}

func (h *Handler) BookmarkFiles(ctx context.Context, req *gen.BookmarkFilesRequest) (gen.BookmarkFilesRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Catalog == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	ids := make([]uuid.UUID, 0, len(req.FileIds))
	for _, id := range req.FileIds {
		ids = append(ids, googleUUID(id))
	}
	if err := h.Catalog.BookmarkFiles(ctx, userID, ids); err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.BookmarkFilesNoContent{}, nil
}

func (h *Handler) RemoveFileBookmarks(ctx context.Context, req *gen.BookmarkFilesRequest) (gen.RemoveFileBookmarksRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Catalog == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	ids := make([]uuid.UUID, 0, len(req.FileIds))
	for _, id := range req.FileIds {
		ids = append(ids, googleUUID(id))
	}
	if err := h.Catalog.RemoveFileBookmarks(ctx, userID, ids); err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.RemoveFileBookmarksNoContent{}, nil
}

func (h *Handler) UpdateFile(ctx context.Context, req *gen.FileUpdateRequest, params gen.UpdateFileParams) (gen.UpdateFileRes, error) {
	if h.Catalog == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	generation, err := parseGenerationETag(params.IfMatch)
	if err != nil {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	var name *string
	if value, ok := req.Name.Get(); ok {
		name = &value
	}
	var modTime *time.Time
	if value, ok := req.ModTime.Get(); ok {
		modTime = &value
	}
	access, err := h.resolveAuthenticatedFileAccess(ctx, googleUUID(params.FileId), true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	file, err := h.Catalog.Update(ctx, catalog.UpdateInput{
		UserID: access.OwnerID, FileID: googleUUID(params.FileId), ExpectedGeneration: generation, Name: name, ModTime: modTime,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	entry, err := fileEntry(file)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.FileEntryHeaders{Etag: generationETag(file.Generation), Response: entry}, nil
}

func (h *Handler) MoveFile(ctx context.Context, req *gen.FileMoveRequest, params gen.MoveFileParams) (gen.MoveFileRes, error) {
	if h.Catalog == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	policy := "fail"
	if value, ok := req.ConflictPolicy.Get(); ok {
		policy = string(value)
	}
	generation, err := parseGenerationETag(params.IfMatch)
	if err != nil {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	sourceAccess, err := h.resolveAuthenticatedFileAccess(ctx, googleUUID(params.FileId), true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	parentID := optionalGoogleUUID(req.ParentId)
	if parentID == nil {
		if !sourceAccess.Owned {
			return nil, mapServiceError(shares.ErrForbidden)
		}
	} else {
		destinationAccess, err := h.resolveAuthenticatedFileAccess(ctx, *parentID, true)
		if err != nil {
			return nil, mapServiceError(err)
		}
		if destinationAccess.OwnerID != sourceAccess.OwnerID {
			return nil, mapServiceError(shares.ErrForbidden)
		}
	}
	file, err := h.Catalog.MoveWithPolicy(ctx, sourceAccess.OwnerID, googleUUID(params.FileId), parentID, generation, policy)
	if err != nil {
		return nil, mapServiceError(err)
	}
	entry, err := fileEntry(file)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.FileEntryHeaders{Etag: generationETag(file.Generation), Response: entry}, nil
}

func (h *Handler) TrashFile(ctx context.Context, params gen.TrashFileParams) (gen.TrashFileRes, error) {
	if h.Catalog == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	fileID := googleUUID(params.FileId)
	access, err := h.resolveAuthenticatedFileAccess(ctx, fileID, true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if !access.Owned && access.RootFileID == fileID {
		return nil, mapServiceError(shares.ErrForbidden)
	}
	if _, err := h.Catalog.Trash(ctx, access.OwnerID, fileID); err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.TrashFileNoContent{}, nil
}

func (h *Handler) RestoreFile(ctx context.Context, params gen.RestoreFileParams) (gen.RestoreFileRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Catalog == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	file, err := h.Catalog.Restore(ctx, userID, googleUUID(params.FileId))
	if err != nil {
		return nil, mapServiceError(err)
	}
	entry, err := fileEntry(file)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.FileEntryHeaders{Etag: generationETag(file.Generation), Response: entry}, nil
}

func (h *Handler) CreateUpload(ctx context.Context, req *gen.UploadCreateRequest, params gen.CreateUploadParams) (gen.CreateUploadRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Uploads == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	admission, err := uploadRequestAdmission(lifecycle.RequestScope{ActorUserID: &userID}, googleUUID(params.IdempotencyKey), req)
	if err != nil {
		return nil, mapServiceError(err)
	}
	session, found, err := h.replayUploadAdmission(ctx, admission, "", "")
	if err != nil {
		return nil, mapServiceError(err)
	}
	if !found {
		parentID := optionalGoogleUUID(req.ParentId)
		ownerID := userID
		if parentID != nil {
			access, resolveErr := h.resolveAuthenticatedFileAccess(ctx, *parentID, true)
			if resolveErr != nil {
				return nil, mapServiceError(resolveErr)
			}
			ownerID = access.OwnerID
		}
		session, err = h.createUploadForOwner(ctx, ownerID, parentID, req, admission)
	}
	if err != nil {
		retained, exists, replayErr := h.replayUploadAdmission(ctx, admission, "", "")
		if replayErr != nil {
			return nil, mapServiceError(replayErr)
		}
		if !exists {
			return nil, err
		}
		session = retained
	}
	if _, err := h.resolveAuthenticatedUploadOwner(ctx, uuid.UUID(session.ID.Bytes), true); err != nil {
		return nil, mapServiceError(err)
	}
	response, err := uploadSession(session)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &response, nil
}

func (h *Handler) GetUpload(ctx context.Context, params gen.GetUploadParams) (gen.GetUploadRes, error) {
	if h.Uploads == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	ownerID, err := h.resolveAuthenticatedUploadOwner(ctx, googleUUID(params.UploadId), false)
	if err != nil {
		return nil, mapServiceError(err)
	}
	session, err := h.Uploads.Get(ctx, ownerID, googleUUID(params.UploadId))
	if err != nil {
		return nil, mapServiceError(err)
	}
	response, err := uploadSession(session)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &response, nil
}

func (h *Handler) ListUploads(ctx context.Context, params gen.ListUploadsParams) (gen.ListUploadsRes, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Uploads == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	var cursor uploadCursor
	if err := decodeCursor(params.Cursor, &cursor); err != nil {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	input := uploads.ListInput{UserID: userID, Limit: params.Limit.Or(100)}
	if value, ok := params.State.Get(); ok {
		state := sqlcgen.UploadState(value)
		input.State = &state
	}
	if cursor.ID != uuid.Nil {
		input.AfterID, input.AfterCreatedAt = &cursor.ID, &cursor.CreatedAt
	}
	sessions, err := h.Uploads.List(ctx, input)
	if err != nil {
		return nil, mapServiceError(err)
	}
	items := make([]gen.UploadSession, 0, len(sessions))
	for _, session := range sessions {
		item, err := uploadSession(session)
		if err != nil {
			return nil, mapServiceError(err)
		}
		items = append(items, item)
	}
	response := gen.ListUploadsOK{Items: items}
	if len(sessions) == int(input.Limit) && len(sessions) > 0 {
		last := sessions[len(sessions)-1]
		lastID, _ := dbtypes.GoogleUUID(last.ID)
		response.NextCursor = encodeCursor(uploadCursor{CreatedAt: last.CreatedAt.Time, ID: lastID})
	}
	return &response, nil
}

func (h *Handler) ListUploadParts(ctx context.Context, params gen.ListUploadPartsParams) (gen.ListUploadPartsRes, error) {
	if h.Uploads == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	var cursor partCursor
	if err := decodeCursor(params.Cursor, &cursor); err != nil {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	ownerID, err := h.resolveAuthenticatedUploadOwner(ctx, googleUUID(params.UploadId), false)
	if err != nil {
		return nil, mapServiceError(err)
	}
	input := uploads.ListPartsInput{UserID: ownerID, UploadID: googleUUID(params.UploadId), Limit: params.Limit.Or(100)}
	if cursor.PartNo > 0 {
		input.AfterPartNo = &cursor.PartNo
	}
	parts, err := h.Uploads.ListParts(ctx, input)
	if err != nil {
		return nil, mapServiceError(err)
	}
	items := make([]gen.UploadPart, 0, len(parts))
	for _, part := range parts {
		item, err := uploadPart(part)
		if err != nil {
			return nil, mapServiceError(err)
		}
		items = append(items, item)
	}
	response := gen.ListUploadPartsOK{Items: items}
	if len(parts) == int(input.Limit) && len(parts) > 0 {
		response.NextCursor = encodeCursor(partCursor{PartNo: parts[len(parts)-1].PartNo})
	}
	return &response, nil
}

func (h *Handler) PutUploadPart(ctx context.Context, req gen.PutUploadPartReq, params gen.PutUploadPartParams) (gen.PutUploadPartRes, error) {
	if h.UploadPipeline == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	var checksum *string
	if value, ok := params.XPartChecksum.Get(); ok {
		text := string(value)
		checksum = &text
	}
	ownerID, err := h.resolveAuthenticatedUploadOwner(ctx, googleUUID(params.UploadId), true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	result, err := h.UploadPipeline.UploadPart(ctx, transfer.UploadPartRequest{
		UserID: ownerID, UploadID: googleUUID(params.UploadId), PartNo: params.PartNo,
		RequestedChannelID: params.ChannelId.Or(0),
		PlainSize:          params.ContentLength, Checksum: checksum, Body: req.Data,
	})
	if _, accessErr := h.resolveAuthenticatedUploadOwner(ctx, googleUUID(params.UploadId), true); accessErr != nil {
		return nil, mapServiceError(accessErr)
	}
	if err != nil {
		return nil, mapServiceError(err)
	}
	part, err := uploadPart(result.Part)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if result.Existing {
		response := gen.PutUploadPartOK(part)
		return &response, nil
	}
	response := gen.PutUploadPartCreated(part)
	return &response, nil
}

func (h *Handler) CompleteUpload(ctx context.Context, req *gen.UploadCompleteRequest, params gen.CompleteUploadParams) (gen.CompleteUploadRes, error) {
	if h.Uploads == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	ownerID, err := h.resolveAuthenticatedUploadOwner(ctx, googleUUID(params.UploadId), true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	in := uploads.CompleteInput{UserID: ownerID, UploadID: googleUUID(params.UploadId), Size: req.Size}
	if hash, ok := req.Hash.Get(); ok {
		algorithm, value := string(hash.Algorithm), string(hash.Value)
		in.HashAlgorithm, in.HashValue = &algorithm, &value
	}
	file, err := h.Uploads.Complete(ctx, in)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if _, err := h.resolveAuthenticatedFileAccess(ctx, uuid.UUID(file.ID.Bytes), true); err != nil {
		return nil, mapServiceError(err)
	}
	entry, err := fileEntry(file)
	if err != nil {
		return nil, mapServiceError(err)
	}
	fileID := googleUUID(entry.ID)
	headers := gen.CopyFileCreatedHeaders{
		Etag: generationETag(file.Generation), Location: gen.URI(url.URL{Path: "/files/" + fileID.String()}),
		Response: entry,
	}
	response := gen.CompleteUploadCreated(headers)
	return &response, nil
}

func (h *Handler) AbortUpload(ctx context.Context, params gen.AbortUploadParams) (gen.AbortUploadRes, error) {
	if h.Uploads == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	ownerID, err := h.resolveAuthenticatedUploadOwner(ctx, googleUUID(params.UploadId), true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if _, err := h.Uploads.Abort(ctx, ownerID, googleUUID(params.UploadId)); err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.AbortUploadNoContent{}, nil
}

func (h *Handler) DiscoverGuardedUpload(ctx context.Context) (gen.DiscoverGuardedUploadRes, error) {
	if _, err := UserIDFromContext(ctx); err != nil {
		return nil, mapServiceError(err)
	}
	if h.Uploads == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	incarnation, err := h.Uploads.RecoveryIncarnation(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.GuardedUploadDiscoveryHeaders{
		XTelDriveRecoveryIncarnation: apiUUID(incarnation),
		Response:                     gen.GuardedUploadDiscovery{},
	}, nil
}

func (h *Handler) CreateGuardedUpload(ctx context.Context, req *gen.GuardedUploadCreateRequest, params gen.CreateGuardedUploadParams) (gen.CreateGuardedUploadRes, error) {
	actorID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if h.Uploads == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	admission, err := guardedCreateRequestAdmission(lifecycle.RequestScope{ActorUserID: &actorID}, googleUUID(params.IdempotencyKey), req)
	if err != nil {
		return nil, mapServiceError(err)
	}
	destination := req.Destination
	guard := &uploads.GuardInput{RecoveryIncarnation: googleUUID(params.XTelDriveRecoveryIncarnation)}
	if mountRoot, ok := destination.MountRoot.Get(); ok {
		id := googleUUID(mountRoot)
		if id == uuid.Nil {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
		guard.MountRoot = &id
	}
	parentID := optionalNilUUID(destination.ParentId)
	conflictPolicy := sqlcgen.NameConflictPolicyFail
	if version, ok := destination.Expected.GetGuardedSlotVersion(); ok {
		generation, parseErr := strconv.ParseInt(version.Generation, 10, 64)
		if parseErr != nil || generation <= 0 || strconv.FormatInt(generation, 10) != version.Generation {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
		fileID := googleUUID(version.FileId)
		if fileID == uuid.Nil {
			return nil, mapServiceError(uploads.ErrInvalidInput)
		}
		guard.ExpectedFileID = &fileID
		guard.ExpectedGeneration = &generation
		conflictPolicy = sqlcgen.NameConflictPolicyReplace
	} else if _, ok := destination.Expected.GetGuardedSlotAbsent(); !ok {
		return nil, mapServiceError(uploads.ErrInvalidInput)
	}
	input := uploads.CreateInput{
		UserID: actorID, ParentID: parentID, Name: req.Name, ExpectedSize: req.Size,
		ModTime: req.ModTime, PartSize: req.PreferredPartSize.Or(0),
		Admission: admission, ConflictPolicy: conflictPolicy, Guard: guard,
	}
	if value, ok := req.MimeType.Get(); ok {
		input.MIMEType = &value
	}
	if value, ok := req.Hash.Get(); ok {
		algorithm, checksum := string(value.Algorithm), string(value.Value)
		input.ExpectedHashAlgorithm, input.ExpectedHashValue = &algorithm, &checksum
	}
	if value, ok := req.Encryption.Get(); ok {
		input.Encryption = value
	}
	input.Encryption = input.Encryption || h.UploadEncryptionRequired
	if input.Encryption {
		if h.ActiveEncryptionKeyVersion <= 0 {
			return nil, mapServiceError(transfer.ErrEncryptionKey)
		}
		version := h.ActiveEncryptionKeyVersion
		input.EncryptionKeyVersion = &version
	}
	session, err := h.Uploads.Create(ctx, input)
	if err != nil {
		return nil, mapServiceError(err)
	}
	response, err := uploadSession(session)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &response, nil
}

func (h *Handler) GetGuardedUpload(ctx context.Context, params gen.GetGuardedUploadParams) (gen.GetGuardedUploadRes, error) {
	uploadID := googleUUID(params.UploadId)
	ownerID, err := h.guardedUploadOwner(ctx, uploadID, false)
	if err != nil {
		return nil, mapServiceError(err)
	}
	incarnation := googleUUID(params.XTelDriveRecoveryIncarnation)
	upload, err := h.Uploads.GetGuarded(ctx, ownerID, uploadID, incarnation)
	if err != nil {
		return nil, mapServiceError(err)
	}
	response, err := guardedUploadSession(upload)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return &gen.GuardedUploadSessionHeaders{
		XTelDriveRecoveryIncarnation: apiUUID(incarnation),
		XTelDriveUploadGuard:         gen.GetGuardedUploadOKXTelDriveUploadGuard1,
		Response:                     response,
	}, nil
}

func (h *Handler) PutGuardedUploadPart(ctx context.Context, req gen.PutGuardedUploadPartReq, params gen.PutGuardedUploadPartParams) (gen.PutGuardedUploadPartRes, error) {
	if h.UploadPipeline == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	ownerID, err := h.guardedUploadOwner(ctx, googleUUID(params.UploadId), true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	var checksum *string
	if value, ok := params.XPartChecksum.Get(); ok {
		text := string(value)
		checksum = &text
	}
	result, err := h.UploadPipeline.UploadPart(ctx, transfer.UploadPartRequest{
		UserID: ownerID, UploadID: googleUUID(params.UploadId), PartNo: params.PartNo,
		RequestedChannelID: params.ChannelId.Or(0),
		PlainSize:          params.ContentLength, Checksum: checksum, Body: req.Data,
		Guarded: true, RecoveryIncarnation: googleUUID(params.XTelDriveRecoveryIncarnation),
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	part, err := uploadPart(result.Part)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if result.Existing {
		response := gen.PutGuardedUploadPartOK(part)
		return &response, nil
	}
	response := gen.PutGuardedUploadPartCreated(part)
	return &response, nil
}

func (h *Handler) CompleteGuardedUpload(ctx context.Context, req *gen.GuardedUploadCompleteRequest, params gen.CompleteGuardedUploadParams) (gen.CompleteGuardedUploadRes, error) {
	if h.Uploads == nil || req == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	uploadID := googleUUID(params.UploadId)
	ownerID, err := h.guardedUploadOwner(ctx, uploadID, true)
	if err != nil {
		return nil, mapServiceError(err)
	}
	actorID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, mapServiceError(err)
	}
	admission, err := guardedCompleteRequestAdmission(lifecycle.RequestScope{ActorUserID: &actorID}, googleUUID(params.IdempotencyKey), req)
	if err != nil {
		return nil, mapServiceError(err)
	}
	incarnation := googleUUID(params.XTelDriveRecoveryIncarnation)
	algorithm, hashValue := string(req.Hash.Algorithm), string(req.Hash.Value)
	file, err := h.Uploads.Complete(ctx, uploads.CompleteInput{
		UserID: ownerID, UploadID: uploadID, Size: req.Size,
		HashAlgorithm: &algorithm, HashValue: &hashValue,
		Guarded: true, Admission: admission, RecoveryIncarnation: incarnation,
	})
	if err != nil {
		return nil, mapServiceError(err)
	}
	var segments []string
	if upload, getErr := h.Uploads.GetGuarded(ctx, ownerID, uploadID, incarnation); getErr == nil && upload.Receipt != nil {
		segments = upload.Receipt.ParentSegments
	}
	entry, err := guardedFileEntry(file, segments)
	if err != nil {
		return nil, mapServiceError(err)
	}
	fileID := googleUUID(entry.ID)
	response := gen.CompleteGuardedUploadOK(gen.GuardedFileEntryHeaders{
		Etag: generationETag(file.Generation), Location: gen.URI(url.URL{Path: "/files/" + fileID.String()}),
		Response: entry,
	})
	return &response, nil
}

func (h *Handler) guardedUploadOwner(ctx context.Context, uploadID uuid.UUID, requireEdit bool) (int64, error) {
	actorID, err := UserIDFromContext(ctx)
	if err != nil {
		return 0, err
	}
	ownerID, err := h.resolveAuthenticatedUploadOwner(ctx, uploadID, requireEdit)
	if err != nil {
		return 0, err
	}
	if ownerID != actorID {
		return 0, shares.ErrForbidden
	}
	return ownerID, nil
}

func optionalNilUUID(value gen.NilUUID) *uuid.UUID {
	id, ok := value.Get()
	if !ok {
		return nil
	}
	converted := googleUUID(id)
	return &converted
}

func guardedCreateRequestAdmission(scope lifecycle.RequestScope, key uuid.UUID, req *gen.GuardedUploadCreateRequest) (*lifecycle.RequestAdmissionInput, error) {
	if key == uuid.Nil || req == nil {
		return nil, uploads.ErrInvalidInput
	}
	h := admissionHasher{digest: sha256.New()}
	h.text("teldrive.request.guarded_upload_create.v1")
	writeNilUUID := func(value gen.NilUUID) {
		id, ok := value.Get()
		h.present(ok)
		if ok {
			converted := googleUUID(id)
			_, _ = h.digest.Write(converted[:])
		}
	}
	destination := req.Destination
	writeNilUUID(destination.MountRoot)
	writeNilUUID(destination.ParentId)
	if absent, ok := destination.Expected.GetGuardedSlotAbsent(); ok {
		h.text("absent")
		h.text(string(absent.Kind))
	} else if version, ok := destination.Expected.GetGuardedSlotVersion(); ok {
		h.text("version")
		h.text(string(version.Kind))
		id := googleUUID(version.FileId)
		_, _ = h.digest.Write(id[:])
		h.text(version.Generation)
	} else {
		return nil, uploads.ErrInvalidInput
	}
	h.text(req.Name)
	h.number(req.Size)
	mime, present := req.MimeType.Get()
	h.optionalText(mime, present)
	h.number(req.ModTime.UTC().UnixMicro())
	proof, present := req.Hash.Get()
	h.present(present)
	if present {
		h.text(string(proof.Algorithm))
		h.text(string(proof.Value))
	}
	h.present(req.Encryption.Or(false))
	size, present := req.PreferredPartSize.Get()
	h.present(present)
	if present {
		h.number(size)
	}
	in := &lifecycle.RequestAdmissionInput{Scope: scope, RequestKey: key}
	h.digest.Sum(in.RequestFingerprint[:0])
	return in, nil
}

func guardedCompleteRequestAdmission(scope lifecycle.RequestScope, key uuid.UUID, req *gen.GuardedUploadCompleteRequest) (*lifecycle.RequestAdmissionInput, error) {
	if key == uuid.Nil || req == nil {
		return nil, uploads.ErrInvalidInput
	}
	h := admissionHasher{digest: sha256.New()}
	h.text("teldrive.request.guarded_upload_complete.v1")
	h.number(req.Size)
	h.text(string(req.Hash.Algorithm))
	h.text(string(req.Hash.Value))
	in := &lifecycle.RequestAdmissionInput{Scope: scope, RequestKey: key}
	h.digest.Sum(in.RequestFingerprint[:0])
	return in, nil
}

func guardedUploadSession(upload *uploads.GuardedUpload) (gen.GuardedUploadSession, error) {
	base, err := uploadSession(upload.Session)
	if err != nil {
		return gen.GuardedUploadSession{}, err
	}
	result := gen.GuardedUploadSession{
		ID: base.ID, ParentId: base.ParentId, Name: base.Name,
		ExpectedSize: base.ExpectedSize, ExpectedHash: base.ExpectedHash, MimeType: base.MimeType,
		ModTime: base.ModTime, Encryption: base.Encryption, ConflictPolicy: base.ConflictPolicy,
		PartSize: base.PartSize, RequiresCompletionManifest: gen.GuardedUploadSessionRequiresCompletionManifestTrue,
		State: base.State, ExpiresAt: base.ExpiresAt, CreatedAt: base.CreatedAt,
		CompletedAt: base.CompletedAt, FileId: base.FileId,
	}
	if upload.Receipt != nil {
		entry, err := guardedFileEntry(upload.Receipt.File, upload.Receipt.ParentSegments)
		if err != nil {
			return gen.GuardedUploadSession{}, err
		}
		result.CompletedReceipt = gen.NewOptGuardedCompletedReceipt(gen.GuardedCompletedReceipt{
			AdmissionKey: upload.Receipt.AdmissionKey.String(),
			UploadId:     apiUUID(upload.Receipt.UploadID),
			Size:         upload.Receipt.Size,
			Hash: gen.FileHash{
				Algorithm: gen.HashAlgorithm(upload.Receipt.HashAlgorithm),
				Value:     gen.Checksum(upload.Receipt.HashValue),
			},
			Result: entry,
		})
	}
	return result, nil
}

func guardedFileEntry(file *sqlcgen.File, parentSegments []string) (gen.GuardedFileEntry, error) {
	entry, err := fileEntry(file)
	if err != nil {
		return gen.GuardedFileEntry{}, err
	}
	return gen.GuardedFileEntry{
		ID: entry.ID, ParentId: entry.ParentId, Name: entry.Name, Kind: entry.Kind,
		MimeType: entry.MimeType, Size: entry.Size, Hash: entry.Hash,
		Encryption: entry.Encryption, Status: entry.Status, ModTime: entry.ModTime,
		Generation: entry.Generation, CreatedAt: entry.CreatedAt, UpdatedAt: entry.UpdatedAt,
		ParentPath: entry.ParentPath, ParentSegments: parentSegments,
	}, nil
}

func (h *Handler) HeadFile(ctx context.Context, params gen.HeadFileParams) (gen.HeadFileRes, error) {
	if h.Catalog == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	file, err := h.resolveFileContent(ctx, googleUUID(params.FileId))
	if err != nil {
		return nil, mapServiceError(err)
	}
	if file.Kind != sqlcgen.FileKindFile || !file.Size.Valid {
		return nil, mapServiceError(transfer.ErrInvalidDownload)
	}
	return &gen.HeadFileOK{
		AcceptRanges: gen.HeadFileOKAcceptRanges("bytes"), ContentDisposition: contentDisposition(file.Name, false),
		ContentLength: file.Size.Int64, Etag: contentETag(file), LastModified: file.ModTime.Time,
	}, nil
}

func (h *Handler) HeadFileLegacy(ctx context.Context, params gen.HeadFileLegacyParams) (gen.HeadFileLegacyRes, error) {
	if h.Catalog == nil {
		return nil, mapServiceError(ErrOperationUnavailable)
	}
	fileID := googleUUID(params.FileId)
	file, err := h.resolveFileContent(ctx, fileID)
	if err != nil {
		return nil, mapServiceError(err)
	}
	if file.Kind != sqlcgen.FileKindFile || !file.Size.Valid {
		return nil, mapServiceError(catalog.ErrNotFound)
	}
	return &gen.HeadFileLegacyOK{
		AcceptRanges: gen.HeadFileLegacyOKAcceptRanges("bytes"), ContentDisposition: contentDisposition(file.Name, false),
		ContentLength: file.Size.Int64, Etag: contentETag(file), LastModified: file.ModTime.Time,
	}, nil
}

func contentETag(file *sqlcgen.File) gen.ETag {
	if file.HashValue.Valid && strings.TrimSpace(file.HashValue.String) != "" {
		return gen.ETag(`"` + file.HashValue.String + `"`)
	}
	return generationETag(file.Generation)
}
