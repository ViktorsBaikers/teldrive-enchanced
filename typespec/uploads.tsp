using TypeSpec.Http;
using TypeSpec.OpenAPI;

namespace TelDrive;

enum UploadState {
  open: "open",
  completing: "completing",
  completed: "completed",
  aborted: "aborted",
  expired: "expired",
}

enum UploadPartState {
  reserved: "reserved",
  uploading: "uploading",
  stored: "stored",
  failed: "failed",
}

model UploadCreateRequest {
  parentId?: Uuid;

  @minLength(1)
  @maxLength(255)
  name: string;

  /** Expected size in bytes, or -1 when the size is unknown until completion. */
  @minValue(-1)
  size: int64;

  mimeType?: string;
  modTime: utcDateTime;
  hash?: FileHash;
  encryption?: boolean = false;
  conflictPolicy?: NameConflictPolicy = NameConflictPolicy.fail;

  /** Optional preferred part size, rounded upward to a 16 MiB multiple. */
  @minValue(1048576)
  preferredPartSize?: int64;
}

/** Current-attempt plaintext EOF and optional whole-source tree hash. */
model UploadCompleteRequest {
  @minValue(0)
  size: int64;

  hash?: FileHash;
}

model UploadSession {
  id: Uuid;
  parentId?: Uuid;
  name: string;
  expectedSize: int64;
  expectedHash?: FileHash;
  mimeType?: string;

  @extension("x-ogen-time-format", "2006-01-02T15:04:05.999999999Z07:00")
  modTime: utcDateTime;

  encryption: boolean;
  conflictPolicy: NameConflictPolicy;
  partSize: int64;

  /** Requires current-attempt completion proof and exact-body part replay. */
  requiresCompletionManifest: true;

  state: UploadState;
  expiresAt: utcDateTime;
  createdAt: utcDateTime;
  completedAt?: utcDateTime;
  fileId?: Uuid;
}

model UploadPart {
  uploadId: Uuid;
  partNo: int32;
  state: UploadPartState;
  plainSize: int64;
  storedSize?: int64;
  checksum?: Checksum;
  createdAt: utcDateTime;
  updatedAt: utcDateTime;
}

model UploadDailyStatistics {
  date: plainDate;
  uploadedBytes: int64;
  completedFiles: int64;
}

enum UploadImportSourceType {
  local: "local",
  http: "http",
}

model UploadImportSource {
  type: UploadImportSourceType;
  path?: string;
  url?: url;
  headers?: Record<string>;
  destinationPath?: string;
  exclude?: string[];
}

model UploadImportRequest {
  /** Destination folder UUID or absolute drive path from root. */
  destination: string;

  sources: UploadImportSource[];
  headers?: Record<string>;
  exclude?: string[];
  minSize?: string;
  maxSize?: string;

  @minValue(1)
  @maxValue(16)
  partConcurrency?: int32 = 4;

  /** Preferred upload part size. Values are aligned to the nearest 16 MiB. */
  @minValue(67108864)
  @maxValue(2097152000)
  chunkSize?: int64 = 536870912;

  encryption?: boolean = false;
}

model UploadPartHeaders {
  @header("Content-Type")
  contentType: "application/octet-stream";

  @header("Content-Length")
  @minValue(0)
  contentLength: int64;

  /** BLAKE3 checksum of the plaintext part. */
  @header("X-Part-Checksum")
  checksum?: Checksum;
}

model UploadPartStoredResponse {
  @statusCode
  statusCode: 200 | 201;

  @body
  body: UploadPart;
}

model UploadCompletedResponse {
  @statusCode
  statusCode: 200 | 201;

  @header("Etag")
  ETag: ETag;

  @header("Location")
  location: Uri;

  @body
  body: FileEntry;
}

/** Authorized guarded discovery response; the body is intentionally empty. */
model GuardedUploadDiscovery {}

@oneOf
union GuardedSlotExpectation {
  absent: GuardedSlotAbsent,
  version: GuardedSlotVersion,
}

/** Empty-slot intent: the admitted destination must stay empty through commit. */
model GuardedSlotAbsent {
  kind: "absent";
}

/** Occupied-slot intent: exactly this file identity and generation are replaced. */
model GuardedSlotVersion {
  kind: "version";
  fileId: Uuid;

  /** Positive canonical int64 decimal file generation. */
  generation: string;
}

/**
 * Immutable destination intent for a guarded upload.
 * A null mountRoot or parentId explicitly means the account root; omissions are invalid.
 * The leaf is the guarded create `name` and is never a path or rename request.
 */
model GuardedUploadDestination {
  mountRoot: Uuid | null;
  parentId: Uuid | null;
  expected: GuardedSlotExpectation;
}

/**
 * Guarded create body. The destination owns the parent slot, so the legacy
 * `parentId` and derived `conflictPolicy` are not accepted here.
 */
model GuardedUploadCreateRequest {
  destination: GuardedUploadDestination;

  @minLength(1)
  @maxLength(255)
  name: string;

  /** Expected size in bytes, or -1 when the size is unknown until completion. */
  @minValue(-1)
  size: int64;

  mimeType?: string;
  modTime: utcDateTime;
  hash?: FileHash;
  encryption?: boolean = false;

  /** Optional preferred part size, rounded upward to a 16 MiB multiple. */
  @minValue(1048576)
  preferredPartSize?: int64;
}

/** Guarded completion always carries the exact plaintext size and BLAKE3 tree digest. */
model GuardedUploadCompleteRequest {
  @minValue(0)
  size: int64;

  hash: FileHash;
}

/** Canonical stored-name segments from the pinned root to the parent; `[]` at root. */
model GuardedFileEntry extends FileEntry {
  parentSegments?: string[];
}

/** Immutable publication receipt for a reauthorized exact guarded result. */
model GuardedCompletedReceipt {
  admissionKey: string;
  uploadId: Uuid;
  size: int64;
  hash: FileHash;
  result: GuardedFileEntry;
}

/** Guarded-only extension of the existing session DTO. */
model GuardedUploadSession extends UploadSession {
  /** Present only for a reauthorized exact result in the current incarnation. */
  completedReceipt?: GuardedCompletedReceipt;
}

model RecoveryIncarnationHeader {
  /** Current recovery incarnation; a mismatch returns 412/upload_recovery_changed. */
  @header("X-TelDrive-Recovery-Incarnation")
  recoveryIncarnation: Uuid;
}

model GuardedDiscoveryResponse {
  @statusCode
  statusCode: 200;

  @header("X-TelDrive-Recovery-Incarnation")
  recoveryIncarnation: Uuid;

  @body
  body: GuardedUploadDiscovery;
}

model GuardedUploadStatusResponse {
  @statusCode
  statusCode: 200;

  @header("X-TelDrive-Recovery-Incarnation")
  recoveryIncarnation: Uuid;

  /** Emitted only after a current guard or exact result check succeeded. */
  @header("X-TelDrive-Upload-Guard")
  uploadGuard: "1";

  @body
  body: GuardedUploadSession;
}

model GuardedUploadCompletedResponse {
  @statusCode
  statusCode: 200 | 201;

  @header("Etag")
  ETag: ETag;

  @header("Location")
  location: Uri;

  @body
  body: GuardedFileEntry;
}

@tag("Uploads")
@route("/v1/uploads")
@useAuth(TelDriveAuth)
namespace Uploads {
  /** List resumable upload sessions owned by the authenticated user. */
  @get
  @operationId("listUploads")
  op list(...CursorQuery, @query state?: UploadState):
    | JsonResponse<200, CursorPage<UploadSession>>
    | ErrorResponse<401>
    | ErrorResponse<422>;

  /** Return completed upload volume grouped by UTC day. */
  @get
  @route("/statistics")
  @operationId("getUploadStatistics")
  op statistics(
    @query
    @minValue(1)
    @maxValue(366)
    days?: int32 = 30,
  ):
    | JsonResponse<200, UploadDailyStatistics[]>
    | ErrorResponse<401>
    | ErrorResponse<422>;

  /**
   * Return the current recovery incarnation to authorized guarded clients.
   * The empty object proves only discovery; it grants no upload capability.
   */
  @get
  @route("/guarded")
  @operationId("discoverGuardedUpload")
  op discoverGuarded(): GuardedDiscoveryResponse | ErrorResponse<401>;

  /**
   * Create an owner-only guarded upload from an immutable destination intent.
   * The required incarnation must match the current one; a mismatch returns
   * 412/upload_recovery_changed before allocation. A slot mismatch returns
   * 412/upload_destination_changed. A same-key changed DTO returns 409/conflict,
   * and terminal destination loss returns 409/upload_parent_unavailable.
   * Semantically malformed guards return 422; undecodable JSON returns 400.
   */
  @post
  @route("/guarded")
  @operationId("createGuardedUpload")
  op createGuarded(
    ...IdempotencyHeader,
    ...RecoveryIncarnationHeader,
    @body body: GuardedUploadCreateRequest,
  ):
    | JsonResponse<201, UploadSession>
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<412>
    | ErrorResponse<413>
    | ErrorResponse<422>
    | ErrorResponse<429>;

  /**
   * Inspect a guarded attempt under the current recovery incarnation.
   * Returns the guarded-only session extension and `X-TelDrive-Upload-Guard: 1`
   * only after a current guard or exact result check succeeds. An incarnation
   * mismatch returns 412/upload_recovery_changed; a changed destination returns
   * 412/upload_destination_changed. Terminal destination loss returns
   * 409/upload_parent_unavailable, and expiry returns 410.
   */
  @get
  @route("/guarded/{uploadId}")
  @operationId("getGuardedUpload")
  op getGuarded(...RecoveryIncarnationHeader, @path uploadId: Uuid):
    | GuardedUploadStatusResponse
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<412>
    | ErrorResponse<422>;

  /**
   * Upload one guarded part. The `(uploadId, partNo)` pair is the idempotency key.
   * The incarnation and admitted destination are rechecked before any body is read;
   * refusal consumes no part body, allocates nothing and publishes nothing.
   * Completed-part retries verify the exact body before returning stored metadata.
   * A mismatched incarnation returns 412/upload_recovery_changed and a changed slot
   * returns 412/upload_destination_changed. Terminal destination loss returns
   * 409/upload_parent_unavailable without relocation.
   */
  @put
  @route("/guarded/{uploadId}/parts/{partNo}")
  @operationId("putGuardedUploadPart")
  op putGuardedPart(
    ...UploadPartHeaders,
    ...RecoveryIncarnationHeader,
    @path uploadId: Uuid,
    @path @minValue(1) partNo: int32,

    /** Store the part in this owned channel instead of the selected channel. */
    @query channelId?: int64,

    @body body: bytes,
  ):
    | UploadPartStoredResponse
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<412>
    | ErrorResponse<413>
    | ErrorResponse<422>
    | ErrorResponse<429>
    | ErrorResponse<503>;

  /**
   * Atomically publish a guarded upload after validating the exact size and
   * BLAKE3 plaintext tree digest against every stored part. The required
   * incarnation and frozen destination are rechecked under the commit guard;
   * a mismatch returns 412/upload_recovery_changed or 412/upload_destination_changed
   * without publication. A same-key changed request returns 409/conflict, terminal
   * destination loss returns 409/upload_parent_unavailable, and expiry returns 410.
   * Success returns the extended FileEntry with the current commit-time location.
   */
  @post
  @route("/guarded/{uploadId}/complete")
  @operationId("completeGuardedUpload")
  op completeGuarded(
    ...IdempotencyHeader,
    ...RecoveryIncarnationHeader,
    @path uploadId: Uuid,
    @body body: GuardedUploadCompleteRequest,
  ):
    | GuardedUploadCompletedResponse
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<412>
    | ErrorResponse<422>;

  /**
   * Queue server-side imports only after durable destination admission.
   * Concrete existing ancestor identities are pinned before source access;
   * destination loss permanently fences that admitted batch, even after restore.
   * Authorized parent loss returns 409/upload_parent_unavailable with message
   * "upload destination folder is unavailable". Generic jobs cannot bypass admission.
   */
  @post
  @route("/imports")
  @operationId("createUploadImport")
  op createImport(@body body: UploadImportRequest):
    | JsonResponse<202, Job>
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<422>
    | ErrorResponse<503>;

  /**
   * Create a durable upload session scoped by the current actor and Idempotency-Key.
   * Matching requests return the same session; changed requests return 409/conflict
   * only after current authorization. A key never grants access.
   * Authorized terminal attempts return 409/upload_parent_unavailable with message
   * "upload destination folder is unavailable"; restoring a folder cannot reopen them.
   * Unpublished legacy sessions whose original destination cannot be established
   * also refuse resume and require a genuinely new attempt, without deleting bytes.
   */
  @post
  @operationId("createUpload")
  op create(...IdempotencyHeader, @body body: UploadCreateRequest):
    | JsonResponse<201, UploadSession>
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<413>
    | ErrorResponse<422>
    | ErrorResponse<429>;

  /** Inspect an authorized attempt. Terminal destination loss returns 409/upload_parent_unavailable. */
  @get
  @route("/{uploadId}")
  @operationId("getUpload")
  op get(@path uploadId: Uuid):
    | JsonResponse<200, UploadSession>
    | ErrorResponse<401>
    | ErrorResponse<404>
    | ErrorResponse<403>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<422>;

  /** List parts already known to the upload session. */
  @get
  @route("/{uploadId}/parts")
  @operationId("listUploadParts")
  op listParts(...CursorQuery, @path uploadId: Uuid):
    | JsonResponse<200, CursorPage<UploadPart>>
    | ErrorResponse<401>
    | ErrorResponse<404>
    | ErrorResponse<403>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<422>;

  /**
   * Upload one part. The `(uploadId, partNo)` pair is the idempotency key.
   * Every retry consumes the exact body and verifies equality before returning stored metadata.
   * Nonfinal parts use the negotiated aligned size; only the final part may be shorter.
   * Completed-part retries recheck access to the actual current result after body verification.
   * Closing/deleted storage channels return ordinary 409/conflict, not parent loss.
   * Terminal destination loss returns 409/upload_parent_unavailable without relocation.
   */
  @put
  @route("/{uploadId}/parts/{partNo}")
  @operationId("putUploadPart")
  op putPart(
    ...UploadPartHeaders,
    @path uploadId: Uuid,
    @path @minValue(1) partNo: int32,

    /**
     * Store the part in this owned channel instead of the selected channel.
     * An explicit channel never rolls over: an unknown channel returns 404 and a
     * full or unavailable channel returns 409.
     */
    @query channelId?: int64,

    @body body: bytes,
  ):
    | UploadPartStoredResponse
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<413>
    | ErrorResponse<422>
    | ErrorResponse<429>
    | ErrorResponse<503>;

  /**
   * Validate the current-attempt size/hash against every stored part and atomically publish.
   * Contradictory completed replays fail; agreeing replays return the original file.
   * Current result access remains required, including after moves, revocation, trash or purge.
   * Terminal destination loss returns 409/upload_parent_unavailable with message
   * "upload destination folder is unavailable", even after restore or restart.
   * Secondary cleanup failure never changes that primary response.
   */
  @post
  @route("/{uploadId}/complete")
  @operationId("completeUpload")
  op complete(
    ...IdempotencyHeader,
    @path uploadId: Uuid,
    @body body: UploadCompleteRequest,
  ):
    | UploadCompletedResponse
    | ErrorResponse<400>
    | ErrorResponse<401>
    | ErrorResponse<403>
    | ErrorResponse<404>
    | ErrorResponse<409>
    | ErrorResponse<410>
    | ErrorResponse<422>;

  /** Abort an upload and schedule physical Telegram cleanup. */
  @delete
  @route("/{uploadId}")
  @operationId("abortUpload")
  op abort(@path uploadId: Uuid):
    | EmptyResponse<204>
    | ErrorResponse<401>
    | ErrorResponse<404>
    | ErrorResponse<409>;
}
