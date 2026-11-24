# Contributing to Teldrive

Bug reports, documentation improvements, and pull requests are welcome. For questions or ideas, join the [Discord community](https://discord.gg/8QAeCvTK7G).

## Report a bug or suggest a feature

Search [existing issues](https://github.com/tgdrive/teldrive/issues) before opening a new one. For bugs, include the Teldrive version, deployment method, steps to reproduce, expected behavior, and relevant logs. Remove tokens, passwords, session data, and other private information.

For larger features or architectural changes, open an issue to discuss the scope before implementing them.

## Set up development

Fork the repository and clone your fork, then choose one of the setups below. Run commands from the repository root unless noted otherwise.

### With Nix and direnv

Install Nix with flakes enabled and direnv. The repository's `.envrc` loads the toolchain defined in `flake.nix`, including Go, Bun, Just, SQLC, PostgreSQL, Podman, and Chromium for browser tests.

Inspect `.envrc` and `flake.nix` before approving the environment:

```bash
direnv allow
direnv exec . just install-tools
```

With this setup, run project commands through `direnv exec .`; do not assume a non-interactive shell has loaded the environment. The dev shell configures Playwright to use its Chromium, so no separate browser download is needed.

### Without Nix

Install these tools using their official installers or your platform's package manager:

- Go 1.26 or later, matching the minimum version in `go.mod`.
- Bun, Node.js, Git, Just, and SQLC. See `flake.nix` for the development toolchain used by this repository.
- PostgreSQL to run the application locally.
- Podman for integration tests, or Docker if Podman is not installed. The test harness prefers Podman whenever it is present; make sure your selected runtime is working.
- FFmpeg for media-related development and browser test fixtures.

Nix and direnv are not required for this manual setup. The checked-in `.envrc` is Nix-specific; do not approve it without Nix or change it just for this setup. Install the pinned JavaScript dependencies, then install Playwright's Chromium:

```bash
just install-tools
cd ui
bunx playwright install chromium
cd ..
```

On supported Linux distributions, use `bunx playwright install --with-deps chromium` from `ui/` to also install browser system dependencies; this may require administrator privileges. See [Playwright's browser setup](https://playwright.dev/docs/browsers).

Leave `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` unset to use Playwright's downloaded browser. If using an existing compatible Chromium instead, set it to that browser's absolute executable path.

The remaining examples use the Nix/direnv prefix. **For manual, non-Nix development, omit `direnv exec .`**: for example, run `just check` or `bash -c 'cd ui && bunx playwright test'` directly. Repository automation and coding agents should continue to follow the environment rules in [AGENTS.md](AGENTS.md).

### Run the application

To run the application locally, configure PostgreSQL and your Telegram application credentials using the [documentation](https://tgdrive.github.io/teldrive/). The `justfile` loads a root `.env` automatically. Never commit secrets or use production data for tests.

```bash
direnv exec . just dev
```

## Make and verify changes

Create a branch, keep the change focused, and add regression tests for new behavior or bug fixes. Reuse existing UI components and update every supported locale when changing visible text.

Useful checks:

```bash
# Format handwritten code.
direnv exec . just format

# Backend unit tests.
direnv exec . just test-unit

# PostgreSQL-backed integration tests.
direnv exec . just test-integration

# UI generation, build, lint, typechecking, and selected browser tests.
direnv exec . just ui-check

# Complete browser suite, including specs outside the default UI test command.
direnv exec . bash -c 'cd ui && bunx playwright test'

# Full project validation, including generated artifacts and documentation.
direnv exec . just check
```

Integration tests must use the PostgreSQL harness rather than an arbitrary database. For a focused run:

```bash
direnv exec . scripts/test-postgres.sh go test -tags=integration ./internal/catalog
```

## Generated files

Edit sources, not generated output, and include the regenerated artifacts in your pull request:

| Source changed | Generation command |
| --- | --- |
| HTTP contracts in `typespec/` | `direnv exec . just generate-api` and `direnv exec . just generate-ui` |
| SQL queries or migrations in `db/` | `direnv exec . just generate-db` |
| Configuration structs in `internal/config` | `direnv exec . just nix-generate` and `direnv exec . just docs-generate` |

Use `direnv exec . just generate` to run all generators. Do not run bare `sqlc generate`: the repository target also applies the required schema-rewriting patch. Preserve `/* TEMPLATE: schema */` markers in SQL and review generated diffs before submitting.

### Browser schema generation with TypeScript 7

The installed `openapi-typescript` 7.13.0 requires the TypeScript 5 compiler API.
The UI uses TypeScript 7.0.2, whose package does not expose that API; the current
`generate:api` recipe fails with `createKeywordTypeNode` undefined. Do not patch
the generated schema or downgrade the application compiler to work around this.

After generating OpenAPI, run the browser generator in an isolated, pinned tool
directory. The install below uses the package registry; approve network access
first in restricted environments. Run from the repository root:

```bash
tools=$(mktemp -d "${TMPDIR:-/tmp}/teldrive-contract-tools.XXXXXXXX")
trap 'rm -rf -- "$tools"' EXIT
cat > "$tools/package.json" <<'JSON'
{"private":true,"dependencies":{"openapi-typescript":"7.13.0","typescript":"5.9.3"}}
JSON
(cd "$tools" && bun install --ignore-scripts)
"$tools/node_modules/.bin/openapi-typescript" \
  openapi/teldrive.openapi.yaml -o ui/src/api/schema.ts
```

This keeps the application manifest and lockfile unchanged. The temporary tool
directory is removed on shell exit. It does not repair the normal generation recipe.
For reproducible offline Go generation, use the module-selected ogen command
instead of the recipe's `@latest`, after its exact dependencies are cached:

```bash
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off \
  go run github.com/ogen-go/ogen/cmd/ogen \
  --config ogen.yml --target internal/api/gen --package gen --clean \
  openapi/teldrive.openapi.yaml
```

Documentation pages live in `docs/content/docs/`. Preview them with `direnv exec . just docs-dev` and validate with `direnv exec . just docs-build`.

## Lifecycle candidate: coordinated contribution and proof

The C1–C6 lifecycle candidate has partial isolated runtime proof, not deployment or phase
acceptance. Missing genuine legacy provenance and open proof obligations still block closure.
Its operator contract is documented in [Uploads and events](docs/content/docs/configuration/uploads-and-events.mdx).
The contract families are C1 durable remote-attempt ledger, C2 short hierarchy
transactions/lock order, C3 remote/SQL failure and bounded recovery, C4 HTTP/importer/client
semantics, C5 events/generation/release compatibility, and C6 isolated migration/recovery proof.
No intermediate source plan is a supported mixed-version release.

### Required caller reconciliation

Review actual callers and generated consumers together, not just interface declarations:

| Boundary | Required callers and invariants |
| --- | --- |
| Receipt composition | `internal/app/app.go` constructs one lifecycle policy for catalog, uploads, fileops, transfer and Runtime. `BindReceiptEnqueuer` binds the real Runtime once before serving/workers; nil/rebinding fails. |
| Caller transaction | `ReceiptEnqueuer.EnqueueReceiptCleanupTx(ctx, tx, ReceiptCleanupInput)` inserts actual River work with positive owner metadata and returns actual positive `Job.ID`. The shared lifecycle boundary partitions receipt sets into batches of at most1000, inserts and assigns every batch in the same caller transaction, and rolls all batches back on failure. `AssignReceiptCleanupJob` preserves first `job_id` and current `wakeup_job_id`; no receipt is truncated. |
| Remote attempts | Pipeline upload and fileops Copy reserve before send and inspect both returned receipt and error. `Reserve`, `Observe`, `BindTx`, `StageCleanupTx`, `ClaimDelete` and exact `AcknowledgeDelete` share ownership/reference/fence rules. Unknown transport outcome is not an owned receipt or confirmed absence. |
| Destructive calls | Upload compensation, copy compensation/purge, upload cleanup, orphan cleanup and receipt workers use the shared ledger. Only committed claims reach `DeleteMessages`; both destructive request classes bypass transient and flood-wait retries. Four lifetime message-delete invocations are distinct from one intentional channel-delete invocation and from lower transport delivery. |
| Hierarchy mutation | Trash, purge, replacement and destination terminalization acquire copy operations before sessions/physical rows. Lock order is owner → admission → import batch/pins/attempt → copy operation → upload session → signed-sorted channels → sorted physical messages → receipts → catalog. No remote RPC spans a SQL transaction. Published/ambiguous winners never become compensation candidates. |
| Completion | `Complete` captures exact immutable part proof and retires only transferred aliases in its publication transaction. Direct GetPart, ClaimPart, waiters and ListParts retain completed replay after retirement. Full current body EOF/hash precedes current-result/ancestry/access recheck, not metadata-only success. |
| Durable import | Runtime Q pins original destination before source IO; actual batch A atomically records folders, children and InsertManyTx owner metadata. `ReserveImportAttemptTx` selects immutable UUID/`selected_create` at S; `SelectedImportCreateInput` feeds common Create/B and selected worker replay. Current root/encryption/key policy can veto, never renegotiate geometry/key/request. Cancelled/removed River work retains identity. |
| Physical channels | `LockChannelWritesTx` and `RequireWritableChannelsTx` cover Resolve/ResolveMany hints, Select, Sync, Create/rollover and authoritative Claim/Reserve/Observe/Store/Complete/Copy. Discover uses `FilterDiscoverable`. All-owner references and nondeleted reservations fence explicit Delete before RPC, even with no file/session/message ID. |
| Channel finalization | Delete commits sparse closing intent, performs one authorized RPC outside SQL, then exact-token `FinalizeChannelDeletionIntent` and `DeleteChannelRegistrationsForIntent`. Failure/ambiguity stays closed; deleted tombstones survive registration removal. Automatic allocation compensation must not return, including pre-bot failures with known remote ID. |
| Recovery jobs | Receipt cleanup retains first owner report/current wakeup IDs, safe primary/cleanup causes and unknown/exhausted dispositions through River removal. Retry refuses unresolved/exhausted debt with existing `409/conflict`. Discovery fixes cutoff and advances 1000-row `(created_at,id)` pages through retained rows; incomplete purge remains incomplete. |
| Periodic ownership | `requiredReceiptPeriodicSpec` supplies Start, catalog and Reset. Start uses actual Upsert and validates every field. All Create/Update/replace/Pause/Resume/Delete writers protect reserved ID/kind aliases. Reset uses one transaction, retains required row, reconciles with UpsertTx, and commits all defaults or retains prior catalog on interruption. Preserve unchanged due time. |
| Generated routes | Existing upload/copy idempotency keys and actor/share access remain authoritative. Periodic mutations authenticate then require admin/owner; system changes conflict even for admins. `jobs.tsp` carries mutation 403/409 and `channels.tsp` carries Sync409; regenerate existing OpenAPI/ogen/browser outputs, never invent DTOs or hand-patch generated methods. |
| Browser | Owner queue retains private process-local File/task/key/payload/session across retry. Public upload has only action-local File and no retained retry UI/persistent resume. Preserve original source bytes, current authorization, manifests and public error privacy. |
| Rclone identity | One original logical source/requested primary target carrier survives server/manual/Update/Put/PutUnchecked/PutStream/chunk/Rcat paths. Wire metadata, comparison FileID and effective transformed target remain separate. Never recover original intent with an inverse transform. |
| Rclone consumers | `operations.moveOrCopyFile`, `syncCopyMove.pairChecker` and `syncCopyMove.SrcOnly` pass original requested primary target to `CompareOrCopyDest(..., destinationRemote)`. Marked admission error stops before equality, fallback, backup/FixCase, rename/upload enqueue and immediate/ordered source deletion. Preserve original cause and exactly-once checking accounting where owned. |
| Movement/discovery | Destination NewObject/ListP/ListR/March guards precede path/cache preparation. `operations.move`, MoveCaseInsensitive, MoveBackupDir, operations.DirMove, sync.MoveDir and backend Move/DirMove reuse the same selector/carrier before destructive shortcuts. A marked NotFound is not genuine absence; Item.reload must retain dirty evidence before Open/refill/queue/clean acknowledgement. |
| Retry lifetime | Durable VFS generation wins over actual RC NewJob identity, effective CLI invocation, inherited transient scope and one-call Background Copy. CopyConfig preserves actual job lookup and intent. Raw rewindable Rcat scopes its existing inner Retry; cmd.Run(false), nonbuffered one-shot streams and independent calls keep their original limits. |
| Cache writes and RC completion | Acknowledged cache writes persist actual present/sparse-zero ranges and logical size with the dirty generation before returning, including later writes. Actual RC job liveness is checked before scope lookup and after registration/readiness; completion order is not inferred from job IDs. Missing identity lookup fails closed, while a successful no-job lookup preserves independent jobless behavior. |

Do not broaden terminal/fatal classification to every conflict. Lost ACK and truncated
responses must preserve identity before a terminal error can be decoded. Later decoded
terminal failure stops actual command retries even after another file's transient failure.
Retained uncertainty must not fall through to fresh allocation, equality acknowledgement,
fallback upload or source removal. Ordinary unadmitted fallback and transient controls
must still work.

### Events, recovery and evidence

Keep existing SSE vocabulary and payloads. Emit `file.created`, `file.trashed`,
`file.deletion_pending`, `file.restored`, `file.purged` and `upload.aborted` only for their
actual committed transitions. Same-key creation/copy replay emits no second publication;
an import batch terminalized before session creation must not fabricate `upload.aborted`.
Admission and pin persistence add no stream. Compare scoped event deltas, not global counts.

Old writable service is unsafe even with workers disabled because inline compensation
bypasses fences. Coordinate service, all workers, browser and custom rclone. Do not allow
old binaries/cache writers to touch candidate evidence. Evidence-bearing recovery is
forward-only with fence-aware code. Down refuses every retained receipt/operation,
request admission, batch/pin/attempt, channel intent or upload terminal reason.
Empty-evidence Down/re-up proves schema behavior only; it does not approve old-binary use.
Legacy detached/root sessions, unassociated imports and dirty cache can be conservatively
unavailable. Unknown destination is not proved deletion; new explicit intent is separate.
Never delete evidence to manufacture an empty downgrade or a successful purge.

Root alone owns coordinated generation, formatting, executable checks, isolated resources,
exact observed validation and proof bookkeeping for this phase. Earlier setup/check commands
are general contributor instructions, not authority to run them during this source-only task.
Preserve original research and historical evidence. Do not save private tool paths, DSNs,
keys, credentials, environment values, source paths or dumps in documentation or Git.

Required proof includes actual generated HTTP/SQL, checksum-bound CLI outer/mixed retries,
raw-stdin inner retries, real RC dispatch, durable VFS child-process restart, full independent
source/winner/foreign SHA-256 and real dump/restore using pinned original source.
The genuine old Create-under-parent → old Purge → dump/restore → candidate migration →
original empty Complete refusal remains pending root execution. Protocol/model tests cannot
replace that chain. Restore must retain roles/extensions/configuration and synthetic key
handles as well as database state; this is not production restore acceptance.

Record selected test names/counts and actual PASS/FAIL/NOT-RUN without narrowing predicates.
Missing selection, skipped test, unresolved generated caller, unavailable tool/resource or
unrun restore is not PASS. Current required gate-name counts are G1=10, G7b=26, G7c=3,
G8=27, G9=12, G10h=65, G11=22 and G12a=31; root reconciles original captures once.
Browser interception proves consumer behavior, not SQL/restore authority. No authored test
establishes Plex/original-quality, Windows execution, live Telegram durability, performance,
production schema/restore, release readiness or permission to restart services.


## Submit a pull request

- Explain what changed and why; link related issues.
- List the checks you ran and disclose any failures or checks you could not run.
- Include screenshots for visual changes when useful.
- Exclude secrets, unrelated changes, and local build or test artifacts.
