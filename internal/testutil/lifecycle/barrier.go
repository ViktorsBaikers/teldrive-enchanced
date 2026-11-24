package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrFixtureClosed = errors.New("synthetic lifecycle fixture closed")
	ErrProcessLost   = errors.New("synthetic process lost after durable boundary; bypass normal compensation")
	ErrResponseLost  = errors.New("synthetic response lost after durable boundary")
)

const barrierTimeout = 5 * time.Second

// Barrier is one reached/release boundary with bounded cancellation and teardown.
// Result is returned only after release; a post-commit error never means rollback.
type Barrier struct {
	reached      chan struct{}
	released     chan struct{}
	closed       chan struct{}
	onceReached  sync.Once
	onceReleased sync.Once
	onceClosed   sync.Once
	result       error
}

func NewBarrier(t testing.TB, result error) *Barrier {
	t.Helper()
	b := &Barrier{reached: make(chan struct{}), released: make(chan struct{}), closed: make(chan struct{}), result: result}
	t.Cleanup(b.Close)
	return b
}

func (b *Barrier) Pause(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, barrierTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-b.closed:
		return ErrFixtureClosed
	default:
	}
	b.onceReached.Do(func() { close(b.reached) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closed:
		return ErrFixtureClosed
	case <-b.released:
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-b.closed:
			return ErrFixtureClosed
		default:
		}
		return b.result
	}
}

func (b *Barrier) Wait(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, barrierTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closed:
		return ErrFixtureClosed
	case <-b.reached:
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-b.closed:
			return ErrFixtureClosed
		default:
		}
		return nil
	}
}

func (b *Barrier) Release() { b.onceReleased.Do(func() { close(b.released) }) }
func (b *Barrier) Close()   { b.onceClosed.Do(func() { close(b.closed) }) }

// Hooks names only B2 boundaries. Nil means no injected fault at that boundary.
// Tests call these at actual domain/transaction seams, never simulate SQL state.
// For trash/restore races, release CopyPart only after both real commits return.
type Hooks struct {
	UploadCreateResponse            *Barrier
	UploadTerminalResponse          *Barrier
	CopyTerminalResponse            *Barrier
	CopySuccessResponse             *Barrier
	CopyOperationReserved           *Barrier
	CopyOperationBeforeCommit       *Barrier
	ObserveBeforeCommit             *Barrier
	ObserveCommittedBeforeStage     *Barrier
	BindBeforeCommit                *Barrier
	BindCommitted                   *Barrier
	DeleteClaimCommitted            *Barrier
	DeleteACKBeforeCommit           *Barrier
	DeleteACKCommitted              *Barrier
	RecoveryBeforeRestart           *Barrier
	IndependentReadback             *Barrier
	ImportQCommittedBeforePreflight *Barrier
	ImportPreflight                 *Barrier
	ImportAFolderCreated            *Barrier
	ImportAChildInserted            *Barrier
	ImportABeforeCommit             *Barrier
	ImportACommitted                *Barrier
	ImportSBeforeCommit             *Barrier
	ImportSCommitted                *Barrier
	ImportBBeforeCommit             *Barrier
	ImportBCommitted                *Barrier
	CopyTerminalBeforeCommit        *Barrier
	CopyTerminalCommitted           *Barrier
}

// Commit helpers use real caller-owned pgx transactions. A failure before COMMIT
// rolls back; after COMMIT it returns the injected loss without compensating.
// Independent database rereads, not these hooks, establish the durable outcome.
func commitBoundary(ctx context.Context, tx pgx.Tx, before, after *Barrier) error {
	ctx, cancel := context.WithTimeout(ctx, barrierTimeout)
	defer cancel()
	if err := pause(ctx, before); err != nil {
		rollbackCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), barrierTimeout)
		defer stop()
		return errors.Join(err, tx.Rollback(rollbackCtx))
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return pause(ctx, after)
}

func (h Hooks) CommitCopyReservation(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.CopyOperationBeforeCommit, h.CopyOperationReserved)
}
func (h Hooks) CommitObserve(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.ObserveBeforeCommit, h.ObserveCommittedBeforeStage)
}
func (h Hooks) CommitBind(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.BindBeforeCommit, h.BindCommitted)
}
func (h Hooks) CommitDeleteClaim(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, nil, h.DeleteClaimCommitted)
}
func (h Hooks) CommitDeleteACK(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.DeleteACKBeforeCommit, h.DeleteACKCommitted)
}
func (h Hooks) CommitImportQ(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, nil, h.ImportQCommittedBeforePreflight)
}
func (h Hooks) Preflight(ctx context.Context, open func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, barrierTimeout)
	defer cancel()
	if err := pause(ctx, h.ImportPreflight); err != nil {
		return err
	}
	return open(ctx)
}
func (h Hooks) AfterImportFolder(ctx context.Context) error {
	return pause(ctx, h.ImportAFolderCreated)
}
func (h Hooks) AfterImportChild(ctx context.Context) error { return pause(ctx, h.ImportAChildInserted) }
func (h Hooks) CommitImportA(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.ImportABeforeCommit, h.ImportACommitted)
}
func (h Hooks) CommitImportS(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.ImportSBeforeCommit, h.ImportSCommitted)
}
func (h Hooks) CommitImportB(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.ImportBBeforeCommit, h.ImportBCommitted)
}
func (h Hooks) CommitCopyTerminal(ctx context.Context, tx pgx.Tx) error {
	return commitBoundary(ctx, tx, h.CopyTerminalBeforeCommit, h.CopyTerminalCommitted)
}
func (h Hooks) BeforeRestart(ctx context.Context) error { return pause(ctx, h.RecoveryBeforeRestart) }
func (h Hooks) BeforeIndependentReadback(ctx context.Context) error {
	return pause(ctx, h.IndependentReadback)
}

// BlockingQuerier is satisfied by pgxpool.Pool and a separate pgx connection.
// Never use the blocked transaction's connection for the observation query.
type BlockingQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// WaitForBlocking proves this exact waiter depends on this exact blocker.
// A timeout is a failed observation, not permission to choose a race winner.
func WaitForBlocking(ctx context.Context, observer BlockingQuerier, waiter, blocker uint32) error {
	ctx, cancel := context.WithTimeout(ctx, barrierTimeout)
	defer cancel()
	if waiter == 0 || blocker == 0 || waiter == blocker {
		return errors.New("invalid SQL blocking identities")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var blocked bool
		if err := observer.QueryRow(ctx, "SELECT $1::integer = ANY(pg_blocking_pids($2::integer))", int32(blocker), int32(waiter)).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			return nil
		}
		runtime.Gosched()
	}
}

func (h Hooks) UploadCreateHandler(next http.Handler) http.Handler {
	return lostResponse(next, h.UploadCreateResponse, 201, 0)
}
func (h Hooks) UploadTerminalHandler(next http.Handler) http.Handler {
	return lostResponse(next, h.UploadTerminalResponse, http.StatusConflict, 1)
}
func (h Hooks) CopyTerminalHandler(next http.Handler) http.Handler {
	return lostResponse(next, h.CopyTerminalResponse, http.StatusConflict, 0)
}
func (h Hooks) CopySuccessHandler(next http.Handler) http.Handler {
	return lostResponse(next, h.CopySuccessResponse, http.StatusCreated, 0)
}

// Execute the real handler first, then sever a real HTTP/1 connection. No route
// interception invents a server commit or terminal outcome. Content-Length stays
// complete so truncated delivery fails at the client instead of decoding as EOF.
func lostResponse(next http.Handler, gate *Barrier, status, keep int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gate == nil {
			next.ServeHTTP(w, r)
			return
		}
		recorder := httptest.NewRecorder()
		next.ServeHTTP(recorder, r)
		if recorder.Code != status {
			for key, values := range recorder.Header() {
				w.Header()[key] = append([]string(nil), values...)
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
			return
		}
		if err := gate.Pause(r.Context()); err != nil && !errors.Is(err, ErrResponseLost) && !errors.Is(err, ErrProcessLost) {
			panic(http.ErrAbortHandler)
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			panic(http.ErrAbortHandler)
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			panic(http.ErrAbortHandler)
		}
		defer conn.Close()
		if keep == 0 {
			return
		}
		_ = conn.SetWriteDeadline(time.Now().Add(barrierTimeout))
		body := recorder.Body.Bytes()
		if len(body) <= keep {
			panic(http.ErrAbortHandler)
		}
		_, err = fmt.Fprintf(rw, "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %s\r\nConnection: close\r\n\r\n", status, http.StatusText(status), strconv.Itoa(len(body)))
		if err != nil {
			return
		}
		if _, err := rw.Write(body[:keep]); err != nil {
			return
		}
		_ = rw.Flush()
	})
}
