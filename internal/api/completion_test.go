package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
)

func TestClassifyResponse(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name        string
		ctx         context.Context
		status      int
		written     int64
		expected    int64
		hasExpected bool
		want        string
	}{
		{name: "full body", ctx: context.Background(), status: http.StatusOK, written: 100, expected: 100, hasExpected: true, want: CompletionComplete},
		{name: "short 206", ctx: context.Background(), status: http.StatusPartialContent, written: 3, expected: 100, hasExpected: true, want: CompletionPartial},
		{name: "no first byte", ctx: context.Background(), status: http.StatusOK, written: 0, expected: 100, hasExpected: true, want: CompletionNoFirstByte},
		{name: "empty file", ctx: context.Background(), status: http.StatusOK, expected: 0, hasExpected: true, want: CompletionComplete},
		{name: "unknown length", ctx: context.Background(), status: http.StatusOK, written: 50, want: CompletionComplete},
		{name: "not modified", ctx: context.Background(), status: http.StatusNotModified, expected: 0, hasExpected: true, want: CompletionComplete},
		{name: "no content", ctx: context.Background(), status: http.StatusNoContent, expected: 0, hasExpected: true, want: CompletionComplete},
		{name: "cancelled", ctx: cancelled, status: http.StatusOK, written: 0, expected: 100, hasExpected: true, want: CompletionClientCancel},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyResponse(test.ctx, test.status, test.written, test.expected, test.hasExpected); got != test.want {
				t.Fatalf("ClassifyResponse() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestClassifyStream(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name     string
		ctx      context.Context
		written  int64
		expected int64
		err      error
		want     string
	}{
		{name: "complete", ctx: context.Background(), written: 100, expected: 100, want: CompletionComplete},
		{name: "short", ctx: context.Background(), written: 3, expected: 100, err: errors.New("upstream read failed"), want: CompletionBackendError},
		{name: "no bytes backend", ctx: context.Background(), written: 0, expected: 100, err: errors.New("upstream read failed"), want: CompletionNoFirstByte},
		{name: "cancelled context", ctx: cancelled, written: 0, expected: 100, err: context.Canceled, want: CompletionClientCancel},
		{name: "broken pipe", ctx: context.Background(), written: 2, expected: 100, err: errors.Join(io.ErrClosedPipe, syscall.EPIPE), want: CompletionClientCancel},
		{name: "connection reset", ctx: context.Background(), written: 2, expected: 100, err: syscall.ECONNRESET, want: CompletionClientCancel},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyStream(test.ctx, test.written, test.expected, test.err); got != test.want {
				t.Fatalf("ClassifyStream() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestErrorKind(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", want: ""},
		{name: "cancelled", err: context.Canceled, want: ErrorKindCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: ErrorKindDeadlineExceeded},
		{name: "broken pipe", err: syscall.EPIPE, want: ErrorKindBrokenPipe},
		{name: "connection reset", err: syscall.ECONNRESET, want: ErrorKindConnectionReset},
		{name: "unknown", err: errors.New("dial https://user:secret@example.invalid/?token=planted"), want: ErrorKindUpstream},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ErrorKind(test.err); got != test.want {
				t.Fatalf("ErrorKind() = %q, want %q", got, test.want)
			}
		})
	}
}

type captureRecords struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureRecords) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureRecords) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}
func (h *captureRecords) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureRecords) WithGroup(string) slog.Handler      { return h }

type shortCatalog struct {
	file  *sqlcgen.File
	parts []*sqlcgen.FilePart
}

func (c shortCatalog) Get(context.Context, int64, uuid.UUID) (*sqlcgen.File, error) {
	return c.file, nil
}

func (c shortCatalog) Parts(context.Context, int64, uuid.UUID) ([]*sqlcgen.FilePart, error) {
	return c.parts, nil
}

type partialErrorReader struct {
	data []byte
	err  error
}

func (r *partialErrorReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func (r *partialErrorReader) Close() error { return nil }

type shortStorage struct{}

func (shortStorage) Upload(context.Context, telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("unused")
}

func (shortStorage) OpenRange(ctx context.Context, _ telegramstore.RangeRequest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &partialErrorReader{data: []byte("abc"), err: errors.New("upstream read failed")}, nil
}

func (shortStorage) DeleteMessages(context.Context, int64, int64, []int64) error { return nil }

func (shortStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("unused")
}

func (shortStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, errors.New("unused")
}

func (shortStorage) DeleteChannel(context.Context, int64, int64) error { return nil }

func TestStreamFailureLogFields(t *testing.T) {
	file := &sqlcgen.File{
		ID:        pgtype.UUID{Bytes: uuid.New(), Valid: true},
		UserID:    1,
		Name:      "movie.mkv",
		Kind:      sqlcgen.FileKindFile,
		Status:    sqlcgen.FileStatusActive,
		Size:      pgtype.Int8{Int64: 100, Valid: true},
		MimeType:  pgtype.Text{String: "video/x-matroska", Valid: true},
		HashValue: pgtype.Text{String: "etag", Valid: true},
		ModTime:   pgtype.Timestamptz{Time: time.Now(), Valid: true},
	}
	part := &sqlcgen.FilePart{
		PartNo: 1, ChannelID: 10, MessageID: 20,
		PlainSize:  pgtype.Int8{Int64: 100, Valid: true},
		StoredSize: pgtype.Int8{Int64: 100, Valid: true},
	}
	downloader := transfer.NewDownloader(shortCatalog{file: file, parts: []*sqlcgen.FilePart{part}}, shortStorage{}, nil)
	handler := NewRawHandler(&Handler{Downloader: downloader})

	for _, test := range []struct {
		name       string
		ctx        context.Context
		level      slog.Level
		completion string
		errorKind  string
		bytes      int64
	}{
		{name: "backend failure", ctx: context.Background(), level: slog.LevelError, completion: CompletionBackendError, errorKind: ErrorKindUpstream, bytes: 3},
		{name: "client cancellation", ctx: cancelledContext(), level: slog.LevelWarn, completion: CompletionClientCancel, errorKind: ErrorKindCanceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := &captureRecords{}
			previous := slog.Default()
			slog.SetDefault(slog.New(capture))
			t.Cleanup(func() { slog.SetDefault(previous) })

			ctx := context.WithValue(test.ctx, middleware.RequestIDKey, "req-stream")
			response := httptest.NewRecorder()

			if err := handler.streamFile(ctx, response, 1, uuid.New(), file, gen.OptString{}, gen.OptETag{}, false); err != nil {
				t.Fatalf("streamFile() error = %v", err)
			}

			if len(capture.records) != 1 {
				t.Fatalf("records = %d, want 1", len(capture.records))
			}
			record := capture.records[0]
			if record.Level != test.level || record.Message != "api.stream_failed" {
				t.Fatalf("record = (%s, %q), want (%s, %q)", record.Level, record.Message, test.level, "api.stream_failed")
			}
			attrs := map[string]any{}
			record.Attrs(func(attr slog.Attr) bool {
				attrs[attr.Key] = attr.Value.Any()
				return true
			})
			if attrs["stage"] != "stream" || attrs["request_id"] != "req-stream" {
				t.Fatalf("attrs = %#v", attrs)
			}
			if attrs["bytes"] != test.bytes || attrs["expected_bytes"] != int64(100) || attrs["offset"] != int64(0) {
				t.Fatalf("attrs = %#v", attrs)
			}
			if attrs["completion"] != test.completion || attrs["error_kind"] != test.errorKind {
				t.Fatalf("attrs = %#v", attrs)
			}
			if value, ok := attrs["file_id"].(string); !ok || value == "" {
				t.Fatalf("file_id = %#v", attrs["file_id"])
			}
			if _, ok := attrs["error"]; ok {
				t.Fatalf("raw error attribute present: %#v", attrs)
			}
			for _, value := range attrs {
				if text, ok := value.(string); ok && strings.Contains(text, "upstream read failed") {
					t.Fatalf("free-form error text leaked: %#v", attrs)
				}
			}
		})
	}
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
