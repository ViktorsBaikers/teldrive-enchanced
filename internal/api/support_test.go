package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/authn"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestMapServiceErrorContextLifecycle(t *testing.T) {
	t.Parallel()

	if err := mapServiceError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled error = %v", err)
	}

	err := mapServiceError(context.DeadlineExceeded)
	var problem *Problem
	if !errors.As(err, &problem) {
		t.Fatalf("deadline error = %T, want *Problem", err)
	}
	if problem.Status != http.StatusGatewayTimeout || problem.Code != "request_timeout" {
		t.Fatalf("deadline problem = %#v", problem)
	}
}

func TestMapServiceErrorUploadConflicts(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{uploads.ErrPartBusy, "upload_part_busy"},
		{uploads.ErrPartConflict, "upload_part_conflict"},
		{uploads.ErrInvalidState, "upload_invalid_state"},
		{uploads.ErrNameConflict, "upload_name_conflict"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()
			err := mapServiceError(fmt.Errorf("upload failed: %w", tc.err))
			var problem *Problem
			if !errors.As(err, &problem) || problem.Status != http.StatusConflict || problem.Code != tc.code {
				t.Fatalf("mapServiceError() = %#v, want HTTP 409 with code %s", err, tc.code)
			}
			if !errors.Is(err, tc.err) {
				t.Fatal("mapping lost underlying conflict cause")
			}
		})
	}
}

func TestMapServiceErrorTelegramPasswordInvalid(t *testing.T) {
	t.Parallel()

	err := mapServiceError(authn.ErrPasswordInvalid)
	var problem *Problem
	if !errors.As(err, &problem) {
		t.Fatalf("password error = %T, want *Problem", err)
	}
	if problem.Status != http.StatusUnauthorized || problem.Code != "telegram_password_invalid" || problem.Message != "Telegram two-step password is invalid" {
		t.Fatalf("password problem = %#v", problem)
	}
}

func TestParseRangeResolvesFullContentLength(t *testing.T) {
	t.Parallel()

	got, partial, err := parseRange(gen.OptString{}, 557123)
	if err != nil {
		t.Fatalf("parseRange() error = %v", err)
	}
	if partial || got.Offset != 0 || got.Length != 557123 {
		t.Fatalf("parseRange() = (%+v, %t), want full content", got, partial)
	}
}

func TestContentDisposition(t *testing.T) {
	t.Parallel()

	if got := contentDisposition("report 2026.pdf", false); got != `inline; filename="report 2026.pdf"` {
		t.Fatalf("inline disposition = %q", got)
	}
	if got := contentDisposition("report 2026.pdf", true); got != `attachment; filename="report 2026.pdf"` {
		t.Fatalf("attachment disposition = %q", got)
	}
}

func TestErrorHandlerIgnoresClientCancellation(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/files", nil)
	ErrorHandler(request.Context(), response, request, context.Canceled)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want untouched recorder status %d", response.Code, http.StatusOK)
	}
	if response.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty", response.Body.String())
	}
}

func TestErrorHandlerRedactsCause(t *testing.T) {
	const (
		secret = "PLANTED-QUERY-SECRET"
		host   = "internal.planted.example"
		dir    = "/home/planted/absolute/path"
	)
	capture := &captureRecords{}
	previous := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(previous) })

	cause := fmt.Errorf("dial https://user:%s@%s%s?token=%s", secret, host, dir, secret)
	request := httptest.NewRequest(http.MethodGet, "/v1/files", nil)
	requestID := "req-5xx-planted"
	ctx := context.WithValue(request.Context(), middleware.RequestIDKey, requestID)
	response := httptest.NewRecorder()

	ErrorHandler(ctx, response, request, &Problem{Status: http.StatusInternalServerError, Code: "internal_error", Message: "request failed", Cause: cause})

	if len(capture.records) != 1 {
		t.Fatalf("records = %d, want 1", len(capture.records))
	}
	attrs := map[string]any{}
	capture.records[0].Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	if attrs["status"] != int64(http.StatusInternalServerError) || attrs["code"] != "internal_error" || attrs["request_id"] != requestID {
		t.Fatalf("attrs = %#v", attrs)
	}
	if attrs["error_kind"] != ErrorKindUpstream {
		t.Fatalf("error_kind = %#v, want %q", attrs["error_kind"], ErrorKindUpstream)
	}
	for _, value := range attrs {
		text, ok := value.(string)
		if !ok {
			continue
		}
		for _, planted := range []string{secret, host, dir} {
			if strings.Contains(text, planted) {
				t.Fatalf("planted literal %q leaked: %#v", planted, attrs)
			}
		}
	}

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if envelope.Error.Code != "internal_error" || envelope.Error.Message != "request failed" {
		t.Fatalf("envelope = %#v", envelope)
	}
}

func TestUploadSessionJSONPreservesSourceModTime(t *testing.T) {
	for _, stamp := range []string{
		"2026-11-16T23:48:18Z",
		"2026-11-16T23:48:18.677975Z",
		"2026-11-16T23:48:18.677975123+02:00",
	} {
		t.Run(stamp, func(t *testing.T) {
			modTime, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(&gen.UploadSession{ModTime: modTime})
			if err != nil {
				t.Fatal(err)
			}
			var client struct {
				ModTime time.Time `json:"modTime"`
			}
			if err := json.Unmarshal(body, &client); err != nil {
				t.Fatal(err)
			}
			if !client.ModTime.Equal(modTime) {
				t.Fatalf("client modTime = %s, want %s", client.ModTime.Format(time.RFC3339Nano), stamp)
			}
		})
	}
}
