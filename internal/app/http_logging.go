package app

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	api "github.com/tgdrive/teldrive/v2/internal/api"
)

// safeQueryKeys lists query parameters whose values are enum, numeric, boolean
// or UUID shaped in the TypeSpec contract. Everything else is counted only.
const safeQueryKeys = " limit sort order kind status scope searchType category bookmarked download mode after types state type queue channelId parentId "

func safeQueryKey(key string) bool {
	return strings.Contains(safeQueryKeys, " "+key+" ")
}

func httpRequestLogger(logger *slog.Logger, security *requestSecurity, routes api.RouteResolver) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if security == nil {
		security = &requestSecurity{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/v1/") && !strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/health/") {
				next.ServeHTTP(w, r)
				return
			}
			started := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			defer func() {
				status := ww.Status()
				level := slog.LevelInfo
				if status >= http.StatusInternalServerError {
					level = slog.LevelError
				} else if status >= http.StatusBadRequest {
					level = slog.LevelWarn
				}

				route, operation := "unmatched", ""
				if routes != nil {
					if info, ok := routes.Route(r.Method, r.URL); ok {
						route, operation = info.Pattern, info.Operation
					}
				}
				expected, hasExpected := responseContentLength(ww.Header())

				attrs := []slog.Attr{
					slog.Int("status", status),
					slog.String("method", r.Method),
					slog.String("route", route),
					slog.String("operation", operation),
					slog.String("stage", "request"),
					slog.Int64("bytes", int64(ww.BytesWritten())),
					slog.Duration("latency", time.Since(started)),
					slog.String("request_id", middleware.GetReqID(r.Context())),
					slog.String("ip", security.clientIP(r)),
					slog.String("user_agent", r.UserAgent()),
					slog.String("completion", api.ClassifyResponse(r.Context(), status, int64(ww.BytesWritten()), expected, hasExpected)),
				}
				if hasExpected {
					attrs = append(attrs, slog.Int64("expected_bytes", expected))
				}
				redacted := 0
				for key, values := range r.URL.Query() {
					if safeQueryKey(key) {
						attrs = append(attrs, slog.String("query."+key, strings.Join(values, ",")))
						continue
					}
					redacted += len(values)
				}
				if redacted > 0 {
					attrs = append(attrs, slog.Int("query_redacted_count", redacted))
				}
				logger.LogAttrs(r.Context(), level, "http.request", attrs...)
			}()

			next.ServeHTTP(ww, r)
		})
	}
}

func responseContentLength(header http.Header) (int64, bool) {
	value := strings.TrimSpace(header.Get("Content-Length"))
	if value == "" {
		return 0, false
	}
	length, err := strconv.ParseInt(value, 10, 64)
	if err != nil || length < 0 {
		return 0, false
	}
	return length, true
}
