package app

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5/middleware"

	api "github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/logging"
)

type recordHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

func newTestRouteResolver(t *testing.T) api.RouteResolver {
	t.Helper()
	server, err := api.NewServer(nil, nil)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}

func recordAttrs(record slog.Record) map[string]any {
	attrs := map[string]any{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}

func TestHTTPRequestLoggerLevelsAndAttributes(t *testing.T) {
	t.Parallel()

	routes := newTestRouteResolver(t)
	for _, test := range []struct {
		name   string
		status int
		level  slog.Level
	}{
		{name: "success", status: http.StatusOK, level: slog.LevelInfo},
		{name: "client error", status: http.StatusBadRequest, level: slog.LevelWarn},
		{name: "server error", status: http.StatusInternalServerError, level: slog.LevelError},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := &recordHandler{}
			logger := slog.New(handler)
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
			})
			wrapped := middleware.RequestID(httpRequestLogger(logger, nil, routes)(next))
			request := httptest.NewRequest(http.MethodGet, "/api/v1/files?limit=10&q=value", nil)
			request.RemoteAddr = "127.0.0.1:1234"
			request.Header.Set("User-Agent", "test-agent")
			response := httptest.NewRecorder()

			wrapped.ServeHTTP(response, request)

			if len(handler.records) != 1 {
				t.Fatalf("records = %d, want 1", len(handler.records))
			}
			record := handler.records[0]
			if record.Level != test.level || record.Message != "http.request" {
				t.Fatalf("record = (%s, %q), want (%s, %q)", record.Level, record.Message, test.level, "http.request")
			}
			attrs := recordAttrs(record)
			if attrs["status"] != int64(test.status) || attrs["route"] != "/v1/files" || attrs["operation"] != "ListFiles" {
				t.Fatalf("attrs = %#v", attrs)
			}
			if attrs["stage"] != "request" || attrs["query.limit"] != "10" || attrs["query_redacted_count"] != int64(1) {
				t.Fatalf("attrs = %#v", attrs)
			}
			if _, ok := attrs["path"]; ok {
				t.Fatalf("raw path attribute present: %#v", attrs)
			}
			if _, ok := attrs["query"]; ok {
				t.Fatalf("raw query attribute present: %#v", attrs)
			}
			if attrs["request_id"] == "" {
				t.Fatalf("request_id is empty: %#v", attrs)
			}
		})
	}
}

func TestHTTPRequestLoggerSkipsUIRequests(t *testing.T) {
	t.Parallel()

	handler := &recordHandler{}
	logger := slog.New(handler)
	wrapped := httpRequestLogger(logger, nil, newTestRouteResolver(t))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/files", nil))

	if len(handler.records) != 0 {
		t.Fatalf("records = %d, want 0", len(handler.records))
	}
}

func TestHTTPRequestLoggerRedactsHostileInput(t *testing.T) {
	t.Parallel()

	const (
		shareToken = "PLANTED-SHARE-TOKEN"
		fileName   = "[PLANTED] movie name.mkv"
		ticket     = "PLANTED-TICKET"
		cursor     = "PLANTEDBASE64CURSOR=="
		search     = "PLANTED-SEARCH-TEXT"
	)
	target := "/api/v1/public/shares/" + shareToken + "/content/%5BPLANTED%5D%20movie%20name.mkv" +
		"?ticket=" + ticket + "&cursor=" + cursor + "&search=" + search + "&limit=25"
	planted := []string{shareToken, fileName, "PLANTED", ticket, cursor, search}

	routes := newTestRouteResolver(t)

	t.Run("json", func(t *testing.T) {
		var out bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&out, nil))
		wrapped := middleware.RequestID(httpRequestLogger(logger, nil, routes)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))

		wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))

		if strings.Count(out.String(), "\n") != 1 {
			t.Fatalf("want one record, got %q", out.String())
		}
		if !strings.Contains(out.String(), `"route":"/v1/public/shares/{token}/content/{fileName}"`) {
			t.Fatalf("generated route pattern missing: %q", out.String())
		}
		for _, literal := range planted {
			if strings.Contains(out.String(), literal) {
				t.Fatalf("planted literal %q leaked: %q", literal, out.String())
			}
		}
	})

	t.Run("text", func(t *testing.T) {
		var out bytes.Buffer
		logger, err := logging.NewLogger(&out, "info", "text")
		if err != nil {
			t.Fatalf("NewLogger() error = %v", err)
		}
		wrapped := middleware.RequestID(httpRequestLogger(logger, nil, routes)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))

		wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))

		if strings.Count(out.String(), "\n") != 1 {
			t.Fatalf("want one record, got %q", out.String())
		}
		if !strings.Contains(out.String(), "route=/v1/public/shares/{token}/content/{fileName}") {
			t.Fatalf("generated route pattern missing: %q", out.String())
		}
		for _, literal := range planted {
			if strings.Contains(out.String(), literal) {
				t.Fatalf("planted literal %q leaked: %q", literal, out.String())
			}
		}
	})
}

func TestHTTPRequestLoggerRouteTemplate(t *testing.T) {
	t.Parallel()

	routes := newTestRouteResolver(t)
	for _, test := range []struct {
		name      string
		target    string
		route     string
		operation string
	}{
		{name: "api prefix", target: "/api/v1/files?limit=5", route: "/v1/files", operation: "ListFiles"},
		{name: "v1 prefix", target: "/v1/files", route: "/v1/files", operation: "ListFiles"},
		{name: "unmatched", target: "/api/v1/not-a-route", route: "unmatched", operation: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := &recordHandler{}
			wrapped := middleware.RequestID(httpRequestLogger(slog.New(handler), nil, routes)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})))

			wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, test.target, nil))

			if len(handler.records) != 1 {
				t.Fatalf("records = %d, want 1", len(handler.records))
			}
			attrs := recordAttrs(handler.records[0])
			if attrs["route"] != test.route || attrs["operation"] != test.operation {
				t.Fatalf("attrs = %#v, want route %q operation %q", attrs, test.route, test.operation)
			}
		})
	}
}

func TestHTTPRequestLoggerCompletion(t *testing.T) {
	t.Parallel()

	handler := &recordHandler{}
	wrapped := middleware.RequestID(httpRequestLogger(slog.New(handler), nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abc"))
	})))

	wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/files/abc/content", nil))

	if len(handler.records) != 1 {
		t.Fatalf("records = %d, want 1", len(handler.records))
	}
	attrs := recordAttrs(handler.records[0])
	if attrs["completion"] != "partial" || attrs["bytes"] != int64(3) || attrs["expected_bytes"] != int64(100) {
		t.Fatalf("attrs = %#v", attrs)
	}
}

func TestHTTPRequestLoggerResolvesClientIPThroughTrustedProxies(t *testing.T) {
	t.Parallel()

	security, err := newRequestSecurity([]string{"172.30.0.0/24", "10.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		remoteAddr string
		forwarded  []string
		want       string
	}{
		{name: "untrusted peer ignores header", remoteAddr: "203.0.113.9:4000", forwarded: []string{"198.51.100.1"}, want: "203.0.113.9"},
		{name: "trusted peer without header", remoteAddr: "172.30.0.11:50264", want: "172.30.0.11"},
		{name: "trusted peer uses forwarded client", remoteAddr: "172.30.0.11:50264", forwarded: []string{"213.35.116.148"}, want: "213.35.116.148"},
		{name: "spoofed leftmost entry ignored", remoteAddr: "172.30.0.11:50264", forwarded: []string{"1.2.3.4, 213.35.116.148"}, want: "213.35.116.148"},
		{name: "skips chained trusted hops across headers", remoteAddr: "172.30.0.11:50264", forwarded: []string{"1.2.3.4, 213.35.116.148", "10.0.0.1"}, want: "213.35.116.148"},
		{name: "malformed hop stops at last trusted", remoteAddr: "172.30.0.11:50264", forwarded: []string{"213.35.116.148, garbage"}, want: "172.30.0.11"},
		{name: "ipv4-mapped peer is trusted", remoteAddr: "[::ffff:172.30.0.11]:50264", forwarded: []string{"213.35.116.148"}, want: "213.35.116.148"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := &recordHandler{}
			wrapped := httpRequestLogger(slog.New(handler), security, nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			}))
			request := httptest.NewRequest(http.MethodGet, "/api/files", nil)
			request.RemoteAddr = test.remoteAddr
			for _, value := range test.forwarded {
				request.Header.Add("X-Forwarded-For", value)
			}

			wrapped.ServeHTTP(httptest.NewRecorder(), request)

			if len(handler.records) != 1 {
				t.Fatalf("records = %d, want 1", len(handler.records))
			}
			var ip any
			handler.records[0].Attrs(func(attr slog.Attr) bool {
				if attr.Key == "ip" {
					ip = attr.Value.Any()
				}
				return true
			})
			if ip != test.want {
				t.Fatalf("ip = %v, want %s", ip, test.want)
			}
		})
	}
}

func TestRequestIDMiddlewareRejectsUnsafeIDs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		header string
		keep   bool
	}{
		{name: "safe", header: "req-1.2_3", keep: true},
		{name: "newline", header: "req\nforged=1", keep: false},
		{name: "space", header: "req 1", keep: false},
		{name: "too long", header: strings.Repeat("a", 65), keep: false},
		{name: "empty", header: "", keep: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got string
			handler := requestIDMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				got = middleware.GetReqID(r.Context())
			}))
			request := httptest.NewRequest(http.MethodGet, "/api/v1/files", nil)
			if test.header != "" {
				request.Header.Set("X-Request-ID", test.header)
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if test.keep {
				if got != test.header {
					t.Fatalf("request id = %q, want %q", got, test.header)
				}
				return
			}
			if got == test.header || got == "" {
				t.Fatalf("request id = %q, want a generated replacement", got)
			}
			if response.Header().Get("X-Request-ID") != got {
				t.Fatalf("response header = %q, want %q", response.Header().Get("X-Request-ID"), got)
			}
		})
	}
}
