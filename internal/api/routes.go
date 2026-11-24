package api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
)

// RouteInfo names a matched request without exposing client path segments.
type RouteInfo struct {
	Pattern   string
	Operation string
}

// RouteResolver maps a request to the generated route template and operation.
type RouteResolver interface {
	Route(method string, u *url.URL) (RouteInfo, bool)
}

// Server exposes the generated route table alongside the finished handler.
type Server struct {
	http.Handler
	routes *gen.Server
}

// Route resolves u through the generated router. The /api prefix is stripped
// here because routeApplication strips it only after the access logger runs.
func (s *Server) Route(method string, u *url.URL) (RouteInfo, bool) {
	if s == nil || s.routes == nil || u == nil {
		return RouteInfo{}, false
	}
	target := *u
	target.Path = strings.TrimPrefix(target.Path, "/api")
	if target.RawPath != "" {
		target.RawPath = strings.TrimPrefix(target.RawPath, "/api")
	}
	route, ok := s.routes.FindPath(method, &target)
	if !ok {
		return RouteInfo{}, false
	}
	return RouteInfo{Pattern: route.PathPattern(), Operation: route.Name()}, true
}
