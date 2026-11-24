package api

import (
	"context"
	"errors"
	"net/http"
	"syscall"
)

// Completion categories describe how much of the declared body the handler
// wrote. They never claim the client received it.
const (
	CompletionComplete     = "complete"
	CompletionPartial      = "partial"
	CompletionNoFirstByte  = "no_first_byte"
	CompletionClientCancel = "client_cancel"
	CompletionBackendError = "backend_error"
)

// Stream error kinds are a closed set so log lines carry no free-form text.
const (
	ErrorKindCanceled         = "canceled"
	ErrorKindDeadlineExceeded = "deadline_exceeded"
	ErrorKindBrokenPipe       = "broken_pipe"
	ErrorKindConnectionReset  = "connection_reset"
	ErrorKindUpstream         = "upstream"
)

// ClassifyResponse categorises an access log record from handler-written bytes
// and the declared Content-Length. expected is meaningful only when hasExpected.
func ClassifyResponse(ctx context.Context, status int, written, expected int64, hasExpected bool) string {
	if ctx.Err() != nil {
		return CompletionClientCancel
	}
	switch status {
	case http.StatusNotModified, http.StatusNoContent:
		return CompletionComplete
	}
	if !hasExpected || written >= expected {
		return CompletionComplete
	}
	if written == 0 {
		return CompletionNoFirstByte
	}
	return CompletionPartial
}

// ClassifyStream categorises a stream failure or completion from copied bytes.
func ClassifyStream(ctx context.Context, written, expected int64, err error) string {
	if err == nil {
		if expected > 0 && written < expected {
			return CompletionPartial
		}
		return CompletionComplete
	}
	if ctx.Err() != nil || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return CompletionClientCancel
	}
	if written == 0 {
		return CompletionNoFirstByte
	}
	return CompletionBackendError
}

// ErrorKind maps err into a closed set without exposing its message.
func ErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return ErrorKindCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorKindDeadlineExceeded
	case errors.Is(err, syscall.EPIPE):
		return ErrorKindBrokenPipe
	case errors.Is(err, syscall.ECONNRESET):
		return ErrorKindConnectionReset
	default:
		return ErrorKindUpstream
	}
}
