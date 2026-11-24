//go:build integration

package api_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/api"
	"github.com/tgdrive/teldrive/v2/internal/authn"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/events"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/principal"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/shares"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

// b3OrderedAuthenticator adds the owner-incarnation snapshot the ordered
// stream requires; ordinary requests still use the synthetic B2 identity.
type b3OrderedAuthenticator struct {
	pairAuthenticator
	events *events.Service
}

func (a b3OrderedAuthenticator) proof(ctx context.Context) (principal.OrderedProof, error) {
	incarnation, err := a.events.OwnerIncarnation(ctx, a.owner)
	if err != nil {
		return principal.OrderedProof{}, err
	}
	now := time.Now().UTC()
	return principal.OrderedProof{
		UserID: a.owner, OwnerIncarnation: incarnation, CredentialKind: "api_key",
		CredentialID: uuid.NewString(), ValidatedAt: now, ExpiresAt: now.Add(time.Hour),
	}, nil
}

func (a b3OrderedAuthenticator) AuthenticateOrderedBearer(ctx context.Context, token string) (principal.OrderedProof, error) {
	if token != "test-token" {
		return principal.OrderedProof{}, errors.New("invalid synthetic token")
	}
	return a.proof(ctx)
}

func (a b3OrderedAuthenticator) AuthenticateOrderedAPIKey(ctx context.Context, key string) (principal.OrderedProof, error) {
	if key != "test-key" {
		return principal.OrderedProof{}, errors.New("invalid synthetic key")
	}
	return a.proof(ctx)
}

type b3Pair struct {
	*pairFixture
	inner   *httptest.Server
	events  *events.Service
	uploads *uploads.Service

	createGate      *b3Gate
	completeGate    *b3Gate
	completePreGate *b3Gate
	reconnectGate   *b3Gate
	reconnectIndex  int
	hold            bool
	holdCh          chan struct{}
	streamCancel    context.CancelFunc
	streamError     string
	conns           []b3StreamConn
	frames          []b3StreamFrame
}

type b3StreamConn struct {
	Cursor, Mode string
	Status       int
}

type b3StreamFrame struct {
	Conn   int
	Name   string
	ID     string
	Cursor string
}

type b3Gate struct {
	entered chan struct{}
	release chan struct{}
}

func newB3Gate() *b3Gate {
	return &b3Gate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *b3Gate) hold(ctx context.Context) bool {
	close(g.entered)
	select {
	case <-g.release:
		return true
	case <-ctx.Done():
		return false
	}
}

func newB3Pair(t *testing.T, binary string) *b3Pair {
	t.Helper()
	db := testpostgres.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	if _, err := db.Pool.Exec(ctx, `INSERT INTO teldrive.users(user_id) VALUES(1001),(1002);
		INSERT INTO teldrive.channels(channel_id,user_id,name,selected) VALUES(9001,1001,'synthetic',true)`); err != nil {
		t.Fatal("paired fixture seed failed")
	}
	directory, err := os.MkdirTemp("", "b3pair.")
	if err != nil {
		t.Fatal("paired private directory creation failed")
	}
	t.Cleanup(func() {
		if os.Getenv("B3_PAIR_KEEP") == "1" {
			fmt.Fprintf(os.Stderr, "B3_PAIR_KEEP directory=%s\n", directory)
			return
		}
		_ = os.RemoveAll(directory)
	})

	f := &pairFixture{
		t: t, ctx: ctx, pool: db.Pool, binary: binary, directory: directory,
		namespace: "__teldrive_audit_" + strings.ReplaceAll(uuid.NewString(), "-", ""), owner: 1001,
	}
	remote := &pairStorage{Storage: fixture.NewStorage(t, []fixture.ChannelScope{
		{ChannelID: 9001, Actors: []int64{1001, 1002}},
		{ChannelID: 9002, Actors: []int64{1001, 1002}},
	})}
	if err := remote.Seed(fixture.Message{UserID: 1002, ChannelID: 9002, MessageID: 700,
		Bytes: []byte("foreign retained payload"), Preserve: true}); err != nil {
		t.Fatal(err)
	}
	f.storage = remote
	f.catalog = catalog.NewService(db.Pool, nil)
	up := uploads.NewService(db.Pool)
	policy := bindAPIReceiptPolicy(t, db.Pool, remote, f.catalog, up)
	ch := channels.NewService(db.Pool, nil, channels.Config{PartLimit: 10000})
	f.ops, err = fileops.NewService(db.Pool, f.catalog, ch, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	f.shares, err = shares.NewService(db.Pool, f.catalog)
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := secureblob.New("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := authn.NewService(db.Pool, cipher, unusedTelegramLogin{}, authn.Config{
		SigningKey: "0123456789abcdef0123456789abcdef", Issuer: "test",
		AccessTokenTTL: time.Hour, RefreshTokenTTL: time.Hour, LoginFlowTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	eventService, err := events.NewService(db.Pool, slog.New(slog.NewTextHandler(io.Discard, nil)), events.Config{
		BatchSize: 100, MaxConnectionsPerUser: 8, Heartbeat: 50 * time.Millisecond,
		WriteTimeout: 2 * time.Second, TicketTTL: time.Minute, CleanupInterval: time.Hour,
		ConnectTimeout: 2 * time.Second, PingInterval: 50 * time.Millisecond,
		ReconnectMin: 10 * time.Millisecond, ReconnectMax: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eventService.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		_ = eventService.Close(closeCtx)
	})
	pipe := transfer.NewPipeline(up, ch, remote, nil, transfer.Config{Receipts: policy})
	handler := api.NewHandler(f.catalog, up, pipe, transfer.NewDownloader(f.catalog, remote, nil),
		health.NewService("fixture", db.Pool), 0, eventService).
		ConfigureDomains(auth, nil, ch, f.ops, f.shares, nil)
	generated, err := api.NewServer(handler, api.NewSecurity(b3OrderedAuthenticator{
		pairAuthenticator: pairAuthenticator{owner: 1001}, events: eventService,
	}))
	if err != nil {
		t.Fatal(err)
	}
	f.generated = generated
	p := &b3Pair{
		pairFixture: f, events: eventService, uploads: up,
		inner: httptest.NewServer(generated), holdCh: make(chan struct{}),
	}
	close(p.holdCh)
	t.Cleanup(p.inner.Close)
	f.server = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(f.server.Close)
	return p
}

func (p *b3Pair) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/v1/events" && r.URL.Query().Get("mode") == "ordered" {
		p.streamOrdered(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api")
	var body []byte
	if r.Method == http.MethodPost || r.Method == http.MethodPatch {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "synthetic body read", http.StatusInternalServerError)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	f := p.pairFixture
	f.mu.Lock()
	index := len(f.trace)
	f.trace = append(f.trace, pairRequest{
		Method: r.Method, Path: path, Query: r.URL.RawQuery,
		Key: r.Header.Get("Idempotency-Key"), Body: body,
	})
	f.mu.Unlock()

	gate := p.takeGate(r.Method, path)
	if gate != nil && !gate.hold(r.Context()) {
		return
	}
	response := httptest.NewRecorder()
	r.URL.Path = path
	p.generated.ServeHTTP(response, r)
	f.mu.Lock()
	f.trace[index].Status = response.Code
	f.mu.Unlock()
	if isCompletePath(path) {
		p.mu.Lock()
		post := p.completeGate
		p.completeGate = nil
		p.mu.Unlock()
		if post != nil && !post.hold(r.Context()) {
			return
		}
	}
	for key, values := range response.Header() {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.Code)
	_, _ = w.Write(response.Body.Bytes())
}

func isCompletePath(path string) bool {
	return strings.HasPrefix(path, "/v1/uploads/guarded/") && strings.HasSuffix(path, "/complete")
}

func (p *b3Pair) takeGate(method, path string) *b3Gate {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case method == http.MethodPost && path == "/v1/uploads/guarded":
		gate := p.createGate
		p.createGate = nil
		return gate
	case method == http.MethodPost && isCompletePath(path):
		gate := p.completePreGate
		p.completePreGate = nil
		return gate
	}
	return nil
}

func (p *b3Pair) setHold(hold bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hold = hold
	if hold {
		p.holdCh = make(chan struct{})
	} else {
		select {
		case <-p.holdCh:
		default:
			close(p.holdCh)
		}
	}
}

func (p *b3Pair) waitHold(ctx context.Context) error {
	p.mu.Lock()
	hold, ch := p.hold, p.holdCh
	p.mu.Unlock()
	if !hold {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *b3Pair) takeReconnectGate(cursor string) *b3Gate {
	if cursor == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	gate := p.reconnectGate
	p.reconnectGate = nil
	if gate != nil {
		p.reconnectIndex = len(p.conns) - 1
	}
	return gate
}

func (p *b3Pair) streamOrdered(w http.ResponseWriter, r *http.Request) {
	cursor := r.Header.Get("Last-Event-ID")
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	p.mu.Lock()
	index := len(p.conns)
	p.conns = append(p.conns, b3StreamConn{Cursor: cursor, Mode: r.URL.Query().Get("mode")})
	p.streamCancel = cancel
	p.mu.Unlock()

	gate := p.takeReconnectGate(cursor)
	if gate != nil && !gate.hold(ctx) {
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.inner.URL+"/v1/events?"+r.URL.RawQuery, nil)
	if err != nil {
		http.Error(w, "synthetic stream request", http.StatusInternalServerError)
		return
	}
	request.Header = r.Header.Clone()
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		http.Error(w, "synthetic stream connect", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	p.mu.Lock()
	p.conns[index].Status = response.StatusCode
	p.mu.Unlock()
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	if flush, ok := w.(http.Flusher); ok {
		flush.Flush()
	}
	reader := bufio.NewReaderSize(response.Body, 64<<10)
	for {
		frame, err := b3ReadFrame(reader)
		if err != nil {
			p.mu.Lock()
			p.streamError = fmt.Sprintf("read: %v", err)
			p.mu.Unlock()
			return
		}
		name, id, next := b3ParseFrame(frame)
		if strings.HasPrefix(name, "file.") {
			if err := p.waitHold(ctx); err != nil {
				return
			}
		}
		if _, err := io.WriteString(w, frame); err != nil {
			return
		}
		if flush, ok := w.(http.Flusher); ok {
			flush.Flush()
		}
		p.mu.Lock()
		p.frames = append(p.frames, b3StreamFrame{Conn: index, Name: name, ID: id, Cursor: next})
		p.mu.Unlock()
	}
}

func b3ReadFrame(reader *bufio.Reader) (string, error) {
	var frame strings.Builder
	for {
		line, err := reader.ReadString('\n')
		frame.WriteString(line)
		if err != nil {
			return "", err
		}
		if line == "\n" || line == "\r\n" {
			return frame.String(), nil
		}
	}
}

func b3ParseFrame(raw string) (name, id, cursor string) {
	var data strings.Builder
	frameID := ""
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSuffix(line, "\r")
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			name = value
		case "id":
			frameID = value
		case "data":
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(value)
		}
	}
	switch name {
	case "sync.ready", "sync.checkpoint":
		var marker struct {
			Cursor string `json:"cursor"`
		}
		if json.Unmarshal([]byte(data.String()), &marker) == nil {
			cursor = marker.Cursor
		}
	case "sync.required", "sync.renew":
	default:
		if strings.HasPrefix(name, "file.") {
			cursor = frameID
			var envelope struct {
				ResourceID string `json:"resourceId"`
			}
			if json.Unmarshal([]byte(data.String()), &envelope) == nil && envelope.ResourceID != "" {
				id = envelope.ResourceID
			} else {
				id = frameID
			}
		}
	}
	return name, id, cursor
}

func (p *b3Pair) severSSE() {
	p.mu.Lock()
	cancel := p.streamCancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// b3Child is the supervised child process and its private line protocol.
type b3Child struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	mu      sync.Mutex
	command *exec.Cmd
	done    chan error
}

type b3ChildReply struct {
	Error     string
	Exists    bool
	IsDir     bool
	Size      int64
	ID        string
	SHA256    string
	Dirty     bool
	Held      string
	WriteBack float64
	Names     []string
}

type b3ChildRequest struct {
	Op   string
	Path string
	Size int
}

func (p *b3Pair) startChild(t *testing.T) *b3Child {
	t.Helper()
	socket := filepath.Join(p.directory, "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal("paired child control listener:", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	log, err := os.OpenFile(filepath.Join(p.directory, "child.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal("paired child log:", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, p.binary,
		"-test.run", "^TestB3PairedMountChild$", "-test.v", "-test.count=1", "-test.timeout", "150s")
	command.Dir = p.directory
	command.Env = []string{
		"B3_PAIR_CHILD=1", "B3_PAIR_ENDPOINT=" + p.server.URL,
		"B3_PAIR_CONTROL=" + socket, "B3_PAIR_DIR=" + p.directory,
		"HOME=" + p.directory, "TMPDIR=" + p.directory, "PATH=/usr/bin:/bin",
	}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal("paired child start:", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("paired child did not exit")
		}
		_ = listener.Close()
		_ = log.Close()
	})
	accepted := make(chan net.Conn, 1)
	rejected := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			rejected <- err
			return
		}
		accepted <- conn
	}()
	select {
	case conn := <-accepted:
		return &b3Child{conn: conn, encoder: json.NewEncoder(conn), decoder: json.NewDecoder(conn), command: command, done: done}
	case err := <-rejected:
		t.Fatal("paired child control accept:", err)
	case <-time.After(20 * time.Second):
		t.Fatal("paired child did not connect to its control socket")
	}
	return nil
}

func (c *b3Child) call(t *testing.T, request b3ChildRequest) b3ChildReply {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatal("paired child deadline:", err)
	}
	if err := c.encoder.Encode(&request); err != nil {
		t.Fatal("paired child request:", err)
	}
	var reply b3ChildReply
	if err := c.decoder.Decode(&reply); err != nil {
		t.Fatalf("paired child reply for %q: %v", request.Op, err)
	}
	if reply.Error != "" {
		t.Fatalf("paired child %q: %s", request.Op, reply.Error)
	}
	return reply
}

func (c *b3Child) close(t *testing.T) {
	t.Helper()
	_ = c.call(t, b3ChildRequest{Op: "exit"})
	_ = c.conn.Close()
}

func b3ChildBinary(t *testing.T) string {
	t.Helper()
	binary, expected := os.Getenv("B3_VFS_TEST_BIN"), os.Getenv("B3_VFS_TEST_SHA256")
	if !filepath.IsAbs(binary) || filepath.Clean(binary) != binary || len(expected) != 64 {
		t.Fatal("fresh B3_VFS_TEST_BIN/B3_VFS_TEST_SHA256 required; paired VFS proof NOT-RUN")
	}
	info, err := os.Lstat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatal("owned paired child executable required")
	}
	resolved, err := filepath.EvalSymlinks(binary)
	if err != nil || resolved != binary {
		t.Fatal("paired child executable path is not canonical")
	}
	owner := reflect.ValueOf(info.Sys())
	if owner.Kind() != reflect.Pointer || owner.IsNil() {
		t.Fatal("paired child ownership unavailable")
	}
	uid := owner.Elem().FieldByName("Uid")
	if !uid.IsValid() || !uid.CanUint() || uid.Uint() != uint64(os.Getuid()) || info.Mode().Perm()&0022 != 0 {
		t.Fatal("paired child executable is not exclusively owner-writable")
	}
	parent, err := os.Lstat(filepath.Dir(binary))
	if err != nil || parent.Mode().Perm() != 0700 {
		t.Fatal("paired child executable directory is not private")
	}
	file, err := os.Open(binary)
	if err != nil {
		t.Fatal("paired child executable unreadable")
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal("paired child executable hash failed")
	}
	if hex.EncodeToString(hash.Sum(nil)) != expected {
		t.Fatal("paired child executable checksum mismatch")
	}
	return binary
}

func (p *b3Pair) waitFrame(t *testing.T, after int, predicate func(b3StreamFrame) bool) (int, b3StreamFrame) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for index, frame := range p.frames {
			if index >= after && predicate(frame) {
				p.mu.Unlock()
				return index, frame
			}
		}
		p.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	p.mu.Lock()
	observed := make([]string, 0, len(p.frames))
	for _, frame := range p.frames {
		observed = append(observed, fmt.Sprintf("%d:%s:%s", frame.Conn, frame.Name, frame.ID))
	}
	connections := make([]string, 0, len(p.conns))
	for _, conn := range p.conns {
		connections = append(connections, fmt.Sprintf("%q:%d", conn.Cursor, conn.Status))
	}
	p.mu.Unlock()
	t.Fatalf("paired SSE frame did not arrive; frames=%v connections=%v stream=%q", observed, connections, p.streamError)
	return 0, b3StreamFrame{}
}

func (p *b3Pair) waitReady(t *testing.T) b3StreamFrame {
	t.Helper()
	_, frame := p.waitFrame(t, 0, func(frame b3StreamFrame) bool { return frame.Name == "sync.ready" })
	return frame
}

func (p *b3Pair) waitConn(t *testing.T, after int, predicate func(b3StreamConn) bool) (int, b3StreamConn) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for index := after; index < len(p.conns); index++ {
			if predicate(p.conns[index]) {
				conn := p.conns[index]
				p.mu.Unlock()
				return index, conn
			}
		}
		p.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("paired SSE connection did not arrive")
	return 0, b3StreamConn{}
}

func b3CursorPosition(value string) int64 {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return -1
	}
	var position int64
	if _, err := fmt.Sscan(parts[2], &position); err != nil {
		return -1
	}
	return position
}

func (p *b3Pair) waitGate(t *testing.T, gate *b3Gate) {
	t.Helper()
	select {
	case <-gate.entered:
	case <-time.After(20 * time.Second):
		t.Fatalf("paired request gate was not entered; trace=%v", p.traceSummary())
	}
}

func (p *b3Pair) traceSummary() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	summary := make([]string, 0, len(p.trace))
	for _, request := range p.trace {
		summary = append(summary, fmt.Sprintf("%s %s %d", request.Method, request.Path, request.Status))
	}
	return summary
}

func (p *b3Pair) waitStatus(t *testing.T, method, suffix string, status int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for _, request := range p.trace {
			if request.Method == method && strings.HasSuffix(request.Path, suffix) && request.Status == status {
				p.mu.Unlock()
				return
			}
		}
		p.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no %s %s with status %d; trace=%v", method, suffix, status, p.traceSummary())
}

func (p *b3Pair) traceLength() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.trace)
}

func (p *b3Pair) frameCount(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, frame := range p.frames {
		if frame.ID == id && strings.HasPrefix(frame.Name, "file.") {
			count++
		}
	}
	return count
}

func (p *b3Pair) guardedMutations(from int) (creates, parts, completes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, request := range p.trace[min(from, len(p.trace)):] {
		switch {
		case request.Method == http.MethodPost && request.Path == "/v1/uploads/guarded":
			creates++
		case request.Method == http.MethodPut && strings.HasPrefix(request.Path, "/v1/uploads/guarded/") &&
			strings.Contains(request.Path, "/parts/"):
			parts++
		case request.Method == http.MethodPost && strings.HasPrefix(request.Path, "/v1/uploads/guarded/") &&
			strings.HasSuffix(request.Path, "/complete"):
			completes++
		}
	}
	return creates, parts, completes
}

func (p *b3Pair) guardedCompleteID(t *testing.T) uuid.UUID {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, request := range p.trace {
		if request.Method == http.MethodPost && strings.HasPrefix(request.Path, "/v1/uploads/guarded/") &&
			strings.HasSuffix(request.Path, "/complete") {
			middle := strings.TrimSuffix(strings.TrimPrefix(request.Path, "/v1/uploads/guarded/"), "/complete")
			id, err := uuid.Parse(middle)
			if err == nil {
				return id
			}
		}
	}
	t.Fatal("no guarded complete request observed")
	return uuid.Nil
}

func (p *b3Pair) recoveryIncarnation(t *testing.T) uuid.UUID {
	t.Helper()
	var value string
	if err := p.pool.QueryRow(p.ctx, `SELECT incarnation::text FROM teldrive.recovery_incarnations WHERE singleton`).Scan(&value); err != nil {
		t.Fatal("recovery incarnation query:", err)
	}
	id, err := uuid.Parse(value)
	if err != nil {
		t.Fatal("recovery incarnation parse:", err)
	}
	return id
}

func (p *b3Pair) orderedState(t *testing.T) (floor, high int64) {
	t.Helper()
	if err := p.pool.QueryRow(p.ctx, `SELECT coalesce(state.floor,0), coalesce(state.high,0)
		FROM teldrive.event_owner_stream_state state
		JOIN teldrive.event_owner_incarnations owner ON owner.incarnation=state.owner_incarnation
		WHERE owner.user_id=$1`, p.owner).Scan(&floor, &high); err != nil {
		t.Fatal("ordered state query:", err)
	}
	return floor, high
}

func (p *b3Pair) eventRows(t *testing.T) int {
	t.Helper()
	var count int
	if err := p.pool.QueryRow(p.ctx, `SELECT count(*) FROM teldrive.user_events`).Scan(&count); err != nil {
		t.Fatal("event row count:", err)
	}
	return count
}

func (p *b3Pair) eventCount(t *testing.T, resource, name string) int {
	t.Helper()
	var count int
	if err := p.pool.QueryRow(p.ctx, `SELECT count(*) FROM teldrive.user_events
		WHERE resource_id=$1 AND event_type=$2`, resource, name).Scan(&count); err != nil {
		t.Fatal("event count:", err)
	}
	return count
}

func (p *b3Pair) fileRow(t *testing.T, id uuid.UUID) (parent uuid.UUID, name string, size int64, hash string) {
	t.Helper()
	var parentText string
	if err := p.pool.QueryRow(p.ctx, `SELECT coalesce(parent_id::text,''),name,size,hash_value
		FROM teldrive.files WHERE id=$1`, id).Scan(&parentText, &name, &size, &hash); err != nil {
		t.Fatal("file row query:", err)
	}
	if parentText != "" {
		var err error
		parent, err = uuid.Parse(parentText)
		if err != nil {
			t.Fatal("file parent parse:", err)
		}
	}
	return parent, name, size, hash
}

func (p *b3Pair) fileIDByName(t *testing.T, name string) uuid.UUID {
	t.Helper()
	var value string
	if err := p.pool.QueryRow(p.ctx, `SELECT id::text FROM teldrive.files
		WHERE user_id=$1 AND kind='file' AND name=$2 AND status='active'`, p.owner, name).Scan(&value); err != nil {
		t.Fatal("file id lookup:", err)
	}
	id, err := uuid.Parse(value)
	if err != nil {
		t.Fatal("file id parse:", err)
	}
	return id
}

func (p *b3Pair) fileCount(t *testing.T, name string) int {
	t.Helper()
	var count int
	if err := p.pool.QueryRow(p.ctx, `SELECT count(*) FROM teldrive.files
		WHERE user_id=$1 AND kind='file' AND name=$2`, p.owner, name).Scan(&count); err != nil {
		t.Fatal("file count:", err)
	}
	return count
}

func (p *b3Pair) activeFiles(t *testing.T) int {
	t.Helper()
	var count int
	if err := p.pool.QueryRow(p.ctx, `SELECT count(*) FROM teldrive.files
		WHERE user_id=$1 AND kind='file' AND status='active'`, p.owner).Scan(&count); err != nil {
		t.Fatal("active file count:", err)
	}
	return count
}

func (p *b3Pair) rename(t *testing.T, id uuid.UUID, name string) {
	t.Helper()
	if _, err := p.catalog.Rename(p.ctx, p.owner, id, nil, name); err != nil {
		t.Fatalf("rename to %q: %v", name, err)
	}
}

func (p *b3Pair) referenceUploads(t *testing.T) {
	t.Helper()
	if _, err := p.pool.Exec(p.ctx, `INSERT INTO teldrive.users(user_id) VALUES(1003)`); err != nil {
		t.Fatal(err)
	}
}

func runB3RetentionRecovery(t *testing.T, binary string) {
	p := newB3Pair(t, binary)
	child := p.startChild(t)
	defer child.close(t)
	p.waitReady(t)

	top := p.folder("top", nil)
	mid := p.folder("mid", &top)
	p.waitFrame(t, 0, func(frame b3StreamFrame) bool { return frame.Name == "file.created" })
	child.call(t, b3ChildRequest{Op: "stat", Path: "top"})
	child.call(t, b3ChildRequest{Op: "stat", Path: "top/mid"})

	pending := 3001
	child.call(t, b3ChildRequest{Op: "write", Path: "top/mid/pending.txt", Size: pending})

	p.setHold(true)
	filler := p.publish(&mid, "filler", pairPayload(101))
	deadline := time.Now().Add(10 * time.Second)
	for p.eventCount(t, filler.String(), "file.created") == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if count := p.eventCount(t, filler.String(), "file.created"); count != 1 {
		t.Fatalf("filler event count = %d", count)
	}

	gate := newB3Gate()
	p.mu.Lock()
	p.reconnectGate = gate
	p.mu.Unlock()
	p.severSSE()
	p.setHold(false)
	index, conn := p.waitConn(t, 0, func(conn b3StreamConn) bool { return conn.Cursor != "" })
	if index < 0 || conn.Cursor == "" {
		t.Fatal("reconnect did not carry the exact strict cursor")
	}
	p.waitGate(t, gate)
	cursor := conn.Cursor
	position := b3CursorPosition(cursor)
	if position < 0 {
		t.Fatalf("strict cursor %q is invalid", cursor)
	}

	if err := jobs.NewEventCleanupWorker(p.pool).Work(p.ctx,
		&river.Job[jobs.EventCleanupArgs]{Args: jobs.EventCleanupArgs{Retention: "1ns"}}); err != nil {
		t.Fatal("real event retention:", err)
	}
	if rows := p.eventRows(t); rows != 0 {
		t.Fatalf("retention retained %d events", rows)
	}
	floor, _ := p.orderedState(t)
	if floor <= position {
		t.Fatalf("retention floor %d did not expire cursor position %d", floor, position)
	}
	close(gate.release)

	frames := len(p.frames)
	_, reset := p.waitFrame(t, frames, func(frame b3StreamFrame) bool {
		return frame.Name == "sync.required" && frame.Conn == index
	})
	if reset.Name != "sync.required" {
		t.Fatal("cursor reset frame missing")
	}
	baselineIndex, baseline := p.waitConn(t, index+1, func(conn b3StreamConn) bool { return conn.Cursor == "" })
	if baseline.Cursor != "" {
		t.Fatal("reconnect after reset still carried a cursor")
	}
	_, ready := p.waitFrame(t, frames, func(frame b3StreamFrame) bool {
		return frame.Conn == baselineIndex && frame.Name == "sync.ready"
	})
	baselinePosition := b3CursorPosition(ready.Cursor)
	if baselinePosition < 0 {
		t.Fatalf("baseline cursor %q is invalid", ready.Cursor)
	}

	newA := pairPayload(211)
	newB := pairPayload(223)
	aID := p.publish(&top, "new-a", newA)
	bID := p.publish(&mid, "new-b", newB)
	for _, id := range []uuid.UUID{aID, bID} {
		_, frame := p.waitFrame(t, frames, func(frame b3StreamFrame) bool {
			return frame.Name == "file.created" && frame.ID == id.String()
		})
		if pos := b3CursorPosition(frame.Cursor); pos <= baselinePosition {
			t.Fatalf("event cursor %d did not advance past baseline H %d", pos, baselinePosition)
		}
	}
	if count := p.frameCount(aID.String()); count != 1 {
		t.Fatalf("new-a delivered %d times", count)
	}
	if count := p.frameCount(bID.String()); count != 1 {
		t.Fatalf("new-b delivered %d times", count)
	}
	p.mu.Lock()
	for _, frame := range p.frames {
		if strings.HasPrefix(frame.Name, "upload.") {
			p.mu.Unlock()
			t.Fatalf("ordered stream leaked %q", frame.Name)
		}
	}
	p.mu.Unlock()

	topList := child.call(t, b3ChildRequest{Op: "list", Path: "top"})
	if !containsName(topList.Names, "new-a") {
		t.Fatalf("top listing did not converge: %v", topList.Names)
	}
	midList := child.call(t, b3ChildRequest{Op: "list", Path: "top/mid"})
	if !containsName(midList.Names, "new-b") {
		t.Fatalf("mid listing did not converge: %v", midList.Names)
	}
	state := child.call(t, b3ChildRequest{Op: "state", Path: "top/mid/pending.txt"})
	if !state.Dirty {
		t.Fatal("pending local bytes were lost across the retained-stream recovery")
	}
	if state.WriteBack != (24 * time.Hour).Seconds() {
		t.Fatalf("write-back = %v", state.WriteBack)
	}
	read := child.call(t, b3ChildRequest{Op: "read", Path: "top/mid/pending.txt"})
	if read.SIZEHASHNotEqual(pairPayload(pending)) {
		t.Fatal("pending local bytes changed")
	}
}

func containsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func (reply b3ChildReply) SIZEHASHNotEqual(want []byte) bool {
	sum := sha256.Sum256(want)
	return reply.SHA256 != hex.EncodeToString(sum[:]) || reply.Size != int64(len(want))
}

func b3TreeHash(data []byte) string {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(data)
	return treehash.SumToHex(treehash.ComputeTreeHash(hasher.Sum()))
}

func b3OrderedStreamNames(t *testing.T, endpoint, cursor, types string, wantStatus int) ([]string, string) {
	t.Helper()
	target := endpoint + "/v1/events?mode=ordered"
	if types != "" {
		target += "&types=" + url.QueryEscape(types)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatal("ordered request:", err)
	}
	request.Header.Set("X-Api-Key", "test-key")
	request.Header.Set("Accept", "text/event-stream")
	if cursor != "" {
		request.Header.Set("Last-Event-ID", cursor)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("ordered connect: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		t.Fatalf("ordered status = %d, want %d", response.StatusCode, wantStatus)
	}
	var names []string
	var ready string
	if wantStatus != http.StatusOK {
		return names, ready
	}
	reader := bufio.NewReaderSize(response.Body, 64<<10)
	for len(names) < 64 {
		frame, err := b3ReadFrame(reader)
		if err != nil {
			break
		}
		name, _, next := b3ParseFrame(frame)
		if strings.HasPrefix(name, "file.") || strings.HasPrefix(name, "upload.") {
			names = append(names, name)
		}
		if name == "sync.ready" {
			ready = next
			break
		}
	}
	return names, ready
}

func runB3OrderedFilter(t *testing.T, _ string) {
	p := newB3Pair(t, "")
	parent := p.folder("filtered", nil)
	_, baseline := b3OrderedStreamNames(t, p.inner.URL, "", "", http.StatusOK)
	if baseline == "" {
		t.Fatal("ordered baseline cursor missing")
	}
	p.publish(&parent, "filtered.txt", pairPayload(37))
	names, _ := b3OrderedStreamNames(t, p.inner.URL, baseline, "", http.StatusOK)
	if len(names) == 0 {
		t.Fatal("ordered replay delivered no events")
	}
	found := false
	for _, name := range names {
		if strings.HasPrefix(name, "upload.") {
			t.Fatalf("ordered stream leaked %q", name)
		}
		if name == "file.created" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ordered replay omitted the file event: %v", names)
	}
	var transitions int
	if err := p.pool.QueryRow(p.ctx, `SELECT count(*) FROM teldrive.user_events
		WHERE user_id=$1 AND event_type LIKE 'upload.%'`, p.owner).Scan(&transitions); err != nil {
		t.Fatal("upload transition count:", err)
	}
	if transitions == 0 {
		t.Fatal("the filtered upload transition was never committed")
	}
	b3OrderedStreamNames(t, p.inner.URL, "", "file.created", http.StatusUnprocessableEntity)
	strict := strings.Join([]string{
		"file.created", "file.updated", "file.trashed",
		"file.deletion_pending", "file.restored", "file.purged",
	}, ",")
	if _, ready := b3OrderedStreamNames(t, p.inner.URL, "", strict, http.StatusOK); ready == "" {
		t.Fatal("exact-set ordered stream did not start")
	}
}

func runB3ConflictRestart(t *testing.T, binary string) {
	p := newB3Pair(t, binary)
	child := p.startChild(t)
	defer child.close(t)
	p.waitReady(t)

	parent := p.folder("conflict", nil)
	local := pairPayload(4096)
	external := bytes.Repeat([]byte("external-winner-"), 256)
	if len(external) != len(local) {
		t.Fatalf("external winner size %d", len(external))
	}
	child.call(t, b3ChildRequest{Op: "write", Path: "conflict/file.txt", Size: len(local)})

	gate := newB3Gate()
	p.mu.Lock()
	p.completePreGate = gate
	p.mu.Unlock()
	child.call(t, b3ChildRequest{Op: "flush", Path: "conflict/file.txt"})
	p.waitGate(t, gate)
	externalID := p.publish(&parent, "file.txt", external)
	close(gate.release)

	p.waitStatus(t, http.MethodPost, "/complete", http.StatusPreconditionFailed)
	deadline := time.Now().Add(20 * time.Second)
	var state b3ChildReply
	for time.Now().Before(deadline) {
		state = child.call(t, b3ChildRequest{Op: "state", Path: "conflict/file.txt"})
		if state.Held != "" && state.Dirty {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state.Held == "" || !state.Dirty {
		t.Fatalf("guarded conflict was not held: %#v", state)
	}
	if !strings.Contains(state.Held, "conflict") {
		t.Fatalf("held reason = %q", state.Held)
	}

	_, name, size, hash := p.fileRow(t, externalID)
	if name != "file.txt" || size != int64(len(external)) || hash != b3TreeHash(external) {
		t.Fatalf("external winner row = %q %d %s, want hash %s", name, size, hash, b3TreeHash(external))
	}
	p.download(externalID, external)

	mark := p.traceLength()
	restart := child.call(t, b3ChildRequest{Op: "restart"})
	if restart.WriteBack != (24 * time.Hour).Seconds() {
		t.Fatalf("write-back after restart = %v", restart.WriteBack)
	}
	read := child.call(t, b3ChildRequest{Op: "read", Path: "conflict/file.txt"})
	if read.SIZEHASHNotEqual(local) {
		t.Fatal("held local bytes did not survive restart")
	}
	if !read.Dirty || read.Held == "" {
		t.Fatalf("restart dropped the held generation: %#v", read)
	}
	creates, parts, completes := p.guardedMutations(mark)
	if creates != 0 || parts != 0 || completes != 0 {
		t.Fatalf("restart re-armed guarded mutations: %d/%d/%d", creates, parts, completes)
	}
	_, name, size, hash = p.fileRow(t, externalID)
	if name != "file.txt" || size != int64(len(external)) || hash != b3TreeHash(external) {
		t.Fatal("external winner changed across restart")
	}
	p.download(externalID, external)
}

func runB3SecondMoveDelayedSSE(t *testing.T, binary string) {
	p := newB3Pair(t, binary)
	child := p.startChild(t)
	defer child.close(t)
	p.waitReady(t)

	alpha := p.folder("alpha", nil)
	sub := p.folder("sub", &alpha)
	payload := pairPayload(5000)
	child.call(t, b3ChildRequest{Op: "write", Path: "alpha/sub/file.txt", Size: len(payload)})
	p.rename(t, alpha, "beta")

	p.setHold(true)
	gate := newB3Gate()
	p.mu.Lock()
	p.createGate = gate
	p.mu.Unlock()
	child.call(t, b3ChildRequest{Op: "flush", Path: "alpha/sub/file.txt"})
	p.waitGate(t, gate)
	p.rename(t, alpha, "gamma")
	close(gate.release)
	p.waitStatus(t, http.MethodPost, "/complete", http.StatusOK)
	p.setHold(false)
	p.waitFrame(t, 0, func(frame b3StreamFrame) bool {
		return frame.Name == "file.created"
	})

	var state b3ChildReply
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		state = child.call(t, b3ChildRequest{Op: "stat", Path: "gamma/sub/file.txt"})
		if state.Exists && state.ID != "" && !state.Dirty && state.Held == "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !state.Exists || state.ID == "" || state.Dirty || state.Held != "" {
		t.Fatalf("authoritative alias was not adopted: %#v", state)
	}
	fileID, err := uuid.Parse(state.ID)
	if err != nil {
		t.Fatalf("adopted alias id %q", state.ID)
	}
	if creates, _, completes := p.guardedMutations(0); creates != 1 || completes != 1 {
		t.Fatalf("guarded mutations = %d creates %d completes", creates, completes)
	}
	p.download(fileID, payload)
	_, name, size, hash := p.fileRow(t, fileID)
	if name != "file.txt" || size != int64(len(payload)) || hash != b3TreeHash(payload) {
		t.Fatalf("authoritative row = %q %d %s", name, size, hash)
	}
	if parent, _, _, _ := p.fileRow(t, fileID); parent != sub {
		t.Fatalf("authoritative parent = %s, want %s", parent, sub)
	}
	uploadID := p.guardedCompleteID(t)
	incarnation := p.recoveryIncarnation(t)
	receipt, err := p.uploads.GetGuarded(p.ctx, p.owner, uploadID, incarnation)
	if err != nil || receipt.Receipt == nil {
		t.Fatalf("guarded receipt: %v", err)
	}
	if strings.Join(receipt.Receipt.ParentSegments, "/") != "gamma/sub" {
		t.Fatalf("receipt segments = %v", receipt.Receipt.ParentSegments)
	}
	if count := p.eventCount(t, fileID.String(), "file.created"); count != 1 {
		t.Fatalf("publication events = %d", count)
	}
	if count := p.fileCount(t, "file.txt"); count != 1 {
		t.Fatalf("file rows named file.txt = %d", count)
	}
	p.setHold(false)
	p.waitFrame(t, 0, func(frame b3StreamFrame) bool {
		return frame.Name == "file.created" && frame.ID == fileID.String()
	})
	if count := p.frameCount(fileID.String()); count != 1 {
		t.Fatalf("publication delivered %d times", count)
	}
	for _, path := range []string{"alpha", "beta"} {
		if absent := child.call(t, b3ChildRequest{Op: "absent", Path: path}); absent.Exists {
			t.Fatalf("old path %q was resurrected or still listed", path)
		}
	}
}

func runB3HeldCompleteAfterCommit(t *testing.T, binary string) {
	p := newB3Pair(t, binary)
	child := p.startChild(t)
	defer child.close(t)
	p.waitReady(t)

	alpha := p.folder("alpha", nil)
	p.folder("sub", &alpha)
	payload := pairPayload(6000)
	child.call(t, b3ChildRequest{Op: "write", Path: "alpha/sub/file.txt", Size: len(payload)})
	p.rename(t, alpha, "beta")

	gate := newB3Gate()
	p.mu.Lock()
	p.completeGate = gate
	p.mu.Unlock()
	child.call(t, b3ChildRequest{Op: "flush", Path: "alpha/sub/file.txt"})
	p.waitGate(t, gate)
	p.waitStatus(t, http.MethodPost, "/complete", http.StatusOK)

	p.rename(t, alpha, "gamma")
	p.waitFrame(t, 0, func(frame b3StreamFrame) bool {
		return frame.Name == "file.updated" && frame.ID == alpha.String()
	})
	close(gate.release)

	var held b3ChildReply
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		held = child.call(t, b3ChildRequest{Op: "state", Path: "alpha/sub/file.txt"})
		if held.Held != "" && held.Dirty {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if held.Held == "" || !held.Dirty {
		t.Fatalf("stale-epoch completion became clean: %#v", held)
	}
	if creates, _, completes := p.guardedMutations(0); creates != 1 || completes != 1 {
		t.Fatalf("guarded mutations = %d creates %d completes", creates, completes)
	}
	fileID := p.fileIDByName(t, "file.txt")
	if count := p.eventCount(t, fileID.String(), "file.created"); count != 1 {
		t.Fatalf("publication events = %d", count)
	}
	if count := p.fileCount(t, "file.txt"); count != 1 {
		t.Fatalf("file rows named file.txt = %d", count)
	}

	mark := p.traceLength()
	child.call(t, b3ChildRequest{Op: "restart"})
	var state b3ChildReply
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		state = child.call(t, b3ChildRequest{Op: "stat", Path: "gamma/sub/file.txt"})
		if state.Exists && state.ID != "" && !state.Dirty && state.Held == "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !state.Exists || state.ID == "" || state.Dirty || state.Held != "" {
		t.Fatalf("restart did not adopt the exact receipt: %#v", state)
	}
	read := child.call(t, b3ChildRequest{Op: "read", Path: "gamma/sub/file.txt"})
	if read.SIZEHASHNotEqual(payload) {
		t.Fatal("adopted local bytes differ")
	}
	for _, path := range []string{"alpha", "beta"} {
		if absent := child.call(t, b3ChildRequest{Op: "absent", Path: path}); absent.Exists {
			t.Fatalf("old path %q survived the later move", path)
		}
	}
	creates, parts, completes := p.guardedMutations(mark)
	if creates != 0 || parts != 0 || completes != 0 {
		t.Fatalf("restart re-armed guarded mutations: %d/%d/%d", creates, parts, completes)
	}
	if count := p.fileCount(t, "file.txt"); count != 1 {
		t.Fatalf("restart duplicated publication: %d rows", count)
	}
	p.download(fileID, payload)
}

// TestB3PairedMountConsistency drives the actual generated server stack with a
// supervised VFS child over a private channel.
func TestB3PairedMountConsistency(t *testing.T) {
	if os.Getenv("B3_VFS_TEST_BIN") == "" || os.Getenv("B3_VFS_TEST_SHA256") == "" {
		t.Skip("private B3 paired VFS authority not supplied; paired proof NOT-RUN")
	}
	binary := b3ChildBinary(t)
	start := time.Now()
	t.Run("retained_stream_recovery", func(t *testing.T) { runB3RetentionRecovery(t, binary) })
	t.Run("guarded_conflict_restart", func(t *testing.T) { runB3ConflictRestart(t, binary) })
	t.Run("second_in_root_move_delayed_sse", func(t *testing.T) { runB3SecondMoveDelayedSSE(t, binary) })
	t.Run("held_complete_after_commit", func(t *testing.T) { runB3HeldCompleteAfterCommit(t, binary) })
	t.Run("ordered_filter_strict", func(t *testing.T) { runB3OrderedFilter(t, binary) })
	t.Logf("paired wall time %s", time.Since(start))
}

var _ = (*pgxpool.Pool)(nil)
var _ = telegramstore.UploadRequest{}
var _ = api.Identity{}
