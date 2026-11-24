//go:build integration && linux

package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	b3InsertWork     = `INSERT INTO b3_qual_control.work_rows (payload) VALUES ('qualification')`
	b3StaleUse       = `SELECT payload FROM b3_qual_control.work_rows`
	b3StaleCleanup   = `DELETE FROM b3_qual_control.work_rows WHERE payload = 'qualification'`
	b3ArchiveSchema  = "b3_qual_archive"
	b3ControlSchema  = "b3_qual_control"
	b3WriterBegin    = "BEGIN"
	b3WriterXID      = `SELECT pg_current_xact_id()::text`
	b3WriterCommit   = "COMMIT"
	b3Setval         = `SELECT setval('b3_qual_control.existing_rows_id_seq'::regclass, 12345, true)`
	b3Nextval        = `SELECT nextval('b3_qual_control.existing_rows_id_seq'::regclass)`
	b3Undeclared     = `INSERT INTO b3_qual_control.existing_rows (payload) VALUES ('must-not-exist')`
	b3XactStatus     = `SELECT pg_xact_status($1::text::xid8)`
	b3NoWork         = `SELECT NOT EXISTS (SELECT FROM b3_qual_control.work_rows)`
	b3NoTrigger      = `SELECT NOT EXISTS (SELECT FROM b3_qual_control.trigger_rows)`
	b3ExistingLedger = `SELECT ARRAY(SELECT id::text || ':' || payload FROM b3_qual_control.existing_rows ORDER BY id, payload COLLATE "C")`
	b3AbsentSQL      = `SELECT NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'b3_qual_archive')
AND NOT EXISTS (SELECT FROM pg_class WHERE oid = ANY($1::text::oid[]))`
	b3DeferredCatalog = `SELECT count(*) = 1 FROM pg_trigger
WHERE tgrelid = 'b3_qual_control.work_rows'::regclass AND tgname = 'qualification_deferred'
AND tgenabled = 'O' AND tgdeferrable AND tginitdeferred AND NOT tgisinternal AND tgtype = 5
AND tgfoid = 'b3_qual_control.qualification_deferred()'::regprocedure`
)

func b3SequenceSQL(name b3SequenceName) string {
	return `SELECT last_value, is_called FROM ` + pgx.Identifier{name.schema, name.sequence}.Sanitize()
}

func b3ObserverQueries(caseName string) []string {
	queries := []string{"begin", "rollback", b3ObserverIdentity, b3XactStatus, b3NoWork, b3ExistingLedger,
		b3SequenceCatalog, b3SequenceSQL(b3ExistingSequence)}
	switch caseName {
	case b3RestoreRollback, b3RestoreTail:
		return append(queries, b3AbsentSQL, b3RestoredLedger, b3SequenceSQL(b3IdentitySequence), b3SequenceSQL(b3SerialSequence))
	case b3ExpiredDeferred:
		return append(queries, b3NoTrigger, b3DeferredCatalog)
	}
	return queries
}

func b3SyntheticQueries(caseName string) []string {
	switch caseName {
	case b3RestoreRollback, b3RestoreTail:
		return []string{b3WriterBegin, b3WriterXID, b3StaleUse, b3StaleCleanup}
	case b3ExistingWrites:
		return []string{b3WriterBegin, b3WriterXID, b3Setval, b3Nextval, b3Undeclared}
	default:
		return []string{b3WriterBegin, b3WriterXID, b3WriterCommit, b3InsertWork, "-- ping"}
	}
}

type b3DatabaseIdentity struct {
	name        string
	oid         uint32
	ownerOID    uint32
	writer      string
	writerOID   uint32
	observer    string
	observerOID uint32
	cluster     string
	system      string
	port        uint16
}

type b3SequenceState struct {
	start     int64
	increment int64
	min       int64
	max       int64
	cache     int64
	last      int64
	called    bool
	cycle     bool
}

func (s b3SequenceState) next() (int64, bool) {
	if s.increment <= 0 || s.cycle || s.last < s.min || s.last > s.max {
		return 0, false
	}
	if !s.called {
		return s.last, true
	}
	if s.increment > s.max || s.last > s.max-s.increment {
		return 0, false
	}
	return s.last + s.increment, true
}

type b3SequenceObservation struct {
	tableOID, sequenceOID uint32
	column                int16
	state                 b3SequenceState
	owned                 string
}

type b3SequenceName struct {
	schema, table, sequence string
}

var (
	b3IdentitySequence = b3SequenceName{b3ArchiveSchema, "identity_rows", "identity_rows_id_seq"}
	b3SerialSequence   = b3SequenceName{b3ArchiveSchema, "serial_rows", "serial_rows_id_seq"}
	b3ExistingSequence = b3SequenceName{b3ControlSchema, "existing_rows", "existing_rows_id_seq"}
)

func b3AssertObserver(t *testing.T, ctx context.Context, tx pgx.Tx, want b3DatabaseIdentity) {
	t.Helper()
	var db, role, cluster, system string
	var dbOID, ownerOID, roleOID, writerOID uint32
	var port uint16
	var version int
	var scoped bool
	err := tx.QueryRow(ctx, b3ObserverIdentity, want.writer).Scan(&db, &dbOID, &ownerOID, &role, &roleOID, &writerOID, &cluster, &system, &port, &version, &scoped)
	if err != nil || !scoped || version != 180007 || db != want.name || dbOID != want.oid || ownerOID != want.ownerOID ||
		role != want.observer || roleOID != want.observerOID || writerOID != want.writerOID ||
		cluster != want.cluster || system != want.system || port != want.port {
		t.Fatal("independent read-only observer identity or privileges do not match qualification scope")
	}
}

const b3ObserverIdentity = `
SELECT d.datname, d.oid, d.datdba, r.rolname, r.oid, w.oid, current_setting('cluster_name'),
       c.system_identifier::text, current_setting('port')::int, current_setting('server_version_num')::int,
       (current_user = session_user AND inet_server_addr() IS NULL AND inet_client_addr() IS NULL
        AND current_setting('transaction_read_only') = 'on' AND current_setting('transaction_isolation') = 'read committed'
        AND d.datdba <> r.oid
        AND NOT (r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls)
        AND NOT (w.rolsuper OR w.rolcreaterole OR w.rolcreatedb OR w.rolreplication OR w.rolbypassrls)
        AND NOT EXISTS (SELECT FROM pg_auth_members m WHERE m.member = r.oid)
        AND NOT has_database_privilege(d.oid, 'CREATE,TEMP')
        AND NOT EXISTS (SELECT FROM pg_namespace n WHERE n.nspname IN ('b3_qual_control', 'b3_qual_archive')
                        AND has_schema_privilege(n.oid, 'CREATE'))
        AND NOT EXISTS (SELECT FROM pg_class k JOIN pg_namespace n ON n.oid = k.relnamespace
                        WHERE n.nspname IN ('b3_qual_control', 'b3_qual_archive') AND
                        CASE k.relkind WHEN 'S' THEN has_sequence_privilege(k.oid, 'USAGE,UPDATE')
                        WHEN 'r' THEN has_table_privilege(k.oid, 'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,MAINTAIN')
                        ELSE false END))
FROM pg_database d
JOIN pg_roles r ON r.rolname = current_user
JOIN pg_roles w ON w.rolname = $1
CROSS JOIN pg_control_system() c
WHERE d.datname = current_database()`

func b3ObserveOutcome(t *testing.T, ctx context.Context, tx pgx.Tx, xid uint64, want string) {
	t.Helper()
	if xid == 0 || (want != "aborted" && want != "committed") {
		t.Fatal("missing full transaction ID or terminal outcome")
	}
	for {
		var state *string
		if err := tx.QueryRow(ctx, b3XactStatus, b3XIDText(xid)).Scan(&state); err != nil || state == nil {
			t.Fatal("independent outcome observation failed; retain target fenced")
		}
		switch *state {
		case want:
			return
		case "in progress":
			select {
			case <-ctx.Done():
				t.Fatal("independent outcome is unresolved; retain target fenced")
			case <-time.After(10 * time.Millisecond):
			}
		default:
			t.Fatal("independent outcome disagrees with directed witness; retain target fenced")
		}
	}
}

func b3ObserveSequence(t *testing.T, ctx context.Context, tx pgx.Tx, name b3SequenceName) b3SequenceObservation {
	t.Helper()
	var s b3SequenceObservation
	err := tx.QueryRow(ctx, b3SequenceCatalog, name.schema, name.table, name.sequence).Scan(
		&s.tableOID, &s.sequenceOID, &s.column, &s.owned, &s.state.start, &s.state.increment,
		&s.state.min, &s.state.max, &s.state.cache, &s.state.cycle)
	if err != nil {
		t.Fatal("owned sequence catalog observation failed")
	}
	if err := tx.QueryRow(ctx, b3SequenceSQL(name)).Scan(&s.state.last, &s.state.called); err != nil {
		t.Fatal("read-only sequence state observation failed")
	}
	return s
}

const b3SequenceCatalog = `
SELECT tab.oid, seq.oid, a.attnum, d.deptype::text,
       p.seqstart, p.seqincrement, p.seqmin, p.seqmax, p.seqcache, p.seqcycle
FROM pg_namespace n
JOIN pg_class tab ON tab.relnamespace = n.oid AND tab.relname = $2 AND tab.relkind = 'r'
JOIN pg_attribute a ON a.attrelid = tab.oid AND a.attname = 'id' AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_class seq ON seq.relnamespace = n.oid AND seq.relname = $3 AND seq.relkind = 'S'
JOIN pg_sequence p ON p.seqrelid = seq.oid
JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = seq.oid AND d.objsubid = 0
                AND d.refclassid = 'pg_class'::regclass AND d.refobjid = tab.oid AND d.refobjsubid = a.attnum
WHERE n.nspname = $1 AND d.deptype IN ('i', 'a')`

func b3ObserveArchiveAbsent(t *testing.T, ctx context.Context, tx pgx.Tx, restoredOIDs ...uint32) {
	t.Helper()
	var absent bool
	oids := make([]string, len(restoredOIDs))
	for i, oid := range restoredOIDs {
		oids[i] = strconv.FormatUint(uint64(oid), 10)
	}
	if err := tx.QueryRow(ctx, b3AbsentSQL, "{"+strings.Join(oids, ",")+"}").Scan(&absent); err != nil || !absent {
		t.Fatal("archive prestate or rollback retained a restored schema, table or sequence OID")
	}
}

func b3ObserveArchiveLedger(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	var relations, attributes, identityRows, serialRows []string
	var extras bool
	err := tx.QueryRow(ctx, b3RestoredLedger).Scan(&relations, &attributes, &identityRows, &serialRows, &extras)
	if err != nil || extras ||
		!slices.Equal(relations, []string{"identity_rows:r", "identity_rows_id_seq:S", "serial_rows:r", "serial_rows_id_seq:S"}) ||
		!slices.Equal(attributes, []string{
			"identity_rows:id:bigint:true:a::", "identity_rows:payload:text:true:::",
			"serial_rows:id:bigint:true:::nextval('b3_qual_archive.serial_rows_id_seq'::regclass)", "serial_rows:payload:text:true:::",
		}) || !slices.Equal(identityRows, []string{"17:identity-first", "22:identity-second"}) ||
		!slices.Equal(serialRows, []string{"29:serial-first", "36:serial-second"}) {
		t.Fatal("independent restored data or schema ledger does not match the approved synthetic archive")
	}
}

const b3RestoredLedger = `
WITH rel AS (
    SELECT c.oid, c.relname, c.relkind
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'b3_qual_archive'
)
SELECT ARRAY(SELECT relname || ':' || relkind::text FROM rel ORDER BY relname COLLATE "C"),
       ARRAY(SELECT relname || ':' || attname || ':' || atttypid::regtype::text || ':' || attnotnull::text || ':' ||
                    attidentity::text || ':' || attgenerated::text || ':' ||
                    coalesce(pg_get_expr(d.adbin, d.adrelid), '')
             FROM rel JOIN pg_attribute a ON a.attrelid = rel.oid
             LEFT JOIN pg_attrdef d ON d.adrelid = rel.oid AND d.adnum = a.attnum
             WHERE relkind = 'r' AND attnum > 0 AND NOT attisdropped ORDER BY relname COLLATE "C", attnum),
       ARRAY(SELECT id::text || ':' || payload FROM b3_qual_archive.identity_rows ORDER BY id, payload COLLATE "C"),
       ARRAY(SELECT id::text || ':' || payload FROM b3_qual_archive.serial_rows ORDER BY id, payload COLLATE "C"),
       EXISTS (SELECT FROM rel JOIN pg_trigger t ON t.tgrelid = rel.oid UNION ALL
               SELECT FROM rel JOIN pg_constraint c ON c.conrelid = rel.oid WHERE c.contype <> 'n' UNION ALL
               SELECT FROM rel JOIN pg_rewrite r ON r.ev_class = rel.oid) OR
       EXISTS (SELECT FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'b3_qual_archive')`

func b3ObserveWorkAbsent(t *testing.T, ctx context.Context, tx pgx.Tx, deferred bool) {
	t.Helper()
	var empty bool
	if err := tx.QueryRow(ctx, b3NoWork).Scan(&empty); err != nil || !empty {
		t.Fatal("unexpected durable writer rows")
	}
	if deferred {
		if err := tx.QueryRow(ctx, b3NoTrigger).Scan(&empty); err != nil || !empty {
			t.Fatal("unexpected durable deferred trigger rows")
		}
	}
}

func b3ObserveExistingRows(t *testing.T, ctx context.Context, tx pgx.Tx) []string {
	t.Helper()
	var rows []string
	if err := tx.QueryRow(ctx, b3ExistingLedger).Scan(&rows); err != nil {
		t.Fatal("independent existing sequence row ledger observation failed")
	}
	return rows
}

// TestB3FencedTailQualification verifies only the sealed qualification lane.
func TestB3FencedTailQualification(t *testing.T) {
	if os.Getenv(b3QualificationEnv) == "" {
		t.Skip("private B3 qualification custody not supplied; qualification proof NOT-RUN")
	}
	if path := os.Getenv(b3CustodianEnv); path != "" {
		if err := b3ServeCustody(t.Context(), path, b3ArchiveRules, b3ObserverQueries, b3SyntheticQueries); err != nil {
			t.Fatal("qualification custodian startup or resource verification failed")
		}
		t.Fatal("custodian mode is not qualification evidence")
	}
	q, err := b3LoadQualification(os.Getenv(b3QualificationEnv))
	if err != nil {
		t.Fatal(errB3Qualification)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 103*time.Second)
	defer cancel()
	run := func(name string, f func(*testing.T)) {
		t.Helper()
		if !t.Run(name, f) {
			t.FailNow()
		}
	}

	var rollback, committed b3RestoreEvidence
	run("actual_pg_restore_tx_expiry_after_timeout_reset_rolls_back", func(t *testing.T) {
		rollback = b3RunRestore(t, ctx, q, b3RestoreRollback, false)
	})
	run("actual_pg_restore_new_identity_rollback_leaves_no_sequence", func(t *testing.T) {
		b3ConfirmRolledBackSequence(t, ctx, q, rollback, 0)
	})
	run("actual_pg_restore_new_serial_rollback_leaves_no_sequence", func(t *testing.T) {
		b3ConfirmRolledBackSequence(t, ctx, q, rollback, 1)
	})
	run("actual_pg_restore_admitted_fenced_tail_resolves_after_expiry", func(t *testing.T) {
		committed = b3RunRestore(t, ctx, q, b3RestoreTail, true)
	})
	run("actual_pg_restore_new_identity_next_value_matches", func(t *testing.T) {
		b3ConfirmRestoredSequence(t, ctx, q, committed, 0)
	})
	run("actual_pg_restore_new_serial_next_value_matches", func(t *testing.T) {
		b3ConfirmRestoredSequence(t, ctx, q, committed, 1)
	})
	run("writer_target_missing_pre_fence_denied", func(t *testing.T) {
		b3RunMissingAdmission(t, ctx, q, b3MissingFence, "prefence_missing")
	})
	run("writer_target_missing_tail_authority_denied", func(t *testing.T) {
		b3RunMissingAdmission(t, ctx, q, b3MissingTail, "tail_missing")
	})
	for _, test := range []struct{ name, caseName, message string }{
		{"authenticated_simple_query_after_expiry_denied", b3ExpiredQuery, "query"},
		{"authenticated_extended_parse_after_expiry_denied", b3ExpiredParse, "parse"},
		{"authenticated_extended_bind_after_expiry_denied", b3ExpiredBind, "bind"},
		{"authenticated_extended_execute_after_expiry_denied", b3ExpiredExecute, "execute"},
	} {
		run(test.name, func(t *testing.T) { b3RunProtocol(t, ctx, q, test.caseName, test.message) })
	}
	run("expiry_before_deferred_trigger_dispatch_rolls_back", func(t *testing.T) {
		b3RunDeferred(t, ctx, q)
	})
	run("existing_sequence_writes_denied_unchanged", func(t *testing.T) {
		b3RunExistingSequences(t, ctx, q)
	})
}

type b3RunCase struct {
	q        *b3Qualification
	ctx      context.Context
	name     string
	deadline uint64
	peer     uint32
}

func b3BeginCase(t *testing.T, ctx context.Context, q *b3Qualification, name string) b3RunCase {
	t.Helper()
	r := b3RunCase{q: q, ctx: ctx, name: name, peer: uint32(os.Getpid())}
	reply := r.control(t, b3ControlRequest{Action: "arm"})
	now, err := b3MonotonicNS()
	if err != nil || reply.Peer != r.peer || reply.Deadline <= now {
		t.Fatal("custodian did not arm the exact current case")
	}
	r.deadline = reply.Deadline
	return r
}

func (r b3RunCase) control(t *testing.T, req b3ControlRequest) b3ControlReply {
	t.Helper()
	req.Case = r.name
	reply, err := r.q.control(r.ctx, req)
	if err != nil {
		t.Fatal("custodian denied exact qualification operation; retain target fenced")
	}
	return reply
}

func (r b3RunCase) observer(t *testing.T) pgx.Tx {
	t.Helper()
	c, err := r.q.connect(r.ctx, r.name, "observe")
	if err != nil {
		t.Fatal("independent observer could not connect in exact approved scope")
	}
	t.Cleanup(func() { _ = c.Close(r.ctx) })
	tx, err := c.Begin(r.ctx)
	if err != nil {
		t.Fatal("independent observer could not open a read-only transaction")
	}
	t.Cleanup(func() { _ = tx.Rollback(r.ctx) })
	b3AssertObserver(t, r.ctx, tx, r.identity())
	return tx
}

func (r b3RunCase) identity() b3DatabaseIdentity {
	target := r.q.Cases[r.name]
	return b3DatabaseIdentity{target.Database, target.DatabaseOID, target.OwnerOID, target.Writer, target.WriterOID,
		target.Observer, target.ObserverOID, r.q.Cluster, r.q.SystemIdentifier, r.q.Port}
}

func (r b3RunCase) writer(t *testing.T) (*pgx.Conn, uint32, uint64) {
	t.Helper()
	c, err := r.q.connect(r.ctx, r.name, "writer_live")
	if err != nil {
		t.Fatal("approved writer did not authenticate before expiry")
	}
	t.Cleanup(func() { _ = c.Close(r.ctx) })
	pid := c.PgConn().PID()
	if pid == 0 || r.receipt(t, r.peer, pid, 0, "connect", "admit").At >= r.deadline {
		t.Fatal("writer connection was not authenticated under live authority")
	}
	if _, err := b3WriterSQL(r.ctx, c, b3WriterBegin); err != nil {
		t.Fatal("writer transaction did not start under live authority")
	}
	var text string
	err = c.QueryRow(r.ctx, b3WriterXID, pgx.QueryExecModeSimpleProtocol).Scan(&text)
	xid, xidErr := strconv.ParseUint(text, 10, 64)
	if err != nil || xidErr != nil || xid == 0 || c.PgConn().TxStatus() != 'T' {
		t.Fatal("writer full transaction ID or live transaction missing")
	}
	return c, pid, xid
}

func b3WriterSQL(ctx context.Context, c *pgx.Conn, sql string) (pgconn.CommandTag, error) {
	return c.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
}

func b3Denied(err error) bool {
	var postgres *pgconn.PgError
	return errors.As(err, &postgres) && postgres.Code == "42501"
}

func (r b3RunCase) receipts(t *testing.T, peer uint32) []b3Receipt {
	t.Helper()
	target := r.q.Cases[r.name]
	reply := r.control(t, b3ControlRequest{Action: "receipts", Peer: peer})
	receipts := make([]b3Receipt, 0, len(reply.Receipts))
	last := make(map[uint32]uint64)
	for _, raw := range reply.Receipts {
		var rec b3Receipt
		if b3Decode(strings.NewReader(string(raw)), 4096, &rec) != nil ||
			rec.Version != 1 || rec.Nonce != r.q.Nonce || rec.Candidate != r.q.Candidate ||
			rec.DB != target.DatabaseOID || rec.Role != target.WriterOID || rec.Peer != peer ||
			rec.Backend == 0 || rec.Seq != last[rec.Backend]+1 || rec.Deadline != r.deadline || rec.At == 0 || !rec.validOrigin() {
			t.Fatal("server receipt does not bind exact case, writer, backend and deadline")
		}
		last[rec.Backend] = rec.Seq
		receipts = append(receipts, rec)
	}
	return receipts
}

func (r b3RunCase) receipt(t *testing.T, peer, backend uint32, xid uint64, event, detail string) b3Receipt {
	t.Helper()
	for {
		for _, rec := range r.receipts(t, peer) {
			if (backend == 0 || rec.Backend == backend) && (xid == 0 || rec.XID == xid) &&
				rec.Event == event && rec.Detail == detail {
				return rec
			}
		}
		if !b3Poll(r.ctx) {
			t.Fatalf("missing server witness %s/%s; retain target fenced", event, detail)
		}
	}
}

func b3Poll(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(15 * time.Millisecond):
		return true
	}
}

func (r b3RunCase) crossExpiry(t *testing.T) {
	t.Helper()
	for {
		now, err := b3MonotonicNS()
		if err != nil {
			t.Fatal("could not read the supervisor/server monotonic time domain")
		}
		if now >= r.deadline {
			return
		}
		if !b3Poll(r.ctx) {
			t.Fatal("expiry witness did not complete; retain target fenced")
		}
	}
}

func (r b3RunCase) release(t *testing.T, barrier b3Receipt) {
	t.Helper()
	r.control(t, b3ControlRequest{Action: "release", Peer: barrier.Peer, Backend: barrier.Backend, Seq: barrier.Seq})
}

func b3RunMissingAdmission(t *testing.T, ctx context.Context, q *b3Qualification, name, reason string) {
	t.Helper()
	r := b3BeginCase(t, ctx, q, name)
	obs := r.observer(t)
	b3ObserveWorkAbsent(t, ctx, obs, false)
	c, err := q.connect(ctx, name, "writer_live")
	if c != nil {
		_ = c.Close(ctx)
	}
	if !b3Denied(err) {
		t.Fatal("unqualified writer authentication did not return the guarded denial")
	}
	rec := r.receipt(t, r.peer, 0, 0, "connect", reason)
	if rec.At >= r.deadline || rec.XID != 0 {
		t.Fatal("missing target condition was not isolated under otherwise live authority")
	}
	b3ObserveWorkAbsent(t, ctx, obs, false)
}

func b3SendWire(c *pgx.Conn, message pgproto3.FrontendMessage, flush bool) (pgproto3.BackendMessage, error) {
	f := c.PgConn().Frontend()
	f.Send(message)
	if flush {
		f.Send(&pgproto3.Flush{})
	}
	if err := f.Flush(); err != nil {
		return nil, err
	}
	return f.Receive()
}

func b3RunProtocol(t *testing.T, ctx context.Context, q *b3Qualification, name, late string) {
	t.Helper()
	r := b3BeginCase(t, ctx, q, name)
	obs := r.observer(t)
	b3ObserveWorkAbsent(t, ctx, obs, false)
	c, pid, xid := r.writer(t)
	dl, _ := ctx.Deadline()
	if c.PgConn().SyncConn(ctx) != nil || c.PgConn().Conn().SetDeadline(dl) != nil {
		t.Fatal("could not bound the authenticated protocol probe")
	}
	parse := &pgproto3.Parse{Name: "b3_probe", Query: b3InsertWork}
	bind := &pgproto3.Bind{DestinationPortal: "b3_probe", PreparedStatement: "b3_probe"}
	if late == "bind" || late == "execute" {
		msg, err := b3SendWire(c, parse, true)
		_, ok := msg.(*pgproto3.ParseComplete)
		if err != nil || !ok || r.receipt(t, r.peer, pid, xid, "dispatch", "parse").At >= r.deadline {
			t.Fatal("extended Parse prerequisite was not processed before expiry")
		}
	}
	if late == "execute" {
		msg, err := b3SendWire(c, bind, true)
		_, ok := msg.(*pgproto3.BindComplete)
		if err != nil || !ok || r.receipt(t, r.peer, pid, xid, "dispatch", "bind").At >= r.deadline {
			t.Fatal("extended Bind prerequisite was not processed before expiry")
		}
	}
	var lateMsg pgproto3.FrontendMessage
	switch late {
	case "query":
		lateMsg = &pgproto3.Query{String: b3InsertWork}
	case "parse":
		lateMsg = parse
	case "bind":
		lateMsg = bind
	case "execute":
		lateMsg = &pgproto3.Execute{Portal: "b3_probe"}
	default:
		t.Fatal("unknown approved frontend message")
	}
	r.crossExpiry(t)
	msg, err := b3SendWire(c, lateMsg, false)
	denial, ok := msg.(*pgproto3.ErrorResponse)
	if err != nil || !ok || denial.Code != "42501" {
		t.Fatal("authenticated frontend message did not receive the guard denial")
	}
	if r.receipt(t, r.peer, pid, xid, "dispatch_denied", late).At < r.deadline {
		t.Fatal("frontend denial did not occur at or after expiry")
	}
	_ = c.Close(ctx)
	b3ObserveOutcome(t, ctx, obs, xid, "aborted")
	b3ObserveWorkAbsent(t, ctx, obs, false)
}

func b3RunDeferred(t *testing.T, ctx context.Context, q *b3Qualification) {
	t.Helper()
	r := b3BeginCase(t, ctx, q, b3ExpiredDeferred)
	obs := r.observer(t)
	var trigger bool
	if err := obs.QueryRow(ctx, b3DeferredCatalog).Scan(&trigger); err != nil || !trigger {
		t.Fatal("real initially deferred constraint trigger is not preowned")
	}
	b3ObserveWorkAbsent(t, ctx, obs, true)
	c, pid, xid := r.writer(t)
	if tag, err := b3WriterSQL(ctx, c, b3InsertWork); err != nil || tag.RowsAffected() != 1 {
		t.Fatal("real write did not queue deferred trigger work")
	}
	if r.receipt(t, r.peer, pid, xid, "write", "rows").At >= r.deadline ||
		r.receipt(t, r.peer, pid, xid, "deferred", "queued").At >= r.deadline {
		t.Fatal("trigger write and actual enqueue were not both under live scope")
	}
	done := make(chan error, 1)
	go func() {
		_, err := b3WriterSQL(ctx, c, b3WriterCommit)
		done <- err
	}()
	barrier := r.receipt(t, r.peer, pid, xid, "barrier", "before_deferred")
	if barrier.At >= r.deadline {
		t.Fatal("backend did not enter the deferred barrier before expiry")
	}
	r.crossExpiry(t)
	r.release(t, barrier)
	select {
	case err := <-done:
		if !b3Denied(err) {
			t.Fatal("deferred transaction did not receive the actual guard denial")
		}
	case <-ctx.Done():
		t.Fatal("deferred transaction unresolved; retain target fenced")
	}
	rec := r.receipt(t, r.peer, pid, xid, "deferred_denied", "batch")
	if rec.At < r.deadline || rec.Seq <= barrier.Seq {
		t.Fatal("deferred denial did not follow the trusted barrier and cutoff")
	}
	b3ObserveOutcome(t, ctx, obs, xid, "aborted")
	for _, rec := range r.receipts(t, r.peer) {
		if rec.Backend == pid && rec.XID == xid && (rec.Event == "decision" ||
			(rec.Event == "deferred" && (rec.Detail == "entry" || rec.Detail == "batch"))) {
			t.Fatal("expired deferred work entered or reached PRE_COMMIT")
		}
	}
	b3ObserveWorkAbsent(t, ctx, obs, true)
}

func b3RunExistingSequences(t *testing.T, ctx context.Context, q *b3Qualification) {
	t.Helper()
	r := b3BeginCase(t, ctx, q, b3ExistingWrites)
	obs := r.observer(t)
	base := b3ObserveSequence(t, ctx, obs, b3ExistingSequence)
	rows := b3ObserveExistingRows(t, ctx, obs)
	if base.owned != "a" || base.state.cache != 1 || !base.state.called || len(rows) == 0 {
		t.Fatal("existing sequence negative control is not preowned and seeded")
	}
	for _, attempt := range []struct{ sql, reason string }{
		{b3Setval, "direct_setval"}, {b3Nextval, "direct_nextval"}, {b3Undeclared, "undeclared_implicit"},
	} {
		c, pid, xid := r.writer(t)
		_, err := b3WriterSQL(ctx, c, attempt.sql)
		if !b3Denied(err) {
			t.Fatalf("actual existing sequence write was not denied: %s", attempt.reason)
		}
		denial := r.receipt(t, r.peer, pid, xid, "sequence_denied", attempt.reason)
		if denial.At >= r.deadline {
			t.Fatal("sequence denial did not isolate the allocator under a live grant")
		}
		b3AssertDeniedSequenceOrigin(t, r, denial, base)
		_ = c.Close(ctx)
		b3ObserveOutcome(t, ctx, obs, xid, "aborted")
		if b3ObserveSequence(t, ctx, obs, b3ExistingSequence) != base ||
			!slices.Equal(b3ObserveExistingRows(t, ctx, obs), rows) {
			t.Fatal("denied existing sequence write changed owned state, cache parameters or committed rows")
		}
	}
}

func b3AssertDeniedSequenceOrigin(t *testing.T, r b3RunCase, denial b3Receipt, base b3SequenceObservation) {
	t.Helper()
	table, column, kind := base.tableOID, base.column, "serial"
	switch denial.Detail {
	case "direct_setval":
		// setval has a separate guard and does not carry nextval-origin fields.
		kind = ""
	case "direct_nextval":
		table, column, kind = 0, 0, "direct_nextval"
	}
	if kind != "" && !denial.allocationOrigin(table, column, base.sequenceOID) {
		t.Fatal("sequence denial did not identify the expected allocator origin")
	}
	var attempted bool
	for _, rec := range r.receipts(t, r.peer) {
		if rec.Backend != denial.Backend || rec.XID != denial.XID {
			continue
		}
		switch rec.Event {
		case "allocation":
			t.Fatal("denied sequence control accepted an allocation")
		case "allocation_attempt":
			if kind == "" || attempted || rec.Detail != kind || rec.Seq >= denial.Seq || rec.At > denial.At ||
				!rec.allocationOrigin(table, column, base.sequenceOID) {
				t.Fatal("sequence allocator attempt did not match its exact denial")
			}
			attempted = true
		}
	}
	if (kind != "") != attempted {
		t.Fatal("sequence allocator attempt witness is missing")
	}
}

type b3RestoreEvidence struct {
	caseName string
	xid      uint64
	state    [2]b3SequenceObservation
}

func b3RestoreState(kind string, detail string) (b3SequenceObservation, bool) {
	var s b3SequenceObservation
	parts := strings.Split(detail, ":")
	if len(parts) != 6 || parts[0] != kind || parts[5] != "1" {
		return s, false
	}
	table, e1 := strconv.ParseUint(parts[1], 10, 32)
	column, e2 := strconv.ParseInt(parts[2], 10, 16)
	seq, e3 := strconv.ParseUint(parts[3], 10, 32)
	last, e4 := strconv.ParseInt(parts[4], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || table == 0 || seq == 0 || table == seq || column != 1 {
		return s, false
	}
	s.tableOID, s.sequenceOID, s.column = uint32(table), uint32(seq), int16(column)
	s.state.last, s.state.called = last, true
	return s, true
}

var b3RestoredKinds = [...]struct {
	kind, ownership              string
	name                         b3SequenceName
	start, increment, last, next int64
}{
	{"identity", "i", b3IdentitySequence, 17, 5, 22, 27},
	{"serial", "a", b3SerialSequence, 29, 7, 36, 43},
}

func b3ReadRestoreWrites(t *testing.T, r b3RunCase, barrier b3Receipt, commit bool, launch b3ControlReply) b3RestoreEvidence {
	t.Helper()
	e := b3RestoreEvidence{caseName: r.name, xid: barrier.XID}
	resets := map[string]bool{"statement_timeout": false, "lock_timeout": false,
		"idle_in_transaction_session_timeout": false, "transaction_timeout": false}
	if launch.MetadataCount < 9 || launch.MetadataCount > 13 || launch.WriteCount != len(b3RestoreStructuralSQL()) ||
		len(launch.CopyStreams) != len(b3RestoredKinds) {
		t.Fatal("approved archive metadata, structural writes, or COPY streams are incomplete")
	}
	metadata, writes := make([]bool, launch.MetadataCount), make([]bool, launch.WriteCount)
	var ddl, copies, copyProofs int
	var decision bool
	receipts := r.receipts(t, barrier.Peer)
	for i, rec := range receipts {
		if rec.Backend != barrier.Backend || rec.XID != barrier.XID || rec.Seq >= barrier.Seq {
			continue
		}
		if rec.At >= r.deadline {
			t.Fatal("actual archive work crossed the writer cutoff")
		}
		switch rec.Event {
		case "timeout_reset":
			if done, wanted := resets[rec.Detail]; !wanted || done {
				t.Fatal("unexpected or duplicate actual pg_restore timeout reset")
			}
			resets[rec.Detail] = true
		case "archive_copy":
			copyProofs++
		case "write":
			switch rec.Detail {
			case "ddl":
				ddl++
			case "copy":
				if i < 2 || copies >= len(launch.CopyStreams) {
					t.Fatal("actual pg_restore COPY has no matching stream validation")
				}
				want := launch.CopyStreams[copies]
				proof, rule := receipts[i-2], receipts[i-1]
				if proof.Backend != rec.Backend || proof.XID != rec.XID || proof.Event != "archive_copy" ||
					proof.Detail != fmt.Sprintf("%d:%d:%s", want.Rule, want.Bytes, want.SHA256) ||
					rule.Backend != rec.Backend || rule.XID != rec.XID || rule.Event != "archive_write" ||
					rule.Detail != strconv.Itoa(want.Rule) {
					t.Fatal("actual pg_restore COPY stream does not match the approved archive write")
				}
				copies++
			}
		case "decision":
			if rec.Detail != "admit" || decision || copies != len(launch.CopyStreams) {
				t.Fatal("unexpected actual restore PRE_COMMIT receipt")
			}
			decision = true
		case "archive_metadata":
			b3MarkArchiveRule(t, metadata, rec.Detail)
		case "archive_write":
			b3MarkArchiveRule(t, writes, rec.Detail)
		case "archive_state":
			for i, kind := range b3RestoredKinds {
				if state, ok := b3RestoreState(kind.kind, rec.Detail); ok {
					if e.state[i].sequenceOID != 0 || state.state.last != kind.last {
						t.Fatal("real archive state entry did not apply exact approved state once")
					}
					e.state[i] = state
				}
			}
		}
	}
	for _, reset := range resets {
		if !reset {
			t.Fatal("actual pg_restore transaction did not reset every client timeout before cutoff")
		}
	}
	for _, group := range [][]bool{metadata, writes} {
		for _, ran := range group {
			if !ran {
				t.Fatal("an approved archive statement did not execute on the restore transaction")
			}
		}
	}
	if ddl == 0 || copies != len(launch.CopyStreams) || copyProofs != copies || decision != commit ||
		e.state[0].sequenceOID == 0 || e.state[1].sequenceOID == 0 ||
		e.state[0].sequenceOID == e.state[1].sequenceOID || e.state[0].tableOID == e.state[1].tableOID {
		t.Fatal("actual restore did not perform exact writes and both archive state entries before barrier")
	}
	return e
}

func b3MarkArchiveRule(t *testing.T, seen []bool, detail string) {
	t.Helper()
	index, err := strconv.Atoi(detail)
	if err != nil || index <= 0 || index > len(seen) || seen[index-1] {
		t.Fatal("unexpected or duplicate exact archive statement execution")
	}
	seen[index-1] = true
}

type b3RetiredSession struct {
	conn *pgx.Conn
	pid  uint32
	xid  uint64
}

func (r b3RunCase) retired(t *testing.T) b3RetiredSession {
	t.Helper()
	conn, pid, xid := r.writer(t)
	return b3RetiredSession{conn, pid, xid}
}

func (r b3RunCase) refuseRetired(t *testing.T, obs pgx.Tx, stale, cleanup b3RetiredSession) {
	t.Helper()
	for _, check := range []struct {
		b3RetiredSession
		sql string
	}{{stale, b3StaleUse}, {cleanup, b3StaleCleanup}} {
		if _, err := b3WriterSQL(r.ctx, check.conn, check.sql); !b3Denied(err) {
			t.Fatal("retired writer reuse or cleanup did not receive exact guard denial")
		}
		if r.receipt(t, r.peer, check.pid, check.xid, "dispatch_denied", "query").At < r.deadline {
			t.Fatal("retired writer denial was not after cutoff")
		}
		_ = check.conn.Close(r.ctx)
		b3ObserveOutcome(t, r.ctx, obs, check.xid, "aborted")
	}
}

func (r b3RunCase) refuseReconnect(t *testing.T, expected int) {
	t.Helper()
	c, err := r.q.connect(r.ctx, r.name, "writer_expired")
	if c != nil {
		_ = c.Close(r.ctx)
	}
	if !b3Denied(err) {
		t.Fatal("retired target did not deny actual writer reconnect")
	}
	count := 0
	for _, rec := range r.receipts(t, r.peer) {
		if rec.Event == "connect" && rec.Detail == "expired" {
			if rec.At < r.deadline || rec.XID != 0 {
				t.Fatal("retired reconnect denial was not at a new guarded startup after cutoff")
			}
			count++
		}
	}
	if count != expected {
		t.Fatal("missing distinct post-cutoff guarded reconnect denial")
	}
}

func b3RunRestore(t *testing.T, ctx context.Context, q *b3Qualification, name string, commit bool) b3RestoreEvidence {
	t.Helper()
	r := b3BeginCase(t, ctx, q, name)
	obs := r.observer(t)
	b3ObserveArchiveAbsent(t, ctx, obs)
	b3ObserveWorkAbsent(t, ctx, obs, false)
	base := b3ObserveSequence(t, ctx, obs, b3ExistingSequence)
	rows := b3ObserveExistingRows(t, ctx, obs)
	stale, cleanup := r.retired(t), r.retired(t)
	launch := r.control(t, b3ControlRequest{Action: "restore"})
	peer := launch.Peer
	if peer == 0 || peer == r.peer || stale.pid == cleanup.pid || stale.xid == cleanup.xid {
		t.Fatal("actual restore and retired-session controls do not have distinct identities")
	}
	stage := "before_decision"
	if commit {
		stage = "after_decision"
	}
	barrier := r.receipt(t, peer, 0, 0, "barrier", stage)
	if barrier.At >= r.deadline || barrier.XID == 0 ||
		r.receipt(t, peer, barrier.Backend, 0, "connect", "admit").At >= r.deadline {
		t.Fatal("actual restore did not authenticate and reach its selected barrier under live authority")
	}
	e := b3ReadRestoreWrites(t, r, barrier, commit, launch)
	r.crossExpiry(t)
	var state string
	if err := obs.QueryRow(ctx, b3XactStatus, b3XIDText(barrier.XID)).Scan(&state); err != nil || state != "in progress" {
		t.Fatal("independent observer did not see actual restore unresolved across cutoff")
	}
	b3ObserveArchiveAbsent(t, ctx, obs)
	r.refuseRetired(t, obs, stale, cleanup)
	r.refuseReconnect(t, 1)
	r.release(t, barrier)
	exit := r.control(t, b3ControlRequest{Action: "wait_restore", Peer: peer}).Exit
	if exit == nil || (commit && *exit != 0) || (!commit && *exit != 1) {
		t.Fatal("actual pg_restore did not reach expected non-watchdog terminal status")
	}
	event, detail, outcome := "decision", "expired", "aborted"
	if commit {
		event, detail, outcome = "commit_record", "ready", "committed"
	}
	terminal := r.receipt(t, peer, barrier.Backend, barrier.XID, event, detail)
	if terminal.At < r.deadline || terminal.Seq <= barrier.Seq {
		t.Fatal("actual restore after-cutoff processing did not follow trusted barrier")
	}
	b3ObserveOutcome(t, ctx, obs, barrier.XID, outcome)
	r.refuseReconnect(t, 2)
	if commit {
		b3AssertObserver(t, ctx, obs, r.identity())
		b3ObserveArchiveLedger(t, ctx, obs)
	} else {
		b3ObserveArchiveAbsent(t, ctx, obs, e.state[0].tableOID, e.state[0].sequenceOID, e.state[1].tableOID, e.state[1].sequenceOID)
	}
	if b3ObserveSequence(t, ctx, obs, b3ExistingSequence) != base || !slices.Equal(b3ObserveExistingRows(t, ctx, obs), rows) {
		t.Fatal("actual archive changed a pre-existing external sequence or rows")
	}
	b3ObserveWorkAbsent(t, ctx, obs, false)
	return e
}

func b3ConfirmRolledBackSequence(t *testing.T, ctx context.Context, q *b3Qualification, e b3RestoreEvidence, i int) {
	t.Helper()
	r := b3RunCase{q: q, ctx: ctx, name: b3RestoreRollback}
	if e.caseName != r.name || e.xid == 0 || e.state[i].sequenceOID == 0 {
		t.Fatal("real rollback did not exercise requested archive sequence kind")
	}
	obs := r.observer(t)
	b3ObserveOutcome(t, ctx, obs, e.xid, "aborted")
	b3ObserveArchiveAbsent(t, ctx, obs, e.state[i].tableOID, e.state[i].sequenceOID)
}

func b3ConfirmRestoredSequence(t *testing.T, ctx context.Context, q *b3Qualification, e b3RestoreEvidence, i int) {
	t.Helper()
	r := b3RunCase{q: q, ctx: ctx, name: b3RestoreTail}
	if e.caseName != r.name || e.xid == 0 {
		t.Fatal("real committed archive witness missing")
	}
	obs := r.observer(t)
	b3ObserveOutcome(t, ctx, obs, e.xid, "committed")
	kind, receipt := b3RestoredKinds[i], e.state[i]
	actual := b3ObserveSequence(t, ctx, obs, kind.name)
	want := b3SequenceState{start: kind.start, increment: kind.increment, min: 1, max: math.MaxInt64,
		cache: 1, last: kind.last, called: true}
	next, valid := actual.state.next()
	if actual.sequenceOID != receipt.sequenceOID || actual.tableOID != receipt.tableOID || actual.column != receipt.column ||
		actual.owned != kind.ownership || actual.state != want || actual.state.last != receipt.state.last ||
		!receipt.state.called || !valid || next != kind.next {
		t.Fatalf("committed %s sequence ownership, exact state or read-only next value disagrees with approved source", kind.kind)
	}
}

type b3ArchiveSourceSequence struct {
	Kind           string   `json:"kind"`
	TableOID       uint32   `json:"table_oid"`
	SequenceOID    uint32   `json:"sequence_oid"`
	Column         int16    `json:"column"`
	CreatedLast    int64    `json:"created_last"`
	CreatedCalled  bool     `json:"created_called"`
	Increment      int64    `json:"increment"`
	Min            int64    `json:"min"`
	Max            int64    `json:"max"`
	Cache          int64    `json:"cache"`
	Cycle          bool     `json:"cycle"`
	ArchivedLast   int64    `json:"archived_last"`
	ArchivedCalled bool     `json:"archived_called"`
	ArchivedNext   int64    `json:"archived_next"`
	SeedXID        uint64   `json:"seed_xid,string"`
	Allocations    []uint64 `json:"allocations_at_ns"`
	Deadline       uint64   `json:"deadline_ns,string"`
}

type b3ArchiveProvenance struct {
	Version                int                       `json:"version"`
	RunNonce               string                    `json:"run_nonce"`
	Candidate              string                    `json:"candidate"`
	SourceRunNonce         string                    `json:"source_run_nonce"`
	SourceApprovalSHA256   string                    `json:"source_approval_sha256"`
	SourceEvidenceSHA256   string                    `json:"source_evidence_sha256"`
	SourceDatabase         string                    `json:"source_database"`
	SourceDatabaseOID      uint32                    `json:"source_database_oid"`
	SourceSystemIdentifier string                    `json:"source_system_identifier"`
	SourceWriter           string                    `json:"source_writer"`
	SourceWriterOID        uint32                    `json:"source_writer_oid"`
	DumpSHA256             string                    `json:"pg_dump_sha256"`
	ArchiveSHA256          string                    `json:"archive_sha256"`
	TOCSHA256              string                    `json:"toc_sha256"`
	ScriptSHA256           string                    `json:"script_sha256"`
	DumpArgs               []string                  `json:"dump_args"`
	Sequences              []b3ArchiveSourceSequence `json:"sequences"`
}

func b3ArchiveRules(ctx context.Context, q *b3Qualification) (b3RestoreRules, error) {
	f, _, err := q.Files["archive_provenance"].open()
	if err != nil {
		return b3RestoreRules{}, err
	}
	defer f.Close()
	var source b3ArchiveProvenance
	if b3Decode(f, 16384, &source) != nil || !source.valid(q) || !b3VerifySourceEvidence(q, source) {
		return b3RestoreRules{}, errB3Qualification
	}
	toc, err := q.restoreInspection(ctx, "archive_toc", "--list")
	if err != nil || !b3ValidateArchiveTOC(toc, source) {
		return b3RestoreRules{}, errB3Qualification
	}
	script, err := q.restoreInspection(ctx, "archive_sql", "--single-transaction", "--file=-", "--restrict-key="+b3ArchiveRestrictKey)
	if err != nil {
		return b3RestoreRules{}, errB3Qualification
	}
	return b3RestoreArchiveRules(script, source, q.Cases[b3RestoreTail].Observer)
}

func (source b3ArchiveProvenance) valid(q *b3Qualification) bool {
	rollback, tail := q.Cases[b3RestoreRollback], q.Cases[b3RestoreTail]
	systemID, systemIDErr := strconv.ParseUint(source.SourceSystemIdentifier, 10, 64)
	if source.Version != 1 || source.RunNonce != q.Nonce || source.Candidate != q.Candidate ||
		!runPattern.MatchString(source.SourceRunNonce) || source.SourceRunNonce == q.Nonce ||
		source.SourceApprovalSHA256 != q.Files["source_approval"].SHA256 || source.SourceEvidenceSHA256 != q.Files["source_evidence"].SHA256 ||
		systemIDErr != nil || systemID == 0 ||
		!systemPattern.MatchString(source.SourceSystemIdentifier) || source.SourceSystemIdentifier == q.SystemIdentifier ||
		!b3Name(source.SourceDatabase) ||
		!strings.HasPrefix(source.SourceDatabase, "b3_qual_"+source.SourceRunNonce[:8]+"_") || source.SourceDatabaseOID == 0 ||
		source.SourceWriterOID == 0 || source.SourceWriter != tail.Writer || tail.Writer != rollback.Writer ||
		tail.Observer != rollback.Observer || source.DumpSHA256 != q.Files["pg_dump"].SHA256 ||
		source.ArchiveSHA256 != q.Files["archive"].SHA256 || source.TOCSHA256 != q.Files["archive_toc"].SHA256 ||
		source.ScriptSHA256 != q.Files["archive_sql"].SHA256 ||
		len(source.Sequences) != len(b3RestoredKinds) ||
		!slices.Equal(source.DumpArgs, []string{"--format=custom", "--no-password", "--dbname=" + source.SourceDatabase}) {
		return false
	}
	for _, target := range q.Cases {
		if source.SourceDatabase == target.Database {
			return false
		}
	}
	for i, seq := range source.Sequences {
		kind := b3RestoredKinds[i]
		if seq.Kind != kind.kind || seq.Column != 1 || seq.TableOID == 0 || seq.SequenceOID == 0 || seq.TableOID == seq.SequenceOID ||
			seq.CreatedLast != kind.start || seq.CreatedCalled || seq.Increment != kind.increment || seq.Cache != 1 ||
			seq.Min != 1 || seq.Max != math.MaxInt64 || seq.Cycle || seq.ArchivedLast != kind.last || !seq.ArchivedCalled ||
			seq.CreatedLast == seq.ArchivedLast || seq.ArchivedNext != kind.next || seq.SeedXID == 0 || len(seq.Allocations) != 2 ||
			seq.Allocations[0] == 0 || seq.Allocations[0] > seq.Allocations[1] || seq.Allocations[1] >= seq.Deadline {
			return false
		}
	}
	return source.Sequences[0].TableOID != source.Sequences[1].TableOID && source.Sequences[0].SequenceOID != source.Sequences[1].SequenceOID
}

type b3SourceEvidence struct {
	Version          int               `json:"version"`
	Nonce            string            `json:"nonce"`
	Candidate        string            `json:"candidate"`
	SystemIdentifier string            `json:"system_identifier"`
	DatabaseOID      uint32            `json:"database_oid"`
	ObserverOID      uint32            `json:"observer_oid"`
	Receipts         []b3Receipt       `json:"server_receipts"`
	Outcomes         []b3SourceOutcome `json:"observer_outcomes"`
}

type b3SourceOutcome struct {
	XID    uint64 `json:"xid,string"`
	At     uint64 `json:"monotonic_ns,string"`
	Status string `json:"status"`
}

func b3VerifySourceEvidence(q *b3Qualification, source b3ArchiveProvenance) bool {
	var proof b3SourceEvidence
	if q.decodePinned("source_evidence", &proof) != nil || proof.Version != 1 || proof.Nonce != source.SourceRunNonce ||
		proof.Candidate != source.Candidate || proof.SystemIdentifier != source.SourceSystemIdentifier ||
		proof.DatabaseOID != source.SourceDatabaseOID || proof.ObserverOID == 0 || proof.ObserverOID == source.SourceWriterOID {
		return false
	}
	for _, seq := range source.Sequences {
		var backend, peer uint32
		var at, decision, record, lastSeq, attestedAttempt uint64
		var allocation []uint64
		for _, rec := range proof.Receipts {
			if rec.Version != 1 || rec.Nonce != proof.Nonce || rec.Candidate != proof.Candidate || rec.DB != proof.DatabaseOID ||
				rec.Role != source.SourceWriterOID || rec.Peer == 0 || rec.Backend == 0 || rec.Seq == 0 || !rec.validOrigin() {
				return false
			}
			if rec.XID != seq.SeedXID {
				continue
			}
			if rec.At == 0 || rec.At < at || rec.Seq <= lastSeq || rec.At >= seq.Deadline || rec.Deadline != seq.Deadline ||
				(backend != 0 && (rec.Backend != backend || rec.Peer != peer)) {
				return false
			}
			backend, peer, at, lastSeq = rec.Backend, rec.Peer, rec.At, rec.Seq
			switch rec.Event {
			case "allocation_attempt", "allocation":
				if !b3SourceAllocationOrigin(source.Sequences, rec) {
					return false
				}
				if rec.Detail != seq.Kind {
					continue
				}
				if rec.Event == "allocation_attempt" {
					if attestedAttempt != 0 || decision != 0 {
						return false
					}
					attestedAttempt = rec.At
				} else {
					if attestedAttempt == 0 || rec.At < attestedAttempt || decision != 0 {
						return false
					}
					attestedAttempt = 0
					allocation = append(allocation, rec.At)
				}
			case "sequence_denied":
				return false
			case "decision":
				if decision != 0 || rec.Detail != "admit" || attestedAttempt != 0 {
					return false
				}
				decision = rec.At
			case "commit_record":
				if decision == 0 || record != 0 || rec.Detail != "ready" {
					return false
				}
				record = rec.At
			}
		}
		if attestedAttempt != 0 || !slices.Equal(allocation, seq.Allocations) || decision <= seq.Allocations[1] || record < decision {
			return false
		}
		matched := false
		for _, outcome := range proof.Outcomes {
			if outcome.XID == seq.SeedXID && outcome.Status == "committed" && outcome.At >= record {
				if matched {
					return false
				}
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func b3ValidateArchiveTOC(toc []byte, source b3ArchiveProvenance) bool {
	var databaseOK, serverOK, toolOK, custom bool
	entries := map[string]int{}
	entryIDs := make(map[int]bool)
	objects := make(map[string]uint32, 2*len(b3RestoredKinds))
	for i, kind := range b3RestoredKinds {
		objects["TABLE "+kind.name.schema+" "+kind.name.table] = source.Sequences[i].TableOID
		objects["SEQUENCE "+kind.name.schema+" "+kind.name.sequence] = source.Sequences[i].SequenceOID
	}
	for _, line := range strings.Split(string(toc), "\n") {
		switch {
		case line == ";     dbname: "+source.SourceDatabase:
			databaseOK = true
		case line == ";     Format: CUSTOM":
			custom = true
		case line == ";     Dumped from database version: 18.7":
			serverOK = true
		case line == ";     Dumped by pg_dump version: 18.7":
			toolOK = true
		case line == "" || strings.HasPrefix(line, ";"):
			continue
		default:
			id, tail, ok := strings.Cut(line, ";")
			n, idErr := strconv.Atoi(id)
			fields := strings.Fields(tail)
			if !ok || idErr != nil || n <= 0 || entryIDs[n] || len(fields) < 6 || fields[len(fields)-1] != source.SourceWriter {
				return false
			}
			entryIDs[n] = true
			entry := strings.Join(fields[2:len(fields)-1], " ")
			if oid, pinned := objects[entry]; pinned && (fields[0] != "1259" || fields[1] != strconv.FormatUint(uint64(oid), 10)) {
				return false
			}
			entries[entry]++
		}
	}
	required := [...]string{
		"SCHEMA - b3_qual_archive", "TABLE b3_qual_archive identity_rows", "TABLE b3_qual_archive serial_rows",
		"TABLE DATA b3_qual_archive identity_rows", "TABLE DATA b3_qual_archive serial_rows",
		"SEQUENCE b3_qual_archive identity_rows_id_seq", "SEQUENCE b3_qual_archive serial_rows_id_seq",
		"SEQUENCE SET b3_qual_archive identity_rows_id_seq", "SEQUENCE SET b3_qual_archive serial_rows_id_seq",
	}
	for _, entry := range required {
		if entries[entry] != 1 {
			return false
		}
		delete(entries, entry)
	}
	for entry, n := range entries {
		switch entry {
		case "SEQUENCE OWNED BY b3_qual_archive serial_rows_id_seq", "DEFAULT b3_qual_archive serial_rows id",
			"ACL - SCHEMA b3_qual_archive", "ACL b3_qual_archive TABLE identity_rows", "ACL b3_qual_archive TABLE serial_rows",
			"ACL b3_qual_archive SEQUENCE identity_rows_id_seq", "ACL b3_qual_archive SEQUENCE serial_rows_id_seq":
			if n != 1 {
				return false
			}
		default:
			return false
		}
	}
	return custom && databaseOK && serverOK && toolOK
}

func b3RestoreMetadata(writer, observer string) (required, optional []string) {
	required = []string{"CREATE SCHEMA b3_qual_archive", "ALTER SCHEMA b3_qual_archive OWNER TO " + writer,
		"GRANT USAGE ON SCHEMA b3_qual_archive TO " + observer}
	for _, kind := range b3RestoredKinds {
		table := b3ArchiveSchema + "." + kind.name.table
		seq := b3ArchiveSchema + "." + kind.name.sequence
		required = append(required, "ALTER TABLE "+table+" OWNER TO "+writer,
			"GRANT SELECT ON TABLE "+table+" TO "+observer, "GRANT SELECT ON SEQUENCE "+seq+" TO "+observer)
		optional = append(optional, "ALTER SEQUENCE "+seq+" OWNER TO "+writer,
			"ALTER TABLE "+seq+" OWNER TO "+writer)
	}
	return required, optional
}

func b3RestoreArchiveRules(script []byte, source b3ArchiveProvenance, observer string) (b3RestoreRules, error) {
	required, optional := b3RestoreMetadata(source.SourceWriter, observer)
	writes := b3RestoreStructuralSQL()
	seen := make(map[string]bool, len(required)+len(optional)+len(writes))
	var rules b3RestoreRules
	var stateRules [2]string
	var pending strings.Builder
	for len(script) != 0 {
		rawLine, rest, hasLF := bytes.Cut(script, []byte{'\n'})
		script = rest
		line := string(rawLine)
		trimmed := strings.TrimSpace(line)
		if pending.Len() == 0 && (trimmed == "" || strings.HasPrefix(trimmed, "--") ||
			trimmed == `\restrict `+b3ArchiveRestrictKey || trimmed == `\unrestrict `+b3ArchiveRestrictKey) {
			continue
		}
		pending.WriteString(line)
		if !strings.HasSuffix(trimmed, ";") {
			pending.WriteByte('\n')
			continue
		}
		sql := strings.TrimSuffix(strings.TrimSpace(pending.String()), ";")
		pending.Reset()
		// Validate the complete statement without changing bytes admitted by the guard.
		form := strings.Join(strings.Fields(sql), " ")
		if strings.ContainsRune(form, ';') {
			return b3RestoreRules{}, errB3Qualification
		}
		switch {
		case slices.Contains(required, form) || slices.Contains(optional, form):
			if seen[form] {
				return b3RestoreRules{}, errB3Qualification
			}
			rules.grant = append(rules.grant, fmt.Sprintf("query\t%x", sql))
			rules.metadataCount++
			seen[form] = true
		case writes[form] != nil:
			if seen[form] {
				return b3RestoreRules{}, errB3Qualification
			}
			seen[form] = true
			rules.writeCount++
			if len(writes[form]) == 0 {
				rules.grant = append(rules.grant, fmt.Sprintf("write\t%x", sql))
				continue
			}
			if !hasLF || line != form+";" || sql != form {
				return b3RestoreRules{}, errB3Qualification
			}
			payload := script
			for _, wantRow := range writes[form] {
				row, rest, hasLF := bytes.Cut(script, []byte{'\n'})
				if !hasLF || string(row) != wantRow {
					return b3RestoreRules{}, errB3Qualification
				}
				script = rest
			}
			// Stock PG18.7 sends the entire archived trailer as CopyData, then sends CopyDone separately.
			rest, ok := bytes.CutPrefix(script, []byte("\\.\n\n\n"))
			if !ok {
				return b3RestoreRules{}, errB3Qualification
			}
			script = rest
			payload = payload[:len(payload)-len(script)]
			digest := sha256.Sum256(payload)
			stream := b3CopyStream{Rule: rules.writeCount, Bytes: len(payload), SHA256: hex.EncodeToString(digest[:])}
			rules.copies = append(rules.copies, stream)
			rules.grant = append(rules.grant, fmt.Sprintf("copy\t%x\t%d\t%s", sql, stream.Bytes, stream.SHA256))
		case strings.HasPrefix(form, "SELECT pg_catalog.setval("):
			if !b3ArchiveStateRule(sql, form, &stateRules) {
				return b3RestoreRules{}, errB3Qualification
			}
		case b3RestoreScaffolding(form):
		default:
			return b3RestoreRules{}, errB3Qualification
		}
	}
	for _, sql := range required {
		if !seen[sql] {
			return b3RestoreRules{}, errB3Qualification
		}
	}
	for sql := range writes {
		if !seen[sql] {
			return b3RestoreRules{}, errB3Qualification
		}
	}
	if pending.Len() != 0 || len(rules.copies) != len(b3RestoredKinds) || stateRules[0] == "" || stateRules[1] == "" {
		return b3RestoreRules{}, errB3Qualification
	}
	rules.grant = append(rules.grant, stateRules[:]...)
	return rules, nil
}

func b3RestoreStructuralSQL() map[string][]string {
	ddl := []string{}
	identity := "b3_qual_archive.identity_rows"
	serial := "b3_qual_archive.serial_rows"
	identitySeq, serialSeq := identity+"_id_seq", serial+"_id_seq"
	return map[string][]string{
		"CREATE TABLE " + identity + " ( id bigint NOT NULL, payload text NOT NULL )": ddl,
		"CREATE TABLE " + serial + " ( id bigint NOT NULL, payload text NOT NULL )":   ddl,
		"ALTER TABLE " + identity + " ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY ( SEQUENCE NAME " + identitySeq +
			" START WITH 17 INCREMENT BY 5 NO MINVALUE NO MAXVALUE CACHE 1 )": ddl,
		"CREATE SEQUENCE " + serialSeq + " START WITH 29 INCREMENT BY 7 NO MINVALUE NO MAXVALUE CACHE 1":     ddl,
		"ALTER SEQUENCE " + serialSeq + " OWNED BY " + serial + ".id":                                        ddl,
		"ALTER TABLE ONLY " + serial + " ALTER COLUMN id SET DEFAULT nextval('" + serialSeq + "'::regclass)": ddl,
		"COPY " + identity + " (id, payload) FROM stdin":                                                     {"17\tidentity-first", "22\tidentity-second"},
		"COPY " + serial + " (id, payload) FROM stdin":                                                       {"29\tserial-first", "36\tserial-second"},
	}
}

func b3ArchiveStateRule(sql, form string, rules *[2]string) bool {
	for i, kind := range b3RestoredKinds {
		want := fmt.Sprintf("SELECT pg_catalog.setval('%s.%s', %d, true)", kind.name.schema, kind.name.sequence, kind.last)
		if form == want && rules[i] == "" {
			rules[i] = fmt.Sprintf("archive\t%s\t%s\tid\t%s\t%s\t%d\t1\t%x",
				kind.name.schema, kind.name.table, kind.name.sequence, kind.kind, kind.last, sql)
			return true
		}
	}
	return false
}

func b3RestoreScaffolding(sql string) bool {
	switch sql {
	case "BEGIN", "COMMIT", "SET statement_timeout = 0", "SET lock_timeout = 0", "SET idle_in_transaction_session_timeout = 0",
		"SET transaction_timeout = 0", "SET client_encoding = 'UTF8'", "SET standard_conforming_strings = on",
		"SELECT pg_catalog.set_config('search_path', '', false)", "SET check_function_bodies = false",
		"SET xmloption = content", "SET client_min_messages = warning", "SET row_security = off",
		"SET default_tablespace = ''", "SET default_table_access_method = heap":
		return true
	}
	return false
}

func b3SourceAllocationOrigin(sequences []b3ArchiveSourceSequence, rec b3Receipt) bool {
	for _, seq := range sequences {
		if seq.Kind == rec.Detail && seq.SeedXID == rec.XID &&
			rec.allocationOrigin(seq.TableOID, seq.Column, seq.SequenceOID) {
			return true
		}
	}
	return false
}
