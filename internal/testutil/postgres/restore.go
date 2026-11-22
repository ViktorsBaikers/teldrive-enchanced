package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RestoreTarget contains independent catalog observations, not caller authority.
// A restore always creates a distinct database; even an empty existing target fails.
type RestoreTarget struct {
	RunID          string
	SourceDatabase string
	SourceOID      int64
	TargetDatabase string
	TargetOID      int64
	TargetObjects  int64
}

func GuardRestore(a Approval, rawURL string, target RestoreTarget, now time.Time, identity *InstanceIdentity, tools ToolContext, mutate func() error) error {
	if target.RunID != a.RunID || target.SourceDatabase == target.TargetDatabase || target.SourceOID <= 0 ||
		target.TargetOID != 0 || target.TargetObjects != 0 || a.ValidateTarget(rawURL, target.SourceDatabase, now) != nil {
		return errIsolation
	}
	return a.Guard(rawURL, target.TargetDatabase, now, identity, tools, mutate)
}

// RestoreInput is root-written private authority for a finite set of fresh DBs.
// Docker is an absolute, checksum-bound installed executable, never PATH discovery.
type RestoreInput struct {
	Version         int      `json:"version"`
	RunID           string   `json:"run_id"`
	SourceDatabase  string   `json:"source_database"`
	TargetDatabases []string `json:"target_databases"`
	Docker          string   `json:"docker"`
	DockerSHA256    string   `json:"docker_sha256"`
	Directory       string   `json:"directory"`
}

func decodePrivate(path string, out any) ([32]byte, error) {
	data, err := readPrivate(path)
	if err != nil || uniqueJSON(json.NewDecoder(bytes.NewReader(data))) != nil {
		return [32]byte{}, errIsolation
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return [32]byte{}, errIsolation
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return [32]byte{}, errIsolation
	}
	return sha256.Sum256(data), nil
}

func LoadRestoreInput(path string, a Approval, rawURL string, now time.Time) (*RestoreInput, error) {
	var in RestoreInput
	if _, err := decodePrivate(path, &in); err != nil {
		return nil, err
	}
	if in.Version != 1 || in.RunID != a.RunID || len(in.TargetDatabases) == 0 || len(in.TargetDatabases) > 128 ||
		!filepath.IsAbs(in.Docker) || filepath.Clean(in.Docker) != in.Docker ||
		!idPattern.MatchString(in.DockerSHA256) || in.Directory != filepath.Dir(path) ||
		a.ValidateTarget(rawURL, in.SourceDatabase, now) != nil {
		return nil, errIsolation
	}
	info, err := os.Lstat(in.Directory)
	if err != nil || !privateInfo(info, true) {
		return nil, errIsolation
	}
	seen := map[string]bool{in.SourceDatabase: true}
	for _, name := range in.TargetDatabases {
		if seen[name] || a.ValidateTarget(rawURL, name, now) != nil {
			return nil, errIsolation
		}
		seen[name] = true
	}
	return &in, nil
}

// Snapshot binds a real custom-format dump to one observed source identity.
// It is reusable only within the same private run and explicitly allowed targets.
type Snapshot struct {
	Version          int         `json:"version"`
	RunID            string      `json:"run_id"`
	SourceDatabase   string      `json:"source_database"`
	SourceOID        int64       `json:"source_oid"`
	SystemIdentifier string      `json:"system_identifier"`
	Role             string      `json:"role"`
	Dump             string      `json:"dump"`
	SHA256           string      `json:"sha256"`
	Size             int64       `json:"size"`
	MigrationVersion int64       `json:"migration_version"`
	Tools            ToolContext `json:"tools"`
}

type restoreOwner struct {
	Version          int    `json:"version"`
	RunID            string `json:"run_id"`
	Database         string `json:"database"`
	SystemIdentifier string `json:"system_identifier"`
	Role             string `json:"role"`
	OID              int64  `json:"oid"`
}

// RestoreFixture owns no workers. Close only closes connections; failed evidence
// is retained until explicit, freshly guarded Drop succeeds.
type RestoreFixture struct {
	Pool         *pgxpool.Pool
	URL          string
	Name         string
	approvalPath string
	inputPath    string
	baseURL      string
	approval     Approval
	input        RestoreInput
	inputDigest  [32]byte
	owner        restoreOwner
}

func OpenRestoreFixture(ctx context.Context, approvalPath, inputPath, rawURL string) (*RestoreFixture, error) {
	a, err := LoadApproval(approvalPath)
	if err != nil {
		return nil, errIsolation
	}
	in, err := LoadRestoreInput(inputPath, *a, rawURL, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "PG") {
			return nil, errIsolation
		}
	}
	var checked RestoreInput
	digest, err := decodePrivate(inputPath, &checked)
	if err != nil || !reflect.DeepEqual(checked, *in) {
		return nil, errIsolation
	}
	f := &RestoreFixture{approvalPath: approvalPath, inputPath: inputPath, baseURL: rawURL,
		approval: *a, input: *in, inputDigest: digest}
	if _, err := f.observeTools(ctx); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *RestoreFixture) current() error {
	a, err := LoadApproval(f.approvalPath)
	if err != nil || a.digest != f.approval.digest {
		return errIsolation
	}
	var in RestoreInput
	digest, err := decodePrivate(f.inputPath, &in)
	if err != nil || digest != f.inputDigest || !reflect.DeepEqual(in, f.input) {
		return errIsolation
	}
	return a.ValidateTarget(f.baseURL, f.input.SourceDatabase, time.Now().UTC())
}

func hashRegular(path string, private bool) (string, int64, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", 0, errIsolation
	}
	for p := filepath.Dir(path); ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			(private && p == filepath.Dir(path) && !privateInfo(info, true)) {
			return "", 0, errIsolation
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || (private && !privateInfo(info, false)) {
		return "", 0, errIsolation
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, errIsolation
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", 0, errIsolation
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(file, (1<<30)+1))
	if err != nil || n > 1<<30 || n != opened.Size() {
		return "", 0, errIsolation
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func (f *RestoreFixture) docker(ctx context.Context, input io.Reader, output io.Writer, password string, args ...string) error {
	if err := f.current(); err != nil {
		return err
	}
	hash, _, err := hashRegular(f.input.Docker, false)
	if err != nil || hash != f.input.DockerSHA256 {
		return errIsolation
	}
	cmd := exec.CommandContext(ctx, f.input.Docker, args...)
	cmd.Env = []string{"DOCKER_HOST=unix:///var/run/docker.sock"}
	if password != "" {
		cmd.Env = append(cmd.Env, "PGPASSWORD="+password)
	}
	cmd.Stdin, cmd.Stdout = input, output
	// Never inherit stderr: Docker/libpq diagnostics can contain credentials.
	cmd.Stderr = io.Discard
	if cmd.Run() != nil {
		return errIsolation
	}
	return nil
}

func (f *RestoreFixture) observeTools(ctx context.Context) (ToolContext, error) {
	var output bytes.Buffer
	if err := f.docker(ctx, nil, &output, "", "inspect", "--type=container", f.approval.Tools.ContainerID); err != nil {
		return ToolContext{}, err
	}
	var rows []struct {
		ID    string `json:"Id"`
		Image string `json:"Image"`
		Name  string `json:"Name"`
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
		NetworkSettings struct {
			Networks map[string]struct {
				NetworkID string `json:"NetworkID"`
				IPAddress string `json:"IPAddress"`
			} `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	if json.Unmarshal(output.Bytes(), &rows) != nil || len(rows) != 1 || !rows[0].State.Running || len(rows[0].NetworkSettings.Networks) != 1 {
		return ToolContext{}, errIsolation
	}
	t := f.approval.Tools
	if rows[0].Name != "/teldrive-audit-b2-"+f.approval.RunID {
		return ToolContext{}, errIsolation
	}
	t.ContainerID, t.ImageID, t.RunTag = rows[0].ID, rows[0].Image, strings.TrimPrefix(rows[0].Name, "/teldrive-audit-b2-")
	for name, network := range rows[0].NetworkSettings.Networks {
		t.NetworkID = network.NetworkID
		if name != "teldrive-b2-"+t.RunTag || network.IPAddress != f.approval.Host {
			return ToolContext{}, errIsolation
		}
	}
	if err := f.approval.ValidateTools(t); err != nil {
		return ToolContext{}, err
	}
	for _, tool := range []struct{ path, name string }{{t.PSQL, "psql"}, {t.PGDump, "pg_dump"}, {t.PGRestore, "pg_restore"}} {
		output.Reset()
		if err := f.docker(ctx, nil, &output, "", "exec", t.ContainerID, tool.path, "--version"); err != nil {
			return ToolContext{}, err
		}
		if strings.TrimSpace(output.String()) != tool.name+" (PostgreSQL) 18.7" {
			return ToolContext{}, errIsolation
		}
	}
	return t, nil
}

func (f *RestoreFixture) connect(ctx context.Context, name string) (*pgx.Conn, error) {
	u, err := url.Parse(f.baseURL)
	if err != nil {
		return nil, errIsolation
	}
	u.Path = "/" + name
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, errIsolation
	}
	cfg.Fallbacks = nil
	cfg.ConnectTimeout = 5 * time.Second
	cfg.RuntimeParams = map[string]string{"application_name": "teldrive-b2-restore"}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, errIsolation
	}
	return conn, nil
}

func restoreIdentity(ctx context.Context, conn *pgx.Conn) (*InstanceIdentity, error) {
	var i InstanceIdentity
	err := conn.QueryRow(ctx, `SELECT host(inet_server_addr()), inet_server_port(), current_user,
current_setting('cluster_name'), system_identifier::text, current_database() FROM pg_control_system()`).Scan(
		&i.Host, &i.Port, &i.Role, &i.ClusterName, &i.SystemIdentifier, &i.Database)
	if err != nil {
		return nil, errIsolation
	}
	return &i, nil
}

func (f *RestoreFixture) guard(ctx context.Context, conn *pgx.Conn, name string, mutate func() error) error {
	if err := f.current(); err != nil {
		return err
	}
	tools, err := f.observeTools(ctx)
	if err != nil {
		return err
	}
	i, err := restoreIdentity(ctx, conn)
	if err != nil {
		return err
	}
	return f.approval.Guard(f.baseURL, name, time.Now().UTC(), i, tools, mutate)
}

func (f *RestoreFixture) recordPath(name string) string {
	return filepath.Join(f.input.Directory, name+".restore-owned.json")
}

func writeRestoreJSON(path string, value any) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errIsolation
	}
	defer file.Close()
	if json.NewEncoder(file).Encode(value) != nil || file.Sync() != nil {
		return errIsolation
	}
	return nil
}

func (f *RestoreFixture) verifyOwner(ctx context.Context, conn *pgx.Conn) error {
	var recorded restoreOwner
	if _, err := decodePrivate(f.recordPath(f.Name), &recorded); err != nil || recorded != f.owner || recorded.OID <= 0 {
		return errIsolation
	}
	var oid int64
	var role string
	if err := conn.QueryRow(ctx, "SELECT oid::bigint, pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1", f.Name).Scan(&oid, &role); err != nil || oid != f.owner.OID || role != f.approval.Role {
		return errIsolation
	}
	return nil
}

func (f *RestoreFixture) create(ctx context.Context, name string) (*RestoreFixture, error) {
	admin, err := f.connect(ctx, "postgres")
	if err != nil {
		return nil, err
	}
	defer admin.Close(ctx)
	child := *f
	child.Name = name
	child.owner = restoreOwner{Version: 1, RunID: f.approval.RunID, Database: name, Role: f.approval.Role, SystemIdentifier: f.approval.SystemIdentifier}
	if err := child.guard(ctx, admin, name, func() error {
		var count int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_database WHERE datname=$1", name).Scan(&count); err != nil || count != 0 {
			return errIsolation
		}
		if err := writeRestoreJSON(child.recordPath(name)+".intent", child.owner); err != nil {
			return err
		}
		_, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0")
		return err
	}); err != nil {
		return nil, errIsolation
	}
	var role string
	if err := admin.QueryRow(ctx, "SELECT oid::bigint, pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1", name).Scan(&child.owner.OID, &role); err != nil || role != child.approval.Role {
		return nil, errIsolation
	}
	if err := writeRestoreJSON(child.recordPath(name), child.owner); err != nil {
		return nil, err
	}
	u, _ := url.Parse(child.baseURL)
	u.Path = "/" + name
	child.URL = u.String()
	config, err := pgxpool.ParseConfig(child.URL)
	if err != nil {
		return nil, errIsolation
	}
	config.ConnConfig.Fallbacks = nil
	config.ConnConfig.RuntimeParams = map[string]string{"application_name": "teldrive-b2-restored-fixture"}
	config.MaxConns = 8
	child.Pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errIsolation
	}
	return &child, nil
}

// CreateSource initializes only the exact empty approved source. The callback must
// use the actual embedded pre-candidate migration chain, never latest then delete.
func (f *RestoreFixture) CreateSource(ctx context.Context, migrateThrough14 func(context.Context, *pgxpool.Pool) error) (*RestoreFixture, error) {
	if f.Name != "" || migrateThrough14 == nil {
		return nil, errIsolation
	}
	source, err := f.create(ctx, f.input.SourceDatabase)
	if err != nil {
		return nil, err
	}
	if err := source.Mutate(ctx, migrateThrough14); err != nil {
		source.Close()
		return nil, err
	}
	var version int64
	if source.Pool.QueryRow(ctx, "SELECT max(version_id) FROM teldrive.migrations WHERE is_applied").Scan(&version) != nil || version != 14 {
		source.Close()
		return nil, errIsolation
	}
	return source, nil
}

// Mutate is the required gate for migration and fixture SQL. Errors remain private.
func (f *RestoreFixture) Mutate(ctx context.Context, mutate func(context.Context, *pgxpool.Pool) error) error {
	if f.Name == "" || f.Pool == nil || mutate == nil {
		return errIsolation
	}
	conn, err := f.connect(ctx, f.Name)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return f.guard(ctx, conn, f.Name, func() error {
		if err := f.verifyOwner(ctx, conn); err != nil {
			return err
		}
		return mutate(ctx, f.Pool)
	})
}

func (f *RestoreFixture) toolStream(ctx context.Context, name string, restore bool, input io.Reader, output io.Writer) error {
	u, err := url.Parse(f.baseURL)
	if err != nil {
		return errIsolation
	}
	password, _ := u.User.Password()
	tool := f.approval.Tools.PGDump
	args := []string{"exec", "-i", "--env", "PGPASSWORD", f.approval.Tools.ContainerID}
	if restore {
		tool = f.approval.Tools.PGRestore
	}
	args = append(args, tool, "--host", f.approval.Host, "--port", strconv.Itoa(int(f.approval.Port)), "--username", f.approval.Role, "--no-password")
	if restore {
		args = append(args, "--exit-on-error", "--single-transaction", "--dbname", name)
	} else {
		args = append(args, "--format=custom", "--dbname", name)
	}
	return f.docker(ctx, input, output, password, args...)
}

func (f *RestoreFixture) Dump(ctx context.Context, stem string) (*Snapshot, error) {
	if !runPattern.MatchString(stem) {
		return nil, errIsolation
	}
	var snap Snapshot
	err := f.Mutate(ctx, func(ctx context.Context, pool *pgxpool.Pool) error {
		var version int64
		if pool.QueryRow(ctx, "SELECT max(version_id) FROM teldrive.migrations WHERE is_applied").Scan(&version) != nil || (version != 14 && version != 15) {
			return errIsolation
		}
		path := filepath.Join(f.input.Directory, stem+".dump")
		out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return errIsolation
		}
		err = f.toolStream(ctx, f.Name, false, nil, out)
		if err == nil {
			err = out.Sync()
		}
		closeErr := out.Close()
		if err != nil || closeErr != nil {
			return errIsolation
		}
		hash, size, err := hashRegular(path, true)
		if err != nil || size == 0 {
			return errIsolation
		}
		snap = Snapshot{Version: 1, RunID: f.approval.RunID, SourceDatabase: f.Name, SourceOID: f.owner.OID,
			SystemIdentifier: f.approval.SystemIdentifier, Role: f.approval.Role, Dump: path, SHA256: hash, Size: size, MigrationVersion: version, Tools: f.approval.Tools}
		return writeRestoreJSON(filepath.Join(f.input.Directory, stem+".snapshot.json"), snap)
	})
	if err != nil {
		return nil, err
	}
	return &snap, nil
}

// CloneSnapshot creates and restores only a never-existing exact approved target.
// No --clean, --create, overwrite, schema filtering or ownership rewrite is used.
func (f *RestoreFixture) CloneSnapshot(ctx context.Context, snapshotPath, target string) (*RestoreFixture, error) {
	allowed := false
	for _, name := range f.input.TargetDatabases {
		if name == target {
			allowed = true
		}
	}
	if !allowed || filepath.Dir(snapshotPath) != f.input.Directory {
		return nil, errIsolation
	}
	var snap Snapshot
	if _, err := decodePrivate(snapshotPath, &snap); err != nil {
		return nil, err
	}
	if snap.Version != 1 || snap.RunID != f.approval.RunID || snap.SystemIdentifier != f.approval.SystemIdentifier || snap.Role != f.approval.Role ||
		snap.Tools != f.approval.Tools || !idPattern.MatchString(snap.SHA256) || filepath.Dir(snap.Dump) != f.input.Directory || snap.Size <= 0 ||
		(snap.MigrationVersion != 14 && snap.MigrationVersion != 15) {
		return nil, errIsolation
	}
	sourceAllowed := snap.SourceDatabase == f.input.SourceDatabase
	for _, name := range f.input.TargetDatabases {
		if name == snap.SourceDatabase {
			sourceAllowed = true
		}
	}
	var recorded restoreOwner
	if !sourceAllowed {
		return nil, errIsolation
	}
	if _, err := decodePrivate(f.recordPath(snap.SourceDatabase), &recorded); err != nil ||
		recorded != (restoreOwner{Version: 1, RunID: snap.RunID, Database: snap.SourceDatabase, SystemIdentifier: snap.SystemIdentifier, Role: snap.Role, OID: snap.SourceOID}) {
		return nil, errIsolation
	}
	hash, size, err := hashRegular(snap.Dump, true)
	if err != nil || hash != snap.SHA256 || size != snap.Size {
		return nil, errIsolation
	}
	admin, err := f.connect(ctx, "postgres")
	if err != nil {
		return nil, err
	}
	defer admin.Close(ctx)
	tools, err := f.observeTools(ctx)
	if err != nil {
		return nil, err
	}
	i, err := restoreIdentity(ctx, admin)
	if err != nil {
		return nil, err
	}
	var sourceOID, targetOID int64
	var owner string
	if admin.QueryRow(ctx, "SELECT oid::bigint, pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1", snap.SourceDatabase).Scan(&sourceOID, &owner) != nil || sourceOID != snap.SourceOID || owner != snap.Role {
		return nil, errIsolation
	}
	if err := admin.QueryRow(ctx, "SELECT oid::bigint FROM pg_database WHERE datname=$1", target).Scan(&targetOID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, errIsolation
	}
	var child *RestoreFixture
	err = GuardRestore(f.approval, f.baseURL, RestoreTarget{RunID: snap.RunID, SourceDatabase: snap.SourceDatabase, SourceOID: sourceOID, TargetDatabase: target, TargetOID: targetOID}, time.Now().UTC(), i, tools, func() error {
		var err error
		child, err = f.create(ctx, target)
		return err
	})
	if err != nil {
		return nil, err
	}
	err = child.Mutate(ctx, func(ctx context.Context, pool *pgxpool.Pool) error {
		var count int64
		if pool.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'`).Scan(&count) != nil || count != 0 {
			return errIsolation
		}
		// Reopen and rehash immediately before restore; changed evidence fails closed.
		hash, size, err := hashRegular(snap.Dump, true)
		if err != nil || hash != snap.SHA256 || size != snap.Size {
			return errIsolation
		}
		in, err := os.Open(snap.Dump)
		if err != nil {
			return errIsolation
		}
		defer in.Close()
		return child.toolStream(ctx, target, true, in, io.Discard)
	})
	if err != nil {
		child.Close()
		return nil, err
	}
	var version int64
	if child.Pool.QueryRow(ctx, "SELECT max(version_id) FROM teldrive.migrations WHERE is_applied").Scan(&version) != nil || version != snap.MigrationVersion {
		child.Close()
		return nil, errIsolation
	}
	return child, nil
}

func (f *RestoreFixture) Close() {
	if f.Pool != nil {
		f.Pool.Close()
	}
}

func (f *RestoreFixture) Drop(ctx context.Context) error {
	if f.Name == "" {
		return errIsolation
	}
	f.Close()
	admin, err := f.connect(ctx, "postgres")
	if err != nil {
		return err
	}
	defer admin.Close(ctx)
	return f.guard(ctx, admin, f.Name, func() error {
		if err := f.verifyOwner(ctx, admin); err != nil {
			return err
		}
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{f.Name}.Sanitize()+" WITH (FORCE)"); err != nil {
			return errIsolation
		}
		return nil
	})
}

// ModeledLegacyTemplate pins synthetic fixture inputs, not historical source.
type ModeledLegacyTemplate struct {
	Version             int    `json:"version"`
	RunID               string `json:"run_id"`
	Provenance          string `json:"provenance"`
	Mode                string `json:"mode"`
	Key                 string `json:"key"`
	Output              string `json:"output"`
	FixtureSource       string `json:"fixture_source"`
	FixtureSourceSHA256 string `json:"fixture_source_sha256"`
	FixtureBinary       string `json:"fixture_binary"`
	FixtureBinarySHA256 string `json:"fixture_binary_sha256"`
	MigrationSHA256     string `json:"migration_sha256"`
}
