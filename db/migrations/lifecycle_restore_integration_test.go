//go:build integration

package migrations_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/divyam234/riverpro"
	"github.com/divyam234/riverpro/driver/riverpropgxv5"
	"github.com/divyam234/riverpro/riverencrypt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"

	"github.com/tgdrive/teldrive/v2/db/migrations"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/contentcrypto"
	"github.com/tgdrive/teldrive/v2/internal/database"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/shares"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

// Each selected case has separate root-approved source and target identities.
// No test can reuse another case's source or overwrite an existing restore.
type restoreCase struct {
	Input    string
	Template string
}
type legacyScenario struct {
	UploadID      uuid.UUID
	ParentID      *uuid.UUID
	FileID        *uuid.UUID
	Size          int64
	Hash          string
	SHA256        [32]byte
	Plain         []byte
	Encrypted     bool
	SessionBefore json.RawMessage
	PartsBefore   json.RawMessage
	SessionAfter  json.RawMessage
}
type legacyMessage struct {
	UserID    int64
	ChannelID int64
	MessageID int64
	Bytes     []byte
	SHA256    [32]byte
}
type legacyEvidence struct {
	Version             int
	RunID               string
	Database            string
	OID                 int64
	Provenance          string
	Mode                string
	TemplatePath        string
	TemplateSHA256      string
	FixtureSource       string
	FixtureSourceSHA256 string
	FixtureBinary       string
	FixtureBinarySHA256 string
	MigrationSHA256     string
	MigrationFiles      []modeledMigrationFile
	Key                 string
	Scenarios           map[string]legacyScenario
	Messages            []legacyMessage
	Rows                map[string]json.RawMessage
	Triggers            map[string]string
	Roles               json.RawMessage
	Extensions          json.RawMessage
	Tools               testpostgres.ToolContext
	Config              map[string]any
}
type restoredFixture struct {
	db       *testpostgres.RestoreFixture
	root     *testpostgres.RestoreFixture
	input    testpostgres.RestoreInput
	evidence legacyEvidence
	storage  *fixture.Storage
	policy   *lifecycle.Service
	catalog  *catalog.Service
	uploads  *uploads.Service
	channels *channels.Service
	ops      *fileops.Service
	pipeline *transfer.Pipeline
	runtime  *jobs.Runtime
	client   *riverpro.Client[pgx.Tx]
	started  bool
	restores int
}
type restoredKeys string

func (k restoredKeys) Key(context.Context, int64, int32) (string, error) { return string(k), nil }

func privateJSON(t *testing.T, path string, out any) []byte {
	t.Helper()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("private restore input path rejected")
	}
	for p := path; ; p = filepath.Dir(p) {
		i, err := os.Lstat(p)
		if err != nil || i.Mode()&os.ModeSymlink != 0 {
			t.Fatal("private restore input rejected")
		}
		if p == path || p == filepath.Dir(path) {
			mode := os.FileMode(0600)
			if p != path {
				mode = 0700
			}
			v := reflect.ValueOf(i.Sys())
			if v.Kind() != reflect.Pointer || v.IsNil() {
				t.Fatal("private restore ownership unavailable")
			}
			uid := v.Elem().FieldByName("Uid")
			if !uid.IsValid() || !uid.CanUint() || uid.Uint() != uint64(os.Getuid()) || i.Mode().Perm() != mode {
				t.Fatal("private restore input permissions rejected")
			}
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 1<<20 {
		t.Fatal("private restore input unreadable")
	}
	if err := uniqueRestoreJSON(json.NewDecoder(bytes.NewReader(b))); err != nil {
		t.Fatal("duplicate/malformed private restore input rejected")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		t.Fatal("private restore input schema rejected")
	}
	return b
}

func preCandidate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, "CREATE SCHEMA teldrive"); err != nil {
		return err
	}
	files := fstest.MapFS{}
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= "000015" {
			continue
		}
		b, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			return err
		}
		files[entry.Name()] = &fstest.MapFile{Data: []byte(strings.ReplaceAll(string(b), "/* TEMPLATE: schema */", `"teldrive".`))}
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files, goose.WithTableName("teldrive.migrations"))
	if err != nil {
		return err
	}
	_, err = provider.UpTo(ctx, 14)
	return err
}

type modeledMigrationFile struct {
	Path   string
	Size   int64
	SHA256 string
}

func modeledMigrationManifest() ([]modeledMigrationFile, string, error) {
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return nil, "", err
	}
	var files []modeledMigrationFile
	var manifest strings.Builder
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= "000015" {
			continue
		}
		if !strings.HasPrefix(entry.Name(), fmt.Sprintf("%06d_", len(files)+1)) {
			return nil, "", errors.New("modeled migration sequence rejected")
		}
		b, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			return nil, "", err
		}
		h := sha256.Sum256(b)
		file := modeledMigrationFile{entry.Name(), int64(len(b)), hex.EncodeToString(h[:])}
		files = append(files, file)
		manifest.WriteString(file.SHA256 + "  " + file.Path + "\n")
	}
	if len(files) != 14 {
		return nil, "", errors.New("modeled migration count rejected")
	}
	h := sha256.Sum256([]byte(manifest.String()))
	return files, hex.EncodeToString(h[:]), nil
}

func modeledSourceHash(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("modeled source path rejected")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || (p == path && !info.Mode().IsRegular()) {
			return "", errors.New("modeled source rejected")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func validateModeledTemplate(v testpostgres.ModeledLegacyTemplate, path, directory, run, mode string) error {
	_, source, _, ok := runtime.Caller(0)
	source, err := filepath.Abs(source)
	if !ok || err != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != directory ||
		v.Version != 2 || v.Provenance != "modeled_legacy" || run == "" || v.RunID != run ||
		(mode != "all" && mode != "unknown_only" && mode != "empty") || v.Mode != mode || v.Key == "" ||
		!filepath.IsAbs(v.Output) || filepath.Clean(v.Output) != v.Output || filepath.Dir(v.Output) != directory ||
		v.Output == path || v.FixtureSource != source {
		return errors.New("modeled template identity rejected")
	}
	_, migrationHash, err := modeledMigrationManifest()
	if err != nil || v.MigrationSHA256 != migrationHash {
		return errors.New("modeled migration pin rejected")
	}
	sourceHash, err := modeledSourceHash(v.FixtureSource)
	if err != nil || v.FixtureSourceSHA256 != sourceHash {
		return errors.New("modeled fixture pin rejected")
	}
	executable, err := os.Executable()
	if err != nil || executable != v.FixtureBinary {
		return errors.New("modeled executable identity rejected")
	}
	binaryHash, err := modeledSourceHash(v.FixtureBinary)
	if err != nil || v.FixtureBinarySHA256 != binaryHash {
		return errors.New("modeled executable pin rejected")
	}
	return nil
}

func validateModeledEvidence(v legacyEvidence, template testpostgres.ModeledLegacyTemplate, path, templateHash string) error {
	files, digest, err := modeledMigrationManifest()
	if err != nil || !reflect.DeepEqual(v.MigrationFiles, files) || v.MigrationSHA256 != digest ||
		v.Version != 2 || v.Provenance != "modeled_legacy" || v.RunID != template.RunID || v.Mode != template.Mode ||
		v.Key != template.Key || v.TemplatePath != path || v.TemplateSHA256 != templateHash || len(templateHash) != 64 ||
		v.FixtureSource != template.FixtureSource || v.FixtureSourceSHA256 != template.FixtureSourceSHA256 ||
		v.FixtureBinary != template.FixtureBinary || v.FixtureBinarySHA256 != template.FixtureBinarySHA256 ||
		v.MigrationSHA256 != template.MigrationSHA256 {
		return errors.New("modeled evidence manifest rejected")
	}
	if decoded, err := hex.DecodeString(templateHash); err != nil || len(decoded) != sha256.Size || strings.ToLower(templateHash) != templateHash {
		return errors.New("modeled template hash rejected")
	}
	return nil
}

func validateModeledTriggers(retained, actual map[string]string) error {
	if retained == nil || !reflect.DeepEqual(retained, actual) {
		return errors.New("retained trigger manifest changed")
	}
	return nil
}

func validateModeledSnapshot(v testpostgres.Snapshot, evidence legacyEvidence, run, source string) error {
	if v.Version != 1 || v.MigrationVersion != 14 || run == "" || v.RunID != run || evidence.RunID != run ||
		v.SourceDatabase != source || evidence.Database != source || evidence.OID <= 0 || v.SourceOID != evidence.OID ||
		v.Tools != evidence.Tools {
		return errors.New("modeled snapshot source identity rejected")
	}
	return nil
}

func writeModeledEvidence(path string, evidence legacyEvidence) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(f).Encode(evidence)
	syncErr := f.Sync()
	return errors.Join(encodeErr, syncErr, f.Close())
}

func TestB2ModeledLegacyManifest(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	source, err := filepath.Abs(source)
	if err != nil {
		t.Fatal(err)
	}
	sourceHash, err := modeledSourceHash(source)
	if err != nil {
		t.Fatal(err)
	}
	files, migrationHash, err := modeledMigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binaryHash, err := modeledSourceHash(executable)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "template.json")
	template := testpostgres.ModeledLegacyTemplate{Version: 2, RunID: "offline-run", Provenance: "modeled_legacy",
		Mode: "all", Key: "disposable-offline-key", Output: filepath.Join(directory, "evidence.json"),
		FixtureSource: source, FixtureSourceSHA256: sourceHash, FixtureBinary: executable,
		FixtureBinarySHA256: binaryHash, MigrationSHA256: migrationHash}
	if err := validateModeledTemplate(template, path, directory, template.RunID, "all"); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*testpostgres.ModeledLegacyTemplate){
		"old_version":                 func(v *testpostgres.ModeledLegacyTemplate) { v.Version = 1 },
		"missing_provenance":          func(v *testpostgres.ModeledLegacyTemplate) { v.Provenance = "" },
		"false_historical_provenance": func(v *testpostgres.ModeledLegacyTemplate) { v.Provenance = "historical" },
		"stale_run":                   func(v *testpostgres.ModeledLegacyTemplate) { v.RunID = "stale-run" },
		"missing_run":                 func(v *testpostgres.ModeledLegacyTemplate) { v.RunID = "" },
		"wrong_mode":                  func(v *testpostgres.ModeledLegacyTemplate) { v.Mode = "empty" },
		"missing_mode":                func(v *testpostgres.ModeledLegacyTemplate) { v.Mode = "" },
		"missing_key":                 func(v *testpostgres.ModeledLegacyTemplate) { v.Key = "" },
		"missing_output":              func(v *testpostgres.ModeledLegacyTemplate) { v.Output = "" },
		"outside_output": func(v *testpostgres.ModeledLegacyTemplate) {
			v.Output = filepath.Join(filepath.Dir(directory), "evidence.json")
		},
		"template_output_alias":  func(v *testpostgres.ModeledLegacyTemplate) { v.Output = path },
		"wrong_fixture_path":     func(v *testpostgres.ModeledLegacyTemplate) { v.FixtureSource = filepath.Join(directory, "fixture.go") },
		"missing_fixture_hash":   func(v *testpostgres.ModeledLegacyTemplate) { v.FixtureSourceSHA256 = "" },
		"stale_fixture_hash":     func(v *testpostgres.ModeledLegacyTemplate) { v.FixtureSourceSHA256 = strings.Repeat("0", 64) },
		"wrong_executable_path":  func(v *testpostgres.ModeledLegacyTemplate) { v.FixtureBinary += ".other" },
		"wrong_executable_hash":  func(v *testpostgres.ModeledLegacyTemplate) { v.FixtureBinarySHA256 = strings.Repeat("0", 64) },
		"missing_migration_hash": func(v *testpostgres.ModeledLegacyTemplate) { v.MigrationSHA256 = "" },
		"stale_migration_hash":   func(v *testpostgres.ModeledLegacyTemplate) { v.MigrationSHA256 = strings.Repeat("0", 64) },
	} {
		t.Run("template/"+name, func(t *testing.T) {
			bad := template
			change(&bad)
			if validateModeledTemplate(bad, path, directory, template.RunID, "all") == nil {
				t.Fatal("bad modeled authority accepted")
			}
		})
	}
	templateHash := strings.Repeat("1", 64)
	evidence := legacyEvidence{Version: 2, RunID: template.RunID, Provenance: template.Provenance, Mode: template.Mode,
		Key: template.Key, TemplatePath: path, TemplateSHA256: templateHash, FixtureSource: source,
		FixtureSourceSHA256: sourceHash, FixtureBinary: executable, FixtureBinarySHA256: binaryHash,
		MigrationSHA256: migrationHash, MigrationFiles: files}
	if err := validateModeledEvidence(evidence, template, path, templateHash); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*legacyEvidence){
		"old_version":           func(v *legacyEvidence) { v.Version = 1 },
		"missing_provenance":    func(v *legacyEvidence) { v.Provenance = "" },
		"stale_run":             func(v *legacyEvidence) { v.RunID = "stale-run" },
		"wrong_mode":            func(v *legacyEvidence) { v.Mode = "empty" },
		"wrong_key":             func(v *legacyEvidence) { v.Key = "other-key" },
		"wrong_template_path":   func(v *legacyEvidence) { v.TemplatePath += ".other" },
		"wrong_template_hash":   func(v *legacyEvidence) { v.TemplateSHA256 = strings.Repeat("2", 64) },
		"wrong_fixture_path":    func(v *legacyEvidence) { v.FixtureSource += ".other" },
		"wrong_fixture_hash":    func(v *legacyEvidence) { v.FixtureSourceSHA256 = strings.Repeat("0", 64) },
		"wrong_executable_path": func(v *legacyEvidence) { v.FixtureBinary += ".other" },
		"wrong_executable_hash": func(v *legacyEvidence) { v.FixtureBinarySHA256 = strings.Repeat("0", 64) },
		"wrong_migration_hash":  func(v *legacyEvidence) { v.MigrationSHA256 = strings.Repeat("0", 64) },
		"missing_manifest":      func(v *legacyEvidence) { v.MigrationFiles = nil },
		"short_manifest":        func(v *legacyEvidence) { v.MigrationFiles = v.MigrationFiles[:13] },
		"wrong_file_hash":       func(v *legacyEvidence) { v.MigrationFiles[0].SHA256 = strings.Repeat("0", 64) },
		"wrong_file_size":       func(v *legacyEvidence) { v.MigrationFiles[0].Size++ },
		"path_alias":            func(v *legacyEvidence) { v.MigrationFiles[0].Path = "./" + v.MigrationFiles[0].Path },
		"duplicate_file":        func(v *legacyEvidence) { v.MigrationFiles[1] = v.MigrationFiles[0] },
		"reordered_files": func(v *legacyEvidence) {
			v.MigrationFiles[0], v.MigrationFiles[1] = v.MigrationFiles[1], v.MigrationFiles[0]
		},
		"self_consistent_forgery": func(v *legacyEvidence) {
			v.MigrationFiles[0].SHA256 = strings.Repeat("0", 64)
			var b strings.Builder
			for _, f := range v.MigrationFiles {
				b.WriteString(f.SHA256 + "  " + f.Path + "\n")
			}
			h := sha256.Sum256([]byte(b.String()))
			v.MigrationSHA256 = hex.EncodeToString(h[:])
		},
	} {
		t.Run("evidence/"+name, func(t *testing.T) {
			bad := evidence
			bad.MigrationFiles = append([]modeledMigrationFile(nil), evidence.MigrationFiles...)
			change(&bad)
			if validateModeledEvidence(bad, template, path, templateHash) == nil {
				t.Fatal("bad modeled manifest accepted")
			}
		})
	}
	t.Run("changed_retained_trigger", func(t *testing.T) {
		retained := map[string]string{"emit_upload_event": "original definition"}
		raw, err := json.Marshal(legacyEvidence{Version: 2, Triggers: retained})
		if err != nil {
			t.Fatal(err)
		}
		var manifest legacyEvidence
		if json.Unmarshal(raw, &manifest) != nil || validateModeledTriggers(manifest.Triggers, retained) != nil {
			t.Fatal("retained trigger manifest did not round trip")
		}
		manifest.Triggers["emit_upload_event"] = "changed definition"
		if validateModeledTriggers(manifest.Triggers, retained) == nil {
			t.Fatal("changed retained trigger manifest accepted")
		}
	})
	t.Run("snapshot_source_identity", func(t *testing.T) {
		snapshotEvidence := evidence
		snapshotEvidence.Database = "modeled-source"
		snapshotEvidence.OID = 42
		snapshot := testpostgres.Snapshot{Version: 1, RunID: template.RunID, SourceDatabase: snapshotEvidence.Database,
			SourceOID: snapshotEvidence.OID, MigrationVersion: 14}
		if err := validateModeledSnapshot(snapshot, snapshotEvidence, template.RunID, snapshotEvidence.Database); err != nil {
			t.Fatal(err)
		}
		for _, change := range []func(*testpostgres.Snapshot){
			func(v *testpostgres.Snapshot) { v.SourceOID++ },
			func(v *testpostgres.Snapshot) { v.SourceDatabase = "another-source" },
			func(v *testpostgres.Snapshot) { v.RunID = "another-run" },
			func(v *testpostgres.Snapshot) { v.MigrationVersion = 15 },
		} {
			bad := snapshot
			change(&bad)
			if validateModeledSnapshot(bad, snapshotEvidence, template.RunID, snapshotEvidence.Database) == nil {
				t.Fatal("mismatched modeled source snapshot accepted")
			}
		}
	})
	for _, badPath := range []string{"relative.json", path + "/../template.json", filepath.Join(filepath.Dir(directory), "template.json")} {
		if validateModeledTemplate(template, badPath, directory, template.RunID, "all") == nil {
			t.Fatal("template path mismatch accepted")
		}
	}
	for _, mode := range []string{"unknown_only", "empty"} {
		v := template
		v.Mode = mode
		if validateModeledTemplate(v, path, directory, v.RunID, mode) != nil {
			t.Fatal("valid modeled mode rejected")
		}
	}
	parent := uuid.New()
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	before := map[string]any{"parent_id": parent.String(), "updated_at": stamp.Format(time.RFC3339Nano), "state": "open", "mime_type": "application/octet-stream", "expected_hash_algorithm": nil, "expected_hash_value": nil, "mod_time": stamp.Format(time.RFC3339Nano)}
	after := map[string]any{}
	for key, value := range before {
		after[key] = value
	}
	after["parent_id"] = nil
	after["updated_at"] = stamp.Add(time.Second).Format(time.RFC3339Nano)
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		t.Fatal(err)
	}
	scenario := legacyScenario{SessionBefore: beforeJSON, SessionAfter: afterJSON}
	if validateModeledDetachment(scenario, parent) != nil {
		t.Fatal("valid modeled detachment rejected")
	}
	for name, change := range map[string]func(map[string]any){
		"lost_parent_field": func(v map[string]any) { delete(v, "parent_id") },
		"backwards_time":    func(v map[string]any) { v["updated_at"] = stamp.Add(-time.Second).Format(time.RFC3339Nano) },
		"changed_state":     func(v map[string]any) { v["state"] = "aborted" },
		"changed_mime":      func(v map[string]any) { v["mime_type"] = nil },
		"changed_null_hash": func(v map[string]any) { v["expected_hash_value"] = "invented" },
		"changed_mod_time":  func(v map[string]any) { v["mod_time"] = stamp.Add(time.Second).Format(time.RFC3339Nano) },
	} {
		t.Run("detachment/"+name, func(t *testing.T) {
			bad := map[string]any{}
			for key, value := range after {
				bad[key] = value
			}
			change(bad)
			raw, err := json.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			v := scenario
			v.SessionAfter = raw
			if validateModeledDetachment(v, parent) == nil {
				t.Fatal("changed modeled detachment accepted")
			}
		})
	}
}

// Seed modeled SQL geometry only; candidate services cannot run against schema14.
func seedModeledLegacy(ctx context.Context, pool *pgxpool.Pool, in testpostgres.ModeledLegacyTemplate, templatePath, templateHash string, tools testpostgres.ToolContext) (legacyEvidence, error) {
	out := legacyEvidence{Version: 2, RunID: in.RunID, Provenance: "modeled_legacy", Mode: in.Mode,
		TemplatePath: templatePath, TemplateSHA256: templateHash, FixtureSource: in.FixtureSource,
		FixtureSourceSHA256: in.FixtureSourceSHA256, MigrationSHA256: in.MigrationSHA256,
		FixtureBinary: in.FixtureBinary, FixtureBinarySHA256: in.FixtureBinarySHA256,
		Key: in.Key, Tools: tools, Scenarios: map[string]legacyScenario{}, Rows: map[string]json.RawMessage{},
		Config: map[string]any{"schema": "teldrive", "storage": "synthetic", "workers_disabled": true, "content_key_version": 1}}
	var err error
	out.MigrationFiles, _, err = modeledMigrationManifest()
	if err != nil {
		return out, err
	}
	var version int64
	var absent bool
	if err := pool.QueryRow(ctx, `SELECT current_database(),(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),(SELECT max(version_id) FROM teldrive.migrations WHERE is_applied),to_regclass('teldrive.remote_write_receipts') IS NULL`).Scan(&out.Database, &out.OID, &version, &absent); err != nil || version != 14 || !absent {
		return out, errors.New("modeled source schema rejected")
	}
	if in.Mode != "empty" {
		if _, err := pool.Exec(ctx, "INSERT INTO teldrive.users(user_id) VALUES(1),(2); INSERT INTO teldrive.channels(channel_id,user_id,name,selected) VALUES(9001,1,'synthetic',true)"); err != nil {
			return out, err
		}
	}
	modTime := time.Now().UTC().Truncate(time.Microsecond)
	nextMessage := int64(100)
	makeUpload := func(name string, parent *uuid.UUID, data []byte, encrypted, completed, cli bool) (legacyScenario, error) {
		h := treehash.NewBlockHasher()
		if _, err := h.Write(data); err != nil {
			return legacyScenario{}, err
		}
		hash := treehash.SumToHex(treehash.ComputeTreeHash(h.Sum()))
		s := legacyScenario{UploadID: uuid.New(), ParentID: parent, Size: int64(len(data)), Hash: hash, SHA256: sha256.Sum256(data), Plain: append([]byte(nil), data...), Encrypted: encrypted}
		var keyVersion *int32
		if encrypted {
			v := int32(1)
			keyVersion = &v
		}
		var algorithm, value, mime *string
		if cli {
			v := "application/octet-stream"
			mime = &v
		} else {
			v := "blake3"
			algorithm, value = &v, &hash
		}
		if _, err := pool.Exec(ctx, `INSERT INTO teldrive.upload_sessions(id,user_id,parent_id,name,expected_size,expected_hash_algorithm,expected_hash_value,mime_type,mod_time,encryption,encryption_key_version,conflict_policy,part_size,expires_at) VALUES($1,1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'replace',536870912,clock_timestamp()+interval '24 hours')`, s.UploadID, parent, name, s.Size, algorithm, value, mime, modTime, encrypted, keyVersion); err != nil {
			return s, err
		}
		if len(data) > 0 {
			stored := append([]byte(nil), data...)
			var salt *string
			if encrypted {
				v := uuid.NewString()
				salt = &v
				cipher, err := contentcrypto.NewCipher(in.Key, v)
				if err != nil {
					return s, err
				}
				reader, err := cipher.EncryptData(bytes.NewReader(data))
				if err != nil {
					return s, err
				}
				stored, err = io.ReadAll(reader)
				closeErr := reader.Close()
				if err != nil || closeErr != nil || int64(len(stored)) != contentcrypto.EncryptedSize(s.Size) {
					return s, errors.New("modeled encryption failed")
				}
			}
			nextMessage++
			actor := int64(1)
			if name == "detached_unknown" {
				actor = 2
			}
			out.Messages = append(out.Messages, legacyMessage{actor, 9001, nextMessage, stored, sha256.Sum256(stored)})
			if _, err := pool.Exec(ctx, `INSERT INTO teldrive.upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,checksum,salt,state,lease_token,lease_expires_at,block_hashes) VALUES($1,1,9001,$2,$3,$4,$5,$6,'stored',$7,clock_timestamp()+interval '5 minutes',$8)`, s.UploadID, nextMessage, s.Size, int64(len(stored)), hash, salt, uuid.New(), h.Sum()); err != nil {
				return s, err
			}
		}
		if completed {
			id := uuid.New()
			s.FileID = &id
			if _, err := pool.Exec(ctx, `INSERT INTO teldrive.files(id,user_id,parent_id,name,kind,size,hash_algorithm,hash_value,encryption,encryption_key_version,mod_time) VALUES($1,1,$2,$3,'file',$4,'blake3',$5,$6,$7,$8)`, id, parent, name, s.Size, hash, encrypted, keyVersion, modTime); err != nil {
				return s, err
			}
			if _, err := pool.Exec(ctx, `INSERT INTO teldrive.file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size,checksum,salt,block_hashes) SELECT $2,part_no,channel_id,message_id,plain_size,stored_size,checksum,salt,block_hashes FROM teldrive.upload_parts WHERE upload_id=$1`, s.UploadID, id); err != nil {
				return s, err
			}
			if _, err := pool.Exec(ctx, `UPDATE teldrive.upload_sessions SET state='completed',file_id=$2,completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, s.UploadID, id); err != nil {
				return s, err
			}
		}
		return s, nil
	}
	folder := func(name, status string) (uuid.UUID, error) {
		id := uuid.New()
		_, err := pool.Exec(ctx, `INSERT INTO teldrive.files(id,user_id,name,kind,status,mod_time,deleted_at) VALUES($1,1,$2,'folder',$3,$4,CASE WHEN $5 THEN clock_timestamp() ELSE NULL END)`, id, name, status, modTime, status != "active")
		return id, err
	}
	session := func(id uuid.UUID) (json.RawMessage, error) {
		var b []byte
		err := pool.QueryRow(ctx, "SELECT to_jsonb(s) FROM teldrive.upload_sessions s WHERE id=$1", id).Scan(&b)
		return b, err
	}
	parts := func(id uuid.UUID) (json.RawMessage, error) {
		var b []byte
		err := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY part_no),'[]') FROM teldrive.upload_parts p WHERE upload_id=$1`, id).Scan(&b)
		return b, err
	}
	if in.Mode != "empty" {
		parent, err := folder("old-detached-parent", "active")
		if err != nil {
			return out, err
		}
		names := []string{"detached_empty"}
		if in.Mode == "all" {
			names = append(names, "detached_owned", "detached_unknown", "detached_cli_empty")
		}
		for _, name := range names {
			var data []byte
			if name == "detached_owned" || name == "detached_unknown" {
				data = bytes.Repeat([]byte(name+"\n"), 128)
			}
			uploadName := name
			if name == "detached_cli_empty" {
				uploadName = "detached_empty"
			}
			s, err := makeUpload(uploadName, &parent, data, false, false, name == "detached_cli_empty")
			if err != nil {
				return out, err
			}
			if s.SessionBefore, err = session(s.UploadID); err != nil {
				return out, err
			}
			if s.PartsBefore, err = parts(s.UploadID); err != nil {
				return out, err
			}
			if _, err := pool.Exec(ctx, `UPDATE teldrive.upload_sessions SET parent_id=NULL,updated_at=clock_timestamp() WHERE id=$1`, s.UploadID); err != nil {
				return out, err
			}
			if s.SessionAfter, err = session(s.UploadID); err != nil {
				return out, err
			}
			afterParts, err := parts(s.UploadID)
			if err != nil || !bytes.Equal(afterParts, s.PartsBefore) || validateModeledDetachment(s, parent) != nil {
				return out, errors.New("modeled detachment changed protected geometry")
			}
			out.Scenarios[name] = s
		}
		if _, err := pool.Exec(ctx, "DELETE FROM teldrive.files WHERE id=$1", parent); err != nil {
			return out, err
		}
	}
	if in.Mode == "all" {
		parent, err := folder("old-active-parent", "active")
		if err != nil {
			return out, err
		}
		for _, entry := range []struct {
			name                 string
			parent               *uuid.UUID
			data                 []byte
			encrypted, completed bool
		}{
			{"legacy_intentional_root", nil, nil, false, false},
			{"legacy_nonroot_active", &parent, bytes.Repeat([]byte("active legacy bytes\n"), 128), false, false},
			{"legacy_completed_root", nil, bytes.Repeat([]byte("encrypted restored bytes\n"), 128), true, true},
			{"legacy_completed_nonroot", &parent, bytes.Repeat([]byte("completed nonroot bytes\n"), 128), false, true},
			{"root_replacement_target", nil, nil, false, true},
			{"legacy_aborted", nil, []byte("aborted retained bytes"), false, false},
			{"legacy_completing", nil, []byte("completing retained bytes"), false, false},
			{"legacy_expired", nil, []byte("expired retained bytes"), false, false},
		} {
			name := entry.name
			if name == "root_replacement_target" {
				name = "detached_empty"
			}
			s, err := makeUpload(name, entry.parent, entry.data, entry.encrypted, entry.completed, false)
			if err != nil {
				return out, err
			}
			if entry.name == "legacy_aborted" {
				if _, err := pool.Exec(ctx, "UPDATE teldrive.upload_sessions SET state='aborted',updated_at=clock_timestamp() WHERE id=$1", s.UploadID); err != nil {
					return out, err
				}
			}
			if entry.name == "legacy_completing" || entry.name == "legacy_expired" {
				state := "completing"
				if entry.name == "legacy_expired" {
					state = "expired"
				}
				if _, err := pool.Exec(ctx, `UPDATE teldrive.upload_sessions SET state=$2::text::teldrive.upload_state,expires_at=CASE WHEN $2::text='expired' THEN clock_timestamp()-interval '1 hour' ELSE expires_at END,updated_at=clock_timestamp() WHERE id=$1`, s.UploadID, state); err != nil {
					return out, err
				}
			}
			if entry.name == "root_replacement_target" {
				hash := sha256.Sum256([]byte("synthetic-share"))
				if _, err := pool.Exec(ctx, "INSERT INTO teldrive.file_shares(id,file_id,owner_id,token_prefix,token_hash) VALUES($1,$2,1,'synthetic',$3)", uuid.New(), *s.FileID, hash[:]); err != nil {
					return out, err
				}
			}
			out.Scenarios[entry.name] = s
		}
		for _, state := range []string{"trashed", "deletion_pending"} {
			if _, err := folder("old-"+state, state); err != nil {
				return out, err
			}
		}
		nextMessage++
		data := []byte("unknown unreferenced bytes must survive restore")
		out.Messages = append(out.Messages, legacyMessage{1, 9001, nextMessage, data, sha256.Sum256(data)})
	}
	for _, name := range []string{"users", "channels", "files", "file_parts", "upload_sessions", "upload_parts", "file_shares", "user_events"} {
		var b []byte
		if err := pool.QueryRow(ctx, "SELECT COALESCE(jsonb_agg(j ORDER BY j::text),'[]'::jsonb) FROM (SELECT to_jsonb(t) j FROM teldrive."+name+" t) x").Scan(&b); err != nil {
			return out, err
		}
		out.Rows[name] = b
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('role',rolname,'super',rolsuper,'create',rolcreatedb,'login',rolcanlogin) ORDER BY rolname),'[]') FROM pg_roles WHERE rolname=current_user`).Scan(&out.Roles); err != nil {
		return out, err
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('name',extname,'version',extversion,'schema',n.nspname) ORDER BY extname),'[]') FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace`).Scan(&out.Extensions); err != nil {
		return out, err
	}
	return out, nil
}

func validateModeledDetachment(s legacyScenario, parent uuid.UUID) error {
	var before, after map[string]any
	if json.Unmarshal(s.SessionBefore, &before) != nil || json.Unmarshal(s.SessionAfter, &after) != nil ||
		before["parent_id"] != parent.String() || after["parent_id"] != nil || after["state"] != "open" {
		return errors.New("modeled detachment identity rejected")
	}
	if _, ok := after["parent_id"]; !ok {
		return errors.New("modeled detachment parent missing")
	}
	b, bok := before["updated_at"].(string)
	a, aok := after["updated_at"].(string)
	bt, be := time.Parse(time.RFC3339Nano, b)
	at, ae := time.Parse(time.RFC3339Nano, a)
	if !bok || !aok || be != nil || ae != nil || bt.IsZero() || at.IsZero() || at.Before(bt) {
		return errors.New("modeled detachment timestamp rejected")
	}
	delete(before, "parent_id")
	delete(after, "parent_id")
	delete(before, "updated_at")
	delete(after, "updated_at")
	if !reflect.DeepEqual(before, after) {
		return errors.New("modeled detachment changed protected fields")
	}
	return nil
}

func newRestoredFixture(t *testing.T, mode string) *restoredFixture {
	t.Helper()
	ctx := context.Background()
	var cases map[string]restoreCase
	privateJSON(t, os.Getenv("B2_RESTORE_CASES"), &cases)
	entry, ok := cases[t.Name()]
	if !ok {
		t.Fatal("exact private restore case authority is missing")
	}
	a, err := testpostgres.LoadApproval(os.Getenv("B2_ISOLATION_APPROVAL"))
	if err != nil {
		t.Fatal("fresh isolation approval required")
	}
	in, err := testpostgres.LoadRestoreInput(entry.Input, *a, os.Getenv("TEST_DATABASE_URL"), time.Now().UTC())
	if err != nil {
		t.Fatal("private restore scope rejected")
	}
	var template testpostgres.ModeledLegacyTemplate
	templateBytes := privateJSON(t, entry.Template, &template)
	if validateModeledTemplate(template, entry.Template, in.Directory, a.RunID, mode) != nil {
		t.Fatal("modeled fixture authority rejected before mutation")
	}
	if _, err := os.Lstat(template.Output); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("modeled evidence output already exists or cannot be inspected")
	}
	templateDigest := sha256.Sum256(templateBytes)
	root, err := testpostgres.OpenRestoreFixture(ctx, os.Getenv("B2_ISOLATION_APPROVAL"), entry.Input, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("observed restore tool context rejected")
	}
	source, err := root.CreateSource(ctx, preCandidate)
	if err != nil {
		t.Fatal("guarded actual pre000015 migration failed")
	}
	cleanupRestoredDatabase(t, source, t.Name() == "TestB2LifecycleRestoredDatabase")
	var old legacyEvidence
	if err := source.Mutate(ctx, func(ctx context.Context, pool *pgxpool.Pool) error {
		if validateModeledTemplate(template, entry.Template, in.Directory, a.RunID, mode) != nil {
			return errors.New("modeled fixture authority changed")
		}
		var currentTemplate testpostgres.ModeledLegacyTemplate
		current := privateJSON(t, entry.Template, &currentTemplate)
		if currentTemplate != template || sha256.Sum256(current) != templateDigest {
			return errors.New("modeled template changed")
		}
		old, err = seedModeledLegacy(ctx, pool, template, entry.Template, hex.EncodeToString(templateDigest[:]), a.Tools)
		return err
	}); err != nil {
		t.Fatal("guarded modeled schema14 seed failed")
	}
	old.Triggers = restoreTriggers(t, source.Pool)
	if err := writeModeledEvidence(template.Output, old); err != nil {
		t.Fatal("private modeled evidence write failed; source retained")
	}
	privateJSON(t, template.Output, &old)
	var finalTemplate testpostgres.ModeledLegacyTemplate
	finalTemplateBytes := privateJSON(t, entry.Template, &finalTemplate)
	if finalTemplate != template || sha256.Sum256(finalTemplateBytes) != templateDigest {
		t.Fatal("modeled template changed after seeding; evidence retained")
	}
	if old.Database != source.Name || old.Database != in.SourceDatabase || old.OID <= 0 || validateModeledEvidence(old, template, entry.Template, hex.EncodeToString(templateDigest[:])) != nil || old.Tools != a.Tools || old.Config["schema"] != "teldrive" || old.Config["storage"] != "synthetic" || old.Config["workers_disabled"] != true || old.Config["content_key_version"] != float64(1) {
		t.Fatal("modeled evidence identity/config/tool manifest mismatch")
	}
	before := restoreRows(t, source.Pool, false)
	if !equalJSONMaps(before, old.Rows) {
		t.Fatal("old independent row manifest differs before dump")
	}
	if validateModeledTriggers(old.Triggers, restoreTriggers(t, source.Pool)) != nil {
		t.Fatal("retained trigger manifest differs before dump")
	}
	stem := strings.ReplaceAll(uuid.NewString(), "-", "")
	snap, err := source.Dump(ctx, stem)
	if err != nil || validateModeledSnapshot(*snap, old, a.RunID, in.SourceDatabase) != nil {
		t.Fatal("real pre000015 pg_dump/source identity mismatch")
	}
	db, err := root.CloneSnapshot(ctx, filepath.Join(in.Directory, stem+".snapshot.json"), in.TargetDatabases[0])
	if err != nil {
		t.Fatal("distinct fresh pg_restore failed")
	}
	cleanupRestoredDatabase(t, db, false)
	if db.Name == source.Name || !equalJSONMaps(before, restoreRows(t, db.Pool, false)) || validateModeledTriggers(old.Triggers, restoreTriggers(t, db.Pool)) != nil {
		t.Fatal("real restore changed rows/triggers")
	}
	var roles, extensions []byte
	if db.Pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('role',rolname,'super',rolsuper,'create',rolcreatedb,'login',rolcanlogin) ORDER BY rolname),'[]') FROM pg_roles WHERE rolname=current_user`).Scan(&roles) != nil || canonicalJSON(roles) != canonicalJSON(old.Roles) {
		t.Fatal("restored private role manifest changed")
	}
	if db.Pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('name',extname,'version',extversion,'schema',n.nspname) ORDER BY extname),'[]') FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace`).Scan(&extensions) != nil || canonicalJSON(extensions) != canonicalJSON(old.Extensions) {
		t.Fatal("restored private extension manifest changed")
	}
	f := &restoredFixture{db: db, root: root, input: *in, evidence: old, restores: 1}
	f.storage = fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1, 2}}, {ChannelID: 9002, Actors: []int64{1, 2}}})
	for _, m := range old.Messages {
		if sha256.Sum256(m.Bytes) != m.SHA256 {
			t.Fatal("old independent byte manifest mismatch")
		}
		if err := f.storage.Seed(fixture.Message{UserID: m.UserID, ChannelID: m.ChannelID, MessageID: m.MessageID, Bytes: m.Bytes, Preserve: true}); err != nil {
			t.Fatal(err)
		}
	}
	f.up(t)
	if !equalJSONMaps(before, restoreRows(t, db.Pool, false)) {
		t.Fatal("migration changed legacy rows beyond additive terminal reason")
	}
	if validateModeledTriggers(old.Triggers, restoreTriggers(t, db.Pool)) != nil {
		t.Fatal("migration changed an installed legacy trigger/function")
	}
	var invented int
	if err := db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM teldrive.remote_write_receipts)+(SELECT count(*) FROM teldrive.request_admissions)+(SELECT count(*) FROM teldrive.import_batches)+(SELECT count(*) FROM teldrive.import_destination_pins)+(SELECT count(*) FROM teldrive.import_job_attempts)`).Scan(&invented); err != nil || invented != 0 {
		t.Fatal("migration manufactured historical ownership/identity")
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM teldrive.upload_sessions WHERE terminal_failure_code IS DISTINCT FROM CASE WHEN parent_id IS NULL AND state<>'completed' THEN 'upload_destination_unknown'::text ELSE NULL END`).Scan(&invented); err != nil || invented != 0 {
		t.Fatal("cutover terminal reason scope mismatch")
	}
	f.compose(t)
	return f
}
func (f *restoredFixture) up(t *testing.T) {
	t.Helper()
	if err := f.db.Mutate(context.Background(), func(ctx context.Context, _ *pgxpool.Pool) error {
		return database.Migrate(ctx, database.Config{URL: f.db.URL, Schema: "teldrive"})
	}); err != nil {
		t.Fatal("guarded actual candidate Up failed")
	}
}
func (f *restoredFixture) compose(t *testing.T) {
	t.Helper()
	f.policy = lifecycle.NewService(f.db.Pool, f.storage)
	f.catalog = catalog.NewService(f.db.Pool, nil)
	f.uploads = uploads.NewService(f.db.Pool)
	f.channels = channels.NewService(f.db.Pool, nil, channels.Config{PartLimit: 10000})
	var err error
	f.ops, err = fileops.NewService(f.db.Pool, f.catalog, f.channels, f.storage)
	if err != nil {
		t.Fatal(err)
	}
	for _, bind := range []func(*lifecycle.Service) error{f.catalog.BindLifecycle, f.uploads.BindLifecycle, f.ops.BindLifecycle} {
		if err := bind(f.policy); err != nil {
			t.Fatal(err)
		}
	}
	f.pipeline = transfer.NewPipeline(f.uploads, f.channels, f.storage, restoredKeys(f.evidence.Key), transfer.Config{Receipts: f.policy})
	key := sha256.Sum256([]byte("b2-disposable-river-key\x00" + f.evidence.Key))
	cipher, err := secureblob.NewWithKey(key[:], rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.runtime, err = jobs.NewRuntimeWithServices(f.db.Pool, f.storage, "teldrive", nil, cipher, time.Hour, jobs.UploaderServices{Catalog: f.catalog, Uploads: f.uploads, Pipeline: f.pipeline}, f.policy, f.ops)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	f.client, err = riverpro.NewClient(riverpropgxv5.New(f.db.Pool), &riverpro.Config{Config: river.Config{Schema: "teldrive", SkipUnknownJobCheck: true, Hooks: []rivertype.Hook{riverencrypt.NewEncryptHookConfig(&riverencrypt.EncryptHookConfig{Encryptor: cipher, JobKindsInclude: []string{jobs.UploadBatchKind, jobs.UploadSourceKind}})}}, DurablePeriodicJobs: riverpro.DurablePeriodicJobsConfig{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.started {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.runtime.Stop(ctx); err != nil {
				t.Error("restored fake-only runtime stop failed")
			}
			f.started = false
		}
	})
}
func restoreRows(t *testing.T, pool *pgxpool.Pool, candidate bool) map[string]json.RawMessage {
	t.Helper()
	names := []string{"users", "channels", "files", "file_parts", "upload_sessions", "upload_parts", "file_shares", "user_events"}
	if candidate {
		names = append(names, "remote_write_receipts", "request_admissions", "import_batches", "import_destination_pins", "import_job_attempts", "channel_deletion_intents", "river_job", "river_periodic_job")
	}
	out := map[string]json.RawMessage{}
	for _, name := range names {
		expr := "to_jsonb(t)"
		if name == "upload_sessions" && !candidate {
			expr += "-'terminal_failure_code'"
		}
		var b []byte
		if err := pool.QueryRow(context.Background(), "SELECT COALESCE(jsonb_agg(j ORDER BY j::text),'[]'::jsonb) FROM (SELECT "+expr+" j FROM teldrive."+name+" t) x").Scan(&b); err != nil {
			t.Fatal("restored exact snapshot query failed")
		}
		out[name] = b
	}
	return out
}
func canonicalJSON(b []byte) string {
	var v any
	if json.Unmarshal(b, &v) != nil {
		return "invalid"
	}
	out, _ := json.Marshal(v)
	return string(out)
}
func equalJSONMaps(a, b map[string]json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if canonicalJSON(v) != canonicalJSON(b[k]) {
			return false
		}
	}
	return true
}
func restoreTriggers(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := map[string]string{}
	rows, err := pool.Query(context.Background(), `SELECT c.relname||'.'||t.tgname,pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='teldrive' AND NOT t.tgisinternal AND t.tgname NOT IN ('upload_terminal_evidence','remote_write_receipts_evidence','request_admissions_evidence','import_batches_evidence','import_destination_pins_evidence','import_job_attempts_evidence','channel_deletion_intents_evidence') ORDER BY 1`)
	if err != nil {
		t.Fatal("installed trigger snapshot failed")
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) != nil {
			t.Fatal("installed trigger decode failed")
		}
		out[k] = v
	}
	if rows.Err() != nil {
		t.Fatal("installed trigger scan failed")
	}
	var body string
	if pool.QueryRow(context.Background(), `SELECT pg_get_functiondef('teldrive.emit_upload_event()'::regprocedure)`).Scan(&body) != nil {
		t.Fatal("installed upload event function missing")
	}
	out["emit_upload_event"] = body
	return out
}
func (f *restoredFixture) bytes(t *testing.T) {
	t.Helper()
	for _, m := range f.evidence.Messages {
		b, ok := f.storage.Bytes(m.ChannelID, m.MessageID)
		if !ok || sha256.Sum256(b) != m.SHA256 {
			t.Fatal("independent old/unknown/encrypted bytes changed")
		}
	}
}
func (f *restoredFixture) complete(t *testing.T, s legacyScenario) (*sqlcgen.File, error) {
	t.Helper()
	algorithm := "blake3"
	return f.uploads.Complete(context.Background(), uploads.CompleteInput{UserID: 1, UploadID: s.UploadID, Size: s.Size, HashAlgorithm: &algorithm, HashValue: &s.Hash})
}
func (f *restoredFixture) download(t *testing.T, id uuid.UUID, want []byte) {
	t.Helper()
	d, err := transfer.NewDownloader(f.catalog, f.storage, restoredKeys(f.evidence.Key)).Open(context.Background(), transfer.DownloadRequest{UserID: 1, FileID: id, Length: -1})
	if err != nil {
		t.Fatal("restored independent download failed")
	}
	defer d.Reader.Close()
	b, err := io.ReadAll(d.Reader)
	if err != nil || len(b) != len(want) || sha256.Sum256(b) != sha256.Sum256(want) {
		t.Fatal("restored download size/SHA-256 mismatch")
	}
}
func (f *restoredFixture) publish(t *testing.T, parent *uuid.UUID, name string, data []byte) (uuid.UUID, uuid.UUID) {
	t.Helper()
	h := treehash.NewBlockHasher()
	_, _ = h.Write(data)
	algorithm, value := "blake3", treehash.SumToHex(treehash.ComputeTreeHash(h.Sum()))
	s, err := f.uploads.Create(context.Background(), uploads.CreateInput{ID: uuid.New(), UserID: 1, ParentID: parent, Name: name, ExpectedSize: int64(len(data)), ExpectedHashAlgorithm: &algorithm, ExpectedHashValue: &value})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.UUID(s.ID.Bytes)
	if len(data) > 0 {
		if _, err := f.pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{UserID: 1, UploadID: id, PartNo: 1, RequestedChannelID: 9001, PlainSize: int64(len(data)), Body: bytes.NewReader(data)}); err != nil {
			t.Fatal(err)
		}
	}
	file, err := f.uploads.Complete(context.Background(), uploads.CompleteInput{UserID: 1, UploadID: id, Size: int64(len(data)), HashAlgorithm: &algorithm, HashValue: &value})
	if err != nil {
		t.Fatal(err)
	}
	fid := uuid.UUID(file.ID.Bytes)
	f.download(t, fid, data)
	return id, fid
}
func TestB2LifecycleRestoredDatabase(t *testing.T) {
	f := newRestoredFixture(t, "all")
	before := restoreRows(t, f.db.Pool, true)
	f.up(t)
	if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
		t.Fatal("repeated actual Up changed evidence")
	}
	for _, name := range []string{"legacy_completed_root", "legacy_completed_nonroot"} {
		s := f.evidence.Scenarios[name]
		if s.FileID == nil {
			t.Fatal("old completed control missing")
		}
		f.download(t, *s.FileID, s.Plain)
	}
	f.bytes(t)
}
func TestB2LegacyDestinationRestore(t *testing.T) {
	for _, name := range []string{"detached_empty_original_manifest", "detached_nonempty_fixture_owned", "detached_nonempty_unknown", "legacy_intentional_root_refused", "new_explicit_root_publishes", "legacy_nonroot_active_publishes", "legacy_completed_replay", "unknown_resume_claim_put_refused", "unknown_evidence_survives_sweeps", "migration_delta_trigger_safe"} {
		t.Run(name, func(t *testing.T) {
			f := newRestoredFixture(t, "all")
			ctx := context.Background()
			scenarioName := map[string]string{"detached_empty_original_manifest": "detached_empty", "detached_nonempty_fixture_owned": "detached_owned", "detached_nonempty_unknown": "detached_unknown", "legacy_intentional_root_refused": "legacy_intentional_root"}[name]
			if scenarioName != "" {
				s, ok := f.evidence.Scenarios[scenarioName]
				if !ok {
					t.Fatal("genuine old scenario missing")
				}
				before := restoreRows(t, f.db.Pool, true)
				if _, err := f.complete(t, s); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatal("original valid legacy manifest did not refuse before publication")
				}
				if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
					t.Fatal("refused original completion changed rows/parts/leases/events/share/replacement")
				}
				f.compose(t)
				if _, err := f.complete(t, s); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatal("fresh service revived unknown destination")
				}
				f.bytes(t)
				return
			}
			switch name {
			case "new_explicit_root_publishes":
				for _, data := range [][]byte{nil, bytes.Repeat([]byte("new owned candidate bytes"), 128)} {
					id, fid := f.publish(t, nil, uuid.NewString(), data)
					var aliases, receipts int
					if f.db.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM teldrive.upload_parts WHERE upload_id=$1),(SELECT count(*) FROM teldrive.remote_write_receipts WHERE upload_id=$1 AND result_file_id=$2 AND state='bound')", id, fid).Scan(&aliases, &receipts) != nil || aliases != 0 || receipts != min(1, len(data)) {
						t.Fatal("new explicit root lacked exact positive receipt/retirement")
					}
				}
			case "legacy_nonroot_active_publishes":
				s := f.evidence.Scenarios["legacy_nonroot_active"]
				file, err := f.complete(t, s)
				if err != nil || !file.ParentID.Valid || s.ParentID == nil || uuid.UUID(file.ParentID.Bytes) != *s.ParentID {
					t.Fatal("actual active legacy nonroot failed")
				}
				f.download(t, uuid.UUID(file.ID.Bytes), s.Plain)
				var n int
				if f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM teldrive.remote_write_receipts").Scan(&n) != nil || n != 0 {
					t.Fatal("legacy references gained receipt ownership")
				}
			case "legacy_completed_replay":
				for _, key := range []string{"legacy_completed_root", "legacy_completed_nonroot"} {
					s := f.evidence.Scenarios[key]
					before := restoreRows(t, f.db.Pool, true)
					file, err := f.complete(t, s)
					if err != nil || s.FileID == nil || uuid.UUID(file.ID.Bytes) != *s.FileID {
						t.Fatal("old completed replay changed current result")
					}
					f.download(t, *s.FileID, s.Plain)
					if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
						t.Fatal("completed replay mutated durable evidence")
					}
					if _, err := f.catalog.Trash(ctx, 1, *s.FileID); err != nil {
						t.Fatal(err)
					}
					if _, err := f.complete(t, s); err == nil {
						t.Fatal("trashed completed result replayed")
					}
					if err := f.ops.QueuePurge(ctx, 1, *s.FileID); err != nil {
						t.Fatal(err)
					}
					if err := f.ops.PurgeMany(ctx, 1, []uuid.UUID{*s.FileID}); !errors.Is(err, fileops.ErrPurgeIncomplete) {
						t.Fatal("historical purge did not truthfully retain unowned physical evidence")
					}
					if _, err := f.complete(t, s); err == nil {
						t.Fatal("purged/pending legacy result replayed")
					}
				}
			case "unknown_resume_claim_put_refused":
				s := f.evidence.Scenarios["detached_owned"]
				before := restoreRows(t, f.db.Pool, true)
				if _, err := f.uploads.Get(ctx, 1, s.UploadID); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatal("exact resume Get revived unknown")
				}
				if _, err := f.uploads.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1, UploadID: s.UploadID, PartNo: 1, ChannelID: 9001, PlainSize: s.Size}); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatal("unknown Claim gained lease")
				}
				if _, err := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1, UploadID: s.UploadID, PartNo: 1, RequestedChannelID: 9001, PlainSize: s.Size, Body: bytes.NewReader(s.Plain)}); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatal("unknown PUT succeeded")
				}
				raw, err := sqlcgen.New(f.db.Pool).GetUploadPart(ctx, sqlcgen.GetUploadPartParams{UploadID: dbtypes.UUID(s.UploadID), PartNo: 1})
				if err != nil {
					t.Fatal("original old stored part missing")
				}
				token := uuid.New()
				if raw.LeaseToken.Valid {
					token = uuid.UUID(raw.LeaseToken.Bytes)
				}
				for range 2 {
					if _, err := f.uploads.GetPart(ctx, 1, s.UploadID, 1); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("unknown operational part read bypassed destination veto")
					}
					if err := f.uploads.RenewPart(ctx, uploads.RenewPartInput{UploadID: s.UploadID, PartNo: 1, LeaseToken: token}); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("unknown renewal touched retained lease")
					}
					if _, err := f.uploads.StorePart(ctx, uploads.StorePartInput{UploadID: s.UploadID, ReceiptID: uuid.New(), PartNo: 1, LeaseToken: token, MessageID: raw.MessageID.Int64, StoredSize: raw.StoredSize.Int64, Checksum: raw.Checksum.String, BlockHashes: raw.BlockHashes}); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("unknown Store reached receipt/lease binding")
					}
					if _, err := f.uploads.ListParts(ctx, uploads.ListPartsInput{UserID: 1, UploadID: s.UploadID, Limit: 100}); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("unknown part listing exposed usable proof")
					}
					algorithm := "blake3"
					if _, err := f.uploads.Create(ctx, uploads.CreateInput{ID: s.UploadID, UserID: 1, Name: "detached_owned", ExpectedSize: s.Size, ExpectedHashAlgorithm: &algorithm, ExpectedHashValue: &s.Hash, ConflictPolicy: sqlcgen.NameConflictPolicyReplace}); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("unknown existing-ID resume admitted replacement")
					}
					if _, err := f.complete(t, s); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("unknown original manifest revived")
					}
					f.compose(t)
				}
				if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
					t.Fatal("unknown resume/Claim/PUT mutated evidence")
				}
			case "unknown_evidence_survives_sweeps":
				// Advance only the fixture expiry boundary, after exact migration checks.
				if _, err := f.db.Pool.Exec(ctx, "UPDATE teldrive.upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE terminal_failure_code='upload_destination_unknown'"); err != nil {
					t.Fatal(err)
				}
				before := restoreRows(t, f.db.Pool, true)
				for range 2 {
					worker := jobs.NewUploadCleanupWorker(f.db.Pool, f.policy)
					if err := worker.Work(ctx, &river.Job[jobs.UploadCleanupSweepArgs]{Args: jobs.UploadCleanupSweepArgs{}}); !errors.Is(err, jobs.ErrUploadCleanupIncomplete) {
						t.Fatal("unknown sweep did not truthfully report retained evidence")
					}
					if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
						t.Fatal("automatic sweep changed unknown legacy rows/parts/leases/events")
					}
					f.bytes(t)
				}
			case "migration_delta_trigger_safe":
				for key, s := range f.evidence.Scenarios {
					var raw []byte
					if f.db.Pool.QueryRow(ctx, "SELECT to_jsonb(s)-'terminal_failure_code' FROM teldrive.upload_sessions s WHERE id=$1", s.UploadID).Scan(&raw) != nil {
						t.Fatal("legacy session missing")
					}
					var originals []map[string]any
					if json.Unmarshal(f.evidence.Rows["upload_sessions"], &originals) != nil {
						t.Fatal("legacy session manifest invalid")
					}
					found := false
					for _, row := range originals {
						if row["id"] == s.UploadID.String() {
							b, _ := json.Marshal(row)
							found = canonicalJSON(raw) == canonicalJSON(b)
						}
					}
					if !found {
						t.Fatalf("migration changed original session for %s", key)
					}
				}
				for _, key := range []string{"legacy_completing", "legacy_expired"} {
					s, ok := f.evidence.Scenarios[key]
					if !ok {
						t.Fatal("required legacy state fixture missing")
					}
					before := restoreRows(t, f.db.Pool, true)
					if _, err := f.complete(t, s); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("original valid completing/expired manifest revived")
					}
					if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
						t.Fatal("refused completing/expired completion changed durable evidence")
					}
					f.bytes(t)
				}
			}
			f.bytes(t)
		})
	}
}

func (f *restoredFixture) seed(t *testing.T) {
	t.Helper()
	if _, err := f.db.Pool.Exec(context.Background(), "INSERT INTO teldrive.users(user_id) VALUES(1),(2); INSERT INTO teldrive.channels(channel_id,user_id,name,selected) VALUES(9001,1,'synthetic',true)"); err != nil {
		t.Fatal("synthetic restored seed failed")
	}
}
func (f *restoredFixture) folder(t *testing.T, name string, parent *uuid.UUID) uuid.UUID {
	t.Helper()
	row, err := f.catalog.CreateFolder(context.Background(), catalog.CreateFolderInput{UserID: 1, ParentID: parent, Name: name})
	if err != nil {
		t.Fatal(err)
	}
	return uuid.UUID(row.ID.Bytes)
}
func (f *restoredFixture) cloneCandidate(t *testing.T) {
	t.Helper()
	if f.started {
		t.Fatal("candidate snapshot requires stopped fixture runtime")
	}
	if f.restores >= len(f.input.TargetDatabases) {
		t.Fatal("next distinct candidate restore target not approved")
	}
	before := restoreRows(t, f.db.Pool, true)
	stem := strings.ReplaceAll(uuid.NewString(), "-", "")
	snap, err := f.db.Dump(context.Background(), stem)
	if err != nil || snap.MigrationVersion != 15 {
		t.Fatal("real candidate snapshot failed")
	}
	db, err := f.root.CloneSnapshot(context.Background(), filepath.Join(f.input.Directory, stem+".snapshot.json"), f.input.TargetDatabases[f.restores])
	if err != nil {
		t.Fatal("distinct candidate pg_restore failed")
	}
	if db.Name == f.db.Name || !equalJSONMaps(before, restoreRows(t, db.Pool, true)) {
		t.Fatal("candidate restore changed exact lifecycle evidence")
	}
	cleanupRestoredDatabase(t, db, false)
	f.db = db
	f.restores++
	f.compose(t)
}
func (f *restoredFixture) blockedDown(t *testing.T) {
	t.Helper()
	if f.started {
		t.Fatal("rollback snapshot cannot race fixture workers")
	}
	before := restoreRows(t, f.db.Pool, true)
	err := f.db.Mutate(context.Background(), func(ctx context.Context, pool *pgxpool.Pool) error {
		db := stdlib.OpenDBFromPool(pool)
		defer db.Close()
		return migrations.Down(ctx, db, "teldrive")
	})
	if err == nil {
		t.Fatal("actual Down erased nonempty lifecycle evidence")
	}
	if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
		t.Fatal("blocked Down changed exact rows/events/parts/leases/reasons")
	}
	var version int64
	if f.db.Pool.QueryRow(context.Background(), "SELECT max(version_id) FROM teldrive.migrations WHERE is_applied").Scan(&version) != nil || version != 15 {
		t.Fatal("blocked Down changed migration version")
	}
	f.bytes(t)
	f.up(t)
}
func eventuallyRestored(t *testing.T, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for !check() {
		if ctx.Err() != nil {
			t.Fatal("bounded actual restored scheduler observation failed")
		}
		runtime.Gosched()
	}
}
func (f *restoredFixture) start(t *testing.T) {
	t.Helper()
	if f.started {
		t.Fatal("repeated Start is not restored recovery")
	}
	if err := f.runtime.Start(context.Background()); err != nil {
		t.Fatal("fresh actual restored Runtime failed")
	}
	f.started = true
	row, err := f.client.PeriodicJobGet(context.Background(), "receipt_cleanup_sweep")
	if err != nil || row.Kind != jobs.ReceiptDiscoverySweepKind || row.Queue != jobs.CleanupQueue || row.Priority != 2 || row.MaxAttempts != 3 || len(row.Tags) != 0 || canonicalJSON(row.Args) != "{}" || row.CronExpression == nil || *row.CronExpression != "@every 12h" || row.CronTimezone != "UTC" || row.PausedAt != nil {
		t.Fatal("restored mandatory definition did not reconcile completely")
	}
}
func (f *restoredFixture) stop(t *testing.T) {
	t.Helper()
	if !f.started {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.runtime.Stop(ctx); err != nil {
		t.Fatal("restored runtime stop failed")
	}
	f.started = false
}
func (f *restoredFixture) due(t *testing.T) {
	t.Helper()
	_, err := f.client.PeriodicJobUpsert(context.Background(), &riverpro.PeriodicJobSpec{ID: "receipt_cleanup_sweep", Kind: jobs.ReceiptDiscoverySweepKind, Args: []byte("{}"), Queue: jobs.CleanupQueue, Priority: 2, MaxAttempts: 3, Tags: []string{}, Schedule: &riverpro.PeriodicJobSchedule{CronExpression: "@every 12h", CronTimezone: "UTC", NextRunAt: time.Now().Add(-time.Second).UTC()}})
	if err != nil {
		t.Fatal("fixture could not make persisted mandatory discovery due")
	}
}

// The tracer stops only after real Observe/Reserve COMMIT. No SQL state is mocked.
type restoredCrash struct {
	mu    sync.Mutex
	seen  map[uint32]bool
	armed atomic.Bool
	query string
}
type restoredQueryKey struct{}

func (c *restoredCrash) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, restoredQueryKey{}, d.SQL)
}
func (c *restoredCrash) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, d pgx.TraceQueryEndData) {
	if d.Err != nil || !c.armed.Load() {
		return
	}
	sql, _ := ctx.Value(restoredQueryKey{}).(string)
	pid := conn.PgConn().PID()
	c.mu.Lock()
	if strings.Contains(sql, "-- name: "+c.query) {
		c.seen[pid] = true
	}
	crash := strings.EqualFold(strings.TrimSpace(sql), "commit") && c.seen[pid]
	if crash {
		delete(c.seen, pid)
		c.armed.Store(false)
	}
	c.mu.Unlock()
	if crash {
		panic(fixture.ErrProcessLost)
	}
}
func (f *restoredFixture) copyCrash(t *testing.T, source, parent uuid.UUID, empty bool) {
	t.Helper()
	tracer := &restoredCrash{seen: map[uint32]bool{}, query: "ObserveMessageReceipt"}
	if empty {
		tracer.query = "ReserveCopyOperation"
	}
	tracer.armed.Store(true)
	cfg, err := pgxpool.ParseConfig(f.db.URL)
	if err != nil {
		t.Fatal("trace pool config failed")
	}
	cfg.ConnConfig.Fallbacks = nil
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	policy := lifecycle.NewService(pool, f.storage)
	if err := policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewService(pool, nil)
	if err := cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	ops, err := fileops.NewService(pool, cat, channels.NewService(pool, nil, channels.Config{PartLimit: 10000}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	var lost any
	func() {
		defer func() { lost = recover() }()
		name := uuid.NewString()
		_, err = ops.Copy(context.Background(), fileops.CopyInput{UserID: 1, FileID: source, ParentID: &parent, Name: &name})
	}()
	if lost != fixture.ErrProcessLost {
		t.Fatal("actual Copy did not cross committed crash boundary")
	}
	if _, err := f.db.Pool.Exec(context.Background(), "UPDATE teldrive.remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE origin_kind='copy' AND state IN ('reserved','observed')"); err != nil {
		t.Fatal(err)
	}
}

func TestB2RestoredRecovery(t *testing.T) {
	t.Run("ledger_only_discovery", func(t *testing.T) {
		for _, definition := range []string{"paused", "wrong"} {
			t.Run(definition, func(t *testing.T) {
				f := newRestoredFixture(t, "empty")
				f.seed(t)
				ctx := context.Background()
				data := []byte("independent restored source bytes")
				_, source := f.publish(t, nil, "source", data)
				parent := f.folder(t, "destination", nil)
				f.copyCrash(t, source, parent, false)
				empty := f.folder(t, "empty-source", nil)
				f.copyCrash(t, empty, parent, true)
				op, result, token := uuid.New(), uuid.New(), uuid.New()
				tx, err := f.db.Pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_, _, err = f.policy.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1, OperationID: op, RootResultFileID: result, LeaseToken: token, DestinationParentID: &parent, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()})
				if err == nil {
					err = tx.Commit(ctx)
				}
				_ = tx.Rollback(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: uuid.New(), UserID: 1, ChannelID: 9001, OriginKind: "copy", OperationID: &op, ResultFileID: &result, PartNo: 1, LeaseToken: token, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()}); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.Pool.Exec(ctx, "UPDATE teldrive.remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE origin_kind='copy' AND state IN ('reserved','observed')"); err != nil {
					t.Fatal(err)
				}
				var debt, refs, jobsCount int
				if f.db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM teldrive.remote_write_receipts WHERE state='observed' AND job_id IS NULL),(SELECT count(*) FROM teldrive.file_parts p JOIN teldrive.remote_write_receipts r ON r.message_id=p.message_id AND r.channel_id=p.channel_id WHERE r.state='observed'),(SELECT count(*) FROM teldrive.river_job WHERE kind=$1)`, jobs.ReceiptCleanupKind).Scan(&debt, &refs, &jobsCount) != nil || debt != 1 || refs != 0 || jobsCount != 0 {
					t.Fatal("pre-dump Copy debt was not real ledger-only Observe")
				}
				spec := &riverpro.PeriodicJobSpec{ID: "receipt_cleanup_sweep", Kind: jobs.ReceiptDiscoverySweepKind, Args: []byte("{}"), Queue: jobs.CleanupQueue, Priority: 2, MaxAttempts: 3, Schedule: &riverpro.PeriodicJobSchedule{CronExpression: "@every 12h", CronTimezone: "UTC", NextRunAt: time.Now().Add(-time.Second).UTC()}, Paused: definition == "paused"}
				if definition == "wrong" {
					spec.Kind = jobs.UploadCleanupSweepKind
					spec.Args = []byte(`{"wrong":true}`)
					spec.Queue = jobs.UploadQueue
					spec.Priority = 4
					spec.MaxAttempts = 1
					spec.Tags = []string{"wrong"}
					spec.Schedule.CronExpression = "@every 24h"
					spec.Schedule.CronTimezone = "Europe/London"
				}
				if _, err := f.client.PeriodicJobUpsert(ctx, spec); err != nil {
					t.Fatal(err)
				}
				f.cloneCandidate(t)
				f.blockedDown(t)
				before := len(f.storage.Deletes())
				f.start(t)
				f.due(t)
				eventuallyRestored(t, func() bool {
					var pending int
					return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM teldrive.remote_write_receipts WHERE origin_kind='copy' AND published_at IS NULL AND (state NOT IN ('deleted','unresolved') OR job_id IS NULL OR wakeup_job_id IS NULL)").Scan(&pending) == nil && pending == 0
				})
				if len(f.storage.Deletes()) != before+1 {
					t.Fatal("restored discovery sent wrong number of exact-message destructive calls")
				}
				f.stop(t)
				f.download(t, source, data)
				f.blockedDown(t)
				var first, current int64
				if f.db.Pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id FROM teldrive.remote_write_receipts WHERE state='deleted'").Scan(&first, &current) != nil || first <= 0 || current <= 0 {
					t.Fatal("restored discovery lost actual first/current report IDs")
				}
				if _, err := f.db.Pool.Exec(ctx, "DELETE FROM teldrive.river_job WHERE id=ANY($1)", []int64{first, current}); err != nil {
					t.Fatal(err)
				}
				f.cloneCandidate(t)
				item, err := f.runtime.GetForUser(ctx, first, 1)
				if err != nil || item.ID != first {
					t.Fatal("River retention erased restored report")
				}
				if _, err := f.runtime.GetForUser(ctx, first, 2); !errors.Is(err, river.ErrNotFound) {
					t.Fatal("restored report crossed owner")
				}
				f.bytes(t)
			})
		}
	})
	t.Run("copy_destination_terminal_epoch", testRestoredCopyEpoch)
	t.Run("completed_put_retired_alias", testRestoredCompletedAlias)
	t.Run("request_admission_replay", testRestoredAdmissions)
	t.Run("import_q_anchor_loss", testRestoredImportAnchors)
	t.Run("receipt_interruption_budget", testRestoredReceiptBudget)
	t.Run("import_asb_retention", testRestoredImportStages)
}

func testRestoredCopyEpoch(t *testing.T) {
	f := newRestoredFixture(t, "empty")
	f.seed(t)
	ctx := context.Background()
	data := []byte("copy source keeps independent bytes")
	_, source := f.publish(t, nil, "source", data)
	parent := f.folder(t, "copy-parent", nil)
	actor := int64(1)
	name := "loser"
	admission := &lifecycle.RequestAdmissionInput{Scope: lifecycle.RequestScope{ActorUserID: &actor}, RequestKey: uuid.New(), RequestFingerprint: sha256.Sum256([]byte("fixed original copy"))}
	in := fileops.CopyInput{UserID: 1, FileID: source, ParentID: &parent, Name: &name, Admission: admission}
	barrier := fixture.NewBarrier(t, nil)
	f.storage.SetSendFaults(fixture.SendFaults{AfterCopy: barrier})
	done := make(chan error, 1)
	go func() { _, err := f.ops.Copy(ctx, in); done <- err }()
	if err := barrier.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.catalog.Trash(ctx, 1, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := f.catalog.Restore(ctx, 1, parent); err != nil {
		t.Fatal(err)
	}
	barrier.Release()
	if err := <-done; !errors.Is(err, lifecycle.ErrCopyDestinationUnavailable) {
		t.Fatal("actual restored-parent Copy revived terminal epoch")
	}
	f.storage.SetSendFaults(fixture.SendFaults{})
	f.cloneCandidate(t)
	f.blockedDown(t)
	before := restoreRows(t, f.db.Pool, true)
	if _, err := f.ops.Copy(ctx, in); !errors.Is(err, lifecycle.ErrCopyDestinationUnavailable) {
		t.Fatal("fresh restored original Copy revived losing operation")
	}
	if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
		t.Fatal("terminal copy replay changed immutable epoch/admission/publication")
	}
	winnerName := "winner"
	winner, err := f.ops.Copy(ctx, fileops.CopyInput{UserID: 1, FileID: source, Name: &winnerName})
	if err != nil {
		t.Fatal(err)
	}
	winnerID := uuid.UUID(winner.ID.Bytes)
	f.start(t)
	f.due(t)
	eventuallyRestored(t, func() bool {
		var pending int
		return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM teldrive.remote_write_receipts WHERE row_kind='message_attempt' AND state IN ('cleanup_pending','deleting')").Scan(&pending) == nil && pending == 0
	})
	f.stop(t)
	f.download(t, source, data)
	f.download(t, winnerID, data)
	f.bytes(t)
}
func testRestoredCompletedAlias(t *testing.T) {
	f := newRestoredFixture(t, "empty")
	f.seed(t)
	ctx := context.Background()
	data := bytes.Repeat([]byte("retired aliases retain proof\n"), 128)
	id, fid := f.publish(t, nil, "completed", data)
	var aliases int
	if f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM teldrive.upload_parts WHERE upload_id=$1", id).Scan(&aliases) != nil || aliases != 0 {
		t.Fatal("actual Complete did not retire owned alias")
	}
	f.cloneCandidate(t)
	f.blockedDown(t)
	before := restoreRows(t, f.db.Pool, true)
	part, err := f.uploads.GetPart(ctx, 1, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1, UploadID: id, PartNo: 1, RequestedChannelID: 9001, PlainSize: int64(len(data)), Body: bytes.NewReader(data)}); err != nil {
		t.Fatal("restored completed PUT proof failed")
	}
	parts, err := f.uploads.ListParts(ctx, uploads.ListPartsInput{UserID: 1, UploadID: id, Limit: 100})
	if err != nil || len(parts) != 1 || parts[0].CreatedAt != part.CreatedAt || parts[0].UpdatedAt != part.UpdatedAt {
		t.Fatal("restored completed list lost original timestamps")
	}
	if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
		t.Fatal("completed PUT/list recreated reference/lease/event")
	}
	f.download(t, fid, data)
	if _, err := f.catalog.Trash(ctx, 1, fid); err != nil {
		t.Fatal(err)
	}
	if err := f.ops.QueuePurge(ctx, 1, fid); err != nil {
		t.Fatal(err)
	}
	if err := f.ops.PurgeMany(ctx, 1, []uuid.UUID{fid}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1, UploadID: id, PartNo: 1, RequestedChannelID: 9001, PlainSize: int64(len(data)), Body: bytes.NewReader(data)}); err == nil {
		t.Fatal("retired proof resurrected purged result")
	}
	if f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM teldrive.upload_parts WHERE upload_id=$1", id).Scan(&aliases) != nil || aliases != 0 {
		t.Fatal("purge replay recreated alias")
	}
}

func testRestoredAdmissions(t *testing.T) {
	for _, scope := range []string{"owner", "public"} {
		t.Run(scope, func(t *testing.T) {
			for _, outcome := range []string{"upload_terminal", "upload_success", "copy_terminal", "copy_success"} {
				t.Run(outcome, func(t *testing.T) {
					f := newRestoredFixture(t, "empty")
					f.seed(t)
					ctx := context.Background()
					parent := f.folder(t, "admitted-parent", nil)
					actor := int64(1)
					access, err := shares.NewService(f.db.Pool, f.catalog)
					if err != nil {
						t.Fatal(err)
					}
					request := lifecycle.RequestAdmissionInput{Scope: lifecycle.RequestScope{ActorUserID: &actor}, RequestKey: uuid.New(), RequestFingerprint: sha256.Sum256([]byte("fixed " + outcome))}
					var public *shares.Created
					if scope == "public" {
						public, err = access.Create(ctx, shares.CreateInput{OwnerID: 1, FileID: parent, Permission: sqlcgen.SharePermissionEdit})
						if err != nil {
							t.Fatal(err)
						}
						shareID := uuid.UUID(public.Row.ID.Bytes)
						request.Scope = lifecycle.RequestScope{PublicShareID: &shareID}
						if _, resolved, err := access.ResolvePublicEditableParent(ctx, public.Token, "", &parent); err != nil || resolved != parent {
							t.Fatal("real public parent authorization failed")
						}
					}
					data := []byte("original admitted plaintext")
					kind := "upload"
					if strings.HasPrefix(outcome, "copy") {
						kind = "copy"
					}
					var original, result, transportSource uuid.UUID
					var upload uploads.CreateInput
					var copyInput fileops.CopyInput
					terminal := strings.HasSuffix(outcome, "terminal")
					if kind == "upload" {
						size := int64(len(data))
						if terminal {
							size = 0
						}
						upload = uploads.CreateInput{ID: uuid.New(), UserID: 1, ParentID: &parent, Name: "original", ExpectedSize: size, Admission: &request}
						row, err := f.uploads.Create(ctx, upload)
						if err != nil {
							t.Fatal(err)
						}
						original = uuid.UUID(row.ID.Bytes)
						if !terminal {
							if _, err := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1, UploadID: original, PartNo: 1, RequestedChannelID: 9001, PlainSize: size, Body: bytes.NewReader(data)}); err != nil {
								t.Fatal(err)
							}
							h := treehash.NewBlockHasher()
							_, _ = h.Write(data)
							algorithm, hash := "blake3", treehash.SumToHex(treehash.ComputeTreeHash(h.Sum()))
							file, err := f.uploads.Complete(ctx, uploads.CompleteInput{UserID: 1, UploadID: original, Size: size, HashAlgorithm: &algorithm, HashValue: &hash})
							if err != nil {
								t.Fatal(err)
							}
							result = uuid.UUID(file.ID.Bytes)
						}
					} else {
						_, source := f.publish(t, &parent, "source", data)
						transportSource = source
						name := "original-copy"
						copyInput = fileops.CopyInput{UserID: 1, FileID: source, ParentID: &parent, Name: &name, Admission: &request}
						if terminal {
							gate := fixture.NewBarrier(t, nil)
							f.storage.SetSendFaults(fixture.SendFaults{AfterCopy: gate})
							done := make(chan error, 1)
							go func() { _, err := f.ops.Copy(ctx, copyInput); done <- err }()
							if err := gate.Wait(ctx); err != nil {
								t.Fatal(err)
							}
							if _, err := f.catalog.Trash(ctx, 1, parent); err != nil {
								t.Fatal(err)
							}
							if _, err := f.catalog.Restore(ctx, 1, parent); err != nil {
								t.Fatal(err)
							}
							gate.Release()
							if err := <-done; !errors.Is(err, lifecycle.ErrCopyDestinationUnavailable) {
								t.Fatal("copy terminal response was not actual parent loss")
							}
							f.storage.SetSendFaults(fixture.SendFaults{})
						} else {
							file, err := f.ops.Copy(ctx, copyInput)
							if err != nil {
								t.Fatal(err)
							}
							result = uuid.UUID(file.ID.Bytes)
						}
						binding, err := f.policy.GetRequestAdmission(ctx, request.Scope, kind, request.RequestKey)
						if err != nil {
							t.Fatal(err)
						}
						original = uuid.UUID(binding.CopyOperationID.Bytes)
					}
					if terminal && kind == "upload" {
						if _, err := f.catalog.Trash(ctx, 1, parent); err != nil {
							t.Fatal(err)
						}
						if _, err := f.catalog.Restore(ctx, 1, parent); err != nil {
							t.Fatal(err)
						}
					}
					// Delivered responses are deliberately discarded; only persisted IDs survive.
					f.cloneCandidate(t)
					f.blockedDown(t)
					before := restoreRows(t, f.db.Pool, true)
					access, err = shares.NewService(f.db.Pool, f.catalog)
					if err != nil {
						t.Fatal(err)
					}
					if !terminal {
						if _, err := access.ResolveAccess(ctx, 2, result, false); err == nil {
							t.Fatal("foreign actor gained restored result access")
						}
						if scope == "public" {
							if _, err := access.ResolvePublicEditableFile(ctx, public.Token, "", result); err != nil {
								t.Fatal("restored public result lost current access")
							}
						}
					}
					if kind == "upload" {
						row, err := f.uploads.Create(ctx, upload)
						if terminal {
							if !errors.Is(err, uploads.ErrParentUnavailable) {
								t.Fatal("restored upload admission replaced original terminal attempt")
							}
						} else if err != nil || uuid.UUID(row.ID.Bytes) != original {
							t.Fatal("restored upload admission changed original UUID")
						}
					} else {
						row, err := f.ops.Copy(ctx, copyInput)
						if terminal {
							if !errors.Is(err, lifecycle.ErrCopyDestinationUnavailable) {
								t.Fatal("restored copy admission revived terminal attempt")
							}
						} else if err != nil || uuid.UUID(row.ID.Bytes) != result {
							t.Fatal("restored copy replay changed result")
						}
					}
					binding, err := f.policy.GetRequestAdmission(ctx, request.Scope, kind, request.RequestKey)
					if err != nil {
						t.Fatal(err)
					}
					retained := binding.UploadID
					if kind == "copy" {
						retained = binding.CopyOperationID
					}
					if uuid.UUID(retained.Bytes) != original {
						t.Fatal("restored key lost original target")
					}
					changed := request
					changed.RequestFingerprint = sha256.Sum256([]byte("changed request"))
					if kind == "upload" {
						upload.Admission = &changed
						_, err = f.uploads.Create(ctx, upload)
					} else {
						copyInput.Admission = &changed
						_, err = f.ops.Copy(ctx, copyInput)
					}
					if !errors.Is(err, lifecycle.ErrRequestMismatch) {
						t.Fatal("restored admission accepted changed fingerprint")
					}
					if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
						t.Fatal("lost-response replay/mismatch changed identities/receipts/events")
					}
					if !terminal {
						f.download(t, result, data)
						if _, err := f.catalog.Trash(ctx, 1, result); err != nil {
							t.Fatal(err)
						}
						if err := f.ops.QueuePurge(ctx, 1, result); err != nil {
							t.Fatal(err)
						}
						if err := f.ops.PurgeMany(ctx, 1, []uuid.UUID{result}); err != nil {
							t.Fatal(err)
						}
						upload.Admission = &request
						copyInput.Admission = &request
						if kind == "upload" {
							_, err = f.uploads.Create(ctx, upload)
						} else {
							_, err = f.ops.Copy(ctx, copyInput)
						}
						if err == nil {
							t.Fatal("removed restored result was recreated by admission replay")
						}
						if scope == "public" {
							if _, err := access.ResolvePublicEditableFile(ctx, public.Token, "", result); err == nil {
								t.Fatal("public authorization ignored removed current result")
							}
						}
					}
					if transportSource != uuid.Nil {
						f.download(t, transportSource, data)
					}
				})
			}
		})
	}
}

type restoredHTTP func(*http.Request) (*http.Response, error)

func (fn restoredHTTP) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func testRestoredImportAnchors(t *testing.T) {
	for _, kind := range []string{"uuid", "absolute", "relative"} {
		t.Run(kind, func(t *testing.T) {
			for _, preflight := range []string{"failed", "blocked"} {
				t.Run(preflight, func(t *testing.T) {
					f := newRestoredFixture(t, "empty")
					f.seed(t)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					anchor := f.folder(t, "anchor", nil)
					target := anchor
					destination, spec := anchor.String(), "source.bin"
					if kind == "absolute" {
						destination = "/anchor/missing/child"
					}
					if kind == "relative" {
						target = f.folder(t, "pinned", &anchor)
						spec = "pinned/missing/source.bin"
					}
					args := jobs.UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1, Destination: destination, Sources: []jobs.UploadSource{{Type: "http", URL: "https://synthetic.invalid/source", DestinationPath: spec}}}
					job, err := f.runtime.InsertUploadBatch(ctx, args)
					if err != nil {
						t.Fatal(err)
					}
					var calls atomic.Int32
					entered, release := make(chan struct{}), make(chan struct{})
					client := &http.Client{Transport: restoredHTTP(func(r *http.Request) (*http.Response, error) {
						n := calls.Add(1)
						if preflight == "failed" {
							return nil, errors.New("synthetic preflight failure")
						}
						if n == 1 {
							close(entered)
							select {
							case <-release:
							case <-r.Context().Done():
								return nil, r.Context().Err()
							}
						}
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}}, ContentLength: 0, Body: io.NopCloser(bytes.NewReader(nil)), Request: r}, nil
					})}
					work := func() error {
						return jobs.NewUploadBatchWorker(f.db.Pool, client, f.catalog, f.uploads).Work(rivertest.WorkContext(ctx, f.client.Client), &river.Job[jobs.UploadBatchArgs]{JobRow: &rivertype.JobRow{ID: job.ID}, Args: args})
					}
					if calls.Load() != 0 {
						t.Fatal("actual Q performed source IO")
					}
					var done chan error
					if preflight == "failed" {
						if err := work(); err == nil {
							t.Fatal("controlled preflight unexpectedly succeeded")
						}
					} else {
						done = make(chan error, 1)
						go func() { done <- work() }()
						select {
						case <-entered:
						case <-ctx.Done():
							t.Fatal("actual blocked preflight not observed")
						}
					}
					if _, err := f.catalog.Trash(ctx, 1, target); err != nil {
						t.Fatal(err)
					}
					if _, err := f.catalog.Restore(ctx, 1, target); err != nil {
						t.Fatal(err)
					}
					if done != nil {
						close(release)
						if err := <-done; !errors.Is(err, uploads.ErrParentUnavailable) {
							t.Fatal("original blocked preflight revived Q after anchor loss")
						}
					}
					if _, err := f.db.Pool.Exec(ctx, "DELETE FROM teldrive.river_job WHERE id=$1", job.ID); err != nil {
						t.Fatal(err)
					}
					f.cloneCandidate(t)
					f.blockedDown(t)
					before := restoreRows(t, f.db.Pool, true)
					count := calls.Load()
					if err := work(); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("restored terminal Q resolved/dispatched again")
					}
					if _, err := f.runtime.InsertUploadBatch(ctx, args); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("restored Q admitted replacement batch")
					}
					if calls.Load() != count || !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
						t.Fatal("restored Q reopened source or changed exact anchors/batch")
					}
				})
			}
		})
	}
}

func testRestoredReceiptBudget(t *testing.T) {
	for _, mode := range []string{"delete_ack_loss", "inaccessible_channel", "crash_after_claim"} {
		t.Run(mode, func(t *testing.T) {
			f := newRestoredFixture(t, "empty")
			f.seed(t)
			ctx := context.Background()
			payload := []byte("winner and source remain readable")
			_, source := f.publish(t, nil, "protected-source", payload)
			parent := f.folder(t, "cleanup-parent", nil)
			f.copyCrash(t, source, parent, false)
			var id uuid.UUID
			var message int64
			if f.db.Pool.QueryRow(ctx, "SELECT id,message_id FROM teldrive.remote_write_receipts WHERE row_kind='message_attempt' AND state='observed'").Scan(&id, &message) != nil {
				t.Fatal("real observed cleanup debt missing")
			}
			if err := f.policy.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if mode == "delete_ack_loss" {
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE teldrive.remote_write_receipts ADD CONSTRAINT restored_ack_failure CHECK(state<>'deleted')"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "inaccessible_channel" {
				f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: errors.New("synthetic inaccessible channel")})
			}
			if mode == "crash_after_claim" {
				claims, err := f.policy.ClaimDelete(ctx, 1, []uuid.UUID{id})
				if err != nil || len(claims) != 1 || claims[0].Attempts != 1 {
					t.Fatal("actual committed deletion reservation missing")
				}
			} else if err := f.policy.Recover(ctx, 1, []uuid.UUID{id}); err == nil {
				t.Fatal("real injected remote/ACK failure reported success")
			}
			f.cloneCandidate(t)
			f.blockedDown(t)
			for attempt := 2; attempt <= 4; attempt++ {
				if _, err := f.db.Pool.Exec(ctx, "UPDATE teldrive.remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				f.compose(t)
				err := f.policy.Recover(ctx, 1, []uuid.UUID{id})
				if mode == "crash_after_claim" {
					if err != nil {
						t.Fatal("forward recovery after committed unused claim failed")
					}
					break
				}
				if err == nil {
					t.Fatal("injected restored failure became success")
				}
				var got int
				if f.db.Pool.QueryRow(ctx, "SELECT attempts FROM teldrive.remote_write_receipts WHERE id=$1", id).Scan(&got) != nil || got != attempt {
					t.Fatal("restored retry reset or skipped durable claim budget")
				}
				if _, err := f.db.Pool.Exec(ctx, "DELETE FROM teldrive.river_job WHERE kind=$1", jobs.ReceiptCleanupKind); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "crash_after_claim" {
				if _, err := f.db.Pool.Exec(ctx, "UPDATE teldrive.remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				if err := f.policy.Recover(ctx, 1, []uuid.UUID{id}); !errors.Is(err, lifecycle.ErrAttemptsExhausted) {
					t.Fatal("restored fifth destructive reservation was not refused")
				}
				if len(f.storage.Deletes()) != 4 {
					t.Fatal("restored lifetime budget was not four exact Storage calls")
				}
			}
			_, exists := f.storage.Bytes(9001, message)
			if (mode == "inaccessible_channel") != exists {
				t.Fatal("byte oracle disagreed with actual remote deletion/retention")
			}
			f.cloneCandidate(t)
			f.download(t, source, payload)
			f.bytes(t)
			f.blockedDown(t)
		})
	}
}

// A uses the actual worker and its actual encrypted InsertManyTx child insertion.
func (f *restoredFixture) batch(t *testing.T, parent uuid.UUID, empty bool) (jobs.UploadBatchArgs, int64, []*sqlcgen.ImportJobAttempt) {
	t.Helper()
	ctx := context.Background()
	args := jobs.UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1, Destination: parent.String(), Sources: []jobs.UploadSource{{Type: "http", URL: "https://synthetic.invalid/source", DestinationPath: "source.bin"}}}
	if empty {
		args.Exclude = []string{"**"}
	}
	job, err := f.runtime.InsertUploadBatch(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: restoredHTTP(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}, "ETag": {`"synthetic-version"`}}, ContentLength: 0, Body: io.NopCloser(bytes.NewReader(nil)), Request: r}, nil
	})}
	worker := jobs.NewUploadBatchWorker(f.db.Pool, client, f.catalog, f.uploads)
	if err := worker.Work(rivertest.WorkContext(ctx, f.client.Client), &river.Job[jobs.UploadBatchArgs]{JobRow: &rivertype.JobRow{ID: job.ID}, Args: args}); err != nil {
		t.Fatal("actual batch A/InsertManyTx failed")
	}
	rows, err := sqlcgen.New(f.db.Pool).ListImportBatchAttempts(ctx, sqlcgen.ListImportBatchAttemptsParams{UserID: 1, BatchID: dbtypes.UUID(uuid.MustParse(args.BatchID))})
	if err != nil {
		t.Fatal(err)
	}
	if empty && len(rows) != 0 || !empty && len(rows) != 1 {
		t.Fatal("actual A child count differs from expansion")
	}
	return args, job.ID, rows
}
func testRestoredImportStages(t *testing.T) {
	for _, stage := range []string{"zero_source", "destination_only", "selected_no_session", "materialized", "completed", "terminal", "matching_result"} {
		t.Run(stage, func(t *testing.T) {
			f := newRestoredFixture(t, "empty")
			f.seed(t)
			ctx := context.Background()
			parent := f.folder(t, "imports", nil)
			args, origin, children := f.batch(t, parent, stage == "zero_source")
			var create uploads.CreateInput
			var attemptID, resultID uuid.UUID
			if len(children) > 0 {
				a := children[0]
				attemptID = uuid.UUID(a.AttemptID.Bytes)
				create = uploads.CreateInput{ID: uuid.New(), UserID: 1, ParentID: &parent, Name: "source.bin", ExpectedSize: 0, ImportAttemptID: &attemptID}
				if stage == "matching_result" {
					_, result := f.publish(t, &parent, "source.bin", nil)
					tx, err := f.db.Pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.catalog.CheckActiveImportResultTx(ctx, tx, catalog.CheckActiveImportResultInput{UserID: 1, FileID: result, ParentID: &parent}); err != nil {
						t.Fatal(err)
					}
					if _, err := f.uploads.RecordImportResultTx(ctx, tx, uploads.RecordImportResultInput{UserID: 1, BatchID: uuid.MustParse(args.BatchID), SourceIndex: 0, JobID: a.JobID, ResultFileID: result}); err != nil {
						t.Fatal(err)
					}
					if err := tx.Commit(ctx); err != nil {
						t.Fatal(err)
					}
					resultID = result
				} else if stage != "destination_only" {
					tx, err := f.db.Pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					selected, err := f.uploads.ReserveImportAttemptTx(ctx, tx, uploads.ReserveImportAttemptInput{UserID: 1, BatchID: uuid.MustParse(args.BatchID), SourceIndex: 0, JobID: a.JobID, UploadID: create.ID, Create: create})
					if err != nil {
						t.Fatal(err)
					}
					if err := tx.Commit(ctx); err != nil {
						t.Fatal(err)
					}
					create.ID = uuid.UUID(selected.UploadID.Bytes)
					frozen, err := uploads.SelectedImportCreateInput(selected)
					if err != nil || len(selected.SelectedCreate) == 0 || frozen.ID != create.ID || frozen.ImportAttemptID == nil || *frozen.ImportAttemptID != attemptID {
						t.Fatal("real S selection did not retain a valid immutable create snapshot")
					}
					create = frozen
					if stage == "materialized" || stage == "terminal" || stage == "completed" {
						if _, err := f.uploads.Create(ctx, create); err != nil {
							t.Fatal(err)
						}
					}
					if stage == "completed" {
						file, err := f.uploads.Complete(ctx, uploads.CompleteInput{UserID: 1, UploadID: create.ID, Size: 0})
						if err != nil {
							t.Fatal(err)
						}
						resultID = uuid.UUID(file.ID.Bytes)
					}
				}
			}
			terminal := stage == "destination_only" || stage == "selected_no_session" || stage == "terminal"
			if terminal {
				if _, err := f.catalog.Trash(ctx, 1, parent); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Restore(ctx, 1, parent); err != nil {
					t.Fatal(err)
				}
			}
			for _, child := range children {
				if _, err := f.runtime.CancelForUser(ctx, child.JobID, 1); err != nil {
					t.Fatal("actual owner child cancellation failed")
				}
			}
			if _, err := f.runtime.CancelForUser(ctx, origin, 1); err != nil {
				t.Fatal("actual owner batch cancellation failed")
			}
			if _, err := f.db.Pool.Exec(ctx, "DELETE FROM teldrive.river_job WHERE id=$1 OR kind=$2", origin, jobs.UploadSourceKind); err != nil {
				t.Fatal(err)
			}
			f.cloneCandidate(t)
			f.blockedDown(t)
			before := restoreRows(t, f.db.Pool, true)
			var called atomic.Int32
			client := &http.Client{Transport: restoredHTTP(func(*http.Request) (*http.Response, error) {
				called.Add(1)
				return nil, errors.New("replay must not open source")
			})}
			worker := jobs.NewUploadBatchWorker(f.db.Pool, client, f.catalog, f.uploads)
			err := worker.Work(rivertest.WorkContext(ctx, f.client.Client), &river.Job[jobs.UploadBatchArgs]{JobRow: &rivertype.JobRow{ID: origin}, Args: args})
			if terminal {
				if !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatal("removed terminal batch revived")
				}
			} else if err != nil {
				t.Fatal("restored dispatched batch replay failed")
			}
			if len(children) > 0 && (terminal || stage == "matching_result" || stage == "completed") {
				source := &river.Job[jobs.UploadSourceArgs]{JobRow: &rivertype.JobRow{ID: children[0].JobID}, Args: jobs.UploadSourceArgs{BatchID: args.BatchID, SourceIndex: 0, UserID: 1, ParentID: parent.String(), Source: jobs.UploadFileSource{Type: "http", Location: "https://synthetic.invalid/source", DestinationPath: "source.bin", Size: 0}}}
				w := jobs.NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, client, 0, false, nil)
				err := w.Work(ctx, source)
				if terminal {
					if !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatal("removed terminal source revived selected identity")
					}
				} else if err != nil {
					t.Fatal("restored current-result source replay failed")
				}
			}
			if stage == "materialized" {
				row, err := f.uploads.Create(ctx, create)
				if err != nil || uuid.UUID(row.ID.Bytes) != create.ID {
					t.Fatal("restored B recreated session")
				}
			}
			if called.Load() != 0 || !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
				t.Fatal("restored Q/A/S/B replay changed identity/path/child/upload/result/events")
			}
			if resultID != uuid.Nil {
				if _, err := f.catalog.Trash(ctx, 1, resultID); err != nil {
					t.Fatal(err)
				}
				if err := f.ops.QueuePurge(ctx, 1, resultID); err != nil {
					t.Fatal(err)
				}
				if err := f.ops.PurgeMany(ctx, 1, []uuid.UUID{resultID}); err != nil {
					t.Fatal(err)
				}
				after := restoreRows(t, f.db.Pool, true)
				source := &river.Job[jobs.UploadSourceArgs]{JobRow: &rivertype.JobRow{ID: children[0].JobID}, Args: jobs.UploadSourceArgs{BatchID: args.BatchID, SourceIndex: 0, UserID: 1, ParentID: parent.String()}}
				if err := jobs.NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, client, 0, false, nil).Work(ctx, source); err == nil {
					t.Fatal("source replay recreated removed completed/matching result")
				}
				if called.Load() != 0 || !equalJSONMaps(after, restoreRows(t, f.db.Pool, true)) {
					t.Fatal("result loss changed original import binding or recreated source")
				}
			}
		})
	}
}

type restoredChannelCreator struct {
	storage *fixture.Storage
	mode    string
}

func (c restoredChannelCreator) Create(ctx context.Context, user int64, name string) (channels.RemoteChannel, error) {
	row, err := c.storage.CreateChannel(ctx, user, name)
	return channels.RemoteChannel{ID: row.ID, Name: row.Name}, err
}
func (c restoredChannelCreator) Delete(ctx context.Context, user, channel int64) error {
	if c.mode == "closing" {
		panic(fixture.ErrProcessLost)
	}
	if c.mode == "failed-closing" {
		return errors.New("synthetic channel unavailable")
	}
	return c.storage.DeleteChannel(ctx, user, channel)
}
func TestB2LifecycleRollbackEvidence(t *testing.T) {
	t.Run("unknown_destination_only", func(t *testing.T) {
		f := newRestoredFixture(t, "unknown_only")
		ctx := context.Background()
		var other int
		if f.db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM teldrive.remote_write_receipts)+(SELECT count(*) FROM teldrive.request_admissions)+(SELECT count(*) FROM teldrive.import_batches)+(SELECT count(*) FROM teldrive.import_job_attempts)+(SELECT count(*) FROM teldrive.import_destination_pins)+(SELECT count(*) FROM teldrive.channel_deletion_intents)`).Scan(&other) != nil || other != 0 {
			t.Fatal("unknown-only rollback oracle has unrelated candidate evidence")
		}
		f.blockedDown(t)
		f.compose(t)
		s := f.evidence.Scenarios["detached_empty"]
		if _, err := f.complete(t, s); !errors.Is(err, uploads.ErrParentUnavailable) {
			t.Fatal("forward restart revived original valid empty unknown")
		}
	})
	t.Run("empty_evidence_reup", func(t *testing.T) {
		f := newRestoredFixture(t, "empty")
		before := restoreRows(t, f.db.Pool, false)
		if err := f.db.Mutate(context.Background(), func(ctx context.Context, pool *pgxpool.Pool) error {
			db := stdlib.OpenDBFromPool(pool)
			defer db.Close()
			return migrations.Down(ctx, db, "teldrive")
		}); err != nil {
			t.Fatal("actual empty-evidence Down failed")
		}
		var version int64
		if f.db.Pool.QueryRow(context.Background(), "SELECT max(version_id) FROM teldrive.migrations WHERE is_applied").Scan(&version) != nil || version != 14 {
			t.Fatal("empty Down did not restore version14")
		}
		f.up(t)
		f.up(t)
		if !equalJSONMaps(before, restoreRows(t, f.db.Pool, false)) {
			t.Fatal("empty Down/re-up changed legacy rows")
		}
	})
	t.Run("channel_intent_only", func(t *testing.T) {
		for _, state := range []string{"closing", "failed-closing", "deleted"} {
			t.Run(state, func(t *testing.T) {
				f := newRestoredFixture(t, "empty")
				f.seed(t)
				ctx := context.Background()
				if _, err := f.db.Pool.Exec(ctx, "INSERT INTO teldrive.channels(channel_id,user_id,name,selected) VALUES(9002,1,'explicit-delete',false)"); err != nil {
					t.Fatal(err)
				}
				payload := []byte("unreferenced physical channel payload")
				if err := f.storage.Seed(fixture.Message{UserID: 1, ChannelID: 9002, MessageID: 71, Bytes: payload, Preserve: state != "deleted"}); err != nil {
					t.Fatal(err)
				}
				svc := channels.NewService(f.db.Pool, restoredChannelCreator{f.storage, state}, channels.Config{})
				var err error
				var lost any
				func() { defer func() { lost = recover() }(); err = svc.Delete(ctx, 1, 9002) }()
				if state == "closing" && lost != fixture.ErrProcessLost || state == "failed-closing" && err == nil || state == "deleted" && (err != nil || lost != nil) {
					t.Fatal("actual deliberate Delete did not create requested intent outcome")
				}
				assertChannelBytes := func() {
					t.Helper()
					b, exists := f.storage.Bytes(9002, 71)
					if state == "deleted" {
						if exists {
							t.Fatal("positive whole-channel ACK retained remote bytes")
						}
					} else if !exists || sha256.Sum256(b) != sha256.Sum256(payload) {
						t.Fatal("closing/failed physical intent lost independent bytes")
					}
				}
				assertChannelBytes()
				if _, err := f.db.Pool.Exec(ctx, "DELETE FROM teldrive.channels WHERE user_id=1; DELETE FROM teldrive.users WHERE user_id=1"); err != nil {
					t.Fatal("intent did not survive registration and requesting user removal")
				}
				f.cloneCandidate(t)
				origin, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 2, Name: "blocked-channel-origin", ExpectedSize: 1})
				if err != nil {
					t.Fatal(err)
				}
				u := uuid.UUID(origin.ID.Bytes)
				before := restoreRows(t, f.db.Pool, true)
				var rowState string
				var failure *string
				if f.db.Pool.QueryRow(ctx, "SELECT state,failure_code FROM teldrive.channel_deletion_intents WHERE channel_id=9002").Scan(&rowState, &failure) != nil || rowState != map[bool]string{true: "deleted", false: "closing"}[state == "deleted"] || (state == "failed-closing" && failure == nil) {
					t.Fatal("restored physical intent identity/outcome lost")
				}
				var other int
				if f.db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM teldrive.remote_write_receipts)+(SELECT count(*) FROM teldrive.request_admissions)+(SELECT count(*) FROM teldrive.import_batches)+(SELECT count(*) FROM teldrive.import_job_attempts)+(SELECT count(*) FROM teldrive.import_destination_pins)+(SELECT count(*) FROM teldrive.upload_sessions WHERE terminal_failure_code IS NOT NULL)`).Scan(&other) != nil || other != 0 {
					t.Fatal("channel-only oracle has unrelated lifecycle evidence")
				}
				f.blockedDown(t)
				fresh := channels.NewService(f.db.Pool, restoredChannelCreator{f.storage, ""}, channels.Config{})
				if _, err := fresh.Sync(ctx, 2, []channels.RemoteChannel{{ID: 9002, Name: "blocked"}}); !errors.Is(err, lifecycle.ErrChannelWriteUnavailable) {
					t.Fatal("restored tombstone allowed discovery registration")
				}
				if _, err := fresh.Select(ctx, 2, 9002); err == nil {
					t.Fatal("restored tombstone allowed selection")
				}
				if _, err := f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: uuid.New(), UserID: 2, ChannelID: 9002, OriginKind: "upload", UploadID: &u, PartNo: 1, LeaseToken: uuid.New(), LeaseExpiresAt: time.Now().Add(time.Minute).UTC()}); !errors.Is(err, lifecycle.ErrChannelWriteUnavailable) {
					t.Fatal("restored tombstone allowed new remote reservation")
				}
				if !equalJSONMaps(before, restoreRows(t, f.db.Pool, true)) {
					t.Fatal("blocked channel re-admission changed tombstone/rows")
				}
				assertChannelBytes()
			})
		}
	})
	for _, kind := range []string{"receipt", "request_admission", "import_pin_and_batch", "import_batch", "import_source_and_batch", "terminal_reason"} {
		t.Run(kind, func(t *testing.T) {
			f := newRestoredFixture(t, "empty")
			f.seed(t)
			ctx := context.Background()
			switch kind {
			case "receipt":
				parent := f.folder(t, "receipt-parent", nil)
				_, source := f.publish(t, nil, "receipt-source", []byte("owned receipt"))
				f.copyCrash(t, source, parent, false)
			case "request_admission":
				actor := int64(1)
				request := &lifecycle.RequestAdmissionInput{Scope: lifecycle.RequestScope{ActorUserID: &actor}, RequestKey: uuid.New(), RequestFingerprint: sha256.Sum256([]byte("original admitted empty upload"))}
				if _, err := f.uploads.Create(ctx, uploads.CreateInput{ID: uuid.New(), UserID: 1, Name: "admission-only", ExpectedSize: 0, Admission: request}); err != nil {
					t.Fatal(err)
				}
			case "import_pin_and_batch", "import_batch", "import_source_and_batch":
				parent := f.folder(t, "import-parent", nil)
				if kind == "import_pin_and_batch" {
					args := jobs.UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1, Destination: parent.String(), Sources: []jobs.UploadSource{{Type: "http", URL: "https://synthetic.invalid/source", DestinationPath: "missing/source"}}}
					if _, err := f.runtime.InsertUploadBatch(ctx, args); err != nil {
						t.Fatal(err)
					}
					// An isolated pin cannot exist under the approved active FK.
					_, err := f.db.Pool.Exec(ctx, "INSERT INTO teldrive.import_destination_pins(user_id,batch_id,destination_ordinal,anchor_is_root,spec_fingerprint,remainder_fingerprint,remainder_segments) VALUES(1,$1,-1,true,$2,$2,0)", uuid.New(), make([]byte, 32))
					var foreignKey *pgconn.PgError
					if !errors.As(err, &foreignKey) || foreignKey.Code != "23503" {
						t.Fatal("orphan pin was not refused by active required-batch FK")
					}
				} else {
					_, _, _ = f.batch(t, parent, kind == "import_batch")
				}
			case "terminal_reason":
				parent := f.folder(t, "terminal-parent", nil)
				if _, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1, ParentID: &parent, Name: "terminal", ExpectedSize: 0}); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Trash(ctx, 1, parent); err != nil {
					t.Fatal(err)
				}
			}
			f.cloneCandidate(t)
			f.blockedDown(t)
		})
	}
}

func uniqueRestoreJSON(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] {
				return errors.New("private JSON object rejected")
			}
			seen[name] = true
		}
		if err := uniqueRestoreJSON(d); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

// Retain failed evidence and one old source for paired CLI reuse, not every successful database.
func cleanupRestoredDatabase(t *testing.T, db *testpostgres.RestoreFixture, retain bool) {
	t.Helper()
	t.Cleanup(func() {
		db.Close()
		if retain || t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.Drop(ctx); err != nil {
			t.Error("guarded restored fixture cleanup failed; resource retained")
		}
	})
}
