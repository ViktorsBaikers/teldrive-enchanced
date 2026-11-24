//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/database"
)

const testDatabaseEnv = "TEST_DATABASE_URL"

type Database struct {
	Pool *pgxpool.Pool
	URL  string
}

// New creates only a fresh run-owned database under exact private B2 authority.
// Cleanup retains the database if approval, instance or recorded ownership changes.
func New(t testing.TB) *Database {
	t.Helper()
	baseURL := os.Getenv(testDatabaseEnv)
	approvalPath := os.Getenv("B2_ISOLATION_APPROVAL")
	approval, err := LoadApproval(approvalPath)
	if err != nil {
		t.Fatal("fresh private B2_ISOLATION_APPROVAL is required")
	}
	databaseName := approval.Namespace + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := approval.ValidateTarget(baseURL, databaseName, time.Now().UTC()); err != nil {
		t.Fatal("B2 database target is not approved")
	}
	// pgx otherwise merges PG* environment, service and password-file defaults.
	// Every credential/endpoint is explicit; inherited connection settings fail closed.
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "PG") {
			t.Fatal("inherited PG connection settings are forbidden in B2 fixtures")
		}
	}
	adminURL, err := withDatabase(baseURL, "postgres")
	if err != nil {
		t.Fatal("invalid B2 database URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(adminURL)
	if err != nil {
		t.Fatal("invalid B2 connection configuration")
	}
	config.Fallbacks = nil
	config.RuntimeParams = map[string]string{"application_name": "teldrive-b2-fixture"}
	config.ConnectTimeout = 10 * time.Second
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("B2 admin connection failed")
	}
	var pool *pgxpool.Pool
	record := ownedDatabase{Version: 1, RunID: approval.RunID, Database: databaseName,
		SystemIdentifier: approval.SystemIdentifier, Role: approval.Role}
	recordPath := filepath.Join(filepath.Dir(approvalPath), databaseName+".owned.json")
	created := false
	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		defer admin.Close(cleanupCtx)
		if !created {
			return
		}
		err := guardMutation(cleanupCtx, admin, approvalPath, approval, baseURL, databaseName, func() error {
			if err := verifyOwned(cleanupCtx, admin, recordPath, record); err != nil {
				return err
			}
			_, err := admin.Exec(cleanupCtx, "DROP DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)")
			return err
		})
		if err != nil {
			t.Errorf("B2 cleanup retained an unverified resource; inspect private owned-resource evidence")
			return
		}
		if err := os.Remove(recordPath); err != nil {
			t.Errorf("B2 cleanup could not retire private owned-resource evidence")
		}
	})
	// Write intent exclusively before CREATE, then replace it with observed DB OID.
	// An ambiguous CREATE/readback leaves intent and DB retained, never guessed owned.
	if err := writeOwned(recordPath, record, true); err != nil {
		t.Fatal("B2 owned-resource intent could not be recorded")
	}
	if err := guardMutation(ctx, admin, approvalPath, approval, baseURL, databaseName, func() error {
		created = true
		_, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize())
		return err
	}); err != nil {
		t.Fatal("B2 guarded database creation failed")
	}
	var owner string
	if err := admin.QueryRow(ctx, "SELECT oid::bigint, pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1", databaseName).Scan(&record.OID, &owner); err != nil || owner != approval.Role {
		t.Fatal("B2 created database identity could not be established")
	}
	if err := writeOwned(recordPath, record, false); err != nil {
		t.Fatal("B2 owned-resource identity could not be recorded")
	}
	targetURL, err := withDatabase(baseURL, databaseName)
	if err != nil {
		t.Fatal("invalid B2 target database URL")
	}
	if err := guardMutation(ctx, admin, approvalPath, approval, targetURL, databaseName, func() error {
		if err := verifyOwned(ctx, admin, recordPath, record); err != nil {
			return err
		}
		if err := database.Migrate(ctx, database.Config{URL: targetURL}); err != nil {
			return err
		}
		_, err := admin.Exec(ctx, "ALTER DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" SET search_path TO "+pgx.Identifier{database.DefaultSchema}.Sanitize()+", public")
		return err
	}); err != nil {
		t.Fatal("B2 guarded database migration failed")
	}
	pool, err = database.Open(ctx, database.Config{
		URL: targetURL, ApplicationName: "teldrive-integration-test",
		MaxConnections: 8, ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal("B2 test database connection failed")
	}
	return &Database{Pool: pool, URL: targetURL}
}

type ownedDatabase struct {
	Version          int    `json:"version"`
	RunID            string `json:"run_id"`
	Database         string `json:"database"`
	SystemIdentifier string `json:"system_identifier"`
	Role             string `json:"role"`
	OID              int64  `json:"oid"`
}

func guardMutation(ctx context.Context, conn *pgx.Conn, path string, original *Approval, rawURL, name string, mutate func() error) error {
	current, err := LoadApproval(path)
	if err != nil || current.digest != original.digest {
		return errIsolation
	}
	if err := current.ValidateTarget(rawURL, name, time.Now().UTC()); err != nil {
		return err
	}
	var identity InstanceIdentity
	err = conn.QueryRow(ctx, `SELECT host(inet_server_addr()), inet_server_port(), current_user,
current_setting('cluster_name'), system_identifier::text, current_database()
FROM pg_control_system()`).Scan(&identity.Host, &identity.Port, &identity.Role,
		&identity.ClusterName, &identity.SystemIdentifier, &identity.Database)
	if err != nil {
		return errIsolation
	}
	return current.Guard(rawURL, name, time.Now().UTC(), &identity, current.Tools, mutate)
}

func writeOwned(path string, record ownedDatabase, exclusive bool) error {
	flags := os.O_WRONLY
	if exclusive {
		flags |= os.O_CREATE | os.O_EXCL
	} else {
		if _, err := readPrivate(path); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return errIsolation
	}
	defer file.Close()
	info, err := file.Stat()
	named, lerr := os.Lstat(path)
	if err != nil || lerr != nil || !privateInfo(info, false) || !os.SameFile(info, named) {
		return errIsolation
	}
	if err := file.Truncate(0); err != nil {
		return errIsolation
	}
	if err := json.NewEncoder(file).Encode(record); err != nil {
		return errIsolation
	}
	if err := file.Sync(); err != nil {
		return errIsolation
	}
	return nil
}

func verifyOwned(ctx context.Context, conn *pgx.Conn, path string, expected ownedDatabase) error {
	data, err := readPrivate(path)
	if err != nil {
		return err
	}
	var recorded ownedDatabase
	if err := json.Unmarshal(data, &recorded); err != nil || recorded != expected || recorded.OID <= 0 {
		return errIsolation
	}
	var oid int64
	var owner string
	err = conn.QueryRow(ctx, "SELECT oid::bigint, pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1", recorded.Database).Scan(&oid, &owner)
	if err != nil || oid != recorded.OID || owner != recorded.Role {
		return errIsolation
	}
	return nil
}

func withDatabase(rawURL, databaseName string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", errors.New("invalid database URL")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return "", errors.New("unsupported database URL scheme")
	}
	u.Path = "/" + databaseName
	return u.String(), nil
}
