package postgres

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixtureApproval(now time.Time) Approval {
	return Approval{
		Version: 1, RunID: strings.Repeat("a", 32), Namespace: "teldrive_test_b2_1234abcd_",
		ExpiresAt: now.Add(time.Hour), Host: "192.0.2.10", Port: 5432, Role: "b2_audit",
		ClusterName: "fixture-nonce", SystemIdentifier: "1234567890123456789",
		Tools: ToolContext{Executor: "docker-exec", ContainerID: strings.Repeat("b", 64),
			ImageID:   "sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015",
			NetworkID: strings.Repeat("c", 64), RunTag: strings.Repeat("a", 32),
			PSQL: "/usr/local/bin/psql", PGDump: "/usr/local/bin/pg_dump", PGRestore: "/usr/local/bin/pg_restore"},
	}
}

func TestB2IsolationGuard(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	const raw = "postgres://b2_audit:private-password@192.0.2.10:5432/postgres?sslmode=disable"
	const target = "teldrive_test_b2_1234abcd_0123456789abcdef0123456789abcdef"
	approved := fixtureApproval(now)
	identity := InstanceIdentity{Host: approved.Host, Port: approved.Port, Role: approved.Role,
		ClusterName: approved.ClusterName, SystemIdentifier: approved.SystemIdentifier, Database: "postgres"}
	for _, tc := range []struct {
		name   string
		change func(*Approval, *InstanceIdentity, *string, *string)
	}{
		{"expired", func(a *Approval, _ *InstanceIdentity, _ *string, _ *string) { a.ExpiresAt = now }},
		{"wrong_endpoint", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) {
			*u = strings.ReplaceAll(*u, "192.0.2.10", "127.0.0.1")
		}},
		{"wrong_port", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) {
			*u = strings.ReplaceAll(*u, ":5432", ":5433")
		}},
		{"wrong_role", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) {
			*u = strings.ReplaceAll(*u, "b2_audit:", "postgres:")
		}},
		{"wrong_instance", func(_ *Approval, i *InstanceIdentity, _ *string, _ *string) { i.SystemIdentifier = "42" }},
		{"wrong_nonce", func(_ *Approval, i *InstanceIdentity, _ *string, _ *string) { i.ClusterName = "other" }},
		{"wrong_server_endpoint", func(_ *Approval, i *InstanceIdentity, _ *string, _ *string) { i.Host = "127.0.0.1" }},
		{"wrong_server_role", func(_ *Approval, i *InstanceIdentity, _ *string, _ *string) { i.Role = "postgres" }},
		{"wrong_database", func(_ *Approval, i *InstanceIdentity, _ *string, _ *string) { i.Database = "production" }},
		{"namespace_only", func(a *Approval, _ *InstanceIdentity, _ *string, _ *string) { a.RunID = "" }},
		{"wrong_namespace", func(_ *Approval, _ *InstanceIdentity, _ *string, n *string) { *n = "teldrive_test_production" }},
		{"namespace_suffix", func(_ *Approval, _ *InstanceIdentity, _ *string, n *string) { *n = target + "_extra" }},
		{"wrong_tool", func(a *Approval, _ *InstanceIdentity, _ *string, _ *string) { a.Tools.PGRestore = "/tmp/pg_restore" }},
		{"wrong_image", func(a *Approval, _ *InstanceIdentity, _ *string, _ *string) { a.Tools.ImageID = "postgres:latest" }},
		{"wrong_run_tag", func(a *Approval, _ *InstanceIdentity, _ *string, _ *string) { a.Tools.RunTag = "other" }},
		{"missing_container", func(a *Approval, _ *InstanceIdentity, _ *string, _ *string) { a.Tools.ContainerID = "" }},
		{"dsn_override", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) { *u += "&host=127.0.0.1" }},
		{"dsn_fallback", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) { *u = "host=127.0.0.1 dbname=production" }},
		{"dsn_database", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) {
			*u = strings.ReplaceAll(*u, "/postgres?", "/production?")
		}},
		{"no_explicit_password", func(_ *Approval, _ *InstanceIdentity, u *string, _ *string) {
			*u = strings.ReplaceAll(*u, ":private-password", "")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, i, u, n := approved, identity, raw, target
			tc.change(&a, &i, &u, &n)
			mutated := false
			err := a.Guard(u, n, now, &i, a.Tools, func() error { mutated = true; return nil })
			if err == nil || mutated {
				t.Fatal("rejected approval reached mutation")
			}
			if strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), "fixture-nonce") {
				t.Fatal("diagnostic disclosed private evidence")
			}
		})
	}
	t.Run("exact_scope", func(t *testing.T) {
		calls := 0
		if err := approved.Guard(raw, target, now, &identity, approved.Tools, func() error { calls++; return nil }); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatal("authorized mutation was not called exactly once")
		}
		if err := approved.Guard(raw, target, now, nil, approved.Tools, func() error { t.Fatal("missing independent identity reached mutation"); return nil }); err == nil {
			t.Fatal("missing identity accepted")
		}
		tools := approved.Tools
		tools.ContainerID = strings.Repeat("d", 64)
		if err := approved.Guard(raw, target, now, &identity, tools, func() error { t.Fatal("wrong tools reached mutation"); return nil }); err == nil {
			t.Fatal("different execution context accepted")
		}
	})
	t.Run("private_manifest", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "approval.json")
		data, err := json.Marshal(approved)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := LoadApproval(path); err == nil {
			t.Fatal("missing approval accepted")
		}
		for _, invalid := range []string{"{", string(data) + "{}", strings.Replace(string(data), "\"version\":1", "\"version\":1,\"extra\":true", 1), strings.Replace(string(data), "\"version\":1", "\"version\":1,\"version\":1", 1)} {
			if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadApproval(path); err == nil {
				t.Fatal("malformed approval accepted")
			}
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadApproval(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadApproval(path); err == nil {
			t.Fatal("public approval accepted")
		}
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, "link.json")
		if err := os.Symlink(path, link); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadApproval(link); err == nil {
			t.Fatal("symlink approval accepted")
		}
		if err := os.Chmod(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadApproval(path); err == nil {
			t.Fatal("public evidence directory accepted")
		}
	})
}
