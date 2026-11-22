package postgres

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestB2RestoreGuard(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	a := fixtureApproval(now)
	const source = "teldrive_test_b2_1234abcd_11111111111111111111111111111111"
	const target = "teldrive_test_b2_1234abcd_22222222222222222222222222222222"
	const raw = "postgres://b2_audit:private-password@192.0.2.10:5432/postgres?sslmode=disable"
	identity := InstanceIdentity{Host: a.Host, Port: a.Port, Role: a.Role, ClusterName: a.ClusterName,
		SystemIdentifier: a.SystemIdentifier, Database: "postgres"}
	valid := RestoreTarget{RunID: a.RunID, SourceDatabase: source, SourceOID: 101, TargetDatabase: target}
	for _, tc := range []struct {
		name   string
		change func(*RestoreTarget, *InstanceIdentity, *ToolContext, *time.Time)
	}{
		{"same_database", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) { r.TargetDatabase = source }},
		{"wrong_run", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) {
			r.RunID = strings.Repeat("f", 32)
		}},
		{"wrong_source_namespace", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) {
			r.SourceDatabase = "production"
		}},
		{"wrong_target_namespace", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) {
			r.TargetDatabase = "production"
		}},
		{"missing_source_identity", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) { r.SourceOID = 0 }},
		{"existing_empty_target", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) { r.TargetOID = 102 }},
		{"nonempty_target", func(r *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, _ *time.Time) { r.TargetObjects = 1 }},
		{"wrong_instance", func(_ *RestoreTarget, i *InstanceIdentity, _ *ToolContext, _ *time.Time) { i.SystemIdentifier = "42" }},
		{"wrong_role", func(_ *RestoreTarget, i *InstanceIdentity, _ *ToolContext, _ *time.Time) { i.Role = "postgres" }},
		{"wrong_nonce", func(_ *RestoreTarget, i *InstanceIdentity, _ *ToolContext, _ *time.Time) { i.ClusterName = "other" }},
		{"wrong_tool", func(_ *RestoreTarget, _ *InstanceIdentity, tools *ToolContext, _ *time.Time) {
			tools.PGRestore = "/tmp/pg_restore"
		}},
		{"unobserved_container", func(_ *RestoreTarget, _ *InstanceIdentity, tools *ToolContext, _ *time.Time) { tools.ContainerID = "" }},
		{"expired", func(_ *RestoreTarget, _ *InstanceIdentity, _ *ToolContext, at *time.Time) { *at = a.ExpiresAt }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, i, tools, at := valid, identity, a.Tools, now
			tc.change(&r, &i, &tools, &at)
			called := false
			err := GuardRestore(a, raw, r, at, &i, tools, func() error { called = true; return nil })
			if err == nil || called {
				t.Fatal("rejected restore reached mutation")
			}
			if strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), a.ClusterName) {
				t.Fatal("restore rejection disclosed private authority")
			}
		})
	}
	t.Run("exact_fresh_distinct_target", func(t *testing.T) {
		called := 0
		if err := GuardRestore(a, raw, valid, now, &identity, a.Tools, func() error { called++; return nil }); err != nil || called != 1 {
			t.Fatal("exact fresh restore did not reach mutation once")
		}
	})
	t.Run("private_input", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "restore.json")
		input := RestoreInput{Version: 1, RunID: a.RunID, SourceDatabase: source,
			TargetDatabases: []string{target}, Docker: "/usr/bin/docker", DockerSHA256: strings.Repeat("a", 64),
			Directory: dir}
		data, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRestoreInput(path, a, raw, now); err != nil {
			t.Fatal("exact private restore input refused")
		}
		for _, data := range [][]byte{
			append(append([]byte{}, data[:len(data)-1]...), []byte(",\"unknown\":true}")...),
			append(append([]byte{}, data...), []byte(" {}")...),
			[]byte(`{"version":1,"version":1}`),
		} {
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadRestoreInput(path, a, raw, now); err == nil {
				t.Fatal("malformed restore input accepted")
			}
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRestoreInput(path, a, raw, now); err == nil {
			t.Fatal("shared restore input accepted")
		}
	})
	t.Run("private_dump_checksum_and_paths", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "payload")
		if err := os.WriteFile(path, []byte("abc"), 0600); err != nil {
			t.Fatal(err)
		}
		hash, size, err := hashRegular(path, true)
		if err != nil || size != 3 || hash != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
			t.Fatal("independent dump checksum/size mismatch")
		}
		if _, _, err := hashRegular(dir+"/./payload", true); err == nil {
			t.Fatal("noncanonical dump path accepted")
		}
		link := filepath.Join(dir, "alias")
		if err := os.Symlink(path, link); err != nil {
			t.Fatal(err)
		}
		if _, _, err := hashRegular(link, true); err == nil {
			t.Fatal("symlink dump accepted")
		}
		real := filepath.Join(dir, "real")
		if err := os.Mkdir(real, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(real, "payload"), []byte("abc"), 0600); err != nil {
			t.Fatal(err)
		}
		parentAlias := filepath.Join(dir, "parent-alias")
		if err := os.Symlink(real, parentAlias); err != nil {
			t.Fatal(err)
		}
		if _, _, err := hashRegular(filepath.Join(parentAlias, "payload"), true); err == nil {
			t.Fatal("symlink dump parent accepted")
		}
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := hashRegular(path, true); err == nil {
			t.Fatal("shared dump accepted")
		}
	})
}
