package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestRenderedFilesQualifyConfiguredSchema(t *testing.T) {
	t.Parallel()

	rendered, err := renderedFiles("tenant_alpha")
	if err != nil {
		t.Fatalf("render migrations: %v", err)
	}
	entries, err := fs.ReadDir(rendered, ".")
	if err != nil {
		t.Fatalf("read rendered migrations: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected rendered migrations")
	}
	for _, entry := range entries {
		data, err := fs.ReadFile(rendered, entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		text := string(data)
		if strings.Contains(text, schemaTemplateMarker) {
			t.Fatalf("migration %s still contains schema marker", entry.Name())
		}
		if !strings.Contains(text, `"tenant_alpha".`) {
			t.Fatalf("migration %s does not contain qualified schema", entry.Name())
		}
	}
}

func TestRenderedFilesRejectInvalidSchema(t *testing.T) {
	t.Parallel()
	if _, err := renderedFiles(`tenant; DROP SCHEMA public`); err == nil {
		t.Fatal("expected invalid schema to fail")
	}
}

// Source controls complement, but never replace, the real restored Up/Down suites.
func TestLifecycleMigrationKeepsLegacyCutoverNarrow(t *testing.T) {
	data, err := Files.ReadFile("000015_lifecycle_receipts.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, down, ok := strings.Cut(string(data), "-- +goose Down")
	if !ok {
		t.Fatal("lifecycle migration has no real Down")
	}
	exact := "UPDATE /* TEMPLATE: schema */upload_sessions\nSET terminal_failure_code = 'upload_destination_unknown'\nWHERE parent_id IS NULL AND state <> 'completed';"
	if strings.Count(up, exact) != 1 {
		t.Fatal("legacy cutover must only add unknown destination reason")
	}
	for _, forbidden := range []string{"DISABLE TRIGGER", "session_replication_role", "SET parent_id", "SET updated_at", "UPDATE /* TEMPLATE: schema */upload_parts"} {
		if strings.Contains(up, forbidden) {
			t.Fatalf("unsafe historical rewrite: %s", forbidden)
		}
	}
	guardEnd := strings.Index(down, "END $$;")
	firstDrop := strings.Index(down, "DROP ")
	if guardEnd < 0 || firstDrop <= guardEnd {
		t.Fatal("Down drops evidence before its transactional guard")
	}
	for _, table := range []string{"remote_write_receipts", "request_admissions", "import_batches", "import_destination_pins", "import_job_attempts", "channel_deletion_intents"} {
		if !strings.Contains(down[:guardEnd], "EXISTS (SELECT 1 FROM /* TEMPLATE: schema */"+table+")") {
			t.Fatalf("Down omits explicit evidence guard for %s", table)
		}
	}
	if !strings.Contains(down[:guardEnd], "upload_sessions WHERE terminal_failure_code IS NOT NULL") {
		t.Fatal("Down omits legacy unknown/terminal reason evidence")
	}
}
