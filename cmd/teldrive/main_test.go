package main

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
)

func TestVersionOutput(t *testing.T) {
	originalVersion, originalCommit, originalDate := version, commit, date
	originalSnapshot, originalDirty := snapshot, dirty
	t.Cleanup(func() {
		version, commit, date = originalVersion, originalCommit, originalDate
		snapshot, dirty = originalSnapshot, originalDirty
	})

	tests := []struct {
		name     string
		version  string
		commit   string
		date     string
		snapshot string
		dirty    string
	}{
		{
			name:     "injected link metadata",
			version:  "v0.1.0-enchanced",
			commit:   "abcdef123456",
			date:     "2026-01-02T03:04:05Z",
			snapshot: "2ab10aab481c2e02f4011a4b6395b8022b88a9a269364758038d87c79d694fe3",
			dirty:    "true",
		},
		{
			name:     "default values",
			version:  "dev",
			commit:   "unknown",
			date:     "unknown",
			snapshot: "unknown",
			dirty:    "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, commit, date, snapshot, dirty = tt.version, tt.commit, tt.date, tt.snapshot, tt.dirty
			shortCommit := tt.commit
			if len(shortCommit) > 7 {
				shortCommit = shortCommit[:7]
			}

			var out bytes.Buffer
			cmd := newVersionCommand()
			cmd.SetOut(&out)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("version command failed: %v", err)
			}

			want := fmt.Sprintf("teldrive %s\n- commit: %s\n- built: %s\n- snapshot: %s\n- dirty: %s\n- go/version: %s\n- os/arch: %s/%s\n",
				buildVersion(), shortCommit, tt.date, tt.snapshot, tt.dirty, runtime.Version(), runtime.GOOS, runtime.GOARCH)
			if got := out.String(); got != want {
				t.Fatalf("version output mismatch\nwant:\n%s\ngot:\n%s", want, got)
			}
		})
	}
}
