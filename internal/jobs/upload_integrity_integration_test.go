//go:build integration

package jobs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestB1HTTPRepresentation(t *testing.T) {
	for _, size := range []int{0, 513, 64*1024*1024 + 17} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			db := testpostgres.New(t)
			seedCleanupOwner(t, db.Pool)
			payload := bytes.Repeat([]byte("x"), size)
			gets := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Type", "video/mp4")
					w.Header().Set("Content-Length", "999")
					return
				}
				gets++
				if r.Method != http.MethodGet || r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" || r.Header.Get("Accept-Encoding") != "identity" {
					t.Error("not an unconditional identity GET")
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
				if size == 513 {
					w.Header().Set("ETag", `"strong"`)
				}
				if size > 513 {
					w.Header().Set("ETag", `W/"weak"`)
				}
				_, _ = w.Write(payload)
			}))
			defer server.Close()
			storage := &uploadWorkerStorage{}
			service := uploads.NewService(db.Pool)
			catalogService := catalog.NewService(db.Pool, nil)
			channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
			policy := bindUploadPolicy(t, db.Pool, storage, catalogService, service)
			pipeline := transfer.NewPipeline(service, channelService, storage, nil, transfer.Config{Receipts: policy})
			admission := newSourceAdmission(t, db.Pool, storage, catalogService, service, pipeline, policy, server.Client(), nil)
			worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, service, pipeline, server.Client(), 0, false, nil)
			name := uuid.NewString() + ".txt"
			args := jobs.UploadSourceArgs{UserID: 1001, ChunkSize: 64 * 1024 * 1024, Source: jobs.UploadFileSource{Type: "http", Location: server.URL, DestinationPath: name, Size: 999, MIMEType: "video/mp4", Headers: map[string]string{"If-Range": "stale", "Accept-Encoding": "gzip"}}}
			if err := worker.Work(context.Background(), admission.job(t, args)); err != nil {
				t.Fatal(err)
			}
			if gets != 1 {
				t.Fatalf("GET count=%d", gets)
			}
			rows, err := db.Pool.Query(context.Background(), `SELECT p.channel_id,p.message_id,p.plain_size FROM file_parts p JOIN files f ON f.id=p.file_id WHERE f.name=$1 AND f.status='active' ORDER BY p.part_no`, name)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var actual bytes.Buffer
			for rows.Next() {
				var channel, message, length int64
				if err := rows.Scan(&channel, &message, &length); err != nil {
					t.Fatal(err)
				}
				reader, err := storage.OpenRange(context.Background(), telegramstore.RangeRequest{ChannelID: channel, MessageID: message, Length: length})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(&actual, reader); err != nil {
					t.Fatal(err)
				}
				_ = reader.Close()
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual.Bytes(), payload) {
				t.Fatal("published representation differs")
			}
			var publishedSize int64
			if err := db.Pool.QueryRow(context.Background(), `SELECT size FROM files WHERE name=$1 AND status='active'`, name).Scan(&publishedSize); err != nil {
				t.Fatal(err)
			}
			if publishedSize != int64(size) {
				t.Fatalf("size=%d", publishedSize)
			}
			var publishedMIME string
			if err := db.Pool.QueryRow(context.Background(), `SELECT mime_type FROM files WHERE name=$1 AND status='active'`, name).Scan(&publishedMIME); err != nil {
				t.Fatal(err)
			}
			if size > 0 && publishedMIME != "text/plain" {
				t.Fatalf("MIME=%s, want current sniffed text/plain", publishedMIME)
			}
		})
	}
}

func TestB1HTTPResumeBytes(t *testing.T) {
	for _, scenario := range []string{"equal", "changed", "missing-proof", "corrupt-proof", "changed-url", "changed-auth"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			seedCleanupOwner(t, db.Pool)
			storage := &uploadWorkerStorage{}
			service := uploads.NewService(db.Pool)
			catalogService := catalog.NewService(db.Pool, nil)
			channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
			policy := bindUploadPolicy(t, db.Pool, storage, catalogService, service)
			pipeline := transfer.NewPipeline(service, channelService, storage, nil, transfer.Config{Receipts: policy})
			admission := newSourceAdmission(t, db.Pool, storage, catalogService, service, pipeline, policy, nil, nil)
			original := []byte("original representation")
			current := append([]byte(nil), original...)
			name := uuid.NewString() + ".txt"
			mimeType := "text/plain"
			old, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, Name: name, ExpectedSize: int64(len(original)), MIMEType: &mimeType})
			if err != nil {
				t.Fatal(err)
			}
			oldID := uuid.UUID(old.ID.Bytes)
			if _, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: oldID, PartNo: 1, PlainSize: int64(len(original)), Body: bytes.NewReader(original)}); err != nil {
				t.Fatal(err)
			}
			file, err := service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: oldID, Size: int64(len(original))})
			if err != nil {
				t.Fatal(err)
			}
			session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, Name: name, ExpectedSize: int64(len(original)), MIMEType: &mimeType, ConflictPolicy: "replace"})
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.UUID(session.ID.Bytes)
			if _, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(original)), Body: bytes.NewReader(original)}); err != nil {
				t.Fatal(err)
			}
			beforeUploads := storage.uploads
			if scenario == "changed" || scenario == "changed-url" || scenario == "changed-auth" {
				current[0] = 'X'
			}
			if scenario == "missing-proof" {
				if _, err := db.Pool.Exec(ctx, `UPDATE upload_parts SET checksum=NULL,block_hashes=NULL WHERE upload_id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "corrupt-proof" {
				if _, err := db.Pool.Exec(ctx, `UPDATE upload_parts SET checksum=repeat('0',64) WHERE upload_id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			gets := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Type", mimeType)
					w.Header().Set("Content-Length", strconv.Itoa(len(current)))
					return
				}
				gets++
				if scenario == "changed-auth" && r.Header.Get("Authorization") != "Bearer changed" {
					t.Error("current authorization missing")
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(current)))
				w.Header().Set("Content-Type", mimeType)
				w.Header().Set("ETag", `W/"new"`)
				_, _ = w.Write(current)
			}))
			defer server.Close()
			admission.http = server.Client()
			worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, service, pipeline, server.Client(), 0, false, nil)
			location := server.URL
			if scenario == "changed-url" {
				location += "/different"
			}
			err = worker.Work(ctx, admission.job(t, jobs.UploadSourceArgs{UserID: 1001, Source: jobs.UploadFileSource{Type: "http", Location: location, DestinationPath: name, Size: int64(len(original)), MIMEType: mimeType, Validator: `"old"`, Headers: map[string]string{"Authorization": "Bearer changed"}}}))
			conflict := scenario == "changed" || scenario == "changed-url" || scenario == "changed-auth"
			if (err != nil) != conflict {
				t.Fatalf("Work error=%v, conflict=%t", err, conflict)
			}
			if gets != 1 {
				t.Fatalf("GETs=%d", gets)
			}
			wantUploads := beforeUploads
			if scenario == "missing-proof" || scenario == "corrupt-proof" {
				wantUploads++
			}
			if storage.uploads != wantUploads {
				t.Fatalf("uploads=%d, want=%d", storage.uploads, wantUploads)
			}
			if conflict {
				var active uuid.UUID
				if err := db.Pool.QueryRow(ctx, `SELECT id FROM files WHERE name=$1 AND status='active'`, name).Scan(&active); err != nil {
					t.Fatal(err)
				}
				if active != uuid.UUID(file.ID.Bytes) {
					t.Fatal("failed replay replaced old file")
				}
			}
		})
	}
}

type integrityTransport func(*http.Request) (*http.Response, error)

func (f integrityTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type integrityErrorReader struct{ err error }

func (r integrityErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestB1HTTPRepresentationFailures(t *testing.T) {
	for _, scenario := range []string{"short", "extra", "read-error", "canceled", "encoding", "partial"} {
		t.Run(scenario, func(t *testing.T) {
			db := testpostgres.New(t)
			ctx := context.Background()
			seedCleanupOwner(t, db.Pool)
			storage := &uploadWorkerStorage{}
			service := uploads.NewService(db.Pool)
			catalogService := catalog.NewService(db.Pool, nil)
			channelService := channels.NewService(db.Pool, channels.TelegramCreator{Storage: storage}, channels.Config{PartLimit: 1000})
			policy := bindUploadPolicy(t, db.Pool, storage, catalogService, service)
			pipeline := transfer.NewPipeline(service, channelService, storage, nil, transfer.Config{Receipts: policy})
			admission := newSourceAdmission(t, db.Pool, storage, catalogService, service, pipeline, policy, nil, nil)
			name := uuid.NewString() + ".txt"
			session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, Name: name, ExpectedSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.UUID(session.ID.Bytes)
			if _, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: id, PartNo: 1, PlainSize: 4, Body: bytes.NewReader([]byte("safe"))}); err != nil {
				t.Fatal(err)
			}
			old, err := service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: id, Size: 4})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Pool.Exec(ctx, `INSERT INTO file_shares(file_id,owner_id,token_prefix,token_hash) VALUES($1,1001,'synthetic',$2)`, old.ID, []byte(uuid.NewString())); err != nil {
				t.Fatal(err)
			}
			snapshot := func() string {
				var value string
				if err := db.Pool.QueryRow(ctx, `SELECT jsonb_build_array(
					(SELECT to_jsonb(f) FROM files f WHERE id=$1),
					(SELECT jsonb_agg(to_jsonb(p) ORDER BY part_no) FROM file_parts p WHERE file_id=$1),
					(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM file_shares s WHERE file_id=$1),
					(SELECT count(*) FROM user_events WHERE user_id=1001 AND resource_id=$1::text))::text`, old.ID).Scan(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := snapshot()
			client := &http.Client{Transport: integrityTransport(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodHead {
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/plain"}}, ContentLength: 4, Body: io.NopCloser(bytes.NewReader(nil))}, nil
				}
				var body io.Reader = bytes.NewReader([]byte("evil"))
				status := 200
				header := http.Header{"Content-Type": {"text/plain"}}
				switch scenario {
				case "short":
					body = bytes.NewReader([]byte("e"))
				case "extra":
					body = bytes.NewReader([]byte("evil!"))
				case "read-error":
					body = io.MultiReader(body, integrityErrorReader{errors.New("synthetic read failure")})
				case "canceled":
					body = integrityErrorReader{context.Canceled}
				case "encoding":
					header.Set("Content-Encoding", "gzip")
				case "partial":
					status = 206
				}
				return &http.Response{StatusCode: status, Header: header, ContentLength: 4, Body: io.NopCloser(body)}, nil
			})}
			admission.http = client
			worker := jobs.NewUploadSourceWorker(db.Pool, catalogService, service, pipeline, client, 0, false, nil)
			if err := worker.Work(ctx, admission.job(t, jobs.UploadSourceArgs{UserID: 1001, Source: jobs.UploadFileSource{Type: "http", Location: "http://synthetic.invalid/source", DestinationPath: name}})); err == nil {
				t.Fatal("invalid response published")
			}
			var active uuid.UUID
			if err := db.Pool.QueryRow(ctx, `SELECT id FROM files WHERE name=$1 AND status='active'`, name).Scan(&active); err != nil {
				t.Fatal(err)
			}
			if active != uuid.UUID(old.ID.Bytes) {
				t.Fatal("failed source replaced old destination")
			}
			var completed int
			if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM upload_sessions WHERE name=$1 AND state='completed'`, name).Scan(&completed); err != nil {
				t.Fatal(err)
			}
			if completed != 1 {
				t.Fatal("failure completed another session")
			}
			if snapshot() != before {
				t.Fatal("failed source changed file/parts/shares/events")
			}
		})
	}
}
