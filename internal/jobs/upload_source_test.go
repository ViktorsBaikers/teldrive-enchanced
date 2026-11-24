package jobs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tgdrive/teldrive/v2/internal/treehash"
)

func TestParseUploadSize(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]int64{
		"0": 0, "1024": 1024, "10MiB": 10 * 1024 * 1024, "1.5GiB": 3 * 1024 * 1024 * 1024 / 2,
	} {
		got, err := parseUploadSize(input)
		if err != nil {
			t.Fatalf("parseUploadSize(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("parseUploadSize(%q) = %d, want %d", input, got, want)
		}
	}
}

type uploadRoundTripper func(*http.Request) (*http.Response, error)

func (f uploadRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestB1HTTPSourcePolicy(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		encoding string
		size     int64
		reject   bool
	}{
		{"full", 200, "", 4, false}, {"identity", 200, "identity", 4, false},
		{"partial", 206, "", 4, true}, {"gzip", 200, "gzip", 4, true},
		{"unknown-size", 200, "", -1, true}, {"not-modified", 304, "", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: uploadRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("If-Range") != "" || r.Header.Get("Range") != "" || r.Header.Get("If-None-Match") != "" || r.Header.Get("Accept-Encoding") != "identity" {
					t.Fatal("unsafe representation headers")
				}
				return &http.Response{StatusCode: test.status, ContentLength: test.size, Header: http.Header{"Content-Encoding": []string{test.encoding}}, Body: io.NopCloser(strings.NewReader("body"))}, nil
			})}
			worker := NewUploadSourceWorker(nil, nil, nil, nil, client, 0, false, nil)
			source := UploadFileSource{Type: "http", Location: "http://synthetic.invalid/source", Size: 99, Headers: map[string]string{"If-Range": "old", "Range": "bytes=1-2", "If-None-Match": "old", "Accept-Encoding": "gzip"}}
			reader, err := worker.openSource(context.Background(), &source)
			if (err != nil) != test.reject {
				t.Fatalf("openSource=%v", err)
			}
			if reader != nil {
				_ = reader.Close()
			}
			if err == nil && source.Size != test.size {
				t.Fatal("queued size retained")
			}
		})
	}
	client := NewUploadHTTPClient()
	from, _ := url.Parse("https://first.invalid/source")
	to, _ := url.Parse("https://second.invalid/source")
	request := &http.Request{URL: to, Header: http.Header{"Authorization": {"secret"}, "Cookie": {"secret"}, "Proxy-Authorization": {"secret"}}}
	if err := client.CheckRedirect(request, []*http.Request{{URL: from}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Authorization", "Cookie", "Proxy-Authorization"} {
		if request.Header.Get(name) != "" {
			t.Fatal("credential leaked across redirect")
		}
	}
	to.Scheme = "file"
	if client.CheckRedirect(request, []*http.Request{{URL: from}}) == nil {
		t.Fatal("unsafe scheme accepted")
	}
	for _, address := range []string{"127.0.0.1", "::1", "10.1.1.1", "169.254.169.254", "::ffff:127.0.0.1"} {
		if safeUploadAddress(netip.MustParseAddr(address)) {
			t.Fatalf("unsafe address accepted: %s", address)
		}
	}
}

func TestNormalizeUploadChunkSizeMatchesRclone(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		input int64
		want  int64
	}{
		{name: "default", want: 512 * 1024 * 1024},
		{name: "aligned", input: 64 * 1024 * 1024, want: 64 * 1024 * 1024},
		{name: "round down", input: 71 * 1024 * 1024, want: 64 * 1024 * 1024},
		{name: "round up", input: 73 * 1024 * 1024, want: 80 * 1024 * 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeUploadChunkSize(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("normalizeUploadChunkSize(%d) = %d, want %d", test.input, got, test.want)
			}
		})
	}
	for _, input := range []int64{63 * 1024 * 1024, 2001 * 1024 * 1024} {
		if _, err := normalizeUploadChunkSize(input); err == nil {
			t.Fatalf("normalizeUploadChunkSize(%d) succeeded", input)
		}
	}
}

func TestExpandLocalSourceRecursesAndFilters(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"keep.bin": "1234567890", "small.bin": "x", "nested/skip.tmp": "1234567890"} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	filter, err := newUploadFilter([]string{"*.tmp"}, "10", "")
	if err != nil {
		t.Fatal(err)
	}
	files, err := expandLocalSource(UploadSource{Type: "local", Path: root, DestinationPath: "backup"}, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].DestinationPath != "backup/keep.bin" || files[0].Size != 10 {
		t.Fatalf("expanded files = %#v", files)
	}
}

func TestInspectHTTPSourceMergesHeadersAndMetadata(t *testing.T) {
	modified := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer source" || request.Header.Get("Referer") != "https://batch.example/" {
			t.Errorf("headers = %#v", request.Header)
		}
		response.Header().Set("Content-Length", "42")
		response.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
		response.Header().Set("Content-Disposition", `inline; filename="remote.bin"`)
	}))
	defer server.Close()
	file, err := inspectHTTPSource(context.Background(), server.Client(), UploadSource{
		Type: "http", URL: server.URL, Headers: map[string]string{"Authorization": "Bearer source"},
	}, map[string]string{"Authorization": "Bearer batch", "Referer": "https://batch.example/"})
	if err != nil {
		t.Fatal(err)
	}
	if file.DestinationPath != "remote.bin" || file.Size != 42 || !file.HasModTime || !file.ModTime.Equal(modified) {
		t.Fatalf("HTTP source = %#v", file)
	}
}

func TestUploadFilterUsesRcloneStyleExclusionsAndInclusiveSizes(t *testing.T) {
	t.Parallel()
	filter, err := newUploadFilter([]string{"*.tmp", ".git/**", "**/node_modules/**"}, "10", "20")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path string
		size int64
		want string
	}{
		{path: "keep.bin", size: 10},
		{path: "keep.bin", size: 20},
		{path: "scratch.tmp", size: 15, want: skipExcluded},
		{path: ".git/objects/a", size: 15, want: skipExcluded},
		{path: "app/node_modules/pkg/index.js", size: 15, want: skipExcluded},
		{path: "node_modules/pkg/index.js", size: 15, want: skipExcluded},
		{path: "small.bin", size: 9, want: skipBelowMinSize},
		{path: "large.bin", size: 21, want: skipAboveMaxSize},
	} {
		if got := filter.skipReason(test.path, test.size); got != test.want {
			t.Errorf("skipReason(%q, %d) = %q, want %q", test.path, test.size, got, test.want)
		}
	}
}

func TestB1LocalImportDeferredRootBoundary(t *testing.T) {
	for _, replacement := range []string{"none", "root-alias", "leaf", "ancestor", "disabled", "outside"} {
		t.Run(replacement, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, "nested")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			location := filepath.Join(directory, "source.txt")
			if err := os.WriteFile(location, []byte("allowed"), 0o600); err != nil {
				t.Fatal(err)
			}
			roots := []string{root}
			batch := NewUploadBatchWorker(nil, nil, nil, nil, roots)
			files, err := batch.expand(context.Background(), UploadSource{Type: "local", Path: location}, nil, uploadFilter{})
			if err != nil || len(files) != 1 {
				t.Fatalf("expand() = %v, %v", files, err)
			}
			outside := t.TempDir()
			external := filepath.Join(outside, "source.txt")
			if err := os.WriteFile(external, []byte("private external bytes"), 0o600); err != nil {
				t.Fatal(err)
			}
			switch replacement {
			case "leaf":
				if err := os.Remove(location); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, location); err != nil {
					t.Fatal(err)
				}
			case "ancestor":
				if err := os.Rename(directory, directory+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, directory); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				roots = nil
			case "outside":
				files[0].Location = external
			case "root-alias":
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(root, alias); err != nil {
					t.Fatal(err)
				}
				roots = []string{alias}
			}
			worker := NewUploadSourceWorker(nil, nil, nil, nil, nil, 0, false, roots)
			reader, err := worker.openSource(context.Background(), &files[0])
			if reader != nil {
				defer reader.Close()
			}
			if replacement != "none" && replacement != "root-alias" {
				if err == nil || reader != nil {
					t.Fatal("deferred source escaped current root policy")
				}
				hasher := treehash.NewBlockHasher()
				_, _ = hasher.Write([]byte("private external bytes"))
				expected := treehash.SumToHex(treehash.ComputeTreeHash(hasher.Sum()))
				files[0].Size = int64(len("private external bytes"))
				if worker.localSourceMatches(context.Background(), files[0], expected) {
					t.Fatal("skip verification read outside current root policy")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			payload, err := io.ReadAll(reader)
			if err != nil || string(payload) != "allowed" {
				t.Fatalf("allowed source = %q, %v", payload, err)
			}
		})
	}
}

// TestLocalSourceWalkErrorHidesPath proves a WalkDir failure cannot leak the
// local path either; the earlier test only covered the Lstat/EvalSymlinks wraps.
func TestLocalSourceWalkErrorHidesPath(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	root := t.TempDir()
	planted := filepath.Join(root, "planted-private-directory")
	if err := os.MkdirAll(planted, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(planted, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(planted, 0o700) })

	_, err := expandLocalSource(UploadSource{Type: "local", Path: root}, uploadFilter{})
	if err == nil {
		t.Fatal("want a walk error for an unreadable directory")
	}
	if strings.Contains(err.Error(), "planted-private-directory") {
		t.Fatalf("planted path leaked: %v", err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("error = %T, want wrapped *os.PathError", err)
	}
	if pathErr.Path != "<redacted>" {
		t.Fatalf("PathError.Path = %q, want <redacted>", pathErr.Path)
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("underlying cause lost: %v", err)
	}
}

func TestLocalSourceErrorHidesPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	planted := filepath.Join(root, "planted-private-directory")
	source := filepath.Join(planted, "secret-movie.mkv")

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "validate", err: validateLocalImportSource(source, []string{root})},
		{name: "expand", err: func() error {
			_, err := expandLocalSource(UploadSource{Type: "local", Path: source}, uploadFilter{})
			return err
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.err == nil {
				t.Fatal("want an error for a missing source")
			}
			for _, literal := range []string{"planted-private-directory", "secret-movie.mkv"} {
				if strings.Contains(test.err.Error(), literal) {
					t.Fatalf("planted path %q leaked: %v", literal, test.err)
				}
			}
			var pathErr *os.PathError
			if !errors.As(test.err, &pathErr) {
				t.Fatalf("error = %T, want wrapped *os.PathError", test.err)
			}
			if pathErr.Path != "<redacted>" {
				t.Fatalf("PathError.Path = %q, want <redacted>", pathErr.Path)
			}
			if !errors.Is(test.err, os.ErrNotExist) {
				t.Fatalf("underlying cause lost: %v", test.err)
			}
		})
	}
}
