//go:build integration && linux

package postgres

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sys/unix"
)

const (
	b3QualificationPackage = "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	b3QualificationSelector = "^TestB3FencedTailQualification$"
	b3QualificationEnv = "B3_QUALIFICATION_APPROVAL"
	b3CustodianEnv = "B3_QUALIFICATION_CUSTODIAN"
	b3MaxControlReply = 2 << 20
)

var errB3Qualification = errors.New("B3 qualification custody or live scope rejected; retain fenced resources and inspect private evidence")

var b3QualificationArgs = []string{"-test.v=test2json", "-test.run", b3QualificationSelector, "-test.count=1", "-test.parallel=1", "-test.timeout=110s"}

const (
	b3RestoreRollback = "restore_rollback"
	b3RestoreTail = "restore_tail"
	b3MissingFence = "missing_prefence"
	b3MissingTail = "missing_tail"
	b3ExpiredQuery = "expired_query"
	b3ExpiredParse = "expired_parse"
	b3ExpiredBind = "expired_bind"
	b3ExpiredExecute = "expired_execute"
	b3ExpiredDeferred = "expired_deferred"
	b3ExistingWrites = "existing_sequence_writes"
)

var b3Cases = [...]string{b3RestoreRollback, b3RestoreTail, b3MissingFence, b3MissingTail, b3ExpiredQuery,
	b3ExpiredParse, b3ExpiredBind, b3ExpiredExecute, b3ExpiredDeferred, b3ExistingWrites}

// The runner-private pointer is not writer authority. The independently owned
// dossier and supervisor both bind the exact sealed test invocation.
type b3ApprovalPointer struct {
	Version int `json:"version"`
	Dossier b3FilePin `json:"dossier"`
}

type b3NodePin struct {
	Path string `json:"path"`
	Device uint64 `json:"device"`
	Inode uint64 `json:"inode"`
	UID uint32 `json:"uid"`
	GID uint32 `json:"gid"`
	Mode uint32 `json:"mode"`
}

type b3FilePin struct {
	b3NodePin
	Bytes int64 `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type b3CaseScope struct {
	Database string `json:"database"`
	DatabaseOID uint32 `json:"database_oid"`
	OwnerOID uint32 `json:"owner_oid"`
	Writer string `json:"writer"`
	WriterOID uint32 `json:"writer_oid"`
	Observer string `json:"observer"`
	ObserverOID uint32 `json:"observer_oid"`
	PhysicalPreFence bool `json:"physical_pre_fence"`
	TailAuthority bool `json:"tail_authority"`
	GrantPreFence bool `json:"grant_pre_fence"`
}

type b3CustodyDossier struct {
	Version int `json:"version"`
	Package string `json:"package"`
	PackageDir b3NodePin `json:"package_dir"`
	Nonce string `json:"nonce"`
	Candidate string `json:"candidate"`
	SystemIdentifier string `json:"system_identifier"`
	Cluster string `json:"cluster"`
	Port uint16 `json:"port"`
	NotAfter time.Time `json:"not_after"`
	NotAfterNS uint64 `json:"not_after_ns,string"`
	RunnerUID uint32 `json:"runner_uid"`
	RunnerGID uint32 `json:"runner_gid"`
	SupervisorUID uint32 `json:"supervisor_uid"`
	SupervisorPID uint32 `json:"supervisor_pid"`
	PostmasterUID uint32 `json:"postmaster_uid"`
	PostmasterPID uint32 `json:"postmaster_pid"`
	PostmasterStart uint64 `json:"postmaster_start_ticks"`
	b3NamespacePins
	Control b3NodePin `json:"control"`
	SocketDir b3NodePin `json:"socket_dir"`
	Socket b3NodePin `json:"socket"`
	DataDir b3NodePin `json:"data_dir"`
	GrantsDir b3NodePin `json:"grants_dir"`
	ReceiptsDir b3NodePin `json:"receipts_dir"`
	Files map[string]b3FilePin `json:"files"`
	Cases map[string]b3CaseScope `json:"cases"`
}

type b3NamespacePins struct {
	NetNSDevice uint64 `json:"netns_device"`
	NetNSInode uint64 `json:"netns_inode"`
	HostNetNSInode uint64 `json:"host_netns_inode"`
	PIDNSDevice uint64 `json:"pidns_device"`
	PIDNSInode uint64 `json:"pidns_inode"`
	HostPIDNSInode uint64 `json:"host_pidns_inode"`
	UserNSDevice uint64 `json:"userns_device"`
	UserNSInode uint64 `json:"userns_inode"`
	HostUserNSInode uint64 `json:"host_userns_inode"`
	TimeNSDevice uint64 `json:"timens_device"`
	TimeNSInode uint64 `json:"timens_inode"`
}

var b3CustodianArgs = []string{"-test.run", b3QualificationSelector, "-test.count=1", "-test.parallel=1", "-test.timeout=0"}

const b3ArchiveRestrictKey = "TelDriveB3QualificationPostgreSQL18"

var b3FileNames = [...]string{"guard_source", "server_patch", "qualification_source", "qualification_support_source",
	"postgres_source_manifest", "postgres_build_receipt", "go_toolchain", "go_package_manifest", "go_input_manifest", "postgres", "guard",
	"pg_restore", "pg_dump", "archive", "archive_toc", "archive_sql", "archive_provenance", "source_approval", "source_evidence",
	"test", "test2json", "custody_evidence", "server_config", "hba_config", "ident_config"}

func b3SealRequired(name string) bool {
	switch name {
	case "test", "test2json", "postgres", "guard", "pg_dump", "pg_restore", "archive", "archive_toc", "archive_sql",
		"archive_provenance", "source_approval", "source_evidence", "custody_evidence", "go_package_manifest",
		"server_config", "hba_config", "ident_config":
		return true
	}
	return false
}

type b3Qualification struct {
	b3CustodyDossier
	pointerPath string
	pointerHash [32]byte
	dossier b3FilePin
	files map[string]os.FileInfo
}

func b3XIDText(xid uint64) string { return strconv.FormatUint(xid, 10) }

func b3Decode(r io.Reader, limit int64, out any) error {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(data)) > limit || uniqueJSON(json.NewDecoder(bytes.NewReader(data))) != nil {
		return errB3Qualification
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return errB3Qualification
	}
	return nil
}

func (p b3NodePin) matches(info os.FileInfo, kind os.FileMode) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && p.Device != 0 && p.Inode != 0 && st.Dev == p.Device && st.Ino == p.Inode &&
		st.Uid == p.UID && st.Gid == p.GID && info.Mode().Type() == kind && uint32(info.Mode().Perm()) == p.Mode
}

func (p b3NodePin) stat(kind os.FileMode) (os.FileInfo, error) {
	if !filepath.IsAbs(p.Path) || filepath.Clean(p.Path) != p.Path {
		return nil, errB3Qualification
	}
	path, err := filepath.EvalSymlinks(p.Path)
	if err != nil || path != p.Path {
		return nil, errB3Qualification
	}
	info, err := os.Lstat(p.Path)
	if err != nil || !p.matches(info, kind) {
		return nil, errB3Qualification
	}
	return info, nil
}

func b3CustodiedPath(path string, owner uint32) bool {
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() {
			return false
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (st.Uid != 0 && st.Uid != owner) ||
			(info.Mode().Perm()&0o022 != 0 && (st.Uid != 0 || info.Mode()&os.ModeSticky == 0)) {
			return false
		}
		if dir == filepath.Dir(dir) {
			return true
		}
	}
}

func (p b3FilePin) open() (*os.File, os.FileInfo, error) {
	info, err := p.stat(0)
	if err != nil || !idPattern.MatchString(p.SHA256) || p.Bytes <= 0 || p.Bytes > 1<<30 || info.Size() != p.Bytes {
		return nil, nil, errB3Qualification
	}
	f, err := os.OpenFile(p.Path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, errB3Qualification
	}
	opened, err := f.Stat()
	if err == nil && os.SameFile(opened, info) && p.matches(opened, 0) {
		h := sha256.New()
		n, readErr := io.Copy(h, io.LimitReader(f, p.Bytes+1))
		after, statErr := f.Stat()
		if readErr == nil && statErr == nil && n == p.Bytes && after.Size() == p.Bytes && after.ModTime().Equal(info.ModTime()) &&
			hex.EncodeToString(h.Sum(nil)) == p.SHA256 {
			if _, err := f.Seek(0, io.SeekStart); err == nil {
				return f, info, nil
			}
		}
	}
	f.Close()
	return nil, nil, errB3Qualification
}

func b3NoInheritedSettings() bool {
	for _, e := range os.Environ() {
		key, _, _ := strings.Cut(e, "=")
		if strings.HasPrefix(key, "PG") || strings.HasPrefix(key, "TELDRIVE_") || strings.HasPrefix(key, "RCLONE_") ||
			strings.HasPrefix(key, "LD_") || strings.HasPrefix(key, "DYLD_") || key == "TEST_DATABASE_URL" || key == "B2_ISOLATION_APPROVAL" {
			return false
		}
	}
	return true
}

func b3Name(s string) bool {
	return len(s) > 0 && len(s) <= 63 && strings.IndexFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_')
	}) == -1
}

func b3LoadQualification(path string) (*b3Qualification, error) {
	var ptr b3ApprovalPointer
	digest, err := decodePrivate(path, &ptr)
	if err != nil || ptr.Version != 1 || ptr.Dossier.Mode&0o222 != 0 || ptr.Dossier.UID == uint32(os.Geteuid()) ||
		!b3CustodiedPath(ptr.Dossier.Path, ptr.Dossier.UID) || !b3NoInheritedSettings() {
		return nil, errB3Qualification
	}
	f, _, err := ptr.Dossier.open()
	if err != nil {
		return nil, errB3Qualification
	}
	defer f.Close()
	q := &b3Qualification{pointerPath: path, pointerHash: digest, dossier: ptr.Dossier}
	if b3Decode(f, 128<<10, &q.b3CustodyDossier) != nil || !q.validDossier() || q.RunnerUID != uint32(os.Geteuid()) ||
		q.RunnerUID != uint32(os.Getuid()) || q.pinFiles() != nil || q.verifyBuildAndFence() != nil || q.current() != nil || !q.launchedCorrectly() {
		return nil, errB3Qualification
	}
	return q, nil
}

func (q *b3Qualification) pinFiles() error {
	q.files = make(map[string]os.FileInfo, len(b3FileNames))
	paths := make(map[string]bool, len(b3FileNames))
	for _, name := range b3FileNames {
		pin := q.Files[name]
		if paths[pin.Path] || (b3SealRequired(name) && (pin.UID != q.SupervisorUID || pin.Mode&0o222 != 0 ||
			!b3CustodiedPath(pin.Path, q.SupervisorUID))) {
			return errB3Qualification
		}
		f, info, err := pin.open()
		if err != nil {
			return errB3Qualification
		}
		f.Close()
		q.files[name], paths[pin.Path] = info, true
	}
	return nil
}

func (q *b3Qualification) validDossier() bool {
	if q.Version != 1 || q.Package != b3QualificationPackage || !runPattern.MatchString(q.Nonce) ||
		!idPattern.MatchString(q.Candidate) || !systemPattern.MatchString(q.SystemIdentifier) ||
		!b3Name(q.Cluster) || q.Port == 0 || q.NotAfter.Location() != time.UTC || !time.Now().Before(q.NotAfter) ||
		q.RunnerUID == 0 || q.RunnerGID == 0 || q.SupervisorUID != 0 || q.PostmasterUID == 0 ||
		q.RunnerUID == q.PostmasterUID || q.dossier.UID != q.SupervisorUID || q.SupervisorPID <= 1 ||
		q.PostmasterPID <= 1 || q.SupervisorPID == q.PostmasterPID || q.PostmasterStart == 0 || q.NotAfterNS == 0 ||
		q.NetNSDevice == 0 || q.NetNSInode == 0 || q.HostNetNSInode == 0 || q.NetNSInode == q.HostNetNSInode ||
		q.PIDNSDevice == 0 || q.PIDNSInode == 0 || q.HostPIDNSInode == 0 || q.PIDNSInode == q.HostPIDNSInode ||
		q.UserNSDevice == 0 || q.UserNSInode == 0 || q.HostUserNSInode == 0 || q.UserNSInode == q.HostUserNSInode ||
		q.TimeNSDevice == 0 || q.TimeNSInode == 0 || len(q.Cases) != len(b3Cases) || len(q.Files) != len(b3FileNames) {
		return false
	}
	if q.Files["qualification_source"].Path != filepath.Join(q.PackageDir.Path, "b3_authority_integration_test.go") ||
		q.Files["qualification_support_source"].Path != filepath.Join(q.PackageDir.Path, "b3_guard.go") ||
		q.Socket.Path != filepath.Join(q.SocketDir.Path, ".s.PGSQL."+strconv.Itoa(int(q.Port))) ||
		q.Control.UID != q.SupervisorUID || q.Control.GID != q.RunnerGID || q.Control.Mode != 0o660 ||
		q.SocketDir.UID != q.SupervisorUID || q.Socket.UID != q.PostmasterUID ||
		q.DataDir.UID != q.PostmasterUID || q.ReceiptsDir.UID != q.PostmasterUID || q.GrantsDir.UID != q.SupervisorUID ||
		q.SocketDir.Mode != 0o770 || q.Socket.Mode&0o007 != 0 || q.DataDir.Mode != 0o700 ||
		q.GrantsDir.Mode != 0o750 || q.ReceiptsDir.Mode != 0o750 ||
		!b3CustodiedPath(q.Control.Path, q.SupervisorUID) || !b3CustodiedPath(q.SocketDir.Path, q.SupervisorUID) ||
		!b3CustodiedPath(q.GrantsDir.Path, q.SupervisorUID) || !b3CustodiedPath(q.ReceiptsDir.Path, q.SupervisorUID) ||
		strings.ContainsAny(q.SocketDir.Path, ",\x00\r\n\t") {
		return false
	}
	dbNames, dbOIDs := make(map[string]bool, len(b3Cases)), make(map[uint32]bool, len(b3Cases))
	for _, name := range b3Cases {
		c := q.Cases[name]
		if !strings.HasPrefix(c.Database, "b3_qual_"+q.Nonce[:8]+"_") || !b3Name(c.Database) ||
			!strings.HasPrefix(c.Writer, "b3_qual_") || !b3Name(c.Writer) ||
			!strings.HasPrefix(c.Observer, "b3_qual_") || !b3Name(c.Observer) || c.Writer == c.Observer ||
			c.DatabaseOID == 0 || c.OwnerOID == 0 || c.OwnerOID == c.ObserverOID || c.WriterOID == 0 ||
			c.ObserverOID == 0 || c.WriterOID == c.ObserverOID ||
			dbNames[c.Database] || dbOIDs[c.DatabaseOID] || !c.PhysicalPreFence ||
			c.GrantPreFence != (name != b3MissingFence) || c.TailAuthority != (name != b3MissingTail) {
			return false
		}
		dbNames[c.Database], dbOIDs[c.DatabaseOID] = true, true
	}
	return true
}

func (q *b3Qualification) current() error {
	if !time.Now().Before(q.NotAfter) || os.Getenv(b3QualificationEnv) != q.pointerPath || !b3NoInheritedSettings() {
		return errB3Qualification
	}
	var ptr b3ApprovalPointer
	if hash, err := decodePrivate(q.pointerPath, &ptr); err != nil || hash != q.pointerHash || ptr.Version != 1 || ptr.Dossier != q.dossier {
		return errB3Qualification
	}
	f, _, err := q.dossier.open()
	if err != nil {
		return errB3Qualification
	}
	f.Close()
	if unix.Faccessat(unix.AT_FDCWD, q.SocketDir.Path, unix.W_OK, unix.AT_EACCESS) == nil ||
		unix.Faccessat(unix.AT_FDCWD, q.GrantsDir.Path, unix.W_OK, unix.AT_EACCESS) == nil ||
		unix.Faccessat(unix.AT_FDCWD, q.ReceiptsDir.Path, unix.W_OK, unix.AT_EACCESS) == nil {
		return errB3Qualification
	}
	return q.resourcePins()
}

func (q *b3Qualification) resourcePins() error {
	now, err := b3MonotonicNS()
	if err != nil || now >= q.NotAfterNS || q.NotAfterNS-now > uint64(180*time.Second) || !time.Now().Before(q.NotAfter) {
		return errB3Qualification
	}
	for _, entry := range []struct{ pin b3NodePin; kind os.FileMode }{
		{q.PackageDir, os.ModeDir}, {q.Control, os.ModeSocket}, {q.SocketDir, os.ModeDir}, {q.Socket, os.ModeSocket},
		{q.DataDir, os.ModeDir}, {q.ReceiptsDir, os.ModeDir}, {q.GrantsDir, os.ModeDir},
	} {
		if _, err := entry.pin.stat(entry.kind); err != nil {
			return errB3Qualification
		}
	}
	for name, before := range q.files {
		p := q.Files[name]
		info, err := p.stat(0)
		if err != nil || !b3SameFileState(info, before) ||
			(b3SealRequired(name) && !b3CustodiedPath(p.Path, q.SupervisorUID)) {
			return errB3Qualification
		}
	}
	if !q.sameNamespaces("/proc/self") || !b3NoTCPListener() ||
		(os.Geteuid() == 0 && (!q.sameNamespaces("/proc/1") || !b3MappedRoot())) {
		return errB3Qualification
	}
	return nil
}

func b3SameFileState(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) &&
		a.Sys().(*syscall.Stat_t).Ctim == b.Sys().(*syscall.Stat_t).Ctim
}

type b3BuildReceipt struct {
	Version int `json:"version"`
	Package string `json:"package"`
	PhysicalPackageDir string `json:"physical_package_dir"`
	GoVersion string `json:"go_version"`
	ToolchainSHA256 string `json:"go_toolchain_sha256"`
	TestSHA256 string `json:"test_sha256"`
	Test2jsonSHA256 string `json:"test2json_sha256"`
	GoFiles []string `json:"go_files"`
	InputManifestSHA256 string `json:"input_manifest_sha256"`
}

type b3FenceReceipt struct {
	Version int `json:"version"`
	Nonce string `json:"nonce"`
	Candidate string `json:"candidate"`
	SystemIdentifier string `json:"system_identifier"`
	ObservedAt uint64 `json:"observed_at_ns,string"`
	NetNSInode uint64 `json:"netns_inode"`
	PIDNSInode uint64 `json:"pidns_inode"`
	UserNSInode uint64 `json:"userns_inode"`
	SocketDir b3NodePin `json:"socket_dir"`
	DataDir b3NodePin `json:"data_dir"`
	PostmasterPID uint32 `json:"postmaster_pid"`
	PostmasterStart uint64 `json:"postmaster_start_ticks"`
	PreFencedTargets map[string]b3CaseScope `json:"pre_fenced_targets"`
	NoConsumerOrUnapprovedWriter bool `json:"no_consumer_or_unapproved_writer"`
	NoTargetReleaseOrCleanup bool `json:"no_target_release_or_cleanup"`
	ServerConfigSHA256 string `json:"server_config_sha256"`
	HBAConfigSHA256 string `json:"hba_config_sha256"`
	IdentConfigSHA256 string `json:"ident_config_sha256"`
}

func (q *b3Qualification) verifyBuildAndFence() error {
	var build b3BuildReceipt
	if q.decodePinned("go_package_manifest", &build) != nil || build.Version != 1 || build.Package != b3QualificationPackage ||
		build.PhysicalPackageDir != q.PackageDir.Path || build.GoVersion != runtime.Version() ||
		build.ToolchainSHA256 != q.Files["go_toolchain"].SHA256 || build.TestSHA256 != q.Files["test"].SHA256 ||
		build.Test2jsonSHA256 != q.Files["test2json"].SHA256 || build.InputManifestSHA256 != q.Files["go_input_manifest"].SHA256 ||
		!slices.Contains(build.GoFiles, "b3_guard.go") || !slices.Contains(build.GoFiles, "b3_authority_integration_test.go") {
		return errB3Qualification
	}
	var fence b3FenceReceipt
	now, err := b3MonotonicNS()
	if err != nil || q.decodePinned("custody_evidence", &fence) != nil || fence.Version != 1 ||
		fence.Nonce != q.Nonce || fence.Candidate != q.Candidate || fence.SystemIdentifier != q.SystemIdentifier ||
		fence.ObservedAt == 0 || fence.ObservedAt > now || now-fence.ObservedAt >= uint64(180*time.Second) ||
		fence.NetNSInode != q.NetNSInode || fence.PIDNSInode != q.PIDNSInode || fence.UserNSInode != q.UserNSInode ||
		fence.SocketDir != q.SocketDir || fence.DataDir != q.DataDir ||
		fence.PostmasterPID != q.PostmasterPID || fence.PostmasterStart != q.PostmasterStart ||
		fence.ServerConfigSHA256 != q.Files["server_config"].SHA256 || fence.HBAConfigSHA256 != q.Files["hba_config"].SHA256 ||
		fence.IdentConfigSHA256 != q.Files["ident_config"].SHA256 || !fence.NoConsumerOrUnapprovedWriter ||
		!fence.NoTargetReleaseOrCleanup || len(fence.PreFencedTargets) != len(q.Cases) {
		return errB3Qualification
	}
	for name, exact := range q.Cases {
		if fence.PreFencedTargets[name] != exact {
			return errB3Qualification
		}
	}
	return nil
}

func (q *b3Qualification) decodePinned(name string, out any) error {
	f, _, err := q.Files[name].open()
	if err != nil {
		return err
	}
	defer f.Close()
	return b3Decode(f, 128<<10, out)
}

func b3NoTCPListener() bool {
	interfaces, err := os.ReadFile("/proc/self/net/dev")
	if err != nil {
		return false
	}
	for line := range bytes.SplitSeq(interfaces, []byte{'\n'}) {
		if name, _, ok := bytes.Cut(line, []byte{':'}); ok && string(bytes.TrimSpace(name)) != "lo" {
			return false
		}
	}
	for _, name := range []string{"/proc/self/net/tcp", "/proc/self/net/tcp6"} {
		f, err := os.Open(name)
		if err != nil {
			return false
		}
		scan := bufio.NewScanner(f)
		valid := scan.Scan() && strings.Contains(scan.Text(), "local_address")
		for valid && scan.Scan() {
			fields := strings.Fields(scan.Text())
			valid = len(fields) > 3 && fields[3] != "0A"
		}
		err = scan.Err()
		f.Close()
		if !valid || err != nil {
			return false
		}
	}
	return true
}

func (q *b3Qualification) launchedCorrectly() bool {
	cwd, err := os.Readlink("/proc/self/cwd")
	if err != nil || cwd != q.PackageDir.Path || len(os.Args) == 0 || os.Args[0] != q.Files["test"].Path ||
		!slices.Equal(os.Args[1:], b3QualificationArgs) {
		return false
	}
	parent := "/proc/"+strconv.Itoa(os.Getppid())
	ownExe, ownErr := os.Stat("/proc/self/exe")
	parentExe, parentErr := os.Stat(parent+"/exe")
	if ownErr != nil || parentErr != nil || !os.SameFile(ownExe, q.files["test"]) || !os.SameFile(parentExe, q.files["test2json"]) {
		return false
	}
	args, err := os.ReadFile(parent+"/cmdline")
	want := append([]string{q.Files["test2json"].Path, "-p", b3QualificationPackage, "-t", q.Files["test"].Path}, b3QualificationArgs...)
	return err == nil && bytes.Equal(args, []byte(strings.Join(want, "\x00")+"\x00"))
}

func b3DialUnix(ctx context.Context, path string, uid, pid uint32) (*net.UnixConn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, errB3Qualification
	}
	u, ok := c.(*net.UnixConn)
	if !ok {
		c.Close()
		return nil, errB3Qualification
	}
	raw, err := u.SyscallConn()
	if err == nil {
		var cred *unix.Ucred
		var sockErr error
		if err = raw.Control(func(fd uintptr) {
			cred, sockErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		}); err == nil && sockErr == nil && cred != nil && cred.Uid == uid && cred.Pid > 1 && uint32(cred.Pid) == pid {
			return u, nil
		}
	}
	u.Close()
	return nil, errB3Qualification
}

func (q *b3Qualification) connect(ctx context.Context, caseName, intent string) (*pgx.Conn, error) {
	c, ok := q.Cases[caseName]
	if !ok || q.current() != nil {
		return nil, errB3Qualification
	}
	scope, err := q.control(ctx, b3ControlRequest{Action: "check", Case: caseName, Intent: intent})
	if err != nil || scope.Peer != uint32(os.Getpid()) || scope.Deadline == 0 {
		return nil, errB3Qualification
	}
	role := c.Writer
	if intent == "observe" {
		role = c.Observer
	}
	u := url.URL{Scheme: "postgresql", User: url.User(role), Host: "localhost", Path: "/"+c.Database}
	u.RawQuery = (url.Values{"host": {q.SocketDir.Path}, "port": {strconv.Itoa(int(q.Port))}, "sslmode": {"disable"},
		"gssencmode": {"disable"}, "passfile": {"/dev/null"}}).Encode()
	config, err := pgx.ParseConfig(u.String())
	if err != nil || config.Host != q.SocketDir.Path || config.Port != q.Port || config.User != role || config.Database != c.Database ||
		config.Password != "" || config.TLSConfig != nil || len(config.Fallbacks) != 0 {
		return nil, errB3Qualification
	}
	config.RuntimeParams = map[string]string{}
	config.ConnectTimeout = 3*time.Second
	config.DefaultQueryExecMode = pgx.QueryExecModeExec
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "unix" || address != q.Socket.Path {
			return nil, errB3Qualification
		}
		return b3DialUnix(ctx, address, q.PostmasterUID, q.PostmasterPID)
	}
	return pgx.ConnectConfig(ctx, config)
}

func b3MonotonicNS() (uint64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil || ts.Sec < 0 || ts.Nsec < 0 {
		return 0, errB3Qualification
	}
	return uint64(ts.Sec)*1_000_000_000 + uint64(ts.Nsec), nil
}

type b3Receipt struct {
	Version int `json:"version"`
	Nonce string `json:"nonce"`
	Candidate string `json:"candidate"`
	DB uint32 `json:"db"`
	Role uint32 `json:"role"`
	Peer uint32 `json:"peer_pid"`
	Backend uint32 `json:"backend_pid"`
	XID uint64 `json:"xid,string"`
	Seq uint64 `json:"seq"`
	Event string `json:"event"`
	Detail string `json:"detail"`
	At uint64 `json:"monotonic_ns,string"`
	Deadline uint64 `json:"deadline_ns,string"`
	TableOID *uint32 `json:"table_oid,omitempty"`
	ColumnAttnum *int16 `json:"column_attnum,omitempty"`
	SequenceOID *uint32 `json:"sequence_oid,omitempty"`
}

func (r b3Receipt) allocationOrigin(table uint32, column int16, sequence uint32) bool {
	return r.TableOID != nil && r.ColumnAttnum != nil && r.SequenceOID != nil &&
		*r.TableOID == table && *r.ColumnAttnum == column && *r.SequenceOID == sequence
}

func (r b3Receipt) validOrigin() bool {
	switch {
	case r.Event == "allocation_attempt", r.Event == "allocation",
		r.Event == "sequence_denied" && (r.Detail == "direct_nextval" || r.Detail == "undeclared_implicit" || r.Detail == "expired" && r.SequenceOID != nil):
		return r.SequenceOID != nil && *r.SequenceOID != 0 && r.TableOID != nil && r.ColumnAttnum != nil &&
			((*r.TableOID == 0 && *r.ColumnAttnum == 0) || (*r.TableOID != 0 && *r.ColumnAttnum > 0))
	default:
		return r.TableOID == nil && r.ColumnAttnum == nil && r.SequenceOID == nil
	}
}

type b3ControlRequest struct {
	Dossier string `json:"dossier_sha256"`
	Action string `json:"action"`
	Case string `json:"case"`
	Intent string `json:"intent,omitempty"`
	Peer uint32 `json:"peer,omitempty"`
	Backend uint32 `json:"backend,omitempty"`
	Seq uint64 `json:"seq,omitempty"`
}

type b3CopyStream struct {
	Rule   int    `json:"rule"`
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type b3RestoreRules struct {
	grant                     []string
	copies                    []b3CopyStream
	metadataCount, writeCount int
}

type b3ControlReply struct {
	OK bool `json:"ok"`
	Peer uint32 `json:"peer,omitempty"`
	MetadataCount int `json:"metadata_count,omitempty"`
	WriteCount int `json:"write_count,omitempty"`
	CopyStreams []b3CopyStream `json:"copy_streams,omitempty"`
	Deadline uint64 `json:"deadline_ns,string,omitempty"`
	Exit *int `json:"exit,omitempty"`
	Receipts [][]byte `json:"receipts,omitempty"`
}

func (q *b3Qualification) control(ctx context.Context, req b3ControlRequest) (b3ControlReply, error) {
	var reply b3ControlReply
	if _, err := q.Control.stat(os.ModeSocket); err != nil {
		return reply, errB3Qualification
	}
	c, err := b3DialUnix(ctx, q.Control.Path, q.SupervisorUID, q.SupervisorPID)
	if err != nil {
		return reply, err
	}
	defer c.Close()
	deadline, ok := ctx.Deadline()
	if !ok || c.SetDeadline(deadline) != nil {
		return reply, errB3Qualification
	}
	req.Dossier = q.dossier.SHA256
	if json.NewEncoder(c).Encode(req) != nil || c.CloseWrite() != nil ||
		b3Decode(c, b3MaxControlReply, &reply) != nil || !reply.OK {
		return b3ControlReply{}, errB3Qualification
	}
	return reply, nil
}

type b3CustodianLaunch struct {
	Version int `json:"version"`
	ControlPath string `json:"control_path"`
	ControlGID uint32 `json:"control_gid"`
	RunnerUID uint32 `json:"runner_uid"`
	DossierPath string `json:"dossier_path"`
	NotAfter time.Time `json:"not_after"`
	Executable b3FilePin `json:"executable"`
	Namespaces b3NamespacePins `json:"namespaces"`
}

type b3Process struct {
	pid, ppid, uid uint32
	start uint64
	exe os.FileInfo
}

func b3ProcessAt(pid uint32) (b3Process, error) {
	p := b3Process{pid: pid}
	proc := "/proc/" + strconv.FormatUint(uint64(pid), 10)
	info, dirErr := os.Stat(proc)
	stat, readErr := os.ReadFile(proc + "/stat")
	p.exe, _ = os.Stat(proc + "/exe")
	i := bytes.LastIndexByte(stat, ')')
	if dirErr != nil || readErr != nil || p.exe == nil || i < 0 {
		return p, errB3Qualification
	}
	f := bytes.Fields(stat[i+1:])
	if len(f) < 20 {
		return p, errB3Qualification
	}
	ticks, tickErr := strconv.ParseUint(string(f[19]), 10, 64)
	parent, ppErr := strconv.ParseUint(string(f[1]), 10, 32)
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || tickErr != nil || ppErr != nil || ticks == 0 {
		return p, errB3Qualification
	}
	p.uid, p.start, p.ppid = s.Uid, ticks, uint32(parent)
	return p, nil
}

func (q *b3Qualification) verifyPostmaster() error {
	proc, err := b3ProcessAt(q.PostmasterPID)
	if err != nil || proc.uid != q.PostmasterUID || proc.start != q.PostmasterStart || !os.SameFile(proc.exe, q.files["postgres"]) {
		return errB3Qualification
	}
	root := "/proc/" + strconv.FormatUint(uint64(q.PostmasterPID), 10)
	argv, err := os.ReadFile(root + "/cmdline")
	if err != nil || !b3ArgsMatch(argv, []string{q.Files["postgres"].Path, "-D", q.DataDir.Path,
		"-c", "config_file="+q.Files["server_config"].Path, "-c", "hba_file="+q.Files["hba_config"].Path,
		"-c", "ident_file="+q.Files["ident_config"].Path}) {
		return errB3Qualification
	}
	if !q.sameNamespaces(root) {
		return errB3Qualification
	}
	env, err := os.ReadFile(root + "/environ")
	if err != nil {
		return errB3Qualification
	}
	for key, expected := range map[string]string{
		"B3_GUARD_GRANTS": q.GrantsDir.Path, "B3_GUARD_RECEIPTS": q.ReceiptsDir.Path,
		"B3_GUARD_SUPERVISOR_UID": "0", "B3_GUARD_NONCE": q.Nonce, "B3_GUARD_CANDIDATE": q.Candidate,
		"B3_GUARD_NETNS_INO": strconv.FormatUint(q.NetNSInode, 10), "B3_GUARD_SOCKET_DIR": q.SocketDir.Path,
		"B3_GUARD_CONTROL": q.Control.Path, "B3_GUARD_SUPERVISOR_PID": strconv.FormatUint(uint64(q.SupervisorPID), 10),
	} {
		count := 0
		for _, entry := range bytes.Split(env, []byte{0}) {
			if k, value, ok := bytes.Cut(entry, []byte{'='}); ok && string(k) == key {
				if string(value) != expected {
					return errB3Qualification
				}
				count++
			}
		}
		if count != 1 {
			return errB3Qualification
		}
	}
	maps, err := os.ReadFile(root + "/maps")
	if err != nil {
		return errB3Qualification
	}
	guard := q.Files["guard"]
	mapped := false
	for _, line := range bytes.Split(maps, []byte{'\n'}) {
		fields := bytes.Fields(line)
		if len(fields) == 6 && string(fields[4]) == strconv.FormatUint(guard.Inode, 10) && string(fields[5]) == guard.Path &&
			b3ProcDevice(fields[3], guard.Device) {
			mapped = true
		}
	}
	pidFile, err := os.ReadFile(filepath.Join(q.DataDir.Path, "postmaster.pid"))
	lines := bytes.Split(pidFile, []byte{'\n'})
	if err != nil || !mapped || len(lines) < 6 || string(lines[0]) != strconv.FormatUint(uint64(q.PostmasterPID), 10) ||
		string(lines[1]) != q.DataDir.Path || string(lines[3]) != strconv.Itoa(int(q.Port)) ||
		string(lines[4]) != q.SocketDir.Path || len(lines[5]) != 0 {
		return errB3Qualification
	}
	return nil
}

func b3ProcDevice(text []byte, device uint64) bool {
	a, b, ok := bytes.Cut(text, []byte{':'})
	major, e1 := strconv.ParseUint(string(a), 16, 32)
	minor, e2 := strconv.ParseUint(string(b), 16, 32)
	return ok && e1 == nil && e2 == nil && uint32(major) == unix.Major(device) && uint32(minor) == unix.Minor(device)
}

func (q b3NamespacePins) sameNamespaces(proc string) bool {
	for _, ns := range [...]struct {
		name string
		dev, ino uint64
	}{{"net", q.NetNSDevice, q.NetNSInode}, {"pid", q.PIDNSDevice, q.PIDNSInode},
		{"user", q.UserNSDevice, q.UserNSInode}, {"time", q.TimeNSDevice, q.TimeNSInode}} {
		info, err := os.Stat(proc+"/ns/"+ns.name)
		if err != nil {
			return false
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Dev != ns.dev || st.Ino != ns.ino {
			return false
		}
	}
	return true
}

func b3MappedRoot() bool {
	data, err := os.ReadFile("/proc/self/uid_map")
	first, _, _ := bytes.Cut(data, []byte{'\n'})
	fields := bytes.Fields(first)
	if err != nil || len(fields) != 3 || string(fields[0]) != "0" {
		return false
	}
	hostUID, err := strconv.ParseUint(string(fields[1]), 10, 32)
	return err == nil && hostUID != 0
}

func (q *b3Qualification) verifyRunner(peer *unix.Ucred) (b3Process, error) {
	var p b3Process
	if peer == nil || peer.Pid <= 1 || peer.Uid != q.RunnerUID || peer.Gid != q.RunnerGID {
		return p, errB3Qualification
	}
	p, err := b3ProcessAt(uint32(peer.Pid))
	if err != nil || p.uid != q.RunnerUID || !os.SameFile(p.exe, q.files["test"]) {
		return p, errB3Qualification
	}
	root := "/proc/" + strconv.FormatUint(uint64(p.pid), 10)
	workdir, wdErr := os.Readlink(root+"/cwd")
	namespacesOK := q.sameNamespaces(root)
	argv, argvErr := os.ReadFile(root+"/cmdline")
	want := append([]string{q.Files["test"].Path}, b3QualificationArgs...)
	if wdErr != nil || !namespacesOK || argvErr != nil || workdir != q.PackageDir.Path || !b3ArgsMatch(argv, want) {
		return p, errB3Qualification
	}
	parent, err := b3ProcessAt(p.ppid)
	if err != nil || parent.uid != q.RunnerUID || !os.SameFile(parent.exe, q.files["test2json"]) {
		return p, errB3Qualification
	}
	argv, err = os.ReadFile("/proc/"+strconv.FormatUint(uint64(p.ppid), 10)+"/cmdline")
	want = append([]string{q.Files["test2json"].Path, "-p", b3QualificationPackage, "-t", q.Files["test"].Path}, b3QualificationArgs...)
	if err != nil || !b3ArgsMatch(argv, want) {
		return p, errB3Qualification
	}
	return p, nil
}

func b3ArgsMatch(argv []byte, want []string) bool {
	return bytes.Equal(argv, []byte(strings.Join(want, "\x00")+"\x00"))
}

func b3UnixCred(c *net.UnixConn) *unix.Ucred {
	var cred *unix.Ucred
	var optErr error
	raw, err := c.SyscallConn()
	if err != nil || raw.Control(func(fd uintptr) {
		cred, optErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}) != nil || optErr != nil {
		return nil
	}
	return cred
}

type b3CaseRun struct {
	deadline uint64
	observeDeadline uint64
	restore uint32
	result chan int
	exit *int
	released bool
	restoreProc b3Process
	restoreFD int
	backend b3Process
}

type b3Custodian struct {
	q *b3Qualification
	runner b3Process
	archive b3RestoreRules
	observerSQL, syntheticSQL func(string) []string
	active map[string]*b3CaseRun
}

func b3ServeCustody(ctx context.Context, path string, archive func(context.Context, *b3Qualification) (b3RestoreRules, error),
	observer, synthetic func(string) []string) error {
	var launch b3CustodianLaunch
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getenv(b3QualificationEnv) != "" || !b3NoInheritedSettings() ||
		!slices.Equal(os.Args[1:], b3CustodianArgs) {
		return errB3Qualification
	}
	if _, err := decodePrivate(path, &launch); err != nil || launch.Version != 1 ||
		launch.NotAfter.Location() != time.UTC || !time.Now().Before(launch.NotAfter) || launch.ControlGID == 0 || launch.RunnerUID == 0 ||
		!filepath.IsAbs(launch.ControlPath) || filepath.Clean(launch.ControlPath) != launch.ControlPath ||
		!launch.Namespaces.isolatedCustodian() ||
		!filepath.IsAbs(launch.DossierPath) || filepath.Clean(launch.DossierPath) != launch.DossierPath ||
		!b3CustodiedPath(launch.ControlPath, 0) || !b3CustodiedPath(launch.DossierPath, 0) ||
		launch.Executable.Path != os.Args[0] || launch.Executable.UID != 0 || launch.Executable.Mode&0o222 != 0 ||
		!b3CustodiedPath(launch.Executable.Path, 0) {
		return errB3Qualification
	}
	f, pin, err := launch.Executable.open()
	if err != nil {
		return err
	}
	f.Close()
	exe, err := os.Stat("/proc/self/exe")
	if err != nil || !os.SameFile(exe, pin) {
		return errB3Qualification
	}
	ctx, cancel := context.WithDeadline(ctx, launch.NotAfter)
	defer cancel()
	previous := unix.Umask(0o077)
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: launch.ControlPath, Net: "unix"})
	unix.Umask(previous)
	if err != nil {
		return errB3Qualification
	}
	defer l.Close()
	if os.Chown(launch.ControlPath, 0, int(launch.ControlGID)) != nil || os.Chmod(launch.ControlPath, 0o660) != nil {
		return errB3Qualification
	}
	var state *b3Custodian
	defer func() {
		if state != nil {
			for _, run := range state.active {
				if run.restore != 0 {
					_ = unix.Close(run.restoreFD)
				}
			}
		}
	}()
	for ctx.Err() == nil {
		if l.SetDeadline(launch.NotAfter) != nil {
			return errB3Qualification
		}
		c, err := l.AcceptUnix()
		if err != nil {
			return errB3Qualification
		}
		func() {
			defer c.Close()
			deadline := time.Now().Add(time.Second)
			if launch.NotAfter.Before(deadline) {
				deadline = launch.NotAfter
			}
			if c.SetDeadline(deadline) != nil {
				return
			}
			peer := b3UnixCred(c)
			reader := bufio.NewReader(c)
			first, err := reader.Peek(1)
			if err != nil || peer == nil {
				return
			}
			if first[0] == 'B' {
				if state != nil && peer.Uid == state.q.PostmasterUID {
					state.attestRestore(c, reader, peer)
				}
				return
			}
			if first[0] != '{' || peer.Gid != launch.ControlGID || peer.Uid != launch.RunnerUID {
				return
			}
			var req b3ControlRequest
			if b3Decode(reader, 4096, &req) != nil {
				return
			}
			if c.SetWriteDeadline(launch.NotAfter) != nil {
				return
			}
			reply := b3ControlReply{}
			if state == nil && req.Action == "arm" && req.Case == b3RestoreRollback {
				state, _ = b3StartCustodian(ctx, launch, req, peer, archive, observer, synthetic)
			}
			if state != nil {
				reply = state.handle(ctx, req, peer)
			}
			_ = json.NewEncoder(c).Encode(reply)
		}()
	}
	return nil
}

func b3StartCustodian(ctx context.Context, launch b3CustodianLaunch, req b3ControlRequest, peer *unix.Ucred,
	archive func(context.Context, *b3Qualification) (b3RestoreRules, error), observer, synthetic func(string) []string) (*b3Custodian, error) {
	info, err := os.Lstat(launch.DossierPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o222 != 0 {
		return nil, errB3Qualification
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 {
		return nil, errB3Qualification
	}
	dossier := b3FilePin{b3NodePin: b3NodePin{Path: launch.DossierPath, Device: st.Dev, Inode: st.Ino,
		UID: st.Uid, GID: st.Gid, Mode: uint32(info.Mode().Perm())}, Bytes: info.Size(), SHA256: req.Dossier}
	f, _, err := dossier.open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	q := &b3Qualification{dossier: dossier}
	if b3Decode(f, 128<<10, &q.b3CustodyDossier) != nil || !q.validDossier() ||
		q.Control.Path != launch.ControlPath || q.Control.GID != launch.ControlGID || q.NotAfter.After(launch.NotAfter) ||
		q.b3NamespacePins != launch.Namespaces || q.RunnerUID != launch.RunnerUID ||
		q.SupervisorPID != uint32(os.Getpid()) || q.Files["test"] != launch.Executable ||
		q.pinFiles() != nil || q.verifyBuildAndFence() != nil || q.resourcePins() != nil || q.verifyPostmaster() != nil {
		return nil, errB3Qualification
	}
	p, err := q.verifyRunner(peer)
	if err != nil {
		return nil, err
	}
	rules, err := archive(ctx, q)
	if err != nil || len(rules.grant) < 3 {
		return nil, errB3Qualification
	}
	return &b3Custodian{q: q, runner: p, archive: rules, observerSQL: observer, syntheticSQL: synthetic,
		active: make(map[string]*b3CaseRun, len(b3Cases))}, nil
}

func (s *b3Custodian) handle(ctx context.Context, req b3ControlRequest, peer *unix.Ucred) b3ControlReply {
	fail := b3ControlReply{}
	target, ok := s.q.Cases[req.Case]
	if !ok || req.Dossier != s.q.dossier.SHA256 || peer == nil || uint32(peer.Pid) != s.runner.pid ||
		peer.Uid != s.runner.uid || peer.Gid != s.q.RunnerGID {
		return fail
	}
	p, err := b3ProcessAt(s.runner.pid)
	if err != nil || p.start != s.runner.start || !os.SameFile(p.exe, s.runner.exe) || s.q.resourcePins() != nil {
		return fail
	}
	now, err := b3MonotonicNS()
	if err != nil {
		return fail
	}
	run := s.active[req.Case]
	if req.Action != "check" && req.Intent != "" {
		return fail
	}
	if req.Action == "arm" {
		if run != nil || req.Peer != 0 || req.Backend != 0 || req.Seq != 0 || s.q.verifyPostmaster() != nil {
			return fail
		}
		return s.arm(req.Case, target, now)
	}
	if run == nil || (req.Action != "release" && (req.Backend != 0 || req.Seq != 0)) {
		return fail
	}
	switch req.Action {
	case "check":
		if req.Peer != 0 || run.deadline == 0 || s.q.verifyPostmaster() != nil {
			return fail
		}
		switch req.Intent {
		case "observe":
			if now < run.observeDeadline {
				return b3ControlReply{OK: true, Peer: s.runner.pid, Deadline: run.observeDeadline}
			}
		case "writer_live":
			if now < run.deadline {
				return b3ControlReply{OK: true, Peer: s.runner.pid, Deadline: run.deadline}
			}
		case "writer_expired":
			if now >= run.deadline && run.restore != 0 && b3RestoreStage(req.Case) != "" {
				return b3ControlReply{OK: true, Peer: s.runner.pid, Deadline: run.deadline}
			}
		}
		return fail
	case "restore":
		if req.Peer != 0 || run.restore != 0 || now >= run.deadline || b3RestoreStage(req.Case) == "" || s.q.verifyPostmaster() != nil {
			return fail
		}
		if s.startRestore(ctx, req.Case, target, run) != nil {
			return fail
		}
		return b3ControlReply{OK: true, Peer: run.restore, MetadataCount: s.archive.metadataCount,
			WriteCount: s.archive.writeCount, CopyStreams: s.archive.copies}
	case "wait_restore":
		if req.Peer == 0 || req.Peer != run.restore || !run.released || run.result == nil {
			return fail
		}
		if run.exit == nil {
			select {
			case code := <-run.result:
				if code < 0 {
					return fail
				}
				run.exit = &code
			case <-ctx.Done():
				return fail
			}
		}
		return b3ControlReply{OK: true, Exit: run.exit}
	case "receipts":
		if req.Peer == 0 || (req.Peer != s.runner.pid && req.Peer != run.restore) {
			return fail
		}
		receipts, err := s.receipts(req.Peer, target)
		return b3ControlReply{OK: err == nil, Receipts: receipts}
	case "release":
		return b3ControlReply{OK: s.release(req, target, run, now)}
	}
	return fail
}

func (s *b3Custodian) arm(name string, target b3CaseScope, now uint64) b3ControlReply {
	// A partial publish is never renewed or reopened.
	if len(s.active) >= len(b3Cases) || b3Cases[len(s.active)] != name || s.q.NotAfterNS <= now+40_000_000_000 {
		return b3ControlReply{}
	}
	duration := uint64(5 * time.Second)
	if b3RestoreStage(name) != "" {
		duration = uint64(12 * time.Second)
	}
	run := &b3CaseRun{deadline: now + duration, observeDeadline: now+40_000_000_000}
	s.active[name] = run
	barrier := "none"
	if name == b3ExpiredDeferred {
		barrier = "before_deferred"
	}
	if s.publishGrant(s.runner, target, target.ObserverOID, "observe", run.observeDeadline, true, false, "none", b3EncodeQueries(s.observerSQL(name))) != nil ||
		s.publishGrant(s.runner, target, target.WriterOID, "synthetic", run.deadline, target.GrantPreFence,
			target.TailAuthority, barrier, b3EncodeQueries(s.syntheticSQL(name))) != nil {
		return b3ControlReply{}
	}
	return b3ControlReply{OK: true, Peer: s.runner.pid, Deadline: run.deadline}
}

func b3EncodeQueries(queries []string) []string {
	rows := make([]string, len(queries))
	for i, sql := range queries {
		rows[i] = "query\t" + hex.EncodeToString([]byte(sql))
	}
	return rows
}

func b3Bit(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *b3Custodian) publishGrant(peer b3Process, target b3CaseScope, role uint32, lane string,
	deadline uint64, prefence, tail bool, barrier string, rules []string) error {
	st, ok := peer.exe.Sys().(*syscall.Stat_t)
	if !ok || peer.uid != s.q.RunnerUID || len(rules) == 0 {
		return errB3Qualification
	}
	var body strings.Builder
	fmt.Fprintf(&body, "B3Q1\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%s\n",
		s.q.Nonce, s.q.Candidate, s.q.SystemIdentifier, peer.start, peer.uid, st.Dev, st.Ino, target.DatabaseOID, role,
		lane, deadline, b3Bit(prefence), b3Bit(tail), barrier)
	for _, row := range rules {
		body.WriteString(row)
		body.WriteByte('\n')
	}
	return s.publish(fmt.Sprintf("grant.%d.%d.%d", peer.pid, target.DatabaseOID, role), body.String())
}

func (s *b3Custodian) publish(name, content string) error {
	pin := s.q.GrantsDir
	if _, err := pin.stat(os.ModeDir); err != nil {
		return errB3Qualification
	}
	dir, err := os.Open(pin.Path)
	if err != nil {
		return errB3Qualification
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !pin.matches(info, os.ModeDir) {
		return errB3Qualification
	}
	f, err := os.CreateTemp(pin.Path, ".b3-custody-")
	if err != nil {
		return errB3Qualification
	}
	defer os.Remove(f.Name())
	_, err = f.WriteString(content)
	if err != nil || f.Chown(int(pin.UID), int(pin.GID)) != nil || f.Chmod(0o440) != nil || f.Sync() != nil {
		f.Close()
		return errB3Qualification
	}
	if f.Close() != nil || unix.Renameat2(int(dir.Fd()), filepath.Base(f.Name()), int(dir.Fd()), name, unix.RENAME_NOREPLACE) != nil ||
		dir.Sync() != nil {
		return errB3Qualification
	}
	return nil
}

func b3RestoreStage(name string) string {
	switch name {
	case b3RestoreRollback:
		return "before_decision"
	case b3RestoreTail:
		return "after_decision"
	}
	return ""
}

func (q *b3Qualification) restoreCommand(ctx context.Context, args ...string) (*exec.Cmd, *os.File, error) {
	tool, _, err := q.Files["pg_restore"].open()
	if err != nil {
		return nil, nil, err
	}
	tool.Close()
	archive, _, err := q.Files["archive"].open()
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.CommandContext(ctx, q.Files["pg_restore"].Path, append(args, "/proc/self/fd/3")...)
	cmd.Env = []string{"LC_ALL=C", "TZ=UTC", "HOME=/nonexistent", "PGPASSFILE=/dev/null", "PGSERVICEFILE=/dev/null"}
	cmd.Dir = q.PackageDir.Path
	cmd.ExtraFiles = []*os.File{archive}
	return cmd, archive, nil
}

func (s *b3Custodian) startRestore(ctx context.Context, name string, target b3CaseScope, run *b3CaseRun) error {
	q := s.q
	argv := q.restoreArgv(target)
	cmd, archive, err := q.restoreCommand(ctx, argv[1:len(argv)-1]...)
	if err != nil {
		return err
	}
	defer archive.Close()
	// Stop at the real executable's exec trap before it can open a database socket.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cmd.Cancel = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Ptrace: true,
		Credential: &syscall.Credential{Uid: q.RunnerUID, Gid: q.RunnerGID}}
	if cmd.Start() != nil {
		return errB3Qualification
	}
	unadmitted := true
	defer func() {
		if unadmitted {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	pid := cmd.Process.Pid
	var status unix.WaitStatus
	for {
		got, err := unix.Wait4(pid, &status, unix.WUNTRACED, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil || got != pid || !status.Stopped() || status.StopSignal() != unix.SIGTRAP {
			return errB3Qualification
		}
		break
	}
	peer, err := b3ProcessAt(uint32(pid))
	now, clockErr := b3MonotonicNS()
	if err != nil || clockErr != nil || now >= run.deadline || peer.ppid != q.SupervisorPID || peer.uid != q.RunnerUID ||
		!os.SameFile(peer.exe, q.files["pg_restore"]) {
		return errB3Qualification
	}
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return errB3Qualification
	}
	defer func() {
		if unadmitted {
			_ = unix.Close(pidfd)
		}
	}()
	if !q.verifyRestoreProcess(peer, target) ||
		s.publishGrant(peer, target, target.WriterOID, "restore", run.deadline, true, true, b3RestoreStage(name), s.archive.grant) != nil ||
		unix.PtraceDetach(pid) != nil {
		return errB3Qualification
	}
	unadmitted = false
	run.restore, run.result = uint32(pid), make(chan int, 1)
	run.restoreProc, run.restoreFD = peer, pidfd
	go func() {
		_ = cmd.Wait()
		if cmd.ProcessState == nil {
			run.result <- -1
			return
		}
		run.result <- cmd.ProcessState.ExitCode()
	}()
	return nil
}

func (s *b3Custodian) receipts(peer uint32, target b3CaseScope) ([][]byte, error) {
	pin := s.q.ReceiptsDir
	if _, err := pin.stat(os.ModeDir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(pin.Path)
	if err != nil || len(entries) > 10000 {
		return nil, errB3Qualification
	}
	type record struct {
		body []byte
		backend uint32
		seq uint64
	}
	var records []record
	prefix := fmt.Sprintf("receipt.%d.", peer)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		f, openErr := os.OpenFile(filepath.Join(pin.Path, entry.Name()), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if openErr != nil {
			return nil, errB3Qualification
		}
		info, statErr := f.Stat()
		raw, readErr := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		var rec b3Receipt
		if statErr != nil || readErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o640 || info.Size() != int64(len(raw)) ||
			b3Decode(bytes.NewReader(raw), 4096, &rec) != nil || !rec.validOrigin() {
			return nil, errB3Qualification
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != pin.UID || rec.Peer != peer || rec.Nonce != s.q.Nonce || rec.Candidate != s.q.Candidate ||
			entry.Name() != fmt.Sprintf("receipt.%d.%d.%d", rec.Peer, rec.Backend, rec.Seq) {
			return nil, errB3Qualification
		}
		if rec.DB == target.DatabaseOID && rec.Role == target.WriterOID {
			records = append(records, record{raw, rec.Backend, rec.Seq})
		}
	}
	slices.SortFunc(records, func(a, b record) int {
		if a.backend != b.backend {
			if a.backend < b.backend {
				return -1
			}
			return 1
		}
		if a.seq < b.seq {
			return -1
		}
		if a.seq > b.seq {
			return 1
		}
		return 0
	})
	result := make([][]byte, 0, len(records))
	for _, rec := range records {
		result = append(result, rec.body)
	}
	return result, nil
}

func (s *b3Custodian) release(req b3ControlRequest, target b3CaseScope, run *b3CaseRun, now uint64) bool {
	stage := b3RestoreStage(req.Case)
	peer := run.restore
	if req.Case == b3ExpiredDeferred {
		stage, peer = "before_deferred", s.runner.pid
	}
	if stage == "" || peer == 0 || req.Peer != peer || req.Backend == 0 || req.Seq == 0 || run.released || now < run.deadline ||
		s.q.verifyPostmaster() != nil {
		return false
	}
	records, err := s.receipts(peer, target)
	if err != nil {
		return false
	}
	var barrier b3Receipt
	for _, raw := range records {
		var r b3Receipt
		if b3Decode(bytes.NewReader(raw), 4096, &r) != nil {
			return false
		}
		if r.Backend == req.Backend && r.Seq == req.Seq && r.Event == "barrier" && r.Detail == stage &&
			r.Deadline == run.deadline && r.At < run.deadline && r.XID > 0 {
			barrier = r
		}
	}
	if barrier.Seq == 0 || s.publish(fmt.Sprintf("release.%d.%d.%d", peer, barrier.Backend, barrier.Seq), "") != nil {
		return false
	}
	run.released = true
	return true
}

func (q *b3Qualification) restoreInspection(ctx context.Context, name string, args ...string) ([]byte, error) {
	pin := q.Files[name]
	expected, _, err := pin.open()
	if err != nil {
		return nil, err
	}
	defer expected.Close()
	if pin.Bytes > 2<<20 {
		return nil, errB3Qualification
	}
	cmd, archive, err := q.restoreCommand(ctx, "--no-password")
	if err != nil {
		return nil, err
	}
	defer archive.Close()
	cmd.Args = append(append(cmd.Args[:2], args...), "/proc/self/fd/3")
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: q.RunnerUID, Gid: q.RunnerGID}}
	pipe, err := cmd.StdoutPipe()
	if err != nil || cmd.Start() != nil {
		return nil, errB3Qualification
	}
	actual, readErr := io.ReadAll(io.LimitReader(pipe, pin.Bytes+1))
	pipe.Close()
	exitErr := cmd.Wait()
	want, pinErr := io.ReadAll(expected)
	if readErr != nil || pinErr != nil || exitErr != nil || !bytes.Equal(actual, want) {
		return nil, errB3Qualification
	}
	return actual, nil
}

func (q b3NamespacePins) isolatedCustodian() bool {
	return q.HostNetNSInode != 0 && q.NetNSInode != q.HostNetNSInode &&
		q.HostPIDNSInode != 0 && q.PIDNSInode != q.HostPIDNSInode &&
		q.HostUserNSInode != 0 && q.UserNSInode != q.HostUserNSInode &&
		q.sameNamespaces("/proc/self") && q.sameNamespaces("/proc/1") && b3MappedRoot() && b3NoTCPListener()
}

func b3LivePIDFD(fd int) bool {
	poll := [...]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, err := unix.Poll(poll[:], 0)
	return err == nil && n == 0
}

func b3SocketPIDFD(c *net.UnixConn) (int, error) {
	conn, err := c.SyscallConn()
	if err != nil {
		return -1, err
	}
	fd := -1
	var sockErr error
	if err = conn.Control(func(open uintptr) {
		fd, sockErr = unix.GetsockoptInt(int(open), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
	}); err != nil {
		return -1, err
	}
	return fd, sockErr
}

func (s *b3Custodian) attestRestore(c *net.UnixConn, reader io.Reader, cred *unix.Ucred) {
	line, err := io.ReadAll(io.LimitReader(reader, 513))
	if err != nil || len(line) > 512 || !bytes.HasPrefix(line, []byte("B3A1\t")) || cred.Pid <= 1 ||
		cred.Uid != s.q.PostmasterUID {
		return
	}
	fd, err := b3SocketPIDFD(c)
	if err != nil {
		return
	}
	defer unix.Close(fd)
	if !b3LivePIDFD(fd) || s.q.resourcePins() != nil || s.q.verifyPostmaster() != nil {
		return
	}
	backend, err := b3ProcessAt(uint32(cred.Pid))
	if err != nil || backend.uid != s.q.PostmasterUID || backend.ppid != s.q.PostmasterPID ||
		!os.SameFile(backend.exe, s.q.files["postgres"]) ||
		!s.q.sameNamespaces(fmt.Sprintf("/proc/%d", backend.pid)) {
		return
	}
	for _, name := range [...]string{b3RestoreRollback, b3RestoreTail} {
		run := s.active[name]
		if run == nil || run.restore == 0 || run.released {
			continue
		}
		target := s.q.Cases[name]
		child := run.restoreProc
		exe := s.q.Files["pg_restore"]
		want := fmt.Sprintf("B3A1\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			s.q.Nonce, s.q.Candidate, s.q.SystemIdentifier, target.DatabaseOID, target.WriterOID,
			backend.pid, child.pid, child.start, child.uid, exe.Device, exe.Inode, run.deadline)
		if !bytes.Equal(line, []byte(want)) {
			continue
		}
		now, err := b3MonotonicNS()
		if err != nil || now >= run.deadline || !b3LivePIDFD(run.restoreFD) ||
			(run.backend.pid != 0 && (run.backend.pid != backend.pid || run.backend.start != backend.start)) ||
			!s.q.verifyRestoreProcess(child, target) {
			return
		}
		now, err = b3MonotonicNS()
		if err != nil || now >= run.deadline || !b3LivePIDFD(fd) || !b3LivePIDFD(run.restoreFD) {
			return
		}
		run.backend = backend
		// A backend peer gets only an echo bound to the already registered child.
		_, _ = c.Write(append([]byte("OK\t"), line...))
		return
	}
}

func (q *b3Qualification) verifyRestoreProcess(want b3Process, target b3CaseScope) bool {
	child, err := b3ProcessAt(want.pid)
	if err != nil || child.start != want.start || child.uid != q.RunnerUID || child.ppid != q.SupervisorPID ||
		!os.SameFile(child.exe, q.files["pg_restore"]) {
		return false
	}
	proc := fmt.Sprintf("/proc/%d", child.pid)
	argv, err := os.ReadFile(proc + "/cmdline")
	if err != nil || !q.sameNamespaces(proc) || !b3ArgsMatch(argv, q.restoreArgv(target)) {
		return false
	}
	fdInfo, err := os.ReadFile(proc + "/fdinfo/3")
	if err != nil {
		return false
	}
	var readOnly bool
	for line := range bytes.SplitSeq(fdInfo, []byte{'\n'}) {
		if value, ok := bytes.CutPrefix(line, []byte("flags:")); ok {
			flags, err := strconv.ParseUint(string(bytes.TrimSpace(value)), 8, 64)
			readOnly = err == nil && flags&unix.O_ACCMODE == unix.O_RDONLY
		}
	}
	if !readOnly {
		return false
	}
	archive := q.Files["archive"]
	info, err := os.Stat(proc + "/fd/3")
	return err == nil && archive.matches(info, 0) && b3SameFileState(info, q.files["archive"])
}

func (q *b3Qualification) restoreArgv(target b3CaseScope) []string {
	return []string{q.Files["pg_restore"].Path, "--single-transaction", "--no-password", "--host="+q.SocketDir.Path,
		"--port="+strconv.Itoa(int(q.Port)), "--username="+target.Writer, "--dbname="+target.Database, "/proc/self/fd/3"}
}
