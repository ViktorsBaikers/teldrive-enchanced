package postgres

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Approval is private run authority, not a reusable test-prefix exception.
// Times are RFC3339 UTC; ports are TCP ports; identifiers are exact strings.
type Approval struct {
	Version          int         `json:"version"`
	RunID            string      `json:"run_id"`
	ExpiresAt        time.Time   `json:"expires_at"`
	Host             string      `json:"host"`
	Port             uint16      `json:"port"`
	Role             string      `json:"role"`
	ClusterName      string      `json:"cluster_name"`
	SystemIdentifier string      `json:"system_identifier"`
	Namespace        string      `json:"namespace"`
	Tools            ToolContext `json:"tools"`
	digest           [32]byte
}

// ToolContext binds later dump/restore callers to the same approved container.
// Callers must independently inspect Docker identity before invoking these paths.
type ToolContext struct {
	Executor    string `json:"executor"`
	ContainerID string `json:"container_id"`
	ImageID     string `json:"image_id"`
	NetworkID   string `json:"network_id"`
	RunTag      string `json:"run_tag"`
	PSQL        string `json:"psql"`
	PGDump      string `json:"pg_dump"`
	PGRestore   string `json:"pg_restore"`
}

// InstanceIdentity must come from a read-only query on the connected server.
type InstanceIdentity struct {
	Host             string
	Port             uint16
	Role             string
	ClusterName      string
	SystemIdentifier string
	Database         string
}

var (
	errIsolation     = errors.New("B2 isolation approval rejected; retain owned resources and inspect private evidence")
	runPattern       = regexp.MustCompile(`^[0-9a-f]{32}$`)
	idPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	namespacePattern = regexp.MustCompile(`^teldrive_test_b2_[0-9a-f]{8}_$`)
	systemPattern    = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
)

// LoadApproval rejects symlinks, shared files/directories, unknown/duplicate keys
// and trailing JSON. It never includes private paths or content in diagnostics.
func LoadApproval(path string) (*Approval, error) {
	data, err := readPrivate(path)
	if err != nil {
		return nil, errIsolation
	}
	if err := uniqueJSON(json.NewDecoder(bytes.NewReader(data))); err != nil {
		return nil, errIsolation
	}
	var a Approval
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&a); err != nil {
		return nil, errIsolation
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errIsolation
	}
	a.digest = sha256.Sum256(data)
	return &a, nil
}

func uniqueJSON(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errIsolation
			}
			seen[name] = true
			if err := uniqueJSON(d); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueJSON(d); err != nil {
				return err
			}
		}
	default:
		return errIsolation
	}
	_, err = d.Token()
	return err
}

func privateInfo(info os.FileInfo, directory bool) bool {
	if info.Mode()&os.ModeSymlink != 0 || info.IsDir() != directory {
		return false
	}
	mode := os.FileMode(0600)
	if directory {
		mode = 0700
	}
	if info.Mode().Perm() != mode {
		return false
	}
	// Fail closed on platforms without a Unix owner field; Windows execution
	// requires its own ACL-backed authority, never a permissions-bit fallback.
	owner := reflect.ValueOf(info.Sys())
	if owner.Kind() != reflect.Pointer || owner.IsNil() {
		return false
	}
	owner = owner.Elem()
	if owner.Kind() != reflect.Struct {
		return false
	}
	uid := owner.FieldByName("Uid")
	return uid.IsValid() && uid.CanUint() && uid.Uint() == uint64(os.Getuid())
}

func readPrivate(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errIsolation
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, errIsolation
		}
		if p == path && (!info.Mode().IsRegular() || !privateInfo(info, false)) {
			return nil, errIsolation
		}
		if p == filepath.Dir(path) && !privateInfo(info, true) {
			return nil, errIsolation
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errIsolation
	}
	defer file.Close()
	opened, err := file.Stat()
	original, lerr := os.Lstat(path)
	if err != nil || lerr != nil || !os.SameFile(opened, original) || !privateInfo(opened, false) {
		return nil, errIsolation
	}
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(data) > 16384 {
		return nil, errIsolation
	}
	return data, nil
}

// ValidateTarget is the offline gate before any connection or mutation.
// Only explicit IP/port/user/password URLs are accepted; no service/env lookup.
func (a Approval) ValidateTarget(rawURL, databaseName string, now time.Time) error {
	if a.Version != 1 || !runPattern.MatchString(a.RunID) || !namespacePattern.MatchString(a.Namespace) ||
		!now.Before(a.ExpiresAt) || a.ExpiresAt.Location() != time.UTC || a.Role != "b2_audit" ||
		a.Port == 0 || a.ClusterName == "" || len(a.ClusterName) > 63 || !systemPattern.MatchString(a.SystemIdentifier) {
		return errIsolation
	}
	ip, err := netip.ParseAddr(a.Host)
	if err != nil || ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" || ip.String() != a.Host {
		return errIsolation
	}
	if !strings.HasPrefix(databaseName, a.Namespace) || !runPattern.MatchString(strings.TrimPrefix(databaseName, a.Namespace)) {
		return errIsolation
	}
	if err := a.ValidateTools(a.Tools); err != nil {
		return err
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil || u.Fragment != "" || u.Opaque != "" {
		return errIsolation
	}
	password, supplied := u.User.Password()
	if !supplied || password == "" || u.User.Username() != a.Role || u.Hostname() != a.Host || u.Port() != strconv.Itoa(int(a.Port)) {
		return errIsolation
	}
	if u.Path != "/postgres" && u.Path != "/"+databaseName {
		return errIsolation
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 1 || len(query["sslmode"]) != 1 || query.Get("sslmode") != "disable" {
		return errIsolation
	}
	return nil
}

// ValidateTools compares independently observed execution context, not just paths.
func (a Approval) ValidateTools(observed ToolContext) error {
	t := a.Tools
	if t != observed || t.Executor != "docker-exec" || !idPattern.MatchString(t.ContainerID) ||
		t.ImageID != "sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015" ||
		!idPattern.MatchString(t.NetworkID) || t.RunTag != a.RunID ||
		t.PSQL != "/usr/local/bin/psql" || t.PGDump != "/usr/local/bin/pg_dump" || t.PGRestore != "/usr/local/bin/pg_restore" {
		return errIsolation
	}
	return nil
}

// Guard reaches mutate only after both local authority and independent identity.
func (a Approval) Guard(rawURL, databaseName string, now time.Time, observed *InstanceIdentity, tools ToolContext, mutate func() error) error {
	if err := a.ValidateTarget(rawURL, databaseName, now); err != nil {
		return err
	}
	if err := a.ValidateTools(tools); err != nil {
		return err
	}
	if observed == nil || observed.Host != a.Host || observed.Port != a.Port || observed.Role != a.Role ||
		observed.ClusterName != a.ClusterName || observed.SystemIdentifier != a.SystemIdentifier ||
		(observed.Database != "postgres" && observed.Database != databaseName) || mutate == nil {
		return errIsolation
	}
	return mutate()
}
