/* Fixture-only preload module for the exact PostgreSQL 18.7 B3 patch. */
#include "postgres.h"

#ifdef __linux__
#include <ctype.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#else
#error "B3 qualification requires Linux Unix peer credentials and pidfds"
#endif

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "executor/executor.h"
#include "libpq/hba.h"
#include "libpq/libpq-be.h"
#include "libpq/libpq.h"
#include "libpq/protocol.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "tcop/fixtureguard.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/wait_classes.h"

#if PG_VERSION_NUM != 180007
#error "B3 qualification is pinned to PostgreSQL 18.7"
#endif

/* Linux UAPI value; older libc headers need not expose the name. */
#ifndef SO_PEERPIDFD
#define SO_PEERPIDFD 77
#endif

PG_MODULE_MAGIC;

#define MAX_RULES 128
#define MAX_LINE 65536

typedef enum Lane { SYNTHETIC, RESTORE, OBSERVE } Lane;
typedef struct ImplicitRule
{
	Oid table;
	int32 attnum;
	Oid sequence;
	bool serial;
} ImplicitRule;

typedef struct ArchiveRule
{
	char *schema;
	char *table;
	char *column;
	char *sequence;
	bool serial;
	int64 value;
	bool called;
	char *sql;
	bool executed;
	bool copied;
} ArchiveRule;

typedef struct CopyRule
{
	uint64 bytes;
	char sha256[PG_SHA256_DIGEST_STRING_LENGTH];
} CopyRule;

static int grants_fd, receipts_fd;
static uid_t supervisor_uid;
static const char *run_nonce, *candidate, *socket_dir;
static uint64 netns_inode;

static int peer_pid, restore_pidfd, supervisor_pid;
static const char *control_path;
static uint64 peer_start_ticks, peer_exe_dev, peer_exe_ino;
static uint32 peer_uid;
static Oid db_oid, role_oid;
static Lane lane;
static uint64 deadline, receipt_seq, decision_xid, sequence_ns;
static bool bound, dispatched, accepted, held;
static bool restore_started, restore_finished, restore_ddl;
static uint64 restore_xid;
static unsigned int reset_mask;
static const char *barrier;
static char *queries[MAX_RULES];
static char *writes[MAX_RULES];
static CopyRule copies[MAX_RULES];
static bool metadata_done[MAX_RULES], writes_done[MAX_RULES];
static ImplicitRule implicit[MAX_RULES];
static ArchiveRule archive[MAX_RULES];
static int nqueries, nwrites, nimplicit, narchive, ncopies;
static ArchiveRule *archive_entry;
static const CopyRule *active_copy;
static pg_cryptohash_ctx *copy_hash;
static uint64 copy_bytes;
static Oid archive_table;
static int32 archive_attnum;
static const char *reset_names[] = {
	"statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout", "transaction_timeout"
};

void _PG_init(void);
pg_noreturn static void reject(const char *reason);

static void
reject(const char *reason)
{
	ereport(FATAL, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("B3 fixture guard rejected %s", reason)));
}

static bool
hex_string(const char *s, size_t n)
{
	return s && strlen(s) == n && strspn(s, "0123456789abcdef") == n;
}

static uint64
number(const char *s)
{
	unsigned long long value;
	char *end;

	if (!s || !*s || strspn(s, "0123456789") != strlen(s))
		reject("invalid unsigned supervisor field");
	errno = 0;
	value = strtoull(s, &end, 10);
	if (errno || *end)
		reject("unsigned supervisor field out of range");
	return value;
}

static uint32
number32(const char *s)
{
	uint64 n = number(s);

	if (n > UINT32_MAX)
		reject("32-bit supervisor field out of range");
	return (uint32) n;
}

static int64
signed_number(const char *s)
{
	long long value;
	char *end;
	const char *digits = s[0] == '-' ? s + 1 : s;

	if (!*digits || strspn(digits, "0123456789") != strlen(digits))
		reject("invalid signed supervisor field");
	errno = 0;
	value = strtoll(s, &end, 10);
	if (errno || *end)
		reject("signed supervisor field out of range");
	return value;
}

static bool
parse_bit(const char *s)
{
	if (strcmp(s, "0") == 0)
		return false;
	if (strcmp(s, "1") != 0)
		reject("invalid supervisor flag");
	return true;
}

static bool
serial_kind(const char *s)
{
	if (strcmp(s, "identity") == 0)
		return false;
	if (strcmp(s, "serial") != 0)
		reject("invalid sequence kind");
	return true;
}

static uint64
now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		reject("monotonic clock unavailable");
	return (uint64) ts.tv_sec * UINT64CONST(1000000000) + ts.tv_nsec;
}

static uint64
full_xid(void)
{
	return U64FromFullTransactionId(GetTopFullTransactionIdIfAny());
}

static void
receipt_with_origin(const char *event, const char *detail, uint64 stamp,
					Oid table, int32 attnum, Oid sequence)
{
	char name[96], temp[96], line[768], origin[96] = "";
	int fd, len;

	++receipt_seq;
	snprintf(name, sizeof(name), "receipt.%d.%d." UINT64_FORMAT,
			 peer_pid, MyProcPid, receipt_seq);
	snprintf(temp, sizeof(temp), ".receipt.%d.%d." UINT64_FORMAT,
			 peer_pid, MyProcPid, receipt_seq);
	if (OidIsValid(sequence))
		snprintf(origin, sizeof(origin),
				 ",\"table_oid\":%u,\"column_attnum\":%d,\"sequence_oid\":%u", table, attnum, sequence);
	len = snprintf(line, sizeof(line),
				   "{\"version\":1,\"nonce\":\"%s\",\"candidate\":\"%s\",\"db\":%u,\"role\":%u,"
				   "\"peer_pid\":%d,\"backend_pid\":%d,\"xid\":\"" UINT64_FORMAT "\",\"seq\":" UINT64_FORMAT ","
				   "\"event\":\"%s\",\"detail\":\"%s\",\"monotonic_ns\":\"" UINT64_FORMAT "\",\"deadline_ns\":\"" UINT64_FORMAT "\"%s}\n",
				   run_nonce, candidate, db_oid, role_oid, peer_pid, MyProcPid, full_xid(),
				   receipt_seq, event, detail, stamp, deadline, origin);
	if (len < 0 || len >= sizeof(line))
		reject("receipt exceeds bound");
	fd = openat(receipts_fd, temp, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
	if (fd < 0)
		reject("cannot create receipt");
	if (write(fd, line, len) != len || fchmod(fd, 0640) != 0 || fsync(fd) != 0)
	{
		close(fd);
		reject("cannot persist receipt");
	}
	if (close(fd) != 0 || linkat(receipts_fd, temp, receipts_fd, name, 0) != 0 ||
		unlinkat(receipts_fd, temp, 0) != 0 || fsync(receipts_fd) != 0)
		reject("cannot publish receipt");
}

static void
receipt_at(const char *event, const char *detail, uint64 stamp)
{
	receipt_with_origin(event, detail, stamp, InvalidOid, 0, InvalidOid);
}

static void
receipt(const char *event, const char *detail)
{
	receipt_at(event, detail, now_ns());
}

static void
deny_at(const char *event, const char *detail, uint64 stamp)
{
	receipt_at(event, detail, stamp);
	reject(detail);
}

static bool
scope(void)
{
	return bound && MyDatabaseId == db_oid && GetAuthenticatedUserId() == role_oid &&
		GetSessionUserId() == role_oid && GetUserId() == role_oid;
}

static bool
peer_alive(int fd)
{
	struct pollfd pfd = {.fd = fd, .events = POLLIN};

	return poll(&pfd, 1, 0) == 0;
}

static void
live(const char *denial, const char *detail)
{
	uint64 stamp = now_ns();

	if (!scope() || stamp >= deadline)
		deny_at(denial, detail, stamp);
}

static void
hold(const char *stage)
{
	char release[96];
	struct stat st;
	uint64 stop;

	if (held || strcmp(barrier, stage) != 0)
		return;
	held = true;
	receipt("barrier", stage);
	snprintf(release, sizeof(release), "release.%d.%d." UINT64_FORMAT,
			 peer_pid, MyProcPid, receipt_seq);
	stop = deadline + UINT64CONST(15000000000);
	for (;;)
	{
		uint64 stamp = now_ns();

		if (stamp >= stop)
			reject("unresolved supervisor barrier");
		if (fstatat(grants_fd, release, &st, AT_SYMLINK_NOFOLLOW) == 0)
		{
			if (!S_ISREG(st.st_mode) || st.st_uid != supervisor_uid ||
				(st.st_mode & 022) || st.st_nlink != 1 || st.st_size != 0)
				reject("invalid barrier release");
			stamp = now_ns();
			if (stamp < deadline)
				reject("supervisor released before cutoff");
			if (stamp >= stop)
				reject("unresolved supervisor barrier");
			break;
		}
		else if (errno != ENOENT)
			reject("cannot inspect barrier release");
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 20L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}

static int
fields(char *line, char **parts, int cap)
{
	char *cursor = line;
	char *part;
	int n = 0;

	while ((part = strsep(&cursor, "\t")) != NULL)
	{
		if (n == cap || !*part)
			reject("malformed supervisor grant");
		parts[n++] = part;
	}
	return n;
}

static bool
read_line(FILE *file, char *line)
{
	size_t n;

	if (!fgets(line, MAX_LINE, file))
	{
		if (ferror(file))
			reject("cannot read supervisor grant");
		return false;
	}
	n = strlen(line);
	if (n == 0 || line[n - 1] != '\n')
		reject("unbounded or unterminated supervisor line");
	line[n - 1] = '\0';
	return true;
}

static char *
hex_sql(const char *s)
{
	size_t n = strlen(s);
	char *result;

	if (n < 2 || n % 2 != 0 || !hex_string(s, n))
		reject("invalid supervisor SQL encoding");
	result = MemoryContextAlloc(TopMemoryContext, n / 2 + 1);
	for (size_t i = 0; i < n / 2; ++i)
	{
		char pair[] = {s[2 * i], s[2 * i + 1], '\0'};

		result[i] = (char) strtoul(pair, NULL, 16);
		if (!result[i])
			reject("NUL in supervisor SQL");
	}
	result[n / 2] = '\0';
	return result;
}

static uint64
peer_start(void)
{
	char path[80], statbuf[4096], *cursor, *token;
	int fd;
	ssize_t len;
	uint64 result = 0;

	snprintf(path, sizeof(path), "/proc/%d/stat", peer_pid);
	fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0)
		reject("cannot inspect peer start time");
	len = read(fd, statbuf, sizeof(statbuf) - 1);
	close(fd);
	if (len <= 0 || len == sizeof(statbuf) - 1)
		reject("invalid peer stat record");
	statbuf[len] = '\0';
	cursor = strrchr(statbuf, ')');
	if (!cursor || cursor[1] != ' ')
		reject("invalid peer stat command field");
	cursor += 2;
	for (int field = 3; field <= 22; ++field)
	{
		token = strsep(&cursor, " ");
		if (!token || !*token)
			reject("missing peer stat field");
		if (field == 22)
			result = number(token);
	}
	return result;
}

static void
read_rules(FILE *file, char *line)
{
	char *v[9];

	while (read_line(file, line))
	{
		int n = fields(line, v, lengthof(v));

		if (n == 2 && strcmp(v[0], "query") == 0 && nqueries < MAX_RULES)
			queries[nqueries++] = hex_sql(v[1]);
		else if (lane == RESTORE && nwrites < MAX_RULES &&
				 ((n == 2 && strcmp(v[0], "write") == 0) ||
				  (n == 4 && strcmp(v[0], "copy") == 0)))
		{
			char *stmt = hex_sql(v[1]);

			for (int i = 0; i < nwrites; ++i)
				if (strcmp(writes[i], stmt) == 0)
					reject("duplicate archive write SQL");
			if (n == 4)
			{
				CopyRule *r = &copies[nwrites];

				r->bytes = number(v[2]);
				if (!r->bytes || !hex_string(v[3], sizeof(r->sha256) - 1))
					reject("invalid archive COPY size or SHA-256");
				memcpy(r->sha256, v[3], sizeof(r->sha256));
				++ncopies;
			}
			writes[nwrites++] = stmt;
		}
		else if (n == 5 && lane == SYNTHETIC && strcmp(v[0], "implicit") == 0 && nimplicit < MAX_RULES)
		{
			uint32 attnum = number32(v[2]);
			ImplicitRule *r = &implicit[nimplicit++];

			if (attnum == 0 || attnum > PG_INT16_MAX)
				reject("invalid declared column number");
			r->table = number32(v[1]);
			r->attnum = attnum;
			r->sequence = number32(v[3]);
			r->serial = serial_kind(v[4]);
			if (!OidIsValid(r->table) || !OidIsValid(r->sequence))
				reject("invalid declared relation OID");
		}
		else if (n == 9 && lane == RESTORE && strcmp(v[0], "archive") == 0 && narchive < MAX_RULES)
		{
			ArchiveRule *r = &archive[narchive++];

			for (int i = 1; i <= 4; ++i)
				if (strlen(v[i]) >= NAMEDATALEN ||
					strspn(v[i], "abcdefghijklmnopqrstuvwxyz0123456789_") != strlen(v[i]))
					reject("non-synthetic archive identifier");
			r->schema = MemoryContextStrdup(TopMemoryContext, v[1]);
			r->table = MemoryContextStrdup(TopMemoryContext, v[2]);
			r->column = MemoryContextStrdup(TopMemoryContext, v[3]);
			r->sequence = MemoryContextStrdup(TopMemoryContext, v[4]);
			r->serial = serial_kind(v[5]);
			r->value = signed_number(v[6]);
			r->called = parse_bit(v[7]);
			r->sql = hex_sql(v[8]);
			if (OidIsValid(get_namespace_oid(r->schema, true)))
				reject("restore schema must not exist before admission");
		}
		else
			reject("unexpected supervisor grant rule");
	}
}

static bool
custody_ready(int fd, short events, uint64 stop)
{
	struct pollfd pfd = {.fd = fd, .events = events};
	uint64 stamp = now_ns();

	return stamp < stop &&
		poll(&pfd, 1, (int) ((stop - stamp + 999999) / 1000000)) == 1 &&
		(pfd.revents & (events | POLLHUP)) != 0 && now_ns() < stop;
}

static bool
custody_exchange(int fd, const char *request, size_t length, uint64 stop)
{
	struct sockaddr_un addr = {.sun_family = AF_UNIX};
	struct ucred cred;
	socklen_t size = sizeof(cred);
	char response[516];
	size_t used = 0;

	strcpy(addr.sun_path, control_path);
	if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) != 0 &&
		(errno != EINPROGRESS || !custody_ready(fd, POLLOUT, stop)))
		return false;
	if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &size) != 0 || size != sizeof(cred) ||
		cred.uid != supervisor_uid || cred.pid != supervisor_pid)
		return false;
	while (used < length)
	{
		ssize_t n;

		if (!custody_ready(fd, POLLOUT, stop))
			return false;
		n = send(fd, request + used, length - used, MSG_NOSIGNAL);
		if (n <= 0)
			return false;
		used += n;
	}
	if (shutdown(fd, SHUT_WR) != 0)
		return false;
	used = 0;
	while (used < sizeof(response))
	{
		ssize_t n;

		if (!custody_ready(fd, POLLIN, stop))
			return false;
		n = recv(fd, response + used, sizeof(response) - used, 0);
		if (n < 0)
			return false;
		if (n == 0)
			return used == length + 3 && memcmp(response, "OK\t", 3) == 0 &&
				memcmp(response + 3, request, length) == 0;
		used += n;
	}
	return false;
}

static void
attest_restore(void)
{
	char request[512];
	uint64 stop = Min(deadline, now_ns() + UINT64CONST(1000000000));
	int fd, len;
	bool ok;

	if (!peer_alive(restore_pidfd))
		reject("restore peer terminated");
	len = snprintf(request, sizeof(request),
				   "B3A1\t%s\t%s\t" UINT64_FORMAT "\t%u\t%u\t%d\t%d\t" UINT64_FORMAT
				   "\t%u\t" UINT64_FORMAT "\t" UINT64_FORMAT "\t" UINT64_FORMAT "\n",
				   run_nonce, candidate, GetSystemIdentifier(), db_oid, role_oid, MyProcPid, peer_pid,
				   peer_start_ticks, peer_uid, peer_exe_dev, peer_exe_ino, deadline);
	if (len <= 0 || len >= sizeof(request))
		reject("invalid restore custody request");
	fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
	if (fd < 0)
		reject("cannot contact restore custodian");
	ok = custody_exchange(fd, request, len, stop);
	if (close(fd) != 0 || !ok || !peer_alive(restore_pidfd) || now_ns() >= stop)
		reject("restore custody not attested");
}

static void
claim_restore(void)
{
	char name[128];
	int fd;

	snprintf(name, sizeof(name), "claim.%s.%d." UINT64_FORMAT ".%u.%u",
			 run_nonce, peer_pid, peer_start_ticks, db_oid, role_oid);
	fd = openat(receipts_fd, name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
	if (fd < 0)
		reject("restore grant already used or unavailable");
	if (close(fd) != 0 || fsync(receipts_fd) != 0)
		reject("cannot persist restore grant claim");
}

static void
bind_backend(void)
{
	struct sockaddr_un server_address = {0};
	struct ucred peer;
	struct pollfd peer_fd = {.events = POLLIN};
	struct stat st;
	char filename[96], *line, *v[15];
	socklen_t size;
	int fd;
	FILE *file;
	bool fenced, tail;
	uint64 start, stamp;

	if (!MyProcPort || MyProcPort->raddr.addr.ss_family != AF_UNIX ||
		!MyProcPort->hba || (MyProcPort->hba->auth_method != uaPeer &&
							 MyProcPort->hba->auth_method != uaSCRAM) || superuser())
		reject("authenticated non-superuser Unix fixture backend required");
	size = sizeof(server_address);
	if (getsockname(MyProcPort->sock, (struct sockaddr *) &server_address, &size) != 0 ||
		server_address.sun_family != AF_UNIX ||
		strncmp(server_address.sun_path, socket_dir, strlen(socket_dir)) != 0 ||
		strncmp(server_address.sun_path + strlen(socket_dir), "/.s.PGSQL.", 10) != 0)
		reject("wrong Unix listener");
	size = sizeof(peer);
	if (getsockopt(MyProcPort->sock, SOL_SOCKET, SO_PEERCRED, &peer, &size) != 0 ||
		size != sizeof(peer) || peer.pid <= 0 || peer.uid == geteuid() || peer.uid == supervisor_uid)
		reject("unscoped Unix peer");
	peer_pid = peer.pid;
	size = sizeof(peer_fd.fd);
	if (getsockopt(MyProcPort->sock, SOL_SOCKET, SO_PEERPIDFD, &peer_fd.fd, &size) != 0 ||
		size != sizeof(peer_fd.fd))
		reject("cannot bind live Unix peer pidfd");
	start = peer_start();
	if (!peer_alive(peer_fd.fd))
		reject("Unix peer terminated before admission");

	db_oid = MyDatabaseId;
	role_oid = GetAuthenticatedUserId();
	snprintf(filename, sizeof(filename), "grant.%d.%u.%u", peer_pid, db_oid, role_oid);
	fd = openat(grants_fd, filename, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_uid != supervisor_uid ||
		(st.st_mode & 022) || st.st_nlink != 1 || st.st_size <= 0 || st.st_size > 2097152)
		reject("cannot open immutable supervisor grant");
	file = fdopen(fd, "r");
	line = palloc(MAX_LINE);
	if (!file || !read_line(file, line) || fields(line, v, lengthof(v)) != lengthof(v))
		reject("missing supervisor grant header");
	if (strcmp(v[0], "B3Q1") != 0 || strcmp(v[1], run_nonce) != 0 || strcmp(v[2], candidate) != 0 ||
		number(v[3]) != GetSystemIdentifier() || number(v[4]) != start || number32(v[5]) != peer.uid ||
		(peer_exe_dev = number(v[6])) == 0 || (peer_exe_ino = number(v[7])) == 0 ||
		number32(v[8]) != db_oid || number32(v[9]) != role_oid)
		deny_at("connect", "scope_mismatch", now_ns());
	if (strcmp(v[10], "synthetic") == 0)
		lane = SYNTHETIC;
	else if (strcmp(v[10], "restore") == 0)
		lane = RESTORE;
	else if (strcmp(v[10], "observe") == 0)
		lane = OBSERVE;
	else
		reject("unknown qualification lane");
	deadline = number(v[11]);
	fenced = parse_bit(v[12]);
	tail = parse_bit(v[13]);
	barrier = MemoryContextStrdup(TopMemoryContext, v[14]);
	if (strcmp(barrier, "none") != 0 && strcmp(barrier, "before_deferred") != 0 &&
		strcmp(barrier, "before_decision") != 0 && strcmp(barrier, "after_decision") != 0 &&
		strcmp(barrier, "before_allocator") != 0)
		reject("unknown qualification barrier");
	stamp = now_ns();
	if (deadline > stamp + UINT64CONST(180000000000))
		reject("deadline outside fixture ceiling");
	if (stamp >= deadline)
		deny_at("connect", "expired", stamp);
	if (!fenced)
		deny_at("connect", "prefence_missing", stamp);
	if (lane != OBSERVE && !tail)
		deny_at("connect", "tail_missing", stamp);
	if (lane == OBSERVE && (tail || strcmp(barrier, "none") != 0))
		reject("observer cannot own commit or barrier authority");
	if (*GetConfigOption("session_preload_libraries", false, false) ||
		*GetConfigOption("local_preload_libraries", false, false))
		reject("session preload libraries are forbidden");
	read_rules(file, line);
	fclose(file);
	pfree(line);
	if (nqueries == 0 || (lane == RESTORE && (narchive != 2 || ncopies != narchive)))
		reject("incomplete qualification rules");
	if (lane == RESTORE && (archive[0].serial == archive[1].serial ||
		strcmp(archive[0].schema, archive[1].schema) != 0 ||
		strcmp(archive[0].table, archive[1].table) == 0 ||
		strcmp(archive[0].sequence, archive[1].sequence) == 0))
		reject("restore requires distinct identity and serial objects in one archive schema");
	if (lane == RESTORE)
	{
		restore_pidfd = peer_fd.fd;
		peer_start_ticks = start;
		peer_uid = peer.uid;
		attest_restore();
		claim_restore();
	}
	else if (close(peer_fd.fd) != 0)
		reject("cannot close qualification peer pidfd");
	if (lane == OBSERVE)
	{
		DefaultXactReadOnly = true;
		XactReadOnly = true;
	}
	bound = true;
	stamp = now_ns();
	if (!scope() || stamp >= deadline)
		deny_at("connect", "expired", stamp);
	receipt_at("connect", "admit", stamp);
}

static void
dispatch(int type)
{
	const char *detail;
	uint64 stamp;

	switch (type)
	{
		case EOF:
		case PqMsg_Terminate: return;
		case PqMsg_Query: detail = "query"; break;
		case PqMsg_Parse: detail = "parse"; break;
		case PqMsg_Bind: detail = "bind"; break;
		case PqMsg_Execute: detail = "execute"; break;
		case PqMsg_Describe: detail = "describe"; break;
		case PqMsg_Close: detail = "close"; break;
		case PqMsg_Flush: detail = "flush"; break;
		case PqMsg_Sync: detail = "sync"; break;
		case PqMsg_CopyData: detail = "copy_data"; break;
		case PqMsg_CopyDone: detail = "copy_done"; break;
		case PqMsg_CopyFail: detail = "copy_fail"; break;
		default: detail = "unsupported"; break;
	}
	stamp = now_ns();
	if (!scope() || stamp >= deadline || strcmp(detail, "unsupported") == 0)
		deny_at("dispatch_denied", detail, stamp);
	if (lane == RESTORE && (restore_finished || !peer_alive(restore_pidfd) ||
		(active_copy ? type != PqMsg_CopyData && type != PqMsg_CopyDone :
		 type != PqMsg_Query)))
		deny_at("dispatch_denied", detail, stamp);
	dispatched = true;
	receipt_at("dispatch", detail, stamp);
}

static void
copy_message(int type, const char *data, size_t len)
{
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	char sha256[PG_SHA256_DIGEST_STRING_LENGTH];

	dispatch(type);
	if (!active_copy || !copy_hash || full_xid() != restore_xid)
		reject("COPY frame outside active archive COPY");
	if (type == PqMsg_CopyData)
	{
		/* Hash the whole frame, including bytes discarded after the text EOF marker. */
		if (len > active_copy->bytes - copy_bytes)
			reject("archive COPY payload overrun");
		if (len == 0 || pg_cryptohash_update(copy_hash, (const uint8 *) data, len) < 0)
			reject("invalid archive CopyData");
		copy_bytes += len;
		return;
	}
	if (type != PqMsg_CopyDone || len != 0)
		reject("invalid archive CopyDone");
	if (copy_bytes != active_copy->bytes)
		reject("archive COPY payload underrun");
	if (pg_cryptohash_final(copy_hash, digest, sizeof(digest)) < 0)
		reject("cannot finalize archive COPY SHA-256");
	hex_encode((const char *) digest, sizeof(digest), sha256);
	sha256[sizeof(sha256) - 1] = '\0';
	if (strcmp(sha256, active_copy->sha256) != 0)
		reject("archive COPY SHA-256 mismatch");
	pg_cryptohash_free(copy_hash);
	copy_hash = NULL;
}

static bool
entry_sql(const char *query, int location, int length, const char *expected)
{
	const char *actual = query + Max(0, location);
	const char *end = length > 0 ? actual + length : actual + strlen(actual);

	while (actual < end && isspace((unsigned char) *actual))
		++actual;
	while (end > actual && isspace((unsigned char) end[-1]))
		--end;
	return (size_t) (end - actual) == strlen(expected) && memcmp(actual, expected, end - actual) == 0;
}

static const char secure_search_path[] = "SELECT pg_catalog.set_config('search_path', '', false)";

static int
sql_rule(const char *query, int location, int length, char **rules, int count)
{
	for (int i = 0; i < count; ++i)
		if (entry_sql(query, location, length, rules[i]))
			return i + 1;
	return 0;
}

static bool
restore_setting(const char *query, int location, int length)
{
	static const char *const settings[] = {
		"SET statement_timeout = 0",
		"SET lock_timeout = 0",
		"SET idle_in_transaction_session_timeout = 0",
		"SET transaction_timeout = 0",
		"SET client_encoding = 'UTF8'",
		"SET standard_conforming_strings = on",
		"SET check_function_bodies = false",
		"SET xmloption = content",
		"SET client_min_messages = warning",
		"SET row_security = off",
		"SET default_tablespace = ''",
		"SET default_table_access_method = heap"
	};

	for (int i = 0; i < lengthof(settings); ++i)
		if (entry_sql(query, location, length, settings[i]))
			return true;
	return false;
}

static ArchiveRule *
archive_rule(const char *query, int loc, int len)
{
	for (int i = 0; i < narchive; ++i)
		if (entry_sql(query, loc, len, archive[i].sql))
			return &archive[i];
	return NULL;
}

static void
parsed(const char *query, List *statements)
{
	ListCell *lc;

	if (!bound || lane != RESTORE)
		return;
	foreach(lc, statements)
	{
		RawStmt *raw = lfirst_node(RawStmt, lc);
		int loc = raw->stmt_location, len = raw->stmt_len;

		if (IsA(raw->stmt, TransactionStmt))
		{
			if (list_length(statements) != 1)
				reject("restore transaction control must be dispatched alone");
			continue;
		}
		if (IsA(raw->stmt, VariableSetStmt) && restore_setting(query, loc, len))
			continue;
		if (IsA(raw->stmt, SelectStmt))
		{
			if (entry_sql(query, loc, len, secure_search_path) || archive_rule(query, loc, len))
				continue;
		}
		else if (sql_rule(query, loc, len, queries, nqueries) || sql_rule(query, loc, len, writes, nwrites))
			continue;
		reject("SQL outside attested restore stream");
	}
}

static void
sql(const char *query, bool extended, int params, const Oid *types)
{
	live("dispatch_denied", extended ? "parse" : "query");
	if ((params != 0 && lane != OBSERVE) || params > 8 || (lane == RESTORE && extended))
		reject("unapproved SQL parameters or restore protocol");
	for (int i = 0; i < params; ++i)
		if (OidIsValid(types[i]) && types[i] != TEXTOID && types[i] != XID8OID &&
			types[i] != OIDOID && types[i] != OIDARRAYOID && types[i] != INT8OID &&
			types[i] != INT4OID && types[i] != BOOLOID)
			reject("observer supplied unapproved SQL parameter type");
	if (lane == RESTORE)
		return; /* Validate every raw statement in parsed() before analysis. */
	for (int i = 0; i < nqueries; ++i)
		if (strcmp(query, queries[i]) == 0)
			return;
	reject("SQL outside signed-off synthetic stream");
}

static void
archive_metadata(Node *node)
{
	if (IsA(node, CreateSchemaStmt))
	{
		CreateSchemaStmt *stmt = (CreateSchemaStmt *) node;

		if (stmt->schemaElts || !stmt->schemaname || strcmp(stmt->schemaname, archive[0].schema) != 0 ||
			(stmt->authrole && get_rolespec_oid(stmt->authrole, false) != role_oid))
			reject("archive schema leaves writer scope");
	}
	else if (IsA(node, AlterOwnerStmt))
	{
		AlterOwnerStmt *stmt = (AlterOwnerStmt *) node;

		if (get_rolespec_oid(stmt->newowner, false) != role_oid)
			reject("archive ownership leaves writer scope");
	}
	else if (IsA(node, AlterTableStmt))
	{
		AlterTableStmt *stmt = (AlterTableStmt *) node;
		AlterTableCmd *cmd;

		if (list_length(stmt->cmds) != 1)
			reject("mixed archive ownership and DDL");
		cmd = linitial_node(AlterTableCmd, stmt->cmds);
		if (cmd->subtype != AT_ChangeOwner || get_rolespec_oid(cmd->newowner, false) != role_oid)
			reject("archive ownership leaves writer scope");
	}
	else if (IsA(node, GrantStmt))
	{
		GrantStmt *stmt = (GrantStmt *) node;
		const char *priv;
		AccessPriv *access;

		if (!stmt->is_grant || stmt->grant_option || stmt->grantor || stmt->targtype != ACL_TARGET_OBJECT ||
			list_length(stmt->objects) != 1 || list_length(stmt->grantees) != 1 || list_length(stmt->privileges) != 1 ||
			linitial_node(RoleSpec, stmt->grantees)->roletype != ROLESPEC_CSTRING)
			reject("archive privilege is not a single scoped observer grant");
		if (stmt->objtype == OBJECT_SCHEMA)
			priv = "usage";
		else if (stmt->objtype == OBJECT_TABLE || stmt->objtype == OBJECT_SEQUENCE)
			priv = "select";
		else
			reject("archive privilege applies to an unsupported object");
		access = linitial_node(AccessPriv, stmt->privileges);
		if (access->cols || !access->priv_name || strcmp(access->priv_name, priv) != 0)
			reject("archive privilege is not read-only");
	}
	else
		reject("unsupported archive metadata");
}

static void
restore_transaction(void)
{
	uint64 xid;

	if (!dispatched || !restore_started || restore_finished ||
		!IsTransactionBlock() || GetCurrentTransactionNestLevel() != 1)
		reject("archive effect outside single top-level transaction");
	xid = U64FromFullTransactionId(GetTopFullTransactionId());
	if (restore_xid && restore_xid != xid)
		reject("archive effect changed transaction");
	restore_xid = xid;
}

static ArchiveRule *
archive_copy(CopyStmt *copy)
{
	RangeVar *target = copy->relation;
	Relation rel;
	ArchiveRule *rule = NULL;

	if (!copy->is_from || copy->is_program || copy->filename || copy->query ||
		copy->whereClause || copy->options || !target || !target->schemaname || target->catalogname)
		reject("only attested archive COPY FROM STDIN is permitted");
	for (int i = 0; i < narchive; ++i)
		if (strcmp(target->schemaname, archive[i].schema) == 0 &&
			strcmp(target->relname, archive[i].table) == 0)
			rule = &archive[i];
	if (!rule || rule->copied)
		reject("COPY target is not a fresh attested archive table");
	rel = relation_openrv(target, RowExclusiveLock);
	if (rel->rd_createSubid != GetCurrentSubTransactionId() ||
		rel->rd_rel->relkind != RELKIND_RELATION || rel->rd_rel->relowner != role_oid ||
		rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		reject("COPY target is not a writer-owned table created by this restore");
	relation_close(rel, NoLock);
	attest_restore();
	return rule;
}

static bool
archive_ddl(Node *node)
{
	return IsA(node, CreateStmt) || IsA(node, CreateSeqStmt) || IsA(node, AlterSeqStmt) ||
		IsA(node, AlterTableStmt) || IsA(node, IndexStmt);
}

static void
utility(PlannedStmt *pstmt, const char *query, bool readonly, ProcessUtilityContext context,
		ParamListInfo params, QueryEnvironment *env, DestReceiver *dest, QueryCompletion *completion)
{
	Node *node = pstmt->utilityStmt;
	ArchiveRule *copy_target = NULL;
	const char *effect = NULL;
	int reset = -1, metadata = 0, write_rule = 0;

	if (!bound)
		goto pass;
	live("utility_denied", "expired");
	if (lane == RESTORE && context == PROCESS_UTILITY_SUBCOMMAND && restore_ddl && archive_ddl(node))
		goto pass; /* Only PostgreSQL-generated work from the authorized outer DDL. */
	if (IsA(node, TransactionStmt))
	{
		TransactionStmt *tx = (TransactionStmt *) node;

		if ((tx->kind != TRANS_STMT_BEGIN && tx->kind != TRANS_STMT_START &&
			 tx->kind != TRANS_STMT_COMMIT && tx->kind != TRANS_STMT_ROLLBACK) || tx->chain || tx->options)
			reject("transaction control outside fixture stream");
		if (lane == RESTORE)
		{
			if (tx->kind == TRANS_STMT_BEGIN || tx->kind == TRANS_STMT_START)
			{
				if (restore_started)
					reject("restore grant cannot start another transaction");
				restore_started = true;
			}
			else if (!restore_started || (tx->kind == TRANS_STMT_COMMIT && !restore_xid))
				reject("restore transaction is not established");
		}
	}
	else if (IsA(node, VariableSetStmt) && lane != OBSERVE)
	{
		VariableSetStmt *set = (VariableSetStmt *) node;
		bool allowed = false;

		if (!set->name || set->kind != VAR_SET_VALUE || set->is_local)
			reject("unsupported session setting form");
		for (int i = 0; i < lengthof(reset_names); ++i)
			if (strcmp(set->name, reset_names[i]) == 0)
			{
				allowed = true;
				reset = i;
			}
		if (lane == RESTORE)
			allowed = restore_setting(query, pstmt->stmt_location, pstmt->stmt_len);
		if (!allowed)
			reject("session setting can change fixture scope");
	}
	else if (lane == RESTORE)
	{
		int loc = pstmt->stmt_location, len = pstmt->stmt_len;

		metadata = sql_rule(query, loc, len, queries, nqueries);
		write_rule = sql_rule(query, loc, len, writes, nwrites);
		if ((metadata != 0) == (write_rule != 0) ||
			(metadata && metadata_done[metadata - 1]) || (write_rule && writes_done[write_rule - 1]))
			reject("unapproved or repeated archive write");
		restore_transaction();
		if (metadata)
			archive_metadata(node);
		else if (IsA(node, CopyStmt))
		{
			if (!copies[write_rule - 1].bytes)
				reject("archive COPY has no payload attestation");
			copy_target = archive_copy((CopyStmt *) node);
		}
		else if (copies[write_rule - 1].bytes || !archive_ddl(node))
			reject("unsupported archive DDL");
		effect = copy_target ? "copy" : "ddl";
	}
	else
		reject("utility outside qualification stream");
	if (reset >= 0 && lane == RESTORE)
	{
		restore_transaction();
		if (reset_mask & (1U << reset))
			reject("repeated restore timeout reset");
	}

pass:
	if (effect)
	{
		restore_ddl = !copy_target;
		PG_TRY();
		{
			if (copy_target)
			{
				if (active_copy || copy_hash || copy_bytes != 0)
					reject("overlapping archive COPY");
				active_copy = &copies[write_rule - 1];
				copy_hash = pg_cryptohash_create(PG_SHA256);
				if (!copy_hash || pg_cryptohash_init(copy_hash) < 0)
					reject("cannot initialize archive COPY SHA-256");
			}
			standard_ProcessUtility(pstmt, query, readonly, context, params, env, dest, completion);
			if (copy_target && (copy_hash || active_copy != &copies[write_rule - 1]))
				reject("archive COPY did not receive a verified CopyDone");
		}
		PG_FINALLY();
		{
			restore_ddl = false;
			if (copy_target)
			{
				if (copy_hash)
					pg_cryptohash_free(copy_hash);
				copy_hash = NULL;
				active_copy = NULL;
				copy_bytes = 0;
			}
		}
		PG_END_TRY();
	}
	else
		standard_ProcessUtility(pstmt, query, readonly, context, params, env, dest, completion);
	if (reset >= 0 && strcmp(GetConfigOption(reset_names[reset], false, false), "0") == 0)
	{
		reset_mask |= 1U << reset;
		receipt("timeout_reset", reset_names[reset]);
	}
	if (metadata)
	{
		char detail[16];

		metadata_done[metadata - 1] = true;
		snprintf(detail, sizeof(detail), "%d", metadata);
		receipt("archive_metadata", detail);
	}
	if (copy_target)
	{
		const CopyRule *r = &copies[write_rule - 1];
		char detail[96];

		copy_target->copied = true;
		snprintf(detail, sizeof(detail), "%d:" UINT64_FORMAT ":%s",
				 write_rule, r->bytes, r->sha256);
		receipt("archive_copy", detail);
	}
	if (write_rule)
	{
		char detail[16];

		writes_done[write_rule - 1] = true;
		snprintf(detail, sizeof(detail), "%d", write_rule);
		receipt("archive_write", detail);
	}
	if (effect)
		receipt("write", effect);
}

static void
executor_start(QueryDesc *desc, int flags)
{
	if (bound)
	{
		live("executor_denied", "expired");
		if (lane == OBSERVE && (!XactReadOnly || desc->operation != CMD_SELECT || desc->plannedstmt->hasModifyingCTE ||
								desc->plannedstmt->rowMarks != NIL))
			reject("observer attempted non-read-only executor work");
	}
	standard_ExecutorStart(desc, flags);
}

static bool
archive_state_query(QueryDesc *desc, ArchiveRule *rule)
{
	PlannedStmt *stmt = desc->plannedstmt;
	Plan *plan = stmt->planTree;
	RangeTblEntry *rte;
	TargetEntry *target;
	FuncExpr *fn;
	Const *seq, *value, *called;

	if (desc->operation != CMD_SELECT || stmt->hasModifyingCTE || stmt->rowMarks ||
		stmt->permInfos || stmt->resultRelations || stmt->subplans || list_length(stmt->rtable) != 1 ||
		!IsA(plan, Result) || plan->lefttree || plan->righttree || plan->initPlan || plan->qual ||
		((Result *) plan)->resconstantqual || list_length(plan->targetlist) != 1)
		return false;
	/* PostgreSQL 18.7 inserts this placeholder for a SELECT without FROM. */
	rte = linitial_node(RangeTblEntry, stmt->rtable);
	if (rte->rtekind != RTE_RESULT || rte->relid != InvalidOid || rte->perminfoindex ||
		rte->alias || !rte->eref || strcmp(rte->eref->aliasname, "*RESULT*") != 0 ||
		rte->eref->colnames || rte->inh || rte->lateral || rte->inFromCl || rte->securityQuals)
		return false;
	target = linitial_node(TargetEntry, plan->targetlist);
	if (target->resjunk || !IsA(target->expr, FuncExpr))
		return false;
	fn = (FuncExpr *) target->expr;
	if (fn->funcid != F_SETVAL_REGCLASS_INT8_BOOL || fn->funcretset || list_length(fn->args) != 3 ||
		!IsA(linitial(fn->args), Const) || !IsA(lsecond(fn->args), Const) || !IsA(lthird(fn->args), Const))
		return false;
	seq = linitial_node(Const, fn->args);
	value = lsecond_node(Const, fn->args);
	called = lthird_node(Const, fn->args);
	return seq->consttype == REGCLASSOID && !seq->constisnull &&
		OidIsValid(DatumGetObjectId(seq->constvalue)) &&
		value->consttype == INT8OID && !value->constisnull && DatumGetInt64(value->constvalue) == rule->value &&
		called->consttype == BOOLOID && !called->constisnull && DatumGetBool(called->constvalue) == rule->called;
}

static void
executor_run(QueryDesc *desc, ScanDirection direction, uint64 count)
{
	ArchiveRule *previous = archive_entry;
	bool write_rows = false;

	if (bound)
	{
		live("executor_denied", "expired");
		write_rows = desc->operation != CMD_SELECT || desc->plannedstmt->hasModifyingCTE;
		if (write_rows)
		{
			if (lane != SYNTHETIC || !dispatched)
				reject("executor write outside synthetic stream");
			(void) GetTopFullTransactionId();
		}
		if (lane == RESTORE)
		{
			int loc = desc->plannedstmt->stmt_location, len = desc->plannedstmt->stmt_len;

			archive_entry = archive_rule(desc->sourceText, loc, len);
			if (archive_entry)
			{
				if (!archive_state_query(desc, archive_entry))
					reject("unapproved archive setval plan");
				restore_transaction();
				attest_restore();
			}
			else if (!entry_sql(desc->sourceText, loc, len, secure_search_path))
				reject("unapproved restore SELECT");
		}
	}
	PG_TRY();
	{
		standard_ExecutorRun(desc, direction, count);
	}
	PG_FINALLY();
	{
		archive_entry = previous;
	}
	PG_END_TRY();
	if (write_rows)
		receipt("write", "rows");
}

static void
queued(Oid table, Oid trigger)
{
	if (bound && OidIsValid(table) && OidIsValid(trigger))
	{
		live("deferred_denied", "queued");
		receipt("deferred", "queued");
	}
}

static void
deferred(Oid table, Oid trigger)
{
	const char *detail = OidIsValid(trigger) ? "entry" : "batch";
	uint64 stamp;

	if (!dispatched)
		return;
	if (!OidIsValid(table))
		hold("before_deferred");
	stamp = now_ns();
	if (!scope() || stamp >= deadline)
		deny_at("deferred_denied", detail, stamp);
	receipt_at("deferred", detail, stamp);
}

static void
commit_phase(bool before_decision)
{
	if (!dispatched || !full_xid())
		return;
	if (before_decision)
	{
		if (lane == RESTORE && now_ns() < deadline)
			attest_restore();
		hold("before_decision");
	}
	else
	{
		if (!accepted || decision_xid != full_xid())
			reject("commit record without transaction decision");
		hold("after_decision");
		receipt("commit_record", "ready");
	}
}

static void
transaction(XactEvent event, void *arg)
{
	uint64 stamp, xid;

	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
	{
		accepted = false;
		decision_xid = 0;
		if (lane == RESTORE && (restore_started || (dispatched && event == XACT_EVENT_ABORT)))
		{
			restore_finished = true;
			restore_xid = 0;
			reset_mask = 0;
			memset(metadata_done, 0, sizeof(metadata_done));
			memset(writes_done, 0, sizeof(writes_done));
			for (int i = 0; i < narchive; ++i)
			{
				archive[i].executed = false;
				archive[i].copied = false;
			}
		}
		return;
	}
	if (event != XACT_EVENT_PRE_COMMIT || !dispatched)
		return;
	xid = full_xid();
	if (!xid)
	{
		live("decision", "expired");
		return;
	}
	if (lane == OBSERVE)
		reject("observer has assigned write transaction ID");
	if (lane == RESTORE)
	{
		if (!restore_started || restore_finished || restore_xid != xid || active_copy ||
			!peer_alive(restore_pidfd))
			reject("restore decision lacks live single-transaction custody");
		if (reset_mask != (1U << lengthof(reset_names)) - 1)
			reject("restore did not reset all four session timeouts");
		for (int i = 0; i < narchive; ++i)
		{
			if (!archive[i].copied)
				reject("restore did not verify attested table data");
			if (!archive[i].executed)
				reject("restore did not execute attested sequence state");
		}
		for (int i = 0; i < nqueries; ++i)
			if (!metadata_done[i])
				reject("restore did not execute attested metadata");
		for (int i = 0; i < nwrites; ++i)
			if (!writes_done[i])
				reject("restore did not execute attested writes");
	}
	stamp = now_ns();
	if (!scope() || stamp >= deadline)
		deny_at("decision", "expired", stamp);
	accepted = true;
	decision_xid = xid;
	receipt_at("decision", "admit", stamp);
}

static bool
owned(Oid sequence, Oid table, int32 attnum, bool serial)
{
	Oid actual_table;
	int32 actual_col;
	HeapTuple attr = SearchSysCache2(ATTNUM, ObjectIdGetDatum(table), Int16GetDatum(attnum));
	char identity;

	if (!HeapTupleIsValid(attr))
		return false;
	identity = ((Form_pg_attribute) GETSTRUCT(attr))->attidentity;
	ReleaseSysCache(attr);

	return (serial ? identity == '\0' : identity != '\0') &&
		sequenceIsOwned(sequence, serial ? DEPENDENCY_AUTO : DEPENDENCY_INTERNAL,
						&actual_table, &actual_col) && actual_table == table && actual_col == attnum;
}

static void
allocation(Oid sequence, Oid table, int32 attnum, bool serial, B3SequenceStep step)
{
	const char *origin = OidIsValid(table) ? (serial ? "serial" : "identity") : "direct_nextval";
	const char *denial;
	uint64 stamp;

	if (step == B3_SEQUENCE_DONE)
	{
		receipt_with_origin("allocation", origin, sequence_ns, table, attnum, sequence);
		return;
	}
	if (step == B3_SEQUENCE_ENTRY)
	{
		bool declared = false;

		receipt_with_origin("allocation_attempt", origin, now_ns(), table, attnum, sequence);
		stamp = now_ns();
		denial = "direct_nextval";
		if (!OidIsValid(table))
			goto deny;
		denial = "expired";
		if (!scope() || stamp >= deadline)
			goto deny;
		if (lane == SYNTHETIC && dispatched && !accepted)
			for (int i = 0; i < nimplicit; ++i)
				if (implicit[i].sequence == sequence && implicit[i].table == table &&
					implicit[i].attnum == attnum && implicit[i].serial == serial && owned(sequence, table, attnum, serial))
					declared = true;
		denial = "undeclared_implicit";
		if (!declared)
			goto deny;
		(void) GetTopFullTransactionId();
		hold("before_allocator");
	}
	stamp = now_ns();
	if (scope() && stamp < deadline)
	{
		sequence_ns = stamp;
		return;
	}
	denial = "expired";
deny:
	receipt_with_origin("sequence_denied", denial, stamp, table, attnum, sequence);
	reject(denial);
}

static void
setval(Oid sequence, Relation seqrel, int64 value, bool called, B3SequenceStep step)
{
	ArchiveRule *r = archive_entry;
	char detail[160];
	uint64 stamp;

	if (lane != RESTORE || !r || r->executed || !restore_xid || full_xid() != restore_xid ||
		!IsTransactionBlock() || GetCurrentTransactionNestLevel() != 1 || r->value != value || r->called != called)
		deny_at("sequence_denied", "direct_setval", now_ns());

	if (step == B3_SEQUENCE_DONE)
	{
		r->executed = true;
		snprintf(detail, sizeof(detail), "%s:%u:%d:%u:" INT64_FORMAT ":%d",
				 r->serial ? "serial" : "identity", archive_table, archive_attnum, sequence, value, called);
		receipt_at("archive_state", detail, sequence_ns);
		return;
	}
	stamp = now_ns();
	if (!scope() || stamp >= deadline)
		deny_at("sequence_denied", "expired", stamp);
	if (!peer_alive(restore_pidfd))
		deny_at("sequence_denied", "restore_peer_gone", stamp);
	if (step == B3_SEQUENCE_EFFECT)
	{
		sequence_ns = stamp;
		return;
	}
	if (step == B3_SEQUENCE_ENTRY)
		return;
	if (seqrel->rd_createSubid != GetCurrentSubTransactionId() || seqrel->rd_rel->relowner != role_oid)
		deny_at("sequence_denied", "existing_oid", now_ns());
	{
		Oid ns = get_namespace_oid(r->schema, false);
		Relation tab;

		archive_table = get_relname_relid(r->table, ns);
		if (sequence != get_relname_relid(r->sequence, ns) || !OidIsValid(archive_table))
			reject("archive object does not match attestation");
		tab = relation_open(archive_table, AccessShareLock);
		archive_attnum = get_attnum(archive_table, r->column);
		if (tab->rd_createSubid != GetCurrentSubTransactionId() || tab->rd_rel->relkind != RELKIND_RELATION ||
			tab->rd_rel->relowner != role_oid || archive_attnum <= 0 ||
			!owned(sequence, archive_table, archive_attnum, r->serial))
			reject("archive sequence lacks same-transaction column ownership");
		relation_close(tab, NoLock);
	}
	if (step != B3_SEQUENCE_PROVENANCE)
		reject("invalid archive state transition");
}

static void
sequence_ddl(Relation rel, bool ownership_only)
{
	live("sequence_denied", "expired");
	if (lane != RESTORE || !restore_ddl || !ownership_only || !restore_xid || full_xid() != restore_xid ||
		!IsTransactionBlock() || GetCurrentTransactionNestLevel() != 1 ||
		rel->rd_createSubid != GetCurrentSubTransactionId() || rel->rd_rel->relowner != role_oid)
		deny_at("sequence_denied", "existing_oid", now_ns());
}

static int
private_dir(const char *path, uid_t owner, mode_t allowed_group_bits)
{
	struct stat st;
	char canonical[MAXPGPATH];
	int fd;

	if (!path || path[0] != '/' || strlen(path) >= sizeof(canonical) ||
		!realpath(path, canonical) || strcmp(path, canonical) != 0)
		reject("noncanonical private fixture directory");
	fd = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &st) != 0 || st.st_uid != owner ||
		(st.st_mode & (077 & ~allowed_group_bits)))
		reject("incorrect private directory custody");
	return fd;
}

static B3FixtureHooks hooks = {
	.backend = bind_backend,
	.message = dispatch,
	.copy_message = copy_message,
	.sql = sql,
	.parsed = parsed,
	.deferred = deferred,
	.queued = queued,
	.commit = commit_phase,
	.allocate = allocation,
	.setval = setval,
	.sequence_ddl = sequence_ddl,
};

void
_PG_init(void)
{
	struct stat st;
	int socket_fd;
	uint32 pid;

	if (!process_shared_preload_libraries_in_progress || B3Fixture ||
		ExecutorStart_hook || ExecutorRun_hook || ProcessUtility_hook)
		reject("guard must own fixture-only shared preload hooks");
	supervisor_uid = number32(getenv("B3_GUARD_SUPERVISOR_UID"));
	if (supervisor_uid != 0 || supervisor_uid == geteuid())
		reject("root supervisor and non-root database server required");
	pid = number32(getenv("B3_GUARD_SUPERVISOR_PID"));
	if (pid <= 1 || pid > INT_MAX)
		reject("invalid custodian PID");
	supervisor_pid = pid;
	control_path = getenv("B3_GUARD_CONTROL");
	if (!control_path || control_path[0] != '/' ||
		strlen(control_path) >= sizeof(((struct sockaddr_un *) 0)->sun_path))
		reject("invalid custodian socket path");
	run_nonce = getenv("B3_GUARD_NONCE");
	candidate = getenv("B3_GUARD_CANDIDATE");
	if (!hex_string(run_nonce, 32) || !hex_string(candidate, 64))
		reject("absent trusted fixture identity");
	grants_fd = private_dir(getenv("B3_GUARD_GRANTS"), supervisor_uid, 050);
	receipts_fd = private_dir(getenv("B3_GUARD_RECEIPTS"), geteuid(), 050);
	socket_dir = getenv("B3_GUARD_SOCKET_DIR");
	socket_fd = private_dir(socket_dir, supervisor_uid, 070);
	close(socket_fd);
	netns_inode = number(getenv("B3_GUARD_NETNS_INO"));
	if (strlen(socket_dir) + 17 >= sizeof(((struct sockaddr_un *) 0)->sun_path) ||
		stat("/proc/self/ns/net", &st) != 0 || st.st_ino != netns_inode ||
		*GetConfigOption("listen_addresses", false, false) ||
		strcmp(GetConfigOption("unix_socket_directories", false, false), socket_dir) != 0)
		reject("fixture namespace or listener does not match supervisor");
	if (strcmp(GetConfigOption("max_worker_processes", false, false), "0") != 0 ||
		strcmp(GetConfigOption("autovacuum", false, false), "off") != 0 ||
		strchr(GetConfigOption("shared_preload_libraries", false, false), ','))
		reject("fixture background workers or another preload module are enabled");
	B3Fixture = &hooks;
	ExecutorStart_hook = executor_start;
	ExecutorRun_hook = executor_run;
	ProcessUtility_hook = utility;
	RegisterXactCallback(transaction, NULL);
}
