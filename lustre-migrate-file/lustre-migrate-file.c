/*
 * lustre-migrate-file - move one Lustre file off a set of banned OSTs.
 *
 * One file per invocation. Usable directly from a shell and as the backend of
 * a scheduler that starts many of these in parallel. The program is the sole
 * authority on Lustre state: whoever calls it neither parses layouts nor
 * decides when a mirror may be deleted.
 *
 * Every parameter is validated here, including when the caller has already
 * checked it. A helper that is only safe because its caller was careful is
 * not safe.
 *
 * The implementation follows the sequences that lfs(1) itself uses in Lustre
 * 2.15 (lustre/utils/lfs.c): mirror_extend_layout() for extend,
 * mirror_split() with fdv == fd for delete, lfs_mirror_resync_file() for
 * resync, migrate_nonblock() for the data-version guard around a copy.
 *
 * Deliberately single-threaded: it uses fchdir() to address the parent
 * directory, which is a process-wide property.
 */

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>

#include <lustre/lustreapi.h>

#define PROGRAM_NAME    "lustre-migrate-file"
#define PROGRAM_VERSION "2.5"

/* Version of the JSON record this program writes. A consumer checks it before
 * reading any other field, so a future incompatible change is refused rather
 * than misread. */
#define PROTOCOL_SCHEMA 2

/* Lustre itself allows LUSTRE_MIRROR_COUNT_MAX mirrors (16 in 2.15). Allowing
 * more here would let --target-mirror-count=17 create fifteen mirrors and then
 * fail on the last extend, leaving the file in a state nobody asked for. */
#ifdef LUSTRE_MIRROR_COUNT_MAX
#define MAX_MIRRORS     LUSTRE_MIRROR_COUNT_MAX
#else
#define MAX_MIRRORS     16
#endif
#define MAX_BANNED_OSTS 4096
#define COPY_BUFFER_SZ  (4 * 1024 * 1024)

/* A mirror is deleted one at a time and the layout is re-read in between, so
 * this only bounds a pathological loop, never normal work. */
#define MAX_DELETE_ROUNDS (MAX_MIRRORS * 2)

/* ------------------------------------------------------------------ */
/* options                                                            */
/* ------------------------------------------------------------------ */

struct options {
	const char *root;
	const char *path;
	const char *pool_name;
	const char *expected_fsname;
	uint64_t    stripe_count;
	bool        stripe_count_set;
	int         banned[MAX_BANNED_OSTS];
	/* Kept verbatim so the filesystem name inside a UUID can be checked
	 * once the file's own filesystem is known. Re-scanning argv would miss
	 * the --banned-ost=UUID spelling, which getopt accepts. */
	const char *banned_text[MAX_BANNED_OSTS];
	size_t      banned_count;
	/* Where a NEW mirror may be placed. Lustre has no "choose from this
	 * set" - only an explicit index per stripe, or a pool. With exactly as
	 * many entries as stripes the list is pinned; with more,
	 * pick_allowed_osts() selects that many. */
	int         allowed[MAX_BANNED_OSTS];
	const char *allowed_text[MAX_BANNED_OSTS];
	size_t      allowed_count;
	bool        keep_mirroring;
	long        target_mirror_count;	/* -1: derive */
	long        allocation_attempts;
	/* The caller may pin the file it inspected: if the name now leads to a
	 * different inode, this is not the file the decision was made about. */
	unsigned long long expect_dev;
	unsigned long long expect_ino;
	bool        expect_identity;
	bool        verify;
	/* Readable output is the default, for a person at a terminal. A caller
	 * that parses the result asks for --json. */
	bool        json_output;
	bool        dry_run;
	bool        inspect_only;
	bool        collapse_to_one;
	bool        allow_other_devices;
};

/* ------------------------------------------------------------------ */
/* result accumulation                                                */
/* ------------------------------------------------------------------ */

struct result {
	const char *status;		/* "ok" or "error"; the protocol knows no third */
	const char *stage;		/* where an error happened */
	char        message[1024];
	char        fid[64];
	bool        have_fid;
	int         mirror_count_before;
	/* What a retry should aim at: the recorded goal when the file carries
	 * one, otherwise the observed count. Reported as
	 * initial_mirror_count, which is the question the caller is asking. */
	int         initial_mirror_count;
	int         mirror_count_after;
	int         banned_mirrors_before;
	int         banned_mirrors_after;
	int         mirrors_added;
	int         mirrors_deleted;
	int         resyncs;
	int         allocation_retries;
	int         target_mirror_count;
	bool        changed;
	/* The layout is still WRITE_PENDING or SYNC_PENDING at the end, and no
	 * resync could clear it - the components behind are NOSYNC ones. The
	 * run is finished; this says the file is not fully in sync and never
	 * will be without changing that mirror's configuration. */
	bool        pending_layout;
	int         errno_value;
	bool        transient;
	int         exit_code;
};

static struct result res = {
	.status = "error",
	.stage  = "startup",
	.mirror_count_before = -1,
	.initial_mirror_count = -1,
	.mirror_count_after  = -1,
	.banned_mirrors_before = -1,
	.banned_mirrors_after  = -1,
	.target_mirror_count   = -1,
};

/*
 * Exit status 3 means "try this file again later": the operation collided with
 * something that moves on its own - another writer, a lease held elsewhere, a
 * layout that changed under us. A scheduler can requeue those and must not
 * requeue a permanent failure, which is why they are not both 1.
 */
static bool error_is_transient(int error)
{
	switch (error) {
	case EAGAIN:
	case EBUSY:
	case ESTALE:
	case EINTR:
	case ENOLCK:
	case ETIMEDOUT:
		return true;
	default:
		return false;
	}
}

static void set_error_code(const char *stage, int error, const char *fmt, ...)
{
	va_list ap;

	/* The first error wins: later cleanup failures must not overwrite the
	 * reason the run actually stopped. */
	if (res.exit_code != 0)
		return;

	res.status = "error";
	res.stage = stage;
	res.errno_value = error;
	res.transient = error_is_transient(error);
	res.exit_code = res.transient ? 3 : 1;

	va_start(ap, fmt);
	vsnprintf(res.message, sizeof(res.message), fmt, ap);
	va_end(ap);
}

/*
 * set_error() is for a failure a SYSCALL just reported: errno then says what
 * went wrong and whether a retry can help.
 *
 * It must not be used for a failure this program decided by itself. errno
 * there is whatever the last successful library call happened to leave
 * behind, and error_is_transient() reads it: a leftover EBUSY turned a
 * permanent logical error into exit code 3, and the scheduler kept requeueing
 * a file that could never succeed. The two macros below state the code
 * instead of inheriting one.
 */
#define set_error(stage, ...) set_error_code((stage), errno, __VA_ARGS__)
/* The request, the file or the layout is not what it has to be. Permanent:
 * repeating it changes nothing. */
#define set_logic_error(stage, ...) set_error_code((stage), EINVAL, __VA_ARGS__)
/* The state moved under this program, or a bounded retry can continue where
 * this one stopped. Transient: exit code 3, and the caller requeues. */
#define set_retry_error(stage, ...) set_error_code((stage), EAGAIN, __VA_ARGS__)
/* Out of memory. malloc does not promise errno, so it is named here. */
#define set_memory_error(stage, ...) set_error_code((stage), ENOMEM, __VA_ARGS__)

/*
 * Several llapi functions report the error in their RETURN VALUE as a negative
 * errno and leave errno itself untouched. Printing strerror(-rc) in the
 * message while classifying on errno mixed two unrelated codes: the operator
 * read the right reason and the scheduler acted on a leftover from some
 * earlier call.
 */
static int errno_of_rc(int rc)
{
	return rc < 0 ? -rc : EIO;
}

/* ------------------------------------------------------------------ */
/* output                                                             */
/* ------------------------------------------------------------------ */

static const char b64_alphabet[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/*
 * A pathname is a byte string. It may be any sequence of bytes except NUL and
 * is not necessarily valid UTF-8, so it can neither be printed into JSON nor
 * decoded as text without losing or corrupting it. base64 is the only
 * representation that survives every name the filesystem accepts.
 */
static char *base64_encode(const unsigned char *data, size_t len)
{
	size_t out_len = 4 * ((len + 2) / 3) + 1;
	char *out = malloc(out_len);
	size_t i, o = 0;

	if (!out)
		return NULL;

	for (i = 0; i + 2 < len; i += 3) {
		uint32_t v = ((uint32_t)data[i] << 16) |
			     ((uint32_t)data[i + 1] << 8) |
			     (uint32_t)data[i + 2];
		out[o++] = b64_alphabet[(v >> 18) & 0x3f];
		out[o++] = b64_alphabet[(v >> 12) & 0x3f];
		out[o++] = b64_alphabet[(v >> 6) & 0x3f];
		out[o++] = b64_alphabet[v & 0x3f];
	}
	if (i < len) {
		uint32_t v = (uint32_t)data[i] << 16;
		bool two = (i + 1 < len);

		if (two)
			v |= (uint32_t)data[i + 1] << 8;
		out[o++] = b64_alphabet[(v >> 18) & 0x3f];
		out[o++] = b64_alphabet[(v >> 12) & 0x3f];
		out[o++] = two ? b64_alphabet[(v >> 6) & 0x3f] : '=';
		out[o++] = '=';
	}
	out[o] = '\0';
	return out;
}

static void json_escape(FILE *stream, const char *text)
{
	const unsigned char *p;

	for (p = (const unsigned char *)text; *p; p++) {
		if (*p == '"' || *p == '\\')
			fprintf(stream, "\\%c", *p);
		else if (*p < 0x20 || *p >= 0x7f)
			fprintf(stream, "\\u%04x", *p);
		else
			fputc(*p, stream);
	}
}

static void json_count(const char *name, int value)
{
	if (value < 0)
		printf(",\"%s\":null", name);
	else
		printf(",\"%s\":%d", name, value);
}

static void emit_result(const struct options *opt)
{
	char *path_b64 = NULL;
	char *root_b64 = NULL;

	if (!opt->json_output) {
		if (res.exit_code == 0)
			/* The message carries the whole point of --dry-run and
			 * --inspect-only: what WOULD be changed, or what the
			 * inspection found. Printing only the counters left
			 * the readable form saying nothing in exactly the two
			 * modes that exist to say something. */
			fprintf(stderr,
				"%s: %s: %s (mirrors %d -> %d, banned %d -> %d, "
				"added %d, deleted %d, resyncs %d)%s%s\n",
				PROGRAM_NAME, opt->path ? opt->path : "?",
				res.status,
				res.mirror_count_before, res.mirror_count_after,
				res.banned_mirrors_before,
				res.banned_mirrors_after,
				res.mirrors_added, res.mirrors_deleted,
				res.resyncs,
				res.message[0] ? ": " : "", res.message);
		else
			fprintf(stderr, "%s: %s: %s: %s\n", PROGRAM_NAME,
				opt->path ? opt->path : "?", res.stage,
				res.message);
		return;
	}

	if (opt->path)
		path_b64 = base64_encode((const unsigned char *)opt->path,
					 strlen(opt->path));
	if (opt->root)
		root_b64 = base64_encode((const unsigned char *)opt->root,
					 strlen(opt->root));

	/* schema first: a consumer reads it before anything else. */
	printf("{\"schema\":%d,\"status\":\"", PROTOCOL_SCHEMA);
	json_escape(stdout, res.status);
	printf("\",\"rc\":%d,\"retryable\":%s",
	       res.exit_code, res.transient ? "true" : "false");
	printf(",\"program\":\"%s\",\"program_version\":\"%s\"",
	       PROGRAM_NAME, PROGRAM_VERSION);
	printf(",\"path_b64\":\"%s\"", path_b64 ? path_b64 : "");
	printf(",\"root_b64\":\"%s\"", root_b64 ? root_b64 : "");
	if (res.have_fid) {
		printf(",\"fid\":\"");
		json_escape(stdout, res.fid);
		printf("\"");
	}
	printf(",\"changed\":%s", res.changed ? "true" : "false");
	printf(",\"pending_layout\":%s", res.pending_layout ? "true" : "false");
	/* A count that was never determined is reported as null, not as the
	 * -1 it is held in internally: a consumer adding these up across a
	 * run must not silently sum sentinels. */
	/* initial_mirror_count is what a retry has to aim at, which is NOT
	 * always what is on the file now: after an interrupted run the file
	 * carries a recorded goal, and the count found today is the reduced
	 * one. mirror_count_before stays the raw observation. Reporting the
	 * observation here was what made the recorded goal useless in the
	 * scheduler - it read this field, passed it back as
	 * --target-mirror-count, and the explicit value then overrode the
	 * record it was supposed to carry. */
	json_count("initial_mirror_count", res.initial_mirror_count);
	json_count("mirror_count_before", res.mirror_count_before);
	json_count("mirror_count_after", res.mirror_count_after);
	json_count("banned_mirrors_before", res.banned_mirrors_before);
	json_count("banned_mirrors_after", res.banned_mirrors_after);
	json_count("target_mirror_count", res.target_mirror_count);
	printf(",\"mirrors_added\":%d", res.mirrors_added);
	printf(",\"mirrors_deleted\":%d", res.mirrors_deleted);
	printf(",\"resyncs\":%d", res.resyncs);
	printf(",\"allocation_retries\":%d", res.allocation_retries);
	/* Always, not only on failure: on success it carries "already in the
	 * requested state", and for --dry-run it carries the entire verdict,
	 * which is the only thing that run produces. */
	printf(",\"message\":\"");
	json_escape(stdout, res.message);
	printf("\"");
	if (res.exit_code != 0) {
		printf(",\"stage\":\"");
		json_escape(stdout, res.stage);
		printf("\",\"errno\":%d", res.errno_value);
	}
	printf("}\n");
	fflush(stdout);

	free(path_b64);
	free(root_b64);
}

/* ------------------------------------------------------------------ */
/* safe path resolution                                               */
/* ------------------------------------------------------------------ */

struct resolved {
	int        root_fd;	/* pinned root used for filesystem-name lookup */
	int        parent_fd;	/* O_RDONLY|O_DIRECTORY, fchdir-able */
	int        file_fd;	/* O_RDWR on the regular file */
	/* The final component, kept so a second descriptor with different
	 * open flags can be obtained through the already verified parent
	 * rather than by resolving the whole path again. */
	char       leaf[NAME_MAX + 1];
	struct stat st;
	dev_t      root_dev;
};

static bool component_is_rejected(const char *component, size_t len)
{
	if (len == 0)
		return true;
	if (len == 1 && component[0] == '.')
		return true;
	if (len == 2 && component[0] == '.' && component[1] == '.')
		return true;
	if (len > NAME_MAX)
		return true;
	return false;
}

/*
 * Resolve path componentwise below root.
 *
 * openat2() with RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS would express this in one
 * call, but it needs kernel 5.6 and the AlmaLinux 8 kernel is 4.18, so the
 * componentwise walk is not a fallback here - it is the implementation.
 *
 * What this buys over open(path):
 *   - no cumulative PATH_MAX limit, only NAME_MAX per component
 *   - no symlink is ever traversed, so the result cannot leave root
 *   - ".." can never appear, so neither can an escape by name
 *   - the returned descriptor keeps pointing at the same inode even if the
 *     file is renamed underneath us
 */
static int resolve_path(const struct options *opt, struct resolved *out)
{
	int root_fd = -1, cur_fd = -1, next_fd = -1, leaf_parent_fd = -1;
	int file_fd = -1;
	int access_mode;
	struct stat root_st, st;
	const char *rel;
	const char *p;
	int rc = -1;

	out->root_fd = -1;
	out->parent_fd = -1;
	out->file_fd = -1;

	root_fd = open(opt->root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (root_fd < 0) {
		set_error("resolve", "cannot open root %s: %s", opt->root,
			  strerror(errno));
		goto out;
	}
	if (fstat(root_fd, &root_st) < 0) {
		set_error("resolve", "cannot stat root %s: %s", opt->root,
			  strerror(errno));
		goto out;
	}
	out->root_dev = root_st.st_dev;

	/* The caller may pass the path absolute or relative to root; both must
	 * end up below root, so an absolute path has to carry root as its
	 * prefix and is reduced to the remainder. */
	rel = opt->path;
	if (rel[0] == '/') {
		size_t root_len = strlen(opt->root);

		while (root_len > 1 && opt->root[root_len - 1] == '/')
			root_len--;
		/* root "/" is the one case where the prefix is the separator
		 * itself: demanding another '/' behind it would reject every
		 * absolute path. */
		if (root_len == 1 && opt->root[0] == '/') {
			root_len = 0;
		} else if (strncmp(rel, opt->root, root_len) != 0 ||
			   (rel[root_len] != '/' && rel[root_len] != '\0')) {
			set_logic_error("resolve",
				  "path is not below root: %s", opt->path);
			goto out;
		}
		rel += root_len;
	}
	while (*rel == '/')
		rel++;
	if (*rel == '\0') {
		set_logic_error("resolve", "path is the root itself: %s", opt->path);
		goto out;
	}

	cur_fd = dup(root_fd);
	if (cur_fd < 0) {
		set_error("resolve", "dup: %s", strerror(errno));
		goto out;
	}

	p = rel;
	for (;;) {
		const char *slash = strchr(p, '/');
		size_t len = slash ? (size_t)(slash - p) : strlen(p);
		char component[NAME_MAX + 1];

		if (component_is_rejected(p, len)) {
			set_logic_error("resolve",
				  "rejected path component in %s", opt->path);
			goto out;
		}
		memcpy(component, p, len);
		component[len] = '\0';

		if (!slash || *(slash + 1) == '\0') {
			/* Last component. A trailing slash would mean the
			 * caller asked for a directory, which is never a
			 * migrate source. */
			if (slash) {
				set_logic_error("resolve",
					  "path names a directory: %s",
					  opt->path);
				goto out;
			}
			leaf_parent_fd = cur_fd;
			cur_fd = -1;

			snprintf(out->leaf, sizeof(out->leaf), "%s",
				 component);
			/*
			 * O_RDWR because the layout swap at the end of an
			 * extend needs write access on this descriptor.
			 * O_NOATIME keeps the migration from touching the
			 * access time, O_FILE_ENC lets it work on an encrypted
			 * file without the key - both are what lfs opens a
			 * migration source with.
			 *
			 * --inspect-only and --dry-run change nothing, so they
			 * ask for no more than they need: a read-only file can
			 * be inspected, which it could not when every mode
			 * demanded O_RDWR.
			 *
			 * O_NOATIME needs ownership or CAP_FOWNER, so a
			 * non-root run on somebody else's file falls back to
			 * an open without it rather than failing.
			 */
			access_mode = (opt->inspect_only || opt->dry_run)
				? O_RDONLY : O_RDWR;
			file_fd = openat(leaf_parent_fd, component,
					 access_mode | O_NOATIME | O_FILE_ENC |
					 O_NOFOLLOW | O_CLOEXEC);
			if (file_fd < 0 && errno == EPERM)
				file_fd = openat(leaf_parent_fd, component,
						 access_mode | O_FILE_ENC |
						 O_NOFOLLOW | O_CLOEXEC);
			if (file_fd < 0) {
				set_error("resolve", "cannot open %s: %s",
					  opt->path, strerror(errno));
				goto out;
			}
			break;
		}

		next_fd = openat(cur_fd, component,
				 O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
				 O_CLOEXEC);
		if (next_fd < 0) {
			set_error("resolve",
				  "cannot descend into %s of %s: %s",
				  component, opt->path, strerror(errno));
			goto out;
		}
		if (fstat(next_fd, &st) < 0) {
			set_error("resolve", "cannot stat %s of %s: %s",
				  component, opt->path, strerror(errno));
			close(next_fd);
			next_fd = -1;
			goto out;
		}
		/* A different st_dev below root means a mount point. Crossing
		 * it would leave the filesystem the caller authorised, and the
		 * layout operations below only make sense on one Lustre mount
		 * anyway. */
		if (!opt->allow_other_devices && st.st_dev != root_st.st_dev) {
			set_logic_error("resolve",
				  "%s of %s is on a different filesystem than root",
				  component, opt->path);
			close(next_fd);
			next_fd = -1;
			goto out;
		}
		close(cur_fd);
		cur_fd = next_fd;
		next_fd = -1;
		p = slash + 1;
	}

	if (fstat(file_fd, &st) < 0) {
		set_error("resolve", "cannot stat %s: %s", opt->path,
			  strerror(errno));
		goto out;
	}
	/* O_NOFOLLOW already refused a symlink at the final component; this
	 * catches every other type, and it inspects the object that was
	 * actually opened rather than a name that could have changed. */
	if (!S_ISREG(st.st_mode)) {
		set_logic_error("resolve", "not a regular file: %s", opt->path);
		goto out;
	}
	if (!opt->allow_other_devices && st.st_dev != root_st.st_dev) {
		set_logic_error("resolve", "%s is on a different filesystem than root",
			  opt->path);
		goto out;
	}

	out->root_fd = root_fd;
	root_fd = -1;
	out->parent_fd = leaf_parent_fd;
	leaf_parent_fd = -1;
	out->file_fd = file_fd;
	file_fd = -1;
	out->st = st;
	rc = 0;

out:
	if (root_fd >= 0)
		close(root_fd);
	if (cur_fd >= 0)
		close(cur_fd);
	if (next_fd >= 0)
		close(next_fd);
	if (leaf_parent_fd >= 0)
		close(leaf_parent_fd);
	if (file_fd >= 0)
		close(file_fd);
	return rc;
}

/* ------------------------------------------------------------------ */
/* layout analysis                                                    */
/* ------------------------------------------------------------------ */

struct mirror_state {
	uint32_t id;
	int      components;
	int      init_components;
	int      stale_components;
	/* NOSYNC means the mirror is deliberately allowed to fall behind, and
	 * OFFLINE that it cannot be reached. Either way it is not a copy that
	 * may justify deleting another one. */
	int      unusable_components;
	int      banned_components;
	/* A component whose extent begins below the end of the file holds data
	 * and must be instantiated. One that begins at or after it does not
	 * exist yet and legitimately has no objects. */
	int      missing_data_components;
	/* An uninstantiated component whose pool contains a banned OST: the
	 * OST is not chosen yet, so the pool is the only thing that can be
	 * checked, and it says this mirror may grow onto a banned device. */
	int      pool_banned_components;
	/* Components holding an object on an OST that --allowed-ost does not
	 * name. Only meaningful while that option is in use. */
	int      outside_allowed_components;
	/* Highest offset reachable from 0 without a hole. */
	uint64_t coverage_end;
	bool     has_gap;
	bool     banned;
	bool     usable;
};

/*
 * Can this mirror be relied on as the copy that survives?
 *
 * Being outside the banned OSTs is not enough. Each of the conditions below
 * means "this mirror is allowed to differ from the file, now or later":
 *
 *   stale        it is behind right now
 *   nosync       it is configured never to catch up
 *   offline      it cannot be reached
 *   missing data a component covering file data was never instantiated
 *   gap          the components do not cover 0..EOF without a hole
 *   pool         an uninstantiated component may later land on a banned OST
 *
 * LCME_FL_INIT means "instantiated" and nothing more, and LCME_FL_EXTENSION
 * is by definition never instantiated, so neither may be read as a statement
 * about the data.
 */
static bool mirror_is_usable(const struct mirror_state *m, off_t file_size)
{
	if (m->banned_components > 0)
		return false;
	if (m->stale_components > 0 || m->unusable_components > 0)
		return false;
	if (m->missing_data_components > 0)
		return false;
	if (m->pool_banned_components > 0)
		return false;
	if (m->has_gap)
		return false;
	if (file_size > 0 && m->coverage_end < (uint64_t)file_size)
		return false;
	/* Belt and braces next to the per-component rule above: a non-empty
	 * file whose mirror has nothing instantiated at all holds no data,
	 * whatever the extents claim. The test is cheap and this decision
	 * deletes data. */
	if (file_size > 0 && m->init_components == 0)
		return false;
	return true;
}

struct layout_state {
	struct mirror_state mirrors[MAX_MIRRORS];
	int  mirror_count;
	/* A mirror holding at least one object on a banned OST. */
	int  banned_mirror_count;
	/* Not banned. This is what "never delete the last mirror" counts. */
	int  safe_mirror_count;
	/* Not banned AND a complete, in-sync copy. Only these justify deleting
	 * another mirror. */
	int  usable_mirror_count;
	off_t file_size;
	uint32_t flr_state;
	bool is_flr;
	bool any_stale;
	/* The file as a whole is mid-write or mid-sync, independent of any
	 * single component's STALE flag. */
	bool needs_resync;
};

/* Split into its own function so the decision that governs deletion can be
 * exercised directly, without a Lustre filesystem underneath. */
static void classify_mirrors(struct layout_state *state)
{
	int i;

	state->banned_mirror_count = 0;
	state->safe_mirror_count = 0;
	state->usable_mirror_count = 0;

	for (i = 0; i < state->mirror_count; i++) {
		struct mirror_state *m = &state->mirrors[i];

		m->banned = (m->banned_components > 0);
		m->usable = mirror_is_usable(m, state->file_size);

		if (m->banned) {
			state->banned_mirror_count++;
			continue;
		}
		state->safe_mirror_count++;
		if (m->usable)
			state->usable_mirror_count++;
	}
}

struct scan_context {
	const struct options *opt;
	struct layout_state  *state;
	const char           *fsname;
	int                   error;
	char                  message[512];
	/* Set by a zero-length component, cleared by the extension component
	 * that must follow it. See scan_component() for why a zero-length
	 * component is a normal part of a self-extending layout. */
	bool                  expect_extension;
	uint32_t              expect_extension_mirror;
};

static struct mirror_state *mirror_slot(struct layout_state *state,
					uint32_t id)
{
	int i;

	for (i = 0; i < state->mirror_count; i++)
		if (state->mirrors[i].id == id)
			return &state->mirrors[i];

	if (state->mirror_count >= MAX_MIRRORS)
		return NULL;

	state->mirrors[state->mirror_count].id = id;
	return &state->mirrors[state->mirror_count++];
}

static bool ost_is_banned(const struct options *opt, int ost)
{
	size_t i;

	for (i = 0; i < opt->banned_count; i++)
		if (opt->banned[i] == ost)
			return true;
	return false;
}

static bool ost_is_allowed(const struct options *opt, int ost)
{
	size_t i;

	if (opt->allowed_count == 0)
		return true;	/* no list given: everything is allowed */
	for (i = 0; i < opt->allowed_count; i++)
		if (opt->allowed[i] == ost)
			return true;
	return false;
}

/*
 * Choose which allowed OSTs this mirror is placed on.
 *
 * Lustre only understands an explicit index per stripe; there is no "pick
 * freely among these". When the list is longer than the mirror needs, this
 * program has to choose, and choosing badly matters: taking the first entries
 * every time would put every migrated file of a whole tree on the same two
 * OSTs.
 *
 * The starting point is therefore derived from the file's own identity, so the
 * choice is spread over the tree, and from the attempt number, so a retry
 * after a rejected allocation moves on instead of asking for the same targets
 * again. Same file, same attempt gives the same answer, which is what makes a
 * resumed run predictable.
 */
static void pick_allowed_osts(const struct options *opt, uint64_t identity,
			      long attempt, uint64_t stripes, int *chosen)
{
	size_t start;
	uint64_t i;

	start = (size_t)((identity + (uint64_t)attempt) % opt->allowed_count);
	for (i = 0; i < stripes; i++)
		chosen[i] = opt->allowed[(start + (size_t)i) %
					 opt->allowed_count];
}

/*
 * Does this OST pool contain any of the banned OSTs?
 *
 * Asked for components that are not instantiated: their objects do not exist
 * yet, so the pool is the only thing that says where they could land. An empty
 * pool name means the filesystem-wide default, which by definition can use
 * every OST including the banned ones.
 */
static int pool_contains_banned(const struct options *opt, const char *fsname,
				const char *pool, bool *contains)
{
	size_t i;

	*contains = false;
	if (!pool || pool[0] == '\0') {
		/*
		 * Without a pool there is nothing to inspect: where Lustre
		 * will allocate is not knowable in advance. "Not knowable" is
		 * not the same as "bad", and treating it as bad would refuse
		 * every run that does not name a pool - which is the ordinary
		 * case, because the banned OSTs are supposed to be blocked
		 * server-side already. A mirror that lands on a banned OST
		 * anyway is caught afterwards, per mirror, by
		 * check_new_mirror().
		 */
		return 0;
	}

	for (i = 0; i < opt->banned_count; i++) {
		char ostname[64];
		int rc;

		snprintf(ostname, sizeof(ostname), "%s-OST%04x_UUID", fsname,
			 (unsigned)opt->banned[i]);
		rc = llapi_search_ost(fsname, pool, ostname);
		if (rc < 0)
			return rc;
		if (rc == 1) {
			*contains = true;
			return 0;
		}
	}
	return 0;
}

static int parse_ost(const char *text, const char *expected_fsname,
		     int *index);

/*
 * How many OSTs can a new mirror actually be placed on?
 *
 * Lustre knows nothing about "banned": if no pool restricts the choice it
 * will happily allocate onto an OST this program is trying to evacuate. The
 * result is only rejected afterwards, by check_new_mirror(), and every such
 * attempt has copied the whole file first. With every OST banned that repeats
 * --allocation-attempts times and never succeeds.
 *
 * Counting the candidates in advance turns that into one immediate refusal.
 * The count is exact, not an estimate over index ranges: llapi_get_obd_count()
 * reports how many OSTs exist, and every banned index was verified to exist at
 * startup, so the difference is the number that remain. For a pool the members
 * are enumerated directly.
 *
 * It is a NECESSARY condition, not a sufficient one. An OST that exists and is
 * in the pool can still be inactive, degraded or full, and then the allocation
 * fails anyway - that decision stays with Lustre. This only rules out what
 * cannot work.
 *
 * Returns the number of usable OSTs, or -1 when it cannot be determined; the
 * caller then proceeds and lets Lustre answer.
 */
static int usable_ost_count(const struct options *opt, const char *fsname)
{
	if (opt->allowed_count > 0) {
		/* The caller named the targets, so the answer is a count of
		 * that list rather than a question for the filesystem. */
		size_t i;
		int usable = 0;

		for (i = 0; i < opt->allowed_count; i++)
			if (!ost_is_banned(opt, opt->allowed[i]))
				usable++;
		return usable;
	}

	if (opt->pool_name) {
		char qualified[LOV_MAXPOOLNAME * 2 + 8];
		int obd_count = 0;
		char *buffer = NULL;
		char **members = NULL;
		int members_found, i, usable = 0;
		size_t uuid_size = 64;

		if (llapi_get_obd_count((char *)opt->root, &obd_count, 0) < 0 ||
		    obd_count <= 0)
			return -1;

		buffer = calloc((size_t)obd_count,
				uuid_size + sizeof(*members));
		if (!buffer)
			return -1;
		members = (char **)(buffer + uuid_size * (size_t)obd_count);

		snprintf(qualified, sizeof(qualified), "%s.%s", fsname,
			 opt->pool_name);
		members_found = llapi_get_poolmembers(qualified, members,
						      obd_count, buffer,
						      (int)(uuid_size *
							    (size_t)obd_count));
		if (members_found <= 0) {
			free(buffer);
			return -1;
		}

		for (i = 0; i < members_found; i++) {
			int index;

			if (parse_ost(members[i], NULL, &index) != 0)
				continue;	/* unreadable name: ignore */
			if (!ost_is_banned(opt, index))
				usable++;
		}
		free(buffer);
		return usable;
	}

	{
		int obd_count = 0;

		if (llapi_get_obd_count((char *)opt->root, &obd_count, 0) < 0 ||
		    obd_count <= 0)
			return -1;
		/* Every banned index was confirmed to exist before this point,
		 * so subtracting them is exact even when indexes are sparse. */
		if ((size_t)obd_count < opt->banned_count)
			return 0;
		return obd_count - (int)opt->banned_count;
	}
}

static int scan_component(struct llapi_layout *layout, void *cbdata)
{
	struct scan_context *ctx = cbdata;
	struct mirror_state *mirror;
	uint32_t mirror_id = 0;
	uint32_t flags = 0;
	uint64_t start = 0, end = 0;
	uint64_t stripe_count = 0;
	uint64_t pattern = 0;
	uint64_t i;
	bool is_dom = false;
	bool instantiated = false;
	int rc;

/* After a failing llapi_layout_*_get(): that family returns -1 and sets
 * errno, so errno describes THIS failure. */
#define SCAN_FAIL(...) do {						\
		ctx->error = errno ? errno : EINVAL;			\
		snprintf(ctx->message, sizeof(ctx->message), __VA_ARGS__); \
		return LLAPI_LAYOUT_ITER_STOP;				\
	} while (0)
/* For a rule this program applies itself, where the preceding call
 * SUCCEEDED and errno is a leftover from somewhere else entirely. */
#define SCAN_LOGIC(...) do {						\
		ctx->error = EINVAL;					\
		snprintf(ctx->message, sizeof(ctx->message), __VA_ARGS__); \
		return LLAPI_LAYOUT_ITER_STOP;				\
	} while (0)

	rc = llapi_layout_mirror_id_get(layout, &mirror_id);
	if (rc < 0)
		SCAN_FAIL("cannot read mirror id of a component: %s",
			  strerror(errno));

	rc = llapi_layout_comp_flags_get(layout, &flags);
	if (rc < 0)
		SCAN_FAIL("cannot read flags of component in mirror %u: %s",
			  mirror_id, strerror(errno));

	/* A flag this program does not know is a layout feature it cannot
	 * reason about. Ignoring it would mean judging the mirror on an
	 * incomplete reading, and the judgement decides whether data is
	 * deleted.
	 *
	 * LCME_FL_OFFLINE is named explicitly because Lustre 2.15.8 does not
	 * list it in LCME_KNOWN_FLAGS. Without this, a component carrying it
	 * was rejected here as "unknown" and the code below that treats
	 * OFFLINE as unusable was never reached - conservative, but not what
	 * the comment there describes. This program does know the flag and
	 * does act on it. */
	if (flags & ~(uint32_t)(LCME_KNOWN_FLAGS | LCME_FL_OFFLINE)) {
		ctx->error = ENOTSUP;
		snprintf(ctx->message, sizeof(ctx->message),
			 "component in mirror %u carries unknown layout flags "
			 "%#x (known: %#x); refusing to judge this layout",
			 mirror_id,
			 flags & ~(uint32_t)(LCME_KNOWN_FLAGS | LCME_FL_OFFLINE),
			 (unsigned)(LCME_KNOWN_FLAGS | LCME_FL_OFFLINE));
		return LLAPI_LAYOUT_ITER_STOP;
	}

	rc = llapi_layout_comp_extent_get(layout, &start, &end);
	if (rc < 0)
		SCAN_FAIL("cannot read the extent of a component in mirror "
			  "%u: %s", mirror_id, strerror(errno));
	/*
	 * Only end < start is corrupt. end == start is a normal part of a
	 * self-extending layout: llapi_layout_sanity() REQUIRES an extendable
	 * component to be zero-length before it is instantiated
	 * (LSE_NOT_ZERO_LENGTH_EXTENDABLE) and allows a zero-length component
	 * exactly when an extension component follows it
	 * (LSE_ZERO_LENGTH_NORMAL). Rejecting end == start here refused
	 * layouts that Lustre's own check - run a few lines earlier in
	 * analyse_layout() - had just accepted.
	 */
	if (end < start)
		SCAN_LOGIC("component in mirror %u has an invalid extent "
			  "[%llu, %llu)", mirror_id,
			  (unsigned long long)start, (unsigned long long)end);

	/* The other half of Lustre's rule: a zero-length component that is NOT
	 * followed by an extension component covers nothing and never will, so
	 * the layout does not cover its file. */
	if (ctx->expect_extension) {
		if (!(flags & LCME_FL_EXTENSION) ||
		    mirror_id != ctx->expect_extension_mirror)
			SCAN_LOGIC("zero-length component in mirror %u is not "
				   "followed by an extension component",
				   ctx->expect_extension_mirror);
		ctx->expect_extension = false;
	}
	if (end == start && !(flags & LCME_FL_EXTENSION)) {
		ctx->expect_extension = true;
		ctx->expect_extension_mirror = mirror_id;
	}

	mirror = mirror_slot(ctx->state, mirror_id);
	if (!mirror) {
		ctx->error = E2BIG;
		snprintf(ctx->message, sizeof(ctx->message),
			 "file has more than %d mirrors", MAX_MIRRORS);
		return LLAPI_LAYOUT_ITER_STOP;
	}

	mirror->components++;
	if (flags & LCME_FL_STALE) {
		mirror->stale_components++;
		/*
		 * any_stale drives the resync and the final check, and neither
		 * a NOSYNC nor an OFFLINE component may raise it.
		 *
		 * For NOSYNC that is what Lustre does:
		 * llapi_mirror_find_stale() skips it when no mirror ids are
		 * given, so a resync would copy nothing and the final check
		 * would reject the same mirror again - the run could never
		 * finish. Being behind is what NOSYNC was configured for.
		 *
		 * For OFFLINE the reason is this program's own: 2.15.8 does not
		 * skip it there, so a resync might well try. But an offline
		 * mirror is one that cannot be reached, and treating "cannot be
		 * reached" as "must be caught up before we may finish" turns an
		 * unreachable OST into a permanently failing file. It is
		 * excluded here deliberately, not because Lustre does.
		 *
		 * Either way the mirror counts as unusable below, so it can
		 * never justify deleting another one.
		 */
		if (!(flags & (LCME_FL_NOSYNC | LCME_FL_OFFLINE)))
			ctx->state->any_stale = true;
	}
	if (flags & (LCME_FL_NOSYNC | LCME_FL_OFFLINE))
		mirror->unusable_components++;

	if (llapi_layout_pattern_get(layout, &pattern) < 0)
		SCAN_FAIL("cannot read the pattern of a component in mirror "
			  "%u: %s", mirror_id, strerror(errno));
	is_dom = (pattern == LLAPI_LAYOUT_MDT);

	/* An extension component is a placeholder that is never instantiated
	 * and owns no objects. It is not part of the coverage and says nothing
	 * about completeness. */
	if (flags & LCME_FL_EXTENSION)
		return LLAPI_LAYOUT_ITER_CONT;

	/*
	 * A zero-length component is NOT skipped here. It covers no byte, so
	 * the coverage and missing-data rules below exclude it explicitly, but
	 * the pool it will grow into still decides where its future objects
	 * land, and objects it already owns still sit on real OSTs. Both are
	 * questions the rest of this function answers.
	 */

	/* Coverage: components of one mirror arrive in extent order, so a
	 * component starting beyond the highest offset reached so far leaves a
	 * hole that no mirror data fills. A zero-length component covers
	 * nothing and is neither a hole nor a contribution. */
	if (end > start) {
		if (start > mirror->coverage_end)
			mirror->has_gap = true;
		else if (end > mirror->coverage_end)
			mirror->coverage_end = end;
	}

	/*
	 * LCME_FL_INIT cannot be used to decide whether objects exist.
	 * liblustreapi sets the component flags to 0 for a NON-COMPOSITE
	 * layout (liblustreapi_layout.c: llc_flags = 0 when there is no
	 * lcme entry), so an ordinary striped file - the most common case
	 * there is - carries no INIT flag while its objects are perfectly
	 * real. Asking the layout for an object is the question that has the
	 * same answer in both worlds.
	 *
	 * Data-on-MDT is the other exception: such a component holds data but
	 * owns no OST object at all, so "no object" must not be read as "not
	 * instantiated" there either.
	 */
	instantiated = is_dom || (flags & LCME_FL_INIT);
	if (!instantiated) {
		uint64_t probe = 0;

		errno = 0;
		if (llapi_layout_ost_index_get(layout, 0, &probe) == 0) {
			instantiated = true;
		} else if (errno != EINVAL && errno != 0) {
			SCAN_FAIL("cannot inspect the objects of a component "
				  "in mirror %u: %s", mirror_id,
				  strerror(errno));
		}
	}

	if (!instantiated) {
		/* Not instantiated: it holds no objects. Whether that is
		 * harmless depends on where it sits relative to the data. A
		 * zero-length component covers no byte, so no data can be
		 * missing from it however low its start offset is. */
		if (end > start && ctx->state->file_size > 0 &&
		    start < (uint64_t)ctx->state->file_size)
			mirror->missing_data_components++;

		if (ctx->opt->banned_count > 0) {
			char pool[LOV_MAXPOOLNAME + 1] = "";
			bool banned_pool = false;

			if (llapi_layout_pool_name_get(layout, pool,
						       sizeof(pool)) < 0)
				SCAN_FAIL("cannot read the pool of an "
					  "uninstantiated component in mirror "
					  "%u: %s", mirror_id, strerror(errno));
			rc = pool_contains_banned(ctx->opt, ctx->fsname, pool,
						  &banned_pool);
			if (rc < 0)
				SCAN_FAIL("cannot check pool %s of mirror %u "
					  "against the banned OSTs: %s",
					  pool[0] ? pool : "(none)", mirror_id,
					  strerror(errno));
			if (banned_pool)
				mirror->pool_banned_components++;
		}
		return LLAPI_LAYOUT_ITER_CONT;
	}

	mirror->init_components++;

	/* Data-on-MDT lives on the MDT and owns no OST object, so there is
	 * nothing here that a banned OST could hold. */
	if (is_dom)
		return LLAPI_LAYOUT_ITER_CONT;

	rc = llapi_layout_stripe_count_get(layout, &stripe_count);
	if (rc < 0)
		SCAN_FAIL("cannot read stripe count of component in mirror "
			  "%u: %s", mirror_id, strerror(errno));

	for (i = 0; i < stripe_count; i++) {
		uint64_t ost = 0;

		errno = 0;
		rc = llapi_layout_ost_index_get(layout, i, &ost);
		if (rc < 0) {
			if (errno == EINVAL && i > 0) {
				/* Fewer objects than stripes: the remaining
				 * ones are not allocated. Everything that
				 * exists has been inspected. */
				break;
			}
			/* An instantiated component must be able to name its
			 * objects. Treating a failure here as "no banned OST"
			 * would let the run delete the only usable mirror on
			 * the strength of a layout it could not read. */
			SCAN_FAIL("cannot read OST index %llu of an "
				  "instantiated component in mirror %u: %s",
				  (unsigned long long)i, mirror_id,
				  strerror(errno));
		}
		if (ost == LLAPI_LAYOUT_DEFAULT)
			SCAN_LOGIC("component in mirror %u reports an "
				   "unallocated OST index although it holds "
				   "objects", mirror_id);
		if (!ost_is_allowed(ctx->opt, (int)ost))
			mirror->outside_allowed_components++;
		if (ost_is_banned(ctx->opt, (int)ost)) {
			mirror->banned_components++;
			break;
		}
	}

	return LLAPI_LAYOUT_ITER_CONT;
#undef SCAN_FAIL
#undef SCAN_LOGIC
}

static int analyse_layout(const struct options *opt, int fd,
			  const char *fsname, struct layout_state *state,
			  const char *stage)
{
	struct llapi_layout *layout;
	struct scan_context ctx;
	struct stat st;
	uint32_t flr_flags = 0;
	int rc;

	memset(state, 0, sizeof(*state));
	memset(&ctx, 0, sizeof(ctx));
	ctx.opt = opt;
	ctx.state = state;
	ctx.fsname = fsname;

	/* The size decides how "complete" is to be read: an empty file has no
	 * instantiated component in any mirror, and never will. Demanding one
	 * would make every zero-length file unmigratable. */
	if (fstat(fd, &st) < 0) {
		set_error(stage, "cannot stat the file: %s", strerror(errno));
		return -1;
	}
	state->file_size = st.st_size;

	/* By fd, never by name: between two steps the pathname could point at
	 * a different inode, and every decision below is about this one. */
	/* GET_CHECK makes liblustreapi validate the xattr it decodes instead of
	 * handing back whatever it found. Every decision below rests on this
	 * reading. */
	layout = llapi_layout_get_by_fd(fd, LLAPI_LAYOUT_GET_CHECK);
	if (!layout) {
		set_error(stage, "cannot read layout: %s", strerror(errno));
		return -1;
	}

	/* A layout that does not satisfy Lustre's own consistency rules is not
	 * one this program may draw deletion decisions from. flr=true because
	 * a mirrored file is exactly what is expected here. */
	rc = llapi_layout_sanity(layout, false, true);
	if (rc) {
		/* A positive LSE_* code, not an errno. The layout is wrong in a
		 * way that re-reading it will not change. */
		set_logic_error(stage, "layout fails Lustre's own sanity check "
				"(llapi_layout_sanity rc=%d)", rc);
		llapi_layout_free(layout);
		return -1;
	}

	rc = llapi_layout_flags_get(layout, &flr_flags);
	if (rc < 0) {
		set_error(stage, "cannot read layout flags: %s",
			  strerror(errno));
		llapi_layout_free(layout);
		return -1;
	}
	state->flr_state = flr_flags & LCM_FL_FLR_MASK;
	state->is_flr = state->flr_state != LCM_FL_NONE;
	/* WRITE_PENDING and SYNC_PENDING are statements about the file as a
	 * whole: a write is in flight or a sync was started. Neither is
	 * visible as a per-component STALE flag, and both mean the mirrors are
	 * not known to agree. RDONLY is the settled state. */
	state->needs_resync = (state->flr_state == LCM_FL_WRITE_PENDING ||
			       state->flr_state == LCM_FL_SYNC_PENDING);

	rc = llapi_layout_comp_iterate(layout, scan_component, &ctx);
	if (state->any_stale)
		state->needs_resync = true;
	if (rc < 0 || ctx.error != 0) {
		set_error_code(stage, ctx.error ? ctx.error : EINVAL, "%s",
			       ctx.error ? ctx.message :
			       "cannot iterate layout components");
		llapi_layout_free(layout);
		return -1;
	}
	if (ctx.expect_extension) {
		/* The layout ended on a zero-length component. Nothing follows
		 * that could ever hold its data. */
		set_logic_error(stage, "the last component of mirror %u is "
				"zero-length and no extension component "
				"follows it", ctx.expect_extension_mirror);
		llapi_layout_free(layout);
		return -1;
	}

	/* Cross-check the mirror ids found in the components against the count
	 * the layout header declares. A disagreement means the layout is not
	 * what it says it is, and every decision below rests on this reading -
	 * so it is refused rather than interpreted. A non-composite layout has
	 * no mirror count and reports 0 or 1 for the single implicit mirror. */
	{
		uint16_t declared = 0;

		if (llapi_layout_mirror_count_get(layout, &declared) < 0) {
			set_error(stage, "cannot read the mirror count: %s",
				  strerror(errno));
			llapi_layout_free(layout);
			return -1;
		}
		if (declared > 1 && (int)declared != state->mirror_count) {
			/* One atomic read of one xattr disagrees with itself.
			 * Reading it again returns the same thing. */
			set_logic_error(stage,
					"layout declares %u mirrors but its "
					"components name %d distinct mirror ids",
					(unsigned)declared, state->mirror_count);
			llapi_layout_free(layout);
			return -1;
		}
	}

	classify_mirrors(state);

	llapi_layout_free(layout);
	return 0;
}

/* ------------------------------------------------------------------ */
/* mirror operations                                                  */
/* ------------------------------------------------------------------ */

/*
 * Copy the whole file into the victim, guarded by the data version.
 *
 * This is what lfs migrate_nonblock() does: read the data version before and
 * after the copy and refuse if it moved, because a concurrent writer would
 * otherwise leave the new mirror holding a mixture of two file states. The
 * lease is held by the caller across all of it.
 */
static int copy_into_victim(int fd, int fdv, const struct stat *st)
{
	void *buffer = NULL;
	size_t buffer_size = COPY_BUFFER_SZ;
	size_t page_size = (size_t)sysconf(_SC_PAGESIZE);
	/* llapi_get_data_version() takes __u64 *, which is unsigned long long.
	 * uint64_t is unsigned long on LP64, so passing &uint64_t is an
	 * incompatible pointer type and does not compile cleanly. */
	__u64 dv1 = 0, dv2 = 0;
	off_t pos = 0;
	off_t data_end = 0;
	bool sparse;
	int rc = -1;

	if (llapi_get_data_version(fd, &dv1, LL_DV_RD_FLUSH) < 0) {
		set_error("extend", "cannot read data version: %s",
			  strerror(errno));
		return -1;
	}

	{
		struct llapi_layout *src_layout;

		/* One stripe per write keeps the copy aligned with the way the
		 * new mirror is laid out. */
		src_layout = llapi_layout_get_by_fd(fd, 0);
		if (src_layout) {
			uint64_t stripe_size = 0;

			if (llapi_layout_stripe_size_get(src_layout,
							 &stripe_size) == 0 &&
			    stripe_size >= page_size &&
			    stripe_size <= 64 * 1024 * 1024)
				buffer_size = (size_t)stripe_size;
			llapi_layout_free(src_layout);
		}
	}

	if (page_size == 0)
		page_size = 4096;
	if (posix_memalign(&buffer, page_size, buffer_size) != 0) {
		set_memory_error("extend", "cannot allocate a %zu byte copy buffer",
			  buffer_size);
		return -1;
	}

	/*
	 * Sparse-aware, like lfs migrate_copy_data(). Writing the holes out as
	 * zero blocks would give the new mirror the full logical size: on a
	 * tree of sparse files that is the difference between a migration that
	 * fits and one that ends in ENOSPC.
	 */
	sparse = llapi_file_is_sparse(fd);
	if (sparse && ftruncate(fdv, 0) < 0) {
		set_error("extend", "cannot truncate the new mirror: %s",
			  strerror(errno));
		goto out;
	}

	for (;;) {
		size_t to_read = buffer_size;
		ssize_t got;
		size_t written = 0;

		if (sparse && pos >= data_end) {
			size_t data_size = 0;
			off_t data_off = llapi_data_seek(fd, pos, &data_size);

			if (data_off < 0) {
				/* Not fatal: fall back to a full copy. */
				sparse = false;
				continue;
			}
			if (data_size == 0) {
				/* Trailing hole: give the mirror the length
				 * without writing the zeros. */
				if (ftruncate(fdv, data_off) < 0) {
					set_error("extend",
						  "cannot extend the new "
						  "mirror to %lld: %s",
						  (long long)data_off,
						  strerror(errno));
					goto out;
				}
				break;
			}
			pos = data_off & ~(off_t)(page_size - 1);
			data_end = data_off + (off_t)data_size;
			to_read = (size_t)(((data_end - pos - 1) |
					    (off_t)(page_size - 1)) + 1);
			if (to_read > buffer_size)
				to_read = buffer_size;
		}

		got = pread(fd, buffer, to_read, pos);
		if (got < 0) {
			if (errno == EINTR)
				continue;
			set_error("extend", "read at offset %lld failed: %s",
				  (long long)pos, strerror(errno));
			goto out;
		}
		if (got == 0)
			break;

		while (written < (size_t)got) {
			ssize_t put = pwrite(fdv, (char *)buffer + written,
					     (size_t)got - written,
					     pos + (off_t)written);

			if (put < 0) {
				if (errno == EINTR)
					continue;
				set_error("extend",
					  "write at offset %lld failed: %s",
					  (long long)(pos + (off_t)written),
					  strerror(errno));
				goto out;
			}
			if (put == 0) {
				/* Not an error code, but no progress either;
				 * looping on it would never terminate. */
				set_error_code("extend", EIO,
					  "write at offset %lld made no "
					  "progress",
					  (long long)(pos + (off_t)written));
				goto out;
			}
			written += (size_t)put;
		}
		pos += got;

		/* Losing the lease mid-copy means somebody else may already be
		 * changing the file; the merge below would then publish a
		 * mirror that never matched any version of it. */
		if (llapi_lease_check(fd) != LL_LEASE_RDLCK) {
			/* Somebody opened the file for writing. Nothing is
			 * wrong with this file or this program - the next
			 * attempt can succeed, so it must be reported as
			 * retryable and not as a permanent failure. */
			set_error_code("extend", EAGAIN,
				       "lost the lease during the copy: the "
				       "file was accessed for writing");
			goto out;
		}
	}

	if (llapi_get_data_version(fd, &dv2, LL_DV_RD_FLUSH) < 0) {
		set_error("extend", "cannot re-read data version: %s",
			  strerror(errno));
		goto out;
	}
	if (dv1 != dv2) {
		set_error_code("extend", EAGAIN,
			       "file changed while the new mirror was written "
			       "(data version %llu -> %llu); nothing was "
			       "merged",
			       (unsigned long long)dv1, (unsigned long long)dv2);
		goto out;
	}

	/*
	 * Get the data out of the client cache before the layouts are merged.
	 * Without this the new mirror can exist in the layout while its blocks
	 * are still only in this client's memory; deleting the old mirror
	 * right afterwards would leave a crash or a delayed writeback error
	 * destroying the only complete copy. lfs calls fsync here for the same
	 * reason.
	 */
	if (fsync(fdv) < 0) {
		set_error("extend", "cannot flush the new mirror: %s",
			  strerror(errno));
		goto out;
	}

	/* The victim becomes a mirror of this file, so it must carry the
	 * file's timestamps rather than the time of the copy. */
	{
		struct timespec times[2];

		times[0] = st->st_atim;
		times[1] = st->st_mtim;
		if (futimens(fdv, times) < 0) {
			set_error("extend",
				  "cannot set timestamps on the new mirror: %s",
				  strerror(errno));
			goto out;
		}
	}

	rc = 0;
out:
	free(buffer);
	return rc;
}

/*
 * One line describing a layout, for an error message.
 *
 * Not a substitute for lfs getstripe: it names the things a merge can object
 * to - how many components and mirrors, whether they are instantiated, the
 * stripe geometry and the OSTs actually in use.
 */
static void describe_layout(int fd, char *out, size_t out_size)
{
	struct llapi_layout *layout;
	uint16_t mirrors = 0;
	int components = 0, init = 0;
	uint64_t stripe_count = 0, stripe_size = 0;
	char osts[96] = "";
	char pool[LOV_MAXPOOLNAME + 1] = "";
	uint32_t flr = 0;
	int rc;

	snprintf(out, out_size, "unreadable");

	layout = llapi_layout_get_by_fd(fd, 0);
	if (!layout)
		return;

	if (llapi_layout_flags_get(layout, &flr) < 0)
		flr = 0;
	if (llapi_layout_mirror_count_get(layout, &mirrors) < 0)
		mirrors = 0;

	rc = llapi_layout_comp_use(layout, LLAPI_LAYOUT_COMP_USE_FIRST);
	while (rc == 0) {
		uint32_t comp_flags = 0;

		components++;
		if (llapi_layout_comp_flags_get(layout, &comp_flags) == 0 &&
		    (comp_flags & LCME_FL_INIT))
			init++;
		if (components == 1) {
			(void)llapi_layout_stripe_count_get(layout,
							    &stripe_count);
			(void)llapi_layout_stripe_size_get(layout,
							   &stripe_size);
			(void)llapi_layout_pool_name_get(layout, pool,
							 sizeof(pool));
			{
				uint64_t i;
				size_t used = 0;

				for (i = 0; i < stripe_count &&
				     used + 8 < sizeof(osts); i++) {
					uint64_t ost = 0;

					if (llapi_layout_ost_index_get(layout, i,
								       &ost) < 0)
						break;
					used += (size_t)snprintf(
						osts + used,
						sizeof(osts) - used,
						"%s%llu", used ? "," : "",
						(unsigned long long)ost);
				}
			}
		}
		rc = llapi_layout_comp_use(layout, LLAPI_LAYOUT_COMP_USE_NEXT);
	}

	snprintf(out, out_size,
		 "flr_state=%#x mirrors=%u components=%d init=%d "
		 "stripe_count=%llu stripe_size=%llu pool=%s osts=[%s]",
		 (unsigned)(flr & LCM_FL_FLR_MASK), (unsigned)mirrors,
		 components, init, (unsigned long long)stripe_count,
		 (unsigned long long)stripe_size, pool[0] ? pool : "-",
		 osts[0] ? osts : "-");

	llapi_layout_free(layout);
}

static int random_tag(void)
{
	unsigned int value = 0;
	int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);

	if (fd >= 0) {
		if (read(fd, &value, sizeof(value)) != (ssize_t)sizeof(value))
			value = 0;
		close(fd);
	}
	if (value == 0)
		value = (unsigned int)getpid() ^ (unsigned int)time(NULL);
	return (int)(value & 0xffff);
}

/*
 * Append exactly one mirror with the configured layout.
 *
 * Same sequence as lfs mirror_extend_layout(): create a volatile victim file
 * carrying the target layout, copy the data under a read lease, then atomically
 * release the lease and merge the victim's layout into the file. There is no
 * llapi call that does this in one step in 2.15.
 */
static int extend_one_mirror(const struct options *opt, struct resolved *r,
			     uint64_t identity, long attempt_seed)
{
	struct llapi_layout *layout = NULL;
	struct ll_ioc_lease *data = NULL;
	char volatile_name[NAME_MAX + 1];
	struct stat source_st, victim_st;
	int fdv = -1;
	int saved_cwd = -1;
	int mdt_index = -1;
	int attempt;
	bool lease_held = false;
	bool in_parent = false;
	int rc = -1;
	int ret;

	layout = llapi_layout_alloc();
	if (!layout) {
		set_error("extend", "cannot allocate a layout: %s",
			  strerror(errno));
		goto out;
	}
	if (opt->stripe_count_set &&
	    llapi_layout_stripe_count_set(layout, opt->stripe_count) < 0) {
		set_error("extend", "cannot set stripe count %llu: %s",
			  (unsigned long long)opt->stripe_count,
			  strerror(errno));
		goto out;
	}
	if (opt->pool_name &&
	    llapi_layout_pool_name_set(layout, opt->pool_name) < 0) {
		set_error("extend", "cannot set pool %s: %s", opt->pool_name,
			  strerror(errno));
		goto out;
	}

	/*
	 * The victim's layout has to be a COMPOSITE one, or the merge is
	 * refused: lod_declare_layout_merge() checks
	 *
	 *     if (le32_to_cpu(merge_lcm->lcm_magic) != LOV_MAGIC_COMP_V1)
	 *             RETURN(-EINVAL);
	 *
	 * A layout from llapi_layout_alloc() is written out as a plain
	 * LOV_MAGIC_V1/V3 unless something marks it composite, and setting the
	 * stripe count or the pool does not. Stating the extent does: it is
	 * already 0..EOF by default, so this changes no geometry - it changes
	 * how the layout is serialised, which is what the server reads.
	 */
	if (llapi_layout_comp_extent_set(layout, 0, LUSTRE_EOF) < 0) {
		set_error("extend",
			  "cannot give the new mirror a composite layout: %s",
			  strerror(errno));
		goto out;
	}

	/*
	 * Name the OSTs explicitly when the caller restricted them. Lustre
	 * takes one index per stripe here, exactly as "lfs setstripe -o" does,
	 * and the number of indexes has to equal the stripe count.
	 */
	if (opt->allowed_count > 0) {
		uint64_t stripes = opt->stripe_count_set ? opt->stripe_count : 1;
		int chosen[MAX_BANNED_OSTS];
		uint64_t i;

		pick_allowed_osts(opt, identity, attempt_seed, stripes, chosen);
		for (i = 0; i < stripes; i++) {
			if (llapi_layout_ost_index_set(layout, (int)i,
						       (uint64_t)chosen[i]) < 0) {
				set_error("extend",
					  "cannot place stripe %llu on OST %d: "
					  "%s", (unsigned long long)i,
					  chosen[i], strerror(errno));
				goto out;
			}
		}
	}

	/* The new mirror is placed on the MDT that already holds the file, so
	 * the metadata of the two mirrors does not end up split across
	 * servers. */
	if (llapi_file_fget_mdtidx(r->file_fd, &mdt_index) < 0) {
		set_error("extend", "cannot read the MDT index: %s",
			  strerror(errno));
		goto out;
	}

	/* llapi_layout_file_open() takes a NAME. The parent directory is
	 * already open and verified, so changing into it reduces that name to
	 * a single component: no second path resolution, and no PATH_MAX. The
	 * program is single-threaded precisely so this is safe. */
	saved_cwd = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (saved_cwd < 0) {
		set_error("extend", "cannot remember the working directory: %s",
			  strerror(errno));
		goto out;
	}
	if (fchdir(r->parent_fd) < 0) {
		set_error("extend",
			  "cannot change into the parent directory: %s",
			  strerror(errno));
		goto out;
	}
	in_parent = true;

	/* A volatile file is unlinked the moment it is created, so a crash
	 * anywhere below leaves no debris in the user's directory. The name is
	 * the one the servers recognise; see LUSTRE_VOLATILE_HDR.
	 *
	 * The tag is random, so two concurrent runs in the same directory can
	 * collide. EEXIST therefore draws a new tag rather than failing the
	 * whole migration, exactly as lfs migrate_open_files() does. */
	for (attempt = 0; attempt < 16; attempt++) {
		ret = snprintf(volatile_name, sizeof(volatile_name),
			       "%s:%.4X:%.4X:fd=%.2d", LUSTRE_VOLATILE_HDR,
			       mdt_index, random_tag(), r->file_fd);
		if (ret < 0 || (size_t)ret >= sizeof(volatile_name)) {
			set_logic_error("extend",
				  "cannot build the volatile file name");
			goto out;
		}

		/* O_FILE_ENC lets this work on an encrypted file without the
		 * key, which is what lfs does for migrate and resync. */
		fdv = llapi_layout_file_open(volatile_name,
					     O_WRONLY | O_CREAT | O_EXCL |
					     O_NOFOLLOW | O_FILE_ENC,
					     S_IRUSR | S_IWUSR, layout);
		if (fdv >= 0 || errno != EEXIST)
			break;
	}
	if (fdv < 0) {
		set_error("extend",
			  "cannot create the new mirror (stripe_count=%llu, "
			  "pool=%s): %s",
			  (unsigned long long)opt->stripe_count,
			  opt->pool_name ? opt->pool_name : "(none)",
			  strerror(errno));
		goto out;
	}

	/* On an MDT that does not implement volatile files the name is a real
	 * directory entry. Removing it keeps the guarantee that nothing is
	 * left behind; on a working MDT it simply fails and that is fine. */
	(void)unlink(volatile_name);

	if (fchdir(saved_cwd) < 0) {
		set_error("extend", "cannot restore the working directory: %s",
			  strerror(errno));
		goto out;
	}
	in_parent = false;
	close(saved_cwd);
	saved_cwd = -1;

	/*
	 * The volatile file belongs to whoever ran this program, usually root.
	 * The layout swap checks that source and victim have the same owner,
	 * and the resulting mirror has to account against the same quota as
	 * the file it belongs to. Done before the lease, in the same order lfs
	 * does it.
	 */
	if (fstat(r->file_fd, &source_st) < 0) {
		set_error("extend", "cannot stat the file: %s",
			  strerror(errno));
		goto out;
	}
	if (fstat(fdv, &victim_st) < 0) {
		set_error("extend", "cannot stat the new mirror: %s",
			  strerror(errno));
		goto out;
	}
	if (source_st.st_uid != victim_st.st_uid ||
	    source_st.st_gid != victim_st.st_gid) {
		if (fchown(fdv, source_st.st_uid, source_st.st_gid) < 0) {
			set_error("extend",
				  "cannot give the new mirror the owner of the "
				  "file (uid %u, gid %u): %s",
				  (unsigned)source_st.st_uid,
				  (unsigned)source_st.st_gid, strerror(errno));
			goto out;
		}
	}

	if (llapi_lease_acquire(r->file_fd, LL_LEASE_RDLCK) < 0) {
		set_error("extend", "cannot acquire a read lease: %s",
			  strerror(errno));
		goto out;
	}
	lease_held = true;

	/* Read the size and timestamps again under the lease: they may have
	 * moved since the check above, and the copy is sized and stamped from
	 * them. */
	if (fstat(r->file_fd, &source_st) < 0) {
		set_error("extend", "cannot stat the file under the lease: %s",
			  strerror(errno));
		goto out;
	}

	if (copy_into_victim(r->file_fd, fdv, &source_st) < 0)
		goto out;

	/* The copy read the whole file, which moves its access time. Put the
	 * original values back before the layouts are merged. */
	{
		struct timespec times[2];

		times[0] = source_st.st_atim;
		times[1] = source_st.st_mtim;
		if (futimens(r->file_fd, times) < 0) {
			set_error("extend",
				  "cannot restore the timestamps of the file: "
				  "%s", strerror(errno));
			goto out;
		}
	}

	/* Release the lease and merge the victim's layout into the file in one
	 * operation. Doing it in two steps would open a window in which the
	 * file has no lease and the merge could apply to a file somebody else
	 * has since changed. */
	data = calloc(1, offsetof(struct ll_ioc_lease, lil_ids[1]));
	if (!data) {
		set_memory_error("extend", "memory allocation failed");
		goto out;
	}
	data->lil_mode = LL_LEASE_UNLCK;
	data->lil_flags = LL_LEASE_LAYOUT_MERGE;
	data->lil_count = 1;
	data->lil_ids[0] = (__u32)fdv;

	ret = llapi_lease_set(r->file_fd, data);
	if (ret < 0) {
		/*
		 * The merge is the one step whose refusal says nothing about
		 * which of the two layouts the server objected to. Describing
		 * both here is the difference between a usable report and
		 * another run.
		 */
		char victim[256] = "unreadable";
		char source[256] = "unreadable";

		describe_layout(fdv, victim, sizeof(victim));
		describe_layout(r->file_fd, source, sizeof(source));
		set_error_code("extend", errno_of_rc(ret),
			       "cannot merge the new mirror: %s; source "
			       "layout: %s; new mirror layout: %s",
			       strerror(-ret), source, victim);
		goto out;
	}
	if (ret == 0) {
		set_error_code("extend", EAGAIN,
			       "lost the lease before the merge: the file was "
			       "accessed for writing");
		goto out;
	}
	lease_held = false;	/* llapi_lease_set released it */

	res.mirrors_added++;
	res.changed = true;
	rc = 0;

out:
	if (lease_held)
		llapi_lease_release(r->file_fd);
	free(data);
	if (fdv >= 0)
		close(fdv);
	if (layout)
		llapi_layout_free(layout);
	if (saved_cwd >= 0) {
		if (in_parent && fchdir(saved_cwd) < 0) {
			/* Best effort: the process is about to exit, and the
			 * caller's working directory is not ours to keep. */
		}
		close(saved_cwd);
	}
	return rc;
}

/*
 * Why a mirror is being deleted. The re-analysis under the write lease has to
 * re-establish the reason, and the reason is not the same question in all
 * three cases:
 *
 *   DELETE_BANNED          it holds objects on a banned OST. If it no longer
 *                          does, the layout moved and this deletion is not
 *                          the one to carry out.
 *   DELETE_ROLLBACK_ADDED  this program created it moments ago and rejected
 *                          it - for a banned OST, for an OST outside
 *                          --allowed-ost, or for being unusable. Requiring it
 *                          to be banned would make the other two rejections
 *                          impossible to undo, and the mirror nobody wants
 *                          would stay.
 *   DELETE_SURPLUS         the file has more mirrors than the caller asked
 *                          for. A surplus mirror is by definition NOT banned
 *                          - the banned ones were removed first - so the
 *                          banned test would reject every single one, which
 *                          is what made --collapse-to-one and any reduction
 *                          to --target-mirror-count silently impossible.
 *
 * What does not depend on the reason: a deletion may never remove the last
 * usable copy. That is checked under the lease in every case.
 */
enum delete_reason {
	DELETE_BANNED,
	DELETE_ROLLBACK_ADDED,
	DELETE_SURPLUS,
};

static const char *delete_reason_name(enum delete_reason reason)
{
	switch (reason) {
	case DELETE_BANNED:
		return "banned";
	case DELETE_ROLLBACK_ADDED:
		return "rollback of the mirror just added";
	case DELETE_SURPLUS:
		return "surplus";
	}
	return "unknown";
}

/*
 * Delete exactly one mirror, addressed by mirror id.
 *
 * lfs mirror delete is mirror_split() with the victim descriptor set to the
 * file's own descriptor, which discards the split-off mirror instead of giving
 * it a name. That is purely descriptor-based: no pathname is resolved, so the
 * mirror that is removed is provably a mirror of the file we opened.
 *
 * surplus_target is the mirror count the caller is aiming at; it is read only
 * for DELETE_SURPLUS and ignored otherwise.
 */
static int delete_one_mirror(const struct options *opt, struct resolved *r,
			     const char *fsname, uint32_t mirror_id,
			     enum delete_reason reason, long surplus_target)
{
	struct layout_state under_lease;
	struct ll_ioc_lease *data;
	bool lease_held = false;
	int i, rc;
	int other_usable = 0;
	const struct mirror_state *target = NULL;

	if (llapi_lease_acquire(r->file_fd, LL_LEASE_WRLCK) < 0) {
		set_error("delete", "cannot acquire a write lease: %s",
			  strerror(errno));
		return -1;
	}
	lease_held = true;

	/*
	 * Everything decided before this point was decided without a lease, so
	 * the layout could have changed since. Reading and judging it again
	 * HERE, while the write lease is held, is what closes that window: the
	 * same lease is then handed back together with the split, so no state
	 * can slip in between the decision and the deletion.
	 *
	 * The server refuses a split that would leave only stale mirrors, but
	 * it knows nothing about banned OSTs, nosync, offline or pools. Those
	 * are this program's criteria and have to be checked here.
	 */
	if (analyse_layout(opt, r->file_fd, fsname, &under_lease,
			   "delete") < 0)
		goto fail;

	for (i = 0; i < under_lease.mirror_count; i++) {
		const struct mirror_state *m = &under_lease.mirrors[i];

		if (m->id == mirror_id)
			target = m;
		else if (m->usable)
			other_usable++;
	}

	if (!target) {
		set_error_code("delete", EAGAIN,
			       "mirror %u is gone from the layout; not "
			       "deleting anything", mirror_id);
		goto fail;
	}

	/* The one rule that holds for every reason. */
	if (other_usable < 1) {
		set_error_code("delete", EAGAIN,
			       "deleting mirror %u (%s) would leave no usable "
			       "copy (mirrors=%d, usable=%d)", mirror_id,
			       delete_reason_name(reason),
			       under_lease.mirror_count,
			       under_lease.usable_mirror_count);
		goto fail;
	}

	switch (reason) {
	case DELETE_BANNED:
		if (!target->banned) {
			set_error_code("delete", EAGAIN,
				       "mirror %u no longer holds objects on a "
				       "banned OST; refusing to delete it",
				       mirror_id);
			goto fail;
		}
		break;
	case DELETE_ROLLBACK_ADDED:
		/* Nothing further. The caller identified this mirror from the
		 * layout it read immediately after creating it, and the only
		 * thing that could make the rollback wrong - taking away the
		 * last copy - was ruled out above. */
		break;
	case DELETE_SURPLUS:
		if (under_lease.mirror_count <= (int)surplus_target) {
			set_error_code("delete", EAGAIN,
				       "the file has %d mirrors and %ld were "
				       "requested; nothing is surplus",
				       under_lease.mirror_count,
				       surplus_target);
			goto fail;
		}
		break;
	}
	if (llapi_lease_check(r->file_fd) != LL_LEASE_WRLCK) {
		set_error_code("delete", EAGAIN,
			       "lost the write lease before the split: the "
			       "file was accessed meanwhile");
		goto fail;
	}

	data = malloc(offsetof(struct ll_ioc_lease, lil_ids[2]));
	if (!data) {
		set_memory_error("delete", "memory allocation failed");
		goto fail;
	}
	data->lil_mode = LL_LEASE_UNLCK;
	data->lil_flags = LL_LEASE_LAYOUT_SPLIT;
	data->lil_count = 2;
	data->lil_ids[0] = (__u32)r->file_fd;	/* same fd: discard, not keep */
	data->lil_ids[1] = mirror_id;

	rc = llapi_lease_set(r->file_fd, data);
	free(data);

	if (rc < 0) {
		set_error_code("delete", errno_of_rc(rc),
			       "cannot delete mirror %u: %s", mirror_id,
			       strerror(-rc));
		goto fail;
	}
	if (rc == 0) {
		set_error_code("delete", EAGAIN,
			       "lost the lease while deleting mirror %u",
			       mirror_id);
		goto fail;
	}
	/* llapi_lease_set released the lease together with the split. */

	res.mirrors_deleted++;
	res.changed = true;
	return 0;

fail:
	if (lease_held)
		llapi_lease_release(r->file_fd);
	return -1;
}

/*
 * Bring every stale mirror up to date.
 *
 * lfs_mirror_resync_file(): find the stale components, take a write lease with
 * LL_LEASE_RESYNC, copy, then release it with LL_LEASE_RESYNC_DONE listing the
 * components that actually synced. The lease must be released even when the
 * copy failed, otherwise the file stays locked.
 */
/*
 * Open a second descriptor on the same inode with O_DIRECT.
 *
 * lfs opens the file with O_DIRECT for mirror resync, and the 2.15 client
 * refuses the mirror-select ioctl on a descriptor without it. The name is
 * re-resolved through the parent descriptor that was already verified, and
 * the resulting inode is compared with the one this program is working on -
 * so a name swapped in meanwhile is detected rather than silently migrated.
 */
static int open_direct_twin(struct resolved *r)
{
	struct stat direct_st;
	int fd;

	fd = openat(r->parent_fd, r->leaf,
		    O_RDWR | O_DIRECT | O_NOFOLLOW | O_CLOEXEC | O_FILE_ENC);
	if (fd < 0) {
		set_error("resync",
			  "cannot reopen the file with O_DIRECT for resync: %s",
			  strerror(errno));
		return -1;
	}
	if (fstat(fd, &direct_st) < 0) {
		set_error("resync", "cannot stat the O_DIRECT descriptor: %s",
			  strerror(errno));
		close(fd);
		return -1;
	}
	if (direct_st.st_dev != r->st.st_dev ||
	    direct_st.st_ino != r->st.st_ino) {
		set_retry_error("resync",
			  "the name now refers to a different file "
			  "(dev/ino %llu/%llu instead of %llu/%llu)",
			  (unsigned long long)direct_st.st_dev,
			  (unsigned long long)direct_st.st_ino,
			  (unsigned long long)r->st.st_dev,
			  (unsigned long long)r->st.st_ino);
		close(fd);
		return -1;
	}
	return fd;
}

static int resync_mirrors(struct resolved *r)
{
	struct llapi_layout *layout = NULL;
	/* Zero-initialised: llapi_mirror_resync_many() marks the components it
	 * synced by setting lrc_synced, and the release below only lists those.
	 * Uninitialised stack content would announce components as synced that
	 * never were. lfs initialises the same array the same way. */
	struct llapi_resync_comp comp_array[1024] = { { 0 } };
	struct ll_ioc_lease *ioc = NULL;
	struct stat st;
	uint64_t start = 0, end = 0;
	int comp_size, idx;
	int rc, rc2;
	int result = -1;
	int direct_fd = -1;

	if (fstat(r->file_fd, &st) < 0) {
		set_error("resync", "cannot stat the file: %s",
			  strerror(errno));
		return -1;
	}

	/* Everything below runs on the O_DIRECT descriptor: the client refuses
	 * the mirror-select ioctl without it. */
	direct_fd = open_direct_twin(r);
	if (direct_fd < 0)
		return -1;

	layout = llapi_layout_get_by_fd(direct_fd, 0);
	if (!layout) {
		set_error("resync", "cannot read layout: %s", strerror(errno));
		goto out;	/* not "return": direct_fd is open */
	}

	comp_size = llapi_mirror_find_stale(layout, comp_array,
					    (int)(sizeof(comp_array) /
						  sizeof(comp_array[0])),
					    NULL, 0);
	if (comp_size < 0) {
		/* llapi_mirror_find_stale() ends in "return rc < 0 ? rc : idx",
		 * so the error is the return value and errno was never touched
		 * by it. Reading errno here classified the failure on whatever
		 * an earlier call had left behind. */
		set_error_code("resync", errno_of_rc(comp_size),
			       "cannot find stale components: %s",
			       strerror(-comp_size));
		goto out;
	}
	if (comp_size == 0) {
		result = 0;	/* nothing stale */
		goto out;
	}

	ioc = calloc(1, sizeof(*ioc) +
		     sizeof(uint32_t) * (size_t)comp_size);
	if (!ioc) {
		set_memory_error("resync", "memory allocation failed");
		goto out;
	}

	ioc->lil_mode = LL_LEASE_WRLCK;
	ioc->lil_flags = LL_LEASE_RESYNC;
	rc = llapi_lease_set(direct_fd, ioc);
	if (rc < 0) {
		if (rc == -EALREADY) {
			/* Another resync is already running on this file. */
			result = 0;
			goto out;
		}
		set_error_code("resync", errno_of_rc(rc),
			       "cannot acquire the resync lease: %s",
			       strerror(-rc));
		goto out;
	}

	start = comp_array[0].lrc_start;
	end = comp_array[0].lrc_end;
	for (idx = 1; idx < comp_size; idx++) {
		if (comp_array[idx].lrc_start < start)
			start = comp_array[idx].lrc_start;
		if (end < comp_array[idx].lrc_end)
			end = comp_array[idx].lrc_end;
	}

	if (llapi_lease_check(direct_fd) != LL_LEASE_WRLCK) {
		set_error_code("resync", EAGAIN,
			       "lost the lease before resyncing");
		goto release;
	}

	rc = llapi_mirror_resync_many(direct_fd, layout, comp_array,
				      comp_size, start, end);
	if (rc < 0)
		set_error_code("resync", errno_of_rc(rc), "resync failed: %s",
			       strerror(-rc));
	else
		res.resyncs++;

	{
		struct timespec times[2];

		times[0] = st.st_atim;
		times[1] = st.st_mtim;
		if (futimens(direct_fd, times) < 0 && rc >= 0)
			set_error("resync",
				  "cannot restore timestamps after resync: %s",
				  strerror(errno));
	}

release:
	/* The lease has to go back even when the copy failed. */
	ioc->lil_mode = LL_LEASE_UNLCK;
	ioc->lil_flags = LL_LEASE_RESYNC_DONE;
	ioc->lil_count = 0;
	for (idx = 0; idx < comp_size; idx++) {
		if (comp_array[idx].lrc_synced) {
			ioc->lil_ids[ioc->lil_count] = comp_array[idx].lrc_id;
			ioc->lil_count++;
		}
	}
	rc2 = llapi_lease_set(direct_fd, ioc);
	if (rc2 <= 0) {
		set_error_code("resync", rc2 == 0 ? EAGAIN : -rc2,
			       "cannot release the resync lease: %s",
			       rc2 == 0 ? "lease lost" : strerror(-rc2));
		goto out;
	}

	if (res.exit_code == 0) {
		res.changed = true;
		result = 0;
	}

out:
	if (ioc)
		free(ioc);
	if (layout)
		llapi_layout_free(layout);
	if (direct_fd >= 0)
		close(direct_fd);
	return result;
}

/* ------------------------------------------------------------------ */
/* the state machine                                                  */
/* ------------------------------------------------------------------ */

static int pick_banned_mirror(const struct layout_state *state,
			      uint32_t *mirror_id)
{
	int i;

	for (i = 0; i < state->mirror_count; i++) {
		if (state->mirrors[i].banned_components > 0) {
			*mirror_id = state->mirrors[i].id;
			return 0;
		}
	}
	return -1;
}

/*
 * Pick a mirror to remove when there are more than requested.
 *
 * Two passes, and the order matters. A mirror that is not usable - stale,
 * nosync, offline, or empty - is the one to drop; keeping it while deleting a
 * complete copy would leave the file with fewer real copies than it had.
 * Only if every surplus mirror is usable is one of those given up, and never
 * the last one.
 */
static int pick_surplus_safe_mirror(const struct layout_state *state,
				    uint32_t *mirror_id)
{
	int i;

	for (i = 0; i < state->mirror_count; i++) {
		const struct mirror_state *m = &state->mirrors[i];

		if (m->banned || m->usable)
			continue;
		*mirror_id = m->id;
		return 0;
	}

	if (state->usable_mirror_count < 2)
		return -1;

	{
		bool kept = false;

		for (i = 0; i < state->mirror_count; i++) {
			const struct mirror_state *m = &state->mirrors[i];

			if (m->banned || !m->usable)
				continue;
			if (!kept) {
				kept = true;
				continue;
			}
			*mirror_id = m->id;
			return 0;
		}
	}
	return -1;
}

/*
 * Identify the mirror that was just created, and judge that one.
 *
 * NOT by looking for an id that was not there before. When a non-FLR file
 * gains its first mirror the server RENUMBERS the existing components:
 * lod_declare_layout_merge() turns mirror id 0 into 1 and gives the merged
 * one 2, so both ids look new and the file appears to have changed underneath.
 *
 * The rule the server follows is simpler and holds in both cases:
 *
 *     id = max(existing component ids);
 *     mirror_id = mirror_id_of(id) + 1;
 *
 * The merged mirror therefore always carries the HIGHEST mirror id in the
 * result. Together with "exactly one mirror more than before" that names it
 * without depending on how the others were numbered.
 */
static int check_new_mirror(const struct layout_state *state, int before_count,
			    uint32_t *added_id)
{
	const struct mirror_state *added = NULL;
	int i;

	*added_id = 0;

	if (state->mirror_count != before_count + 1) {
		set_error_code("extend", EBUSY,
			       "the file has %d mirrors after adding one to "
			       "%d; the layout changed outside this program",
			       state->mirror_count, before_count);
		return -1;
	}

	for (i = 0; i < state->mirror_count; i++)
		if (!added || state->mirrors[i].id > added->id)
			added = &state->mirrors[i];

	if (!added) {
		set_error_code("extend", EIO,
			       "no mirror found after the extend succeeded");
		return -1;
	}
	*added_id = added->id;

	if (added->banned) {
		set_error_code("extend", EINVAL,
			       "the new mirror %u was placed on a banned OST",
			       added->id);
		return -1;
	}
	if (added->outside_allowed_components > 0) {
		set_error_code("extend", EINVAL,
			       "the new mirror %u uses an OST that "
			       "--allowed-ost does not name", added->id);
		return -1;
	}
	if (!added->usable) {
		set_error_code("extend", EINVAL,
			       "the new mirror %u is not usable (stale=%d, "
			       "nosync/offline=%d, missing data=%d, gap=%s, "
			       "banned pool=%d)",
			       added->id, added->stale_components,
			       added->unusable_components,
			       added->missing_data_components,
			       added->has_gap ? "yes" : "no",
			       added->pool_banned_components);
		return -1;
	}
	return 0;
}

/*
 * Can a new mirror be placed at all?
 *
 * Asked here and not in main(), because here is where a new mirror is about
 * to be allocated. Asked before the file is copied, because the alternative
 * is to find out afterwards: Lustre does not know what "banned" means, so it
 * allocates, the result is rejected, the mirror is removed and the whole file
 * is copied again for the next attempt.
 *
 * A pool that merely CONTAINS a banned OST is fine as long as enough others
 * remain - allocation may pick a banned one, and that case is handled by
 * retrying. What cannot work is having fewer usable OSTs than the mirror
 * needs stripes.
 */
static bool allocation_capacity(const struct options *opt, const char *fsname,
				int *usable_out, uint64_t *needed_out)
{
	int usable;
	uint64_t needed;

	if (opt->banned_count == 0)
		return true;

	usable = usable_ost_count(opt, fsname);
	if (usable < 0)
		/* Not determinable here; Lustre answers it when the allocation
		 * is attempted. */
		return true;

	needed = opt->stripe_count_set ? opt->stripe_count : 1;
	if (usable_out)
		*usable_out = usable;
	if (needed_out)
		*needed_out = needed;
	return (uint64_t)usable >= needed;
}

static int allocation_is_possible(const struct options *opt, const char *fsname)
{
	int usable = 0;
	uint64_t needed = 0;

	if (allocation_capacity(opt, fsname, &usable, &needed))
		return 0;

	set_error_code("allocation", ENOSPC,
		       "only %d OST%s outside the banned set%s%s, but a new "
		       "mirror needs %llu stripe%s; nothing was changed",
		       usable, usable == 1 ? "" : "s",
		       opt->pool_name ? " in pool " : "",
		       opt->pool_name ? opt->pool_name : "",
		       (unsigned long long)needed, needed == 1 ? "" : "s");
	return -1;
}

static int add_verified_mirror(const struct options *opt, struct resolved *r,
			       const char *fsname, struct layout_state *state)
{
	/* The inode number spreads the OST choice over a tree of files; see
	 * pick_allowed_osts(). */
	uint64_t identity = (uint64_t)r->st.st_ino;
	long attempt;

	if (allocation_is_possible(opt, fsname) < 0)
		return -1;

	for (attempt = 1; attempt <= opt->allocation_attempts; attempt++) {
		int before_count = state->mirror_count;
		uint32_t added_id = 0;

		if (extend_one_mirror(opt, r, identity, attempt) < 0)
			return -1;
		if (analyse_layout(opt, r->file_fd, fsname, state,
				   "analyse") < 0)
			return -1;
		if (state->needs_resync) {
			if (resync_mirrors(r) < 0)
				return -1;
			if (analyse_layout(opt, r->file_fd, fsname, state,
					   "analyse") < 0)
				return -1;
		}

		/* Identify the mirror that was just created by its id and
		 * judge THAT one, rather than concluding from "some usable
		 * mirror now exists" that the new one is sound. */
		if (check_new_mirror(state, before_count, &added_id) == 0)
			return 0;

		if (added_id == 0)
			return -1;	/* nothing identifiable to take back */

		/*
		 * The new mirror is not usable. Take it back out in every
		 * case, not only when another attempt follows: leaving it
		 * behind would turn a failed attempt into exactly what this
		 * program exists to remove - one more mirror on a banned OST.
		 */
		{
			char kept[sizeof(res.message)];
			int kept_code = res.exit_code;

			snprintf(kept, sizeof(kept), "%s", res.message);
			res.exit_code = 0;
			res.status = "error";

			if (delete_one_mirror(opt, r, fsname, added_id,
					      DELETE_ROLLBACK_ADDED, 0) < 0) {
				/* Report the removal failure: the file now
				 * carries a mirror nobody wants. */
				return -1;
			}
			if (analyse_layout(opt, r->file_fd, fsname, state,
					   "analyse") < 0)
				return -1;

			if (attempt >= opt->allocation_attempts) {
				/* Out of attempts: restore the reason the new
				 * mirror was rejected. */
				set_error_code("extend", EAGAIN, "%s", kept);
				res.exit_code = kept_code ? kept_code : 1;
				res.transient = false;
				return -1;
			}
			res.allocation_retries++;
		}
	}
	return -1;
}

/*
 * Where the originally observed mirror count is kept so that it survives this
 * process.
 *
 * --keep-mirroring without an explicit --target-mirror-count derives the
 * target from what it finds. That is only correct as long as what it finds is
 * the untouched file. A run that died after deleting a banned mirror and
 * before restoring the count leaves a file with FEWER mirrors, and the next
 * run then adopts the reduced number as its goal - the redundancy is gone and
 * nothing says so.
 *
 * The note lives on the inode, because that is the only place that survives a
 * crash of any of the processes involved and travels with the file. It is
 * written before the first deletion and removed once the end state has been
 * verified, so a file carrying it is exactly a file whose migration did not
 * finish.
 */
/*
 * Which namespace the note lives in follows the privilege the run has.
 *
 * trusted.* can only be read and written with CAP_SYS_ADMIN, so under root -
 * the way this is normally run - the number cannot be forged by the owner of
 * the file. That matters: the note decides how many mirrors this program
 * creates, and a user-writable value would let the owner of any file direct a
 * root-run migration to build up to MAX_MIRRORS copies of it. trusted.* is
 * also independent of the user_xattr mount option.
 *
 * When trusted.* is missing, a root run checks whether an old release left
 * a user.* goal behind. It refuses that ambiguous state rather than treating
 * the smaller current mirror count as the original goal or trusting an
 * attribute the file owner can forge.
 *
 * Without CAP_SYS_ADMIN the only namespace available is user.*, which the
 * owner can change. That is tolerable there and only there: an unprivileged
 * run can only migrate files it may already rewrite, so the owner could
 * change the layout directly anyway. A root run never uses a user.* value as
 * a target, precisely so that a planted value cannot control its work.
 */
#define MIRROR_GOAL_XATTR_TRUSTED "trusted.lustre_migrate_file.mirror_goal"
#define MIRROR_GOAL_XATTR_USER    "user.lustre_migrate_file.mirror_goal"

static const char *mirror_goal_xattr(void)
{
	return geteuid() == 0 ? MIRROR_GOAL_XATTR_TRUSTED
			      : MIRROR_GOAL_XATTR_USER;
}

/* Linux reports "no such attribute" as ENODATA; the BSD/macOS spelling is
 * ENOATTR. Only the syntax check on a developer machine ever sees the latter. */
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

/* macOS takes two extra arguments; the target platform is Linux. */
#ifdef __APPLE__
#define fgetxattr(fd, name, value, size) \
	fgetxattr((fd), (name), (value), (size), 0, 0)
#define fsetxattr(fd, name, value, size, flags) \
	fsetxattr((fd), (name), (value), (size), 0, (flags))
#define fremovexattr(fd, name) fremovexattr((fd), (name), 0)
#endif

/* Return 1 for a valid goal, 0 only if it is absent, and -1 on every other
 * condition. The caller must stop on -1 before any layout change. */
static int read_mirror_goal(int fd, int *goal)
{
	char buffer[32];
	ssize_t got;
	long value;
	char *end;

	got = fgetxattr(fd, mirror_goal_xattr(), buffer, sizeof(buffer) - 1);
	if (got < 0) {
		int error = errno;

		if (error != ENODATA && error != ENOATTR) {
			set_error_code("prepare", error,
				       "cannot read mirror goal %s: %s",
				       mirror_goal_xattr(), strerror(error));
			return -1;
		}
		if (geteuid() == 0) {
			/* Older root runs wrote user.*. It is visible to the file
			 * owner and therefore cannot be adopted as an authority for
			 * how many new mirrors root should create. */
			got = fgetxattr(fd, MIRROR_GOAL_XATTR_USER, NULL, 0);
			if (got >= 0) {
				set_logic_error("prepare",
					"legacy user.* mirror goal exists without a "
					"trusted.* goal: recover the original intended "
					"mirror count from independent records, "
					"record it in %s with administrator privileges, "
					"then remove %s before retrying",
					MIRROR_GOAL_XATTR_TRUSTED,
					MIRROR_GOAL_XATTR_USER);
				return -1;
			}
			if (errno != ENODATA && errno != ENOATTR) {
				int legacy_error = errno;

				set_error_code("prepare", legacy_error,
					       "cannot check legacy mirror goal %s: %s",
					       MIRROR_GOAL_XATTR_USER,
					       strerror(legacy_error));
				return -1;
			}
		}
		*goal = 0;
		return 0;
	}
	if (got == 0) {
		set_logic_error("prepare", "empty mirror goal in %s",
				mirror_goal_xattr());
		return -1;
	}
	buffer[got] = '\0';
	errno = 0;
	value = strtol(buffer, &end, 10);
	if (errno != 0 || end == buffer || *end != '\0' ||
	    value < 1 || value > MAX_MIRRORS) {
		set_logic_error("prepare", "invalid mirror goal in %s",
				mirror_goal_xattr());
		return -1;
	}
	*goal = (int)value;
	return 1;
}

/* Returns 0 when the note is on the file, -1 when it is not. */
static int write_mirror_goal(int fd, long goal)
{
	char buffer[32];

	snprintf(buffer, sizeof(buffer), "%ld", goal);
	if (fsetxattr(fd, mirror_goal_xattr(), buffer, strlen(buffer), 0) < 0)
		return -1;
	return 0;
}

/*
 * Returns 0 when the file carries no goal any more - including when it never
 * did - and -1 when one is still there.
 *
 * A note left behind is not cosmetic. It says "an earlier run did not finish",
 * and the next --keep-mirroring believes it: a file explicitly extended to
 * three mirrors while an old note says two would have the third one deleted.
 * So a run that cannot remove it must not report success either.
 */
static int clear_mirror_goal(int fd)
{
	if (fremovexattr(fd, mirror_goal_xattr()) == 0)
		return 0;
	if (errno == ENODATA || errno == ENOATTR)
		return 0;
	return -1;
}

static int migrate_file(const struct options *opt, struct resolved *r,
			const char *fsname)
{
	struct layout_state state;
	long target;
	int round;
	int recorded_goal = 0;

	if (analyse_layout(opt, r->file_fd, fsname, &state, "analyse") < 0)
		return -1;

	res.mirror_count_before = state.mirror_count;
	res.banned_mirrors_before = state.banned_mirror_count;

	/* Read once, here, so every branch below answers the same question.
	 * A file carrying a goal is a file an earlier run did not finish. */
	if (read_mirror_goal(r->file_fd, &recorded_goal) < 0)
		return -1;
	res.initial_mirror_count = recorded_goal > 0 ? recorded_goal
						     : state.mirror_count;

	/*
	 * The target mirror count is decided once, from the state found at the
	 * start. Deriving it again after mirrors have been deleted would
	 * silently lower the redundancy the caller asked for.
	 */
	if (opt->target_mirror_count > 0) {
		/* Stated by the caller; nothing on the file overrides it. */
		target = opt->target_mirror_count;
	} else if (opt->collapse_to_one) {
		target = 1;
	} else if (opt->keep_mirroring) {
		/*
		 * A note left by an earlier, unfinished run wins over what is
		 * on the file now: the file may already have lost a mirror to
		 * that run, and counting what is left would make the loss
		 * permanent. Only without a note is the observation used.
		 */
		target = res.initial_mirror_count > 0 ?
			 res.initial_mirror_count : 1;
	} else {
		target = 1;
	}

	res.target_mirror_count = (int)target;

	if (target > MAX_MIRRORS) {
		set_error_code("prepare", EINVAL,
			       "target mirror count %ld exceeds the %d "
			       "mirrors Lustre allows", target, MAX_MIRRORS);
		return -1;
	}

	if (state.banned_mirror_count == 0 &&
	    state.mirror_count == (int)target && !state.any_stale &&
	    !state.needs_resync && state.usable_mirror_count > 0) {
		res.status = "ok";
		res.mirror_count_after = state.mirror_count;
		res.banned_mirrors_after = state.banned_mirror_count;
		/* An earlier run was interrupted and the file has since reached
		 * the requested state, so the note has outlived its purpose.
		 * Left behind it would mark the file as unfinished for good -
		 * and nothing would ever come back to remove it, because every
		 * future run takes exactly this early exit. Not done for
		 * --inspect-only or --dry-run: those promise to change
		 * nothing. */
		if (recorded_goal > 0 && !opt->inspect_only && !opt->dry_run &&
		    clear_mirror_goal(r->file_fd) < 0) {
			set_error("verify",
				  "the file is in the requested state, but the "
				  "mirror goal recorded by an earlier run "
				  "could not be removed from %s: %s; a later "
				  "--keep-mirroring would aim at that stale "
				  "number", mirror_goal_xattr(),
				  strerror(errno));
			return -1;
		}
		snprintf(res.message, sizeof(res.message),
			 "already in the requested state");
		return 0;
	}

	if (opt->inspect_only) {
		/*
		 * Report what is there and change nothing. This is what a
		 * caller uses to learn initial_mirror_count before the first
		 * attempt, so that a retry resuming after mirrors were
		 * already deleted still aims at the original number.
		 */
		res.status = "ok";
		res.mirror_count_after = state.mirror_count;
		res.banned_mirrors_after = state.banned_mirror_count;
		snprintf(res.message, sizeof(res.message),
			 "inspected: %d mirrors, %d on banned OSTs, %d usable, "
			 "size %lld%s",
			 state.mirror_count, state.banned_mirror_count,
			 state.usable_mirror_count,
			 (long long)state.file_size,
			 recorded_goal > 0 ?
			 "; an earlier run did not finish and recorded its "
			 "mirror goal on this file" : "");
		return 0;
	}

	if (opt->dry_run) {
		int usable_osts = 0;
		uint64_t needed_osts = 0;
		bool needs_new_mirror = (state.usable_mirror_count == 0 ||
					 state.mirror_count < (int)target);
		size_t used;

		/* status stays "ok": the protocol knows only ok and error, and
		 * nothing failed here. "changed" is false and the message
		 * carries the verdict. */
		res.status = "ok";
		res.mirror_count_after = state.mirror_count;
		res.banned_mirrors_after = state.banned_mirror_count;
		snprintf(res.message, sizeof(res.message),
			 "would migrate: %d of %d mirrors on banned OSTs, "
			 "%d usable, target mirror count %ld",
			 state.banned_mirror_count, state.mirror_count,
			 state.usable_mirror_count, target);

		/* Only when this file would actually need one: the OST count
		 * is a statement about the filesystem, and reporting it for a
		 * file that needs no new mirror was what made the check fire
		 * on files that were already clean. */
		if (needs_new_mirror &&
		    !allocation_capacity(opt, fsname, &usable_osts,
					 &needed_osts)) {
			used = strlen(res.message);
			snprintf(res.message + used, sizeof(res.message) - used,
				 "; but only %d OST%s lie outside the banned "
				 "set and a new mirror needs %llu",
				 usable_osts, usable_osts == 1 ? "" : "s",
				 (unsigned long long)needed_osts);
		}
		return 0;
	}

	/* Record the original goal BEFORE resync or extend can change the
	 * layout. If the filesystem cannot persist this record, stop without
	 * adding a mirror which a later run might mistake for the original
	 * target. An explicit CLI target does not replace crash recovery. */
	/*
	 * Not gated on a banned mirror being present. Step 1 below may add a
	 * usable copy before anything is deleted, and a run that dies between
	 * that and the return to the original count leaves a file with MORE
	 * mirrors than it should have - which the next run then adopts as the
	 * new goal. Any layout change at all is enough reason to write the
	 * number down first.
	 */
	if (opt->keep_mirroring &&
	    write_mirror_goal(r->file_fd, target) < 0) {
		set_error("prepare",
			  "cannot record the mirror goal in %s: %s; "
			  "refusing to change the layout without crash-safe "
			  "mirror-count recovery",
			  mirror_goal_xattr(), strerror(errno));
		return -1;
	}

	/*
	 * Step 1: there must be a mirror that is safe AND complete before
	 * anything is deleted. Without it, deleting the banned mirrors would
	 * destroy the only copy of the data.
	 *
	 * At most one extend and one resync: if adding a mirror did not
	 * produce a usable one, adding a second will not either, and a loop
	 * would keep appending mirrors to a file whose real problem is
	 * elsewhere.
	 */
	/*
	 * Here the resync serves step 1's own purpose and nothing else: get one
	 * good copy before anything is deleted. Catching up a mirror that is
	 * merely behind is cheaper and less disruptive than adding another one.
	 *
	 * Deliberately still conditioned on there being no usable mirror.
	 * Version 2.1 briefly resynced whatever was behind at this point, which
	 * also copied mirrors that the delete phase was about to remove - on a
	 * banned OST that is pure waste, and on a failing one the copy can fail
	 * and take down a migration that would have succeeded by simply
	 * deleting the mirror. What is still behind AND still there is caught
	 * up after the deletions instead, below.
	 */
	if (state.usable_mirror_count == 0 && state.needs_resync) {
		if (resync_mirrors(r) < 0)
			return -1;
		if (analyse_layout(opt, r->file_fd, fsname, &state, "analyse") < 0)
			return -1;
	}

	if (state.usable_mirror_count == 0) {
		if (add_verified_mirror(opt, r, fsname, &state) < 0)
			return -1;
	}

	if (state.usable_mirror_count == 0) {
		set_logic_error("prepare",
			  "could not establish a usable mirror outside the "
			  "banned OSTs (mirrors=%d, banned=%d, safe=%d, "
			  "stale=%s, size=%lld); check that pool %s has OSTs "
			  "outside the banned set",
			  state.mirror_count, state.banned_mirror_count,
			  state.safe_mirror_count,
			  state.any_stale ? "yes" : "no",
			  (long long)state.file_size,
			  opt->pool_name ? opt->pool_name : "(none)");
		return -1;
	}

	/*
	 * Step 2: delete the banned mirrors, one per round, re-reading the
	 * layout in between. Each deletion is a separate lease, so the state
	 * this program believes in is never older than the last operation.
	 */
	for (round = 0; round < MAX_DELETE_ROUNDS; round++) {
		uint32_t mirror_id;

		if (state.banned_mirror_count == 0)
			break;
		/* The guard is the number of USABLE mirrors, not of mirrors
		 * that merely avoid the banned OSTs: a stale, nosync or
		 * offline mirror is not a copy this may rely on. */
		if (state.usable_mirror_count < 1) {
			set_logic_error("delete",
				  "refusing to delete: no usable mirror left "
				  "(safe=%d, usable=0)",
				  state.safe_mirror_count);
			return -1;
		}
		if (pick_banned_mirror(&state, &mirror_id) < 0)
			break;

		if (delete_one_mirror(opt, r, fsname, mirror_id,
				      DELETE_BANNED, target) < 0)
			return -1;
		if (analyse_layout(opt, r->file_fd, fsname, &state, "analyse") < 0)
			return -1;
		/* Only to keep a usable copy available for the next round. A
		 * mirror that is behind and about to be deleted must not be
		 * copied first. */
		if (state.usable_mirror_count == 0 && state.needs_resync &&
		    resync_mirrors(r) < 0)
			return -1;
		if (analyse_layout(opt, r->file_fd, fsname, &state, "analyse") < 0)
			return -1;
	}

	if (state.banned_mirror_count != 0) {
		set_retry_error("delete",
			  "%d mirrors on banned OSTs remain after %d rounds",
			  state.banned_mirror_count, MAX_DELETE_ROUNDS);
		return -1;
	}

	/*
	 * Step 3: reach the requested mirror count exactly - add what is
	 * missing, and remove a surplus that a crashed earlier run may have
	 * left behind.
	 */
	for (round = 0; round < MAX_DELETE_ROUNDS &&
	     state.mirror_count < (int)target; round++) {
		int before = state.mirror_count;

		if (add_verified_mirror(opt, r, fsname, &state) < 0)
			return -1;
		if (state.mirror_count <= before) {
			set_error_code("restore", EIO,
				       "adding a mirror did not increase the "
				       "mirror count (still %d, target %ld)",
				       state.mirror_count, target);
			return -1;
		}
	}

	for (round = 0; round < MAX_DELETE_ROUNDS &&
	     state.mirror_count > (int)target; round++) {
		uint32_t mirror_id;

		if (pick_surplus_safe_mirror(&state, &mirror_id) < 0)
			break;
		if (delete_one_mirror(opt, r, fsname, mirror_id,
				      DELETE_SURPLUS, target) < 0)
			return -1;
		if (analyse_layout(opt, r->file_fd, fsname, &state, "analyse") < 0)
			return -1;
	}

	/*
	 * Step 3b: whatever is still behind is caught up now - after the
	 * deletions and after the count is right, so only mirrors that will
	 * actually survive are copied. This is what makes a file with the
	 * correct mirror count, no banned mirror and one stale mirror finish:
	 * before, it fell through every step and failed the final check on
	 * every attempt, because each attempt took the identical path.
	 */
	if (state.needs_resync) {
		if (resync_mirrors(r) < 0)
			return -1;
		if (analyse_layout(opt, r->file_fd, fsname, &state, "analyse") < 0)
			return -1;
	}

	/* Step 4: the end state is what the caller asked for, or this failed. */
	if (analyse_layout(opt, r->file_fd, fsname, &state, "verify") < 0)
		return -1;

	res.mirror_count_after = state.mirror_count;
	res.banned_mirrors_after = state.banned_mirror_count;

	if (state.banned_mirror_count != 0) {
		set_retry_error("verify", "%d mirrors still use banned OSTs",
			  state.banned_mirror_count);
		return -1;
	}
	if (state.mirror_count != (int)target) {
		set_retry_error("verify",
			  "final mirror count is %d, requested %ld",
			  state.mirror_count, target);
		return -1;
	}
	if (state.usable_mirror_count < 1) {
		set_retry_error("verify", "no usable mirror remains");
		return -1;
	}
	if (state.any_stale) {
		/* A mirror left behind is a redundancy the file claims but does
		 * not have. A nosync or offline mirror does not reach this:
		 * scan_component() does not raise any_stale for one, because
		 * being behind is what it was configured for. */
		set_retry_error("verify", "a mirror is still stale after the run");
		return -1;
	}
	/*
	 * A pending FLR state on its own is NOT rejected here, and that is a
	 * correction of 2.2.
	 *
	 * A write marks NOSYNC components STALE as well and puts the file into
	 * WRITE_PENDING. llapi_mirror_find_stale() skips NOSYNC by design, so
	 * the resync in step 3b finds nothing, changes nothing, and the pending
	 * state stays. Rejecting on it meant a file with one usable mirror and
	 * one deliberately lagging NOSYNC mirror failed after every retry and
	 * again on every later run - for ever, deterministically.
	 *
	 * What USUALLY catches a genuine writer is the resync itself: it takes
	 * a write lease, and a concurrent writer breaks that lease, which is a
	 * retryable error. That is not a guarantee, and saying it without the
	 * qualifier claimed more than the code does: when
	 * llapi_mirror_find_stale() reports nothing to do, resync_mirrors()
	 * returns before taking any lease at all, so a writer active at that
	 * moment goes unnoticed here.
	 *
	 * The end state is still verified - no banned mirror, the requested
	 * count, a usable copy, nothing actionably stale - and pending_layout
	 * says the file is not fully in sync. What cannot be told apart at this
	 * point is "pending because of NOSYNC" from "pending because somebody
	 * is writing right now". Both are reported the same way, and neither is
	 * treated as a failure.
	 */
	if (state.needs_resync)
		res.pending_layout = true;

	/*
	 * The end state is what was asked for, so the note has done its job.
	 *
	 * Removed regardless of WHICH option set the target. Clearing it only
	 * under --keep-mirroring meant that a file extended with an explicit
	 * --target-mirror-count kept an older, smaller goal, and the next
	 * --keep-mirroring read that number and deleted a mirror the operator
	 * had just asked for. The note describes an unfinished run, not a
	 * mode.
	 */
	if (clear_mirror_goal(r->file_fd) < 0) {
		set_error("verify",
			  "the layout is correct, but the mirror goal could "
			  "not be removed from %s: %s; a later "
			  "--keep-mirroring would aim at that stale number",
			  mirror_goal_xattr(), strerror(errno));
		return -1;
	}

	if (opt->verify) {
		/* Read the layout once more, after everything is closed out,
		 * so the reported end state is not the one this program
		 * happens to have cached. */
		struct layout_state again;

		if (analyse_layout(opt, r->file_fd, fsname, &again, "verify") < 0)
			return -1;
		if (again.banned_mirror_count != 0 ||
		    again.mirror_count != state.mirror_count ||
		    again.usable_mirror_count < 1 || again.any_stale) {
			set_retry_error("verify",
				  "layout changed during final verification");
			return -1;
		}
	}

	/* A successful run said nothing at all before: message is reported
	 * unconditionally, so it has to carry the outcome here too. */
	snprintf(res.message, sizeof(res.message),
		 "migrated: %d mirror%s, none on a banned OST; added %d, "
		 "deleted %d, resynced %d%s",
		 state.mirror_count, state.mirror_count == 1 ? "" : "s",
		 res.mirrors_added, res.mirrors_deleted, res.resyncs,
		 res.pending_layout ?
		 "; the layout is still write/sync pending and no resync could "
		 "clear it - either the components behind are NOSYNC ones, "
		 "which a resync skips, or a writer is active right now; this "
		 "run cannot tell which" : "");
	res.status = "ok";
	return 0;
}

/* ------------------------------------------------------------------ */
/* argument parsing                                                   */
/* ------------------------------------------------------------------ */

static void usage(FILE *stream)
{
	fprintf(stream,
"Usage: %s --root PATH [options] FILE\n"
"\n"
"Move one Lustre file off a set of banned OSTs and leave it with the\n"
"requested number of mirrors. Exactly one file per invocation.\n"
"\n"
"  --root PATH               only operate below this directory (required)\n"
"  --stripe-count COUNT      stripe count for newly created mirrors\n"
"  --pool-name POOL          OST pool for newly created mirrors. A pool that\n"
"                            merely CONTAINS a banned OST is accepted: enough\n"
"                            others may remain, and refusing it would rule out\n"
"                            a workable configuration. Where Lustre allocates\n"
"                            cannot be known in advance either way, so a\n"
"                            mirror that lands on a banned OST is removed\n"
"                            again and retried, see --allocation-attempts.\n"
"                            What IS refused up front is a pool naming another\n"
"                            filesystem, and a pool with fewer usable OSTs\n"
"                            than the mirror needs stripes\n"
"  --allowed-ost OST         OST a NEW mirror may be placed on; repeatable,\n"
"                            same spellings as --banned-ost. Naming exactly\n"
"                            as many as --stripe-count pins the placement,\n"
"                            like lfs setstripe -o. Naming more lets this\n"
"                            program choose that many, spread across files\n"
"                            by the source inode so a whole tree does not\n"
"                            land on the same OSTs. Without it Lustre\n"
"                            chooses. It says nothing about which mirrors\n"
"                            must be evacuated - that is --banned-ost.\n"
"  --banned-ost OST          OST to evacuate; repeatable. Accepts:\n"
"                              14                 decimal index\n"
"                              0x0014, OST0014    hexadecimal index\n"
"                              l1fs-OST0014_UUID  as lfs df prints it\n"
"                            A bare number with a leading zero is refused:\n"
"                            0014 could be decimal, octal or the four hex\n"
"                            digits of a UUID, and guessing wrong evacuates\n"
"                            a different OST. Write 20 or 0x0014.\n"
"  --keep-mirroring          end with as many mirrors as the file had. The\n"
"                            number comes from the goal an unfinished\n"
"                            earlier run recorded, else from what is there\n"
"  --target-mirror-count N   end with exactly N mirrors\n"
"  --expected-fsname NAME    refuse unless the file is on this filesystem\n"
"  --verify                  re-read and re-check the layout at the end\n"
"  --inspect-only            report the layout and change nothing; this is\n"
"                            how a caller learns initial_mirror_count\n"
"  --collapse-to-one         end with exactly one mirror (the default)\n"
"                            These three each state the final mirror count;\n"
"                            exactly one of them may be given\n"
"  --json                    one JSON object on stdout instead of the\n"
"                            readable form; paths as base64\n"
"  --dry-run                 analyse and report, change nothing\n"
"  --allow-other-devices     permit crossing a mount point below root;\n"
"                            cannot be combined with --banned-ost\n"
"  --allocation-attempts N   retry N times when a new mirror lands on a\n"
"                            banned OST (default 1, no retry)\n"
"  --expected-dev DEV        refuse unless the file still has this st_dev\n"
"  --expected-ino INO        refuse unless the file still has this st_ino\n"
"  --help, --version\n"
"\n"
"Exit status:\n"
"  0  success, or the file was already in the requested state\n"
"  1  permanent failure\n"
"  2  usage error\n"
"  3  transient failure (EAGAIN, EBUSY, ESTALE, ...); try this file again\n",
		PROGRAM_NAME);
}

/*
 * Accept an OST in every spelling an operator actually has in front of them,
 * and refuse the one that is ambiguous.
 *
 *   14                    decimal, as "lfs osts" prints it in the first column
 *   0x0014, 0X14          hexadecimal, explicit
 *   OST0014               hexadecimal, as it appears inside a UUID
 *   l1fs-OST0014_UUID     what "lfs df" prints; the _UUID suffix is optional
 *
 * A leading zero on a bare number is REFUSED. strtol() with base 0 would read
 * "0014" as octal 12, while anyone writing it has copied the four hex digits
 * out of a UUID and means 20. Both readings are plausible, the wrong one is
 * silent, and the consequence is evacuating a different OST than intended -
 * so the program asks instead of guessing.
 *
 * Returns 0 on success, -1 on a malformed value, -2 when the filesystem name
 * inside a UUID does not match the expected one.
 */
static int parse_ost(const char *text, const char *expected_fsname, int *index)
{
	const char *digits = NULL;
	const char *marker;
	char *end;
	long value;
	int base;

	if (!text || text[0] == '\0')
		return -1;
	/* strtol() skips leading whitespace; an OST never has any. */
	if (text[0] == ' ' || text[0] == '\t')
		return -1;

	marker = strstr(text, "-OST");
	if (marker) {
		if (expected_fsname) {
			size_t len = (size_t)(marker - text);

			if (strlen(expected_fsname) != len ||
			    strncmp(text, expected_fsname, len) != 0)
				return -2;
		}
		digits = marker + 4;
		base = 16;
	} else if (strncmp(text, "OST", 3) == 0) {
		/* The bare component of a UUID, without a filesystem name.
		 * Unambiguously hexadecimal, so it is accepted: it is what
		 * somebody trimming the UUID is most likely to be left with. */
		digits = text + 3;
		base = 16;
	} else if (text[0] == '0' && (text[1] == 'x' || text[1] == 'X')) {
		digits = text + 2;
		base = 16;
	} else {
		if (text[0] == '0' && text[1] != '\0')
			return -1;	/* ambiguous: see above */
		digits = text;
		base = 10;
	}

	if (digits[0] == '\0')
		return -1;
	errno = 0;
	value = strtol(digits, &end, base);
	if (errno != 0 || end == digits)
		return -1;
	if (*end != '\0' && strcmp(end, "_UUID") != 0)
		return -1;

	if (value < 0 || value > 0xffff)
		return -1;
	*index = (int)value;
	return 0;
}

enum {
	OPT_ROOT = 1000, OPT_STRIPE_COUNT, OPT_POOL, OPT_BANNED,
	OPT_KEEP_MIRRORING, OPT_TARGET, OPT_VERIFY, OPT_DRY_RUN,
	OPT_FSNAME, OPT_OTHER_DEVICES, OPT_ATTEMPTS, OPT_EXPECT_DEV,
	OPT_EXPECT_INO, OPT_INSPECT_ONLY, OPT_COLLAPSE, OPT_JSON, OPT_ALLOWED,
	OPT_HELP, OPT_VERSION,
};

static const struct option long_options[] = {
	{ "root",                required_argument, NULL, OPT_ROOT },
	{ "stripe-count",        required_argument, NULL, OPT_STRIPE_COUNT },
	{ "pool-name",           required_argument, NULL, OPT_POOL },
	{ "banned-ost",          required_argument, NULL, OPT_BANNED },
	{ "allowed-ost",         required_argument, NULL, OPT_ALLOWED },
	{ "keep-mirroring",      no_argument,       NULL, OPT_KEEP_MIRRORING },
	{ "target-mirror-count", required_argument, NULL, OPT_TARGET },
	{ "expected-fsname",     required_argument, NULL, OPT_FSNAME },
	{ "verify",              no_argument,       NULL, OPT_VERIFY },
	{ "inspect-only",        no_argument,       NULL, OPT_INSPECT_ONLY },
	{ "collapse-to-one",     no_argument,       NULL, OPT_COLLAPSE },
	{ "json",                no_argument,       NULL, OPT_JSON },
	{ "dry-run",             no_argument,       NULL, OPT_DRY_RUN },
	{ "allow-other-devices", no_argument,       NULL, OPT_OTHER_DEVICES },
	{ "allocation-attempts", required_argument, NULL, OPT_ATTEMPTS },
	{ "expected-dev",        required_argument, NULL, OPT_EXPECT_DEV },
	{ "expected-ino",        required_argument, NULL, OPT_EXPECT_INO },
	{ "help",                no_argument,       NULL, OPT_HELP },
	{ "version",             no_argument,       NULL, OPT_VERSION },
	{ NULL, 0, NULL, 0 },
};

/* Parse errors have to reach a machine caller the same way every other error
 * does. A bare stderr line would leave a scheduler with an exit code and no
 * parseable reason. */
static void usage_error(struct options *opt, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(res.message, sizeof(res.message), fmt, ap);
	va_end(ap);

	res.status = "error";
	res.stage = "arguments";
	res.errno_value = EINVAL;
	res.exit_code = 2;

	if (opt->json_output)
		emit_result(opt);
	else
		fprintf(stderr, "%s: %s\n", PROGRAM_NAME, res.message);
}

static int parse_arguments(int argc, char **argv, struct options *opt)
{
	int c, i;

	memset(opt, 0, sizeof(*opt));

	/*
	 * Find --json before parsing anything else. getopt would only reach it
	 * after the option that is wrong, so an argument error before it would
	 * answer in the readable form to a caller that cannot read it.
	 */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0)
			break;
		if (strcmp(argv[i], "--json") == 0) {
			opt->json_output = true;
			break;
		}
	}
	opt->target_mirror_count = -1;
	opt->allocation_attempts = 1;

	/* A '+' prefix stops option parsing at the first non-option, so a
	 * filename that begins with '-' cannot be mistaken for a flag. */
	while ((c = getopt_long(argc, argv, "+", long_options, NULL)) != -1) {
		char *end;
		long value;

		switch (c) {
		case OPT_ROOT:
			opt->root = optarg;
			break;
		case OPT_POOL:
			if (optarg[0] == '\0') {
				usage_error(opt, "empty pool name");
				return -1;
			}
			opt->pool_name = optarg;
			break;
		case OPT_FSNAME:
			opt->expected_fsname = optarg;
			break;
		case OPT_STRIPE_COUNT:
			errno = 0;
			value = strtol(optarg, &end, 10);
			if (errno != 0 || *end != '\0' || value < 1 ||
			    value > 2000) {
				usage_error(opt,
					"bad stripe count: %s", optarg);
				return -1;
			}
			opt->stripe_count = (uint64_t)value;
			opt->stripe_count_set = true;
			break;
		case OPT_BANNED:
			if (opt->banned_count >= MAX_BANNED_OSTS) {
				usage_error(opt, "too many banned OSTs");
				return -1;
			}
			/* The fsname check happens later, once the file's own
			 * filesystem is known; here only the syntax. */
			{
				int index;
				int rc = parse_ost(optarg, NULL, &index);

				if (rc != 0) {
					/* The leading-zero case is the one an
					 * operator hits by trimming a UUID, so
					 * it gets its own answer instead of a
					 * generic refusal. */
					if (optarg[0] == '0' && optarg[1] &&
					    optarg[1] != 'x' &&
					    optarg[1] != 'X')
						usage_error(opt,
							"ambiguous OST %s: a "
							"leading zero could mean "
							"decimal, octal or the "
							"hex digits of a UUID. "
							"Write the decimal "
							"index, or 0x%s, or "
							"OST%s",
							optarg, optarg + 1,
							optarg);
					else
						usage_error(opt,
							"bad OST: %s (accepted: "
							"14, 0x0014, OST0014, "
							"fs-OST0014_UUID)",
							optarg);
					return -1;
				}
				{
					size_t k;

					/* usable_ost_count() subtracts the
					 * length of this list from the number
					 * of OSTs, so a duplicate removes an
					 * OST that is still there and can
					 * produce a false ENOSPC. */
					for (k = 0; k < opt->banned_count; k++) {
						if (opt->banned[k] != index)
							continue;
						usage_error(opt,
							"OST %d is named twice "
							"by --banned-ost",
							index);
						return -1;
					}
				}
				opt->banned_text[opt->banned_count] = optarg;
				opt->banned[opt->banned_count++] = index;
			}
			break;
		case OPT_ALLOWED:
			if (opt->allowed_count >= MAX_BANNED_OSTS) {
				usage_error(opt, "too many allowed OSTs");
				return -1;
			}
			{
				int index;

				if (parse_ost(optarg, NULL, &index) != 0) {
					usage_error(opt,
						"bad allowed OST: %s "
						"(accepted: 14, 0x0014, "
						"OST0014, fs-OST0014_UUID)",
						optarg);
					return -1;
				}
				opt->allowed_text[opt->allowed_count] = optarg;
				opt->allowed[opt->allowed_count++] = index;
			}
			break;
		case OPT_KEEP_MIRRORING:
			opt->keep_mirroring = true;
			break;
		case OPT_TARGET:
			errno = 0;
			value = strtol(optarg, &end, 10);
			if (errno != 0 || *end != '\0' || value < 1 ||
			    value > MAX_MIRRORS) {
				usage_error(opt,
					"bad target mirror count: %s", optarg);
				return -1;
			}
			opt->target_mirror_count = value;
			break;
		case OPT_VERIFY:
			opt->verify = true;
			break;
		case OPT_JSON:
			opt->json_output = true;
			break;
		case OPT_INSPECT_ONLY:
			opt->inspect_only = true;
			break;
		case OPT_COLLAPSE:
			opt->collapse_to_one = true;
			break;
		case OPT_DRY_RUN:
			opt->dry_run = true;
			break;
		case OPT_OTHER_DEVICES:
			opt->allow_other_devices = true;
			break;
		case OPT_ATTEMPTS:
			errno = 0;
			value = strtol(optarg, &end, 10);
			/* The same range the scheduler validates, so a value
			 * it accepts is never rejected here. Each attempt
			 * creates and deletes a mirror, so a large number says
			 * "keep trying", it is not a plan. */
			if (errno != 0 || *end != '\0' || value < 1 ||
			    value > 100000) {
				usage_error(opt,
					"bad allocation attempts: %s", optarg);
				return -1;
			}
			opt->allocation_attempts = value;
			break;
		case OPT_EXPECT_DEV:
		case OPT_EXPECT_INO: {
			unsigned long long parsed;

			errno = 0;
			parsed = strtoull(optarg, &end, 10);
			if (errno != 0 || end == optarg || *end != '\0') {
				usage_error(opt,
					"bad %s: %s",
					c == OPT_EXPECT_DEV ? "--expected-dev"
							    : "--expected-ino",
					optarg);
				return -1;
			}
			if (c == OPT_EXPECT_DEV)
				opt->expect_dev = parsed;
			else
				opt->expect_ino = parsed;
			opt->expect_identity = true;
			break;
		}
		case OPT_HELP:
			usage(stdout);
			exit(0);
		case OPT_VERSION:
			printf("%s %s\n", PROGRAM_NAME, PROGRAM_VERSION);
			exit(0);
		default:
			usage(stderr);
			return -1;
		}
	}

	if (!opt->root || opt->root[0] != '/') {
		usage_error(opt,
			"--root is required and must be an absolute path");
		return -1;
	}
	if (optind >= argc) {
		usage_error(opt, "no file given");
		return -1;
	}
	if (argc - optind != 1) {
		/* One file per invocation keeps every failure isolated and
		 * matches a scheduler that runs one process per file. */
		usage_error(opt,
			"exactly one file per invocation, got %d", argc - optind);
		return -1;
	}
	opt->path = argv[optind];
	if (opt->path[0] == '\0') {
		usage_error(opt, "empty file name");
		return -1;
	}
	if (opt->banned_count == 0 && opt->target_mirror_count < 0 &&
	    !opt->keep_mirroring && !opt->collapse_to_one &&
	    !opt->inspect_only) {
		/* Every option the message names has to count here. Leaving
		 * --collapse-to-one and --inspect-only out of the test refused
		 * exactly the two invocations the message recommends: an
		 * inspection, and a collapse of clean surplus mirrors - which
		 * is also what the scheduler sends when banned_osts is an
		 * empty list. */
		usage_error(opt,
			"nothing to do: give --banned-ost, "
			"--target-mirror-count, --keep-mirroring, "
			"--collapse-to-one or --inspect-only");
		return -1;
	}
	/*
	 * How many mirrors the file ends with is said by exactly ONE option,
	 * and the three are mutually exclusive:
	 *
	 *   --keep-mirroring         as many as the file had
	 *   --target-mirror-count N  exactly N
	 *   --collapse-to-one        exactly one
	 *
	 * --keep-mirroring with --target-mirror-count used to be allowed,
	 * because this program derived its target from the state it found and
	 * after an interrupted run that was the REDUCED count - so the caller
	 * had to read the number beforehand and pin it here. That crutch is
	 * gone: the goal is recorded on the file before anything changes and
	 * read back at the start of every run, so "as many as it had" is a
	 * question this program answers itself. A second number next to it is
	 * not a refinement of "keep what was there", it is a different answer.
	 */
	{
		int given = (opt->keep_mirroring ? 1 : 0) +
			    (opt->collapse_to_one ? 1 : 0) +
			    (opt->target_mirror_count > 0 ? 1 : 0);

		if (given > 1) {
			usage_error(opt,
				"--keep-mirroring, --target-mirror-count and "
				"--collapse-to-one each state the final mirror "
				"count; give only one of them");
			return -1;
		}
	}
	if (opt->allowed_count > 0) {
		size_t allowed_index, banned_index;
		uint64_t stripes = opt->stripe_count_set ? opt->stripe_count : 1;

		for (allowed_index = 0; allowed_index < opt->allowed_count;
		     allowed_index++) {
			for (banned_index = 0;
			     banned_index < opt->banned_count; banned_index++) {
				if (opt->allowed[allowed_index] !=
				    opt->banned[banned_index])
					continue;
				usage_error(opt,
					"OST %d is named by both --allowed-ost "
					"and --banned-ost",
					opt->allowed[allowed_index]);
				return -1;
			}
			for (banned_index = allowed_index + 1;
			     banned_index < opt->allowed_count;
			     banned_index++) {
				if (opt->allowed[allowed_index] !=
				    opt->allowed[banned_index])
					continue;
				usage_error(opt,
					"OST %d is named twice by "
					"--allowed-ost",
					opt->allowed[allowed_index]);
				return -1;
			}
		}
		if ((uint64_t)opt->allowed_count < stripes) {
			/* Lustre wants one index per stripe; fewer cannot be
			 * turned into a placement. */
			usage_error(opt,
				"--allowed-ost names %zu OSTs but a mirror of "
				"%llu stripes needs at least that many",
				opt->allowed_count,
				(unsigned long long)stripes);
			return -1;
		}
	}
	if (opt->allow_other_devices && opt->banned_count > 0) {
		/* The filesystem name is derived from --root, and an OST index
		 * only means something within one filesystem. Letting the file
		 * live on a different mount would evacuate indexes of the
		 * wrong filesystem. */
		usage_error(opt,
			"--allow-other-devices cannot be combined with "
			"--banned-ost: an OST index is only meaningful within "
			"the filesystem of --root");
		return -1;
	}
	if (opt->expect_identity &&
	    (opt->expect_dev == 0 || opt->expect_ino == 0)) {
		usage_error(opt,
			"--expected-dev and --expected-ino must be given "
			"together and be non-zero");
		return -1;
	}

	return 0;
}

/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
	struct options opt;
	struct resolved r = {
		.root_fd = -1, .parent_fd = -1, .file_fd = -1
	};
	char fsname[PATH_MAX];
	char root_fd_path[64];
	struct lu_fid fid;
	int fs_rc, path_len;

	if (parse_arguments(argc, argv, &opt) < 0)
		return 2;

	if (resolve_path(&opt, &r) < 0)
		goto out;

	/*
	 * The caller inspected a pathname and then started this program with
	 * it. Between the two the name can have been pointed at a different
	 * inode. Comparing the identity the caller saw against the descriptor
	 * actually opened here is the only way to notice.
	 */
	if (opt.expect_identity &&
	    ((unsigned long long)r.st.st_dev != opt.expect_dev ||
	     (unsigned long long)r.st.st_ino != opt.expect_ino)) {
		set_error_code("identity", ESTALE,
			       "the name now refers to a different file: "
			       "caller expected dev/ino %llu/%llu, found "
			       "%llu/%llu",
			       opt.expect_dev, opt.expect_ino,
			       (unsigned long long)r.st.st_dev,
			       (unsigned long long)r.st.st_ino);
		goto out;
	}

	/*
	 * liblustreapi obtains the filesystem name from the path's device. Use
	 * the held root descriptor through procfs: resolving opt.root again could
	 * inspect another filesystem after a rename in its parent. The same fd
	 * was used to resolve the source file, so the name and file stay paired.
	 */
	path_len = snprintf(root_fd_path, sizeof(root_fd_path),
			    "/proc/self/fd/%d", r.root_fd);
	if (path_len < 0 || path_len >= (int)sizeof(root_fd_path)) {
		set_error_code("fsname", ENAMETOOLONG,
			       "cannot represent the pinned root descriptor");
		goto out;
	}
	fs_rc = llapi_search_fsname(root_fd_path, fsname);
	if (fs_rc < 0) {
		set_error_code("fsname", errno_of_rc(fs_rc),
			       "cannot identify the Lustre filesystem of "
			       "the pinned root %s: %s", opt.root,
			       strerror(errno_of_rc(fs_rc)));
		goto out;
	}
	if (opt.expected_fsname && strcmp(opt.expected_fsname, fsname) != 0) {
		set_logic_error("fsname",
			  "root is on Lustre filesystem %s, expected %s",
			  fsname, opt.expected_fsname);
		goto out;
	}

	/* Re-check every OST spelled as a UUID now that the filesystem name is
	 * known. A UUID naming another filesystem is a configuration mistake
	 * with destructive consequences, not a detail to normalise away.
	 *
	 * --allowed-ost is checked the same way and for the same reason: its
	 * indexes decide where new mirrors are PLACED, so a UUID of another
	 * filesystem silently becomes a bare index here and directs the
	 * placement at whatever device happens to carry that number. */
	{
		size_t i;

		for (i = 0; i < opt.allowed_count; i++) {
			int index;

			if (!strstr(opt.allowed_text[i], "-OST"))
				continue;	/* a bare index names no fs */
			if (parse_ost(opt.allowed_text[i], fsname, &index) == -2) {
				set_error_code("fsname", EINVAL,
					       "allowed OST %s does not belong "
					       "to filesystem %s",
					       opt.allowed_text[i], fsname);
				goto out;
			}
		}

		for (i = 0; i < opt.banned_count; i++) {
			int index;

			if (!strstr(opt.banned_text[i], "-OST"))
				continue;	/* a bare index names no fs */
			if (parse_ost(opt.banned_text[i], fsname, &index) == -2) {
				set_logic_error("fsname",
					  "banned OST %s does not belong to "
					  "filesystem %s",
					  opt.banned_text[i], fsname);
				goto out;
			}
		}
	}

	/*
	 * Every banned OST has to exist in this filesystem. A typo in an index
	 * would otherwise silently ban nothing at all, and the run would
	 * report success while the file stays where it is.
	 */
	{
		size_t i;

		for (i = 0; i < opt.banned_count; i++) {
			char ostname[80];
			int rc;
			int written;

			written = snprintf(ostname, sizeof(ostname),
					   "%s-OST%04x_UUID", fsname,
					   (unsigned)opt.banned[i]);
			/* A truncated name would be looked up as a DIFFERENT
			 * OST, which is worse than not looking it up at all. */
			if (written < 0 || (size_t)written >= sizeof(ostname)) {
				set_error_code("ost", ENAMETOOLONG,
					       "filesystem name %s is too long "
					       "to build an OST name from",
					       fsname);
				goto out;
			}
			rc = llapi_search_ost(fsname, NULL, ostname);
			if (rc < 0) {
				set_error("ost",
					  "cannot look up OST %s in filesystem "
					  "%s: %s", ostname, fsname,
					  strerror(errno));
				goto out;
			}
			if (rc == 0) {
				set_error_code("ost", ENODEV,
					       "banned OST %s does not exist "
					       "in filesystem %s",
					       ostname, fsname);
				goto out;
			}
		}

		/*
		 * The allowed list gets the same treatment. Its indexes decide
		 * where new mirrors are PLACED, so a typo there is at least as
		 * consequential - but it surfaced only per file, at allocation
		 * time, as a puzzling failure, while the same typo in the
		 * banned list was caught right here.
		 */
		for (i = 0; i < opt.allowed_count; i++) {
			char ostname[80];
			int rc;
			int written;

			written = snprintf(ostname, sizeof(ostname),
					   "%s-OST%04x_UUID", fsname,
					   (unsigned)opt.allowed[i]);
			if (written < 0 || (size_t)written >= sizeof(ostname)) {
				set_error_code("ost", ENAMETOOLONG,
					       "filesystem name %s is too long "
					       "to build an OST name from",
					       fsname);
				goto out;
			}
			rc = llapi_search_ost(fsname, NULL, ostname);
			if (rc < 0) {
				set_error("ost",
					  "cannot look up OST %s in filesystem "
					  "%s: %s", ostname, fsname,
					  strerror(errno));
				goto out;
			}
			if (rc == 0) {
				set_error_code("ost", ENODEV,
					       "allowed OST %s does not exist "
					       "in filesystem %s",
					       ostname, fsname);
				goto out;
			}
		}
	}

	/*
	 * A pool name may be written as "fsname.pool".
	 * llapi_layout_pool_name_set() keeps only the part behind the dot and
	 * does not check the filesystem, so a pool of another filesystem would
	 * silently become a same-named pool of this one.
	 */
	if (opt.pool_name) {
		const char *dot = strchr(opt.pool_name, '.');

		if (dot) {
			size_t len = (size_t)(dot - opt.pool_name);

			if (strlen(fsname) != len ||
			    strncmp(opt.pool_name, fsname, len) != 0) {
				set_error_code("pool", EINVAL,
					       "pool %s names a different "
					       "filesystem than %s",
					       opt.pool_name, fsname);
				goto out;
			}
			opt.pool_name = dot + 1;
		}
		if (opt.pool_name[0] == '\0' ||
		    strlen(opt.pool_name) > LOV_MAXPOOLNAME) {
			set_error_code("pool", EINVAL,
				       "invalid pool name after the filesystem "
				       "qualifier");
			goto out;
		}
	}

	if (llapi_fd2fid(r.file_fd, &fid) == 0) {
		snprintf(res.fid, sizeof(res.fid), DFID, PFID(&fid));
		res.have_fid = true;
	}

	if (migrate_file(&opt, &r, fsname) < 0)
		goto out;


out:
	if (r.root_fd >= 0)
		close(r.root_fd);
	if (r.file_fd >= 0)
		close(r.file_fd);
	if (r.parent_fd >= 0)
		close(r.parent_fd);

	emit_result(&opt);
	return res.exit_code;
}
