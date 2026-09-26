#!/usr/bin/env python3.11
import os
import sys
import time
import json
import io
import signal
import queue
import threading
import hashlib
import math
import subprocess
from typing import List, Tuple, Dict, Optional
from datetime import datetime
import stat
import argparse
import base64
import errno
import traceback
import secrets
import shutil
import re
import ctypes
import struct
import zlib
import gzip
import heapq
import fcntl
from collections import deque, OrderedDict

try:
    import blake3
except ImportError:
    blake3 = None

# Optional, like blake3: only required when spill_compression='zstd' is
# configured. zstd is roughly seven times faster than zlib at the same ratio,
# but it is not part of the standard library before Python 3.14.
try:
    import zstandard
except ImportError:
    zstandard = None

# Internal helper command used for PATH_MAX-safe lfs operations.
# Not intended for direct user invocation.
LFS_HELPER_COMMAND = "__parallel_tools_lfs_helper__"

PROGRAM_VERSION = "2.1"

# The C helper is the authoritative implementation of per-file Lustre
# migration.  A null migrate_helper configuration first selects an executable
# placed next to this script and then falls back to PATH.
MIGRATE_HELPER_BASENAME = "lustre-migrate-file"
MIGRATE_HELPER_PROTOCOL_SCHEMA = 2
# The helper names itself in every answer. The schema number alone does not
# identify it: any program that prints {"schema":2,...} would pass, and an
# older helper carrying the same schema would be accepted although its
# behaviour differs. Both are checked.
MIGRATE_HELPER_PROGRAM_NAME = "lustre-migrate-file"
MIGRATE_HELPER_MIN_VERSION = (2, 6)
DEFAULT_MIGRATE_ALLOCATION_ATTEMPTS = 32
# The helper's own limits. A value the scheduler accepts and the helper then
# refuses turns into one usage error per file, for the whole run: the
# configuration loads, work starts, and every single invocation exits with
# status 2. They are checked here so the run refuses to start instead.
MAX_MIGRATE_STRIPE_COUNT = 2000          # --stripe-count
MAX_MIGRATE_OST_INDEX = 0xFFFF           # an OST index is 16 bits
MAX_MIGRATE_OST_LIST = 4096              # MAX_BANNED_OSTS in the helper
# How often a worker waiting for the helper looks up whether the run is
# shutting down or has left its schedule window. It bounds how long a blocked
# Lustre call can hold a worker past that point, and costs one wakeup per
# interval while a migration runs.
MIGRATE_HELPER_POLL_SEC = 5.0
# Between SIGTERM and SIGKILL. The helper holds a lease; the kernel drops it
# when the descriptor closes, so both signals are safe, and the shorter this
# is the sooner the worker is free.
MIGRATE_HELPER_KILL_GRACE_SEC = 5.0

HARD_LINK_PRESERVE_MODES = frozenset({
    "lustre2posix",
    "posix2lustre",
    "lustre2lustre",
    "posix2posix",
})
HARD_LINK_ALLOWED_MODES = frozenset({"prohibited"}) | HARD_LINK_PRESERVE_MODES
HARDLINK_TASK_KIND = "resolved_hardlink_groups"

SPILL_COMPRESSION_MODES = ("zlib", "zstd", "none")

# Durability levels for written destination data. Measured on Lustre, one
# fsync of a freshly written 4 KiB file cost 353 ms of the 360 ms that the
# whole file copy took, so where this guarantee is placed decides small-file
# throughput almost on its own.
FSYNC_MODES = ("file", "batch", "hourly")
FSYNC_INTERVAL_SEC = 3600.0

# Below this many recorded directories the repair stays single-threaded: the
# thread setup costs more than the metadata latency it would hide.
DIRECTORY_REPAIR_MIN_PARALLEL_RECORDS = 10000
# Bounded hand-off per repair thread, so the reader cannot outrun the appliers.
DIRECTORY_REPAIR_QUEUE_DEPTH = 1000

# Buffered copy/hash I/O defaults to the benchmarked 8 MiB path.
# buffer_mb is always an integer >= 1. For internal copy it controls both
# the copy read size and hash-verification I/O; for diff/external copy it
# controls each digest reader.

# ------------------------------------------------------------
# Default-Konfiguration
# ------------------------------------------------------------
COMMON_DEFAULTS = {
    "max_workers": 8,
    "batch_max_files": 100,
    "max_retries": 7,              
    "reload_input_file": None,      # optional: File with addtional list of null-terminierten paths
    "remaining_tasks_file": None,   # optional: File to save not-yet-started queued paths on shutdown, NUL-terminated
    "queue_maxsize": 1000,
    "stdin_chunk_size": 16777216,    # 16 MiB
    "max_input_record_bytes": 8388608,  # 8 MiB per NUL-terminated path
    "retry_backoff_base": 2.0,      
    "retry_backoff_max": 60.0,
    # Optional weekly run schedule. If null, work is always allowed.
    # Example:
    # "run_schedule": {
    #   "timezone": "local",
    #   "check_interval_sec": 60,
    #   "windows": {
    #     "mon": [["22:00", "24:00"], ["00:00", "07:00"]],
    #     "tue": [["22:00", "24:00"], ["00:00", "07:00"]],
    #     "wed": [["22:00", "24:00"], ["00:00", "07:00"]],
    #     "thu": [["22:00", "24:00"], ["00:00", "07:00"]],
    #     "fri": [["22:00", "24:00"], ["00:00", "07:00"]],
    #     "sat": [["00:00", "24:00"]],
    #     "sun": [["00:00", "24:00"]]
    #   }
    # }
    "run_schedule": None,
    # "engine": no parallel_tools policy limit. Internal engines may switch
    # to PATH_MAX-safe openat/cwd handling; external engines receive paths as-is.
    # Integer N: paths longer than N encoded bytes are rejected; all allowed
    # paths use the normal/classic engine path with no automatic long-path fallback.
    "max_path_length": "engine",
    # null: trust that no hard links exist and do not inspect st_nlink.
    # prohibited: reject regular files with st_nlink > 1.
    # *2* modes: preserve groups using the named source/destination backends.
    "hard_link": None,
    # Compression of that log: zlib (stdlib, default), zstd (optional module,
    # ~7x faster at the same ratio), or none (plain, inspectable bytes).
    "spill_compression": "zlib",
    # Where this run keeps its own state: the directory-timestamp spill and the
    # not-yet-started paths saved on shutdown. Relative names are resolved
    # against the current working directory. It is checked before anything is
    # written into it and created only when it is actually needed.
    "working_directory": ".parallel_tool_workdir",
    # How much COMPRESSED destination metadata - what directories, and for
    # rsync files, must carry at the end - is held in memory before it is
    # spilled into working_directory. Integer bytes, or a string with a unit
    # ("100MiB", "512KiB", "2GB"). The former name directory_times_maxsize is
    # still accepted.
    "metadata_maxsize": "100MiB",
}

DEFAULT_CONFIG_RSYNC = {
    "method": "rsync",
    "copy_timeout": None,           # z.B. 3600 oder null
    "rsync_zero_bytes_timeout": 300,
    # null trusts the rsync exit code; size_iferr re-checks sizes only after a
    # failed run; hash modes compare content with the diff engine afterwards.
    "verify": "size_iferr",
    "buffer_mb": 8,
    "relative_path": False,
    "xattr": False,
    "permissions": "pog",
    "mtime": True,
    "sparse": False,
    "ignore_existing": False,
    "fsync": "hourly",
    # false: a destination that could only partly be protected during the run
    # is reported in the statistics and the log. true: it also ends the run
    # with exit status 1.
    "require_destination_protection": False,
    "src_root": "/src",
    "dst_root": "/dst",
}

DEFAULT_CONFIG_COPY = {
    "method": "copy",
    "engine": "internal",
    "copy_timeout": None,           # z.B. 3600 oder null
    "verify": "size",              # size, sha256, blake3, blake3thread<N>, blake3threadinga
    "buffer_mb": 8,          # MiB; internal copy + hash buffer / digest buffer
    "relative_path": False,
    "xattr": False,
    "permissions": "pog",
    "mtime": True,
    "sparse": False,
    "skip_existing": False,
    "fsync": "hourly",
    # See DEFAULT_CONFIG_RSYNC.
    "require_destination_protection": False,
    "src_root": "/src",
    "dst_root": "/dst",
}

# The four settings a migrate run cannot be given a default for: they decide
# which OSTs are emptied and how many mirrors a file keeps. 'create migrate'
# writes them as null, and loading a configuration that still has them null is
# refused by name, so an unedited file cannot start a run.
MIGRATE_REQUIRED_KEYS = ("src_root", "stripcount", "banned_osts",
                        "keep_mirroring")
MIGRATE_REQUIRED_HINTS = {
    "src_root": "absolute path of the tree to migrate, e.g. \"/lustre/project\"",
    "stripcount": "stripe count for new mirrors, 1..2000, e.g. 2",
    "banned_osts": "OST indexes to empty, e.g. [10, 11]",
    "keep_mirroring": "false = end with one mirror, true = keep the count",
}

DEFAULT_CONFIG_MIGRATE = {
    "method": "migrate",
    # Must be filled in; see MIGRATE_REQUIRED_KEYS.
    "src_root": None,
    "stripcount": None,
    "banned_osts": None,
    "keep_mirroring": None,
    # Optional, shown at their defaults.
    "poolname": None,
    "allowed_osts": [],
    "migrate_helper": None,
    "migrate_allocation_attempts": DEFAULT_MIGRATE_ALLOCATION_ATTEMPTS,
    "migrate_timeout": None,
}

DEFAULT_CONFIG_DIFF = {
    "method": "diff",
    "verify": "sha256",
    "buffer_mb": 8,          # MiB; parallel digest buffer per reader
    "relative_path": False,
    "xattr": False,
    "permissions": "pog",
    "mtime": True,
    "src_root": "/src",
    "dst_root": "/dst",
}

# ------------------------------------------------------------
# Globaler Zustand
# ------------------------------------------------------------

reload_event = threading.Event()
shutdown_event = threading.Event()
stdin_closed_event = threading.Event()
# Set when input could not be read completely, so the run cannot report success.
input_failed_event = threading.Event()
# Set when not-yet-started work could not be written to remaining_tasks_file.
shutdown_save_failed = threading.Event()
sync_failed_event = threading.Event()
# Set by a SECOND stop signal. The first one means "finish the current work",
# which is what the program promises; the second one says the operator is no
# longer willing to wait for it. Only this event, or a configured
# migrate_timeout, may cut a running migrate helper short.
force_stop_event = threading.Event()

config_lock = threading.Lock()
workers_lock = threading.Lock()
active_tasks_lock = threading.Lock()
migrate_debug_lock = threading.Lock()
# Guards (st_dev, st_ino) pairs currently undergoing mirror operations. Two
# hard-link names of one Lustre inode must never run extend/delete/resync at
# the same time, which is possible whenever hard_link is not a preserve mode.
migrate_inode_lock = threading.Lock()
migrate_inodes_in_progress = set()
# Inodes whose helper could not be confirmed dead, mapped to that pid. Their
# claim is never given back, so this run starts no second helper on them.
migrate_inodes_abandoned = {}
queue_batching_lock = threading.Lock()

worker_context = threading.local()

config = dict()

task_queue: queue.Queue = queue.Queue(maxsize=COMMON_DEFAULTS["queue_maxsize"])
result_queue: queue.Queue = queue.Queue()
admin_queue: queue.Queue = queue.Queue()
hardlink_group_queue: queue.Queue = queue.Queue()

# Capability cache for lfs fid2path. Lustre 2.15 has --link but not -0;
# newer clients can return the complete group as one NUL-terminated stream.
lustre_fid2path_capability_lock = threading.Lock()
lustre_fid2path_print0_supported: Optional[bool] = None

workers = []              # Liste von Thread-Objekten
next_worker_id = 0         # Monotonically increasing ID; thread names are never reused
active_tasks = 0          # Anzahl aktuell laufender Worker-Jobs


# ------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------

def log(msg: str) -> None:
    """Write one operational line to stderr, timestamped.

    The timestamp belongs here rather than at the call sites. Stamping only
    the lines someone remembered to stamp makes the log unusable for the
    questions it exists to answer - when a reload took effect, how long a
    batch ran, whether the stop signal arrived before or after a failure.

    A message carrying its own newlines (a traceback) is stamped on its first
    line only, so the traceback stays readable as one block.
    """
    print(f"{datetime.now()} {msg}", file=sys.stderr, flush=True)


def get_logical_cpu_count() -> int:
    """Return the number of logical CPUs visible to this process.

    os.cpu_count() can return None on unusual platforms. Treat that case as
    one logical CPU so worker/hash budgeting always remains well-defined.
    """
    return max(1, int(os.cpu_count() or 1))


def get_worker_cpu_budget() -> int:
    """Logical CPUs available to workers while reserving one CPU."""
    return max(1, get_logical_cpu_count() - 1)


# One local rsync invocation is three processes, not one: the client forks a
# server child, and the receiver forks the generator. Counted on a running
# local transfer with rsync 3.5.0 - three processes sharing one command line.
# This program never uses a remote shell, so the local structure is the only
# one it produces. N workers therefore put 3N rsync processes on the run
# queue, and a worker count sized against NCPU-1 oversubscribes by three.
RSYNC_PROCESSES_PER_INVOCATION = 3


def worker_process_factor(cfg: Optional[dict]) -> int:
    """How many processes one worker puts on the run queue at a time."""
    if cfg is None:
        return 1
    try:
        method = normalize_method(cfg.get("method", ""))
    except Exception:
        return 1
    return RSYNC_PROCESSES_PER_INVOCATION if method == "rsync" else 1


def warn_if_workers_exceed_cpu_budget(
    worker_count: int, cfg: Optional[dict] = None
) -> None:
    """Warn, but do not reject, configurations that oversubscribe CPUs."""
    ncpu = get_logical_cpu_count()
    budget = max(0, ncpu - 1)
    factor = worker_process_factor(cfg)
    requested = worker_count * factor

    if requested <= budget:
        return

    if factor == 1:
        log(
            f"WARNING: max_workers={worker_count} exceeds NCPU-1={budget} "
            f"(NCPU={ncpu} logical CPUs); CPU oversubscription may reduce performance"
        )
        return

    log(
        f"WARNING: max_workers={worker_count} with method=rsync runs "
        f"{worker_count} x {factor} = {requested} rsync processes, which "
        f"exceeds NCPU-1={budget} (NCPU={ncpu} logical CPUs). One local rsync "
        f"is three processes - the invocation, its server child and the "
        f"generator - so the usable worker count here is NCPU-1 divided by "
        f"three: max_workers={max(1, budget // factor)}. The run continues; "
        f"CPU oversubscription may reduce performance"
    )


def get_blake3_thread_count(worker_count: Optional[int] = None) -> int:
    """Return per-hasher BLAKE3 threads for the CPU-budgeted auto mode.

    One logical CPU is reserved for the main process/OS. The remaining CPU
    budget is divided equally across configured parallel_tools workers. Based
    on benchmark results, a single BLAKE3 hasher is capped at four threads;
    additional intra-file threads did not improve throughput on the target
    workload. At least one BLAKE3 thread is always used.
    """
    if worker_count is None:
        cfg = get_config_snapshot()
        worker_count = int(cfg.get("max_workers", COMMON_DEFAULTS["max_workers"]))
    worker_count = max(1, int(worker_count))
    ncpu_minus_one = max(0, get_logical_cpu_count() - 1)
    cpu_budgeted_threads = max(1, ncpu_minus_one // worker_count)
    return min(4, cpu_budgeted_threads)

def inc_active() -> None:
    global active_tasks
    with active_tasks_lock:
        active_tasks += 1


def dec_active() -> None:
    global active_tasks
    with active_tasks_lock:
        active_tasks -= 1


def get_active() -> int:
    with active_tasks_lock:
        return active_tasks


def get_config_snapshot() -> dict:
    with config_lock:
        return dict(config)


def get_queue_size() -> int:
    try:
        return task_queue.qsize()
    except Exception:
        return -1


def get_unfinished_tasks() -> int:
    """Return queued plus already-dequeued tasks not yet marked done."""
    with task_queue.all_tasks_done:
        return task_queue.unfinished_tasks


def get_inflight_batches() -> int:
    """Batches a worker has dequeued but not yet marked done.

    Unlike the active-task counter this is exact: a worker increments the
    counter only after Queue.get() has returned, so a shutdown check based on
    it could declare the run finished while a batch was already being handed
    to a worker.
    """
    with task_queue.mutex:
        return max(0, task_queue.unfinished_tasks - task_queue._qsize())


def path_display(path_b: bytes) -> str:
    return os.fsdecode(path_b)


def normalize_root_path(name: str, value: str) -> str:
    """Validate and normalize a configured filesystem root."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty absolute path string")
    if "\0" in value:
        raise ValueError(f"{name} must not contain a NUL character")
    if not os.path.isabs(value):
        raise ValueError(f"{name} must be an absolute path")
    return os.path.normpath(value)


def roots_overlap(src_root: str, dst_root: str) -> bool:
    """Return True when either configured root contains the other.

    Both lexical and real paths are checked. The lexical check also works for
    roots that do not exist yet; the real-path check catches existing symlinked
    roots. Sibling trees below a common parent are allowed.
    """

    def overlaps(path_a: str, path_b: str) -> bool:
        try:
            common = os.path.commonpath((path_a, path_b))
        except ValueError:
            return False
        return common == path_a or common == path_b

    if overlaps(src_root, dst_root):
        return True

    src_real = os.path.normpath(os.path.realpath(src_root))
    dst_real = os.path.normpath(os.path.realpath(dst_root))
    if overlaps(src_real, dst_real):
        return True

    # realpath() cannot reveal bind-mount aliases. If the relevant paths exist,
    # compare each root with the existing ancestors of the other tree.
    def root_matches_ancestor(root: str, path: str) -> bool:
        current = path
        while True:
            try:
                if os.path.samefile(root, current):
                    return True
            except OSError:
                pass
            parent = os.path.dirname(current)
            if parent == current:
                return False
            current = parent

    return root_matches_ancestor(src_root, dst_root) or root_matches_ancestor(
        dst_root, src_root
    )


def human_bytes(value: float) -> str:
    """Return a compact IEC representation, e.g. 1.23 GiB."""
    value = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")
    unit_index = 0
    while abs(value) >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1

    if unit_index == 0:
        return f"{value:.0f} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def file_type_name(mode: int) -> bytes:
    """Return a stable ASCII description for an st_mode file type."""
    if stat.S_ISREG(mode):
        return b"regular file"
    if stat.S_ISDIR(mode):
        return b"directory"
    if stat.S_ISLNK(mode):
        return b"symlink"
    if stat.S_ISFIFO(mode):
        return b"FIFO"
    if stat.S_ISSOCK(mode):
        return b"socket"
    if stat.S_ISCHR(mode):
        return b"character device"
    if stat.S_ISBLK(mode):
        return b"block device"
    return b"unknown file type"


def normalize_method(value: str) -> str:
    value = value.strip().lower()
    if value in ("rsync", "copy", "migrate", "diff"):
        return value
    raise ValueError(f"unsupported method: {value}")


PERMISSION_FLAGS = ("p", "o", "g")


def normalize_permissions(value) -> str:
    """Validate which ownership/mode classes are transferred or compared.

    p: access mode bits, including setuid/setgid/sticky
    o: owning user
    g: owning group

    null and "" both mean that no mode or ownership handling takes place. The
    classes are separate because preserving ownership needs privileges while
    preserving the mode does not, so an unprivileged run can ask for "p" alone
    instead of silently failing on every chown.
    """
    if value is None:
        return ""
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(
            "permissions must be null or a string built from 'p', 'o' and 'g'"
        )

    seen = set()
    for flag in value.strip().lower():
        if flag not in PERMISSION_FLAGS:
            raise ValueError(
                f"permissions may contain only 'p', 'o' and 'g': {value!r}"
            )
        seen.add(flag)
    return "".join(flag for flag in PERMISSION_FLAGS if flag in seen)


def permissions_mode(cfg: dict) -> bool:
    return "p" in (cfg.get("permissions") or "")


def permissions_owner(cfg: dict) -> bool:
    return "o" in (cfg.get("permissions") or "")


def permissions_group(cfg: dict) -> bool:
    return "g" in (cfg.get("permissions") or "")


def permissions_ownership(cfg: dict) -> bool:
    """True when the owning user or group has to be applied or compared."""
    return permissions_owner(cfg) or permissions_group(cfg)


def preserves_any_metadata(cfg: dict) -> bool:
    return bool(cfg.get("permissions")) or bool(cfg.get("mtime", False))


def mtime_enabled(cfg: dict) -> bool:
    return bool(cfg.get("mtime", False))


def normalize_fsync_mode(value) -> Optional[str]:
    """Validate where the durability barrier for written data is placed.

    file    fsync every file and its parent directory, as it is written.
    batch   one syncfs() per finished batch.
    hourly  one syncfs() per hour and one at the end of the run.
    null    none at all; the caller is responsible, for example with sync -f.

    An fsync only buys crash consistency of the rename: without it the kernel
    may make the new name durable before the data behind it, leaving a
    destination that exists under the right name with unwritten blocks. It
    does not make the verification more meaningful, because the read-back
    after it is still served from the page cache.
    """
    if value is None or value is False:
        return None
    if value is True:
        return "file"
    if not isinstance(value, str):
        raise ValueError(
            "fsync must be null, false, 'file', 'batch' or 'hourly'"
        )
    normalized = value.strip().lower()
    if normalized in ("", "none", "false"):
        return None
    if normalized not in FSYNC_MODES:
        raise ValueError(
            "fsync must be null, false, 'file', 'batch' or 'hourly'"
        )
    return normalized


_syncfs_lock = threading.Lock()
_syncfs_entry = None


def get_syncfs():
    """Resolve syncfs(2), which Python does not expose.

    os.sync() is not an alternative here: it flushes every mounted filesystem
    and would stall unrelated jobs on a shared machine. syncfs() is limited to
    the filesystem holding the given descriptor, the same thing `sync -f` does.
    """
    global _syncfs_entry
    with _syncfs_lock:
        if _syncfs_entry is None:
            try:
                libc = ctypes.CDLL(None, use_errno=True)
                entry = libc.syncfs
                entry.argtypes = [ctypes.c_int]
                entry.restype = ctypes.c_int
                _syncfs_entry = entry
            except (OSError, AttributeError):
                _syncfs_entry = False
        return _syncfs_entry or None


_syncfs_warned = threading.Event()


def sync_destination_filesystem(cfg: dict, reason: str) -> bool:
    """Flush the filesystem holding dst_root and wait for it.

    A refused or failing flush is recorded in sync_failed_event, because a run
    that promised durability and did not get it must not report success.
    syncfs(2) merely being absent is a property of the platform rather than a
    failure of this run, so it stays a warning.
    """
    destination = cfg.get("dst_root")
    if not destination:
        return False

    entry = get_syncfs()
    if entry is None:
        if not _syncfs_warned.is_set():
            _syncfs_warned.set()
            log(
                "WARNING: syncfs(2) is not available; written data is not "
                "flushed explicitly. Use fsync='file' or flush externally."
            )
        return False

    started = time.monotonic()
    try:
        descriptor = os.open(os.fsencode(destination), os.O_RDONLY)
    except OSError as exc:
        log(f"WARNING: cannot open {destination} for syncfs: {exc}")
        sync_failed_event.set()
        return False
    try:
        if entry(descriptor) != 0:
            code = ctypes.get_errno()
            log(f"WARNING: syncfs failed: {os.strerror(code)}")
            sync_failed_event.set()
            return False
    finally:
        os.close(descriptor)

    log(
        f"syncfs on {destination} ({reason}) took "
        f"{time.monotonic() - started:.2f}s"
    )
    return True


periodic_sync_lock = threading.Lock()
periodic_sync_due = False


def mark_periodic_sync_due() -> None:
    """Note that the interval has elapsed; the next free worker will flush.

    The scheduler owns the clock but must not own the flush: its loop also
    emits results, replaces dead workers, handles SIGHUP and observes
    shutdown, and a syncfs on a loaded filesystem can stall all of that.
    """
    global periodic_sync_due
    with periodic_sync_lock:
        periodic_sync_due = True


def claim_periodic_sync() -> bool:
    """Take the pending flush, if there is one. At most one caller wins."""
    global periodic_sync_due
    with periodic_sync_lock:
        if not periodic_sync_due:
            return False
        periodic_sync_due = False
        return True


def fsync_per_file(cfg: dict) -> bool:
    """True when every written object is flushed as it is created."""
    return cfg.get("fsync") == "file"


def fsync_if_per_file(cfg: dict, descriptor: int) -> None:
    if fsync_per_file(cfg):
        os.fsync(descriptor)


def fsync_barrier_per_batch(cfg: dict) -> bool:
    """True when a finished batch has to be flushed before the next one starts.

    Deliberately NOT fsync_needs_barrier(): that one answers "does this run
    owe a barrier at all", which is true for "hourly" as well. Using it here
    made "hourly" flush after every batch, exactly like "batch", so the one
    thing the mode exists for - not paying the serialisation per batch - did
    not happen. The end-of-run barrier and the hourly mark are separate sites
    and keep their own conditions.
    """
    mode = cfg.get("fsync")
    if mode == "batch":
        return True
    # fsync="file" with an rsync that has no --fsync: parallel_tools never
    # sees those descriptors, so a barrier per batch is what is left of the
    # per-file promise. Anything wider would be weaker than what was asked.
    return mode == "file" and _rsync_fsync_warned.is_set()


def fsync_needs_barrier(cfg: dict) -> bool:
    """True when written data still needs a filesystem-wide barrier.

    "batch" and "hourly" are barrier modes by definition. "file" normally
    needs none, because every descriptor is flushed as it is written - except
    for the files an rsync without --fsync writes. parallel_tools never sees
    those descriptors, so the per-file promise cannot be forwarded and the
    barrier is what is left of it. Without this the run warned about the
    missing option and then flushed nothing at all, because every barrier was
    conditioned on fsync being "batch" or "hourly".
    """
    mode = cfg.get("fsync")
    if mode in ("batch", "hourly"):
        return True
    # "file" too, and not only when rsync lacked --fsync. The directory
    # timestamps are applied with utimensat AFTER the last file was flushed,
    # and the hard-link phase publishes names after that again. Neither is
    # covered by a per-file fsync, and rsync's --fsync promises written files,
    # not the directories they live in. Without a barrier here the very last
    # metadata this run produces is the one part that is not durable.
    return mode == "file"


def normalize_spill_compression(value) -> str:
    """Validate the compression used for the directory-timestamp log."""
    if value is None:
        return "none"
    if not isinstance(value, str):
        raise ValueError(
            "spill_compression must be 'zlib', 'zstd' or 'none'"
        )

    normalized = value.strip().lower()
    if normalized not in SPILL_COMPRESSION_MODES:
        raise ValueError(
            "spill_compression must be 'zlib', 'zstd' or 'none'"
        )
    if normalized == "zstd" and zstandard is None:
        raise ValueError(
            "spill_compression='zstd' requires the Python module 'zstandard'; "
            "use 'zlib' to stay within the standard library"
        )
    return normalized


def normalize_hard_link_mode(value) -> Optional[str]:
    """Validate the hard-link policy shared by every processing method."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(
            "hard_link must be null, 'prohibited', 'lustre2posix', "
            "'posix2lustre', 'lustre2lustre', or 'posix2posix'"
        )

    normalized = value.strip().lower()
    if normalized not in HARD_LINK_ALLOWED_MODES:
        raise ValueError(
            "hard_link must be null, 'prohibited', 'lustre2posix', "
            "'posix2lustre', 'lustre2lustre', or 'posix2posix'"
        )
    return normalized


def hard_link_preserves_groups(mode: Optional[str]) -> bool:
    return mode in HARD_LINK_PRESERVE_MODES


def hard_link_source_backend(mode: str) -> str:
    if mode not in HARD_LINK_PRESERVE_MODES:
        raise ValueError(f"hard_link mode does not preserve groups: {mode!r}")
    return mode.split("2", 1)[0]


def hard_link_destination_backend(mode: str) -> str:
    if mode not in HARD_LINK_PRESERVE_MODES:
        raise ValueError(f"hard_link mode does not preserve groups: {mode!r}")
    return mode.split("2", 1)[1]


def normalize_copy_engine(value):
    """Normalize copy.engine.

    Supported forms:
      * "internal"
      * argv template list, e.g. ["/usr/bin/cp", "-d", "$SRC", "$DST"]

    $SRC and $DST must each be standalone argv elements. This keeps arbitrary
    POSIX path bytes out of shell parsing and avoids quoting ambiguities.
    """
    if value == "internal":
        return "internal"
    if not isinstance(value, list) or not value:
        raise ValueError(
            "copy engine must be 'internal' or a non-empty argv list"
        )
    if not all(isinstance(arg, str) and arg and "\0" not in arg for arg in value):
        raise ValueError("copy engine argv entries must be non-empty strings without NUL")
    if value.count("$SRC") != 1 or value.count("$DST") != 1:
        raise ValueError(
            "external copy engine must contain exactly one standalone $SRC and $DST"
        )
    return list(value)


def expand_copy_engine_argv(engine, src_b: bytes, dst_b: bytes):
    if engine == "internal":
        raise ValueError("internal copy engine has no external argv")
    argv = [
        src_b if arg == "$SRC" else dst_b if arg == "$DST" else os.fsencode(arg)
        for arg in engine
    ]
    # argv[0] becomes the descriptor that was inspected at startup, so what
    # was judged is what runs. Falls back to the configured name only where
    # /proc is absent, which resolve_trusted_executable() reports once.
    argv[0] = os.fsencode(resolve_trusted_executable(engine[0]))
    return argv


def verify_mode_allowed(mode: Optional[str], method: str) -> bool:
    """Return True when a normalized verify mode is legal for one method.

    Timestamps are no longer part of verify; they are an independent ``mtime``
    setting, so there is no combined size_mtime mode any more.

    copy and diff always compare at least the size, so null is not a valid
    choice for them. rsync may skip the check entirely (null) or limit it to
    sizes of a failed transfer (size_iferr).
    """
    if mode is None or mode == "size_iferr":
        return method == "rsync"
    return mode == "size" or is_hash_compare_mode(mode)


def normalize_compare_mode(value, method: Optional[str] = None) -> Optional[str]:
    """Normalize a verification mode and canonicalize BLAKE3 thread modes.

    Explicit-thread syntax is ``blake3thread<N>`` where N >= 1. When ``method``
    is given, the mode is additionally checked against that method.
    """
    if value is None:
        normalized = None
    elif not isinstance(value, str):
        raise ValueError("verify mode must be a string or null")
    else:
        text = value.strip().lower()
        if text == "size_mtime":
            raise ValueError(
                "verify='size_mtime' no longer exists; use verify='size' "
                "together with mtime=true"
            )
        if text in ("size", "size_iferr", "sha256", "blake3", "blake3threadinga"):
            normalized = text
        else:
            match = re.fullmatch(r"blake3thread(\d+)", text)
            if match is None:
                raise ValueError(f"unsupported verify mode: {value}")
            thread_count = int(match.group(1))
            if thread_count < 1:
                raise ValueError("BLAKE3 thread count must be >= 1")
            normalized = f"blake3thread{thread_count}"

    if method is not None and not verify_mode_allowed(normalized, method):
        if method == "rsync":
            allowed = "null, 'size', 'size_iferr', 'sha256', 'blake3', 'blake3thread<N>' or 'blake3threadinga'"
        else:
            allowed = "'size', 'sha256', 'blake3', 'blake3thread<N>' or 'blake3threadinga'"
        raise ValueError(
            f"verify={normalized!r} is not valid for method={method}; "
            f"allowed values are {allowed}"
        )

    return normalized


def is_blake3_mode(value: Optional[str]) -> bool:
    if value in ("blake3", "blake3threadinga"):
        return True
    return isinstance(value, str) and re.fullmatch(r"blake3thread\d+", value) is not None


def is_hash_compare_mode(value: Optional[str]) -> bool:
    return value == "sha256" or is_blake3_mode(value)


def get_blake3_threads_for_mode(
    mode: str, worker_count: Optional[int] = None
) -> int:
    """Return the effective max_threads value for a normalized BLAKE3 mode."""
    if mode == "blake3":
        return 1
    if mode == "blake3threadinga":
        return get_blake3_thread_count(worker_count)

    match = re.fullmatch(r"blake3thread(\d+)", mode)
    if match is not None:
        return int(match.group(1))

    raise ValueError(f"unsupported BLAKE3 verify mode: {mode}")


def uses_parallel_digest(cfg: dict) -> bool:
    """Return True when one worker hashes source and destination concurrently."""
    mode = cfg.get("verify")
    if not is_hash_compare_mode(mode):
        return False

    method = cfg.get("method")
    if method in ("diff", "rsync"):
        # rsync verification runs the diff engine afterwards.
        return True
    if method == "copy":
        # Internal sha256/blake3 uses source hashing fused into the copy stream.
        # External engines cannot expose that stream, so they use parallel
        # post-copy source+destination hashing.
        engine = cfg.get("engine", "internal")
        if engine != "internal":
            return True
        # Experimental internally-threaded BLAKE3 modes are not fused.
        return mode not in ("sha256", "blake3")
    return False


def hash_threads_per_hasher(mode: Optional[str], workers: int) -> int:
    if mode == "sha256":
        return 1
    if is_blake3_mode(mode):
        return get_blake3_threads_for_mode(mode, workers)
    return 1


def warn_if_hash_threads_exceed_cpu_budget(cfg: dict) -> None:
    """Warn when configured hashing can exceed the logical-CPU budget."""
    mode = cfg.get("verify")
    if not is_hash_compare_mode(mode):
        return

    workers = max(1, int(cfg.get("max_workers", COMMON_DEFAULTS["max_workers"])))
    per_hasher = hash_threads_per_hasher(mode, workers)
    concurrent_hashers = 2 if uses_parallel_digest(cfg) else 1
    budget = max(1, get_logical_cpu_count() - 1)
    requested = workers * concurrent_hashers * per_hasher

    # Plain fused sha256/blake3 has the same one-CPU-per-worker budget already
    # covered by warn_if_workers_exceed_cpu_budget(). Avoid duplicate warnings.
    if concurrent_hashers == 1 and per_hasher == 1:
        return

    if requested > budget:
        detail = (
            f"max_workers={workers} * concurrent_hashers_per_worker={concurrent_hashers} "
            f"* threads_per_hasher={per_hasher} = {requested}"
        )
        log(
            "WARNING: hash concurrency may oversubscribe logical CPUs: "
            f"{detail} > NCPU-1={budget}; run continues"
        )


def buffers_per_worker(cfg: dict) -> int:
    """Maximum simultaneously live configured I/O buffers per worker."""
    method = cfg.get("method")
    if method == "copy":
        if cfg.get("engine", "internal") == "internal":
            # Plain/fused internal copy has one copy buffer. Experimental
            # non-fused hash modes use two concurrent digest buffers later.
            return 2 if uses_parallel_digest(cfg) else 1
        return 2 if is_hash_compare_mode(cfg.get("verify")) else 0
    if method in ("diff", "rsync"):
        return 2 if is_hash_compare_mode(cfg.get("verify")) else 0
    return 0


def warn_if_buffer_exceeds_memory_budget(cfg: dict) -> None:
    """Warn when configured copy/digest buffers may exceed 1 GiB aggregate."""
    buffer_mb = cfg.get("buffer_mb")
    if buffer_mb is None:
        return

    per_worker = buffers_per_worker(cfg)
    if per_worker == 0:
        return

    workers = max(1, int(cfg.get("max_workers", COMMON_DEFAULTS["max_workers"])))
    total_mb = int(buffer_mb) * workers * per_worker
    if total_mb > 1024:
        log(
            "WARNING: configured I/O buffers may use more than 1 GiB: "
            f"buffer_mb={buffer_mb} MiB * max_workers={workers} "
            f"* buffers_per_worker={per_worker} = {total_mb} MiB; run continues"
        )


def buffer_bytes(buffer_mb: int) -> int:
    """Convert the validated positive buffer_mb setting to bytes."""
    return int(buffer_mb) * 1024 * 1024


_process_umask = None
_process_umask_lock = threading.Lock()


def get_process_umask() -> int:
    """Return this process' umask, probed exactly once.

    os.umask() can only read by writing, so probing it from several threads
    would briefly expose a wrong value to concurrent file creation.
    """
    global _process_umask
    with _process_umask_lock:
        if _process_umask is None:
            current = os.umask(0o022)
            os.umask(current)
            _process_umask = current
        return _process_umask


_zero_reference_lock = threading.Lock()
_zero_reference_buffer = b""


def buffer_is_all_zero(data: bytes) -> bool:
    """Return True when the buffer contains nothing but NUL bytes.

    Comparing against a shared reference buffer uses memcmp, which aborts at
    the first differing byte. Dense data therefore costs almost nothing, while
    a real zero block runs at memory bandwidth. Counting bytes instead would
    always scan the whole buffer and measured roughly 15x slower.
    """
    global _zero_reference_buffer

    length = len(data)
    if not length:
        return False

    reference = _zero_reference_buffer
    if len(reference) < length:
        with _zero_reference_lock:
            if len(_zero_reference_buffer) < length:
                _zero_reference_buffer = bytes(length)
            reference = _zero_reference_buffer

    return data == (
        reference if len(reference) == length else reference[:length]
    )


_system_path_max_cache = None
_system_path_max_lock = threading.Lock()


def open_protected_file(
    path: str,
    *,
    append: bool,
    mode: int = 0o600,
    exclusive: bool = False,
) -> int:
    """Open a predictable output path without ever writing through a symlink.

    The program is routinely run as root and writes several files whose names
    an unprivileged user can guess. Checking the path before opening it would
    be a time-of-check-to-time-of-use race: between the check and the open the
    name can be replaced, and losing that race once is enough.

    The guarantees therefore come from the open itself and from the resulting
    descriptor, never from a preceding path lookup:

      O_NOFOLLOW  refuses a symlink at the final component.
      O_EXCL      refuses any pre-existing name (when exclusive=True).
      fstat()     inspects the object actually opened, so a regular file an
                  attacker placed beforehand is still rejected. Working on the
                  descriptor leaves no window between checking and using it.

    O_NOFOLLOW only protects the last component. A symlinked parent directory
    is not covered, so predictable outputs belong in a directory that
    unprivileged users cannot modify.
    """
    # O_TRUNC is deliberately NOT part of the open: the kernel would apply it
    # before the checks below could refuse the file, so a hard-linked or
    # foreign-owned victim would already be empty by the time the refusal is
    # raised. Truncation happens after the descriptor has been accepted.
    flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW
    if append:
        flags |= os.O_APPEND
    if exclusive:
        flags |= os.O_EXCL

    fd = os.open(os.fsencode(path), flags, mode)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise OSError(
                errno.EPERM,
                f"refusing to write to a non-regular file: {path}",
            )
        if st.st_nlink != 1:
            raise OSError(
                errno.EPERM,
                f"refusing to write to a hard-linked file ({st.st_nlink} links): {path}",
            )
        expected_uid = os.geteuid()
        if st.st_uid != expected_uid:
            raise OSError(
                errno.EPERM,
                f"refusing to write to a file owned by uid {st.st_uid} "
                f"instead of {expected_uid}: {path}",
            )
        if not append:
            os.ftruncate(fd, 0)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    return fd


def warn_if_directory_is_world_writable(directory: str) -> None:
    """Warn when predictable output lands in a directory others can modify.

    O_NOFOLLOW covers the file name, not the path leading to it. A writable
    parent also lets anyone pre-create the name, which the descriptor checks
    then reject at the cost of a lost diagnostic.
    """
    try:
        st = os.stat(directory)
    except OSError:
        return
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        log(
            f"WARNING: {directory} is writable by group or other; predictable "
            f"output file names there can be pre-created by another user"
        )


# ------------------------------------------------------------
# Externe Programme
# ------------------------------------------------------------

TRUSTED_EXECUTABLE_PATH = ("/usr/bin", "/bin", "/usr/sbin", "/sbin", "/usr/local/bin")

_trusted_executables = {}
_trusted_executable_lock = threading.Lock()


def directory_chain_is_safe(directory: str) -> Optional[str]:
    """Return why this directory or one above it can be modified by others.

    A writable directory anywhere in the chain lets somebody rename it and put
    their own in its place, which defeats every check made on what is inside.
    The sticky bit is the exception that makes /tmp usable: there, only the
    owner of an entry may remove or rename it.

    Returns None when the whole chain is sound.
    """
    while True:
        try:
            dir_st = os.stat(directory)
        except OSError as exc:
            return f"{directory}: {exc}"
        if dir_st.st_mode & (stat.S_IWGRP | stat.S_IWOTH) and not (
            dir_st.st_mode & stat.S_ISVTX
        ):
            return f"{directory} is writable by group or other"
        # Mode bits alone are not enough. A directory owned by somebody else
        # may be 0700 and still let its owner rename or replace what is inside
        # it, so every check made further down is theirs to defeat. Only this
        # user and root are accepted as owners.
        if dir_st.st_uid not in (os.geteuid(), 0):
            return (
                f"{directory} is owned by uid {dir_st.st_uid}, who can replace "
                f"what is inside it"
            )
        parent = os.path.dirname(directory)
        if parent == directory:
            return None
        directory = parent


def _executable_directory_is_safe(path: str) -> Optional[str]:
    """Refuse a program whose directory chain somebody else can modify.

    The descriptor checks below cover the file that is executed. This covers
    the next run and every other tool on the machine: a writable parent means
    the binary can be replaced at will.
    """
    return directory_chain_is_safe(os.path.dirname(path) or "/")


# ------------------------------------------------------------
# Arbeitsverzeichnis
# ------------------------------------------------------------

WORKING_DIRECTORY_MODE = 0o700

_working_directory_lock = threading.Lock()
_working_directory_path: Optional[str] = None
_working_directory_problem: Optional[str] = None
_working_directory_fd = -1


def parse_size_bytes(name: str, value) -> int:
    """Accept a byte count as an integer or as a string with a unit."""
    units = {
        "B": 1, "KB": 10 ** 3, "MB": 10 ** 6, "GB": 10 ** 9, "TB": 10 ** 12,
        "KIB": 2 ** 10, "MIB": 2 ** 20, "GIB": 2 ** 30, "TIB": 2 ** 40,
    }
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a byte count, not a boolean")
    if isinstance(value, int):
        number, factor = value, 1
    elif isinstance(value, str):
        text = value.strip().upper().replace(" ", "")
        suffix = ""
        while text and text[-1].isalpha():
            suffix = text[-1] + suffix
            text = text[:-1]
        if not text or suffix not in units and suffix != "":
            raise ValueError(
                f"{name}: cannot read {value!r} as a size; use bytes or a "
                f"unit such as 100MiB, 512KiB, 2GB"
            )
        try:
            number = int(text)
        except ValueError:
            raise ValueError(
                f"{name}: cannot read {value!r} as a size; the number part "
                f"must be a whole number"
            ) from None
        factor = units.get(suffix or "B", 1)
    else:
        raise ValueError(f"{name} must be a byte count or a size string")
    total = number * factor
    if total <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return total


def _judge_working_directory_fd(fd: int, path: str) -> Optional[str]:
    """Judge the directory that was actually opened, not the name."""
    st = os.fstat(fd)
    if not stat.S_ISDIR(st.st_mode):
        return f"{path} is not a directory"
    expected_uid = os.geteuid()
    if st.st_uid != expected_uid:
        return (
            f"{path} is owned by uid {st.st_uid} instead of {expected_uid}"
        )
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        return f"{path} is writable by group or other"
    return None


def resolve_existing_prefix(path: str) -> str:
    """Resolve the part of this path that exists, symlinks and all.

    os.path.abspath() only cleans a name up; it resolves nothing. A check that
    compares cleaned-up names therefore says "outside dst_root" about a path
    whose parent is a symlink INTO dst_root, and as root that lets whoever
    controls that symlink decide where a directory is created. realpath() on
    the whole path is not enough either, because the last component usually
    does not exist yet - so the longest existing prefix is resolved and the
    remainder appended.
    """
    absolute = os.path.abspath(os.path.expanduser(path))
    remainder = []
    current = absolute
    while True:
        if os.path.lexists(current):
            resolved = os.path.realpath(current)
            return os.path.join(resolved, *reversed(remainder)) if remainder else resolved
        parent = os.path.dirname(current)
        if parent == current:
            return absolute
        remainder.append(os.path.basename(current))
        current = parent


def path_is_inside(candidate: str, root: str) -> bool:
    """Both sides resolved first: a symlinked parent must not slip through."""
    resolved_root = os.path.realpath(root)
    resolved_candidate = resolve_existing_prefix(candidate)
    return (
        resolved_candidate == resolved_root
        or resolved_candidate.startswith(resolved_root + os.sep)
    )


def inspect_working_directory(path: str) -> Optional[str]:
    """Judge the working directory without creating anything.

    Called at startup so an unsafe location is reported while the operator is
    still watching, rather than at the first spill hours later. A directory
    that does not exist yet is fine as long as the chain above it is sound -
    nobody but this run can create it there.
    """
    # Judged where it really lands, not where the name says. Everything below
    # works on that resolved location.
    absolute = resolve_existing_prefix(path)
    chain = directory_chain_is_safe(os.path.dirname(absolute) or "/")
    if chain is not None:
        return chain
    try:
        fd = os.open(
            absolute,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
            | os.O_CLOEXEC,
        )
    except FileNotFoundError:
        return None
    except OSError as exc:
        return f"cannot open {absolute}: {exc}"
    try:
        return _judge_working_directory_fd(fd, absolute)
    finally:
        os.close(fd)


def prepare_working_directory(cfg: dict) -> None:
    """Record where the run keeps its state, and whether that place is sound.

    Nothing is created here. Most runs never spill and never leave anything
    behind, and creating a directory on every start would be a visible side
    effect for a file that is usually not needed.
    """
    global _working_directory_path, _working_directory_problem
    path = cfg.get("working_directory") or COMMON_DEFAULTS["working_directory"]
    absolute = resolve_existing_prefix(path)
    problem = None
    # Writing state into the data would defeat the very repair it exists for:
    # removing the file at the end changes the mtime of a directory that was
    # just restored. An explicitly configured path is refused at load time; the
    # default lands here, where it is reported and dropped.
    for root_key in ("src_root", "dst_root"):
        root = cfg.get(root_key)
        if not root:
            continue
        if path_is_inside(absolute, root):
            problem = (
                f"{absolute} is inside {root_key} ({os.path.realpath(root)}); "
                f"state must not be written into the tree being copied"
            )
            break
    if problem is None:
        problem = inspect_working_directory(absolute)
    with _working_directory_lock:
        _working_directory_path = absolute
        _working_directory_problem = problem
    if problem is None:
        log(f"working directory: {absolute}")
    else:
        log(
            f"WARNING: working directory {absolute} cannot be used: "
            f"{problem}. Nothing is written there. A run that needs it - a "
            f"directory-timestamp spill, or saving not-yet-started paths on "
            f"shutdown - fails rather than writing there anyway. Set "
            f"working_directory to a directory outside the copied trees that "
            f"only this user can change"
        )


def working_directory_problem() -> Optional[str]:
    with _working_directory_lock:
        return _working_directory_problem


def working_directory_fd() -> int:
    """Open the working directory on first use, creating it if needed.

    Returns -1 when the location was refused at startup. Every file this run
    writes there is created THROUGH this descriptor, so no pathname is
    resolved a second time and the directory that was judged is the directory
    that is written to.
    """
    global _working_directory_fd, _working_directory_problem
    with _working_directory_lock:
        if _working_directory_fd >= 0:
            return _working_directory_fd
        if _working_directory_problem is not None or not _working_directory_path:
            return -1
        path = _working_directory_path
        try:
            os.mkdir(path, WORKING_DIRECTORY_MODE)
        except FileExistsError:
            pass
        except OSError as exc:
            _working_directory_problem = f"cannot create {path}: {exc}"
            log(f"WARNING: {_working_directory_problem}")
            return -1
        try:
            fd = os.open(
                path,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
                | os.O_CLOEXEC,
            )
        except OSError as exc:
            _working_directory_problem = f"cannot open {path}: {exc}"
            log(f"WARNING: {_working_directory_problem}")
            return -1
        problem = _judge_working_directory_fd(fd, path)
        if problem is not None:
            os.close(fd)
            _working_directory_problem = problem
            log(
                f"WARNING: refusing to use the working directory: {problem}"
            )
            return -1
        _working_directory_fd = fd
        return fd


def working_directory_display() -> str:
    with _working_directory_lock:
        return _working_directory_path or "(unset)"


_run_state_id = None
_run_state_id_lock = threading.Lock()


def run_state_id() -> str:
    """One identifier per run, shared by every state file it leaves behind.

    Timestamp plus a short random block. The random part is NOT what makes
    these files safe - the directory check is - it only keeps two runs started
    in the same second out of each other's files. The timestamp is there so a
    human looking at three leftovers can tell which is the newest, and the
    shared id so they can tell which belong together.
    """
    global _run_state_id
    with _run_state_id_lock:
        if _run_state_id is None:
            _run_state_id = (
                f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-"
                f"{secrets.token_hex(2)}"
            )
        return _run_state_id


def run_state_name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{run_state_id()}{suffix}"


def resolve_run_state_path(configured, prefix: str, suffix: str) -> Optional[str]:
    """Where a state file goes: named absolutely, or inside working_directory.

    null means "pick a name for me". A relative name is taken as relative to
    the working directory rather than to whatever directory the program
    happened to be started from - that is the whole point of having one place
    for this run's state. An absolute name is the operator's own choice and is
    used as given.

    Returns None when there is no usable working directory and none was named;
    the caller then knows the file cannot be written rather than writing it
    somewhere unchecked.
    """
    if configured and os.path.isabs(os.path.expanduser(configured)):
        return os.path.abspath(os.path.expanduser(configured))

    fd = working_directory_fd()
    if fd < 0:
        return None
    name = configured if configured else run_state_name(prefix, suffix)
    if os.path.sep in name or name in (".", "..") or name.startswith("."):
        # A relative value is a NAME inside the working directory, not a path
        # into it. "../elsewhere" would leave the directory that was checked,
        # which is the one thing the directory check is for.
        raise ValueError(
            f"{prefix}: a relative name must be a plain file name inside "
            f"working_directory, not a path: {name!r}"
        )
    return os.path.join(working_directory_display(), name)


def descriptor_pathname(descriptor: int) -> Optional[bytes]:
    """A pathname that resolves to one open descriptor, or None.

    /dev/fd/N is the spelling both target platforms understand: on Linux it is
    a symlink to /proc/self/fd/N, on BSD and macOS it is its own filesystem.
    Handing this to a child instead of a name in the destination directory
    removes the directory lookup - and with it every race over that name.
    """
    for template in (b"/dev/fd/%d", b"/proc/self/fd/%d"):
        candidate = template % descriptor
        if os.path.exists(candidate):
            return candidate
    return None


def _check_executable_descriptor(descriptor: int, path: str) -> Optional[str]:
    """Judge the object that was actually opened, not the name it had."""
    try:
        st = os.fstat(descriptor)
    except OSError as exc:
        return str(exc)
    if not stat.S_ISREG(st.st_mode):
        return "not a regular file"
    if not st.st_mode & 0o111:
        return "not executable"
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        return "writable by group or other"
    # Running as root, only root may own what root executes. Otherwise the
    # caller's own binaries are acceptable besides root's.
    euid = os.geteuid()
    if euid == 0:
        if st.st_uid != 0:
            return f"owned by uid {st.st_uid}, not by root"
    elif st.st_uid not in (0, euid):
        return f"owned by uid {st.st_uid}, neither root nor {euid}"
    return _executable_directory_is_safe(path)


def resolve_trusted_executable(name: str) -> str:
    """Pin one helper program and return the argv[0] that runs exactly it.

    Checking a pathname and then executing that pathname leaves a window: the
    name can be pointed at something else in between, and as root that decides
    which code runs. The program is therefore opened once, judged on the
    resulting descriptor, and executed through /proc/self/fd - so what was
    inspected is what runs, for this and every later invocation.

    An executable that fails the checks is REFUSED, not warned about. The
    alternative - running it anyway - makes the check a diagnostic that
    changes nothing, and substituting a different binary would silently change
    what the run does.

    Every caller that spawns one of these must pass trusted_exec_fds() as
    pass_fds, otherwise the descriptor does not reach the child.
    """
    with _trusted_executable_lock:
        cached = _trusted_executables.get(name)
    if cached is not None:
        if isinstance(cached, Exception):
            raise cached
        return cached[0]

    def remember_failure(error: Exception):
        with _trusted_executable_lock:
            _trusted_executables[name] = error
        return error

    resolved = shutil.which(name)
    if resolved is None:
        for directory in TRUSTED_EXECUTABLE_PATH:
            candidate = os.path.join(directory, name)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                resolved = candidate
                break
    if resolved is None:
        raise remember_failure(
            OSError(errno.ENOENT, f"required helper program not found: {name}")
        )

    resolved = os.path.realpath(resolved)
    open_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        open_flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(resolved, open_flags)
    except OSError as exc:
        raise remember_failure(
            OSError(exc.errno, f"cannot open {name} at {resolved}: {exc}")
        )

    problem = _check_executable_descriptor(descriptor, resolved)
    if problem is not None:
        if os.geteuid() == 0:
            # As root, whoever can write there decides what runs with root's
            # privileges. That is not something to note and continue past.
            os.close(descriptor)
            raise remember_failure(
                PermissionError(
                    errno.EPERM,
                    f"refusing to run {name} at {resolved}: {problem}. "
                    f"Running as root, this program is chosen by whoever can "
                    f"write there. Fix the permissions, or point PATH at a "
                    f"protected copy."
                )
            )
        # Unprivileged: the exposure is to the user's own account, and an
        # ordinary package manager installs into exactly such a directory.
        # Refusing would make the tool unusable for the case it does not
        # protect anyway; the descriptor is still pinned, so the binary
        # cannot be swapped between here and exec.
        log(
            f"WARNING: {name} at {resolved} is not protected against "
            f"replacement ({problem}); under root this would be refused"
        )

    # Deliberately /proc/self/fd and not the /dev/fd spelling used for the
    # copy engine's destination: those two paths look alike but are not the
    # same thing to exec(). On Linux /dev/fd is a symlink to /proc/self/fd and
    # either works; on a BSD-style /dev/fd, executing through the descriptor
    # is refused with EACCES. Naming the one that is known to exec keeps this
    # from turning into a platform surprise.
    argv0 = f"/proc/self/fd/{descriptor}"
    if not os.path.exists(argv0):
        # No procfs: the descriptor cannot be turned into something exec()
        # accepts, so the name has to be used. The checks above still applied,
        # but the window between them and the exec is back. Said once, not per
        # invocation.
        os.close(descriptor)
        if not _proc_exec_warned.is_set():
            _proc_exec_warned.set()
            log(
                "WARNING: /proc is not available, so helper programs are run "
                "by pathname instead of by descriptor; a program replaced "
                "between the check and the exec would not be noticed"
            )
        with _trusted_executable_lock:
            _trusted_executables[name] = (resolved, -1)
        return resolved

    with _trusted_executable_lock:
        _trusted_executables[name] = (argv0, descriptor)
    return argv0


def trusted_exec_fds() -> Tuple[int, ...]:
    """Descriptors that must be inherited so /proc/self/fd/N resolves.

    All of them, not the one belonging to a single call: threading individual
    descriptors through every spawn point is where one gets forgotten, and a
    forgotten one fails at exec time with a bare ENOENT.
    """
    with _trusted_executable_lock:
        return tuple(
            entry[1]
            for entry in _trusted_executables.values()
            if not isinstance(entry, Exception) and entry[1] >= 0
        )


_proc_exec_warned = threading.Event()
_rsync_fsync_supported = None
_rsync_fsync_warned = threading.Event()


def lfs_executable() -> bytes:
    """Absolute, non-replaceable pathname of the Lustre lfs utility."""
    return os.fsencode(resolve_trusted_executable("lfs"))


def rsync_supports_fsync() -> bool:
    """Probe once whether the installed rsync understands --fsync (3.2.4+)."""
    global _rsync_fsync_supported
    with _trusted_executable_lock:
        known = _rsync_fsync_supported
    if known is not None:
        return known

    supported = False
    try:
        rsync_argv0 = resolve_trusted_executable("rsync")
        proc = subprocess.run(
            [rsync_argv0, "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            pass_fds=trusted_exec_fds(),
        )
        supported = b"--fsync" in (proc.stdout or b"")
    except Exception as exc:
        log(f"WARNING: cannot ask rsync whether it supports --fsync: {exc}")

    with _trusted_executable_lock:
        _rsync_fsync_supported = supported
    return supported


def get_system_path_max() -> int:
    """Return the system pathname limit reported by POSIX pathconf().

    On Linux this is normally 4096. PATH_MAX includes the terminating NUL, so
    an encoded pathname with len(path) >= this value needs long-path handling.
    A conservative 4096 fallback is used only if pathconf itself is unavailable.
    """
    global _system_path_max_cache
    with _system_path_max_lock:
        if _system_path_max_cache is None:
            try:
                value = int(os.pathconf("/", "PC_PATH_MAX"))
                if value < 2:
                    raise ValueError(f"invalid PC_PATH_MAX={value}")
                _system_path_max_cache = value
            except Exception as exc:
                _system_path_max_cache = 4096
                log(
                    "WARNING: cannot query PC_PATH_MAX with os.pathconf('/'); "
                    f"using fallback 4096: {exc}"
                )
        return int(_system_path_max_cache)


def path_needs_long_handling(*paths_b: bytes) -> bool:
    """True when at least one encoded pathname reaches the system PATH_MAX."""
    path_max = get_system_path_max()
    return any(path is not None and len(path) >= path_max for path in paths_b)


def path_length_policy_error(
    cfg: dict, src_b: bytes, dst_b: Optional[bytes] = None
) -> Optional[bytes]:
    """Return an error message when numeric max_path_length is exceeded.

    The limit is measured in encoded pathname bytes. Exactly N bytes is
    allowed for max_path_length=N; only values > N are rejected.
    """
    limit = cfg.get("max_path_length", "engine")
    # String values select engine behavior and do not impose a numeric policy
    # limit. Validation restricts which strings are legal for each method.
    if isinstance(limit, str):
        return None

    src_len = len(src_b)
    dst_len = len(dst_b) if dst_b is not None else None
    exceeded = []
    if src_len > limit:
        exceeded.append("source")
    if dst_len is not None and dst_len > limit:
        exceeded.append("destination")
    if not exceeded:
        return None

    details = [
        "path_length_exceeded",
        f"max_path_length={limit}",
        f"source_path_length={src_len}",
    ]
    if dst_len is not None:
        details.append(f"destination_path_length={dst_len}")
    details.append(f"exceeded={','.join(exceeded)}")
    return "; ".join(details).encode("ascii")


def require_config_int(
    name: str,
    value,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    """Validate a JSON configuration value as a strict integer.

    ``bool`` is rejected explicitly because it is a subclass of ``int`` in
    Python. Floats and numeric strings are also rejected instead of being
    silently truncated or converted.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return value


def require_config_number(
    name: str,
    value,
    *,
    minimum: Optional[float] = None,
    minimum_exclusive: bool = False,
    allow_none: bool = False,
) -> Optional[float]:
    """Validate a finite numeric JSON configuration value.

    Integers and floating-point values are accepted, but booleans, strings,
    NaN and infinities are rejected.
    """
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{name} must be a number")

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        suffix = " or null" if allow_none else ""
        raise ValueError(f"{name} must be a number{suffix}")

    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")

    if minimum is not None:
        if minimum_exclusive and result <= minimum:
            raise ValueError(f"{name} must be > {minimum:g}")
        if not minimum_exclusive and result < minimum:
            raise ValueError(f"{name} must be >= {minimum:g}")

    return result


def _terminate_process_group(proc: "subprocess.Popen", reason: str) -> None:
    """End a child and everything it forked, then reap it.

    proc.kill() reaches one process. rsync in local mode is three, and the two
    it forked keep their descriptors on the destination - they would go on
    writing while the next attempt is already running, which is how two
    versions of the same file end up interleaved. The child is started in its
    own session, so the negative pid addresses the whole group.
    """
    try:
        pgid = os.getpgid(proc.pid)
    except OSError:
        pgid = None

    def group_is_gone() -> bool:
        """True when no process of the group answers any more.

        Reaping the parent says nothing about its children. The first version
        returned as soon as proc.wait() succeeded, so a child that ignores
        SIGTERM went on writing into the destination while the next attempt
        was already running - two writers on one file, which is exactly what
        the timeout is meant to prevent.
        """
        if pgid is None:
            return proc.poll() is not None
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            # Something in the group is not ours to signal. Saying "gone"
            # would be a guess in the dangerous direction.
            return False
        except OSError:
            return True
        return False

    for signum in (signal.SIGTERM, signal.SIGKILL):
        try:
            if pgid is not None:
                os.killpg(pgid, signum)
            else:
                proc.kill()
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except OSError:
                pass

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                proc.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                continue
            if group_is_gone():
                return
            time.sleep(0.1)
        # The parent may already be reaped here; the loop continues to SIGKILL
        # because something else in the group is still alive.

    if not group_is_gone():
        log(
            f"WARNING: {reason}: part of the rsync process group survived "
            f"SIGKILL and may still be writing into the destination"
        )


def build_rsync_parameters(cfg: dict) -> List[bytes]:
    """Build rsync arguments from independent semantic configuration flags."""
    hardlink_group_task = bool(cfg.get("_hardlink_group_task", False))
    # -H is never used. Normal batches contain no preserved hard-linked files,
    # and a resolved group hands rsync only its primary name, so there is no
    # second name in the transfer set for rsync to link against. The aliases
    # are created and verified by parallel_tools itself.
    parameters = [b"-dlR0"]

    # rsync never sets the final owner, group, mode, ACL or xattrs. Directories
    # stay locked for the whole run and the metadata of every transferred file
    # and symlink is recorded and applied by this program at the end, in a
    # fixed order and after the hard-link phase. -o/-g/-p on a directory would
    # hand it back to its final owner and mode in the middle of the run.
    #
    # --chmod=D0700 is the safety net for a directory rsync would create
    # itself: without --perms it only affects NEW objects, which then start
    # locked. Normally there are none - the parent chain of every path is
    # created and locked by this program before rsync runs.
    parameters.append(b"--chmod=D0700")

    if hardlink_group_task:
        # Force one fresh destination inode even when size and mtime already
        # match. Without this, rsync may reuse a destination inode that has an
        # unrelated hard link outside dst_root, modifying that external name or
        # leaving the reconstructed group with an incorrect link count.
        parameters.append(b"--ignore-times")

    if mtime_enabled(cfg):
        # File times are still set by rsync at transfer time: without -t the
        # destination carries the transfer time, and rsync's quick check then
        # re-transfers every file on the next run. Directory times are set at
        # the very end by this program (-O), after the last entry was written.
        parameters.append(b"-t")
        parameters.append(b"-O")

    if cfg.get("sparse", False):
        # Let rsync punch holes instead of materialising runs of zero bytes.
        parameters.append(b"--sparse")

    if fsync_per_file(cfg):
        # fsync="file" is a promise about every written file. rsync writes them,
        # so the promise has to be forwarded; parallel_tools never sees those
        # descriptors. --fsync exists since rsync 3.2.4.
        if rsync_supports_fsync():
            parameters.append(b"--fsync")
        elif not _rsync_fsync_warned.is_set():
            _rsync_fsync_warned.set()
            log(
                "WARNING: fsync='file' was requested but this rsync has no "
                "--fsync option; rsync-written files are flushed by a syncfs "
                "after each batch and at the end of the run instead"
            )

    if cfg.get("ignore_existing", False) and not hardlink_group_task:
        # Never for a resolved hard-link group: the primary must be written as
        # a fresh inode, and skipping an existing destination name would leave
        # the reconstructed group pointing at an unrelated inode.
        parameters.append(b"--ignore-existing")

    return parameters


# ------------------------------------------------------------
# Signal-Handler
# ------------------------------------------------------------

def handle_sighup(signum, frame):
    reload_event.set()


# Set by SIGUSR1, cleared by SIGUSR2. Process state, not configuration: it
# survives a reload and is deliberately not written anywhere, because a paused
# run that is then killed must come back as a normal run, not as a paused one.
pause_event = threading.Event()
PAUSE_CHECK_INTERVAL_SEC = 5.0


_DESCRIPTOR_ARGV_PREFIXES = ("/proc/self/fd/", "/dev/fd/")

# Where procfs is mounted. One spelling in one place, so everything below can
# be exercised against a stand-in on a system that has no procfs of its own -
# which is every system this program is developed on, and none that it runs on.
PROC_ROOT = "/proc"


def proc_path(pid, *parts: str) -> str:
    return os.path.join(PROC_ROOT, str(pid), *parts)


def _own_script_identity() -> Optional[Tuple[int, int]]:
    """dev/ino of this script file, or None when it cannot be determined."""
    try:
        st = os.stat(os.path.abspath(__file__))
    except OSError:
        return None
    return (int(st.st_dev), int(st.st_ino))


def _runs_this_script_by_descriptor(pid: int, argv: List[str]) -> bool:
    """True when that process runs THIS file, named as /proc/self/fd/N.

    A run started by the project launcher has no script NAME in its command
    line at all. It is started through the descriptor the launcher holds on
    this file, so procfs shows:

        /usr/bin/python3.11 /proc/self/fd/5 start test.cfg

    and a match on the basename finds nothing - which is how "stop" came to
    refuse the very runs this program had started itself. The descriptor is
    not a weaker name, it is a stronger one: /proc/PID/fd/5 leads to the file
    that process is actually executing, and comparing dev/ino to this script
    PROVES it is the same file rather than inferring it from a name. Somebody
    else's process cannot acquire that identity by being called something.
    """
    own = _own_script_identity()
    if own is None:
        return False
    for arg in argv:
        for prefix in _DESCRIPTOR_ARGV_PREFIXES:
            if not arg.startswith(prefix):
                continue
            number = arg[len(prefix):]
            if not number.isdigit():
                continue
            try:
                st = os.stat(proc_path(pid, "fd", number))
            except OSError:
                # Not readable, or gone. Unknown is not a match.
                continue
            if (int(st.st_dev), int(st.st_ino)) == own:
                return True
    return False


def _looks_like_this_program(argv: List[str], pid: Optional[int] = None) -> bool:
    """True when one ARGUMENT of that process is this script.

    Deliberately not a substring search over the whole command line. That is
    what the first version did, and it accepted every process that happened to
    mention the name anywhere - "vim notes_about_parallel_tools.txt" and
    "cat /var/log/parallel_tools.log" both passed, and on Linux an unhandled
    SIGUSR1 would have killed them. A whole argument whose basename is this
    file is a statement about what the process IS, not about what it mentions.

    A run may also carry no name at all, because it was started through a
    descriptor; _runs_this_script_by_descriptor() resolves that case, and
    answers it more strictly than any name comparison can.
    """
    marker = os.path.basename(os.path.abspath(__file__))
    if any(os.path.basename(arg) == marker for arg in argv):
        return True
    if pid is not None and _runs_this_script_by_descriptor(pid, argv):
        return True
    return False


def _process_command_line(pid: int) -> Optional[List[str]]:
    """The argument vector of a running process, or None when unreadable.

    procfs where it exists; ps(1) ONLY where there is no procfs at all.

    An empty command line is an ANSWER, not a failure: every kernel thread has
    an empty /proc/PID/cmdline - readable, simply without argv - and a process
    with no argv cannot be this program.

    Treating empty like unreadable is what the first version did, and it fell
    through to ps. That was survivable for one pid and ruinous for many: a
    version of this program that searched all of /proc then forked one ps per
    kernel thread, thousands of them on a Lustre client (kworker, ptlrpcd,
    ll_ost_io), each ps rereading all of /proc, sequentially, with a ten
    second timeout apiece. The search is gone, but the shape of this function
    is what made it possible, so it is worth saying why it is shaped this way.

    Today there is exactly one caller per control command, on a pid that is
    about to be signalled.
    """
    procfs_present = os.path.isdir(PROC_ROOT)
    if procfs_present:
        try:
            with open(proc_path(pid, "cmdline"), "rb") as stream:
                raw = stream.read()
        except OSError:
            return None
        argv = [
            part.decode("utf-8", errors="backslashreplace")
            for part in raw.split(b"\0")
            if part
        ]
        return argv or None

    try:
        argv0 = resolve_trusted_executable("ps")
    except Exception:
        return None
    try:
        proc = subprocess.run(
            [argv0, "-p", str(pid), "-o", "command="],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            pass_fds=trusted_exec_fds(),
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    text = proc.stdout.decode("utf-8", errors="backslashreplace").strip()
    # ps gives one line, so the argument boundaries are gone - a name with a
    # space in it splits wrongly here. That is a weaker reading than procfs
    # gives, and it is the reason the match below is by whole argument rather
    # than by substring: a wrong split can only ever reject, never accept
    # something it should not.
    return text.split() or None


SCHEDULER_PID_MARKER = "Scheduler PID:"


def _pid_from_stderr_log(path: str) -> Optional[int]:
    """The pid of the run that last wrote to this stderr log, or None.

    The scheduler writes a status line whenever the worker count, the active
    count or the queue changes, and at least every two minutes:

        2026-09-22 20:58:15.203098 Scheduler PID: 4096426 status: workers=8 ...

    The last one wins. The file is APPENDED to, so an old run's lines are
    still in there; using one log for two runs at a time makes this - and
    reading the log at all - ambiguous, and is simply not worth doing.

    What comes out of here is a SUGGESTION, never an authorisation. A log is
    an ordinary file: whoever can write it can put "Scheduler PID: 1" in it.
    The caller verifies, and the verification is what decides.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as stream:
            text = stream.read()
    except OSError:
        return None
    pid = None
    for line in text.splitlines():
        position = line.find(SCHEDULER_PID_MARKER)
        if position < 0:
            continue
        rest = line[position + len(SCHEDULER_PID_MARKER):].split()
        if rest and rest[0].isdigit():
            candidate = int(rest[0], 10)
            if candidate > 0:
                pid = candidate
    return pid


def _process_stderr_is(pid: int, identity: Tuple[int, int]) -> bool:
    """True when that process's descriptor 2 is this very file.

    Descriptor 2 and not a search through all of them: the log IS the
    process's stderr, that is how it came to be written. One stat answers it.

    This reads the file table of the process, not its memory, which is the
    difference that matters here - the memory of a process stuck in
    uninterruptible I/O cannot be read at all, and on a busy Lustre node that
    is not a rare state.
    """
    try:
        st = os.stat(proc_path(pid, "fd", "2"))
    except OSError:
        return False
    return (int(st.st_dev), int(st.st_ino)) == identity


def resolve_signal_target(target: str) -> int:
    """Turn what the operator typed into one pid, or explain why not.

    Two forms, and no searching. A number is that pid, unchanged. Anything
    else is the stderr log of a run: the pid is read out of it and then
    VERIFIED against the process that carries it - is its stderr this same
    file, and is it this program - before anything is signalled.

    An earlier version took the path of the CONFIGURATION and looked for the
    run by walking all of /proc. It worked, but it read the command line of
    every process on the machine to do it, which on a Lustre client under
    load is both slow and able to block outright. Reading one line out of the
    log the operator already has costs two stats and touches exactly one
    process - the one that is about to be signalled.
    """
    if target.isdigit():
        pid = int(target, 10)
        if pid <= 0:
            # kill(0, sig) signals the WHOLE process group and kill(-1, sig)
            # everything this user may signal. As root that is the machine.
            raise SystemExit(
                f"{target} is not a process ID. Zero and negative values "
                f"address whole process groups, which these commands never "
                f"signal; give the PID of one run, or its stderr log."
            )
        return pid

    if target[:1] in "+-" and target[1:].isdigit():
        raise SystemExit(
            f"{target} is not a process ID and not a path. A PID is written "
            f"without a sign; zero and negative values address whole process "
            f"groups and are never signalled here."
        )

    log_path = os.path.abspath(os.path.expanduser(target))
    try:
        st = os.stat(log_path)
    except OSError as exc:
        raise SystemExit(f"cannot read {log_path}: {exc}")
    identity = (int(st.st_dev), int(st.st_ino))

    pid = _pid_from_stderr_log(log_path)
    if pid is None:
        raise SystemExit(
            f"{log_path} does not name a running instance: no "
            f"{SCHEDULER_PID_MARKER!r} line in it. Give the PID instead, or "
            f"point at the stderr log of the run (the file the run's own '2>' "
            f"redirection writes)."
        )

    if not os.path.isdir(PROC_ROOT):
        raise SystemExit(
            f"{log_path} names PID {pid}, but {PROC_ROOT} is not available on "
            f"this system, so that cannot be checked. Give the PID directly "
            f"if you are sure."
        )

    if not _process_stderr_is(pid, identity):
        raise SystemExit(
            f"{log_path} names PID {pid}, but that process is not writing to "
            f"this log - the run it belongs to has ended, or the number now "
            f"belongs to something else. Nothing was signalled."
        )

    identity_argv = _process_command_line(pid)
    if identity_argv is None or not _looks_like_this_program(identity_argv, pid):
        raise SystemExit(
            f"{log_path} names PID {pid}, and that process has this log open, "
            f"but it is not parallel_tools. Nothing was signalled.\n"
            f"  it is: {' '.join(identity_argv or ['<unreadable>'])[:200]}"
        )

    return pid



_self_script_fd = -1
_self_script_lock = threading.Lock()


def self_script_argv0() -> str:
    """How this program names itself when it starts another copy of itself.

    sys.argv[0] is a pathname, and a pathname can be replaced while the run is
    going. As root that decides which code the next self-invocation executes -
    the same hole that resolve_trusted_executable() closes for rsync, lfs and
    the migrate helper, left open for the one program whose own behaviour is
    at stake. The script is therefore opened ONCE and kept, and later
    invocations go through the descriptor.

    Without procfs the name has to be used; that is said once, by
    resolve_trusted_executable(), and the same concession applies here.
    """
    global _self_script_fd
    script = os.path.abspath(
        sys.argv[0] if sys.argv and sys.argv[0] not in ("", "-") else __file__
    )
    with _self_script_lock:
        if _self_script_fd < 0:
            try:
                _self_script_fd = os.open(
                    script,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                )
            except OSError:
                return script
        by_descriptor = f"/proc/self/fd/{_self_script_fd}"
        if os.path.exists(by_descriptor):
            return by_descriptor
        return script


def self_invocation(*arguments: str) -> List[str]:
    """argv for starting this program again, with the subcommand included.

    The subcommand used to be missing here, and the project launcher therefore
    started its child as "parallel_tools.py NAME.cfg" - which the parser
    refuses. The launcher reported a pid, the child died immediately with
    "invalid choice", and the json log stayed empty. Building the argv in one
    place is what keeps the two callers from drifting apart again.
    """
    return [sys.executable, self_script_argv0(), *arguments]


def self_invocation_fds() -> Tuple[int, ...]:
    with _self_script_lock:
        return (_self_script_fd,) if _self_script_fd >= 0 else ()


def signal_running_instance(
    pid: int, signum: int, label: str, resolved_from_log: bool = False
) -> int:
    """Send a control signal to a pid the operator named, or one we found.

    A named pid is OBEYED. An earlier version refused when the target did not
    look like a parallel_tools run, and that was the wrong shape for this
    command: the pid an operator types is read out of the program's own
    output, not invented, so the refusal almost never caught a mistake - but
    when the identification itself was wrong it locked the operator out of
    their own run, which is what happened. A control command that can refuse
    to control is worse than one that does what it is told.

    The identification is still made, and still reported. What it says is put
    in front of the operator BEFORE the signal goes out, so a pid that turns
    out to belong to something else is visible in the output rather than
    silent. Finding the right pid is the job of the search by name, where a
    wrong target is impossible by construction instead of being argued about
    here.

    ``resolved_from_log`` inverts that for a pid the operator did NOT name.
    It came out of a log file and was verified moments ago; if it no longer
    matches, that run ended and the number was taken over in between, and
    signalling it would be this program's own mistake rather than the
    operator's instruction. There is nothing to obey in that case, so it
    stops.
    """
    # Where the kernel offers it, the process is pinned BEFORE it is
    # identified. Without that, the pid could be reused between reading the
    # command line and the signal, and the signal would land on whatever took
    # the number over. It costs one syscall and closes the window entirely.
    pidfd = -1
    if hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"):
        try:
            pidfd = os.pidfd_open(pid)
        except (OSError, AttributeError):
            pidfd = -1

    try:
        identity = _process_command_line(pid)

        if identity is None:
            log(
                f"WARNING: cannot check what process {pid} is on this system; "
                f"sending {label} unchecked"
            )
        elif not _looks_like_this_program(identity, pid):
            if resolved_from_log:
                raise SystemExit(
                    f"process {pid} was verified from the log a moment "
                    f"ago but is no longer this program - it ended and the "
                    f"number was reused. Nothing was signalled.\n"
                    f"  it is now: {' '.join(identity)[:200]}"
                )
            # Said, not refused. SIGUSR1 and SIGUSR2 terminate a process that
            # does not handle them, so this line may be the only trace of what
            # was hit - which is exactly why it names the target in full.
            log(
                f"WARNING: process {pid} does not look like a parallel_tools "
                f"run; sending {label} anyway, as asked. It is: "
                f"{' '.join(identity)[:200]}"
            )

        try:
            if pidfd >= 0:
                signal.pidfd_send_signal(pidfd, signum)
            else:
                # No pidfd on this kernel: the window between reading the
                # command line and signalling stays open. Narrow, and nothing
                # here can close it.
                os.kill(pid, signum)
        except OSError as exc:
            raise SystemExit(f"cannot send {label} to process {pid}: {exc}")
    finally:
        if pidfd >= 0:
            os.close(pidfd)

    print(f"{label} sent to {pid}")
    return 0


def signal_target(target: str, signum: int, label: str) -> int:
    """The one entry point behind stop, reload, pause and resume."""
    named_pid = target.isdigit()
    pid = resolve_signal_target(target)
    return signal_running_instance(
        pid, signum, label, resolved_from_log=not named_pid
    )


def handle_pause(signum, frame):
    """Hold the run. Everything already under way finishes first."""
    if not pause_event.is_set():
        pause_event.set()
        log(
            f"pause signal received: signum={signum}; workers stop taking new "
            f"files once the current one is done. Queued work is kept, "
            f"nothing is discarded. Resume with 'resume {os.getpid()}'"
        )


def handle_resume(signum, frame):
    if pause_event.is_set():
        pause_event.clear()
        log(f"resume signal received: signum={signum}; workers carry on")


def handle_sigterm(signum, frame):
    if shutdown_event.is_set():
        # The operator is telling the run a second time. That is the only way,
        # apart from a configured migrate_timeout, that work already under way
        # is cut short rather than finished.
        if not force_stop_event.is_set():
            force_stop_event.set()
            log(
                f"stop signal received again: signum={signum}; work already "
                f"under way is cut short, a running migrate helper is "
                f"terminated and its file is reported as unfinished"
            )
        return
    log(f"stop signal received: signum={signum}; finish current worker jobs only")
    shutdown_event.set()


# ------------------------------------------------------------
# Konfiguration
# ------------------------------------------------------------

def validate_helper_file_paths(cfg: dict, config_path: Optional[str]) -> None:
    """Refuse helper files that collide with each other or with the data.

    Every one of these names is opened by the program itself. Two of them
    pointing at the same file means one run writing a byte stream into
    another's file; a log inside dst_root means that deleting it at the end
    changes the mtime of a directory the run has just restored.
    """
    # The third field says whether this name is WRITTEN by the run. Only a
    # written name may not lie inside the data: reading an input list from
    # there is harmless, creating and later removing a file there is not.
    entries = []
    if config_path:
        entries.append(("config", os.path.abspath(config_path), False))
    if cfg.get("reload_input_file"):
        entries.append((
            "reload_input_file",
            os.path.abspath(os.path.expanduser(cfg["reload_input_file"])),
            False,
        ))
    for key in ("remaining_tasks_file",):
        value = cfg.get(key)
        if value:
            entries.append((key, os.path.abspath(os.path.expanduser(value)), True))
    if cfg.get("working_directory"):
        # Only refused when it was chosen. The default is resolved against the
        # current directory, which may well be inside the tree being copied -
        # refusing there would mean the program cannot be started from the
        # place people naturally stand in. prepare_working_directory() then
        # reports it as unavailable instead, and the run proceeds without a
        # spill target.
        entries.append((
            "working_directory",
            os.path.abspath(os.path.expanduser(cfg["working_directory"])),
            bool(cfg.get("_working_directory_explicit")),
        ))

    seen = {}
    for name, resolved, _inside_check in entries:
        other = seen.get(resolved)
        if other is not None:
            raise ValueError(
                f"{name} and {other} are the same path: {resolved}"
            )
        seen[resolved] = name

    for name, resolved, inside_check in entries:
        if not inside_check:
            continue
        for root_key in ("src_root", "dst_root"):
            root = cfg.get(root_key)
            if not root:
                continue
            # Resolved on both sides: a parent symlink pointing into the tree
            # would otherwise pass a comparison of cleaned-up names.
            if path_is_inside(resolved, root):
                raise ValueError(
                    f"{name} must not be inside {root_key}: "
                    f"{resolve_existing_prefix(resolved)} is below "
                    f"{os.path.realpath(root)}"
                )


def check_configured_copy_engine(cfg: dict) -> str:
    """Pin the external copy engine and return the argv[0] that runs exactly it.

    The first version of this checked the program and then let the ORIGINAL
    configured pathname be executed later. Two things were wrong with that:
    the check ran against the resolved file while the run executed the name,
    so a symlink swapped after the check ran a different program; and nothing
    kept the inspected file open, so even the same name could be replaced.

    resolve_trusted_executable() does both - it judges the descriptor and
    hands back /proc/self/fd/N - and is what rsync and lfs already go through.
    The engine has no reason to be the exception. Callers must pass
    trusted_exec_fds() as pass_fds so the descriptor reaches the child.
    """
    engine = cfg.get("engine", "internal")
    if engine == "internal" or not engine:
        return ""

    program = engine[0]
    if program in ("$SRC", "$DST"):
        raise ValueError("the copy engine must start with a program name")
    if not os.path.isabs(program):
        raise ValueError(
            f"copy engine {program!r} must be an absolute path; a name looked "
            f"up in PATH is chosen by the environment, not by the operator"
        )

    return resolve_trusted_executable(program)


def required_external_programs(cfg: dict) -> Tuple[str, ...]:
    """Which helper programs the configured method will actually spawn."""
    method = cfg.get("method")
    programs = []
    if method == "rsync":
        programs.append("rsync")
    if cfg.get("hard_link") in ("lustre2posix", "posix2lustre", "lustre2lustre"):
        programs.append("lfs")
    return tuple(programs)


def load_config_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("config file must contain a JSON object")

    if "method" not in data:
        raise ValueError("config file must contain a method value")

    method = normalize_method(str(data["method"]))

    common_keys = (
        "max_workers",
        "batch_max_files",
        "max_retries",
        "reload_input_file",
        "remaining_tasks_file",
        "queue_maxsize",
        "stdin_chunk_size",
        "max_input_record_bytes",
        "retry_backoff_base",
        "retry_backoff_max",
        "run_schedule",
        "max_path_length",
        "hard_link",
        "spill_compression",
        "working_directory",
        "metadata_maxsize",
    )

    new_cfg = {"method": method}
    for key in common_keys:
        new_cfg[key] = data.get(key, COMMON_DEFAULTS[key])

    # A default that does not fit the situation must not stop the run; a value
    # the operator chose and that cannot work must be refused. The two need to
    # be told apart - and the presence of the key says nothing, because
    # 'create' writes every key into the generated file. Differing from the
    # shipped default is what counts as a choice.
    new_cfg["_working_directory_explicit"] = (
        data.get("working_directory", COMMON_DEFAULTS["working_directory"])
        != COMMON_DEFAULTS["working_directory"]
    )

    new_cfg["hard_link"] = normalize_hard_link_mode(new_cfg["hard_link"])
    new_cfg["spill_compression"] = normalize_spill_compression(
        new_cfg["spill_compression"]
    )

    if method == "migrate":
        # Absent and null are the same thing here: null is what
        # 'create migrate' writes, so that the generated file is a form to
        # fill in rather than something that could be started by accident.
        # All of them are named at once - answering one question at a time
        # over four runs of the program is not a diagnosis.
        unfilled = [
            key for key in MIGRATE_REQUIRED_KEYS
            if data.get(key) is None
        ]
        if unfilled:
            raise ValueError(
                "migrate configuration has unfilled mandatory settings: "
                + ", ".join(unfilled)
                + ". They have no defaults because they decide which OSTs are "
                  "emptied and how many mirrors a file keeps; see "
                  "'parallel_tools.py start --help', section MIGRATE "
                  "PARAMETERS"
            )

        new_cfg["stripcount"] = data["stripcount"]
        new_cfg["poolname"] = data.get("poolname", None)
        new_cfg["banned_osts"] = data["banned_osts"]
        new_cfg["allowed_osts"] = data.get("allowed_osts", [])
        new_cfg["keep_mirroring"] = data["keep_mirroring"]
        new_cfg["migrate_helper"] = data.get("migrate_helper", None)
        new_cfg["migrate_allocation_attempts"] = data.get(
            "migrate_allocation_attempts",
            DEFAULT_MIGRATE_ALLOCATION_ATTEMPTS,
        )
        new_cfg["migrate_timeout"] = data.get("migrate_timeout", None)

        # Always, not only for the hard-link modes: src_root is the directory
        # the helper resolves within, and every path it is given has to stay
        # below it. Keeping it only when hard links are preserved meant that
        # with the default hard_link=null a configured boundary was accepted
        # by validation and then dropped, and the helper was started with
        # "--root /".
        new_cfg["src_root"] = normalize_root_path(
            "src_root", data.get("src_root")
        )
        if new_cfg["src_root"] == "/":
            # A boundary of "/" is the absence of one. The helper accepts it -
            # it has no way to know what a caller intends - but a scheduler
            # that hands it out has lost the operator's restriction.
            raise ValueError(
                "src_root must name the tree to migrate, not '/'"
            )

        if hard_link_preserves_groups(new_cfg["hard_link"]):
            if new_cfg["hard_link"] != "lustre2lustre":
                raise ValueError(
                    "method=migrate supports preserved hard links only with "
                    "hard_link='lustre2lustre'"
                )

        if not isinstance(new_cfg["keep_mirroring"], bool):
            raise ValueError("keep_mirroring must be true or false")

        new_cfg["stripcount"] = require_config_int(
            "stripcount",
            new_cfg["stripcount"],
            minimum=1,
            maximum=MAX_MIGRATE_STRIPE_COUNT,
        )

        new_cfg["migrate_allocation_attempts"] = require_config_int(
            "migrate_allocation_attempts",
            new_cfg["migrate_allocation_attempts"],
            minimum=1,
            maximum=100000,
        )

        if new_cfg["migrate_timeout"] is not None:
            new_cfg["migrate_timeout"] = require_config_number(
                "migrate_timeout",
                new_cfg["migrate_timeout"],
                minimum=1,
            )

        if new_cfg["migrate_helper"] is not None:
            if (
                not isinstance(new_cfg["migrate_helper"], str)
                or not new_cfg["migrate_helper"]
            ):
                raise ValueError(
                    "migrate_helper must be an absolute executable path or null"
                )
            if "\0" in new_cfg["migrate_helper"]:
                raise ValueError("migrate_helper must not contain a NUL character")
            if not os.path.isabs(new_cfg["migrate_helper"]):
                raise ValueError(
                    "migrate_helper must be an absolute path when explicitly set"
                )
            new_cfg["migrate_helper"] = os.path.normpath(
                new_cfg["migrate_helper"]
            )


        if new_cfg["poolname"] is not None:
            if not isinstance(new_cfg["poolname"], str) or not new_cfg["poolname"].strip():
                raise ValueError("poolname must be a non-empty string or null")
            if "\0" in new_cfg["poolname"]:
                raise ValueError("poolname must not contain a NUL character")
            new_cfg["poolname"] = new_cfg["poolname"].strip()

    elif method in ("copy", "rsync"):
        defaults = DEFAULT_CONFIG_COPY if method == "copy" else DEFAULT_CONFIG_RSYNC

        if method == "copy":
            new_cfg["engine"] = normalize_copy_engine(
                data.get("engine", defaults["engine"])
            )
        else:
            new_cfg["rsync_zero_bytes_timeout"] = data.get(
                "rsync_zero_bytes_timeout", defaults["rsync_zero_bytes_timeout"]
            )
            new_cfg["ignore_existing"] = data.get(
                "ignore_existing", defaults["ignore_existing"]
            )

        new_cfg["verify"] = data.get("verify", defaults["verify"])
        new_cfg["buffer_mb"] = data.get("buffer_mb", defaults["buffer_mb"])
        new_cfg["src_root"] = data.get("src_root", None)
        new_cfg["dst_root"] = data.get("dst_root", None)
        new_cfg["relative_path"] = data.get("relative_path", None)
        new_cfg["xattr"] = data.get("xattr", defaults["xattr"])
        new_cfg["permissions"] = normalize_permissions(
            data.get("permissions", defaults["permissions"])
        )
        new_cfg["mtime"] = data.get("mtime", defaults["mtime"])
        new_cfg["sparse"] = data.get("sparse", defaults["sparse"])
        if method == "copy":
            new_cfg["skip_existing"] = data.get(
                "skip_existing", defaults["skip_existing"]
            )
        new_cfg["fsync"] = normalize_fsync_mode(
            data.get("fsync", defaults["fsync"])
        )
        new_cfg["copy_timeout"] = data.get("copy_timeout", defaults["copy_timeout"])
        new_cfg["require_destination_protection"] = data.get(
            "require_destination_protection",
            defaults["require_destination_protection"],
        )

        boolean_keys = ["xattr", "sparse", "mtime", "require_destination_protection"]
        if method == "rsync":
            boolean_keys.append("ignore_existing")
        else:
            boolean_keys.append("skip_existing")
        for key in boolean_keys:
            if not isinstance(new_cfg[key], bool):
                raise ValueError(f"{key} must be true or false")

        for key in ("src_root", "dst_root"):
            new_cfg[key] = normalize_root_path(key, new_cfg[key])

        if roots_overlap(new_cfg["src_root"], new_cfg["dst_root"]):
            raise ValueError(
                "src_root and dst_root must be separate, non-overlapping "
                "directory trees for a mutating copy method"
            )

        if not isinstance(new_cfg["relative_path"], bool):
            raise ValueError("relative_path must be true or false")

        new_cfg["copy_timeout"] = require_config_number(
            "copy_timeout",
            new_cfg["copy_timeout"],
            minimum=0.0,
            minimum_exclusive=True,
            allow_none=True,
        )

        if method == "rsync":
            new_cfg["rsync_zero_bytes_timeout"] = require_config_number(
                "rsync_zero_bytes_timeout",
                new_cfg["rsync_zero_bytes_timeout"],
                minimum=0.0,
                minimum_exclusive=True,
            )

    elif method == "diff":
        new_cfg["verify"] = data.get("verify", DEFAULT_CONFIG_DIFF["verify"])
        new_cfg["buffer_mb"] = data.get("buffer_mb", DEFAULT_CONFIG_DIFF["buffer_mb"])
        new_cfg["src_root"] = data.get("src_root", None)
        new_cfg["dst_root"] = data.get("dst_root", None)
        new_cfg["relative_path"] = data.get("relative_path", None)
        new_cfg["xattr"] = data.get("xattr", DEFAULT_CONFIG_DIFF["xattr"])
        new_cfg["permissions"] = normalize_permissions(
            data.get("permissions", DEFAULT_CONFIG_DIFF["permissions"])
        )
        new_cfg["mtime"] = data.get("mtime", DEFAULT_CONFIG_DIFF["mtime"])
        for key in ("xattr", "mtime"):
            if not isinstance(new_cfg[key], bool):
                raise ValueError(f"{key} must be true or false")

        for key in ("src_root", "dst_root"):
            new_cfg[key] = normalize_root_path(key, new_cfg[key])

        if not isinstance(new_cfg["relative_path"], bool):
            raise ValueError("relative_path must be true or false")

    if "verify" in new_cfg:
        new_cfg["verify"] = normalize_compare_mode(new_cfg["verify"], method)

        if is_blake3_mode(new_cfg["verify"]) and blake3 is None:
            raise ValueError(
                f"verify mode '{new_cfg['verify']}' requires the Python module 'blake3'"
            )

        new_cfg["buffer_mb"] = require_config_int(
            "buffer_mb", new_cfg.get("buffer_mb"), minimum=1
        )

    max_path_length = new_cfg.get("max_path_length", "engine")
    if isinstance(max_path_length, str):
        max_path_length = max_path_length.strip().lower()
        allowed_strings = {"engine"}
        if method == "rsync":
            allowed_strings.add("rsync")
        if max_path_length not in allowed_strings:
            allowed_text = "'engine' or 'rsync'" if method == "rsync" else "'engine'"
            raise ValueError(
                f"max_path_length must be {allowed_text} or an integer >= 1 "
                f"for method={method}"
            )
        new_cfg["max_path_length"] = max_path_length
    else:
        new_cfg["max_path_length"] = require_config_int(
            "max_path_length", max_path_length, minimum=1
        )

    for key in (
        "max_workers",
        "batch_max_files",
        "max_retries",
        "queue_maxsize",
        "stdin_chunk_size",
        "max_input_record_bytes",
    ):
        new_cfg[key] = require_config_int(key, new_cfg[key], minimum=1)

    remaining_tasks_file = new_cfg.get("remaining_tasks_file")
    if remaining_tasks_file is not None and not isinstance(remaining_tasks_file, str):
        raise ValueError("remaining_tasks_file must be a path string or null")

    reload_input_file = new_cfg.get("reload_input_file")
    if reload_input_file is not None and not isinstance(reload_input_file, str):
        raise ValueError("reload_input_file must be a path string or null")

    working_directory = new_cfg.get("working_directory")
    if not isinstance(working_directory, str) or not working_directory:
        raise ValueError(
            "working_directory must be a non-empty path string; it is where "
            "this run keeps its own state"
        )
    if "\0" in working_directory:
        raise ValueError("working_directory must not contain a NUL character")
    if "directory_times_maxsize" in data:
        # The former name. Both at once with different values is a contradiction
        # the operator has to resolve, not something to pick a winner for.
        if "metadata_maxsize" in data and data["metadata_maxsize"] != data[
            "directory_times_maxsize"
        ]:
            raise ValueError(
                "metadata_maxsize and its former name directory_times_maxsize "
                "are both set, to different values; keep metadata_maxsize"
            )
        new_cfg["metadata_maxsize"] = data["directory_times_maxsize"]
    new_cfg["metadata_maxsize"] = parse_size_bytes(
        "metadata_maxsize", new_cfg.get("metadata_maxsize")
    )

    # Retired. Recording is now always on for method=copy with mtime=true and
    # is held in memory, so there is no file to name. A configuration that
    # still sets it is refused rather than ignored: silently dropping it would
    # leave the operator believing the timestamps go somewhere they chose.
    # A null value is the harmless leftover of an older generated file.
    if data.get("directory_times_file") is not None:
        raise ValueError(
            "directory_times_file no longer exists. Directory timestamps are "
            "recorded in memory for every method=copy run with mtime=true and "
            "only spilled to disk above metadata_maxsize; the spill "
            "goes into working_directory. Remove the key, and set "
            "working_directory if you want to choose where the spill lands"
        )

    if method == "migrate":
        for key in ("banned_osts", "allowed_osts"):
            if not isinstance(new_cfg[key], list):
                raise ValueError(f"{key} must be a list")
            if len(new_cfg[key]) > MAX_MIGRATE_OST_LIST:
                raise ValueError(
                    f"{key} names {len(new_cfg[key])} OSTs; the helper accepts "
                    f"at most {MAX_MIGRATE_OST_LIST}"
                )
            new_cfg[key] = [
                require_config_int(
                    f"{key}[{index}]",
                    value,
                    minimum=0,
                    maximum=MAX_MIGRATE_OST_INDEX,
                )
                for index, value in enumerate(new_cfg[key])
            ]
            # A duplicate is not harmless arithmetic: the helper counts how
            # many OSTs remain outside the banned set by subtracting the
            # length of this list, so a repeated index removes a device that
            # is still there and can refuse the whole run with ENOSPC.
            duplicates = sorted(
                {
                    value
                    for value in new_cfg[key]
                    if new_cfg[key].count(value) > 1
                }
            )
            if duplicates:
                raise ValueError(
                    f"{key} names these OSTs more than once: "
                    + ", ".join(str(value) for value in duplicates)
                )
        overlap = sorted(set(new_cfg["allowed_osts"]) & set(new_cfg["banned_osts"]))
        if overlap:
            raise ValueError(
                "these OSTs appear in both allowed_osts and banned_osts: "
                + ", ".join(str(value) for value in overlap)
            )

    new_cfg["retry_backoff_base"] = require_config_number(
        "retry_backoff_base", new_cfg["retry_backoff_base"], minimum=0.0
    )
    new_cfg["retry_backoff_max"] = require_config_number(
        "retry_backoff_max", new_cfg["retry_backoff_max"], minimum=0.0
    )

    new_cfg["run_schedule"] = validate_run_schedule(new_cfg.get("run_schedule"))

    # A silently ignored key is indistinguishable from a working setting. A
    # misspelled "permitions" would otherwise run the whole job without
    # preserving ownership while still reporting status "ok".
    allowed_keys = set(common_keys) | {"method", "directory_times_maxsize"}
    if method == "migrate":
        allowed_keys |= {
            "stripcount", "poolname", "banned_osts", "keep_mirroring",
            "src_root", "migrate_helper", "migrate_allocation_attempts",
            "allowed_osts", "migrate_timeout",
        }
    elif method in ("copy", "rsync"):
        allowed_keys |= {
            "src_root", "dst_root", "relative_path", "xattr", "permissions",
            "mtime", "sparse", "fsync", "copy_timeout", "verify", "buffer_mb",
            "require_destination_protection",
        }
        if method == "copy":
            allowed_keys |= {"engine", "skip_existing"}
        else:
            allowed_keys |= {"rsync_zero_bytes_timeout", "ignore_existing"}
    elif method == "diff":
        allowed_keys |= {
            "src_root", "dst_root", "relative_path", "verify", "buffer_mb",
            "xattr", "permissions", "mtime",
        }

    unknown_keys = sorted(set(data) - allowed_keys)
    if unknown_keys:
        raise ValueError(
            f"unknown configuration keys for method={method}: "
            + ", ".join(unknown_keys)
        )

    return new_cfg


def is_hardlink_group_task(task) -> bool:
    return isinstance(task, dict) and task.get("kind") == HARDLINK_TASK_KIND


def hardlink_group_source_paths(group: dict) -> List[bytes]:
    primary = group.get("source_primary")
    links = list(group.get("source_links", ()))
    paths = ([primary] if primary else []) + links
    return [os.fsencode(path) if isinstance(path, str) else path for path in paths]


def task_remaining_paths(task) -> List[bytes]:
    """Return restartable input paths represented by either queue task type."""
    if not is_hardlink_group_task(task):
        return [os.fsencode(path) if isinstance(path, str) else path for path in task]

    paths = []
    for group in task.get("groups", ()):
        candidate_paths = group.get("input_paths") or [
            group.get("source_primary")
        ]
        paths.extend(
            os.fsencode(path) if isinstance(path, str) else path
            for path in candidate_paths
            if path
        )
    return paths


def task_item_count(task) -> int:
    if is_hardlink_group_task(task):
        return len(task.get("groups", ()))
    return len(task)

def resize_task_queue(new_maxsize: int) -> Tuple[int, int]:
    """Change Queue.maxsize in place without dropping queued batches.

    The Queue object itself must not be replaced because reader and worker
    threads may currently be blocked on that exact object. When the current
    queue length is above the new limit, all existing batches remain queued and
    producers wait until workers drain the queue below the configured limit.

    Returns:
        (old_maxsize, queued_batches_at_resize)
    """
    new_maxsize = int(new_maxsize)
    if new_maxsize < 1:
        raise ValueError("queue_maxsize must be >= 1")

    with task_queue.mutex:
        old_maxsize = task_queue.maxsize
        queued_batches = task_queue._qsize()
        task_queue.maxsize = new_maxsize
        task_queue.not_full.notify_all()

    return old_maxsize, queued_batches


def rebatch_queued_tasks(new_batch_max_files: int) -> Tuple[int, int, int]:
    """Repack only not-yet-dequeued queue entries to a new maximum batch size.

    Active worker batches are intentionally untouched. The operation is atomic
    with respect to Queue.get()/put()/task_done() because Queue.mutex is held for
    the complete transformation. Path order is preserved exactly.

    Queue.unfinished_tasks counts queue *items* (batches), not paths, so its
    value is adjusted by the change in queued batch count while active batches
    remain represented by their existing unfinished-task entries.

    Returns:
        (old_batch_count, new_batch_count, queued_path_count)
    """
    new_batch_max_files = int(new_batch_max_files)
    if new_batch_max_files < 1:
        raise ValueError("batch_max_files must be >= 1")

    with task_queue.mutex:
        old_batch_count = task_queue._qsize()
        if old_batch_count == 0:
            return 0, 0, 0

        old_queue = task_queue.queue
        new_queue = deque()
        current_batch = []
        queued_path_count = 0

        def flush_normal_batch() -> None:
            nonlocal current_batch
            if current_batch:
                new_queue.append(current_batch)
                current_batch = []

        # Consume the old deque destructively so references to old batch lists
        # are released progressively instead of first building one giant flat
        # list of all queued paths. Resolved hard-link tasks are indivisible and
        # are therefore retained unchanged and in order.
        while old_queue:
            old_batch = old_queue.popleft()
            if is_hardlink_group_task(old_batch):
                flush_normal_batch()
                new_queue.append(old_batch)
                queued_path_count += len(task_remaining_paths(old_batch))
                continue

            for path in old_batch:
                current_batch.append(path)
                queued_path_count += 1
                if len(current_batch) >= new_batch_max_files:
                    flush_normal_batch()

        flush_normal_batch()

        new_batch_count = len(new_queue)
        delta = new_batch_count - old_batch_count
        task_queue.queue = new_queue
        task_queue.unfinished_tasks += delta

        if task_queue.unfinished_tasks < 0:
            raise RuntimeError("internal queue accounting error after rebatching")

        if new_batch_count:
            task_queue.not_empty.notify_all()
        task_queue.not_full.notify_all()
        if task_queue.unfinished_tasks == 0:
            task_queue.all_tasks_done.notify_all()

        return old_batch_count, new_batch_count, queued_path_count


def wait_for_queue_capacity(timeout: float = 0.5) -> None:
    """Wait briefly for Queue capacity without holding queue_batching_lock."""
    with task_queue.not_full:
        if task_queue.maxsize > 0 and task_queue._qsize() >= task_queue.maxsize:
            task_queue.not_full.wait(timeout=timeout)


def apply_new_config(new_cfg: dict) -> bool:
    global config

    old_cfg = get_config_snapshot()

    immutable_keys = (
        "method",
        "hard_link",
        "src_root",
        "dst_root",
        "relative_path",
        # The codec is chosen once; switching it while the log is being
        # written would produce an unreadable mixture.
        "spill_compression",
        # Opened and pinned once at start-up, and every invocation goes
        # through that descriptor. Accepting a new path on reload would mean
        # the checked helper and the executed helper are different files.
        "migrate_helper",
        # Read once when the log is built. Accepting a new value on reload and
        # logging it as applied, while the running log keeps the old one, is
        # worse than refusing the change.
        "metadata_maxsize",
        # The metadata classes decide what is recorded for the restore at the
        # end. Changing them halfway would restore one part of the tree by
        # the old rule and the rest by the new one - and a class switched on
        # later would never be restored for what was already recorded.
        "mtime",
        "permissions",
        "xattr",
        # Opened once and then held as a descriptor. Changing the name while
        # files are being written into it would leave the run with state in
        # two places and nothing able to find both.
        "working_directory",
    )
    immutable_changes = [
        key
        for key in immutable_keys
        if old_cfg.get(key) != new_cfg.get(key)
    ]
    if immutable_changes:
        details = ", ".join(
            f"{key}: {old_cfg.get(key)!r} -> {new_cfg.get(key)!r}"
            for key in immutable_changes
        )
        log(f"ignoring reload with immutable configuration changes: {details}")
        return False

    old_workers = old_cfg["max_workers"]
    old_queue_maxsize = int(old_cfg["queue_maxsize"])
    new_queue_maxsize = int(new_cfg["queue_maxsize"])
    old_batch_max_files = int(old_cfg["batch_max_files"])
    new_batch_max_files = int(new_cfg["batch_max_files"])

    # Serialize configuration changes that affect batching with producer-side
    # batch creation. Producers never hold this lock while waiting for capacity.
    with queue_batching_lock:
        queued_batches = get_queue_size()

        if new_queue_maxsize != old_queue_maxsize:
            actual_old_maxsize, queued_batches = resize_task_queue(new_queue_maxsize)
            log(
                f"queue capacity changed without replacing or draining queue: "
                f"{actual_old_maxsize} -> {new_queue_maxsize}; "
                f"queued_batches={queued_batches}"
            )

        # Publish the new config before rebatching. While queue_batching_lock is
        # held no producer can create/enqueue a batch using stale batch settings.
        with config_lock:
            config = dict(new_cfg)

        if new_batch_max_files != old_batch_max_files:
            old_batches, new_batches, queued_paths = rebatch_queued_tasks(
                new_batch_max_files
            )
            queued_batches = new_batches
            log(
                f"batch_max_files changed: {old_batch_max_files} -> "
                f"{new_batch_max_files}; rebatching queued work: "
                f"{old_batches} -> {new_batches} batches; "
                f"queued_paths={queued_paths}"
            )

        if queued_batches > new_queue_maxsize:
            log(
                f"queue temporarily exceeds configured queue_maxsize by "
                f"{queued_batches - new_queue_maxsize} batches "
                f"({queued_batches} queued, limit {new_queue_maxsize}); "
                f"all existing data is retained and producers will wait until "
                f"the queue drains below the configured limit"
            )

    new_workers = new_cfg["max_workers"]
    warn_if_workers_exceed_cpu_budget(new_workers, new_cfg)
    warn_if_hash_threads_exceed_cpu_budget(new_cfg)
    warn_if_buffer_exceeds_memory_budget(new_cfg)
    if new_workers != old_workers:
        resize_workers(new_workers)

    verify_text = (
        f" verify_mode={new_cfg.get('verify')}"
        if "verify" in new_cfg
        else ""
    )
    engine_text = (
        f" engine={new_cfg.get('engine')}"
        if new_cfg.get("method") == "copy"
        else ""
    )
    buffer_text = (
        f" buffer_mb={new_cfg.get('buffer_mb')}"
        if "buffer_mb" in new_cfg
        else ""
    )
    path_text = (
        f" max_path_length={new_cfg.get('max_path_length')}"
        f" system_PATH_MAX={get_system_path_max()}"
    )
    blake3_thread_text = (
        f" blake3_threads_per_hasher={get_blake3_threads_for_mode(new_cfg.get('verify'), new_workers)}"
        if is_blake3_mode(new_cfg.get("verify"))
        else ""
    )
    log(
        f"config applied: max_workers={new_cfg['max_workers']} "
        f"method={new_cfg['method']} hard_link={new_cfg.get('hard_link')!r}"
        f"{engine_text}{verify_text} "
        f"batch_max_files={new_cfg['batch_max_files']} "
        f"queue_maxsize={new_cfg['queue_maxsize']} "
        f"queued_batches={queued_batches}"
        f"{buffer_text}{path_text}{blake3_thread_text}"
    )
    return True


def validate_relative_path(rel_b: bytes, allow_root: bool = False) -> None:
    if allow_root and rel_b == b".":
        return
    if os.path.isabs(rel_b):
        raise ValueError(f"absolute path not allowed with relative_path=true: {path_display(rel_b)}")

    parts = rel_b.split(b"/")
    if not parts or any(p in (b"", b".", b"..") for p in parts):
        raise ValueError(f"invalid relative path: {path_display(rel_b)}")


def count_failed_input_records(
    paths: List[bytes], failed_files: dict, cfg: dict
) -> int:
    """How many INPUT RECORDS failed - not how many distinct messages there are.

    failed_files maps a path to its message, which is right for display: the
    same path has the same reason. It is wrong as a count. Two identical
    records that both fail collapse to one key, len() says one, and the batch
    then reports the second record as a transferred object. Reproduced with
    two identical missing paths: 2 inputs, 1 failure, 1 object that never
    existed - and the accounting balanced, because the phantom filled the gap.

    So the records are counted, with their repetitions, against the keys the
    batch actually used. Both spellings are accepted because the batches key
    some failures by the normalized absolute path and some by the raw input
    record.
    """
    if not failed_files:
        return 0
    failed = 0
    for src_b in paths:
        if src_b in failed_files:
            failed += 1
            continue
        try:
            _, src_abs_b, _ = normalize_input_path(src_b, cfg)
        except Exception:
            continue
        if src_abs_b in failed_files:
            failed += 1
    return failed


def normalize_input_path(path_b: bytes, cfg: dict) -> Tuple[bytes, bytes, bytes]:
    """
    Return:
      rel_b
      src_abs_b
      dst_abs_b
    """
    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)

    src_root_b = os.fsencode(cfg["src_root"])
    dst_root_b = os.fsencode(cfg["dst_root"])

    if cfg.get("relative_path", False):
        validate_relative_path(path_b)
        rel_b = path_b
        src_abs_b = os.path.join(src_root_b, rel_b)
    else:
        if not os.path.isabs(path_b):
            raise ValueError(f"relative path not allowed with relative_path=false: {path_display(path_b)}")
        src_abs_b = path_b
        rel_b = make_relative_bytes(src_abs_b, src_root_b)
        # Prefix matching alone is insufficient: /src/../outside has the byte
        # prefix /src/ but resolves outside the configured source root. Reject
        # every non-canonical component before constructing the destination.
        validate_relative_path(rel_b, allow_root=True)

    dst_abs_b = os.path.join(dst_root_b, rel_b)
    return rel_b, src_abs_b, dst_abs_b

# ------------------------------------------------------------
# Null-terminierte Eingabe
# ------------------------------------------------------------

# A producer that fails halfway closes its pipe, and a closed pipe is
# indistinguishable from a complete listing: the reader sees clean EOF and the
# run reports a successful partial transfer. The producer therefore appends
# this record when it failed.
#
# With relative_path=false a record equal to this one is impossible: paths are
# absolute and start with a slash. With relative_path=true they need neither,
# so a file of this exact name directly below src_root would produce the same
# bytes. _InputAbortFilter therefore does not decide per chunk; it holds a
# matching record back and only reads it as the marker if the stream really
# ends there. Anything that follows proves it was a filename.
INPUT_ABORT_SENTINEL = b"\x01parallel_tools:input-aborted"


class _InputAbortFilter:
    """Separate the producer's abort marker from a path that looks like it."""

    def __init__(self, source: str):
        self.source = source
        self.held = False

    def filter(self, records: List[bytes]) -> List[bytes]:
        out = list(records)
        if self.held and out:
            # Records followed the held one, so it was a pathname after all.
            out.insert(0, INPUT_ABORT_SENTINEL)
            self.held = False
            log(
                f"{self.source}: a path is named like the input abort marker; "
                f"treating it as a pathname because the list continues"
            )
        if out and out[-1] == INPUT_ABORT_SENTINEL:
            out.pop()
            self.held = True
        return out

    def aborted(self, cfg: Optional[dict] = None) -> bool:
        """Decide what the held last record was, now that the stream ended.

        The two readings are indistinguishable at end of stream, and an
        earlier attempt to tell them apart by asking whether such a file
        exists was wrong in the dangerous direction: with the file present, a
        REAL abort was read as a pathname, and the run then reported success
        while part of the tree had never been listed.

        So the ambiguity resolves the safe way - the marker wins - and the
        collision is named instead of guessed at. What is lost is one input
        record for a file nobody should have called this; what is kept is the
        guarantee that a truncated listing is never reported as a complete
        one.
        """
        if not self.held:
            return False
        if self._sentinel_names_a_file(cfg):
            log(
                f"WARNING: {self.source}: a file below src_root is named like "
                f"the input abort marker. It cannot be transferred through "
                f"this input channel, and the run is reported as incomplete "
                f"because a real abort marker would look exactly the same. "
                f"Rename the file, or pass it in a separate run with "
                f"relative_path=false."
            )
        return True

    def _sentinel_names_a_file(self, cfg: Optional[dict]) -> bool:
        if not cfg or not cfg.get("relative_path", False):
            # Absolute records start with a slash; the marker cannot be one.
            return False
        root = cfg.get("src_root")
        if not root:
            return False
        try:
            os.lstat(
                os.path.join(os.fsencode(root), INPUT_ABORT_SENTINEL)
            )
            return True
        except OSError:
            return False

# Records accepted from stdin and from reload_input_file. This is the reference
# the final accounting is checked against: without it, "1.2 M transferred" says
# nothing about whether the producer ever reached the end of the tree.
input_record_lock = threading.Lock()
input_record_count = 0


def note_input_records(count: int) -> None:
    global input_record_count
    if count <= 0:
        return
    with input_record_lock:
        input_record_count += count


def get_input_record_count() -> int:
    with input_record_lock:
        return input_record_count
INPUT_ABORT_SHELL_LITERAL = "\\01parallel_tools:input-aborted\\0"


def _input_separator_hint(data: bytes) -> str:
    # Both bytes are legal in a pathname, so this can only be a hint.
    if b"\n" in data:
        return "; newline found (check for find -print instead of -print0)"
    if b"\x07" in data:
        return "; BEL byte found (check the list's separator)"
    return ""


def iter_null_terminated(
    stream, chunk_size: int, max_record_bytes: int = 8388608,
):
    buf = b""
    reached_eof = False

    while not shutdown_event.is_set():
        chunk = stream.read(chunk_size)
        if not chunk:
            reached_eof = True
            break

        buf += chunk
        parts = buf.split(b"\0")

        complete = parts[:-1]
        for index, record in enumerate(complete):
            if len(record) > max_record_bytes:
                if index:
                    yield complete[:index]
                raise ValueError(
                    f"NUL-terminated input record exceeds {max_record_bytes} "
                    f"bytes{_input_separator_hint(record)}"
                )
        if complete:
            yield complete

        buf = parts[-1]

        if len(buf) > max_record_bytes:
            raise ValueError(
                f"no NUL terminator within {max_record_bytes} bytes of an "
                f"input record; invalid list or path above configured limit"
                f"{_input_separator_hint(buf)}"
            )

    # Only a NUL terminates a record. A partial last path is never a task,
    # including at EOF; on shutdown it is discarded without declaring EOF.
    if reached_eof and buf:
        raise ValueError(
            f"input ends with {len(buf)} bytes without a NUL terminator"
            f"{_input_separator_hint(buf)}"
        )


def make_relative_bytes(path_b: bytes, root_b: bytes) -> bytes:
    """
    Pure byte-prefix relative path calculation.
    Avoids os.path.relpath() PATH_MAX issues.
    """

    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)

    if isinstance(root_b, str):
        root_b = os.fsencode(root_b)

    root_b = root_b.rstrip(b"/") or b"/"

    if path_b == root_b:
        return b"."

    prefix = b"/" if root_b == b"/" else root_b + b"/"

    if not path_b.startswith(prefix):
        raise ValueError(
            f"path is outside root: {path_display(path_b)} root={path_display(root_b)}"
        )

    return path_b[len(prefix):]


def make_destination(src_b: bytes, src_root_b: bytes, dst_root_b: bytes) -> bytes:
    rel_b = make_relative_bytes(src_b, src_root_b)
    validate_relative_path(rel_b, allow_root=True)
    return os.path.join(dst_root_b, rel_b)


def force_enqueue_task(task, reason: str) -> None:
    """Append a batch without applying Queue.maxsize.

    This path is used only after shutdown has been requested. Workers no longer
    start new queue items then, so blocking in Queue.put() could deadlock. The
    batch is retained in the same queue and is later written by
    save_remaining_tasks_to_file().
    """
    with task_queue.mutex:
        task_queue.queue.append(task)
        task_queue.unfinished_tasks += 1
        task_queue.not_empty.notify()

    log(
        f"preserved queue task with {task_item_count(task)} items "
        f"despite shutdown ({reason})"
    )


def enqueue_balanced_batches(paths, cfg=None) -> int:
    """Enqueue paths using the *current* runtime batch configuration.

    Batch creation and the nonblocking Queue.put() attempt are serialized with
    runtime rebatching. If the queue is full (including the intentional
    temporary-overflow state after shrinking batch_max_files), the producer
    releases queue_batching_lock before waiting, so workers and later reloads
    continue to make progress.

    ``cfg`` is not trusted for batch sizing: it may be stale after a SIGHUP
    reload, so the sizes are read from the current snapshot instead.
    """
    if not paths:
        return 0

    index = 0
    enqueued_paths = 0
    total_paths = len(paths)

    while index < total_paths:
        if shutdown_event.is_set():
            # No worker should start new queue items after shutdown. Preserve all
            # complete paths already parsed from input without respecting maxsize.
            current_cfg = get_config_snapshot()
            batch_max_files = int(current_cfg["batch_max_files"])
            remaining = paths[index:]
            for i in range(0, len(remaining), batch_max_files):
                batch = remaining[i:i + batch_max_files]
                force_enqueue_task(batch, "shutdown while enqueueing input")
                enqueued_paths += len(batch)
            return enqueued_paths

        inserted = False
        batch = None

        with queue_batching_lock:
            current_cfg = get_config_snapshot()
            max_workers = int(current_cfg["max_workers"])
            batch_max_files = int(current_cfg["batch_max_files"])

            remaining_count = total_paths - index
            n_batches = min(max_workers, remaining_count)
            batch_size = min(
                batch_max_files,
                (remaining_count + n_batches - 1) // n_batches,
            )
            batch = paths[index:index + batch_size]

            try:
                task_queue.put_nowait(batch)
                inserted = True
            except queue.Full:
                inserted = False

        if inserted:
            index += len(batch)
            enqueued_paths += len(batch)
            continue

        # Do not hold queue_batching_lock here. In particular, after a reload
        # that turns 1000x100-file batches into 10000x10-file batches, producers
        # must be able to wait while workers drain the temporary overflow.
        wait_for_queue_capacity(timeout=0.5)

    return enqueued_paths


def stdin_reader_main() -> None:
    try:
        if not shutdown_event.is_set():
            cfg = get_config_snapshot()
            chunk_size = int(cfg["stdin_chunk_size"])
            max_record_bytes = int(cfg["max_input_record_bytes"])

            # Use buffered blocking reads for maximum input throughput.
            # Complete records already returned by the parser are still preserved
            # by enqueue_balanced_batches() when shutdown is requested.
            abort_filter = _InputAbortFilter("input stream")
            for src_b_array in iter_null_terminated(
                sys.stdin.buffer,
                chunk_size=chunk_size,
                max_record_bytes=max_record_bytes,
            ):
                src_b_array = abort_filter.filter(src_b_array)

                # src_b_array contains complete, already-read path records. Even
                # if shutdown is now set, enqueue_balanced_batches() preserves all
                # of them for remaining_tasks_file.
                note_input_records(len(src_b_array))
                cfg = get_config_snapshot()
                enqueue_balanced_batches(src_b_array, cfg)

            if abort_filter.aborted(get_config_snapshot()):
                log(
                    "input stream carries the abort marker: the process "
                    "producing it (find/tee) failed, so the list is "
                    "incomplete"
                )
                input_failed_event.set()

    except Exception as e:
        log(f"stdin reader failed: {e}")
        input_failed_event.set()
    finally:
        stdin_closed_event.set()


def load_extra_tasks_from_file(path: str) -> int:
    count = 0
    cfg = get_config_snapshot()
    chunk_size = int(cfg["stdin_chunk_size"])
    max_record_bytes = int(cfg["max_input_record_bytes"])

    abort_filter = _InputAbortFilter(path)
    with open(path, "rb") as f:
        for src_b_array in iter_null_terminated(
            f, chunk_size=chunk_size, max_record_bytes=max_record_bytes,
        ):
            src_b_array = abort_filter.filter(src_b_array)
            # Preserve every complete record already returned by the parser even
            # if shutdown was requested immediately before enqueueing it.
            count += len(src_b_array)
            note_input_records(len(src_b_array))
            # The generator was created with the initial chunk size; re-reading
            # the setting here would have no effect on it.
            enqueue_balanced_batches(src_b_array, get_config_snapshot())

    if abort_filter.aborted(get_config_snapshot()):
        log(f"{path} carries the abort marker: the list is incomplete")
        input_failed_event.set()

    return count

WEEKDAY_KEYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

def parse_hhmm(value: str) -> int:
    if not isinstance(value, str):
        raise ValueError("schedule time must be a string in HH:MM format")

    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid schedule time: {value}")

    hour = int(parts[0])
    minute = int(parts[1])

    if hour == 24 and minute == 0:
        return 24 * 60
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"invalid schedule time: {value}")

    return hour * 60 + minute

def validate_run_schedule(schedule) -> Optional[dict]:
    if schedule is None:
        return None
    if not isinstance(schedule, dict):
        raise ValueError("run_schedule must be an object or null")

    unknown_schedule_keys = sorted(
        set(schedule) - {"timezone", "check_interval_sec", "windows"}
    )
    if unknown_schedule_keys:
        raise ValueError(
            "unknown run_schedule keys: " + ", ".join(unknown_schedule_keys)
        )

    windows = schedule.get("windows")
    if not isinstance(windows, dict):
        raise ValueError("run_schedule.windows must be an object")

    # A misspelled day name would otherwise leave every real day empty, which
    # silently means "never run" instead of the intended schedule.
    unknown_days = sorted(set(windows) - set(WEEKDAY_KEYS))
    if unknown_days:
        raise ValueError(
            "unknown run_schedule.windows day names: "
            + ", ".join(unknown_days)
            + "; valid names are "
            + " ".join(WEEKDAY_KEYS)
        )

    validated_windows = {}
    for day in WEEKDAY_KEYS:
        day_windows = windows.get(day, [])
        if day_windows is None:
            day_windows = []
        if not isinstance(day_windows, list):
            raise ValueError(f"run_schedule.windows.{day} must be a list")

        validated = []
        for item in day_windows:
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError(f"run_schedule window for {day} must be [start, end]")
            start = parse_hhmm(item[0])
            end = parse_hhmm(item[1])
            if start == end:
                continue
            if start > end:
                raise ValueError(
                    f"run_schedule window for {day} crosses midnight; split it into two windows"
                )
            validated.append((start, end))
        validated_windows[day] = validated

    check_interval = require_config_number(
        "run_schedule.check_interval_sec",
        schedule.get("check_interval_sec", 60.0),
        minimum=0.0,
        minimum_exclusive=True,
    )

    timezone_name = schedule.get("timezone", "local")
    if timezone_name != "local":
        raise ValueError("only run_schedule.timezone='local' is currently supported")

    return {
        "timezone": "local",
        "check_interval_sec": check_interval,
        "windows": validated_windows,
    }

def schedule_allows_work(cfg: dict, now: Optional[datetime] = None) -> bool:
    schedule = cfg.get("run_schedule")
    if schedule is None:
        return True

    if now is None:
        now = datetime.now()

    day = WEEKDAY_KEYS[now.weekday()]
    minute = now.hour * 60 + now.minute

    for start, end in schedule.get("windows", {}).get(day, []):
        if start <= minute < end:
            return True

    return False


def batch_pause_reason(cfg: dict, phase: str) -> Optional[str]:
    """Return why a dequeued batch must pause, if it may still be paused.

    A resolved hard-link group is one exclusive logical task. Once that task
    starts, it must be allowed to finish as a unit; otherwise a signal between
    copying the primary and creating its aliases could leave a partial group
    and could requeue only the primary pathname. The outer hard-link loop still
    observes shutdown and schedule changes before it starts the next group.
    """
    if cfg.get("_finish_current_task", False):
        return None
    if shutdown_event.is_set():
        return f"shutdown {phase}"
    if pause_event.is_set():
        return f"paused {phase}"
    if not schedule_allows_work(cfg):
        return f"outside run schedule {phase}"
    return None


def wait_until_work_is_allowed(worker_id: int, cfg: dict) -> bool:
    """Hold this worker while the run is paused or outside its schedule.

    Both reasons wait in the same place and in the same way, so there is one
    loop that has to react to a stop signal rather than two. False means the
    run is shutting down and the worker must return.
    """
    logged = None
    # Each reason keeps its own pair of messages. An operator greps for these,
    # so "the thing that was holding you no longer holds" is not good enough -
    # it has to say which one.
    released = {
        "paused": "resumed; carrying on",
        "outside configured run schedule": "run schedule allows work again",
    }

    while not shutdown_event.is_set():
        cfg = get_config_snapshot()
        schedule = cfg.get("run_schedule")

        if pause_event.is_set():
            reason = "paused"
            # A pause is answered by hand, so the wakeup is frequent enough to
            # feel immediate without polling in a tight loop.
            interval = PAUSE_CHECK_INTERVAL_SEC
        elif schedule is not None and not schedule_allows_work(cfg):
            reason = "outside configured run schedule"
            interval = float(schedule.get("check_interval_sec", 60.0))
        else:
            if logged is not None:
                log(f"worker-{worker_id}: {released[logged]}")
            return True

        if logged != reason:
            log(f"worker-{worker_id}: {reason}; waiting")
            logged = reason

        # Waiting on the event honours the interval and still reacts to
        # SIGTERM immediately, with fewer wakeups than polling.
        shutdown_event.wait(timeout=interval)

    return False

def requeue_unprocessed_paths(paths: List[bytes], reason: str = "shutdown") -> int:
    """
    Put not-yet-processed paths from an already dequeued worker batch back
    into task_queue so save_remaining_tasks_to_file() can persist them.

    This intentionally bypasses Queue.maxsize. During shutdown no worker is
    expected to consume new work, so a blocking put() could deadlock if the
    queue is already full.
    """
    batch = [os.fsencode(p) if isinstance(p, str) else p for p in paths if p]
    if not batch:
        return 0

    with task_queue.mutex:
        task_queue.queue.appendleft(batch)
        task_queue.unfinished_tasks += 1
        task_queue.not_empty.notify()

    log(f"worker requeued {len(batch)} unprocessed paths ({reason})")
    return len(batch)


def requeue_hardlink_groups(groups: List[dict], reason: str) -> int:
    """Requeue complete resolved groups without splitting group membership."""
    groups = list(groups)
    if not groups:
        return 0

    task = {"kind": HARDLINK_TASK_KIND, "groups": groups}
    with task_queue.mutex:
        task_queue.queue.appendleft(task)
        task_queue.unfinished_tasks += 1
        task_queue.not_empty.notify()

    path_count = len(task_remaining_paths(task))
    log(
        f"worker requeued {len(groups)} resolved hard-link groups "
        f"representing {path_count} input paths ({reason})"
    )
    return path_count

def count_unstarted_paths() -> int:
    """Count input paths in batches that no worker has taken yet.

    Used when the run ends with work still queued and no remaining_tasks_file
    is configured: the paths are lost, and a run that loses work must not
    report success.
    """
    total = 0
    with task_queue.mutex:
        batches = list(task_queue.queue)
    for batch in batches:
        try:
            total += len(task_remaining_paths(batch))
        except Exception:
            pass
    return total


def save_remaining_tasks_to_file(path: str) -> int:
    """
    Drain not-yet-started batches from task_queue and save all contained
    paths as a NUL-terminated byte stream.

    This does not include files that a worker has already taken from the
    queue and is currently processing.
    """
    count = 0
    absolute = os.path.abspath(os.path.expanduser(path))
    leaf = os.path.basename(absolute)
    tmp_leaf = f"{leaf}.tmp.{os.getpid()}.{secrets.token_hex(8)}"
    tmp_path = os.path.join(os.path.dirname(absolute), tmp_leaf)
    if not leaf or leaf in (".", ".."):
        raise OSError(errno.EINVAL, f"not a usable file name: {path}")

    parent = os.path.dirname(absolute)
    if parent:
        # Created component by component through held descriptors, never with
        # os.makedirs(): that walks by pathname and follows every symlink on
        # the way, and this runs as root at shutdown, so a redirected
        # component would decide where root creates a directory.
        # A directory others may write to is a DIFFERENT problem, and it must
        # not be answered by refusing to write. Everything above keeps root
        # from being redirected; what remains is that somebody could delete
        # this file afterwards. Losing the only record of the unstarted work
        # to avoid a risk that it might be deleted later would be trading a
        # certainty for a possibility. So: say it, and write it.
        problem = directory_chain_is_safe(parent)
        if problem is not None:
            log(
                f"WARNING: the remaining-task file is being written to a "
                f"directory that is not protected against other users "
                f"({problem}). The file itself is created safely, but nothing "
                f"stops somebody from removing it before the run is resumed"
            )

    # ONE descriptor for the parent, opened componentwise, and everything that
    # follows - create, write, fsync, rename, cleanup - goes through it. The
    # previous version resolved the pathname again for each of those steps, so
    # a component turned into a symlink in between redirected the file even
    # though every individual step looked careful.
    parent_fd = open_dir_fd_componentwise(os.fsencode(parent or "/"), create=True)
    try:
        tmp_fd = os.open(
            tmp_leaf,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
            0o600,
            dir_fd=parent_fd,
        )
    except BaseException:
        os.close(parent_fd)
        raise
    try:
        tmp_st = os.fstat(tmp_fd)
        if not stat.S_ISREG(tmp_st.st_mode) or tmp_st.st_nlink != 1:
            raise OSError(
                errno.EPERM,
                f"refusing to write to {tmp_path}: not a plain, unlinked file",
            )
    except BaseException:
        os.close(tmp_fd)
        raise
    content_complete = False
    # Every batch taken out of the queue is held here until the file is
    # actually in place. Putting back only the batch that failed was not
    # enough: the ones written before it were already gone from the queue,
    # and the partial file was then deleted - so a write failure in the middle
    # destroyed exactly the record it was supposed to preserve. Reproduced
    # with a simulated ENOSPC on the second batch: two paths in, one path
    # left, no file anywhere.
    drained = []
    try:
        with open(tmp_fd, "wb", closefd=True) as f:
            tmp_fd = None
            while True:
                try:
                    batch = task_queue.get_nowait()
                except queue.Empty:
                    break

                drained.append(batch)
                for src_b in task_remaining_paths(batch):
                    if isinstance(src_b, str):
                        src_b = os.fsencode(src_b)
                    f.write(src_b)
                    f.write(b"\0")
                    count += 1

            f.flush()
            os.fsync(f.fileno())
            content_complete = True

        # renameat through the SAME descriptor the file was created in: no
        # pathname is resolved a second time, so nothing that happens to the
        # names in between can redirect it.
        os.replace(tmp_leaf, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    except BaseException:
        # get_nowait() does not touch unfinished_tasks - only task_done() does
        # - so putting the batches back is all that is needed to restore the
        # queue exactly as it was. The caller recounts from the queue, so the
        # paths are reported as unsaved rather than silently gone.
        if drained:
            with task_queue.mutex:
                for batch in reversed(drained):
                    task_queue.queue.appendleft(batch)
                task_queue.not_empty.notify_all()
            drained = []
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except OSError:
                pass
        if content_complete:
            # The content was complete and only the rename failed, so the
            # temporary file is a usable copy. It is kept AND the batches went
            # back on the queue: two records of the same work are harmless,
            # none is not.
            log(
                f"WARNING: could not rename the remaining-task file into "
                f"place; the same paths are in {tmp_path} and back on the "
                f"queue"
            )
        else:
            try:
                os.unlink(tmp_leaf, dir_fd=parent_fd)
            except OSError:
                pass
        raise
    finally:
        os.close(parent_fd)

    # Only now: the file is in place, so the queue may give them up.
    for _ in drained:
        task_queue.task_done()

    return count


# ------------------------------------------------------------
# Vergleich
# ------------------------------------------------------------

def create_hasher(algo: str):
    """Create a BLAKE3 hasher for a normalized BLAKE3 verify mode."""
    if not is_blake3_mode(algo):
        raise ValueError(f"unsupported BLAKE3 hash algorithm: {algo}")
    if blake3 is None:
        raise RuntimeError(
            f"verify mode '{algo}' requires the Python module 'blake3'"
        )

    max_threads = get_blake3_threads_for_mode(algo)
    return blake3.blake3(max_threads=max_threads)


def create_streaming_hasher(algo: str):
    """Create a streaming hasher used by fused/internal and buffered paths."""
    if algo == "sha256":
        return hashlib.sha256()
    if is_blake3_mode(algo):
        return create_hasher(algo)
    raise ValueError(f"unsupported hash algorithm: {algo}")


def is_fused_internal_hash_mode(mode: Optional[str]) -> bool:
    """Only the benchmarked single-hasher modes are fused into internal copy."""
    return mode in ("sha256", "blake3")


def hash_file(
    path_b: bytes,
    algo: str,
    buffer_mb: int = 8,
) -> str:
    """Hash a regular file with the benchmarked buffered implementation."""
    bufsize = buffer_bytes(buffer_mb)
    h = create_streaming_hasher(algo)

    # Benchmarks on Lustre showed different best buffered paths:
    # SHA-256: os.read() into a fresh bytes object;
    # BLAKE3:   one reusable bytearray filled with readv().
    fd = os.open(path_b, os.O_RDONLY)
    try:
        if algo == "sha256":
            while True:
                data = os.read(fd, bufsize)
                if not data:
                    break
                h.update(data)
        else:
            buf = bytearray(bufsize)
            view = memoryview(buf)
            while True:
                n = os.readv(fd, [view])
                if n == 0:
                    break
                h.update(view[:n])
    finally:
        os.close(fd)
    return h.hexdigest()


def hash_fd(
    fd: int,
    algo: str,
    buffer_mb: int = 8,
) -> str:
    """Hash an open regular file with buffered pread()/preadv() from offset 0."""
    bufsize = buffer_bytes(buffer_mb)
    h = create_streaming_hasher(algo)

    offset = 0
    if algo == "sha256":
        while True:
            data = os.pread(fd, bufsize, offset)
            if not data:
                break
            h.update(data)
            offset += len(data)
    else:
        buf = bytearray(bufsize)
        view = memoryview(buf)
        while True:
            n = os.preadv(fd, [view], offset)
            if n == 0:
                break
            h.update(view[:n])
            offset += n
    return h.hexdigest()

def hash_pair_parallel_fds(
    src_fd: int,
    dst_fd: int,
    algo: str,
    buffer_mb: int,
) -> Tuple[str, str]:
    """Hash two already-open files concurrently without changing offsets."""
    result = {}
    errors = []
    lock = threading.Lock()

    def worker(name: str, fd: int) -> None:
        try:
            digest = hash_fd(fd, algo, buffer_mb)
            with lock:
                result[name] = digest
        except BaseException as exc:
            with lock:
                errors.append(exc)

    src_thread = threading.Thread(
        target=worker, args=("src", src_fd), name=f"{algo}-src"
    )
    dst_thread = threading.Thread(
        target=worker, args=("dst", dst_fd), name=f"{algo}-dst"
    )
    src_thread.start()
    dst_thread.start()
    src_thread.join()
    dst_thread.join()

    if errors:
        raise errors[0]
    return result["src"], result["dst"]


def hash_pair_parallel_paths(
    src_b: bytes,
    dst_b: bytes,
    algo: str,
    buffer_mb: int,
) -> Tuple[str, str]:
    """Hash source and destination concurrently, one hasher per thread."""
    result = {}
    errors = []
    lock = threading.Lock()

    def worker(name: str, path_b: bytes) -> None:
        try:
            digest = hash_file(path_b, algo, buffer_mb)
            with lock:
                result[name] = digest
        except BaseException as exc:
            with lock:
                errors.append(exc)

    src_thread = threading.Thread(
        target=worker, args=("src", src_b), name=f"{algo}-src"
    )
    dst_thread = threading.Thread(
        target=worker, args=("dst", dst_b), name=f"{algo}-dst"
    )
    src_thread.start()
    dst_thread.start()
    src_thread.join()
    dst_thread.join()

    if errors:
        raise errors[0]
    return result["src"], result["dst"]


def compare_metadata(src_st, dst_st, cfg: dict) -> Optional[bytes]:
    """Compare the configured metadata classes of two objects.

    Returns an error message, or None when everything requested matches.
    Symlinks carry no meaningful mode of their own, so the mode class is
    skipped for them while ownership and timestamps still apply.
    """
    if permissions_mode(cfg) and not stat.S_ISLNK(src_st.st_mode):
        if (src_st.st_mode & 0o7777) != (dst_st.st_mode & 0o7777):
            return (
                f"mode mismatch: {src_st.st_mode & 0o7777:04o} != "
                f"{dst_st.st_mode & 0o7777:04o}"
            ).encode()

    if permissions_owner(cfg) and src_st.st_uid != dst_st.st_uid:
        return f"owner mismatch: {src_st.st_uid} != {dst_st.st_uid}".encode()

    if permissions_group(cfg) and src_st.st_gid != dst_st.st_gid:
        return f"group mismatch: {src_st.st_gid} != {dst_st.st_gid}".encode()

    if mtime_enabled(cfg) and src_st.st_mtime_ns != dst_st.st_mtime_ns:
        return (
            f"mtime mismatch: {src_st.st_mtime_ns} ns != {dst_st.st_mtime_ns} ns"
        ).encode()

    return None


def open_checked_entry_at(parent_fd: int, name_b: bytes, expected) -> int:
    """Pin the regular file or directory seen by lstat without opening a FIFO.

    O_NONBLOCK is needed *before* fstat: an attacker can replace a regular
    file with a FIFO between the two syscalls. On Linux it has no effect on
    reads from a regular file. O_NOFOLLOW refuses a swapped symlink.
    """
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    if stat.S_ISDIR(expected.st_mode):
        flags |= os.O_DIRECTORY
    elif not stat.S_ISREG(expected.st_mode):
        raise OSError(errno.EINVAL, "expected a regular file or directory")

    fd = os.open(name_b, flags, dir_fd=parent_fd)
    try:
        actual = os.fstat(fd)
        if (
            stat.S_IFMT(actual.st_mode) != stat.S_IFMT(expected.st_mode)
            or (int(actual.st_dev), int(actual.st_ino))
            != (int(expected.st_dev), int(expected.st_ino))
        ):
            raise OSError(
                errno.ESTALE, "entry type or identity changed after lstat"
            )
        return fd
    except BaseException:
        os.close(fd)
        raise


def compare_xattrs_at(
    src_parent_fd: int,
    dst_parent_fd: int,
    name_b: bytes,
    *,
    follow_symlinks: bool,
    src_st,
    dst_st,
) -> Optional[bytes]:
    """Compare the objects inspected by lstat, without following a new leaf."""
    if follow_symlinks:
        src_fd = dst_fd = None
        try:
            src_fd = open_checked_entry_at(src_parent_fd, name_b, src_st)
            dst_fd = open_checked_entry_at(dst_parent_fd, name_b, dst_st)
            return compare_xattrs_path(src_fd, dst_fd)
        except OSError as exc:
            return f"cannot pin entries for xattr comparison: {exc}".encode()
        finally:
            if src_fd is not None:
                os.close(src_fd)
            if dst_fd is not None:
                os.close(dst_fd)

    # Python has no fgetxattr on an O_PATH descriptor for a symlink itself.
    # l*xattr on the leaf never follows its target; also detect a changed link
    # on either side before/after comparing it by name.
    src_path = os.fsencode(f"/proc/self/fd/{src_parent_fd}/") + name_b
    dst_path = os.fsencode(f"/proc/self/fd/{dst_parent_fd}/") + name_b
    entries = ((src_parent_fd, src_st), (dst_parent_fd, dst_st))

    def identities_unchanged() -> bool:
        for parent_fd, expected in entries:
            try:
                current = os.stat(name_b, dir_fd=parent_fd,
                                  follow_symlinks=False)
            except OSError:
                return False
            if (current.st_dev, current.st_ino, stat.S_IFMT(current.st_mode)) != (
                expected.st_dev, expected.st_ino, stat.S_IFMT(expected.st_mode)
            ):
                return False
        return True

    if not identities_unchanged():
        return b"symlink identity changed before xattr comparison"
    result = compare_xattrs_path(src_path, dst_path, follow_symlinks=False)
    if not identities_unchanged():
        return b"symlink identity changed during xattr comparison"
    return result


def compare_xattrs_path(
    src_b: bytes, dst_b: bytes, *, follow_symlinks: bool = True
) -> Optional[bytes]:
    try:
        src_names = sorted(os.listxattr(src_b, follow_symlinks=follow_symlinks))
        dst_names = sorted(os.listxattr(dst_b, follow_symlinks=follow_symlinks))
    except (OSError, AttributeError) as exc:
        return f"cannot list xattrs: {exc}".encode()

    if src_names != dst_names:
        missing = sorted(set(src_names) - set(dst_names))
        extra = sorted(set(dst_names) - set(src_names))
        return (
            f"xattr name mismatch: missing={missing}, extra={extra}"
        ).encode()

    for name in src_names:
        try:
            src_value = os.getxattr(src_b, name, follow_symlinks=follow_symlinks)
            dst_value = os.getxattr(dst_b, name, follow_symlinks=follow_symlinks)
        except (OSError, AttributeError) as exc:
            return f"cannot read xattr {name!r}: {exc}".encode()
        if src_value != dst_value:
            return f"xattr value mismatch: {name!r}".encode()

    return None


def destination_is_current(
    src_parent_fd: int,
    dst_parent_fd: int,
    leaf_b: bytes,
    src_st,
    cfg: dict,
    compare_mode: Optional[str],
) -> bool:
    """True when the destination already is what this run would write.

    One fstatat over the already-open parent answers most of the question:
    type, size and every configured metadata class come from that single call.

    Extended attributes are compared when xattr=true, because a destination
    that lacks them is not what this run would write. A comparison that cannot
    be carried out counts as a mismatch: skipping has to be certain, not
    merely unrefuted.

    Hash verify modes deliberately do not read the data here. The point of
    skipping is to avoid touching the file at all; a run that wants certainty
    about content should verify with method=diff afterwards.

    This is sound because the internal engine writes through a temporary name
    and renames: an interrupted copy never appears under its final name, so a
    destination that exists is a destination that was completed.
    """
    try:
        dst_st = os.lstat(leaf_b, dir_fd=dst_parent_fd)
    except OSError:
        return False

    if stat.S_IFMT(src_st.st_mode) != stat.S_IFMT(dst_st.st_mode):
        return False

    if stat.S_ISREG(src_st.st_mode):
        if src_st.st_size != dst_st.st_size:
            return False
    elif stat.S_ISLNK(src_st.st_mode):
        # Two readlink calls at roughly 16 us each, against recreating the link
        # with symlink + chown + utime + rename, each a metadata round trip of
        # milliseconds. Comparing is by far the cheaper of the two.
        try:
            if os.readlink(leaf_b, dir_fd=src_parent_fd) != os.readlink(
                leaf_b, dir_fd=dst_parent_fd
            ):
                return False
        except OSError:
            return False
    elif not stat.S_ISDIR(src_st.st_mode):
        return False

    if compare_metadata(src_st, dst_st, cfg) is not None:
        return False

    if cfg.get("xattr", False):
        if compare_xattrs_at(
            src_parent_fd,
            dst_parent_fd,
            leaf_b,
            follow_symlinks=not stat.S_ISLNK(src_st.st_mode),
            src_st=src_st,
            dst_st=dst_st,
        ) is not None:
            return False

    return True


def compare_files(
    src_b: bytes,
    dst_b: bytes,
    mode: str,
    buffer_mb: int = 8,
) -> Tuple[int, bytes]:
    """Compare two copied filesystem objects without following symlinks.

    Symlinks, including dangling symlinks, are verified by comparing their link
    targets.  Regular files continue to use the configured content/metadata
    comparison mode.
    """
    try:
        s1 = os.lstat(src_b)
    except FileNotFoundError:
        return -1, f"source missing: {path_display(src_b)}".encode()
    except OSError as e:
        return -1, f"cannot stat source {path_display(src_b)}: {e}".encode()

    try:
        s2 = os.lstat(dst_b)
    except FileNotFoundError:
        return -1, f"target missing: {path_display(dst_b)}".encode()
    except OSError as e:
        return -1, f"cannot stat target {path_display(dst_b)}: {e}".encode()

    src_type = stat.S_IFMT(s1.st_mode)
    dst_type = stat.S_IFMT(s2.st_mode)
    if src_type != dst_type:
        return -1, (
            f"type mismatch: {path_display(src_b)} != {path_display(dst_b)}"
        ).encode()

    if stat.S_ISLNK(s1.st_mode):
        try:
            src_target = os.readlink(src_b)
            dst_target = os.readlink(dst_b)
        except OSError as e:
            return -1, f"cannot read symlink target: {e}".encode()

        if src_target != dst_target:
            return -1, (
                f"symlink target mismatch: {path_display(src_b)} -> "
                f"{path_display(src_target)} != {path_display(dst_b)} -> "
                f"{path_display(dst_target)}"
            ).encode()

        return 0, b"symlink target ok"

    if mode is None:
        return 0, b"no compare"

    if s1.st_size != s2.st_size:
        return -1, (
            f"size mismatch: {path_display(src_b)} ({s1.st_size}) != {path_display(dst_b)} ({s2.st_size})".encode()
        )

    if mode == "size":
        return 0, b"size ok"

    if is_hash_compare_mode(mode):
        if not stat.S_ISREG(s1.st_mode):
            return -1, b"hash comparison is supported only for regular files"

        try:
            h1, h2 = hash_pair_parallel_paths(
                src_b, dst_b, mode, buffer_mb
            )
        except (RuntimeError, ValueError, OSError) as e:
            return -1, str(e).encode()

        if h1 != h2:
            return -1, (
                f"{mode} mismatch: {path_display(src_b)} != "
                f"{path_display(dst_b)}"
            ).encode()
        return 0, f"{mode} ok".encode()

    return -1, f"unknown compare mode: {mode}".encode()

# ------------------------------------------------------------
# own implementation for long file paths - slow but secure
# ------------------------------------------------------------
def open_src_dir_fd_from_root(src_root_b: bytes, rel_parent_b: bytes) -> int:
    if isinstance(src_root_b, str):
        src_root_b = os.fsencode(src_root_b)
    if isinstance(rel_parent_b, str):
        rel_parent_b = os.fsencode(rel_parent_b)

    src_fd = open_dir_fd_componentwise(src_root_b, create=False)

    try:
        parts = [p for p in rel_parent_b.split(b"/") if p and p != b"."]

        for part in parts:
            next_fd = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=src_fd,
            )
            os.close(src_fd)
            src_fd = next_fd

        return src_fd

    except Exception:
        try:
            os.close(src_fd)
        except Exception:
            pass
        raise

def open_dir_fd_componentwise(
    abs_path_b: bytes,
    create: bool = False,
    mode: int = 0o755,
    nofollow_from: Optional[bytes] = None,
) -> int:
    """
    Open an absolute directory path component-by-component using openat/mkdirat.
    Returns fd of the final directory. Caller must close it.

    ``nofollow_from`` is a configured root. Components AT OR ABOVE it are
    walked normally, because a root such as /lustre/data reached through a
    symlink is ordinary administration and refusing it would make the tree
    unusable. Components BELOW it are opened with O_NOFOLLOW: those names come
    from the input list or from the tree being written, and following a
    symlink there is how a copy leaves the tree it was told to stay in.

    Without the argument no component is refused, which is the behaviour every
    caller had before the distinction existed.
    """
    if isinstance(abs_path_b, str):
        abs_path_b = os.fsencode(abs_path_b)

    if not abs_path_b.startswith(b"/"):
        raise ValueError("open_dir_fd_componentwise requires absolute path")

    guard_depth = None
    if nofollow_from:
        if isinstance(nofollow_from, str):
            nofollow_from = os.fsencode(nofollow_from)
        root_b = os.path.normpath(nofollow_from)
        if _path_is_below_or_equal(abs_path_b, root_b):
            guard_depth = len([p for p in root_b.split(b"/") if p])

    fd = os.open(b"/", os.O_RDONLY | os.O_DIRECTORY)

    try:
        parts = [p for p in abs_path_b.split(b"/") if p]

        for index, part in enumerate(parts):
            if create:
                try:
                    os.mkdir(part, mode, dir_fd=fd)
                except FileExistsError:
                    pass

            flags = os.O_RDONLY | os.O_DIRECTORY
            if guard_depth is not None and index >= guard_depth:
                flags |= getattr(os, "O_NOFOLLOW", 0)

            next_fd = os.open(
                part,
                flags,
                dir_fd=fd,
            )
            os.close(fd)
            fd = next_fd

        return fd

    except Exception:
        try:
            os.close(fd)
        except Exception:
            pass
        raise

def lstat_path_openat(abs_path_b: bytes):
    """lstat an absolute path without handing the full pathname to the kernel."""
    if isinstance(abs_path_b, str):
        abs_path_b = os.fsencode(abs_path_b)
    if not abs_path_b.startswith(b"/"):
        raise ValueError("lstat_path_openat requires an absolute path")

    parent_b = os.path.dirname(abs_path_b) or b"/"
    leaf_b = os.path.basename(abs_path_b)
    if not leaf_b:
        fd = open_dir_fd_componentwise(parent_b, create=False)
        try:
            return os.fstat(fd)
        finally:
            os.close(fd)

    parent_fd = open_dir_fd_componentwise(parent_b, create=False)
    try:
        return os.lstat(leaf_b, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def open_file_fd_componentwise(
    abs_path_b: bytes,
    flags: int,
    mode: int = 0o666,
    nofollow_from: Optional[bytes] = None,
) -> int:
    """Open one absolute non-directory path via its componentwise-opened parent.

    The parent is walked one component at a time, so no single pathname of the
    whole depth is ever handed to the kernel: this is what lets destinations
    beyond PATH_MAX work at all.
    """
    if isinstance(abs_path_b, str):
        abs_path_b = os.fsencode(abs_path_b)
    if not abs_path_b.startswith(b"/"):
        raise ValueError("open_file_fd_componentwise requires an absolute path")
    parent_b = os.path.dirname(abs_path_b) or b"/"
    leaf_b = os.path.basename(abs_path_b)
    if not leaf_b or leaf_b in (b".", b".."):
        raise ValueError("invalid file leaf for componentwise open")
    parent_fd = open_dir_fd_componentwise(
        parent_b, create=False, nofollow_from=nofollow_from
    )
    try:
        return os.open(leaf_b, flags, mode, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


ownership_skipped_lock = threading.Lock()
ownership_skipped_count = 0


def note_ownership_not_preserved() -> None:
    """Count objects whose owner/group could not be applied.

    chown needs privileges. Tolerating the failure is what allows unprivileged
    runs at all, but it must not be invisible: the counter is reported per
    batch so a run cannot claim success while silently dropping ownership.
    """
    global ownership_skipped_count
    with ownership_skipped_lock:
        ownership_skipped_count += 1


def take_ownership_skipped_count() -> int:
    global ownership_skipped_count
    with ownership_skipped_lock:
        value = ownership_skipped_count
        ownership_skipped_count = 0
        return value


def apply_metadata_fd(fd: int, src_st, cfg: dict) -> None:
    """Apply the configured metadata classes to an open destination object."""
    if permissions_ownership(cfg):
        try:
            os.fchown(
                fd,
                src_st.st_uid if permissions_owner(cfg) else -1,
                src_st.st_gid if permissions_group(cfg) else -1,
            )
        except PermissionError:
            note_ownership_not_preserved()

    if permissions_mode(cfg):
        os.fchmod(fd, src_st.st_mode & 0o7777)

    if mtime_enabled(cfg):
        os.utime(fd, ns=(src_st.st_atime_ns, src_st.st_mtime_ns))


def copy_xattrs_path(src_b: bytes, dst_b: bytes) -> None:
    """Copy and verify xattrs for a short-path filesystem object.

    The source and destination are addressed by pathname here because this
    helper is used only by the normal, PATH_MAX-safe cp path. Long paths use
    the existing fd/openat implementations. Symlink attributes are handled on
    the link itself rather than on its target.
    """
    src_st = os.lstat(src_b)
    dst_st = os.lstat(dst_b)

    if stat.S_IFMT(src_st.st_mode) != stat.S_IFMT(dst_st.st_mode):
        raise OSError(errno.EINVAL, "cannot copy xattrs across different file types")

    follow_symlinks = not stat.S_ISLNK(src_st.st_mode)

    try:
        names = os.listxattr(src_b, follow_symlinks=follow_symlinks)
    except (OSError, AttributeError) as e:
        raise OSError(
            getattr(e, "errno", errno.ENOTSUP) or errno.ENOTSUP,
            f"cannot list source xattrs: {e}",
        ) from e

    for name in names:
        try:
            value = os.getxattr(
                src_b, name, follow_symlinks=follow_symlinks
            )
            os.setxattr(
                dst_b, name, value, follow_symlinks=follow_symlinks
            )
            copied_value = os.getxattr(
                dst_b, name, follow_symlinks=follow_symlinks
            )
        except (OSError, AttributeError) as e:
            raise OSError(
                getattr(e, "errno", errno.EIO) or errno.EIO,
                f"cannot copy xattr {name!r}: {e}",
            ) from e

        if copied_value != value:
            raise OSError(
                errno.EIO,
                f"xattr verification failed for {name!r}",
            )


def copy_xattrs_fd(
    src_fd: int, dst_fd: int, *, prune_extra: bool = False,
) -> None:
    """Copy xattrs through held fds, pruning extra directory attributes.

    This function is deliberately strict.  When ``xattr=true`` is requested,
    silently omitting an attribute would report a successful but incomplete
    copy.  Any list/get/set/verify error therefore aborts the current copy.
    """
    try:
        names = set(os.listxattr(src_fd))
        dst_names = set(os.listxattr(dst_fd)) if prune_extra else set()
    except (OSError, AttributeError) as e:
        raise OSError(
            getattr(e, "errno", errno.ENOTSUP) or errno.ENOTSUP,
            f"cannot list xattrs for copy: {e}",
        ) from e

    for name in sorted(dst_names - names):
        name_b = os.fsencode(name)
        if not (
            name_b.startswith(b"user.")
            or name_b in (b"system.posix_acl_access", b"system.posix_acl_default")
        ):
            # Filesystem/LSM-managed attributes must not be removed merely
            # because a source directory lacks them. Report the mismatch.
            raise OSError(
                errno.EPERM,
                f"extra destination xattr needs manual handling: {name!r}",
            )
        try:
            os.removexattr(dst_fd, name)
        except (OSError, AttributeError) as e:
            raise OSError(
                getattr(e, "errno", errno.EIO) or errno.EIO,
                f"cannot remove extra destination xattr {name!r}: {e}",
            ) from e

    for name in sorted(names):
        try:
            value = os.getxattr(src_fd, name)
            os.setxattr(dst_fd, name, value)
            copied_value = os.getxattr(dst_fd, name)
        except (OSError, AttributeError) as e:
            raise OSError(
                getattr(e, "errno", errno.EIO) or errno.EIO,
                f"cannot copy xattr {name!r}: {e}",
            ) from e

        if copied_value != value:
            raise OSError(
                errno.EIO,
                f"xattr verification failed for {name!r}",
            )

    if prune_extra and set(os.listxattr(dst_fd)) != names:
        raise OSError(errno.EIO, "destination xattr names differ after copy")


def copy_xattrs_at(
    src_parent_fd: int,
    src_name_b: bytes,
    dst_parent_fd: int,
    dst_name_b: bytes,
    *,
    follow_symlinks: bool,
) -> None:
    """Copy xattrs for directory entries addressed relative to open parents.

    ``/proc/self/fd`` keeps the pathname passed to the xattr syscalls short,
    while ``follow_symlinks=False`` allows attributes of a symlink itself to be
    handled without resolving its target.
    """
    src_path = os.fsencode(f"/proc/self/fd/{src_parent_fd}/") + src_name_b
    dst_path = os.fsencode(f"/proc/self/fd/{dst_parent_fd}/") + dst_name_b

    try:
        names = os.listxattr(src_path, follow_symlinks=follow_symlinks)
    except (OSError, AttributeError) as e:
        raise OSError(
            getattr(e, "errno", errno.ENOTSUP) or errno.ENOTSUP,
            f"cannot list source xattrs: {e}",
        ) from e

    for name in names:
        try:
            value = os.getxattr(
                src_path,
                name,
                follow_symlinks=follow_symlinks,
            )
            os.setxattr(
                dst_path,
                name,
                value,
                follow_symlinks=follow_symlinks,
            )
            copied_value = os.getxattr(
                dst_path,
                name,
                follow_symlinks=follow_symlinks,
            )
        except (OSError, AttributeError) as e:
            raise OSError(
                getattr(e, "errno", errno.EIO) or errno.EIO,
                f"cannot copy xattr {name!r}: {e}",
            ) from e

        if copied_value != value:
            raise OSError(
                errno.EIO,
                f"xattr verification failed for {name!r}",
            )


def prelock_batch(paths, cfg: dict) -> None:
    """Lock the existing parent directories of a batch with one journal flush.

    Without this every existing directory costs its own durable journal
    write when the walk first reaches it. Best effort: whatever cannot be
    resolved here is left to the ordinary walk, which locks it itself.
    """
    guard = destination_guard
    if guard is None:
        return
    rel_dirs = set()
    for path_b in paths:
        try:
            rel_b, _src_abs_b, _dst_b = normalize_input_path(path_b, cfg)
        except Exception:
            continue
        if isinstance(rel_b, str):
            rel_b = os.fsencode(rel_b)
        if rel_b != b".":
            rel_dirs.add(os.path.dirname(rel_b))
    if not rel_dirs:
        return
    try:
        guard.prelock(rel_dirs)
    except Exception as exc:
        log(f"WARNING: prelocking the batch directories failed, locking one by one: {exc}")


def open_destination_root(dst_root_b: bytes) -> int:
    """A descriptor for dst_root: the held one when the run guards it."""
    guard = destination_guard
    if guard is not None and os.path.normpath(dst_root_b) == guard.dst_root_b:
        return guard.open_root()
    return open_dir_fd_componentwise(dst_root_b, create=True)


def prepare_destination_directory(
    dst_parent_fd: int,
    name_b: bytes,
    rel_b: bytes,
    src_st,
    src_fd: Optional[int],
    cfg: Optional[dict],
) -> Tuple[int, bool]:
    """Create or open one destination directory, locked, and record it.

    A new directory is created 0700 and is therefore locked from its first
    moment; an existing one is locked here, with its original values recorded
    and journalled first. Nothing of the directory's final metadata is applied
    now - owner, mode, ACL, xattrs and times are recorded and set by the
    restore at the end, bottom-up, when nothing writes into the tree any more.

    Returns (fd, created). The caller owns the descriptor.
    """
    created = False
    dst_st = None
    try:
        os.mkdir(name_b, DESTINATION_LOCK_PERMISSIONS, dir_fd=dst_parent_fd)
        created = True
    except FileExistsError:
        dst_st = os.stat(name_b, dir_fd=dst_parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(dst_st.st_mode):
            raise NotADirectoryError(
                errno.ENOTDIR,
                f"destination path component is not a directory: "
                f"{path_display(name_b)}",
            )

    fd = os.open(
        name_b,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
        dir_fd=dst_parent_fd,
    )
    try:
        if dst_st is not None:
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino) != (dst_st.st_dev, dst_st.st_ino):
                raise OSError(
                    errno.ESTALE, "destination directory changed while opening it"
                )

        guard = destination_guard
        if guard is None:
            # Nothing guards this tree (a caller outside a copy or rsync run):
            # the metadata is applied at once, as it always was.
            if cfg is not None and preserves_any_metadata(cfg) and created:
                apply_metadata_fd(fd, src_st, cfg)
            if created and (cfg is None or not permissions_mode(cfg)):
                os.fchmod(fd, 0o777 & ~get_process_umask())
            return fd, created

        locked = guard.lock_directory(fd, rel_b)
        record_directory_final(
            rel_b, src_st, src_fd, cfg if cfg is not None else get_config_snapshot(),
            created, locked,
        )
        # Only fsync="file" pays for durability of a directory entry.
        if created and cfg is not None and fsync_per_file(cfg):
            try:
                os.fsync(fd)
            except OSError:
                pass
        return fd, created
    except BaseException:
        os.close(fd)
        raise


def open_src_and_dst_dir_fds(
    src_root_b: bytes,
    dst_root_b: bytes,
    rel_parent_b: bytes,
    xattr_copy: bool = False,
    metadata_cfg: Optional[dict] = None,
) -> Tuple[int, int]:
    """
    Open/create dst_root/rel_parent_b component-by-component.

    Every destination directory on the way is locked and recorded by
    prepare_destination_directory(); its final metadata, xattrs included, is
    applied by the restore at the end of the run. xattr_copy is kept for the
    callers' signature and decides nothing here any more: whether xattrs are
    recorded follows the configuration.

    Source and destination are walked once in lockstep and BOTH resulting
    parent fds are returned, because the source chain has to be opened anyway
    to read the metadata of each directory component. Callers that need the
    source parent as well must therefore not walk it a second time.

    Returns (src_parent_fd, dst_parent_fd). Caller must close both.
    """
    if isinstance(src_root_b, str):
        src_root_b = os.fsencode(src_root_b)
    if isinstance(dst_root_b, str):
        dst_root_b = os.fsencode(dst_root_b)
    if isinstance(rel_parent_b, str):
        rel_parent_b = os.fsencode(rel_parent_b)

    if not src_root_b.startswith(b"/") or not dst_root_b.startswith(b"/"):
        raise ValueError("absolute src_root and dst_root required")

    src_fd = None
    dst_fd = None

    try:
        src_fd = open_dir_fd_componentwise(src_root_b, create=False)
        dst_fd = open_destination_root(dst_root_b)
        parts = [p for p in rel_parent_b.split(b"/") if p and p != b"."]
        walked_rel_b = b""

        for part in parts:
            walked_rel_b = (
                os.path.join(walked_rel_b, part) if walked_rel_b else part
            )
            next_src_fd = None
            next_dst_fd = None
            try:
                next_src_fd = os.open(
                    part,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=src_fd,
                )
                src_st = os.fstat(next_src_fd)
                next_dst_fd, _created = prepare_destination_directory(
                    dst_fd, part, walked_rel_b, src_st, next_src_fd, metadata_cfg
                )
            except Exception:
                if next_src_fd is not None:
                    try:
                        os.close(next_src_fd)
                    except OSError:
                        pass
                if next_dst_fd is not None:
                    try:
                        os.close(next_dst_fd)
                    except OSError:
                        pass
                raise

            os.close(src_fd)
            os.close(dst_fd)
            src_fd = next_src_fd
            dst_fd = next_dst_fd

        result_src_fd = src_fd
        result_dst_fd = dst_fd
        src_fd = None
        dst_fd = None
        return result_src_fd, result_dst_fd

    finally:
        if src_fd is not None:
            try:
                os.close(src_fd)
            except OSError:
                pass
        if dst_fd is not None:
            try:
                os.close(dst_fd)
            except OSError:
                pass


def open_dst_dir_fd_with_attrs(
    src_root_b: bytes,
    dst_root_b: bytes,
    rel_parent_b: bytes,
    xattr_copy: bool = False,
    metadata_cfg: Optional[dict] = None,
) -> int:
    """Destination-only wrapper around open_src_and_dst_dir_fds().

    Returns fd of final destination parent directory. Caller must close it.
    """
    src_parent_fd, dst_parent_fd = open_src_and_dst_dir_fds(
        src_root_b,
        dst_root_b,
        rel_parent_b,
        xattr_copy,
        metadata_cfg,
    )
    try:
        os.close(src_parent_fd)
    except OSError:
        pass
    return dst_parent_fd


class ParentDirFdCache:
    """Reuse source/destination parent directory fds inside one worker batch.

    Without this cache every single file re-walks src_root and dst_root from
    "/" downwards, which costs O(root_depth + rel_depth) openat calls per file
    and is the dominant metadata cost on network filesystems. Input produced by
    'find -print0' is grouped by directory and batches are cut from consecutive
    records, so one batch almost always shares a single parent directory.

    The cache lives only for the duration of one batch inside one worker
    thread, so it is never shared between threads. Entries are bounded and
    evicted least-recently-used; close() must be called when the batch ends.

    Trade-off: files of the same batch that share a parent directory resolve
    that directory exactly once. A source parent replaced mid-batch is
    therefore noticed by the per-file st_dev/st_ino checks rather than by the
    directory walk itself.
    """

    def __init__(
        self,
        src_root_b: bytes,
        dst_root_b: bytes,
        xattr_copy: bool,
        metadata_cfg: Optional[dict],
        max_entries: int = 16,
    ) -> None:
        self.src_root_b = src_root_b
        self.dst_root_b = dst_root_b
        self.xattr_copy = bool(xattr_copy)
        self.metadata_cfg = metadata_cfg
        self.max_entries = max(1, int(max_entries))
        self._entries = OrderedDict()

    def get(self, rel_parent_b: bytes) -> Tuple[int, int]:
        entry = self._entries.get(rel_parent_b)
        if entry is not None:
            self._entries.move_to_end(rel_parent_b)
            return entry

        fds = open_src_and_dst_dir_fds(
            self.src_root_b,
            self.dst_root_b,
            rel_parent_b,
            self.xattr_copy,
            self.metadata_cfg,
        )

        self._entries[rel_parent_b] = fds
        while len(self._entries) > self.max_entries:
            _, evicted = self._entries.popitem(last=False)
            self._close_pair(evicted)
        return fds

    @staticmethod
    def _close_pair(fds: Tuple[int, int]) -> None:
        for fd in fds:
            try:
                os.close(fd)
            except OSError:
                pass

    def close(self) -> None:
        while self._entries:
            _, fds = self._entries.popitem()
            self._close_pair(fds)


class DiffParentDirFdCache:
    """Bounded, read-only parent fd cache for a single diff worker batch.

    The copy cache creates destination directories and updates metadata, so
    it cannot be used for comparisons. Hold both checked parent descriptors
    while neighbouring input paths are compared, and close them at batch end.
    """

    def __init__(self, src_root_b: bytes, dst_root_b: bytes,
                 max_entries: int = 16) -> None:
        self.src_root_b = src_root_b
        self.dst_root_b = dst_root_b
        self.max_entries = max(1, int(max_entries))
        self._entries = OrderedDict()

    def get(self, rel_parent_b: bytes) -> Tuple[int, int]:
        entry = self._entries.get(rel_parent_b)
        if entry is not None:
            self._entries.move_to_end(rel_parent_b)
            return entry

        src_fd = open_src_dir_fd_from_root(self.src_root_b, rel_parent_b)
        try:
            dst_fd = open_src_dir_fd_from_root(self.dst_root_b, rel_parent_b)
        except BaseException:
            os.close(src_fd)
            raise

        entry = (src_fd, dst_fd)
        self._entries[rel_parent_b] = entry
        while len(self._entries) > self.max_entries:
            _, old = self._entries.popitem(last=False)
            ParentDirFdCache._close_pair(old)
        return entry

    def close(self) -> None:
        while self._entries:
            _, fds = self._entries.popitem()
            ParentDirFdCache._close_pair(fds)


# ------------------------------------------------------------
# Destination metadata: protection during the run, restore at the end
# ------------------------------------------------------------
#
# copy and rsync write into destination directories that are LOCKED for the
# whole run: owned by the user running this program, no permissions for group
# or other. Nobody else can then reach anything below a locked dst_root by
# path, whatever the final permissions of the tree are going to be. Every
# metadata value a directory must carry in the end - and for rsync also the
# owner, mode, ACL and xattrs of files and symlinks - is recorded instead of
# being applied, and applied once, after all batches and the hard-link phase.
#
# One logical entry is "path -> {the metadata to set at the end}". The keys
# that are present ARE the fields; there is no field mask. A missing key means
# "leave as it is", while for example acl_access=None means "remove the ACL".
#
# Two kinds of records keep source values and original destination values
# apart:
#
#   FINAL  the value the configured metadata classes ask for, taken from the
#          source. Recorded whenever the object is met; the last one wins.
#   ORIG   the value a pre-existing destination directory had before the lock
#          changed it. Recorded only at the moment the lock actually changes
#          a field, which happens once: a later batch finds the directory
#          already locked, changes nothing and therefore records nothing - it
#          can never store the temporary 0700 as the original. The first one
#          wins.
#
# At restore time a FINAL value overrides an ORIG value of the same field.
#
# Record layout, all lengths as unsigned LEB128 varints:
#
#   len(body) body
#   body = len(path) path, u8 kind, u8 origin, { u8 key, len(value)+1 value }*
#
# A value length of 0 encodes None. Integers are varints, times are two
# little-endian int64 nanosecond values, xattrs a sequence of
# len(name) name len(value) value.

META_KIND_DIR = 1
META_KIND_FILE = 2
META_KIND_SYMLINK = 3
META_ORIGIN_FINAL = 1
META_ORIGIN_ORIG = 2

_META_KEYS = {
    "uid": 1,
    "gid": 2,
    "mode": 3,
    "acl_access": 4,
    "acl_default": 5,
    "xattrs": 6,
    "times": 7,
    # Present when the run locked (or created locked) this directory. At the
    # restore the directory must still be locked; if it is not, somebody or
    # something released it during the run.
    "locked": 8,
    # ORIG records only: (st_dev, st_ino) of the directory whose original
    # values they are. Checked before anything is rolled back, never applied.
    "identity": 9,
}
_META_KEY_NAMES = {number: name for name, number in _META_KEYS.items()}
_META_INT_KEYS = frozenset(("uid", "gid", "mode"))
_META_TIMES = struct.Struct("<qq")
# A single record can never be larger than this. The limit exists so a corrupt
# stream cannot make a reader buffer without bound.
META_MAX_RECORD = 16 * 1024 * 1024

ACL_ACCESS_XATTR = "system.posix_acl_access"
ACL_DEFAULT_XATTR = "system.posix_acl_default"
_ACL_FIELDS = (("acl_access", ACL_ACCESS_XATTR), ("acl_default", ACL_DEFAULT_XATTR))
_ACL_XATTR_NAMES = frozenset((ACL_ACCESS_XATTR, ACL_DEFAULT_XATTR))

# The lock: owner rwx, nothing for group and other. Special bits are kept,
# they grant nobody access.
DESTINATION_LOCK_PERMISSIONS = 0o700


def _put_varint(out: bytearray, value: int) -> None:
    value = int(value)
    if value < 0:
        raise ValueError(f"negative value cannot be stored as varint: {value}")
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


def _try_varint(buf, pos: int) -> Tuple[Optional[int], int]:
    """Decode one varint; (None, pos) when the buffer ends inside it."""
    shift = 0
    value = 0
    while True:
        if pos >= len(buf):
            return None, pos
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift > 63:
            raise ValueError("corrupt metadata record: varint too long")


def _get_varint(buf, pos: int) -> Tuple[int, int]:
    value, pos = _try_varint(buf, pos)
    if value is None:
        raise ValueError("corrupt metadata record: truncated varint")
    return value, pos


def _encode_meta_value(key: str, value) -> Optional[bytes]:
    if value is None:
        return None
    if key in _META_INT_KEYS:
        out = bytearray()
        _put_varint(out, value)
        return bytes(out)
    if key == "times":
        return _META_TIMES.pack(int(value[0]), int(value[1]))
    if key == "locked":
        return b""
    if key == "identity":
        out = bytearray()
        _put_varint(out, value[0])
        _put_varint(out, value[1])
        return bytes(out)
    if key == "xattrs":
        out = bytearray()
        for name in sorted(value):
            name_b = os.fsencode(name)
            value_b = bytes(value[name])
            _put_varint(out, len(name_b))
            out += name_b
            _put_varint(out, len(value_b))
            out += value_b
        return bytes(out)
    return bytes(value)


def _decode_meta_value(key: str, raw: Optional[bytes]):
    if raw is None:
        return None
    if key in _META_INT_KEYS:
        value, pos = _get_varint(raw, 0)
        if pos != len(raw):
            raise ValueError(f"corrupt metadata record: trailing bytes in {key}")
        return value
    if key == "times":
        if len(raw) != _META_TIMES.size:
            raise ValueError("corrupt metadata record: bad times field")
        return _META_TIMES.unpack(raw)
    if key == "locked":
        return True
    if key == "identity":
        device, pos = _get_varint(raw, 0)
        inode, pos = _get_varint(raw, pos)
        if pos != len(raw):
            raise ValueError("corrupt metadata record: bad identity field")
        return (device, inode)
    if key == "xattrs":
        result = {}
        pos = 0
        while pos < len(raw):
            length, pos = _get_varint(raw, pos)
            name_b = raw[pos:pos + length]
            pos += length
            length, pos = _get_varint(raw, pos)
            value_b = raw[pos:pos + length]
            pos += length
            if pos > len(raw):
                raise ValueError("corrupt metadata record: bad xattrs field")
            result[name_b] = value_b
        return result
    return raw


def encode_meta_record(path_b: bytes, kind: int, origin: int, fields: dict) -> bytes:
    """One length-prefixed record: path -> fields."""
    body = bytearray()
    _put_varint(body, len(path_b))
    body += path_b
    body.append(kind)
    body.append(origin)
    for key in sorted(fields, key=_META_KEYS.__getitem__):
        raw = _encode_meta_value(key, fields[key])
        body.append(_META_KEYS[key])
        if raw is None:
            _put_varint(body, 0)
        else:
            _put_varint(body, len(raw) + 1)
            body += raw
    if len(body) > META_MAX_RECORD:
        raise ValueError(
            f"metadata record for {path_display(path_b)} exceeds "
            f"{META_MAX_RECORD} bytes"
        )
    out = bytearray()
    _put_varint(out, len(body))
    out += body
    return bytes(out)


def meta_body_path(body: bytes) -> bytes:
    length, pos = _get_varint(body, 0)
    if pos + length > len(body):
        raise ValueError("corrupt metadata record: path runs past the record")
    return bytes(body[pos:pos + length])


def decode_meta_body(body: bytes) -> Tuple[bytes, int, int, dict]:
    length, pos = _get_varint(body, 0)
    path_b = bytes(body[pos:pos + length])
    pos += length
    if pos + 2 > len(body):
        raise ValueError("corrupt metadata record: missing kind/origin")
    kind = body[pos]
    origin = body[pos + 1]
    pos += 2
    fields = {}
    while pos < len(body):
        key = _META_KEY_NAMES.get(body[pos])
        if key is None:
            raise ValueError(f"corrupt metadata record: unknown key {body[pos]}")
        pos += 1
        length, pos = _get_varint(body, pos)
        if length == 0:
            raw = None
        else:
            raw = bytes(body[pos:pos + length - 1])
            pos += length - 1
            if pos > len(body):
                raise ValueError("corrupt metadata record: value runs past the record")
        fields[key] = _decode_meta_value(key, raw)
    return path_b, kind, origin, fields


def iter_meta_bodies(chunks, label: str, tolerate_truncated_tail: bool = False):
    """Split a stream of length-prefixed records into record bodies."""
    pending = b""
    for chunk in chunks:
        pending = pending + chunk if pending else bytes(chunk)
        pos = 0
        while True:
            length, start = _try_varint(pending, pos)
            if length is None:
                break
            if length > META_MAX_RECORD:
                raise ValueError(
                    f"corrupt metadata log {label}: record of {length} bytes"
                )
            if start + length > len(pending):
                break
            yield pending[start:start + length]
            pos = start + length
        pending = pending[pos:]
    if pending and not tolerate_truncated_tail:
        # Dropping it silently would leave exactly the objects unrestored
        # whose loss the record count in the log would not reveal.
        raise ValueError(
            f"metadata log {label} ends inside a record; {len(pending)} "
            f"trailing bytes are not a complete entry"
        )


class MetadataLog:
    """Append-only, compressed log of metadata records, spilled when large.

    Held in memory first; a tree small enough to fit leaves nothing behind.
    The limit is measured on the COMPRESSED bytes, because that is what is
    actually held. Above it the whole stream moves into a file created in the
    working directory, and recording goes on there.

    This is not a crash-safe journal. It exists so that the restore at the end
    of a run knows what to do; a process that dies takes the in-memory part
    with it. What must survive a crash - the original values of destination
    directories the lock changed - goes through DestinationJournal as well.
    """

    FILE_MAGIC = b"parallel_tools-destination-metadata\n"
    FILE_FORMAT_VERSION = 1

    def __init__(
        self,
        maxsize: int,
        compression: str = "zlib",
        dst_root: Optional[str] = None,
        label: str = "dstmeta",
    ) -> None:
        self.maxsize = max(1, int(maxsize))
        self.compression = compression
        self.dst_root = dst_root
        self.label = label
        self.lock = threading.Lock()
        self.record_count = 0
        self.closed = False
        self.buffer = bytearray()
        # A compressor does not hand out every byte it is given: deflate
        # accumulates roughly a window's worth of input before it emits
        # anything. Input fed since the last emission is an exact upper bound
        # on what is held in there, so the two together are what "size" means.
        self.pending_input = 0
        self.stream = None
        self.path = None
        self.name = None
        self.identity = None
        self.header_size = 0
        # Set when a spill was needed and could not be done. Recording stops
        # there: growing without bound would trade wrong metadata for a dead
        # machine. What is already held is still restored.
        self.spill_refused = False
        self.compressor = self._make_compressor()

    def _make_compressor(self):
        if self.compression == "zlib":
            # Level 1: measured 6x smaller at 185 MB/s. The gzip container
            # keeps a spilled file zcat-readable.
            return zlib.compressobj(1, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        if self.compression == "zstd":
            return zstandard.ZstdCompressor(level=1).compressobj()
        return None

    def _header_bytes(self) -> bytes:
        payload = json.dumps(
            {
                "version": self.FILE_FORMAT_VERSION,
                "program_version": PROGRAM_VERSION,
                "compression": self.compression,
                "dst_root": self.dst_root,
            },
            ensure_ascii=True,
            sort_keys=True,
        ).encode("ascii")
        return self.FILE_MAGIC + payload + b"\n"

    def _spill_locked(self) -> bool:
        fd = working_directory_fd()
        if fd < 0:
            self.spill_refused = True
            metadata_restore_incomplete.set()
            log(
                f"WARNING: destination metadata exceeds metadata_maxsize="
                f"{self.maxsize} bytes and there is no usable working "
                f"directory to spill into ({working_directory_problem()}). "
                f"Recording stops here; the {self.record_count} records "
                f"already held are still restored, later objects are not"
            )
            return False

        name = run_state_name(self.label, ".log")
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                0o600,
                dir_fd=fd,
            )
        except OSError as exc:
            self.spill_refused = True
            metadata_restore_incomplete.set()
            log(f"WARNING: cannot create the destination metadata spill: {exc}")
            return False

        try:
            written = os.fstat(descriptor)
            self.identity = (int(written.st_dev), int(written.st_ino))
            self.stream = open(descriptor, "wb", buffering=1024 * 1024,
                               closefd=True)
        except BaseException:
            os.close(descriptor)
            self.spill_refused = True
            metadata_restore_incomplete.set()
            raise

        self.name = name
        self.path = os.path.join(working_directory_display(), name)
        header = self._header_bytes()
        self.stream.write(header)
        self.header_size = len(header)
        self.stream.write(bytes(self.buffer))
        held = len(self.buffer) + self.pending_input
        self.buffer = bytearray()
        self.pending_input = 0
        log(
            f"destination metadata: {held} compressed bytes over "
            f"{self.record_count} records reached metadata_maxsize; spilled "
            f"to {self.path}"
        )
        return True

    def record_raw(self, record: bytes) -> None:
        """Append one already encoded, length-prefixed record."""
        with self.lock:
            if self.closed or self.spill_refused:
                if self.closed:
                    raise RuntimeError(f"{self.label} log is already closed")
                return
            payload = record
            if self.compressor is not None:
                compressed = self.compressor.compress(payload)
                if compressed:
                    self.pending_input = 0
                else:
                    self.pending_input += len(payload)
                payload = compressed
            if payload:
                if self.stream is not None:
                    self.stream.write(payload)
                else:
                    self.buffer += payload
            self.record_count += 1
            if (
                self.stream is None
                and len(self.buffer) + self.pending_input >= self.maxsize
            ):
                self._spill_locked()

    def record_entry(self, path_b: bytes, kind: int, origin: int, fields: dict) -> None:
        self.record_raw(encode_meta_record(path_b, kind, origin, fields))

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            tail = self.compressor.flush() if self.compressor is not None else b""
            if self.stream is None:
                if tail:
                    self.buffer += tail
                return
            if tail:
                self.stream.write(tail)
            self.stream.flush()
            self.stream.close()

    def discard(self) -> None:
        """Remove the spilled file, if there is one."""
        self.buffer = bytearray()
        if not self.name:
            return
        fd = working_directory_fd()
        if fd < 0:
            return
        try:
            os.unlink(self.name, dir_fd=fd)
        except OSError:
            pass

    def _reader_for(self, stream):
        if self.compression == "none":
            return stream
        if self.compression == "zstd":
            return zstandard.ZstdDecompressor().stream_reader(
                stream, read_across_frames=True
            )
        return gzip.GzipFile(fileobj=stream, mode="rb")

    def _iter_raw_chunks(self, chunk_size: int = 1024 * 1024):
        if self.name is None:
            reader = self._reader_for(io.BytesIO(bytes(self.buffer)))
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    return
                yield chunk

        fd = working_directory_fd()
        if fd < 0:
            raise RuntimeError(
                f"the working directory holding {self.path} is gone"
            )
        descriptor = os.open(
            self.name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
            dir_fd=fd,
        )
        try:
            reopened = os.fstat(descriptor)
            if (int(reopened.st_dev), int(reopened.st_ino)) != self.identity:
                raise RuntimeError(
                    f"destination metadata log was replaced between writing "
                    f"and reading: {self.path}"
                )
        except BaseException:
            os.close(descriptor)
            raise
        with open(descriptor, "rb", buffering=chunk_size, closefd=True) as stream:
            stream.seek(self.header_size)
            reader = self._reader_for(stream)
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    return
                yield chunk

    def bodies(self):
        """Yield record bodies in write order, streaming."""
        yield from iter_meta_bodies(
            self._iter_raw_chunks(), self.path or f"<{self.label} in memory>"
        )


class DestinationJournal:
    """Write-ahead journal of original destination values changed by the lock.

    The MetadataLog alone is not crash-safe: after a killed process the
    original owner and mode of a pre-existing directory that was locked would
    be lost, and the directory would stay 0700 for good. Every ORIG record is
    therefore made durable HERE before the change it describes is made.

    Writes are group-committed: whoever finds no fsync in progress performs
    one for everything written so far, everybody else waits for it. There is
    no timer; concurrent lockers share an fsync naturally, and a single one
    pays exactly one.

    The name is fixed per dst_root, so the next start finds the journal of a
    run that died and rolls the originals back before it locks anything.
    Only ORIG records are journalled. They are rare - a destination directory
    is changed once, when it already existed - so a fresh copy pays nothing.
    """

    MAGIC = b"parallel_tools-destination-journal\n"
    # 2: the header names the identity of dst_root, and every record the
    # identity of its directory. Version 1 journals carried neither and are
    # not rolled back: nothing could tell whether the path still holds the
    # directory whose values they are.
    VERSION = 2

    def __init__(self, dst_root_b: bytes) -> None:
        self.dst_root_b = dst_root_b
        # (st_dev, st_ino) of the held dst_root; set before the first write.
        self.root_identity: Optional[Tuple[int, int]] = None
        self.name = self.name_for(dst_root_b)
        self.fd = -1
        self.cond = threading.Condition()
        self.written = 0
        self.synced = 0
        self.syncing = False
        self.failed: Optional[str] = None
        self.used = False

    @staticmethod
    def name_for(dst_root_b: bytes) -> str:
        digest = hashlib.sha256(os.path.normpath(dst_root_b)).hexdigest()[:24]
        return f"dstlock-{digest}.journal"

    def _header(self) -> bytes:
        payload = json.dumps(
            {
                "version": self.VERSION,
                "dst_root_b64": base64.b64encode(self.dst_root_b).decode("ascii"),
                "dst_root_identity": list(self.root_identity or ()),
                "program_version": PROGRAM_VERSION,
                "pid": os.getpid(),
            },
            ensure_ascii=True,
            sort_keys=True,
        ).encode("ascii")
        return self.MAGIC + payload + b"\n"

    def _open_locked(self) -> None:
        wd = working_directory_fd()
        if wd < 0:
            raise OSError(
                errno.ENOENT,
                f"no usable working directory for the destination journal "
                f"({working_directory_problem()})",
            )
        fd = os.open(
            self.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND
            | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
            0o600,
            dir_fd=wd,
        )
        try:
            _write_all(fd, self._header())
            os.fsync(fd)
            # The directory entry itself must survive a crash, or the next
            # start finds nothing to roll back.
            os.fsync(wd)
        except BaseException:
            os.close(fd)
            try:
                os.unlink(self.name, dir_fd=wd)
            except OSError:
                pass
            raise
        self.fd = fd
        self.used = True
        log(
            f"destination journal: {os.path.join(working_directory_display(), self.name)}"
        )

    def append_durable(self, record: bytes) -> None:
        """Return only once the record is on stable storage."""
        with self.cond:
            if self.failed is not None:
                raise OSError(errno.EIO, f"destination journal failed earlier: {self.failed}")
            if self.fd < 0:
                self._open_locked()
            _write_all(self.fd, record)
            self.written += 1
            ticket = self.written
            while self.synced < ticket:
                if self.failed is not None:
                    raise OSError(errno.EIO, f"destination journal fsync failed: {self.failed}")
                if self.syncing:
                    self.cond.wait()
                    continue
                self.syncing = True
                target = self.written
                error = None
                self.cond.release()
                try:
                    os.fsync(self.fd)
                except OSError as exc:
                    error = exc
                finally:
                    self.cond.acquire()
                    self.syncing = False
                if error is not None:
                    self.failed = str(error)
                    self.cond.notify_all()
                    raise error
                self.synced = max(self.synced, target)
                self.cond.notify_all()

    def remove(self) -> None:
        """The run restored everything it changed: nothing left to roll back."""
        with self.cond:
            if self.fd >= 0:
                try:
                    os.close(self.fd)
                except OSError:
                    pass
                self.fd = -1
            if not self.used:
                return
            wd = working_directory_fd()
            if wd < 0:
                return
            try:
                os.unlink(self.name, dir_fd=wd)
            except FileNotFoundError:
                pass
            self.used = False

    def close_keep(self) -> None:
        """Leave the journal for the next start to roll back."""
        with self.cond:
            if self.fd >= 0:
                try:
                    os.close(self.fd)
                except OSError:
                    pass
                self.fd = -1

    @classmethod
    def read_existing(cls, dst_root_b: bytes):
        """(header, record bodies) of a journal left by a dead run, or None.

        The bodies come as a generator that reads the file block by block:
        a journal over a very large tree is never held in memory as a whole.
        The header is read and checked before anything is returned.
        """
        directory = working_directory_display()
        name = cls.name_for(dst_root_b)
        label = os.path.join(directory, name)
        if working_directory_problem() is not None or not os.path.isdir(directory):
            return None
        wd = working_directory_fd()
        if wd < 0:
            return None
        try:
            fd = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                dir_fd=wd,
            )
        except FileNotFoundError:
            return None
        try:
            head = b""
            while len(head) < 64 * 1024:
                chunk = os.read(fd, 64 * 1024)
                if not chunk:
                    break
                head += chunk
                if head.find(b"\n", len(cls.MAGIC)) >= 0:
                    break
            if not head.startswith(cls.MAGIC) and not cls.MAGIC.startswith(head):
                raise ValueError(f"{label} is not a destination journal")
            end = head.find(b"\n", len(cls.MAGIC))
            if end < 0:
                # The header itself never became durable: nothing was changed.
                os.close(fd)
                return {}, iter(())
            header = json.loads(head[len(cls.MAGIC):end].decode("ascii"))
            if header.get("version") != cls.VERSION:
                raise ValueError(
                    f"{label} has journal format {header.get('version')!r}, this "
                    f"program rolls back only format {cls.VERSION}. Inspect the "
                    f"destination by hand and remove the journal"
                )
            if base64.b64decode(header.get("dst_root_b64", "")) != os.path.normpath(dst_root_b):
                raise ValueError(f"{label} belongs to another dst_root")
        except BaseException:
            os.close(fd)
            raise

        def chunks():
            try:
                yield head[end + 1:]
                while True:
                    chunk = os.read(fd, 1024 * 1024)
                    if not chunk:
                        return
                    yield chunk
            finally:
                os.close(fd)

        # A record whose write was cut off never reached fsync, and the change
        # it announces was therefore never made.
        return header, iter_meta_bodies(chunks(), label, tolerate_truncated_tail=True)

    @classmethod
    def discard_existing(cls, dst_root_b: bytes) -> None:
        wd = working_directory_fd()
        if wd < 0:
            return
        try:
            os.unlink(cls.name_for(dst_root_b), dir_fd=wd)
        except FileNotFoundError:
            pass


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _read_xattr_or_none(getter, name: str) -> Optional[bytes]:
    try:
        return getter(name)
    except OSError as exc:
        if exc.errno in (errno.ENODATA, getattr(errno, "ENOATTR", errno.ENODATA),
                         errno.ENOTSUP, errno.EOPNOTSUPP):
            return None
        raise


class DestinationGuard:
    """Holds dst_root, locks destination directories and reports how well.

    dst_root is opened ONCE and held for the whole run. Every walk below it
    starts from a duplicate of that descriptor instead of resolving the path
    from "/" again, so a dst_root renamed or replaced during the run cannot
    redirect the writes or the final restore anywhere else.
    """

    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self.dst_root_b = os.fsencode(os.path.normpath(cfg["dst_root"]))
        self.euid = os.geteuid()
        self.root_fd = -1
        self.root_identity = None
        self.root_created = False
        self.lock_held = False
        self.chain_problem: Optional[str] = None
        self.root_problem: Optional[str] = None
        self.lock = threading.Lock()
        self.unlocked_count = 0
        # Directories that could not be locked, by identity: several workers
        # meet the same one, and it is one unprotected directory, not three.
        # Only failures are held, so this stays small.
        self.unlocked_identities = set()
        self.violation_count = 0
        self.journal = DestinationJournal(self.dst_root_b)
        self.journal_problem: Optional[str] = None

    # -- state ---------------------------------------------------------

    def is_locked_state(self, st) -> bool:
        return (
            int(st.st_uid) == self.euid
            and (st.st_mode & 0o077) == 0
            and (st.st_mode & 0o700) == DESTINATION_LOCK_PERMISSIONS
        )

    def status(self) -> str:
        if self.chain_problem is not None or self.root_problem is not None:
            return "not_enforced"
        if self.unlocked_count or self.violation_count:
            return "partial"
        return "enforced"

    def summary(self) -> dict:
        return {
            "destination_protection": self.status(),
            "destination_unlocked_directory_count": self.unlocked_count,
            "destination_protection_violation_count": self.violation_count,
            "destination_root_problem": self.root_problem,
            "destination_parent_chain_problem": self.chain_problem,
            "destination_journal_available": self.journal_problem is None,
        }

    def note_unlocked(self, rel_b: bytes, reason: str) -> None:
        with self.lock:
            self.unlocked_count += 1
            count = self.unlocked_count
        if count <= 20:
            log(
                f"WARNING: destination directory is NOT protected during the "
                f"run: {path_display(rel_b)}: {reason}"
            )
        elif count == 21:
            log("WARNING: further unprotected destination directories are only counted")

    def note_violation(self, rel_b: bytes, st) -> None:
        with self.lock:
            self.violation_count += 1
            count = self.violation_count
        if count <= 20:
            log(
                f"WARNING: destination directory was released during the run: "
                f"{path_display(rel_b)} is uid={st.st_uid} "
                f"mode={stat.S_IMODE(st.st_mode):04o}, not locked"
            )

    # -- setup ---------------------------------------------------------

    def _check_parent_chain(self) -> Optional[str]:
        """Why an ancestor of dst_root lets somebody else replace it, or None.

        Walked through descriptors from "/", the same way the root is then
        opened, so the chain that is judged is the chain that is used.
        """
        parent_b = os.path.dirname(self.dst_root_b) or b"/"
        fd = os.open(b"/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            walked = b"/"
            parts = [p for p in parent_b.split(b"/") if p]
            for index in range(len(parts) + 1):
                st = os.fstat(fd)
                if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH) and not (
                    st.st_mode & stat.S_ISVTX
                ):
                    return f"{path_display(walked)} is writable by group or other"
                if int(st.st_uid) not in (self.euid, 0):
                    return (
                        f"{path_display(walked)} is owned by uid {st.st_uid}, "
                        f"who can replace what is inside it"
                    )
                if index == len(parts):
                    return None
                next_fd = os.open(
                    parts[index], os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
                    dir_fd=fd,
                )
                os.close(fd)
                fd = next_fd
                walked = os.path.join(walked, parts[index])
        finally:
            os.close(fd)
        return None

    def start(self, src_st=None) -> None:
        """Open dst_root, roll back an interrupted run, lock the root.

        A start that fails removes again what it created itself - dst_root
        and any missing ancestor - as long as each is still the directory it
        created and still empty. The lock can only be taken on a directory
        that exists, so creating first is unavoidable; leaving an empty,
        locked dst_root behind after a refused start is not.
        """
        # (holder fd, name, identity) of every directory this start created,
        # outermost first. The holder is the parent it was created in.
        created_chain = []
        try:
            self._open_root(created_chain)
            self._start_locked(created_chain)
        except BaseException:
            self._undo_created(created_chain)
            raise
        finally:
            for holder_fd, _name, _identity in created_chain:
                try:
                    os.close(holder_fd)
                except OSError:
                    pass

    def _open_root(self, created_chain: list) -> None:
        parent_b = os.path.dirname(self.dst_root_b) or b"/"
        leaf_b = os.path.basename(self.dst_root_b)
        # The root may be reached through a symlink: that is how filesystems
        # are laid out. Missing ancestors are created as before (0755).
        fd = os.open(b"/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            components = [p for p in parent_b.split(b"/") if p]
            if leaf_b:
                components.append(leaf_b)
            for index, part in enumerate(components):
                is_root = bool(leaf_b) and index == len(components) - 1
                created = False
                try:
                    os.mkdir(
                        part,
                        DESTINATION_LOCK_PERMISSIONS if is_root else 0o755,
                        dir_fd=fd,
                    )
                    created = True
                except FileExistsError:
                    pass
                next_fd = os.open(
                    part, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC, dir_fd=fd
                )
                if created:
                    st = os.fstat(next_fd)
                    created_chain.append(
                        (os.dup(fd), part, (int(st.st_dev), int(st.st_ino)))
                    )
                os.close(fd)
                fd = next_fd
            self.root_fd = fd
            fd = -1
        finally:
            if fd >= 0:
                os.close(fd)
        self.root_created = bool(created_chain) and bool(leaf_b) and (
            created_chain[-1][1] == leaf_b
        )

    def _undo_created(self, created_chain: list) -> None:
        """Remove, innermost first, what this start created and left empty.

        Never while another run holds the lock: two starts may race for a
        missing dst_root, and the one that created it can lose the lock to
        the other - which is then working in it. The removal happens while
        this start holds the lock itself, so nobody can take it in between.
        Where flock() is not available at all, no other run can hold it.
        """
        try:
            if created_chain and self.root_fd >= 0 and not self.lock_held:
                try:
                    fcntl.flock(self.root_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.lock_held = True
                except BlockingIOError:
                    log(
                        f"another run holds {path_display(self.dst_root_b)}; "
                        f"nothing created by this failed start is removed"
                    )
                    return
                except OSError:
                    pass
            self._remove_created(created_chain)
        finally:
            if self.root_fd >= 0:
                try:
                    os.close(self.root_fd)
                except OSError:
                    pass
                self.root_fd = -1
                self.lock_held = False

    def _remove_created(self, created_chain: list) -> None:
        for holder_fd, name_b, identity in reversed(created_chain):
            try:
                st = os.stat(name_b, dir_fd=holder_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            except OSError as exc:
                log(f"WARNING: cannot inspect {path_display(name_b)} created by this start: {exc}")
                return
            if (int(st.st_dev), int(st.st_ino)) != identity or not stat.S_ISDIR(st.st_mode):
                log(
                    f"WARNING: {path_display(name_b)}, created by this failed "
                    f"start, was replaced in the meantime; it is left in place"
                )
                return
            try:
                # rmdir removes nothing but an empty directory.
                os.rmdir(name_b, dir_fd=holder_fd)
                log(f"removed {path_display(name_b)}, created by this failed start")
            except OSError as exc:
                log(
                    f"WARNING: {path_display(name_b)}, created by this failed "
                    f"start, is left in place: {exc}"
                )
                return

    def _start_locked(self, created_chain: list) -> None:
        created = self.root_created
        root_st = os.fstat(self.root_fd)
        self.root_identity = (int(root_st.st_dev), int(root_st.st_ino))

        # One run per destination. The lock sits on dst_root itself, not on a
        # file in working_directory: two runs with different working
        # directories must exclude each other just the same. It is held on the
        # descriptor until close() - after the restore - and the kernel drops
        # it when the process dies, which is what makes a journal found while
        # holding it a genuine leftover of a dead run.
        self.acquire_exclusive_lock()
        self.journal.root_identity = self.root_identity

        self.chain_problem = self._check_parent_chain()
        if self.chain_problem is not None:
            log(
                f"WARNING: the directory chain above dst_root is not "
                f"trustworthy: {self.chain_problem}. Whoever controls it can "
                f"move dst_root away and put another directory in its place; "
                f"this run holds dst_root open and keeps writing into the "
                f"original, but the tree is reported as not protected"
            )

        recover_interrupted_run(self)

        if created:
            # Created by this run: already locked. What mode it should have
            # in the end is fixed now, when it is known that it is new.
            record_created_root(self.cfg)
        locked = self.lock_directory(self.root_fd, b".")
        if not locked:
            self.root_problem = "dst_root could not be locked"
        root_st = os.fstat(self.root_fd)
        log(
            f"destination protection: dst_root uid={root_st.st_uid} "
            f"mode={stat.S_IMODE(root_st.st_mode):04o} "
            f"{'locked' if locked else 'NOT locked'}; status {self.status()}"
        )

    def acquire_exclusive_lock(self) -> None:
        try:
            fcntl.flock(self.root_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_held = True
        except BlockingIOError:
            raise RuntimeError(
                f"another run is working on dst_root "
                f"{path_display(self.dst_root_b)}; two runs on one destination "
                f"would undo each other's lock and restore"
            ) from None
        except OSError as exc:
            # Lustre mounted without -o flock (or localflock) answers flock()
            # with an error. Running on without the lock would leave two runs
            # free to roll back each other's journal; that is refused.
            raise RuntimeError(
                f"cannot take the exclusive run lock on dst_root "
                f"{path_display(self.dst_root_b)}: {exc}. On Lustre, mount the "
                f"client with -o flock (cluster-wide) or -o localflock (this "
                f"node only)"
            ) from exc

    def open_root(self) -> int:
        """A private duplicate of the held dst_root descriptor."""
        return os.dup(self.root_fd)

    def close(self) -> None:
        if self.root_fd >= 0:
            try:
                os.close(self.root_fd)
            except OSError:
                pass
            self.root_fd = -1

    # -- locking -------------------------------------------------------

    def lock_directory(self, fd: int, rel_b: bytes) -> bool:
        """Lock one destination directory; see lock_directories()."""
        return self.lock_directories([(fd, rel_b)])[0]

    def lock_directories(self, items) -> List[bool]:
        """Lock destination directories; record and journal what changes.

        items is a list of (fd, rel_b). Returns, per item, whether the
        directory is locked afterwards. Nothing is recorded for a directory
        that is already locked: it is either one this run locked earlier -
        whose original values are already recorded - or one that was like
        this before, which needs nothing restored.

        All original values of one call go into the journal together and are
        made durable with ONE fsync before the first directory is changed.
        A batch prelocks its directories this way, so a run over an existing
        tree pays one journal flush per batch rather than one per directory.
        """
        results = [True] * len(items)
        pending = []
        records = []
        for index, (fd, rel_b) in enumerate(items):
            st = os.fstat(fd)
            if self.is_locked_state(st):
                continue
            identity = (int(st.st_dev), int(st.st_ino))
            with self.lock:
                if identity in self.unlocked_identities:
                    results[index] = False
                    continue

            original = {}
            if int(st.st_uid) != self.euid:
                original["uid"] = int(st.st_uid)
            wanted_mode = (st.st_mode & 0o7000) | DESTINATION_LOCK_PERMISSIONS
            if stat.S_IMODE(st.st_mode) != wanted_mode:
                original["mode"] = stat.S_IMODE(st.st_mode)
                # chmod rewrites the mask of an access ACL, so the ACL is an
                # original value as well.
                acl = _read_xattr_or_none(
                    lambda name: os.getxattr(fd, name), ACL_ACCESS_XATTR
                )
                if acl is not None:
                    original["acl_access"] = acl
            # Which directory these values belong to. A rollback applies them
            # only to that same directory, never to whatever holds the path
            # by then.
            original["identity"] = identity
            records.append(
                encode_meta_record(rel_b, META_KIND_DIR, META_ORIGIN_ORIG, original)
            )
            pending.append((index, fd, rel_b, identity, original, wanted_mode))

        if not pending:
            return results

        try:
            self.journal.append_durable(b"".join(records))
        except OSError as exc:
            if self.journal_problem is None:
                self.journal_problem = str(exc)
                log(
                    f"WARNING: the destination journal cannot be written "
                    f"({exc}). Directories are locked anyway; if this process "
                    f"dies, their original owner and mode cannot be rolled "
                    f"back automatically"
                )
        log_object = destination_metadata_log
        if log_object is not None:
            try:
                for record in records:
                    log_object.record_raw(record)
            except Exception as exc:
                note_metadata_record_failure(exc)

        for index, fd, rel_b, identity, original, wanted_mode in pending:
            try:
                if "uid" in original:
                    os.fchown(fd, self.euid, -1)
                if "mode" in original:
                    os.fchmod(fd, wanted_mode)
            except OSError as exc:
                results[index] = False
                with self.lock:
                    if identity in self.unlocked_identities:
                        continue
                    self.unlocked_identities.add(identity)
                self.note_unlocked(rel_b, f"cannot lock it: {exc}")
        return results

    def prelock(self, rel_dirs) -> None:
        """Lock the existing destination directories a batch will write into.

        Only what already exists is touched - components are opened with
        O_NOFOLLOW from the held dst_root, and the walk of a chain stops at
        the first missing one; new directories are created locked anyway.
        The walk that later hands out the parent descriptors finds these
        locked and has nothing left to journal.
        """
        opened = {}
        items = []
        try:
            for rel_b in sorted(set(rel_dirs), key=lambda r: (r.count(b"/"), r)):
                if not rel_b or rel_b == b".":
                    continue
                parts = rel_b.split(b"/")
                parent_fd = self.root_fd
                walked = b""
                for part in parts:
                    walked = os.path.join(walked, part) if walked else part
                    fd = opened.get(walked)
                    if fd is None:
                        if walked in opened:
                            break
                        try:
                            fd = os.open(
                                part,
                                os.O_RDONLY | os.O_DIRECTORY
                                | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                                dir_fd=parent_fd,
                            )
                        except OSError:
                            # Missing, not a directory or a symlink: the
                            # ordinary walk creates it or reports it.
                            opened[walked] = None
                            break
                        opened[walked] = fd
                        items.append((fd, walked))
                    parent_fd = fd
            if items:
                self.lock_directories(items)
        finally:
            for fd in opened.values():
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass


destination_guard: Optional[DestinationGuard] = None
# A guard whose run lock must outlive the run: kept referenced, never closed.
_abandoned_guard: Optional[DestinationGuard] = None
# How long the shutdown waits for each worker thread before it gives up on
# restoring the destination and leaves the tree locked instead.
WORKER_JOIN_TIMEOUT_SEC = 30.0
destination_metadata_log: Optional[MetadataLog] = None
destination_file_metadata_log: Optional[MetadataLog] = None
# Set whenever a record could not be written or the restore did not complete.
# Every such case ends the run with exit status 1.
metadata_restore_incomplete = threading.Event()


def note_metadata_record_failure(exc: Exception) -> None:
    # Losing a record silently would produce a destination with wrong
    # metadata that still reports success.
    if not metadata_restore_incomplete.is_set():
        log(f"WARNING: cannot record destination metadata: {exc}")
    metadata_restore_incomplete.set()


def _source_xattr_fields(listxattr, getxattr, is_directory: bool) -> dict:
    """The xattr and ACL fields a destination object must end up with."""
    names = set(listxattr())
    fields = {
        "acl_access": (
            getxattr(ACL_ACCESS_XATTR) if ACL_ACCESS_XATTR in names else None
        ),
        "xattrs": {
            os.fsencode(name): getxattr(name)
            for name in names
            if name not in _ACL_XATTR_NAMES
        },
    }
    if is_directory:
        fields["acl_default"] = (
            getxattr(ACL_DEFAULT_XATTR) if ACL_DEFAULT_XATTR in names else None
        )
    return fields


def directory_final_fields(src_st, src_fd: Optional[int], cfg: dict, created: bool) -> dict:
    fields = {}
    if permissions_owner(cfg):
        fields["uid"] = int(src_st.st_uid)
    if permissions_group(cfg):
        fields["gid"] = int(src_st.st_gid)
    if permissions_mode(cfg):
        fields["mode"] = stat.S_IMODE(src_st.st_mode)
    elif created:
        # No mode class: a directory this run created gets what mkdir would
        # have given it, fixed now, while it is known to be new.
        fields["mode"] = 0o777 & ~get_process_umask()
    if cfg.get("xattr", False) and src_fd is not None:
        try:
            fields.update(_source_xattr_fields(
                lambda: os.listxattr(src_fd),
                lambda name: os.getxattr(src_fd, name),
                True,
            ))
        except (OSError, AttributeError) as exc:
            raise OSError(
                getattr(exc, "errno", errno.ENOTSUP) or errno.ENOTSUP,
                f"cannot read source directory xattrs: {exc}",
            ) from exc
    if mtime_enabled(cfg):
        fields["times"] = (int(src_st.st_atime_ns), int(src_st.st_mtime_ns))
    return fields


def record_directory_final(
    rel_b: bytes,
    src_st,
    src_fd: Optional[int],
    cfg: dict,
    created: bool,
    locked: bool,
) -> None:
    """Note what one destination directory must look like at the end."""
    log_object = destination_metadata_log
    if log_object is None:
        return
    fields = directory_final_fields(src_st, src_fd, cfg, created)
    if locked:
        fields["locked"] = True
    try:
        log_object.record_entry(rel_b or b".", META_KIND_DIR, META_ORIGIN_FINAL, fields)
    except Exception as exc:
        note_metadata_record_failure(exc)


def record_created_root(cfg: dict) -> None:
    log_object = destination_metadata_log
    if log_object is None or permissions_mode(cfg):
        return
    try:
        log_object.record_entry(
            b".", META_KIND_DIR, META_ORIGIN_FINAL,
            {"mode": 0o777 & ~get_process_umask(), "locked": True},
        )
    except Exception as exc:
        note_metadata_record_failure(exc)


def record_root_final(cfg: dict) -> None:
    """dst_root takes the configured metadata of src_root, like any directory."""
    if destination_metadata_log is None:
        return
    src_fd = open_dir_fd_componentwise(os.fsencode(cfg["src_root"]), create=False)
    try:
        record_directory_final(
            b".", os.fstat(src_fd), src_fd, cfg, created=False,
            locked=destination_guard is not None
            and destination_guard.root_problem is None,
        )
    finally:
        os.close(src_fd)


def record_file_final(
    rel_b: bytes, src_st, src_parent_fd: int, leaf_b: bytes, cfg: dict
) -> None:
    """rsync only: the metadata a file or symlink gets in the restore phase."""
    log_object = destination_file_metadata_log
    if log_object is None:
        return
    if stat.S_ISLNK(src_st.st_mode):
        kind = META_KIND_SYMLINK
    elif stat.S_ISREG(src_st.st_mode):
        kind = META_KIND_FILE
    else:
        return
    fields = {}
    if permissions_owner(cfg):
        fields["uid"] = int(src_st.st_uid)
    if permissions_group(cfg):
        fields["gid"] = int(src_st.st_gid)
    if permissions_mode(cfg) and kind == META_KIND_FILE:
        fields["mode"] = stat.S_IMODE(src_st.st_mode)
    if cfg.get("xattr", False):
        path_b = os.fsencode(f"/proc/self/fd/{src_parent_fd}/") + leaf_b
        try:
            xattr_fields = _source_xattr_fields(
                lambda: os.listxattr(path_b, follow_symlinks=False),
                lambda name: os.getxattr(path_b, name, follow_symlinks=False),
                False,
            )
        except (OSError, AttributeError) as exc:
            raise OSError(
                getattr(exc, "errno", errno.ENOTSUP) or errno.ENOTSUP,
                f"cannot read source xattrs: {exc}",
            ) from exc
        if kind == META_KIND_SYMLINK:
            # Symlinks carry no ACL; user.* is not allowed on them at all.
            xattr_fields.pop("acl_access", None)
        fields.update(xattr_fields)
    if not fields:
        return
    try:
        log_object.record_entry(rel_b, kind, META_ORIGIN_FINAL, fields)
    except Exception as exc:
        note_metadata_record_failure(exc)


# -- restore -------------------------------------------------------------


class _FdTarget:
    """Metadata operations on an open descriptor."""

    is_symlink = False

    def __init__(self, fd: int) -> None:
        self.fd = fd

    def stat(self):
        return os.fstat(self.fd)

    def chown(self, uid: int, gid: int) -> None:
        os.fchown(self.fd, uid, gid)

    def chmod(self, mode: int) -> None:
        os.fchmod(self.fd, mode)

    def listxattr(self):
        return os.listxattr(self.fd)

    def getxattr(self, name: str) -> bytes:
        return os.getxattr(self.fd, name)

    def setxattr(self, name: str, value: bytes) -> None:
        os.setxattr(self.fd, name, value)

    def removexattr(self, name: str) -> None:
        os.removexattr(self.fd, name)

    def utime(self, times) -> None:
        os.utime(self.fd, ns=tuple(times))


class _AtTarget:
    """Metadata operations on a name inside a held parent directory.

    Used for symlinks, which cannot be opened, and for files an unprivileged
    run cannot open. The parent is a locked directory of this run, so the
    name cannot be swapped by anybody else between the checks and the call.
    """

    def __init__(self, parent_fd: int, leaf_b: bytes, is_symlink: bool) -> None:
        self.parent_fd = parent_fd
        self.leaf_b = leaf_b
        self.is_symlink = is_symlink
        self.proc_path = os.fsencode(f"/proc/self/fd/{parent_fd}/") + leaf_b

    def stat(self):
        return os.stat(self.leaf_b, dir_fd=self.parent_fd, follow_symlinks=False)

    def chown(self, uid: int, gid: int) -> None:
        os.chown(self.leaf_b, uid, gid, dir_fd=self.parent_fd, follow_symlinks=False)

    def chmod(self, mode: int) -> None:
        if self.is_symlink:
            return
        os.chmod(self.leaf_b, mode, dir_fd=self.parent_fd)

    def listxattr(self):
        return os.listxattr(self.proc_path, follow_symlinks=False)

    def getxattr(self, name: str) -> bytes:
        return os.getxattr(self.proc_path, name, follow_symlinks=False)

    def setxattr(self, name: str, value: bytes) -> None:
        os.setxattr(self.proc_path, name, value, follow_symlinks=False)

    def removexattr(self, name: str) -> None:
        os.removexattr(self.proc_path, name, follow_symlinks=False)

    def utime(self, times) -> None:
        os.utime(self.leaf_b, ns=tuple(times), dir_fd=self.parent_fd,
                 follow_symlinks=False)


def _apply_meta_fields(target, fields: dict, rel_b: bytes, counters: dict) -> None:
    """Apply one merged entry in the fixed order, then verify it.

    chown first: it clears setuid/setgid and security.capability of files.
    Then the plain xattrs, while the mode still allows writing them. Then the
    ACLs, which rewrite the group bits. Then chmod, which puts the special
    bits back and fixes the ACL mask to the final group bits. Then everything
    is checked, and the timestamps come last: nothing after them may touch
    the object again.
    """
    guard = destination_guard
    st = target.stat()
    if fields.get("locked") and guard is not None and not guard.is_locked_state(st):
        guard.note_violation(rel_b, st)
        counters["violations"] += 1

    ownership_applied = True
    uid = fields.get("uid")
    gid = fields.get("gid")
    want_uid = uid if uid is not None and uid != st.st_uid else -1
    want_gid = gid if gid is not None and gid != st.st_gid else -1
    if want_uid != -1 or want_gid != -1:
        try:
            target.chown(want_uid, want_gid)
        except PermissionError:
            # chown needs privileges; tolerated as everywhere else, counted.
            ownership_applied = False
            counters["ownership_not_preserved"] += 1

    if "xattrs" in fields:
        wanted = {os.fsdecode(name): value for name, value in fields["xattrs"].items()}
        current = {name for name in target.listxattr() if name not in _ACL_XATTR_NAMES}
        for name in sorted(current - set(wanted)):
            if not os.fsencode(name).startswith(b"user."):
                raise OSError(
                    errno.EPERM,
                    f"extra destination xattr needs manual handling: {name!r}",
                )
            target.removexattr(name)
        for name in sorted(wanted):
            if _read_xattr_or_none(target.getxattr, name) != wanted[name]:
                target.setxattr(name, wanted[name])

    for key, name in _ACL_FIELDS:
        if key not in fields:
            continue
        value = fields[key]
        current = _read_xattr_or_none(target.getxattr, name)
        if value is None:
            if current is not None:
                target.removexattr(name)
        elif current != value:
            target.setxattr(name, value)

    if "mode" in fields and not target.is_symlink:
        if stat.S_IMODE(target.stat().st_mode) != fields["mode"]:
            target.chmod(fields["mode"])

    st = target.stat()
    problems = []
    if "mode" in fields and not target.is_symlink and stat.S_IMODE(st.st_mode) != fields["mode"]:
        problems.append(
            f"mode is {stat.S_IMODE(st.st_mode):04o}, expected {fields['mode']:04o}"
        )
    if ownership_applied:
        if uid is not None and st.st_uid != uid:
            problems.append(f"uid is {st.st_uid}, expected {uid}")
        if gid is not None and st.st_gid != gid:
            problems.append(f"gid is {st.st_gid}, expected {gid}")
    for key, name in _ACL_FIELDS:
        if key in fields and not fields.get("_acl_unverified"):
            if _read_xattr_or_none(target.getxattr, name) != fields[key]:
                problems.append(f"{name} differs after restore")
    if "xattrs" in fields:
        for name, value in fields["xattrs"].items():
            if _read_xattr_or_none(target.getxattr, os.fsdecode(name)) != value:
                problems.append(f"xattr {os.fsdecode(name)!r} differs after restore")
    if problems:
        raise OSError(errno.EIO, "; ".join(problems))

    if "times" in fields:
        target.utime(fields["times"])


class _MetadataApplier:
    """Applies entries below the held dst_root; one instance per thread."""

    def __init__(self, max_parent_entries: int = 64, rollback: bool = False) -> None:
        # rollback: the entries come from a dead run's journal. A directory
        # that is gone since has nothing left to roll back, and one that is
        # not the journalled directory or no longer locked is left alone.
        self.rollback = rollback
        self.max_parent_entries = max(1, int(max_parent_entries))
        self.parents = OrderedDict()
        self.counters = {
            "records": 0,
            "applied": 0,
            "failed": 0,
            "violations": 0,
            "ownership_not_preserved": 0,
            "left_locked": 0,
            "missing": 0,
            "skipped": 0,
        }
        self.errors = []

    def _parent_fd(self, rel_parent_b: bytes) -> int:
        guard = destination_guard
        if not rel_parent_b:
            return guard.root_fd
        descriptor = self.parents.get(rel_parent_b)
        if descriptor is not None:
            self.parents.move_to_end(rel_parent_b)
            return descriptor
        # Below dst_root nothing is followed: every component is opened with
        # O_NOFOLLOW, starting from the descriptor held since the start.
        descriptor = guard.open_root()
        try:
            for part in rel_parent_b.split(b"/"):
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
                    | os.O_CLOEXEC,
                    dir_fd=descriptor,
                )
                os.close(descriptor)
                descriptor = next_fd
        except BaseException:
            os.close(descriptor)
            raise
        self.parents[rel_parent_b] = descriptor
        while len(self.parents) > self.max_parent_entries:
            _, evicted = self.parents.popitem(last=False)
            try:
                os.close(evicted)
            except OSError:
                pass
        return descriptor

    def apply(self, rel_b: bytes, kind: int, fields: dict) -> None:
        self.counters["records"] += 1
        fd = None
        try:
            validate_relative_path(rel_b, allow_root=True)
            if rel_b == b".":
                fd = destination_guard.open_root()
                target = _FdTarget(fd)
            else:
                parent_fd = self._parent_fd(os.path.dirname(rel_b))
                leaf_b = os.path.basename(rel_b)
                if kind == META_KIND_DIR:
                    fd = os.open(
                        leaf_b,
                        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
                        | os.O_CLOEXEC,
                        dir_fd=parent_fd,
                    )
                    target = _FdTarget(fd)
                elif kind == META_KIND_FILE:
                    try:
                        fd = os.open(
                            leaf_b,
                            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                            | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC,
                            dir_fd=parent_fd,
                        )
                        target = _FdTarget(fd)
                    except PermissionError:
                        target = _AtTarget(parent_fd, leaf_b, False)
                    if not stat.S_ISREG(target.stat().st_mode):
                        raise OSError(errno.EINVAL, "destination is no longer a regular file")
                elif kind == META_KIND_SYMLINK:
                    target = _AtTarget(parent_fd, leaf_b, True)
                    if not stat.S_ISLNK(target.stat().st_mode):
                        raise OSError(errno.EINVAL, "destination is no longer a symlink")
                else:
                    raise ValueError(f"unknown metadata record kind {kind}")
            if self.rollback and "identity" not in fields:
                self.counters["skipped"] += 1
                log(
                    f"WARNING: rollback skips {path_display(rel_b)}: the "
                    f"journal record names no directory identity"
                )
                return
            if "identity" in fields:
                fields = self._check_identity(target, rel_b, fields)
                if fields is None:
                    return
            _apply_meta_fields(target, fields, rel_b, self.counters)
            self.counters["applied"] += 1
        except FileNotFoundError as exc:
            if kind == META_KIND_DIR and self.rollback:
                self.counters["missing"] += 1
            elif kind == META_KIND_DIR:
                self._fail(rel_b, exc)
                self._report_left_locked(rel_b)
            else:
                # The transfer of this file did not happen - it failed, or its
                # batch was stopped - and was reported there already.
                self.counters["missing"] += 1
        except Exception as exc:
            self._fail(rel_b, exc)
            if kind == META_KIND_DIR:
                self._report_left_locked(rel_b)
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def _check_identity(self, target, rel_b: bytes, fields: dict) -> Optional[dict]:
        """Original values only go back onto the directory they came from.

        Returns the fields to apply, or None to leave the object alone.
        """
        st = target.stat()
        same = (int(st.st_dev), int(st.st_ino)) == tuple(fields["identity"])
        if self.rollback:
            reason = None
            if not same:
                reason = "it is not the directory the journal was written for"
            elif not destination_guard.is_locked_state(st):
                reason = (
                    f"it is no longer locked (uid={st.st_uid} "
                    f"mode={stat.S_IMODE(st.st_mode):04o}); somebody changed it"
                )
            if reason is not None:
                self.counters["skipped"] += 1
                log(
                    f"WARNING: rollback skips {path_display(rel_b)}: {reason}. "
                    f"path_b64={base64.b64encode(rel_b).decode('ascii')}"
                )
                return None
            return fields
        if same:
            return fields
        # Replaced during the run: the source values still apply to what is
        # there now, the original values of another directory do not.
        self.counters["skipped"] += 1
        log(
            f"WARNING: {path_display(rel_b)} is not the directory this run "
            f"locked; its original values are not applied to it"
        )
        return {
            key: value for key, value in fields.items()
            if key not in fields.get("_orig_keys", ()) and key != "identity"
        }

    def _fail(self, rel_b: bytes, exc: Exception) -> None:
        self.counters["failed"] += 1
        if len(self.errors) < 10:
            self.errors.append(f"{path_display(rel_b)}: {exc}")

    def _report_left_locked(self, rel_b: bytes) -> None:
        guard = destination_guard
        try:
            if rel_b == b".":
                st = os.fstat(guard.root_fd)
            else:
                st = os.stat(
                    os.path.basename(rel_b),
                    dir_fd=self._parent_fd(os.path.dirname(rel_b)),
                    follow_symlinks=False,
                )
        except Exception:
            return
        if guard.is_locked_state(st):
            self.counters["left_locked"] += 1
            # base64 as well, so the name survives any encoding.
            log(
                f"WARNING: destination directory left locked "
                f"(mode {stat.S_IMODE(st.st_mode):04o}): {path_display(rel_b)} "
                f"path_b64={base64.b64encode(rel_b).decode('ascii')}"
            )

    def close(self) -> None:
        while self.parents:
            _, descriptor = self.parents.popitem()
            try:
                os.close(descriptor)
            except OSError:
                pass


def _meta_depth(rel_b: bytes) -> int:
    return 0 if rel_b == b"." else rel_b.count(b"/") + 1


class _SortRun:
    """One sorted run of the external sort, compressed.

    Kept in an unlinked file in the working directory when there is one, in
    memory otherwise. Either way it holds deflate output, not Python objects.
    """

    def __init__(self) -> None:
        self.fd = -1
        self.memory = None
        wd = working_directory_fd()
        if wd >= 0:
            name = run_state_name("mtsort", f".{secrets.token_hex(4)}.tmp")
            try:
                self.fd = os.open(
                    name,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                    0o600,
                    dir_fd=wd,
                )
                os.unlink(name, dir_fd=wd)
            except OSError:
                self.fd = -1
        if self.fd < 0:
            self.memory = io.BytesIO()
        self.compressor = zlib.compressobj(1)

    def write(self, items) -> None:
        sink = self.memory
        for negative_depth, path_b, sequence, body in items:
            payload = bytearray()
            _put_varint(payload, -negative_depth)
            _put_varint(payload, sequence)
            payload += body
            framed = bytearray()
            _put_varint(framed, len(payload))
            framed += payload
            data = self.compressor.compress(bytes(framed))
            if data:
                self._emit(data, sink)
        self._emit(self.compressor.flush(), sink)

    def _emit(self, data: bytes, sink) -> None:
        if sink is not None:
            sink.write(data)
        else:
            _write_all(self.fd, data)

    def _chunks(self):
        decompressor = zlib.decompressobj()
        if self.memory is not None:
            yield decompressor.decompress(self.memory.getvalue())
        else:
            os.lseek(self.fd, 0, os.SEEK_SET)
            while True:
                data = os.read(self.fd, 1024 * 1024)
                if not data:
                    break
                yield decompressor.decompress(data)
        yield decompressor.flush()

    def __iter__(self):
        for payload in iter_meta_bodies(self._chunks(), "<sort run>"):
            depth, pos = _get_varint(payload, 0)
            sequence, pos = _get_varint(payload, pos)
            body = payload[pos:]
            yield (-depth, meta_body_path(body), sequence, body)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1
        self.memory = None


def _sorted_directory_bodies(log_object: MetadataLog, chunk_limit: int):
    """All directory records, deepest first, grouped by path, in write order.

    An external sort: chunks of at most chunk_limit decoded bytes are sorted
    in memory, and when there is more than one they are merged from
    compressed runs. Memory therefore stays bounded however many directories
    the tree has.
    """
    chunk = []
    size = 0
    runs = []
    sequence = 0
    try:
        for body in log_object.bodies():
            path_b = meta_body_path(body)
            chunk.append((-_meta_depth(path_b), path_b, sequence, body))
            sequence += 1
            size += len(body) + 160
            if size >= chunk_limit:
                chunk.sort(key=lambda item: item[:3])
                run = _SortRun()
                runs.append(run)
                run.write(chunk)
                chunk = []
                size = 0
        chunk.sort(key=lambda item: item[:3])
        if not runs:
            for item in chunk:
                yield item
            return
        run = _SortRun()
        runs.append(run)
        run.write(chunk)
        chunk = []
        log(
            f"destination metadata: {sequence} directory records sorted in "
            f"{len(runs)} runs"
        )
        for item in heapq.merge(*runs, key=lambda item: item[:3]):
            yield item
    finally:
        for run in runs:
            run.close()


def _coalesced_directory_entries(sorted_items):
    """Merge every record of one path into the entry that is applied.

    ORIG values: the first one wins. FINAL values: the last one wins, and a
    FINAL value always overrides an ORIG value of the same field.
    """
    current_path = None
    current_depth = 0
    original = {}
    final = {}

    def merged():
        entry = dict(original)
        entry.update(final)
        orig_only = set(original) - set(final) - {"identity"}
        if orig_only:
            entry["_orig_keys"] = frozenset(orig_only)
        if "acl_access" in original and "acl_access" not in final and "mode" in final:
            # The original ACL entries with the final group bits: the mask
            # follows the mode, so the ACL cannot compare equal afterwards.
            entry["_acl_unverified"] = True
        return entry

    for negative_depth, path_b, _sequence, body in sorted_items:
        if path_b != current_path:
            if current_path is not None:
                yield current_depth, current_path, merged()
            current_path = path_b
            current_depth = -negative_depth
            original = {}
            final = {}
        _, _kind, origin, fields = decode_meta_body(body)
        if origin == META_ORIGIN_ORIG:
            for key, value in fields.items():
                original.setdefault(key, value)
        else:
            final.update(fields)
    if current_path is not None:
        yield current_depth, current_path, merged()


def _apply_entries(entries, thread_count: int, level_barrier: bool,
                   rollback: bool = False):
    """Apply (depth, path, kind, fields) entries with bounded parallelism.

    Entries are sharded by their PARENT directory: siblings land in one
    thread, which keeps that thread's parent-fd cache warm, and records of one
    path keep their order. With level_barrier every entry of one depth is
    applied before the next, shallower level starts - a directory is only
    released once everything below it is done.
    """
    if thread_count <= 1:
        applier = _MetadataApplier(rollback=rollback)
        try:
            for _depth, rel_b, kind, fields in entries:
                applier.apply(rel_b, kind, fields)
        finally:
            applier.close()
        return [applier]

    per_thread_cache = max(8, 256 // thread_count)
    appliers = [
        _MetadataApplier(per_thread_cache, rollback=rollback)
        for _ in range(thread_count)
    ]
    queues = [
        queue.Queue(maxsize=DIRECTORY_REPAIR_QUEUE_DEPTH)
        for _ in range(thread_count)
    ]

    def worker(index: int) -> None:
        applier = appliers[index]
        work = queues[index]
        try:
            while True:
                item = work.get()
                try:
                    if item is None:
                        return
                    applier.apply(*item)
                finally:
                    work.task_done()
        finally:
            applier.close()

    threads = [
        threading.Thread(target=worker, args=(index,), name=f"metarestore-{index}",
                         daemon=True)
        for index in range(thread_count)
    ]
    for thread in threads:
        thread.start()
    current_depth = None
    try:
        for depth, rel_b, kind, fields in entries:
            if level_barrier and depth != current_depth:
                for work in queues:
                    work.join()
                current_depth = depth
            parent_b = b"" if rel_b == b"." else os.path.dirname(rel_b)
            queues[zlib.crc32(parent_b) % thread_count].put((rel_b, kind, fields))
    finally:
        for work in queues:
            work.put(None)
        for thread in threads:
            thread.join()
    return appliers


def _collect(appliers, label: str) -> dict:
    result = {
        "records": 0, "applied": 0, "failed": 0, "violations": 0,
        "ownership_not_preserved": 0, "left_locked": 0, "missing": 0,
        "skipped": 0,
    }
    reported = 0
    for applier in appliers:
        for key in result:
            result[key] += applier.counters[key]
        for message in applier.errors:
            if reported >= 10:
                break
            log(f"WARNING: cannot restore {label} metadata for {message}")
            reported += 1
    return result


def _restore_thread_count(cfg: dict, record_count: int) -> int:
    if record_count < DIRECTORY_REPAIR_MIN_PARALLEL_RECORDS:
        return 1
    return max(1, int(cfg.get("max_workers", 1)))


def restore_files(log_object: Optional[MetadataLog], cfg: dict) -> dict:
    """rsync: owner, mode, ACL and xattrs of files and symlinks, streamed."""
    if log_object is None or not log_object.record_count:
        return _collect([], "file")

    def entries():
        for body in log_object.bodies():
            rel_b, kind, _origin, fields = decode_meta_body(body)
            yield 0, rel_b, kind, fields

    appliers = _apply_entries(
        entries(), _restore_thread_count(cfg, log_object.record_count),
        level_barrier=False,
    )
    return _collect(appliers, "file")


def restore_directories(
    log_object: Optional[MetadataLog], cfg: dict, rollback: bool = False
) -> dict:
    """Directories bottom-up, dst_root last."""
    if log_object is None or not log_object.record_count:
        return _collect([], "directory")
    # The decoded chunk may be as large as the compressed budget: decoded
    # records are several times larger, but they exist only during this pass.
    chunk_limit = max(4096, int(cfg.get("metadata_maxsize") or 0))
    entries = (
        (depth, rel_b, META_KIND_DIR, fields)
        for depth, rel_b, fields in _coalesced_directory_entries(
            _sorted_directory_bodies(log_object, chunk_limit)
        )
    )
    appliers = _apply_entries(
        entries, _restore_thread_count(cfg, log_object.record_count),
        level_barrier=True, rollback=rollback,
    )
    return _collect(appliers, "directory")


def recover_interrupted_run(guard: DestinationGuard) -> None:
    """Roll back the original values a run that died had journalled.

    Runs with the exclusive lock on dst_root held and before this run locks
    anything, so the journal found here belongs to a run that is gone.

    Nothing is rolled back onto another directory than the one the values
    were taken from. dst_root must be the same directory as in the journal
    header, or the start is refused. Each directory must be the one named by
    its record's identity AND must still be locked - owned by this user, no
    permissions for group and other - or it is skipped and reported: somebody
    changed or replaced it since, and its current state is theirs.

    Directories the dead run created stay locked; they are handled like every
    other directory of this run. A rollback that does not complete stops the
    start: locking on top of it would make the originals unrecoverable.
    """
    found = DestinationJournal.read_existing(guard.dst_root_b)
    if found is None:
        return
    header, bodies = found
    journal_label = os.path.join(
        working_directory_display(), DestinationJournal.name_for(guard.dst_root_b)
    )
    if header:
        recorded_root = tuple(header.get("dst_root_identity") or ())
        if recorded_root != tuple(guard.root_identity):
            raise RuntimeError(
                f"the journal {journal_label} of an interrupted run belongs to "
                f"dst_root identity {recorded_root!r}, but the directory at "
                f"{path_display(guard.dst_root_b)} is {guard.root_identity!r}: "
                f"it was moved or replaced. Nothing is rolled back and nothing "
                f"is started. Find the original tree, or inspect it by hand "
                f"and remove the journal"
            )
    log(
        f"destination journal of an interrupted run found: rolling back the "
        f"original directory values before locking"
    )
    cfg = get_config_snapshot()
    # Streamed into a log bounded by metadata_maxsize, spilled above it: the
    # journal of a very large tree is never loaded as a whole.
    rollback_log = MetadataLog(
        int(cfg.get("metadata_maxsize") or 100 * 1024 * 1024),
        cfg.get("spill_compression") or "zlib",
        cfg.get("dst_root"),
        label="dstrollback",
    )
    try:
        for body in bodies:
            framed = bytearray()
            _put_varint(framed, len(body))
            framed += body
            rollback_log.record_raw(bytes(framed))
        rollback_log.close()
        if rollback_log.spill_refused:
            raise RuntimeError(
                "the rollback records exceed metadata_maxsize and cannot be "
                "spilled; the journal is kept"
            )
        outcome = restore_directories(rollback_log, cfg, rollback=True)
    finally:
        rollback_log.discard()
    if outcome["failed"]:
        raise RuntimeError(
            f"rollback of the interrupted run failed for {outcome['failed']} "
            f"directories; the journal is kept. Fix the cause and start again"
        )
    DestinationJournal.discard_existing(guard.dst_root_b)
    log(
        f"destination journal rolled back: {outcome['applied']} directories; "
        f"{outcome['skipped']} skipped because they were changed or replaced "
        f"since, {outcome['missing']} gone"
    )


def finalize_destination_metadata(apply_restore: bool, run_stats: Optional[dict] = None) -> None:
    """Close the logs and, when nothing writes any more, restore everything.

    Also after a stop: leaving the tree locked would be worse than restoring
    directories that are not yet complete. A resumed run locks them again and
    records the restored values as their originals, which is exactly right.

    apply_restore=False is for a run that cannot be sure its writers are gone.
    The directories then stay locked and the journal is kept, so the next
    start rolls back what the lock changed.
    """
    global destination_guard, destination_metadata_log, destination_file_metadata_log

    guard = destination_guard
    if guard is None:
        return
    dir_log = destination_metadata_log
    file_log = destination_file_metadata_log
    # From here on nothing records any more.
    destination_metadata_log = None
    destination_file_metadata_log = None

    for log_object in (dir_log, file_log):
        if log_object is None:
            continue
        try:
            log_object.close()
        except Exception as exc:
            log(f"WARNING: cannot close destination metadata log: {exc}")
            metadata_restore_incomplete.set()

    if run_stats is not None:
        run_stats["destination_protection_summary"] = guard.summary()

    if not apply_restore:
        metadata_restore_incomplete.set()
        guard.journal.close_keep()
        # The dst_root descriptor is deliberately NOT closed: it carries the
        # run lock, and a worker that may still be writing must never write
        # without it. Another run would otherwise take the lock, roll back
        # this run's journal and restore under a live writer. The kernel drops
        # the lock when this process ends - which also ends its worker
        # threads - and once every child still holding a duplicate of the
        # descriptor (rsync, an external copy engine) has exited.
        log(
            "WARNING: destination metadata was NOT restored because workers "
            "may still be writing. Destination directories stay locked (0700). "
            "The run lock on dst_root stays held until this process and its "
            "writing child processes have exited; the next start with this "
            "dst_root then rolls back what the lock changed, and a re-run "
            "restores the rest"
        )
        destination_guard = None
        global _abandoned_guard
        _abandoned_guard = guard
        return

    cfg = get_config_snapshot()
    try:
        files = restore_files(file_log, cfg)
        directories = restore_directories(dir_log, cfg)
    except Exception as exc:
        log(f"WARNING: destination metadata restore failed: {exc}")
        metadata_restore_incomplete.set()
        guard.journal.close_keep()
        if run_stats is not None:
            run_stats["destination_protection_summary"] = guard.summary()
        destination_guard = None
        guard.close()
        return

    if files["failed"] or directories["failed"]:
        metadata_restore_incomplete.set()
    if run_stats is not None:
        run_stats["metadata_restore"] = {"files": files, "directories": directories}
        run_stats["ownership_not_preserved_count"] = int(
            run_stats.get("ownership_not_preserved_count", 0)
        ) + files["ownership_not_preserved"] + directories["ownership_not_preserved"]
        run_stats["destination_protection_summary"] = guard.summary()

    log(
        f"destination metadata restored: files applied={files['applied']} "
        f"failed={files['failed']}; directories applied={directories['applied']} "
        f"failed={directories['failed']} left_locked={directories['left_locked']}; "
        f"protection={guard.status()} "
        f"unlocked={guard.unlocked_count} violations={guard.violation_count}"
    )

    # The restore ran: what it could not do is reported above and in the
    # statistics. Rolling the originals back on the next start would undo
    # the values that WERE restored, so the journal goes either way.
    guard.journal.remove()
    for log_object in (dir_log, file_log):
        if log_object is not None:
            log_object.discard()
    destination_guard = None
    guard.close()


def copy_one_openat_with_verify(
    src_b: bytes,
    dst_b: bytes,
    cfg: dict,
    compare_mode: str,
    xattr_copy: bool = False,
    parent_fd_cache: Optional["ParentDirFdCache"] = None,
) -> Tuple[int, bytes, int]:
    """
    Copy one file/dir/symlink without passing full long paths to external tools.
    Uses openat-style dir_fd operations.

    ``parent_fd_cache`` optionally supplies already-open parent directory fds
    for the current batch. Without it the parent chain is resolved once per
    call and closed again before returning.

    Returns:
        (rc, msg, copied_bytes)

    copied_bytes:
        - source file size for successfully copied regular files
        - 0 for dirs, symlinks, unsupported files, failures
    """

    def _write_all(fd: int, data: bytes) -> None:
        """Write the complete buffer, handling short writes and EINTR."""
        view = memoryview(data)
        while view:
            try:
                written = os.write(fd, view)
            except InterruptedError:
                continue

            if written <= 0:
                raise OSError(errno.EIO, "write made no progress")

            view = view[written:]

    def _new_tmp_name() -> bytes:
        """Return a short, process/thread-specific and collision-resistant name."""
        return (
            f".parallel_tools.tmp.{os.getpid()}."
            f"{threading.get_ident()}.{secrets.token_hex(8)}"
        ).encode("ascii")

    try:
        src_root_b = os.fsencode(cfg["src_root"])
        dst_root_b = os.fsencode(cfg["dst_root"])
        metadata_cfg = cfg

        rel_b = make_relative_bytes(src_b, src_root_b)
        validate_relative_path(rel_b, allow_root=True)

        expected_dst_b = os.path.join(dst_root_b, rel_b)
        if os.path.normpath(dst_b) != os.path.normpath(expected_dst_b):
            return -1, b"destination does not match configured root mapping", 0

        if isinstance(rel_b, str):
            rel_b = os.fsencode(rel_b)

        if rel_b == b".":
            # src_root itself, which 'find SRC -print0' emits as its first
            # record. dst_root exists already - the run opened and locked it
            # at the start - so nothing is transferred; it takes the
            # configured metadata of src_root in the restore, like every
            # other directory.
            record_root_final(cfg)
            return 0, b"source root; metadata recorded", 0

        rel_parent_b = os.path.dirname(rel_b)
        leaf_b = os.path.basename(rel_b)

        if not leaf_b or leaf_b in (b".", b".."):
            return -1, b"invalid relative file name for openat copy", 0

        src_parent_fd = None
        dst_parent_fd = None
        owns_parent_fds = parent_fd_cache is None
        try:
            # Source and destination parent chains are resolved in one lockstep
            # walk; the source chain must be opened anyway to read the metadata
            # of each directory component.
            if parent_fd_cache is not None:
                src_parent_fd, dst_parent_fd = parent_fd_cache.get(rel_parent_b)
            else:
                src_parent_fd, dst_parent_fd = open_src_and_dst_dir_fds(
                    src_root_b,
                    dst_root_b,
                    rel_parent_b,
                    xattr_copy,
                    metadata_cfg,
                )
            st = os.lstat(leaf_b, dir_fd=src_parent_fd)

            expected_identities = cfg.get("_expected_source_identities") or {}
            expected_identity = expected_identities.get(src_b)
            if expected_identity is not None:
                try:
                    expected_identity = tuple(
                        int(value) for value in expected_identity
                    )
                except (TypeError, ValueError) as exc:
                    return -1, f"invalid expected source identity: {exc}".encode(), 0
                actual_identity = (int(st.st_dev), int(st.st_ino))
                if len(expected_identity) != 2 or actual_identity != expected_identity:
                    return -1, (
                        f"source identity changed before openat copy: expected="
                        f"{expected_identity!r}, found={actual_identity!r}"
                    ).encode(errors="replace"), 0

            # Directory
            if stat.S_ISDIR(st.st_mode):
                # O_NOFOLLOW on the source as well: lstat said "directory" a
                # moment ago, and between then and here the name can be a
                # symlink to a directory outside src_root. What is read through
                # the descriptor afterwards - the xattrs - would then come
                # from out there.
                dir_flags = (
                    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
                )
                src_dir_fd = None
                dst_dir_fd = None
                try:
                    src_dir_fd = os.open(leaf_b, dir_flags, dir_fd=src_parent_fd)
                    # And the descriptor is compared to what was judged.
                    # O_NOFOLLOW refuses a symlink; it does not refuse a
                    # DIFFERENT directory swapped in under the same name.
                    opened_src_st = os.fstat(src_dir_fd)
                    if (
                        int(opened_src_st.st_dev),
                        int(opened_src_st.st_ino),
                    ) != (int(st.st_dev), int(st.st_ino)):
                        return -1, (
                            b"source directory identity changed while opening "
                            b"it for copy"
                        ), 0
                    try:
                        # Locked and recorded; the final metadata follows in
                        # the restore at the end of the run.
                        dst_dir_fd, created = prepare_destination_directory(
                            dst_parent_fd, leaf_b, rel_b, opened_src_st,
                            src_dir_fd, metadata_cfg,
                        )
                    except NotADirectoryError:
                        dst_st = os.stat(
                            leaf_b, dir_fd=dst_parent_fd, follow_symlinks=False
                        )
                        return -1, (
                            b"destination type mismatch: source is directory, "
                            b"destination is " + file_type_name(dst_st.st_mode)
                        ), 0
                finally:
                    if src_dir_fd is not None:
                        try:
                            os.close(src_dir_fd)
                        except OSError:
                            pass
                    if dst_dir_fd is not None:
                        try:
                            os.close(dst_dir_fd)
                        except OSError:
                            pass

                fsync_if_per_file(cfg, dst_parent_fd)
                return 0, b"directory created" if created else b"directory updated", 0

            # Symlink
            if stat.S_ISLNK(st.st_mode):
                target = os.readlink(leaf_b, dir_fd=src_parent_fd)
                tmp_b = None
                tmp_exists = False

                try:
                    # Never reuse or unlink a predictable name. A stale file
                    # from an earlier process therefore cannot block this retry
                    # and cannot be mistaken for our own temporary object.
                    for _ in range(100):
                        candidate = _new_tmp_name()
                        try:
                            os.symlink(target, candidate, dir_fd=dst_parent_fd)
                        except FileExistsError:
                            continue
                        tmp_b = candidate
                        tmp_exists = True
                        break
                    else:
                        raise FileExistsError(
                            errno.EEXIST,
                            "cannot allocate unique temporary symlink name",
                        )

                    # A symlink has no meaningful mode of its own, so only
                    # ownership and timestamps apply here.
                    if permissions_ownership(cfg):
                        try:
                            os.chown(
                                tmp_b,
                                st.st_uid if permissions_owner(cfg) else -1,
                                st.st_gid if permissions_group(cfg) else -1,
                                dir_fd=dst_parent_fd,
                                follow_symlinks=False,
                            )
                        except PermissionError:
                            note_ownership_not_preserved()

                    if mtime_enabled(cfg):
                        # utimensat(AT_SYMLINK_NOFOLLOW) on the link itself;
                        # without this a symlink kept its creation time.
                        try:
                            os.utime(
                                tmp_b,
                                ns=(st.st_atime_ns, st.st_mtime_ns),
                                dir_fd=dst_parent_fd,
                                follow_symlinks=False,
                            )
                        except (OSError, NotImplementedError):
                            pass

                    if xattr_copy:
                        copy_xattrs_at(
                            src_parent_fd,
                            leaf_b,
                            dst_parent_fd,
                            tmp_b,
                            follow_symlinks=False,
                        )

                    os.replace(
                        tmp_b,
                        leaf_b,
                        src_dir_fd=dst_parent_fd,
                        dst_dir_fd=dst_parent_fd,
                    )
                    tmp_exists = False

                    fsync_if_per_file(cfg, dst_parent_fd)
                    return 0, b"symlink ok", 0

                finally:
                    if tmp_exists and tmp_b is not None:
                        try:
                            os.unlink(tmp_b, dir_fd=dst_parent_fd)
                        except FileNotFoundError:
                            pass

            # Regular file only
            if not stat.S_ISREG(st.st_mode):
                return -1, b"unsupported file type for openat copy", 0

            tmp_b = None
            src_fd = None
            dst_fd = None
            tmp_exists = False

            try:
                src_fd = open_checked_entry_at(src_parent_fd, leaf_b, st)

                # A fresh random name is used for every attempt. Ordinary
                # failures are cleaned up in finally; a remnant left by SIGKILL
                # cannot block subsequent retries.
                for _ in range(100):
                    candidate = _new_tmp_name()
                    try:
                        dst_fd = os.open(
                            candidate,
                            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_TRUNC,
                            # Always 0o600 while the content is being written.
                            # Creating it with the source mode would expose a
                            # setuid or world-readable file before it holds the
                            # verified data; the real mode is applied below,
                            # immediately before the rename.
                            0o600,
                            dir_fd=dst_parent_fd,
                        )
                    except FileExistsError:
                        continue
                    tmp_b = candidate
                    tmp_exists = True
                    break
                else:
                    raise FileExistsError(
                        errno.EEXIST,
                        "cannot allocate unique temporary file name",
                    )

                fused_src_hasher = (
                    create_streaming_hasher(compare_mode)
                    if is_fused_internal_hash_mode(compare_mode)
                    else None
                )
                copy_bufsize = buffer_bytes(cfg["buffer_mb"])
                sparse_copy = bool(cfg.get("sparse", False))
                logical_size = 0
                skipped_zero_bytes = 0

                while True:
                    data = os.read(src_fd, copy_bufsize)
                    if not data:
                        break
                    logical_size += len(data)
                    # The hasher always sees the logical byte stream, including
                    # the bytes of a skipped hole. Otherwise the fused source
                    # digest would not match the destination readback, which
                    # reads holes back as zeros.
                    if fused_src_hasher is not None:
                        fused_src_hasher.update(data)
                    if sparse_copy and buffer_is_all_zero(data):
                        os.lseek(dst_fd, len(data), os.SEEK_CUR)
                        skipped_zero_bytes += len(data)
                    else:
                        _write_all(dst_fd, data)

                if sparse_copy:
                    # Seeking past a hole does not extend the file. Without
                    # this the destination would be short whenever the source
                    # ends with a run of zero bytes.
                    os.ftruncate(dst_fd, logical_size)

                src_after = os.fstat(src_fd)
                if (
                    src_after.st_size != st.st_size
                    or src_after.st_mtime_ns != st.st_mtime_ns
                ):
                    return -1, b"source changed during copy", 0

                if metadata_cfg is not None:
                    apply_metadata_fd(dst_fd, st, metadata_cfg)
                if metadata_cfg is None or not permissions_mode(metadata_cfg):
                    os.fchmod(dst_fd, 0o666 & ~get_process_umask())

                if xattr_copy:
                    copy_xattrs_fd(src_fd, dst_fd)

                # Only fsync='file' pays for durability here. The read-back
                # below is served from the page cache either way, so this is
                # about crash consistency of the rename, not about the
                # verification.
                fsync_if_per_file(cfg, dst_fd)

                # Verify the temporary file before it is allowed to replace an
                # existing destination. A failed or incomplete copy therefore
                # leaves the old destination untouched.
                dst_st = os.fstat(dst_fd)

                if st.st_size != dst_st.st_size:
                    return -1, b"openat verify failed: size mismatch", 0

                # mtime is an independent setting now: when it is transferred
                # it is also verified, alongside the size check above.
                if mtime_enabled(cfg) and st.st_mtime_ns != dst_st.st_mtime_ns:
                    return -1, b"openat verify failed: mtime mismatch", 0

                if is_hash_compare_mode(compare_mode):
                    try:
                        if fused_src_hasher is not None:
                            # Internal sha256/blake3 hashes the exact source byte
                            # stream while it is already being read for copying.
                            # Only destination must be reread afterwards.
                            src_hash = fused_src_hasher.hexdigest()
                            dst_hash = hash_fd(
                                dst_fd, compare_mode, cfg.get("buffer_mb")
                            )
                        else:
                            src_hash, dst_hash = hash_pair_parallel_fds(
                                src_fd,
                                dst_fd,
                                compare_mode,
                                cfg.get("buffer_mb"),
                            )
                    except (RuntimeError, ValueError, OSError) as e:
                        return -1, f"openat verify failed: {e}".encode(), 0

                    if src_hash != dst_hash:
                        return -1, (
                            f"openat verify failed: {compare_mode} mismatch"
                        ).encode(), 0

                elif compare_mode not in (None, "size"):
                    return -1, (
                        f"openat verify failed: unsupported compare mode: "
                        f"{compare_mode}"
                    ).encode(), 0

                os.replace(
                    tmp_b,
                    leaf_b,
                    src_dir_fd=dst_parent_fd,
                    dst_dir_fd=dst_parent_fd,
                )
                tmp_exists = False
                # Making the name durable while the data is not would be worse
                # than neither, so both barriers share one setting.
                fsync_if_per_file(cfg, dst_parent_fd)

                return 0, b"openat copy ok", st.st_size

            finally:
                if src_fd is not None:
                    os.close(src_fd)
                if dst_fd is not None:
                    os.close(dst_fd)
                if tmp_exists and tmp_b is not None:
                    try:
                        os.unlink(tmp_b, dir_fd=dst_parent_fd)
                    except FileNotFoundError:
                        pass

        finally:
            if owns_parent_fds:
                if src_parent_fd is not None:
                    try:
                        os.close(src_parent_fd)
                    except OSError:
                        pass
                if dst_parent_fd is not None:
                    try:
                        os.close(dst_parent_fd)
                    except OSError:
                        pass

    except Exception as e:
        return -1, f"openat copy failed: {e}".encode(), 0


# ------------------------------------------------------------
# Copy / Retry
# ------------------------------------------------------------

def replace_in_parent_componentwise(
    tmp_b: bytes, dst_b: bytes, nofollow_from: Optional[bytes] = None
) -> None:
    """Rename inside one directory, addressed through its descriptor.

    os.replace() on a full pathname fails with ENAMETOOLONG once the name
    passes PATH_MAX, which would defeat an external engine that handles long
    paths itself. Both names live in the same directory, so one componentwise
    open of that directory makes the rename a pure renameat() on two leaf
    names.
    """
    parent_b = os.path.dirname(dst_b) or b"/"
    if os.path.dirname(tmp_b) != os.path.dirname(dst_b):
        raise ValueError("componentwise replace requires one shared parent")
    parent_fd = open_dir_fd_componentwise(
        parent_b, create=False, nofollow_from=nofollow_from
    )
    try:
        os.replace(
            os.path.basename(tmp_b),
            os.path.basename(dst_b),
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
    finally:
        os.close(parent_fd)


def unlink_in_parent_componentwise(
    path_b: bytes, nofollow_from: Optional[bytes] = None
) -> None:
    """Remove one leaf, addressed through its componentwise-opened parent."""
    parent_b = os.path.dirname(path_b) or b"/"
    parent_fd = open_dir_fd_componentwise(
        parent_b, create=False, nofollow_from=nofollow_from
    )
    try:
        os.unlink(os.path.basename(path_b), dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def lstat_in_parent_componentwise(
    path_b: bytes, nofollow_from: Optional[bytes] = None
):
    """lstat one leaf through its componentwise-opened parent.

    os.lstat() on the full pathname fails with ENAMETOOLONG past PATH_MAX,
    which would make this check the one step that breaks a destination the
    rest of the path handles.
    """
    parent_b = os.path.dirname(path_b) or b"/"
    parent_fd = open_dir_fd_componentwise(
        parent_b, create=False, nofollow_from=nofollow_from
    )
    try:
        return os.stat(
            os.path.basename(path_b),
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
    finally:
        os.close(parent_fd)


def copy_one_external_with_verify(
    src_b: bytes,
    dst_b: bytes,
    cfg: dict,
    compare_mode: str,
    xattr_copy: bool = False,
    expected_source_identity: Optional[Tuple[int, int]] = None,
) -> Tuple[int, bytes]:
    """Run the user-supplied argv-template copy engine without a shell.

    The source and the temporary destination are handed to the engine through
    inherited descriptors. The source descriptor is also used for metadata
    and verification, so changing the source name cannot redirect the copy.
    Componentwise opens keep paths beyond PATH_MAX usable.
    """
    timeout = cfg["copy_timeout"]
    engine = cfg.get("engine", "internal")
    if engine == "internal":
        return -1, b"external copy called with engine=internal"

    # Below these roots a symlink is refused; the roots themselves may be
    # reached through one, which is how filesystems are normally laid out.
    src_root_b = os.fsencode(cfg["src_root"]) if cfg.get("src_root") else None
    dst_root_b = os.fsencode(cfg["dst_root"]) if cfg.get("dst_root") else None

    long_internal_post = (
        cfg.get("max_path_length", "engine") == "engine"
        and path_needs_long_handling(src_b, dst_b)
    )

    # The engine never receives the final destination name. A symlink sitting
    # at that name would be followed and its target overwritten outside
    # dst_root, and the type check afterwards would report the damage rather
    # than prevent it. The engine writes to a fresh temporary name in the same
    # directory instead; only after the result has been verified does an atomic
    # rename put it in place, and rename replaces the symlink entry itself.
    tmp_b = os.path.join(os.path.dirname(dst_b), new_temporary_leaf_name("ext"))

    # The engine is given a DESCRIPTOR, not a name.
    #
    # Reserving the name with O_CREAT|O_EXCL was not enough: the entry can be
    # unlinked and replaced with a symlink after the reservation and before
    # the engine opens it, and then the engine writes through that symlink,
    # outside dst_root. Checking the name afterwards only reports the damage.
    #
    # /proc/self/fd/N resolves to the open file itself, so there is no
    # directory lookup left for anyone to race. The number is the same in the
    # child because pass_fds keeps it, and the path is short whatever the
    # destination depth is - which is also why an over-PATH_MAX destination
    # works again: the reservation walks the parent componentwise instead of
    # passing one long pathname to open().
    try:
        # O_RDWR and not O_WRONLY: everything after the engine returns -
        # metadata, xattrs, the verification read, the flush - goes through
        # THIS descriptor. Touching the name again would reopen a question
        # that was already answered, and the answer can change between two
        # lookups.
        tmp_fd = open_file_fd_componentwise(
            tmp_b,
            os.O_CREAT | os.O_EXCL | os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            nofollow_from=dst_root_b,
        )
    except OSError as exc:
        return -1, (
            f"cannot reserve the temporary name for "
            f"{path_display(src_b)}: {exc}"
        ).encode()
    tmp_exists = True
    src_fd = None
    try:
        try:
            src_fd = open_file_fd_componentwise(
                src_b, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                nofollow_from=src_root_b,
            )
            source_st = os.fstat(src_fd)
            if not stat.S_ISREG(source_st.st_mode):
                return -1, b"external copy source is not a regular file"
            if expected_source_identity is not None and (
                source_st.st_dev, source_st.st_ino
            ) != expected_source_identity:
                return -1, b"source identity changed before external copy"

            engine_source = descriptor_pathname(src_fd)
            engine_destination = descriptor_pathname(tmp_fd)
            if engine_source is None or engine_destination is None:
                return -1, (
                    b"external copy requires /dev/fd or /proc/self/fd "
                    b"for both source and destination"
                )

            argv = expand_copy_engine_argv(engine, engine_source, engine_destination)
            # A duplicate of the locked dst_root descriptor goes along, unused
            # by the engine: flock() belongs to the open file, so the run lock
            # then lasts as long as an engine that outlives this process
            # could still be writing.
            lock_fd = (
                destination_guard.open_root()
                if destination_guard is not None else None
            )
            try:
                proc = subprocess.run(
                    argv,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=timeout,
                    pass_fds=trusted_exec_fds() + (src_fd, tmp_fd)
                    + ((lock_fd,) if lock_fd is not None else ()),
                )
            finally:
                if lock_fd is not None:
                    os.close(lock_fd)
        except subprocess.TimeoutExpired:
            return -1, f"timeout copying {path_display(src_b)}".encode()
        except Exception as e:
            return -1, str(e).encode()

        if proc.returncode != 0:
            err = proc.stderr or (
                f"external copy engine exited with status {proc.returncode}".encode()
            )
            return proc.returncode, err

        # From here on the temporary file is addressed ONLY through tmp_fd.
        # Checking the name and then acting on the name again is what let a
        # swapped entry take the metadata: an fchmod through a re-opened name
        # changed the permissions of a file outside the destination tree,
        # and the verification only noticed afterwards.
        try:
            apply_metadata = preserves_any_metadata(cfg)

            if apply_metadata:
                apply_metadata_fd(tmp_fd, source_st, cfg)
            if not permissions_mode(cfg):
                # The name was reserved with 0600, so without the mode class
                # the file would keep that instead of the mode the engine
                # used to give it when it created the file itself. The test
                # is on the mode class alone: preserving only mtime is enough
                # to make apply_metadata true while leaving the mode unset.
                os.fchmod(
                    tmp_fd,
                    source_st.st_mode & 0o7777 & ~get_process_umask(),
                )

            if xattr_copy:
                copy_xattrs_fd(src_fd, tmp_fd)

            if fsync_per_file(cfg):
                os.fsync(tmp_fd)

        except OSError as e:
            return -1, f"post-copy metadata/fsync failed: {e}".encode()

        if compare_mode is not None:
            rc, msg = compare_source_against_fd(
                src_fd, tmp_fd, cfg, compare_mode
            )
            if rc != 0:
                return rc, f"verify failed: {msg.decode(errors='replace')}".encode()

        # The rename is the one step that still has to name the temporary
        # file, so the name is checked here rather than earlier: everything
        # between the engine and this point went through tmp_fd and needed no
        # lookup at all.
        own_st = os.fstat(tmp_fd)
        try:
            now_st = lstat_in_parent_componentwise(
                tmp_b, nofollow_from=dst_root_b
            )
        except OSError as exc:
            return -1, (
                f"the temporary name disappeared before it could be renamed: "
                f"{exc}"
            ).encode()
        if (now_st.st_dev, now_st.st_ino) != (own_st.st_dev, own_st.st_ino):
            return -1, (
                b"the temporary name was replaced before the rename; "
                b"refusing to publish a file this run did not write"
            )

        # ALWAYS through the componentwise parent, not only for over-PATH_MAX
        # destinations. os.replace() on two full pathnames resolves both of
        # them again at that moment: swap a directory component below dst_root
        # for a symlink in between and the rename lands outside the tree,
        # which as root means overwriting a file the run was never allowed to
        # touch. Opening the parent componentwise with nofollow_from refuses
        # that component, and the rename is then a renameat() on two leaf
        # names inside a descriptor this process already holds.
        try:
            replace_in_parent_componentwise(
                tmp_b, dst_b, nofollow_from=dst_root_b
            )
        except OSError as exc:
            # A refused directory component lands here, and so does a full
            # disk. Either way this is one file that did not make it, not a
            # reason to let the exception out and take the whole batch with
            # it. The temporary file stays claimed and is removed below.
            return -1, (
                f"cannot put the copy in place: {exc}"
            ).encode()
        tmp_exists = False

        # And once more afterwards. A rename names its source, and between the
        # check above and the call itself the entry can be swapped again - the
        # window is microseconds, but the consequence is that the destination
        # holds a file nobody verified while this function reports success.
        # Somebody who can write in that directory could have put the same
        # file there without any of this; what must not happen is calling it
        # ok.
        try:
            published_st = lstat_in_parent_componentwise(
                dst_b, nofollow_from=dst_root_b
            )
        except OSError as exc:
            return -1, (
                f"the destination disappeared right after the rename: {exc}"
            ).encode()
        if (published_st.st_dev, published_st.st_ino) != (
            own_st.st_dev,
            own_st.st_ino,
        ):
            return -1, (
                b"the destination name does not hold the file this run "
                b"wrote; something replaced it during the rename"
            )

        if fsync_per_file(cfg):
            # The file's own data was flushed above, but the rename that gave
            # it its final name lives in the parent directory. Without this the
            # external engine would promise less durability than the internal
            # one for the same setting.
            parent_b = os.path.dirname(dst_b) or b"/"
            try:
                if long_internal_post:
                    parent_fd = open_dir_fd_componentwise(parent_b, create=False)
                else:
                    parent_fd = os.open(parent_b, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(parent_fd)
                finally:
                    os.close(parent_fd)
            except OSError as e:
                return -1, f"destination directory fsync failed: {e}".encode()

        return 0, b"ok"

    finally:
        if src_fd is not None:
            try:
                os.close(src_fd)
            except OSError:
                pass
        try:
            os.close(tmp_fd)
        except OSError:
            pass
        if tmp_exists:
            try:
                # Same reason as the rename above: never a full pathname here.
                unlink_in_parent_componentwise(
                    tmp_b, nofollow_from=dst_root_b
                )
            except OSError:
                pass


def process_copy_batch(paths: List[bytes], cfg: dict) -> dict:
    retries = int(cfg["max_retries"])
    backoff_base = float(cfg["retry_backoff_base"])
    backoff_max = float(cfg["retry_backoff_max"])
    compare_mode = cfg["verify"]
    xattr_copy = cfg["xattr"]
    engine = cfg.get("engine", "internal")
    engine_controls_path_length = cfg.get("max_path_length", "engine") == "engine"

    remaining = list(paths)
    failed_files = {}
    next_round = None
    total = 0
    written_count = 0
    requeued_count = 0
    path_length_error_count = 0
    skipped_count = 0
    skip_existing = bool(cfg.get("skip_existing", False))
    stopped = False

    t0 = time.time()
    parent_dirs = set()

    # One shared parent-directory resolution for the whole batch. Input from
    # 'find -print0' is grouped by directory, so consecutive paths of a batch
    # normally share their parent and resolve it exactly once instead of once
    # per file.
    prelock_batch(paths, cfg)
    parent_fd_cache = ParentDirFdCache(
        os.fsencode(cfg["src_root"]),
        os.fsencode(cfg["dst_root"]),
        xattr_copy,
        cfg,
    )

    try:
        for attempt in range(1, retries + 1):
            next_round = {}

            for idx, src_b in enumerate(remaining):
                reason = batch_pause_reason(cfg, "during copy batch")
                if reason is not None:
                    requeue_list = list(next_round.keys()) + remaining[idx:]
                    requeued_count += requeue_unprocessed_paths(requeue_list, reason)
                    stopped = True
                    break

                # A single malformed input record must not abort the whole batch.
                # Every other path of this batch is still processed normally.
                try:
                    rel_b, src_abs_b, dst_b = normalize_input_path(src_b, cfg)
                except Exception as exc:
                    failed_files[
                        os.fsencode(src_b) if isinstance(src_b, str) else src_b
                    ] = str(exc).encode(errors="replace")
                    continue

                policy_error = path_length_policy_error(cfg, src_abs_b, dst_b)
                if policy_error is not None:
                    failed_files[src_abs_b] = policy_error
                    path_length_error_count += 1
                    continue

                technical_long = path_needs_long_handling(src_abs_b, dst_b)

                # Numeric max_path_length intentionally disables automatic long-path
                # handling: allowed paths use classic pathname syscalls and may fail
                # with ENAMETOOLONG. With "engine", the internal engine may inspect
                # the source componentwise so long paths can still be classified.
                st = None
                try:
                    if engine_controls_path_length and technical_long:
                        st = lstat_path_openat(src_abs_b)
                    else:
                        st = os.lstat(src_abs_b)
                except OSError as e:
                    # With a numeric max_path_length an allowed over-PATH_MAX path
                    # must not be silently rescued with openat. For an external
                    # copy engine, however, ENAMETOOLONG from this optional Python
                    # pre-stat must also not prevent the external utility itself
                    # from trying the pathname.
                    if not (
                        engine != "internal"
                        and technical_long
                        and not engine_controls_path_length
                        and e.errno == errno.ENAMETOOLONG
                    ):
                        failed_files[src_abs_b] = str(e).encode()
                        continue
                except Exception as e:
                    failed_files[src_abs_b] = str(e).encode()
                    continue

                # Defined for every path, including the external-engine case
                # where the optional pre-stat could not be performed.
                candidate_size = 0

                if st is not None:
                    expected_identities = cfg.get("_expected_source_identities") or {}
                    expected_identity = expected_identities.get(src_abs_b)
                    if expected_identity is not None:
                        actual_identity = (int(st.st_dev), int(st.st_ino))
                        expected_identity = tuple(
                            int(value) for value in expected_identity
                        )
                        if len(expected_identity) != 2 or actual_identity != expected_identity:
                            failed_files[src_abs_b] = (
                                f"source identity changed before copy: expected="
                                f"{expected_identity!r}, found={actual_identity!r}"
                            ).encode(errors="replace")
                            continue

                    if not (
                        stat.S_ISREG(st.st_mode)
                        or stat.S_ISDIR(st.st_mode)
                        or stat.S_ISLNK(st.st_mode)
                    ):
                        failed_files[src_abs_b] = b"unsupported file type"
                        continue

                    if stat.S_ISREG(st.st_mode):
                        # Recorded only after the copy actually succeeds, see
                        # below; a failed file must not inflate throughput.
                        candidate_size = st.st_size
                    else:
                        candidate_size = 0

                # Resume support: one fstatat over the cached destination
                # parent decides whether this object is already what the run
                # would write. With skip_existing=false the branch is a single
                # comparison and issues no syscall at all.
                # Never for a directory: it is cheap to process, and it has to
                # be locked and recorded whether or not its metadata already
                # matches - children copied later still reset its mtime, and
                # the restore at the end must know it exists.
                if (
                    skip_existing
                    and st is not None
                    and rel_b != b"."
                    and not stat.S_ISDIR(st.st_mode)
                ):
                    try:
                        skip_src_parent_fd, skip_dst_parent_fd = (
                            parent_fd_cache.get(os.path.dirname(rel_b))
                        )
                        if destination_is_current(
                            skip_src_parent_fd,
                            skip_dst_parent_fd,
                            os.path.basename(rel_b),
                            st,
                            cfg,
                            compare_mode,
                        ):
                            skipped_count += 1
                            continue
                    except OSError:
                        # No destination parent yet means nothing can be
                        # skipped; fall through and copy normally.
                        pass

                # Directory/symlink handling remains internal and non-recursive
                # when the source can be classified. For regular files, only the
                # internal engine switches to openat when max_path_length="engine".
                use_openat = (
                    st is not None
                    and (
                        stat.S_ISDIR(st.st_mode)
                        or stat.S_ISLNK(st.st_mode)
                        or (
                            engine == "internal"
                            and engine_controls_path_length
                            and technical_long
                        )
                    )
                )

                if use_openat or engine == "internal":
                    # The internal engine creates the destination parent chain
                    # itself, component by component and with O_NOFOLLOW, and
                    # copies directory metadata for every directory it creates.
                    # A preceding os.makedirs() would both defeat that metadata
                    # copy and follow an intermediate destination symlink.
                    rc, msg, _copied_bytes = copy_one_openat_with_verify(
                        src_abs_b,
                        dst_b,
                        cfg,
                        compare_mode,
                        xattr_copy,
                        parent_fd_cache=parent_fd_cache,
                    )
                else:
                    # For an external engine in "engine" mode, do not let Python's
                    # own PATH_MAX prevent the utility from deciding whether it can
                    # handle an overlong destination path.
                    delegate_long_to_external = technical_long

                    parent = os.path.dirname(dst_b)
                    if parent and parent not in parent_dirs and not delegate_long_to_external:
                        try:
                            # Componentwise creation with O_NOFOLLOW inside the
                            # relative part; an intermediate destination symlink
                            # can therefore not redirect the external utility
                            # outside dst_root.
                            rel_parent_b = os.path.dirname(
                                make_relative_bytes(
                                    src_abs_b, os.fsencode(cfg["src_root"])
                                )
                            )
                            # The cache owns both fds; only the directory
                            # creation side effect is needed here.
                            parent_fd_cache.get(rel_parent_b)
                        except Exception as e:
                            if not is_retryable(str(e).encode()):
                                failed_files[src_abs_b] = os.fsencode(parent) + b":" + str(e).encode()
                                continue
                            next_round[src_b] = str(e).encode()
                            continue
                        parent_dirs.add(parent)

                    rc, msg = copy_one_external_with_verify(
                        src_abs_b, dst_b, cfg, compare_mode, xattr_copy,
                        (st.st_dev, st.st_ino) if st is not None else None,
                    )

                if rc != 0:
                    if not is_retryable(msg):
                        failed_files[src_abs_b] = msg
                        continue
                    next_round[src_b] = msg
                else:
                    total += candidate_size
                    written_count += 1

            if stopped:
                break

            if not next_round:
                break

            reason = batch_pause_reason(cfg, "before copy retry round")
            if reason is not None:
                requeued_count += requeue_unprocessed_paths(list(next_round.keys()), reason)
                stopped = True
                break

            remaining = list(next_round.keys())
            backoff_sleep(attempt, backoff_base, backoff_max)

    finally:
        parent_fd_cache.close()

    duration = max(time.time() - t0, 0.001)
    bytes_per_sec = total / duration

    if not stopped and next_round is not None and next_round:
        for retry_src_b, msg in next_round.items():
            _, retry_src_abs_b, _ = normalize_input_path(retry_src_b, cfg)
            failed_files[retry_src_abs_b] = msg

    base = {
        "method": "copy",
        "attempt": attempt,
        "count": len(paths),
        "bytes_total": total,
        "duration_sec": round(duration, 3),
        "bytes_per_sec": round(bytes_per_sec, 2),
        "path_length_error_count": path_length_error_count,
        # Objects the destination already matched, so nothing was written.
        "skipped_count": skipped_count,
    }

    if stopped:
        base.update({
            "status": "stopped",
            "message": "stopped; unprocessed files requeued",
            "failed_count": count_failed_input_records(paths, failed_files, cfg),
            "failed_files": {
                path_display(path_b): msg.decode(errors="replace")
                for path_b, msg in failed_files.items()
            },
            "requeued_count": requeued_count,
        })
        return base

    if fsync_barrier_per_batch(cfg) and written_count:
        # Once per batch instead of once per file. The same bytes are written
        # either way; what changes is how often the run waits for them.
        # The condition counts written OBJECTS, not bytes: a batch of
        # directories, symlinks or empty files transfers zero bytes and still
        # created durable entries that the promise covers.
        sync_destination_filesystem(cfg, "batch complete")

    if not failed_files:
        base.update({
            "status": "ok",
            "message": "all files copied",
        })
        return base

    base.update({
        "status": "failed",
        "message": "errors",
        "failed_count": count_failed_input_records(
            paths, failed_files, cfg
        ),
        "failed_files": {
            path_display(path_b): msg.decode(errors="replace")
            for path_b, msg in failed_files.items()
        },
    })
    return base

def parse_rsync_stats(stderr_b: bytes) -> Tuple[int, int]:
    transferred_bytes = 0
    transferred_files = 0

    for line in stderr_b.decode(errors="replace").splitlines():
        line = line.strip()

        if line.startswith("Total transferred file size:"):
            value = line.split(":", 1)[1].strip()
            value = value.split()[0].replace(",", "")
            try:
                transferred_bytes = int(value)
            except ValueError:
                pass
        elif line.startswith("Number of regular files transferred:"):
            value = line.split(":", 1)[1].strip()
            value = value.split()[0].replace(",", "")
            try:
                transferred_files = int(value)
            except ValueError:
                pass

    return transferred_bytes, transferred_files

def build_rsync_verification_cfg(cfg: dict, compare_mode: str) -> dict:
    """Derive a diff configuration that checks what rsync was asked to do.

    The metadata classes are taken over unchanged, so the verification asks for
    exactly what the transfer was configured to produce: with mtime=false rsync
    gets no -t and the check does not look at timestamps either.
    """
    verify_cfg = dict(cfg)
    verify_cfg["method"] = "diff"
    verify_cfg["verify"] = compare_mode
    verify_cfg.pop("_hardlink_group_task", None)
    # Owner, mode, ACL and xattrs are applied by the restore at the end of the
    # run, not by rsync, so right after a batch they are not there yet and
    # comparing them would report every file. File times ARE set by rsync.
    verify_cfg["permissions"] = ""
    verify_cfg["xattr"] = False
    # A directory's mtime is a record of the last entry created in it, so while
    # other workers are still writing into the same destination directories its
    # value is meaningless. Checking it per batch would report races, not
    # errors. Directory metadata is verified by a separate diff run after the
    # job, when nothing writes any more.
    verify_cfg["_inline_verification"] = True
    return verify_cfg


def verify_rsync_batch(
    input_paths: List[bytes], cfg: dict, transfer_failed: bool
) -> Optional[dict]:
    """Re-check a finished rsync batch with the diff engine.

    null         no check at all; the rsync exit code is trusted.
    size_iferr   sizes are compared only when rsync reported a problem, so a
                 clean run costs nothing while a failed one is itemised.
    size / hash  the batch is always compared with that mode.
    """
    compare_mode = cfg.get("verify")
    if compare_mode is None or not input_paths:
        return None

    if compare_mode == "size_iferr":
        if not transfer_failed:
            return None
        compare_mode = "size"

    return process_diff_batch(
        list(input_paths),
        build_rsync_verification_cfg(cfg, compare_mode),
    )


def process_rsync_batch(paths: List[bytes], cfg: dict) -> dict:
    retries = int(cfg["max_retries"])
    backoff_base = float(cfg["retry_backoff_base"])
    backoff_max = float(cfg["retry_backoff_max"])
    src_root_b = os.fsencode(cfg["src_root"])
    dst_root_b = os.fsencode(cfg["dst_root"])
    timeout = cfg["copy_timeout"]
    zero_bytes_timeout = float(cfg.get("rsync_zero_bytes_timeout", 300))
    operative_parameters = build_rsync_parameters(cfg)
    xattr_copy = bool(cfg.get("xattr", False))
    max_path_policy = cfg.get("max_path_length", "engine")

    # rsync --timeout=N aborts on N seconds without I/O. This is a real
    # inactivity watchdog, unlike a total-duration check after the fact.
    # The destination is handed to rsync as the descriptor this run holds for
    # dst_root, not as its name: whatever happens to the name during the run,
    # rsync writes into the directory that was checked and locked at the
    # start. The source is still a name - see the README on rsync resolving
    # source paths again.
    rsync_root_fd = None
    if destination_guard is not None:
        rsync_root_fd = open_destination_root(dst_root_b)
        rsync_destination_b = os.fsencode(f"/proc/self/fd/{rsync_root_fd}/")
    else:
        rsync_destination_b = dst_root_b + b"/"
    cmd = [os.fsencode(resolve_trusted_executable("rsync"))] + operative_parameters + [
        b"--files-from=-", b"-0", b"--stats",
        f"--timeout={int(math.ceil(zero_bytes_timeout))}".encode("ascii"),
        src_root_b + b"/", rsync_destination_b,
    ]
    rsync_pass_fds = trusted_exec_fds() + (
        (rsync_root_fd,) if rsync_root_fd is not None else ()
    )
    try:
        return _process_rsync_batch_with_cmd(
            paths, cfg, cmd, rsync_pass_fds, retries, backoff_base, backoff_max,
            src_root_b, dst_root_b, timeout, xattr_copy, max_path_policy,
        )
    finally:
        if rsync_root_fd is not None:
            os.close(rsync_root_fd)


def _process_rsync_batch_with_cmd(
    paths: List[bytes],
    cfg: dict,
    cmd: List[bytes],
    rsync_pass_fds: Tuple[int, ...],
    retries: int,
    backoff_base: float,
    backoff_max: float,
    src_root_b: bytes,
    dst_root_b: bytes,
    timeout,
    xattr_copy: bool,
    max_path_policy,
) -> dict:
    # rsync resolves the names it is handed itself, and it follows symlinks in
    # the intermediate components of a source path. A directory replaced by a
    # symlink between the listing and the transfer would therefore be read
    # from outside src_root, which is exactly what the internal engine avoids
    # by opening every component with O_NOFOLLOW. The same walk is done here,
    # once per distinct parent directory of the batch, so rsync only ever sees
    # paths whose source chain was verified to be free of symlinks.
    #
    # This does not make the transfer race-free: rsync re-resolves the name
    # afterwards, so a swap inside that window is still possible. It closes
    # the ordinary case, and it matches what the hard_link modes already do
    # componentwise.
    checked_source_parents = {}

    def source_parent_error(rel_parent_b: bytes) -> bytes:
        cached = checked_source_parents.get(rel_parent_b)
        if cached is not None:
            return cached
        try:
            parent_fd = open_src_dir_fd_from_root(src_root_b, rel_parent_b)
        except OSError as exc:
            # The three causes need different answers from an operator, so
            # they are not merged into one message: a symlink in the chain is
            # the escape this check exists for, a missing component means the
            # list no longer matches the tree, and EACCES is a permission
            # problem of this process.
            if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                reason = (
                    "a component of the source path below src_root is a "
                    "symlink or not a directory, so the path would resolve "
                    "outside the source tree"
                )
            elif exc.errno == errno.ENOENT:
                reason = (
                    "a component of the source path below src_root does not "
                    "exist any more"
                )
            else:
                reason = "the source path below src_root cannot be opened"
            cached = (
                f"{reason}: {path_display(rel_parent_b)}: {exc}"
            ).encode(errors="replace")
        else:
            os.close(parent_fd)
            cached = b""
        checked_source_parents[rel_parent_b] = cached
        return cached

    # Destination side, before rsync sees anything: every parent chain is
    # created and locked by this program, a directory named in the batch is
    # created and locked itself, and the metadata every file and symlink must
    # get at the end is recorded from the source. rsync then only ever writes
    # into locked directories and never creates one.
    parent_fd_cache = ParentDirFdCache(src_root_b, dst_root_b, xattr_copy, cfg)
    root_recorded = False
    # rsync skips existing destination FILES with --ignore-existing (never
    # for a resolved hard-link group). Existing directories still get their
    # metadata from rsync in that mode, so they are recorded as usual.
    skip_existing_files = bool(cfg.get("ignore_existing", False)) and not bool(
        cfg.get("_hardlink_group_task", False)
    )
    prelock_batch(paths, cfg)

    def prepare_destination(rel_b: bytes) -> bytes:
        nonlocal root_recorded
        if destination_guard is None:
            return b""
        try:
            if rel_b == b".":
                if not root_recorded:
                    record_root_final(cfg)
                    root_recorded = True
                return b""
            src_parent_fd, dst_parent_fd = parent_fd_cache.get(
                os.path.dirname(rel_b)
            )
            leaf_b = os.path.basename(rel_b)
            try:
                leaf_st = os.lstat(leaf_b, dir_fd=src_parent_fd)
            except FileNotFoundError:
                # Vanished: rsync reports it the way it always has.
                return b""
            if stat.S_ISDIR(leaf_st.st_mode):
                parent_fd_cache.get(rel_b)
                return b""
            if skip_existing_files:
                # --ignore-existing leaves an existing destination file
                # completely alone, its metadata included, so nothing may be
                # recorded for it either. The answer cannot change before
                # rsync looks: the parent is locked, and only this run can
                # add or remove entries in it.
                try:
                    os.lstat(leaf_b, dir_fd=dst_parent_fd)
                    return b""
                except FileNotFoundError:
                    pass
            record_file_final(rel_b, leaf_st, src_parent_fd, leaf_b, cfg)
        except OSError as exc:
            return (
                f"cannot prepare the destination for "
                f"{path_display(rel_b)}: {exc}"
            ).encode(errors="replace")
        return b""

    try:
        return _run_rsync_batch(
            paths, cfg, cmd, rsync_pass_fds, retries, backoff_base,
            backoff_max, src_root_b, timeout, xattr_copy, max_path_policy,
            source_parent_error, prepare_destination,
        )
    finally:
        parent_fd_cache.close()


def _run_rsync_batch(
    paths, cfg, cmd, rsync_pass_fds, retries, backoff_base, backoff_max,
    src_root_b, timeout, xattr_copy, max_path_policy,
    source_parent_error, prepare_destination,
) -> dict:
    t0 = time.time()
    env = dict(os.environ)
    env["LC_ALL"] = "C"

    rsync_paths = []
    rsync_input_paths = []
    # (original input, absolute source, absolute destination)
    internal_long_paths = []
    path_length_errors = {}
    input_errors = {}

    for src_b in paths:
        try:
            rel_b, src_abs_b, dst_b = normalize_input_path(src_b, cfg)
        except Exception as exc:
            input_errors[src_b] = str(exc).encode()
            continue

        policy_error = path_length_policy_error(cfg, src_abs_b, dst_b)
        if policy_error is not None:
            path_length_errors[src_abs_b] = policy_error
            continue

        if (
            max_path_policy == "engine"
            and path_needs_long_handling(src_abs_b, dst_b, rel_b)
        ):
            # rsync has no configurable engine.  In "engine" mode, PATH_MAX
            # sized paths are therefore redirected to parallel_tools' internal
            # long-path-safe copy engine (openat/dir_fd).  That engine walks
            # the source chain componentwise itself, so it needs no pre-check.
            internal_long_paths.append((src_b, src_abs_b, dst_b))
        else:
            parent_error = source_parent_error(os.path.dirname(rel_b))
            if parent_error:
                input_errors[src_abs_b] = parent_error
                continue
            destination_error = prepare_destination(rel_b)
            if destination_error:
                input_errors[src_abs_b] = destination_error
                continue
            # max_path_length="rsync": delegate even technically long paths to
            # rsync unchanged.  Numeric max_path_length also reaches this path
            # after the explicit policy limit check and intentionally gets no
            # automatic openat fallback.
            rsync_paths.append(rel_b)
            rsync_input_paths.append(src_b)

    requeued_count = 0
    status = "ok"
    last_err = b""
    openat_failed = {}
    openat_bytes = 0
    openat_files = 0
    partial_transfer = False

    def current_stop_reason(phase: str) -> Optional[str]:
        return batch_pause_reason(cfg, phase)

    def preserve_rsync_paths(reason: str, previous_error: bytes = b"") -> bytes:
        nonlocal requeued_count
        requeued_count += requeue_unprocessed_paths(rsync_input_paths, reason)
        stop_message = (
            f"{reason}; requeued {requeued_count} unprocessed paths"
        ).encode()
        if previous_error:
            return previous_error.rstrip() + b"\n" + stop_message
        return stop_message

    # PATH_MAX-sized records in max_path_length="engine" mode use the same
    # internal openat copy implementation that copy uses. rsync is bypassed for
    # them, so their contents are verified here with SHA-256.
    long_path_stopped = False
    for long_index, (_input_b, src_abs_b, dst_b) in enumerate(internal_long_paths):
        reason = current_stop_reason("during rsync long-path fallback")
        if reason is not None:
            requeued_count += requeue_unprocessed_paths(
                [entry[0] for entry in internal_long_paths[long_index:]],
                reason,
            )
            long_path_stopped = True
            status = "stopped"
            last_err = reason.encode()
            break

        rc, msg, file_sz = copy_one_openat_with_verify(
            src_abs_b,
            dst_b,
            cfg,
            "sha256",
            xattr_copy,
        )
        if rc != 0:
            openat_failed[src_abs_b] = msg
        else:
            openat_bytes += file_sz
            openat_files += 1

    # If there are no ordinary rsync records, the internal fallback constitutes
    # the complete transfer result.
    success = not rsync_paths and not openat_failed and not long_path_stopped

    if rsync_paths:
        for attempt in range(1, retries + 1):
            reason = current_stop_reason("before rsync attempt")
            if reason is not None:
                last_err = preserve_rsync_paths(reason, last_err)
                status = "stopped"
                break

            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                pass_fds=rsync_pass_fds,
                # A local rsync is three processes: the invocation forks a
                # server child, and the receiver forks the generator. Killing
                # only the one this program started leaves the other two
                # writing into the destination while the next attempt already
                # runs. Its own session makes the whole set addressable.
                start_new_session=True,
            )
            input_data = b"\0".join(rsync_paths) + b"\0"

            try:
                stdout, stderr = proc.communicate(input=input_data, timeout=timeout)
            except BrokenPipeError:
                proc.wait()
                stderr = b"broken pipe"
                last_err = stderr
                reason = current_stop_reason("after interrupted rsync attempt")
                if reason is not None:
                    last_err = preserve_rsync_paths(reason, last_err)
                    status = "stopped"
                    break
                backoff_sleep(attempt, backoff_base, backoff_max)
                continue
            except subprocess.TimeoutExpired:
                _terminate_process_group(proc, "rsync timeout")
                last_err = b"rsync timeout"
                reason = current_stop_reason("after timed-out rsync attempt")
                if reason is not None:
                    last_err = preserve_rsync_paths(reason, last_err)
                    status = "stopped"
                    break
                if attempt >= retries:
                    status = "failed"
                    break
                # max_retries promises repeated attempts for transient
                # failures; a timeout is the most transient of them.
                backoff_sleep(attempt, backoff_base, backoff_max)
                continue

            last_err = stdout + b"\n" + stderr
            if proc.returncode == 0:
                success = True
                break

            if proc.returncode == 24:
                # "Partial transfer due to vanished source files": rsync did
                # not transfer everything it was asked to. Reporting this as
                # success hides real gaps in the destination.
                partial_transfer = True
                last_err = last_err.rstrip() + (
                    b"\nrsync exit code 24: partial transfer, "
                    b"source files vanished during the run"
                )
                break

            reason = current_stop_reason("after unsuccessful rsync attempt")
            if reason is not None:
                last_err = preserve_rsync_paths(reason, last_err)
                status = "stopped"
                break

            if not is_retryable(stderr):
                status = "failed"
                break

            backoff_sleep(attempt, backoff_base, backoff_max)
        else:
            status = "failed"

    rsync_bytes, rsync_files = parse_rsync_stats(last_err)
    bytes_total = rsync_bytes + openat_bytes
    files_total = rsync_files + openat_files
    duration = max(time.time() - t0, 0.001)
    bytes_per_sec = bytes_total / duration

    # The former total-duration heuristic flagged correct no-op transfers as
    # failures. Inactivity is now detected by rsync itself via --timeout.

    if partial_transfer:
        success = False
        status = "failed"
    if long_path_stopped:
        status = "stopped"
    if status == "ok" and not success:
        status = "failed"
    if (path_length_errors or input_errors or openat_failed) and status == "ok":
        status = "failed"

    # Verification runs after the transfer attempt, never after a stop: a
    # requeued batch has not been fully written yet.
    verification = None
    if status != "stopped":
        try:
            verification = verify_rsync_batch(
                rsync_input_paths, cfg, transfer_failed=not success
            )
        except Exception as exc:
            verification = {
                "status": "failed",
                "failed_count": len(rsync_input_paths),
                "failed_files": {
                    "<verification>": f"rsync verification failed: {exc}"
                },
                "checked": 0,
                "requeued_count": 0,
            }

    if verification is not None:
        requeued_count += int(verification.get("requeued_count", 0) or 0)
        if verification.get("status") == "stopped":
            status = "stopped"
        elif verification.get("status") != "ok":
            success = False
            if status == "ok":
                status = "failed"

    result = {
        "status": status,
        "method": "rsync",
        "count": len(paths),
        "transferred": files_total,
        "bytes_total": bytes_total,
        "duration_sec": round(duration, 3),
        "bytes_per_sec": round(bytes_per_sec, 2),
        "message": "" if success else last_err.decode(errors="replace"),
        "requeued_count": requeued_count,
        "path_length_error_count": len(path_length_errors),
        "long_path_internal_count": len(internal_long_paths),
        "long_path_internal_failed_count": len(openat_failed),
    }

    if path_length_errors:
        result["path_length_errors"] = {
            path_display(path_b): msg.decode(errors="replace")
            for path_b, msg in path_length_errors.items()
        }
    if input_errors:
        result["input_errors"] = {
            path_display(path_b): msg.decode(errors="replace")
            for path_b, msg in input_errors.items()
        }
    if openat_failed:
        result["long_path_internal_errors"] = {
            path_display(path_b): msg.decode(errors="replace")
            for path_b, msg in openat_failed.items()
        }

    if fsync_barrier_per_batch(cfg) and (rsync_paths or internal_long_paths):
        # Objects, not bytes: rsync also creates directories, symlinks and
        # empty files, and those carry no bytes at all.
        sync_destination_filesystem(cfg, "rsync batch complete")

    result["verify"] = cfg.get("verify")
    if verification is not None:
        result["verify_status"] = verification.get("status")
        result["verify_checked"] = int(verification.get("checked", 0) or 0)
        result["verify_failed_count"] = int(
            verification.get("failed_count", 0) or 0
        )
        if verification.get("failed_files"):
            result["verify_failed_files"] = verification["failed_files"]

    # The count and the display list are two different things, and conflating
    # them lost failures here. The list is deduplicated on purpose - the same
    # path has the same reason and repeating it helps nobody - but the count
    # has to stay one per INPUT RECORD, or a path listed twice and failing
    # twice is reported as one failure and one transferred object.
    failed_records = 0
    failed_files = []
    if status == "failed" and not success:
        failed_records += len(rsync_input_paths)
        failed_files.extend(path_display(src_b) for src_b in rsync_input_paths)
    for quelle in (openat_failed, path_length_errors, input_errors):
        failed_records += count_failed_input_records(paths, quelle, cfg)
        failed_files.extend(path_display(path_b) for path_b in quelle)
    if failed_files:
        # Preserve order while avoiding duplicates.
        failed_files = list(dict.fromkeys(failed_files))
        result["failed_files"] = failed_files

    # rsync reports no per-file outcome, so the failed set is derived from the
    # names that went into the failing call. Without this field the run-level
    # accounting reads failed_count=0 from every rsync batch and counts a
    # batch that transferred nothing as a batch of successful objects.
    result["failed_count"] = failed_records

    return result

def split_long_path_for_cwd(
    path_b: bytes, limit: Optional[int] = None
) -> Tuple[bytes, bytes]:
    """
    Return (cwd_b, rel_b) so that both values are short enough for the
    traditional subprocess.run(..., cwd=cwd_b) path.

    If no safe split exists, return (None, None).  The caller must then use
    the fd/openat based helper path.
    """
    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)

    if limit is None:
        # Leave headroom below the system pathname limit for ./ prefixes and
        # implementation details in libc/subprocess.
        limit = max(256, get_system_path_max() - 128)

    if not path_b.startswith(b"/"):
        return os.getcwd().encode(), path_b

    parts = [p for p in path_b.split(b"/") if p]

    # Last component is the filename. cwd must be some parent directory.
    # Prefer the deepest possible cwd, because then the path passed to lfs is
    # shortest.  But cwd itself must still be short enough for chdir(cwd).
    for i in range(len(parts) - 1, -1, -1):
        cwd_b = b"/" + b"/".join(parts[:i])
        rel_b = b"/".join(parts[i:])

        if len(cwd_b) < limit and len(rel_b) < limit:
            return cwd_b, b"./" + rel_b

    return None, None


def _b64_encode_bytes(value: bytes) -> str:
    if isinstance(value, str):
        value = os.fsencode(value)
    return base64.b64encode(value).decode("ascii")


def _b64_decode_bytes(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def run_lfs_path_via_helper(
    args_before_path: List[bytes],
    path_b: bytes,
) -> subprocess.CompletedProcess:
    """
    PATH_MAX-safe fallback for lfs commands on extremely long paths.

    The parent process does not chdir/fchdir.  Instead it starts this same
    script in an internal helper mode and passes the long path through stdin.
    The helper opens the parent directory component-by-component with openat(),
    calls fchdir() in the helper process only, and then runs the requested lfs
    command with ./leafname.
    """
    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)

    args_b = [os.fsencode(a) if isinstance(a, str) else a for a in args_before_path]

    payload = {
        "args": [_b64_encode_bytes(a) for a in args_b],
        "path": _b64_encode_bytes(path_b),
        # The helper spawns the pinned program itself, and its own cache of
        # trusted executables is empty - it is a fresh process. pass_fds keeps
        # the numbers, so the parent's list is valid there.
        "fds": list(trusted_exec_fds()) + list(self_invocation_fds()),
    }
    payload_b = json.dumps(payload, separators=(",", ":")).encode("ascii")

    helper_cmd = self_invocation(LFS_HELPER_COMMAND)

    try:
        return subprocess.run(
            helper_cmd,
            input=payload_b,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # The payload carries /proc/self/fd/N as the program to run, so
            # the descriptor has to reach the helper process as well.
            pass_fds=tuple(trusted_exec_fds()) + self_invocation_fds(),
        )
    except Exception as e:
        return subprocess.CompletedProcess(
            helper_cmd,
            127,
            b"",
            f"lfs helper start failed: {e}".encode(errors="replace"),
        )


def lfs_helper_main() -> int:
    """
    Internal helper entry point.  This runs in a separate process.

    It intentionally may call os.fchdir(), because doing so here cannot change
    the cwd of the multi-threaded parent process.
    """
    try:
        payload_b = sys.stdin.buffer.read()
        payload = json.loads(payload_b.decode("ascii"))

        args_before_path = [_b64_decode_bytes(x) for x in payload["args"]]
        path_b = _b64_decode_bytes(payload["path"])
        inherited_fds = tuple(int(fd) for fd in payload.get("fds", ()))

        if not path_b.startswith(b"/"):
            raise ValueError("helper requires an absolute path")

        parent_b = os.path.dirname(path_b)
        leaf_b = os.path.basename(path_b)

        if not leaf_b or leaf_b in (b".", b"..") or b"/" in leaf_b:
            raise ValueError(f"invalid leaf name: {path_display(leaf_b)}")

        if not parent_b:
            parent_b = b"/"

        parent_fd = open_dir_fd_componentwise(parent_b, create=False)
        try:
            os.fchdir(parent_fd)
        finally:
            os.close(parent_fd)

        proc = subprocess.run(
            args_before_path + [b"./" + leaf_b],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Same reason as in the parent: args_before_path[0] is
            # /proc/self/fd/N, and close_fds would shut it before exec.
            pass_fds=inherited_fds,
        )

        if proc.stdout:
            sys.stdout.buffer.write(proc.stdout)
            sys.stdout.buffer.flush()
        if proc.stderr:
            sys.stderr.buffer.write(proc.stderr)
            sys.stderr.buffer.flush()

        return int(proc.returncode)

    except Exception as e:
        sys.stderr.buffer.write(
            f"lfs helper error: {e}\n".encode(errors="replace")
        )
        sys.stderr.buffer.flush()
        return 127


def run_lfs_path(
    args_before_path: List[bytes],
    path_b: bytes,
    allow_long_path_handling: bool = True,
) -> subprocess.CompletedProcess:
    """Run an lfs command, optionally enabling PATH_MAX-safe cwd/openat fallback.

    Normal paths are always passed directly to lfs. With
    allow_long_path_handling=True, only paths that reach the system PATH_MAX
    are rewritten through cwd/helper logic. With False, even overlong paths are
    passed directly so the native engine error is preserved in the JSON log.
    """
    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)

    args_b = [os.fsencode(a) if isinstance(a, str) else a for a in args_before_path]

    if not allow_long_path_handling or not path_needs_long_handling(path_b):
        try:
            return subprocess.run(
                args_b + [path_b],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                pass_fds=trusted_exec_fds(),
            )
        except OSError as e:
            return subprocess.CompletedProcess(
                args_b + [path_b],
                127,
                b"",
                f"lfs run failed before exec: {e}".encode(errors="replace"),
            )

    cwd_b, rel_b = split_long_path_for_cwd(path_b)

    if cwd_b is not None:
        try:
            return subprocess.run(
                args_before_path + [rel_b],
                cwd=cwd_b,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                pass_fds=trusted_exec_fds(),
            )
        except OSError as e:
            # If the supposedly safe cwd still fails because of a path-length
            # limit, retry through the fd/openat helper.  Other errors are
            # returned as a CompletedProcess to keep the old call contract.
            if e.errno == errno.ENAMETOOLONG:
                return run_lfs_path_via_helper(args_before_path, path_b)
            return subprocess.CompletedProcess(
                args_before_path + [rel_b],
                127,
                b"",
                f"lfs run failed before exec: {e}".encode(errors="replace"),
            )

    return run_lfs_path_via_helper(args_before_path, path_b)


_MOUNTINFO_ESCAPE_RE = re.compile(rb"\\([0-7]{3})")


def _decode_mountinfo_field(value: bytes) -> bytes:
    return _MOUNTINFO_ESCAPE_RE.sub(
        lambda match: bytes((int(match.group(1), 8),)),
        value,
    )


def read_mountinfo_entries() -> List[Tuple[bytes, bytes]]:
    """Return ``(mountpoint, filesystem_type)`` from this process namespace."""
    entries = []
    with open("/proc/self/mountinfo", "rb") as stream:
        for raw_line in stream:
            left, separator, right = raw_line.rstrip(b"\n").partition(b" - ")
            if not separator:
                continue
            left_fields = left.split()
            right_fields = right.split()
            if len(left_fields) < 5 or not right_fields:
                continue
            mountpoint = os.path.normpath(_decode_mountinfo_field(left_fields[4]))
            entries.append((mountpoint, right_fields[0]))
    return entries


def _path_is_below_or_equal(path_b: bytes, root_b: bytes) -> bool:
    try:
        rel_b = make_relative_bytes(path_b, root_b)
        validate_relative_path(rel_b, allow_root=True)
        return True
    except ValueError:
        return False


def find_mountinfo_entry(path_b: bytes) -> Tuple[bytes, bytes]:
    """Find the longest mountpoint containing one absolute pathname."""
    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)
    if not os.path.isabs(path_b):
        raise ValueError("mount lookup requires an absolute path")

    matches = [
        (mountpoint, fs_type)
        for mountpoint, fs_type in read_mountinfo_entries()
        if _path_is_below_or_equal(path_b, mountpoint)
    ]
    if not matches:
        raise ValueError(f"cannot determine mountpoint for {path_display(path_b)}")
    return max(matches, key=lambda entry: len(entry[0]))


_mountinfo_unavailable_warned = threading.Event()


def check_hardlink_backends(cfg: dict) -> None:
    """Refuse a hard_link mode whose declared backends are not what is mounted.

    The mode names both sides: "posix2lustre" is a POSIX source and a Lustre
    destination. The source half decides how the names of one inode are found
    - through the Lustre FID, or by scanning the subtree - and getting it
    wrong means either an 'lfs fid2path' that fails on every file or a slow
    scan where the FID was available. The destination half was never checked
    at all, so a mode could declare a Lustre destination and run against an
    overlay without a word.

    Checked once, at startup, against the configured roots. On a system
    without /proc/self/mountinfo nothing can be established; that warns
    instead of refusing, because it is a property of the platform rather than
    of this configuration.
    """
    mode = cfg.get("hard_link")
    if mode not in HARD_LINK_PRESERVE_MODES:
        return

    source_kind, _, destination_kind = mode.partition("2")
    roots = [("src_root", cfg.get("src_root"), source_kind)]
    # migrate operates on the source in place; there is no second tree, and
    # "lustre2lustre" says the same thing about it twice.
    if cfg.get("method") != "migrate":
        roots.append(("dst_root", cfg.get("dst_root"), destination_kind))

    for key, root, expected in roots:
        if not root:
            continue
        try:
            # realpath first: mountinfo lists real mount points, and matching a
            # lexical path against them answers for the wrong filesystem as
            # soon as any component is a symlink. Since a root reached through
            # one is allowed, that was a configuration refused for a mount it
            # is actually on.
            fs_type = find_mountinfo_entry(
                os.fsencode(os.path.realpath(root))
            )[1]
        except (OSError, ValueError) as exc:
            if not _mountinfo_unavailable_warned.is_set():
                _mountinfo_unavailable_warned.set()
                log(
                    f"WARNING: cannot determine the filesystem type of "
                    f"{key}: {exc}; hard_link={mode!r} is taken at its word"
                )
            continue
        is_lustre = fs_type == b"lustre"
        if (expected == "lustre") != is_lustre:
            raise ValueError(
                f"hard_link={mode!r} declares a {expected} {key}, but "
                f"{root} is mounted as "
                f"{fs_type.decode(errors='replace')}"
            )


def _lstat_for_hardlink(path_b: bytes, root_b: Optional[bytes] = None):
    """Inspect a hard-link name without following its final or relative parents."""
    if isinstance(path_b, str):
        path_b = os.fsencode(path_b)
    if root_b is not None:
        if isinstance(root_b, str):
            root_b = os.fsencode(root_b)
        rel_b = make_relative_bytes(path_b, root_b)
        # The root itself is an ordinary input record: 'find SRC -print0'
        # always emits SRC first. It is a directory and therefore never a
        # hard-link candidate, but rejecting it here would make every
        # hard_link mode fail on the very first record of the documented
        # workflow.
        validate_relative_path(rel_b, allow_root=True)
        if rel_b == b".":
            root_fd = open_dir_fd_componentwise(root_b, create=False)
            try:
                return os.fstat(root_fd)
            finally:
                os.close(root_fd)
        parent_fd = open_src_dir_fd_from_root(root_b, os.path.dirname(rel_b))
        try:
            return os.stat(
                os.path.basename(rel_b),
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
        finally:
            os.close(parent_fd)

    if path_needs_long_handling(path_b):
        return lstat_path_openat(path_b)
    return os.lstat(path_b)


def _source_path_for_hardlink(input_path_b: bytes, cfg: dict) -> bytes:
    """Normalize an input record to an absolute source path for inspection."""
    if isinstance(input_path_b, str):
        input_path_b = os.fsencode(input_path_b)

    if cfg.get("method") != "migrate":
        _, source_b, _ = normalize_input_path(input_path_b, cfg)
        return source_b

    if not os.path.isabs(input_path_b):
        raise ValueError(
            f"migrate input path must be absolute: {path_display(input_path_b)}"
        )

    source_b = input_path_b
    source_root = cfg.get("src_root")
    if source_root is not None:
        rel_b = make_relative_bytes(source_b, os.fsencode(source_root))
        validate_relative_path(rel_b, allow_root=True)
    return source_b


def _lustre_path_to_fid(path_b: bytes) -> bytes:
    proc = run_lfs_path(
        [lfs_executable(), b"path2fid"],
        path_b,
        allow_long_path_handling=True,
    )
    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or b"lfs path2fid failed").strip()
        raise RuntimeError(message.decode(errors="replace"))

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if len(lines) != 1 or any(byte in lines[0] for byte in b" \t\0"):
        raise RuntimeError(
            "lfs path2fid returned an ambiguous file identifier: "
            + proc.stdout.decode(errors="backslashreplace")
        )
    return lines[0]


def _run_lustre_fid2path(cmd: List[bytes]) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    try:
        return subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            # cmd[0] is lfs_executable(), which is /proc/self/fd/N wherever
            # /proc exists. Without the descriptor the name resolves to
            # nothing and exec fails with a bare ENOENT - which is what the
            # lustre2posix and lustre2lustre hard-link modes ran into.
            pass_fds=trusted_exec_fds(),
        )
    except OSError as exc:
        raise RuntimeError(f"cannot execute lfs fid2path: {exc}") from exc


def _normalize_lustre_fid_paths(
    paths: List[bytes],
    mountpoint_b: bytes,
) -> List[bytes]:
    result = []
    for path_b in paths:
        if not path_b:
            raise RuntimeError("lfs fid2path returned an empty pathname")
        if not os.path.isabs(path_b):
            path_b = os.path.join(mountpoint_b, path_b)
        result.append(os.path.normpath(path_b))
    return result


def _lustre_fid_to_paths(
    mountpoint_b: bytes,
    fid_b: bytes,
    expected_nlink: int,
) -> List[bytes]:
    """Resolve every Lustre hard-link name, including on 2.15 clients."""
    global lustre_fid2path_print0_supported

    expected_nlink = int(expected_nlink)
    if expected_nlink <= 0:
        raise RuntimeError(f"invalid Lustre hard-link count: {expected_nlink}")

    with lustre_fid2path_capability_lock:
        print0_supported = lustre_fid2path_print0_supported
        if print0_supported is None:
            cmd = [lfs_executable(), b"fid2path", b"-0", mountpoint_b, fid_b]
            proc = _run_lustre_fid2path(cmd)
            option_error = (proc.stderr or b"").lower()
            unsupported = any(
                marker in option_error
                for marker in (
                    b"invalid option",
                    b"unrecognized option",
                    b"unknown option",
                    b"illegal option",
                )
            )
            if proc.returncode == 0:
                if proc.stdout.endswith(b"\0"):
                    lustre_fid2path_print0_supported = True
                    paths = proc.stdout[:-1].split(b"\0")
                    return _normalize_lustre_fid_paths(paths, mountpoint_b)
                # Some older/vendor clients may not reject the unknown short
                # option cleanly. A non-NUL success is still safe to retry via
                # the unambiguous one-link-at-a-time interface.
                unsupported = True
            if not unsupported:
                message = (
                    proc.stderr
                    or proc.stdout
                    or b"lfs fid2path -0 failed"
                ).strip()
                raise RuntimeError(message.decode(errors="replace"))
            lustre_fid2path_print0_supported = False
            print0_supported = False

    if print0_supported:
        cmd = [lfs_executable(), b"fid2path", b"-0", mountpoint_b, fid_b]
        proc = _run_lustre_fid2path(cmd)
        if proc.returncode != 0:
            message = (proc.stderr or proc.stdout or b"lfs fid2path failed").strip()
            raise RuntimeError(message.decode(errors="replace"))
        if not proc.stdout or not proc.stdout.endswith(b"\0"):
            raise RuntimeError(
                "lfs fid2path -0 did not return a non-empty NUL-terminated path list"
            )
        return _normalize_lustre_fid_paths(
            proc.stdout[:-1].split(b"\0"),
            mountpoint_b,
        )

    # Lustre 2.15-compatible path: --link selects exactly one name. Remove
    # exactly the command's final newline rather than using splitlines(), since
    # a valid POSIX pathname may itself contain newline bytes.
    paths = []
    for link_number in range(expected_nlink):
        cmd = [
            lfs_executable(),
            b"fid2path",
            b"--link",
            str(link_number).encode("ascii"),
            mountpoint_b,
            fid_b,
        ]
        proc = _run_lustre_fid2path(cmd)
        if proc.returncode != 0:
            message = (proc.stderr or proc.stdout or b"lfs fid2path failed").strip()
            raise RuntimeError(
                f"lfs fid2path --link {link_number} failed: "
                f"{message.decode(errors='replace')}"
            )
        path_b = proc.stdout[:-1] if proc.stdout.endswith(b"\n") else proc.stdout
        if not path_b:
            raise RuntimeError(
                f"lfs fid2path --link {link_number} returned an empty pathname"
            )
        paths.append(path_b)

    return _normalize_lustre_fid_paths(paths, mountpoint_b)


def _build_resolved_hardlink_group(
    source_paths: List[bytes],
    source_root_b: bytes,
    expected_nlink: int,
    source_identity: Tuple[int, int],
    source_fingerprint: Tuple[int, int, int],
    source_backend: str,
    identity_key,
    identity_display: str,
    input_path_b: bytes,
) -> dict:
    paths = sorted(set(source_paths))
    if len(paths) != expected_nlink:
        raise RuntimeError(
            f"hard-link group is incomplete inside src_root: "
            f"st_nlink={expected_nlink}, resolved_paths={len(paths)}; "
            f"at least one name is outside the source tree or the namespace "
            f"changed during resolution"
        )

    primary_st = None
    for path_b in paths:
        rel_b = make_relative_bytes(path_b, source_root_b)
        validate_relative_path(rel_b, allow_root=False)

        st = _lstat_for_hardlink(path_b, source_root_b)
        if not stat.S_ISREG(st.st_mode):
            raise RuntimeError(
                f"resolved hard-link path is not a regular file: "
                f"{path_display(path_b)}"
            )
        identity = (int(st.st_dev), int(st.st_ino))
        if identity != source_identity:
            raise RuntimeError(
                f"resolved hard-link identity mismatch: {path_display(path_b)}; "
                f"expected dev={source_identity[0]}, ino={source_identity[1]}, "
                f"found dev={identity[0]}, ino={identity[1]}"
            )
        if int(st.st_nlink) != expected_nlink:
            raise RuntimeError(
                f"hard-link count changed during resolution: "
                f"{path_display(path_b)}; expected={expected_nlink}, "
                f"found={st.st_nlink}"
            )
        if path_b == paths[0]:
            primary_st = st

    if primary_st is None:
        raise RuntimeError("resolved hard-link group has no primary pathname")
    actual_fingerprint = (
        int(primary_st.st_size),
        int(primary_st.st_mtime_ns),
        int(primary_st.st_ctime_ns),
    )
    if actual_fingerprint != source_fingerprint:
        raise RuntimeError(
            f"hard-link source changed during group resolution: expected="
            f"{source_fingerprint!r}, found={actual_fingerprint!r}"
        )

    return {
        "identity_key": identity_key,
        "identity_display": identity_display,
        "source_backend": source_backend,
        "source_identity": source_identity,
        # st_dev/st_ino alone are insufficient on POSIX because an inode number
        # can be reused while a large normal phase is still running.
        "source_fingerprint": (
            int(source_fingerprint[0]),
            int(source_fingerprint[1]),
            int(source_fingerprint[2]),
        ),
        "source_primary": paths[0],
        "source_links": paths[1:],
        "expected_nlink": expected_nlink,
        # Preserve original stdin records (absolute or relative) for graceful
        # shutdown/restart. Cross-worker duplicates are merged by the scheduler.
        "input_paths": [input_path_b],
    }


def resolve_lustre_hardlink_group(
    source_b: bytes,
    source_root_b: bytes,
    source_st,
    input_path_b: bytes,
) -> dict:
    mountpoint_b, fs_type = find_mountinfo_entry(source_b)
    if fs_type != b"lustre":
        raise RuntimeError(
            f"hard_link source backend is Lustre, but {path_display(source_b)} "
            f"is mounted as {fs_type.decode(errors='replace')}"
        )

    fid_b = _lustre_path_to_fid(source_b)
    source_paths = _lustre_fid_to_paths(
        mountpoint_b,
        fid_b,
        int(source_st.st_nlink),
    )
    source_identity = (int(source_st.st_dev), int(source_st.st_ino))
    source_fingerprint = (
        int(source_st.st_size),
        int(source_st.st_mtime_ns),
        int(source_st.st_ctime_ns),
    )
    identity_key = ("lustre", mountpoint_b, fid_b)
    return _build_resolved_hardlink_group(
        source_paths,
        source_root_b,
        int(source_st.st_nlink),
        source_identity,
        source_fingerprint,
        "lustre",
        identity_key,
        f"lustre:{path_display(mountpoint_b)}:{os.fsdecode(fid_b)}",
        input_path_b,
    )


def _scan_posix_inode_paths(
    source_b: bytes,
    source_root_b: bytes,
    source_identity: Tuple[int, int],
    expected_nlink: int,
) -> List[bytes]:
    """Scan the relevant source subtree without following symlinks.

    The already inspected input name is the first known member. Once exactly
    st_nlink distinct names have been found, the scan can stop: no additional
    directory entry for this inode can exist without increasing st_nlink.
    """
    candidate_mount_b, _ = find_mountinfo_entry(source_b)
    if _path_is_below_or_equal(candidate_mount_b, source_root_b):
        scan_root_b = candidate_mount_b
    else:
        scan_root_b = source_root_b

    if not _path_is_below_or_equal(source_b, scan_root_b):
        raise RuntimeError("hard-link candidate is outside the selected scan root")

    all_mountpoints = {entry[0] for entry in read_mountinfo_entries()}
    root_fd = open_dir_fd_componentwise(scan_root_b, create=False)
    root_st = os.fstat(root_fd)
    if int(root_st.st_dev) != source_identity[0]:
        os.close(root_fd)
        raise RuntimeError("hard-link candidate and scan root are on different filesystems")

    # Depth-first with an explicit chain of open directory fds. Subdirectory
    # NAMES are collected while scanning and opened one at a time on descent,
    # so at most one fd per tree level is held instead of one fd per pending
    # sibling. The number of openat()/scandir() calls is unchanged; a directory
    # with more subdirectories than RLIMIT_NOFILE no longer exhausts fds.
    stack = [(root_fd, b"", None)]
    visited_dirs = set()
    matches = {source_b}
    child_flags = (
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    )

    def close_stack() -> None:
        while stack:
            pending_fd, _, _ = stack.pop()
            try:
                os.close(pending_fd)
            except OSError:
                pass

    try:
        while stack:
            directory_fd, rel_dir_b, pending_names = stack[-1]

            if pending_names is None:
                directory_st = os.fstat(directory_fd)
                directory_identity = (
                    int(directory_st.st_dev),
                    int(directory_st.st_ino),
                )
                if directory_identity in visited_dirs:
                    stack.pop()
                    os.close(directory_fd)
                    continue
                visited_dirs.add(directory_identity)

                subdirectory_names = []
                with os.scandir(directory_fd) as entries:
                    for entry in entries:
                        name_b = os.fsencode(entry.name)
                        if name_b in (b".", b".."):
                            continue

                        rel_b = (
                            os.path.join(rel_dir_b, name_b)
                            if rel_dir_b
                            else name_b
                        )
                        abs_b = os.path.join(scan_root_b, rel_b)
                        st = os.stat(
                            name_b,
                            dir_fd=directory_fd,
                            follow_symlinks=False,
                        )

                        if stat.S_ISDIR(st.st_mode):
                            if abs_b in all_mountpoints and abs_b != scan_root_b:
                                continue
                            if int(st.st_dev) != source_identity[0]:
                                continue
                            subdirectory_names.append(name_b)
                            continue

                        if (
                            stat.S_ISREG(st.st_mode)
                            and int(st.st_dev) == source_identity[0]
                            and int(st.st_ino) == source_identity[1]
                        ):
                            matches.add(abs_b)
                            if len(matches) == expected_nlink:
                                close_stack()
                                return sorted(matches)

                subdirectory_names.reverse()
                stack[-1] = (directory_fd, rel_dir_b, subdirectory_names)
                continue

            if not pending_names:
                stack.pop()
                os.close(directory_fd)
                continue

            name_b = pending_names.pop()
            rel_b = (
                os.path.join(rel_dir_b, name_b) if rel_dir_b else name_b
            )
            child_fd = os.open(name_b, child_flags, dir_fd=directory_fd)
            stack.append((child_fd, rel_b, None))

    except Exception:
        close_stack()
        raise

    return sorted(matches)


def resolve_posix_hardlink_group(
    source_b: bytes,
    source_root_b: bytes,
    source_st,
    input_path_b: bytes,
) -> dict:
    source_identity = (int(source_st.st_dev), int(source_st.st_ino))
    source_fingerprint = (
        int(source_st.st_size),
        int(source_st.st_mtime_ns),
        int(source_st.st_ctime_ns),
    )
    source_paths = _scan_posix_inode_paths(
        source_b,
        source_root_b,
        source_identity,
        int(source_st.st_nlink),
    )
    identity_key = ("posix", source_identity[0], source_identity[1])
    return _build_resolved_hardlink_group(
        source_paths,
        source_root_b,
        int(source_st.st_nlink),
        source_identity,
        source_fingerprint,
        "posix",
        identity_key,
        f"posix:{source_identity[0]}:{source_identity[1]}",
        input_path_b,
    )


def resolve_hardlink_group(
    source_b: bytes,
    source_st,
    input_path_b: bytes,
    cfg: dict,
) -> dict:
    mode = cfg.get("hard_link")
    source_backend = hard_link_source_backend(mode)
    source_root = cfg.get("src_root")
    if source_root is None:
        raise RuntimeError(
            f"hard_link={mode!r} requires an explicit src_root"
        )
    source_root_b = os.fsencode(source_root)

    if source_backend == "lustre":
        return resolve_lustre_hardlink_group(
            source_b,
            source_root_b,
            source_st,
            input_path_b,
        )
    return resolve_posix_hardlink_group(
        source_b,
        source_root_b,
        source_st,
        input_path_b,
    )


def partition_normal_batch_for_hardlinks(paths: List[bytes], cfg: dict) -> dict:
    """Inspect one normal batch and fully resolve every discovered group."""
    mode = cfg.get("hard_link")
    if mode is None:
        return {
            "normal_paths": list(paths),
            "groups": [],
            "errors": {},
            "candidate_count": 0,
            "prohibited_count": 0,
            "inspection_failed_count": 0,
            "resolution_failed_count": 0,
            "deferred_input_count": 0,
        }

    normal_paths = []
    groups = []
    errors = {}
    candidate_count = 0
    prohibited_count = 0
    inspection_failed_count = 0
    resolution_failed_count = 0
    local_groups = {}
    local_failures = {}
    deferred_input_count = 0

    for input_path_b in paths:
        if isinstance(input_path_b, str):
            input_path_b = os.fsencode(input_path_b)

        try:
            source_b = _source_path_for_hardlink(input_path_b, cfg)
        except Exception as exc:
            inspection_failed_count += 1
            errors[input_path_b] = (
                f"hard-link source normalization failed: {exc}"
            ).encode(errors="replace")
            continue

        # A configured path-length rejection is already a terminal method
        # error. Leave it to the ordinary method so its established JSON fields
        # and accounting remain unchanged.
        if path_length_policy_error(cfg, source_b) is not None:
            normal_paths.append(input_path_b)
            continue

        try:
            source_root = cfg.get("src_root")
            source_st = _lstat_for_hardlink(
                source_b,
                os.fsencode(source_root) if source_root is not None else None,
            )
        except Exception as exc:
            inspection_failed_count += 1
            errors[source_b] = (
                f"hard-link inspection failed: {exc}"
            ).encode(errors="replace")
            continue

        if not stat.S_ISREG(source_st.st_mode) or int(source_st.st_nlink) <= 1:
            normal_paths.append(input_path_b)
            continue

        candidate_count += 1
        if mode == "prohibited":
            prohibited_count += 1
            errors[source_b] = (
                f"hard links are prohibited: st_nlink={source_st.st_nlink}"
            ).encode("ascii")
            continue

        stat_key = (int(source_st.st_dev), int(source_st.st_ino))
        if stat_key in local_groups:
            group = local_groups[stat_key]
            # Every input record, repetitions included. This used to skip a
            # name already present, so a batch that deferred two records
            # handed on a group carrying one - and the run-level accounting
            # was then short by exactly the records that were dropped here.
            # A repeated name is the operator's business; losing track of it
            # is not.
            group["input_paths"].append(input_path_b)
            deferred_input_count += 1
            continue
        if stat_key in local_failures:
            resolution_failed_count += 1
            errors[source_b] = local_failures[stat_key]
            continue

        try:
            group = resolve_hardlink_group(
                source_b,
                source_st,
                input_path_b,
                cfg,
            )
        except Exception as exc:
            resolution_failed_count += 1
            message = (
                f"hard-link group resolution failed: {exc}"
            ).encode(errors="replace")
            local_failures[stat_key] = message
            errors[source_b] = message
            continue

        local_groups[stat_key] = group
        groups.append(group)
        deferred_input_count += 1

    return {
        "normal_paths": normal_paths,
        "groups": groups,
        "errors": errors,
        "candidate_count": candidate_count,
        "prohibited_count": prohibited_count,
        "inspection_failed_count": inspection_failed_count,
        "resolution_failed_count": resolution_failed_count,
        # Input records handed on to the hard-link phase, which reports their
        # outcome itself. This batch must not report them as transferred.
        "deferred_input_count": deferred_input_count,
    }


class MigrateSourceValidationError(ValueError):
    """Raised when a migrate pathname is not the expected regular file."""


def _migrate_source_identity(st) -> Tuple[int, int]:
    return int(st.st_dev), int(st.st_ino)


def validate_migrate_source(
    source_path: bytes,
    allow_long_path_handling: bool = True,
    expected_identity: Optional[Tuple[int, int]] = None,
):
    """Validate a migrate source without following its final symlink.

    The device/inode check detects a pathname that was replaced between
    attempts or between the read-only layout checks and a mirror operation.
    """
    if isinstance(source_path, str):
        source_path = os.fsencode(source_path)

    try:
        if allow_long_path_handling and path_needs_long_handling(source_path):
            st = lstat_path_openat(source_path)
        else:
            st = os.lstat(source_path)
    except Exception as exc:
        raise MigrateSourceValidationError(
            f"cannot inspect migrate source without following the final symlink: "
            f"{path_display(source_path)}: {exc}"
        ) from exc

    if stat.S_ISLNK(st.st_mode):
        raise MigrateSourceValidationError(
            f"migrate source is a symbolic link and will not be followed: "
            f"{path_display(source_path)}"
        )
    if not stat.S_ISREG(st.st_mode):
        raise MigrateSourceValidationError(
            f"migrate source is not a regular file: {path_display(source_path)}"
        )

    identity = _migrate_source_identity(st)
    if expected_identity is not None:
        try:
            expected = tuple(int(value) for value in expected_identity)
        except (TypeError, ValueError) as exc:
            raise MigrateSourceValidationError(
                f"invalid expected migrate source identity: "
                f"{expected_identity!r}"
            ) from exc
        if len(expected) != 2:
            raise MigrateSourceValidationError(
                f"invalid expected migrate source identity: "
                f"{expected_identity!r}"
            )
        if identity != expected:
            raise MigrateSourceValidationError(
                f"migrate source identity changed: {path_display(source_path)}; "
                f"expected dev={expected[0]}, ino={expected[1]}, "
                f"found dev={identity[0]}, ino={identity[1]}"
            )

    return st, identity


def process_migrate(
    src_b: bytes,
    cfg: dict,
    source_identity: Optional[Tuple[int, int]] = None,
) -> Tuple[int, bytes, bool, Optional[dict]]:
    """Run the descriptor-pinned C migration helper for one source inode.

    Python supplies the policy and the identity it first saw; the helper
    performs every Lustre layout check and mutation itself. The helper's own
    record is handed back so a failure can be logged with the facts needed to
    finish the file later.
    """
    return invoke_migrate_helper(
        src_b,
        cfg,
        inspect_only=False,
        source_identity=source_identity,
    )


def _configured_migrate_helper(cfg: dict) -> str:
    configured = cfg.get("migrate_helper")
    if configured:
        return configured

    sibling = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        MIGRATE_HELPER_BASENAME,
    )
    if os.path.isfile(sibling) and os.access(sibling, os.X_OK):
        return sibling

    from_path = shutil.which(MIGRATE_HELPER_BASENAME)
    if from_path:
        return os.path.realpath(from_path)
    raise FileNotFoundError(
        f"cannot find {MIGRATE_HELPER_BASENAME!r}; install it beside "
        "parallel_tools.py, put it in PATH, or set migrate_helper"
    )


_pinned_migrate_helper_fd = -1
_pinned_migrate_helper_path = None
_pinned_migrate_helper_lock = threading.Lock()


def pinned_migrate_helper(cfg: dict) -> Tuple[int, str]:
    """Open the helper ONCE and keep that descriptor for the whole run.

    The version probe used to open the helper, judge it and close it again,
    and every migration then opened the configured path anew. Between the two
    the name can be pointed at a different binary - and it was, in a test: the
    probe passed on 2.4, the symlink was moved to 2.1, and 2.1 ran and changed
    the file before its answer was rejected for its version. Checking a name
    and later executing that name is the exact race
    resolve_trusted_executable() exists to close; the helper had it anyway,
    because the descriptor was not kept.

    One descriptor, opened at start-up, used by the probe and by every
    invocation. The caller must NOT close it.
    """
    global _pinned_migrate_helper_fd, _pinned_migrate_helper_path
    with _pinned_migrate_helper_lock:
        if _pinned_migrate_helper_fd >= 0:
            return _pinned_migrate_helper_fd, _pinned_migrate_helper_path
        fd, path = _open_trusted_migrate_helper(cfg)
        _pinned_migrate_helper_fd = fd
        _pinned_migrate_helper_path = path
        return fd, path


def check_migrate_helper_version(cfg: dict) -> str:
    """Ask the helper who it is, before it is ever allowed to change a file.

    The answer of a migration carries program and program_version, but by then
    the helper has already run: an old or foreign one could alter the layout
    and only afterwards be rejected for its version, which leaves the file
    changed and the change unaccounted for. So it is asked once, up front,
    with --version - and through the SAME pinned descriptor that the real
    invocations use, so the thing that answered is the thing that runs.
    """
    try:
        helper_fd, helper_path = pinned_migrate_helper(cfg)
        # The same descriptor the real invocations use - not merely the same
        # construction. Without procfs the name has to be used,
        # and the name is used instead - the same concession
        # resolve_trusted_executable() already makes, and warns about once.
        by_descriptor = f"/proc/self/fd/{helper_fd}"
        if os.path.exists(by_descriptor):
            argv0, inherit = by_descriptor, (helper_fd,)
        else:
            argv0, inherit = helper_path, ()
        proc = subprocess.run(
            [argv0, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            pass_fds=inherit,
        )
    except Exception as exc:
        raise RuntimeError(
            f"cannot ask the migrate helper for its version: {exc}"
        ) from exc

    text = proc.stdout.decode("utf-8", errors="backslashreplace").strip()
    parts = text.split()
    if proc.returncode != 0 or len(parts) != 2:
        raise RuntimeError(
            f"the migrate helper did not answer --version as expected: "
            f"exit={proc.returncode} output={text!r}"
        )
    name, version_text = parts
    if name != MIGRATE_HELPER_PROGRAM_NAME:
        raise RuntimeError(
            f"the configured migrate helper calls itself {name!r}, not "
            f"{MIGRATE_HELPER_PROGRAM_NAME}"
        )
    version = _parse_helper_version(version_text)
    if version is None or version < MIGRATE_HELPER_MIN_VERSION:
        raise RuntimeError(
            f"migrate helper {version_text} is older than the required "
            f"{'.'.join(str(part) for part in MIGRATE_HELPER_MIN_VERSION)}; "
            f"deploy the helper that belongs to this parallel_tools"
        )
    return text


def _open_trusted_migrate_helper(cfg: dict) -> Tuple[int, str]:
    """Open and pin the helper executable before spawning it.

    Executing /proc/self/fd/N closes the stat/exec replacement race.  When the
    scheduler runs as root, a helper not owned by root is rejected; in all
    cases group- or world-writable executables are rejected.
    """
    # A packaged helper is often a symlink, so the name is resolved FIRST and
    # the resolved file is then pinned with O_NOFOLLOW. Opening the configured
    # name directly and waiting for ELOOP does not work: with O_PATH, Linux
    # answers O_NOFOLLOW by returning a descriptor for the symlink ITSELF
    # rather than failing, so the check below rejected every symlinked helper
    # as "not a regular file" and the fallback was never reached. On a system
    # without O_PATH the open degrades to O_RDONLY, where O_NOFOLLOW does
    # fail - which is why this only ever showed on the target platform.
    helper_path = os.path.realpath(_configured_migrate_helper(cfg))
    open_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        open_flags |= os.O_NOFOLLOW
    descriptor = os.open(helper_path, open_flags)

    try:
        helper_st = os.fstat(descriptor)
        if stat.S_ISLNK(helper_st.st_mode):
            # Only reachable if the resolved name was replaced by a symlink in
            # between. Refusing is the point of O_NOFOLLOW.
            raise PermissionError(
                "migrate helper became a symlink between resolving and "
                "opening it"
            )
        if not stat.S_ISREG(helper_st.st_mode):
            raise PermissionError("migrate helper is not a regular file")
        if helper_st.st_mode & 0o022:
            raise PermissionError(
                "migrate helper is group- or world-writable"
            )
        expected_owner = 0 if os.geteuid() == 0 else os.geteuid()
        if helper_st.st_uid != expected_owner:
            raise PermissionError(
                f"migrate helper owner uid={helper_st.st_uid}, "
                f"expected uid={expected_owner}"
            )
        if not helper_st.st_mode & 0o111:
            raise PermissionError("migrate helper is not executable")
        return descriptor, helper_path
    except Exception:
        os.close(descriptor)
        raise


def _parse_helper_version(text) -> Optional[Tuple[int, ...]]:
    """Read "2.1" into (2, 1). Anything unreadable is not tolerated."""
    if not isinstance(text, str) or not text.strip():
        return None
    parts = text.strip().split(".")
    try:
        return tuple(int(part) for part in parts)
    except ValueError:
        return None


def _migrate_helper_arguments(
    executable_b: bytes,
    src_b: bytes,
    cfg: dict,
    *,
    inspect_only: bool,
    source_identity: Optional[Tuple[int, int]],
) -> List[bytes]:
    root = cfg.get("src_root")
    if not root:
        # Cannot happen after validation. Refusing here rather than falling
        # back to "/" keeps a future change from widening the boundary in
        # silence.
        raise RuntimeError("migrate requires src_root; refusing to use '/'")
    arguments = [
        executable_b,
        # First, so an argument error further along still answers in JSON.
        b"--json",
        b"--root",
        os.fsencode(root),
        b"--stripe-count",
        str(cfg["stripcount"]).encode("ascii"),
        b"--allocation-attempts",
        str(cfg.get(
            "migrate_allocation_attempts",
            DEFAULT_MIGRATE_ALLOCATION_ATTEMPTS,
        )).encode("ascii"),
    ]
    pool_name = cfg.get("poolname")
    if pool_name is not None:
        arguments.extend([b"--pool-name", os.fsencode(pool_name)])
    for ost_index in cfg["banned_osts"]:
        arguments.extend([
            b"--banned-ost",
            str(ost_index).encode("ascii"),
        ])
    for ost_index in cfg.get("allowed_osts", ()):
        arguments.extend([
            b"--allowed-ost",
            str(ost_index).encode("ascii"),
        ])
    if cfg.get("keep_mirroring", False):
        arguments.append(b"--keep-mirroring")
    else:
        arguments.append(b"--collapse-to-one")
    if source_identity is not None:
        arguments.extend([
            b"--expected-dev",
            str(source_identity[0]).encode("ascii"),
            b"--expected-ino",
            str(source_identity[1]).encode("ascii"),
        ])
    if inspect_only:
        arguments.append(b"--inspect-only")
    arguments.extend([b"--", src_b])
    return arguments


def _parse_migrate_helper_record(
    stdout_b: bytes,
    stderr_b: bytes,
    process_returncode: int,
    src_b: bytes,
) -> dict:
    try:
        # The helper promises one ASCII-only line. Requiring exactly one JSON
        # value catches accidental liblustreapi chatter instead of silently
        # accepting a damaged machine-readable protocol.
        text = stdout_b.decode("ascii")
        lines = [line for line in text.splitlines() if line.strip()]
        if len(lines) != 1:
            raise ValueError(f"expected one JSON line, received {len(lines)}")
        record = json.loads(lines[0])
    except Exception as exc:
        stderr_text = stderr_b[:4096].decode("utf-8", errors="backslashreplace")
        raise RuntimeError(
            f"invalid migrate-helper response: {exc}; "
            f"process_returncode={process_returncode}; stderr={stderr_text!r}"
        ) from exc

    if not isinstance(record, dict):
        raise RuntimeError("migrate-helper response is not a JSON object")
    if record.get("schema") != MIGRATE_HELPER_PROTOCOL_SCHEMA:
        raise RuntimeError(
            f"unsupported migrate-helper schema {record.get('schema')!r}"
        )
    if record.get("program") != MIGRATE_HELPER_PROGRAM_NAME:
        raise RuntimeError(
            f"the program that answered is not {MIGRATE_HELPER_PROGRAM_NAME}: "
            f"it calls itself {record.get('program')!r}"
        )
    helper_version = _parse_helper_version(record.get("program_version"))
    if helper_version is None:
        raise RuntimeError(
            f"migrate-helper reports an unreadable version: "
            f"{record.get('program_version')!r}"
        )
    if helper_version < MIGRATE_HELPER_MIN_VERSION:
        raise RuntimeError(
            f"migrate-helper {record.get('program_version')} is older than the "
            f"required "
            f"{'.'.join(str(part) for part in MIGRATE_HELPER_MIN_VERSION)}; "
            f"the protocol schema matches but the behaviour does not. Deploy "
            f"the helper that belongs to this parallel_tools"
        )
    try:
        returned_path = base64.b64decode(
            record["path_b64"].encode("ascii"), validate=True
        )
    except Exception as exc:
        raise RuntimeError("invalid path_b64 in migrate-helper response") from exc
    if returned_path != src_b:
        raise RuntimeError("migrate-helper response belongs to a different path")
    helper_rc = record.get("rc")
    if isinstance(helper_rc, bool) or not isinstance(helper_rc, int):
        raise RuntimeError("migrate-helper rc is not an integer")
    status = record.get("status")
    if status not in ("ok", "error"):
        raise RuntimeError("migrate-helper status is invalid")
    if (helper_rc == 0) != (status == "ok"):
        raise RuntimeError("migrate-helper status and rc disagree")
    if helper_rc == 0 and process_returncode != 0:
        raise RuntimeError(
            "migrate-helper reported success but process exit status was nonzero"
        )
    if helper_rc != 0 and process_returncode == 0:
        raise RuntimeError(
            "migrate-helper reported an error but process exit status was zero"
        )
    if not isinstance(record.get("retryable"), bool):
        raise RuntimeError("migrate-helper retryable flag is not boolean")
    return record


def _stop_migrate_helper(proc: "subprocess.Popen") -> bool:
    """End a helper that must not keep running, and release its pipes.

    Returns True only when the process was actually reaped. Returning False
    means it survived SIGKILL, so it sits in an uninterruptible kernel call
    and will continue when that call returns - the caller must not treat the
    file as free.

    SIGTERM first, SIGKILL after the grace period. The helper works under a
    Lustre lease, which the kernel drops when its descriptor closes, so
    neither signal can leave the file locked. What it can leave behind is a
    half-changed layout - that is what the unfinished-migration report is for.

    Deliberately proc.wait() and not proc.communicate(): communicate waits for
    end-of-file on the pipes, and any process that inherited them keeps them
    open long after the helper itself is gone. Waiting for the child that was
    signalled is the question that has an answer.
    """
    stopped = False
    for stop in (proc.terminate, proc.kill):
        try:
            stop()
        except OSError:
            pass
        try:
            proc.wait(timeout=MIGRATE_HELPER_KILL_GRACE_SEC)
            stopped = True
            break
        except subprocess.TimeoutExpired:
            continue

    for pipe in (proc.stdout, proc.stderr):
        if pipe is not None:
            try:
                pipe.close()
            except OSError:
                pass

    return stopped


def _note_stopped_helper(
    proc: "subprocess.Popen",
    source_identity: Optional[Tuple[int, int]],
    reason: str,
) -> str:
    """Stop the helper and say what is known about it afterwards."""
    if _stop_migrate_helper(proc):
        return reason
    # It outlived SIGKILL. The file must not be handed to another helper in
    # this run, and it must not be retried either: a retry would find the
    # inode still claimed and requeue forever.
    if source_identity is not None:
        abandon_migrate_inode(source_identity, proc.pid)
    return (
        f"{reason}, but process {proc.pid} is still alive and may still be "
        f"changing this file; it is not retried in this run"
    )


def _await_migrate_helper(
    proc: "subprocess.Popen",
    cfg: dict,
    src_b: bytes,
    source_identity: Optional[Tuple[int, int]],
) -> Tuple[bytes, bytes, Optional[str]]:
    """Wait for the helper without becoming uninterruptible.

    Returns (stdout, stderr, abort_reason); on an abort the output is dropped,
    because the helper was stopped mid-sentence and its JSON record would be
    incomplete anyway.

    Terminating the helper is NOT the ordinary answer to a shutdown. The
    program promises that a stop signal lets the work already under way finish,
    and for a migration that promise is worth keeping: a helper cut short can
    leave the file with a mirror it does not want yet, and with
    keep_mirroring the file is left holding fewer mirrors than it should
    until a later run reads the goal the helper recorded and finishes it.
    Data is never at risk - the helper deletes a mirror only under a write
    lease and only while another usable one exists, and neither signal can
    tear that ioctl apart - but "not losing data" is a lower bar than "not
    leaving work half done".

    So the wait is broken into intervals and only ever cut short for a reason
    the operator stated: a configured migrate_timeout, or a second stop
    signal. Everything else it merely reports, so that a run which is not
    ending is at least explaining itself.
    """
    limit = cfg.get("migrate_timeout")
    deadline = None if not limit else time.monotonic() + float(limit)
    waiting_logged = False
    started = time.monotonic()

    while True:
        wait = MIGRATE_HELPER_POLL_SEC
        if deadline is not None:
            wait = min(wait, max(0.1, deadline - time.monotonic()))
        try:
            stdout_b, stderr_b = proc.communicate(timeout=wait)
            return stdout_b, stderr_b, None
        except subprocess.TimeoutExpired:
            pass

        if deadline is not None and time.monotonic() >= deadline:
            reason = (
                f"migrate helper exceeded migrate_timeout ({limit}s) and was "
                f"terminated"
            )
            return b"", b"", _note_stopped_helper(
                proc, source_identity, reason
            )

        if force_stop_event.is_set():
            return b"", b"", _note_stopped_helper(
                proc,
                source_identity,
                "migrate helper terminated: second stop signal received",
            )

        if not waiting_logged:
            reason = batch_pause_reason(cfg, "while the migrate helper ran")
            if reason is not None:
                # Once per helper run. A run that looks hung after a stop
                # signal should say which file it is still finishing and what
                # would end the wait, rather than leaving the operator to
                # guess.
                waiting_logged = True
                log(
                    f"{reason}: still finishing {path_display(src_b)} after "
                    f"{time.monotonic() - started:.0f}s; send the stop signal "
                    f"again to terminate the helper, or configure "
                    f"migrate_timeout"
                )


def invoke_migrate_helper(
    src_b: bytes,
    cfg: dict,
    *,
    inspect_only: bool,
    source_identity: Optional[Tuple[int, int]] = None,
) -> Tuple[int, bytes, bool, Optional[dict]]:
    """Invoke the pinned helper and validate its versioned JSON protocol."""
    if isinstance(src_b, str):
        src_b = os.fsencode(src_b)
    try:
        helper_fd, helper_path = pinned_migrate_helper(cfg)
        proc_path_b = f"/proc/self/fd/{helper_fd}".encode("ascii")
        arguments = _migrate_helper_arguments(
            proc_path_b,
            src_b,
            cfg,
            inspect_only=inspect_only,
            source_identity=source_identity,
        )
        proc = subprocess.Popen(
            arguments,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(helper_fd,),
        )
        stdout_b, stderr_b, abort_reason = _await_migrate_helper(
            proc, cfg, src_b, source_identity
        )
        if abort_reason is not None:
            # No record: what the helper had already done to the layout is
            # unknown, and claiming otherwise would let the
            # unfinished-migration report answer a question nobody can answer
            # here. An orphaned helper additionally makes the file permanently
            # out of reach for this run - retryable=False, because a retry
            # would only meet the claim that is never released.
            orphaned = (
                source_identity is not None
                and migrate_inode_abandoned(source_identity) is not None
            )
            return (
                3 if not orphaned else 1,
                abort_reason.encode("utf-8", errors="backslashreplace"),
                not orphaned,
                None,
            )
        record = _parse_migrate_helper_record(
            stdout_b, stderr_b, proc.returncode, src_b
        )
        message = str(record.get("message") or "")
        if stderr_b:
            stderr_text = stderr_b[:4096].decode(
                "utf-8", errors="backslashreplace"
            )
            message = f"{message}; helper stderr: {stderr_text}" if message else stderr_text
        return (
            int(record["rc"]),
            message.encode("utf-8", errors="backslashreplace"),
            bool(record["retryable"]),
            record,
        )
    except Exception as exc:
        return (
            -1,
            f"migrate helper failed: {exc}".encode(
                "utf-8", errors="backslashreplace"
            ),
            isinstance(exc, OSError) and exc.errno in (
                errno.EAGAIN,
                errno.EBUSY,
                errno.ESTALE,
                errno.EINTR,
                errno.ETIMEDOUT,
            ),
            None,
        )
    # The descriptor is NOT closed here: it belongs to the run, not to this
    # invocation, and closing it would reopen the very window this exists to
    # close.


def migrate_unfinished_report(
    failed_files: Dict[bytes, bytes],
    helper_records: Dict[bytes, Optional[dict]],
    requeued: Optional[Dict[bytes, bytes]] = None,
) -> dict:
    """Describe the files that did not finish, so a later run can pick them up.

    ``requeued`` holds the paths that were put back on the queue when the run
    was stopped. They belong here even though they are not failures: a stop is
    exactly when a half-migrated file is most likely, and reporting only the
    permanent failures meant that the interesting case - a helper cut short by
    a second stop signal - appeared in the record as nothing but a bump in
    requeued_count. They are marked ``requeued``, so a reader can tell the
    files a later run will pick up on its own from the ones that need a
    decision. They are deliberately NOT added to failed_files: they were not
    failures, and counting them there would move the path accounting and the
    exit status.

    The distinction that matters is ``changed``: the helper reports it true
    when it already altered the layout - a mirror was added, or one was
    deleted - and false when the file is untouched. A file with changed=true
    is half migrated. It is not damaged: every step of this program is
    verified before the next one, and the deletion only happens under a write
    lease with the layout read again. But it is not finished either, and
    nothing else will notice.

    ``path_b64`` carries the pathname byte for byte, which the human-readable
    failed_files entries cannot: they are escaped for display and a name that
    is not valid UTF-8 does not survive them.
    """
    sources = [(src_b, message, False) for src_b, message in failed_files.items()]
    sources.extend(
        (src_b, message, True)
        for src_b, message in (requeued or {}).items()
        if src_b not in failed_files
    )

    unfinished = []
    for src_b, message, was_requeued in sources:
        record = helper_records.get(src_b)
        entry = {
            "path_b64": base64.b64encode(src_b).decode("ascii"),
            "message": message.decode(errors="replace"),
            # True: already back on the queue, so a later run reaches it
            # without anyone listing it by hand.
            "requeued": was_requeued,
        }
        if isinstance(record, dict):
            entry.update({
                "layout_changed": bool(record.get("changed")),
                "stage": record.get("stage"),
                "errno": record.get("errno"),
                "retryable": record.get("retryable"),
                "initial_mirror_count": record.get("initial_mirror_count"),
                "mirror_count_after": record.get("mirror_count_after"),
                "banned_mirrors_after": record.get("banned_mirrors_after"),
                "mirrors_added": record.get("mirrors_added"),
                "mirrors_deleted": record.get("mirrors_deleted"),
            })
        else:
            # No record at all: the helper could not be started or its answer
            # was unusable. Nothing is known about the layout, so nothing is
            # claimed about it.
            entry["layout_changed"] = None
        unfinished.append(entry)

    if not unfinished:
        return {}

    touched = sum(1 for entry in unfinished if entry.get("layout_changed"))
    return {
        "migrate_unfinished": unfinished,
        "migrate_unfinished_count": len(unfinished),
        "migrate_layout_changed_count": touched,
        "migrate_unfinished_requeued_count": sum(
            1 for entry in unfinished if entry.get("requeued")
        ),
    }


def process_migrate_batch(paths: List[bytes], cfg: dict) -> dict:
    retries = int(cfg["max_retries"])
    backoff_base = float(cfg["retry_backoff_base"])
    backoff_max = float(cfg["retry_backoff_max"])

    remaining = list(paths)
    failed_files = {}
    next_round = None
    # Pin each input pathname to the regular file seen on its first attempt.
    # This identity is reused for retries and every later mirror operation.
    source_identities: Dict[bytes, Tuple[int, int]] = {}
    # The helper's last record for every path that did not finish. It says
    # whether the layout was already touched, which is what separates "nothing
    # happened" from "half migrated".
    helper_records: Dict[bytes, Optional[dict]] = {}
    # Paths put back on the queue by a stop while the helper had already
    # touched them. Reported as unfinished, but not as failures.
    requeued_unfinished: Dict[bytes, bytes] = {}
    # Files the helper finished, but whose layout stays pending. Not failures:
    # the run did what it was asked. Still worth naming.
    pending_layout_files: Dict[bytes, bytes] = {}
    configured_source_identities = cfg.get("_expected_source_identities") or {}
    total = 0
    requeued_count = 0
    path_length_error_count = 0
    stopped = False

    t0 = time.time()

    for attempt in range(1, retries + 1):
        next_round = {}

        for idx, src_b in enumerate(remaining):
            # Shutdown semantics: complete the current file migration, but do
            # not start another file from the same dequeued batch. Requeue the
            # rest so save_remaining_tasks_to_file() persists them.
            reason = batch_pause_reason(cfg, "during migrate batch")
            if reason is not None:
                # Only the ones the helper actually ran on: a path that is in
                # next_round because another hard-link name of the same inode
                # was busy has not been touched at all.
                requeued_unfinished.update({
                    path_b: message
                    for path_b, message in next_round.items()
                    if path_b in helper_records
                })
                requeue_list = list(next_round.keys()) + remaining[idx:]
                requeued_count += requeue_unprocessed_paths(requeue_list, reason)
                stopped = True
                break

            policy_error = path_length_policy_error(cfg, src_b)
            if policy_error is not None:
                failed_files[src_b] = policy_error
                path_length_error_count += 1
                continue

            # Validate on every retry. A regular file accepted on the first
            # round must not later turn into a symlink or a different inode.
            try:
                st, source_identity = validate_migrate_source(
                    src_b,
                    allow_long_path_handling=(
                        cfg.get("max_path_length", "engine") == "engine"
                    ),
                    expected_identity=source_identities.get(
                        src_b,
                        configured_source_identities.get(src_b),
                    ),
                )
            except MigrateSourceValidationError as exc:
                failed_files[src_b] = str(exc).encode(errors="replace")
                continue

            if src_b not in source_identities:
                source_identities[src_b] = source_identity
            if attempt == 1:
                total += st.st_size

            identity = source_identities[src_b]
            if not acquire_migrate_inode(identity):
                # Another worker holds a different name of the same inode.
                # Retry later instead of running concurrent mirror operations
                # on one file.
                next_round[src_b] = (
                    b"another hard-link name of the same inode is currently "
                    b"being migrated"
                )
                continue

            record = None
            try:
                # One helper invocation per attempt. keep_mirroring used to
                # cost two: an --inspect-only run to learn the mirror count,
                # and then the work with that number pinned as
                # --target-mirror-count, because the helper derived its target
                # from the state it found and after a crash that was the
                # reduced one. The helper now records the number on the file
                # before it changes anything and reads it back itself, so the
                # detour through this process - and the chance of the number
                # going wrong on the way - is gone.
                rc, err, retryable, record = process_migrate(
                    src_b,
                    cfg,
                    source_identity=identity,
                )
            finally:
                release_migrate_inode(identity)

            if rc != 0:
                # Remember the helper's own account of the attempt. A file
                # whose layout was already changed needs a later run to finish
                # it, and that run has to be able to find it again.
                helper_records[src_b] = record
                if retryable:
                    next_round[src_b] = err
                else:
                    failed_files[src_b] = err
            else:
                helper_records.pop(src_b, None)
                if isinstance(record, dict) and record.get("pending_layout"):
                    # The migration finished, but the file stays write- or
                    # sync-pending because the only components behind are
                    # NOSYNC ones, which a resync skips. Reporting nothing but
                    # "ok" would hide which files those are - and they are the
                    # ones whose redundancy is not what the layout claims.
                    pending_layout_files[src_b] = (
                        record.get("message") or "layout stays write/sync pending"
                    ).encode(errors="replace")

        if stopped:
            break

        if next_round:
            reason = batch_pause_reason(cfg, "before migrate retry round")
            if reason is not None:
                requeued_unfinished.update({
                    path_b: message
                    for path_b, message in next_round.items()
                    if path_b in helper_records
                })
                requeued_count += requeue_unprocessed_paths(list(next_round.keys()), reason)
                stopped = True
                break

            remaining = list(next_round.keys())
            backoff_sleep(attempt, backoff_base, backoff_max)
        else:
            break

    duration = max(time.time() - t0, 0.001)
    bytes_per_sec = total / duration

    if not stopped and next_round is not None and next_round:
        failed_files.update(next_round)

    return {
        "status": "stopped" if stopped else ("ok" if not failed_files else "failed"),
        "message": "stopped; unprocessed files requeued" if stopped else ("" if not failed_files else "errors"),
        "method": "migrate",
        "count": len(paths),
        "bytes_total": total,
        "duration_sec": round(duration, 3),
        "bytes_per_sec": round(bytes_per_sec, 2),
        "failed_count": count_failed_input_records(
            paths, failed_files, cfg
        ),
        "failed_files": {path_display(src_b): msg.decode(errors="replace") for src_b, msg in failed_files.items()},
        "requeued_count": requeued_count,
        "path_length_error_count": path_length_error_count,
        "migrate_pending_layout_count": len(pending_layout_files),
        "migrate_pending_layout": [
            {
                "path_b64": base64.b64encode(path_b).decode("ascii"),
                "message": message.decode(errors="replace"),
            }
            for path_b, message in pending_layout_files.items()
        ],
        **migrate_unfinished_report(
            failed_files, helper_records, requeued_unfinished
        ),
    }

def diff_one_openat(
    src_b: bytes,
    dst_b: bytes,
    cfg: dict,
    compare_mode: str = "blake3",
    parent_fd_cache: Optional[DiffParentDirFdCache] = None,
) -> Tuple[int, bytes]:
    try:
        src_root_b = os.fsencode(cfg["src_root"])
        dst_root_b = os.fsencode(cfg["dst_root"])

        rel_b = make_relative_bytes(src_b, src_root_b)
        validate_relative_path(rel_b, allow_root=True)

        rel_parent_b = os.path.dirname(rel_b)
        leaf_b = os.path.basename(rel_b)

        if not leaf_b or leaf_b == b".":
            return 0, b"directory ok"

        if leaf_b == b"..":
            return -1, b"invalid relative file name for diff"

        src_parent_fd = None
        dst_parent_fd = None
        owns_parent_fds = parent_fd_cache is None
        try:
            if parent_fd_cache is None:
                src_parent_fd = open_src_dir_fd_from_root(
                    src_root_b, rel_parent_b
                )
                dst_parent_fd = open_src_dir_fd_from_root(
                    dst_root_b, rel_parent_b
                )
            else:
                src_parent_fd, dst_parent_fd = parent_fd_cache.get(rel_parent_b)
            src_st = os.lstat(leaf_b, dir_fd=src_parent_fd)
            dst_st = os.lstat(leaf_b, dir_fd=dst_parent_fd)

            if stat.S_IFMT(src_st.st_mode) != stat.S_IFMT(dst_st.st_mode):
                return -1, b"type mismatch"

            if stat.S_ISDIR(src_st.st_mode) and cfg.get("_inline_verification"):
                return 0, b"directory ok (metadata not checked inline)"

            metadata_error = compare_metadata(src_st, dst_st, cfg)
            if metadata_error is not None:
                return -1, metadata_error

            if cfg.get("xattr", False):
                xattr_error = compare_xattrs_at(
                    src_parent_fd,
                    dst_parent_fd,
                    leaf_b,
                    follow_symlinks=not stat.S_ISLNK(src_st.st_mode),
                    src_st=src_st,
                    dst_st=dst_st,
                )
                if xattr_error is not None:
                    return -1, xattr_error

            if stat.S_ISDIR(src_st.st_mode):
                return 0, b"directory ok"

            if stat.S_ISLNK(src_st.st_mode):
                src_target = os.readlink(leaf_b, dir_fd=src_parent_fd)
                dst_target = os.readlink(leaf_b, dir_fd=dst_parent_fd)
                if src_target != dst_target:
                    return -1, b"symlink target mismatch"
                return 0, b"symlink ok"

            if not stat.S_ISREG(src_st.st_mode):
                return -1, b"unsupported file type"

            if compare_mode is None:
                return 0, b"metadata ok"

            if src_st.st_size != dst_st.st_size:
                return -1, b"size mismatch"

            if compare_mode == "size":
                return 0, b"size ok"

            if is_hash_compare_mode(compare_mode):
                src_fd = None
                dst_fd = None
                try:
                    # O_NOFOLLOW and an identity check, the same pair the
                    # copy engine makes. Without them a name swapped between
                    # the lstat above and this open is hashed instead, and a
                    # "sha256 ok" is then a statement about two files that
                    # were never the ones named. A comparison that can be
                    # made to agree is worse than none.
                    src_fd = open_checked_entry_at(
                        src_parent_fd, leaf_b, src_st
                    )
                    dst_fd = open_checked_entry_at(
                        dst_parent_fd, leaf_b, dst_st
                    )
                    src_hash, dst_hash = hash_pair_parallel_fds(
                        src_fd,
                        dst_fd,
                        compare_mode,
                        cfg.get("buffer_mb"),
                    )
                finally:
                    if src_fd is not None:
                        try:
                            os.close(src_fd)
                        except OSError:
                            pass
                    if dst_fd is not None:
                        try:
                            os.close(dst_fd)
                        except OSError:
                            pass

                if src_hash != dst_hash:
                    return -1, f"{compare_mode} mismatch".encode()
                return 0, f"{compare_mode} ok".encode()

            return -1, f"unknown diff mode: {compare_mode}".encode()

        finally:
            if owns_parent_fds and src_parent_fd is not None:
                try:
                    os.close(src_parent_fd)
                except OSError:
                    pass
            if owns_parent_fds and dst_parent_fd is not None:
                try:
                    os.close(dst_parent_fd)
                except OSError:
                    pass

    except FileNotFoundError as e:
        return -1, f"missing file: {e}".encode()
    except Exception as e:
        return -1, f"openat diff failed: {e}".encode()

def process_diff_batch(paths: List[bytes], cfg: dict) -> dict:
    parent_fd_cache = DiffParentDirFdCache(
        os.fsencode(cfg["src_root"]), os.fsencode(cfg["dst_root"])
    )
    try:
        return _process_diff_batch_cached(paths, cfg, parent_fd_cache)
    finally:
        parent_fd_cache.close()


def _process_diff_batch_cached(
    paths: List[bytes], cfg: dict, parent_fd_cache: DiffParentDirFdCache,
) -> dict:
    # Validated by load_config_file(); diff never runs without a compare mode.
    compare_mode = cfg["verify"]

    failed_files = {}
    checked = 0
    total = 0
    path_length_error_count = 0
    t0 = time.time()

    engine_controls_path_length = cfg.get("max_path_length", "engine") == "engine"

    requeued_count = 0
    stopped = False

    for idx, src_b in enumerate(paths):
        # diff previously ran a dequeued batch to completion regardless of
        # SIGTERM or the configured run schedule.
        reason = batch_pause_reason(cfg, "during diff batch")
        if reason is not None:
            requeued_count += requeue_unprocessed_paths(list(paths[idx:]), reason)
            stopped = True
            break

        try:
            rel_b, src_abs_b, dst_b = normalize_input_path(src_b, cfg)

            policy_error = path_length_policy_error(cfg, src_abs_b, dst_b)
            if policy_error is not None:
                failed_files[src_abs_b] = policy_error
                path_length_error_count += 1
                continue

            technical_long = path_needs_long_handling(src_abs_b, dst_b)

            # Count the logical source bytes using the same pathname policy as
            # the actual comparison. Numeric max_path_length deliberately uses
            # classic syscalls even when PATH_MAX would be exceeded.
            try:
                if engine_controls_path_length and technical_long:
                    src_st = lstat_path_openat(src_abs_b)
                else:
                    src_st = os.lstat(src_abs_b)
                if stat.S_ISREG(src_st.st_mode):
                    total += src_st.st_size
            except Exception:
                pass

            if technical_long and not engine_controls_path_length:
                # A numeric max_path_length says the engine takes paths of
                # that length and this run should behave like it does. Classic
                # syscalls cannot address a path beyond PATH_MAX, so this one
                # fails - as it did before, only it used to fail by handing
                # the long name to lstat and reporting whatever errno came
                # back. Said plainly instead.
                rc, msg = -1, (
                    b"path exceeds PATH_MAX and max_path_length is numeric, "
                    b"so classic syscalls were used as configured"
                )
            else:
                # One implementation for every path length. The second one,
                # diff_one_path(), resolved whole pathnames a second time:
                # a directory component replaced by a symlink between the
                # listing and the comparison was followed out of src_root, and
                # the file out there was reported as "sha256 ok". Reproduced.
                # A comparison whose answer can be arranged from outside the
                # tree is worse than no comparison, and there was no reason
                # for diff to be less careful than copy, which has walked
                # componentwise all along.
                rc, msg = diff_one_openat(
                    src_abs_b, dst_b, cfg, compare_mode,
                    parent_fd_cache=parent_fd_cache,
                )

            if rc != 0:
                failed_files[src_abs_b] = msg
            else:
                checked += 1

        except Exception as e:
            failed_files[src_b] = str(e).encode()

    duration = max(time.time() - t0, 0.001)

    return {
        "status": (
            "stopped" if stopped else ("ok" if not failed_files else "failed")
        ),
        "message": "stopped; unprocessed files requeued" if stopped else "",
        "method": "diff",
        "count": len(paths),
        "checked": checked,
        "bytes_total": total,
        "verify": compare_mode,
        "duration_sec": round(duration, 3),
        "failed_count": count_failed_input_records(
            paths, failed_files, cfg
        ),
        "requeued_count": requeued_count,
        "path_length_error_count": path_length_error_count,
        "failed_files": {
            path_display(path_b): msg.decode(errors="replace")
            for path_b, msg in failed_files.items()
        },
    }


def validate_resolved_hardlink_group(
    group: dict,
    cfg: dict,
    *,
    check_fingerprint: bool = True,
):
    """Revalidate a previously resolved group without repeating tree search."""
    resolution_error = group.get("resolution_error")
    if resolution_error:
        raise RuntimeError(resolution_error)

    mode = cfg.get("hard_link")
    if not hard_link_preserves_groups(mode):
        raise RuntimeError(
            f"resolved hard-link task cannot run with hard_link={mode!r}"
        )

    expected_backend = hard_link_source_backend(mode)
    if group.get("source_backend") != expected_backend:
        raise RuntimeError(
            f"hard-link source backend changed: expected={expected_backend}, "
            f"resolved={group.get('source_backend')}"
        )

    source_root_b = os.fsencode(cfg["src_root"])
    source_paths = hardlink_group_source_paths(group)
    expected_nlink = int(group.get("expected_nlink", 0))
    if expected_nlink <= 1 or len(source_paths) != expected_nlink:
        raise RuntimeError(
            f"invalid resolved hard-link group: expected_nlink={expected_nlink}, "
            f"paths={len(source_paths)}"
        )

    try:
        expected_identity = tuple(
            int(value) for value in group["source_identity"]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("resolved hard-link group has no valid source identity") from exc
    if len(expected_identity) != 2:
        raise RuntimeError("resolved hard-link group has no valid source identity")
    primary_st = None
    for source_b in source_paths:
        rel_b = make_relative_bytes(source_b, source_root_b)
        validate_relative_path(rel_b, allow_root=False)
        st = _lstat_for_hardlink(source_b, source_root_b)
        if not stat.S_ISREG(st.st_mode):
            raise RuntimeError(
                f"hard-link source is no longer a regular file: "
                f"{path_display(source_b)}"
            )
        identity = (int(st.st_dev), int(st.st_ino))
        if identity != expected_identity:
            raise RuntimeError(
                f"hard-link source identity changed: {path_display(source_b)}; "
                f"expected dev={expected_identity[0]}, ino={expected_identity[1]}, "
                f"found dev={identity[0]}, ino={identity[1]}"
            )
        if int(st.st_nlink) != expected_nlink:
            raise RuntimeError(
                f"hard-link count changed before processing: "
                f"{path_display(source_b)}; expected={expected_nlink}, "
                f"found={st.st_nlink}"
            )
        if source_b == group.get("source_primary"):
            primary_st = st

    if expected_backend == "lustre":
        current_fid = _lustre_path_to_fid(group["source_primary"])
        identity_key = group.get("identity_key")
        expected_fid = (
            identity_key[2]
            if isinstance(identity_key, (tuple, list)) and len(identity_key) >= 3
            else None
        )
        if not isinstance(expected_fid, bytes) or not expected_fid:
            raise RuntimeError("resolved Lustre hard-link group has no valid FID")
        if current_fid != expected_fid:
            raise RuntimeError(
                f"Lustre FID changed before processing: expected="
                f"{os.fsdecode(expected_fid or b'')}, "
                f"found={os.fsdecode(current_fid)}"
            )

    if primary_st is None:
        raise RuntimeError("resolved hard-link group has no primary pathname")
    if check_fingerprint:
        try:
            expected_fingerprint = tuple(
                int(value) for value in group["source_fingerprint"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "resolved hard-link group has no valid source fingerprint"
            ) from exc
        actual_fingerprint = (
            int(primary_st.st_size),
            int(primary_st.st_mtime_ns),
            int(primary_st.st_ctime_ns),
        )
        if len(expected_fingerprint) != 3 or actual_fingerprint != expected_fingerprint:
            raise RuntimeError(
                f"hard-link source changed after group resolution: expected="
                f"{expected_fingerprint!r}, found={actual_fingerprint!r}"
            )
    return source_paths, primary_st


def build_destination_hardlink_task(group: dict, cfg: dict) -> dict:
    source_root_b = os.fsencode(cfg["src_root"])
    destination_root_b = os.fsencode(cfg["dst_root"])
    source_paths = hardlink_group_source_paths(group)
    destination_paths = [
        make_destination(source_b, source_root_b, destination_root_b)
        for source_b in source_paths
    ]

    if len(set(destination_paths)) != len(destination_paths):
        raise RuntimeError("multiple hard-link source names map to one destination path")

    return {
        "source_primary": source_paths[0],
        "source_links": source_paths[1:],
        "destination_primary": destination_paths[0],
        "destination_links": destination_paths[1:],
        "destination_root": destination_root_b,
        "expected_nlink": int(group["expected_nlink"]),
    }


def new_temporary_leaf_name(tag: str) -> bytes:
    """A short, collision-resistant name for a temporary destination entry."""
    return (
        f".parallel_tools.{tag}.{os.getpid()}."
        f"{threading.get_ident()}.{secrets.token_hex(8)}"
    ).encode("ascii")


def compare_source_against_fd(
    src_fd: int,
    dst_fd: int,
    cfg: dict,
    compare_mode: Optional[str],
) -> Tuple[int, bytes]:
    """Compare a source file against an already-open destination descriptor.

    The descriptor is what the external engine wrote into, and comparing
    through it rather than through the temporary name is the point: a name can
    be pointed at a different file between the check and the read, and then
    the verification would confirm something nobody wrote.
    """
    try:
        src_st = os.fstat(src_fd)
        dst_st = os.fstat(dst_fd)
        if src_st.st_size != dst_st.st_size:
            return -1, (
                f"size mismatch: {src_st.st_size} != {dst_st.st_size}"
            ).encode()
        if compare_mode is None or compare_mode == "size":
            return 0, b"size ok"
        if is_hash_compare_mode(compare_mode):
            os.lseek(src_fd, 0, os.SEEK_SET)
            os.lseek(dst_fd, 0, os.SEEK_SET)
            src_hash, dst_hash = hash_pair_parallel_fds(
                src_fd, dst_fd, compare_mode, cfg.get("buffer_mb")
            )
            if src_hash != dst_hash:
                return -1, f"{compare_mode} mismatch".encode()
            return 0, f"{compare_mode} ok".encode()
        return -1, f"unsupported compare mode: {compare_mode}".encode()
    except (OSError, RuntimeError, ValueError) as exc:
        return -1, str(exc).encode()


def compare_source_against_file(
    src_b: bytes,
    other_b: bytes,
    cfg: dict,
    compare_mode: Optional[str],
    long_path: bool,
) -> Tuple[int, bytes]:
    """Compare a source file with an arbitrary other pathname.

    Needed because the external engine writes to a temporary name: the normal
    diff helpers derive the destination from src_root/dst_root and would
    compare against the final name, which does not hold the data yet.
    """
    if not long_path:
        return compare_files(src_b, other_b, compare_mode, cfg.get("buffer_mb"))

    src_fd = None
    dst_fd = None
    try:
        src_fd = open_file_fd_componentwise(src_b, os.O_RDONLY)
        dst_fd = open_file_fd_componentwise(other_b, os.O_RDONLY)
        src_st = os.fstat(src_fd)
        dst_st = os.fstat(dst_fd)
        if src_st.st_size != dst_st.st_size:
            return -1, b"size mismatch"
        if compare_mode is None or compare_mode == "size":
            return 0, b"size ok"
        if is_hash_compare_mode(compare_mode):
            src_hash, dst_hash = hash_pair_parallel_fds(
                src_fd, dst_fd, compare_mode, cfg.get("buffer_mb")
            )
            if src_hash != dst_hash:
                return -1, f"{compare_mode} mismatch".encode()
            return 0, f"{compare_mode} ok".encode()
        return -1, f"unsupported compare mode: {compare_mode}".encode()
    except OSError as exc:
        return -1, str(exc).encode()
    finally:
        for descriptor in (src_fd, dst_fd):
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _new_hardlink_tmp_name() -> bytes:
    return (
        f".parallel_tools.hardlink.{os.getpid()}."
        f"{threading.get_ident()}.{secrets.token_hex(8)}"
    ).encode("ascii")


def create_or_replace_destination_hardlink(
    source_primary_b: bytes,
    source_link_b: bytes,
    destination_primary_b: bytes,
    destination_link_b: bytes,
    cfg: dict,
) -> None:
    """Atomically make one destination name reference the primary inode."""
    source_root_b = os.fsencode(cfg["src_root"])
    destination_root_b = os.fsencode(cfg["dst_root"])
    metadata_cfg = cfg
    copy_xattr = bool(cfg.get("xattr", False))

    primary_rel_b = make_relative_bytes(source_primary_b, source_root_b)
    link_rel_b = make_relative_bytes(source_link_b, source_root_b)
    validate_relative_path(primary_rel_b, allow_root=False)
    validate_relative_path(link_rel_b, allow_root=False)
    if (
        make_destination(source_primary_b, source_root_b, destination_root_b)
        != destination_primary_b
        or make_destination(source_link_b, source_root_b, destination_root_b)
        != destination_link_b
    ):
        raise RuntimeError("destination hard-link mapping changed unexpectedly")

    primary_parent_fd = None
    link_parent_fd = None
    primary_leaf_b = os.path.basename(primary_rel_b)
    link_leaf_b = os.path.basename(link_rel_b)
    tmp_b = None
    tmp_exists = False

    try:
        primary_parent_fd = open_dst_dir_fd_with_attrs(
            source_root_b,
            destination_root_b,
            os.path.dirname(primary_rel_b),
            copy_xattr,
            metadata_cfg,
        )
        link_parent_fd = open_dst_dir_fd_with_attrs(
            source_root_b,
            destination_root_b,
            os.path.dirname(link_rel_b),
            copy_xattr,
            metadata_cfg,
        )

        # Creating an alias sets the mtime of the directory it happens in.
        # That needs no care here: both parents were walked above, so they are
        # locked and recorded, and their times are set by the restore at the
        # end, after this phase.

        primary_st = os.stat(
            primary_leaf_b,
            dir_fd=primary_parent_fd,
            follow_symlinks=False,
        )
        if not stat.S_ISREG(primary_st.st_mode):
            raise RuntimeError("destination hard-link primary is not a regular file")
        primary_identity = (int(primary_st.st_dev), int(primary_st.st_ino))

        try:
            link_st = os.stat(
                link_leaf_b,
                dir_fd=link_parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            link_st = None

        if link_st is not None and (
            int(link_st.st_dev), int(link_st.st_ino)
        ) == primary_identity:
            return

        for _ in range(100):
            candidate = _new_hardlink_tmp_name()
            try:
                os.link(
                    primary_leaf_b,
                    candidate,
                    src_dir_fd=primary_parent_fd,
                    dst_dir_fd=link_parent_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                continue
            tmp_b = candidate
            tmp_exists = True
            break
        else:
            raise FileExistsError(
                errno.EEXIST,
                "cannot allocate a unique temporary hard-link name",
            )

        os.replace(
            tmp_b,
            link_leaf_b,
            src_dir_fd=link_parent_fd,
            dst_dir_fd=link_parent_fd,
        )
        tmp_exists = False
        fsync_if_per_file(cfg, link_parent_fd)
    finally:
        try:
            if tmp_exists and tmp_b is not None:
                try:
                    os.unlink(tmp_b, dir_fd=link_parent_fd)
                except FileNotFoundError:
                    pass
        finally:
            for parent_fd in (primary_parent_fd, link_parent_fd):
                if parent_fd is not None:
                    try:
                        os.close(parent_fd)
                    except OSError:
                        pass


def verify_destination_hardlink_group(destination_task: dict) -> None:
    paths = [destination_task["destination_primary"]] + list(
        destination_task["destination_links"]
    )
    expected_nlink = int(destination_task["expected_nlink"])
    destination_root_b = os.fsencode(destination_task["destination_root"])
    expected_identity = None

    for path_b in paths:
        st = _lstat_for_hardlink(path_b, destination_root_b)
        if not stat.S_ISREG(st.st_mode):
            raise RuntimeError(
                f"destination hard-link path is not a regular file: "
                f"{path_display(path_b)}"
            )
        identity = (int(st.st_dev), int(st.st_ino))
        if expected_identity is None:
            expected_identity = identity
        elif identity != expected_identity:
            raise RuntimeError(
                f"destination hard-link identity mismatch: {path_display(path_b)}"
            )
        if int(st.st_nlink) != expected_nlink:
            raise RuntimeError(
                f"destination hard-link count mismatch: {path_display(path_b)}; "
                f"expected={expected_nlink}, found={st.st_nlink}"
            )


def preflight_destination_hardlink_group(destination_task: dict, cfg: dict) -> None:
    """Reject predictable topology failures before changing any alias name."""
    source_root_b = os.fsencode(cfg["src_root"])
    destination_root_b = os.fsencode(cfg["dst_root"])
    primary_b = destination_task["destination_primary"]
    primary_rel_b = make_relative_bytes(primary_b, destination_root_b)
    validate_relative_path(primary_rel_b, allow_root=False)

    primary_parent_fd = open_src_dir_fd_from_root(
        destination_root_b,
        os.path.dirname(primary_rel_b),
    )
    try:
        primary_st = os.stat(
            os.path.basename(primary_rel_b),
            dir_fd=primary_parent_fd,
            follow_symlinks=False,
        )
    finally:
        os.close(primary_parent_fd)

    if not stat.S_ISREG(primary_st.st_mode):
        raise RuntimeError("destination hard-link primary is not a regular file")
    primary_identity = (int(primary_st.st_dev), int(primary_st.st_ino))
    mapped_links_to_primary = 1

    for source_b, destination_b in zip(
        destination_task["source_links"],
        destination_task["destination_links"],
    ):
        source_rel_b = make_relative_bytes(source_b, source_root_b)
        destination_rel_b = make_relative_bytes(destination_b, destination_root_b)
        validate_relative_path(source_rel_b, allow_root=False)
        validate_relative_path(destination_rel_b, allow_root=False)

        parent_fd = open_dst_dir_fd_with_attrs(
            source_root_b,
            destination_root_b,
            os.path.dirname(source_rel_b),
            bool(cfg.get("xattr", False)),
            cfg,
        )
        try:
            parent_st = os.fstat(parent_fd)
            if int(parent_st.st_dev) != primary_identity[0]:
                raise RuntimeError(
                    f"destination hard-link paths cross filesystem boundaries: "
                    f"{path_display(primary_b)} -> {path_display(destination_b)}"
                )

            try:
                existing_st = os.stat(
                    os.path.basename(destination_rel_b),
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                existing_st = None

            if existing_st is not None:
                if stat.S_ISDIR(existing_st.st_mode):
                    raise RuntimeError(
                        f"destination hard-link alias is a directory: "
                        f"{path_display(destination_b)}"
                    )
                if (
                    int(existing_st.st_dev),
                    int(existing_st.st_ino),
                ) == primary_identity:
                    mapped_links_to_primary += 1
        finally:
            os.close(parent_fd)

    if int(primary_st.st_nlink) != mapped_links_to_primary:
        raise RuntimeError(
            f"destination primary has hard links outside the mapped group: "
            f"{path_display(primary_b)}; st_nlink={primary_st.st_nlink}, "
            f"mapped_links={mapped_links_to_primary}"
        )


def ensure_destination_hardlink_group(destination_task: dict, cfg: dict) -> None:
    preflight_destination_hardlink_group(destination_task, cfg)
    primary_source_b = destination_task["source_primary"]
    primary_destination_b = destination_task["destination_primary"]
    for source_link_b, destination_link_b in zip(
        destination_task["source_links"],
        destination_task["destination_links"],
    ):
        create_or_replace_destination_hardlink(
            primary_source_b,
            source_link_b,
            primary_destination_b,
            destination_link_b,
            cfg,
        )
    verify_destination_hardlink_group(destination_task)


def _method_result_error(result: dict) -> str:
    message = str(result.get("message") or "").strip()
    if message:
        return message
    for key in (
        "failed_files",
        "hardlink_errors",
        "input_errors",
        "long_path_internal_errors",
    ):
        if result.get(key):
            return json.dumps(result[key], ensure_ascii=True)
    return f"method returned status={result.get('status')!r}"


def process_one_resolved_hardlink_group(group: dict, cfg: dict):
    source_paths, source_st = validate_resolved_hardlink_group(group, cfg)
    method = cfg.get("method")
    group_cfg = dict(cfg)
    group_cfg["run_schedule"] = None
    group_cfg["_hardlink_group_task"] = True
    group_cfg["_finish_current_task"] = True
    group_cfg["_expected_source_identities"] = {
        source_paths[0]: tuple(group["source_identity"])
    }
    if method != "migrate":
        group_cfg["relative_path"] = False
        destination_task = build_destination_hardlink_task(group, group_cfg)
    else:
        destination_task = None

    if method == "copy":
        # A custom copy command may update an existing multiply-linked target
        # inode in place. Use the atomic internal engine for resolved groups so
        # pre-existing aliases outside dst_root cannot be modified and the new
        # destination group starts with a private inode.
        group_cfg["engine"] = "internal"
        method_result = process_copy_batch([source_paths[0]], group_cfg)
    elif method == "rsync":
        # Only the primary, exactly like copy, diff and migrate. rsync is never
        # asked to reason about hard links: the group was already resolved and
        # verified here, and ensure_destination_hardlink_group() below creates
        # every alias with linkat and checks the resulting st_nlink. Letting
        # rsync derive the topology a second time would be a competing source
        # of truth for the same fact.
        method_result = process_rsync_batch([source_paths[0]], group_cfg)
    elif method == "diff":
        # Deliberate policy: compare only the deterministic primary name.
        method_result = process_diff_batch([source_paths[0]], group_cfg)
    elif method == "migrate":
        method_result = process_migrate_batch([source_paths[0]], group_cfg)
    else:
        raise RuntimeError(f"unsupported hard-link group method: {method!r}")

    if method_result.get("status") != "ok":
        return (
            -1,
            _method_result_error(method_result).encode(errors="replace"),
            int(method_result.get("bytes_total", 0) or 0),
            method_result.get("status"),
        )

    # Detect namespace changes that happened while data was copied, compared,
    # or migrated. Migrate additionally receives the expected identity above,
    # so every destructive mirror command is guarded against path replacement.
    validate_resolved_hardlink_group(
        group,
        group_cfg,
        check_fingerprint=(method != "migrate"),
    )

    if method in ("copy", "rsync"):
        ensure_destination_hardlink_group(destination_task, group_cfg)
        # Keep source and destination validation adjacent to the commit point.
        # This does not lock a live namespace, but it prevents a changed group
        # from being reported as a successful, coherent snapshot.
        validate_resolved_hardlink_group(group, group_cfg)

    return (
        0,
        b"hard-link group processed",
        int(method_result.get("bytes_total", source_st.st_size) or 0),
        "ok",
    )


def hardlink_group_input_path_count(group: dict) -> int:
    """Input records represented by one resolved group."""
    paths = group.get("input_paths")
    if not paths:
        primary = group.get("source_primary")
        paths = [primary] if primary else []
    return len([path for path in paths if path])


def process_hardlink_group_batch(groups: List[dict], cfg: dict) -> dict:
    """Process fully resolved groups exclusively after the normal phase."""
    groups = list(groups)
    failed_groups = {}
    failed_path_count = 0
    processed_groups = 0
    bytes_total = 0
    requeued_count = 0
    stopped = False
    t0 = time.time()

    for index, group in enumerate(groups):
        # batch_pause_reason() rather than two of the three conditions spelled
        # out again: written by hand, this loop knew about shutdown and the
        # schedule but not about pause, so a paused run went on reconstructing
        # every remaining group. The group already started is still finished
        # as a unit - that is what _finish_current_task is for - but a new one
        # is not begun.
        reason = batch_pause_reason(cfg, "during hard-link phase")
        if reason is not None:
            requeued_count += requeue_hardlink_groups(groups[index:], reason)
            stopped = True
            break

        identity_display = group.get("identity_display", "unknown")
        try:
            rc, message, group_bytes, child_status = (
                process_one_resolved_hardlink_group(group, cfg)
            )
            bytes_total += group_bytes
            if rc == 0:
                processed_groups += 1
                continue

            failed_groups[identity_display] = {
                "source_primary": path_display(group.get("source_primary", b"")),
                "error": message.decode(errors="replace"),
            }
            failed_path_count += hardlink_group_input_path_count(group)
            if child_status == "stopped":
                requeued_count += requeue_hardlink_groups(
                    groups[index + 1:],
                    "child method stopped during hard-link phase",
                )
                stopped = True
                break
        except Exception as exc:
            failed_groups[identity_display] = {
                "source_primary": path_display(group.get("source_primary", b"")),
                "error": str(exc),
            }
            failed_path_count += hardlink_group_input_path_count(group)

    duration = max(time.time() - t0, 0.001)
    return {
        "status": (
            "stopped"
            if stopped
            else ("ok" if not failed_groups else "failed")
        ),
        "method": cfg.get("method"),
        "task_kind": HARDLINK_TASK_KIND,
        # The discovery batches deliberately did NOT count these records as
        # transferred objects; they were deferred to this phase. Counting them
        # here is what closes the accounting, and it is also what makes a
        # failing group show up as failed paths rather than as successes.
        "count": sum(
            hardlink_group_input_path_count(group) for group in groups
        ),
        "failed_count": failed_path_count,
        "hard_link": cfg.get("hard_link"),
        "hardlink_source_backend": hard_link_source_backend(cfg["hard_link"]),
        "hardlink_destination_backend": hard_link_destination_backend(
            cfg["hard_link"]
        ),
        "hardlink_group_count": len(groups),
        "hardlink_processed_group_count": processed_groups,
        "hardlink_failed_group_count": len(failed_groups),
        "hardlink_path_count": sum(
            int(group.get("expected_nlink", 0)) for group in groups
        ),
        "bytes_total": bytes_total,
        "duration_sec": round(duration, 3),
        "bytes_per_sec": round(bytes_total / duration, 2),
        "failed_groups": failed_groups,
        "requeued_count": requeued_count,
        "message": (
            "stopped; remaining hard-link groups requeued"
            if stopped
            else ("" if not failed_groups else "hard-link group errors")
        ),
    }

def is_retryable(stderr_b: bytes) -> bool:
    """Decide from a message whether repeating the work could succeed.

    A timeout is deliberately NOT in the permanent list. It says the work took
    longer than the configured limit, which is a statement about time and not
    about the file; max_retries with its backoff exists for exactly that, and
    the rsync path has always retried its own timeouts. Listing it as
    permanent meant copy_timeout silently disabled max_retries for every file
    it hit.
    """
    s = (stderr_b or b"").lower()
    permanent_patterns = [
        b"no such file or directory",
        b"permission denied",
        b"not a directory",
        b"invalid argument",
        b"file name too long",
        b"enametoolong",
        b"path_length_exceeded",
        b"requires the python module 'blake3'",
    ]
    return not any(p in s for p in permanent_patterns)


def backoff_sleep(attempt: int, base: float, max_delay: float) -> None:
    delay = min(base * attempt, max_delay)
    if delay > 0:
        time.sleep(delay)

def acquire_migrate_inode(identity) -> bool:
    """Claim one inode for exclusive mirror operations."""
    key = (int(identity[0]), int(identity[1]))
    with migrate_inode_lock:
        if key in migrate_inodes_in_progress:
            return False
        migrate_inodes_in_progress.add(key)
        return True


def abandon_migrate_inode(identity, pid: int) -> None:
    """Keep an inode claimed because a helper for it may still be running.

    SIGKILL cannot be caught, so a helper that survives it is stuck in an
    uninterruptible kernel call and will run again when that call returns -
    possibly holding a Lustre lease and half way through a layout change.
    Releasing the claim would let this run start a second helper on the same
    inode, which is the one thing the claim exists to prevent. The claim is
    process-local, so keeping it only stops THIS run from touching that inode
    again.
    """
    key = (int(identity[0]), int(identity[1]))
    with migrate_inode_lock:
        migrate_inodes_in_progress.add(key)
        migrate_inodes_abandoned[key] = pid
    log(
        f"WARNING: migrate helper pid {pid} did not die; dev/ino "
        f"{key[0]}/{key[1]} stays claimed for the rest of this run"
    )


def migrate_inode_abandoned(identity) -> Optional[int]:
    key = (int(identity[0]), int(identity[1]))
    with migrate_inode_lock:
        return migrate_inodes_abandoned.get(key)


def release_migrate_inode(identity) -> None:
    key = (int(identity[0]), int(identity[1]))
    with migrate_inode_lock:
        if key in migrate_inodes_abandoned:
            # A helper for this inode may still be alive; the claim is not
            # ours to give back.
            return
        migrate_inodes_in_progress.discard(key)


def get_admin_command():
    try:
        return admin_queue.get_nowait()
    except queue.Empty:
        return None
# ------------------------------------------------------------
# Worker-Threads
# ------------------------------------------------------------

def dispatch_batch(batch: List[bytes], cfg: dict) -> dict:
    """Run one batch using the method from an immutable config snapshot."""
    method = cfg.get("method")

    if method == "migrate":
        return process_migrate_batch(batch, cfg)
    if method == "copy":
        return process_copy_batch(batch, cfg)
    if method == "diff":
        return process_diff_batch(batch, cfg)
    if method == "rsync":
        return process_rsync_batch(batch, cfg)

    raise ValueError(f"unsupported worker method: {method!r}")


def build_empty_normal_batch_result(cfg: dict) -> dict:
    return {
        "status": "ok",
        "method": cfg.get("method"),
        "count": 0,
        "bytes_total": 0,
        "duration_sec": 0.0,
        "message": "hard-link candidates resolved and deferred",
    }


def merge_hardlink_discovery_result(
    result: dict,
    discovery: dict,
    original_count: int,
    cfg: dict,
) -> dict:
    """Attach user-visible discovery accounting, never internal group objects."""
    result = dict(result)
    errors = discovery.get("errors", {})
    candidate_count = int(discovery.get("candidate_count", 0))
    normal_count = len(discovery.get("normal_paths", ()))

    result["count"] = original_count
    result["normal_count"] = normal_count
    result["hard_link"] = cfg.get("hard_link")
    result["hardlink_candidate_count"] = candidate_count
    result["hardlink_resolved_group_count"] = len(discovery.get("groups", ()))
    result["hardlink_prohibited_count"] = int(
        discovery.get("prohibited_count", 0)
    )
    result["hardlink_inspection_failed_count"] = int(
        discovery.get("inspection_failed_count", 0)
    )
    result["hardlink_resolution_failed_count"] = int(
        discovery.get("resolution_failed_count", 0)
    )
    result["hardlink_error_count"] = len(errors)
    # Not transferred by this batch: the hard-link phase reconstructs these
    # groups later and reports their outcome with its own result records.
    result["deferred_count"] = int(discovery.get("deferred_input_count", 0))

    if errors:
        result["hardlink_errors"] = {
            path_display(path_b): message.decode(errors="replace")
            for path_b, message in errors.items()
        }
        try:
            result["failed_count"] = int(result.get("failed_count", 0)) + len(errors)
        except (TypeError, ValueError):
            result["failed_count"] = len(errors)
        if result.get("status") == "ok":
            result["status"] = "failed"
            result["message"] = "hard-link policy or group-resolution errors"

    return result


def build_internal_error_result(
    worker_id: int,
    batch,
    cfg: Optional[dict],
    exc: BaseException,
) -> dict:
    """Create a result that accounts for every path in a failed worker batch.

    Programmer errors are deliberately not requeued automatically. Requeueing the
    whole batch could repeat already completed migrations/copies forever. Instead,
    every input path is emitted as failed so callers can decide whether to retry it.
    """
    try:
        batch_items = task_remaining_paths(batch)
    except Exception:
        batch_items = []

    error_text = f"{type(exc).__name__}: {exc}"
    failed_files = {}

    for index, path in enumerate(batch_items):
        try:
            display = path_display(path)
        except Exception:
            display = f"<unprintable-path-{index}>"

        # Preserve duplicate input records in the JSON object by adding an index
        # only when the same display path occurs more than once.
        key = display
        if key in failed_files:
            key = f"{display} [duplicate #{index}]"
        failed_files[key] = f"internal worker exception: {error_text}"

    return {
        "status": "internal_error",
        "method": (cfg or {}).get("method"),
        "worker_id": worker_id,
        "count": len(batch_items),
        "failed_count": len(batch_items),
        "failed_files": failed_files,
        "message": error_text,
        "requeued_count": 0,
    }


def worker_main(worker_id: int) -> None:
    """Worker loop with batch-level exception containment.

    A normal Exception from a processing method becomes an ``internal_error``
    result and the worker continues with the next batch. A BaseException outside
    Exception is still accounted for, then the worker exits so the scheduler can
    replace it.
    """
    worker_context.worker_id = worker_id

    try:
        while not shutdown_event.is_set():
            cmd = get_admin_command()
            if cmd == "stop":
                admin_queue.task_done()
                return

            cfg_for_schedule = get_config_snapshot()
            if not wait_until_work_is_allowed(worker_id, cfg_for_schedule):
                return

            try:
                batch = task_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            cfg = None
            active_counted = False
            fatal_worker_error = False

            try:
                cfg = get_config_snapshot()
                inc_active()
                active_counted = True

                try:
                    if is_hardlink_group_task(batch):
                        result = process_hardlink_group_batch(
                            batch.get("groups", ()),
                            cfg,
                        )
                    else:
                        original_count = len(batch)
                        discovery = partition_normal_batch_for_hardlinks(
                            batch,
                            cfg,
                        )

                        # The scheduler receives only complete, validated groups.
                        # This happens before task_done(), so its phase barrier
                        # cannot pass before every group has become visible.
                        for group in discovery["groups"]:
                            hardlink_group_queue.put(group)

                        normal_batch = discovery["normal_paths"]
                        if normal_batch:
                            result = dispatch_batch(normal_batch, cfg)
                        else:
                            result = build_empty_normal_batch_result(cfg)
                        result = merge_hardlink_discovery_result(
                            result,
                            discovery,
                            original_count,
                            cfg,
                        )
                except BaseException as exc:
                    fatal_worker_error = not isinstance(exc, Exception)
                    result = build_internal_error_result(worker_id, batch, cfg, exc)
                    log(
                        f"worker-{worker_id}: unhandled batch exception; "
                        f"batch recorded as internal_error\n{traceback.format_exc()}"
                    )

                # Deliberately after the batch has been measured: the flush is
                # not part of this batch's throughput and must not distort it.
                # "hourly" therefore means "at the end of the first batch that
                # finishes after the interval elapsed", which is what a loose
                # durability strategy asks for.
                # Ownership is drained centrally rather than inside one
                # method: chown is refused for directories, hard-link aliases
                # and rsync batches too, and a counter that only the copy
                # engine empties would attribute those to whichever copy batch
                # happened to run next.
                result = dict(result)
                result["ownership_not_preserved_count"] = take_ownership_skipped_count()

                if cfg.get("fsync") == "hourly" and claim_periodic_sync():
                    sync_started = time.monotonic()
                    sync_destination_filesystem(cfg, f"hourly interval, worker-{worker_id}")
                    result = dict(result)
                    result["periodic_sync_sec"] = round(
                        time.monotonic() - sync_started, 3
                    )

                result_queue.put(result)

            except BaseException as exc:
                # This also protects failures in config snapshotting, active-count
                # bookkeeping, result construction, or result queueing. Best effort
                # still records the complete dequeued batch before the worker exits.
                fatal_worker_error = not isinstance(exc, Exception)
                log(
                    f"worker-{worker_id}: exception in worker bookkeeping\n"
                    f"{traceback.format_exc()}"
                )
                try:
                    result_queue.put(
                        build_internal_error_result(worker_id, batch, cfg, exc)
                    )
                except BaseException:
                    log(
                        f"worker-{worker_id}: unable to emit internal_error result\n"
                        f"{traceback.format_exc()}"
                    )
                    fatal_worker_error = True

            finally:
                if active_counted:
                    dec_active()
                task_queue.task_done()

            if fatal_worker_error:
                return

            cmd = get_admin_command()
            if cmd == "stop":
                admin_queue.task_done()
                return

    except BaseException:
        # No worker failure should disappear without a diagnostic. The scheduler
        # removes this thread and starts a replacement when work is still allowed.
        log(f"worker-{worker_id}: fatal worker-loop exception\n{traceback.format_exc()}")


def cleanup_dead_workers_locked() -> List[threading.Thread]:
    global workers

    dead_workers = [t for t in workers if not t.is_alive()]
    workers = [t for t in workers if t.is_alive()]
    return dead_workers


def start_worker_locked() -> threading.Thread:
    """Start one worker. ``workers_lock`` must already be held."""
    global next_worker_id

    worker_id = next_worker_id
    next_worker_id += 1

    thread = threading.Thread(
        target=worker_main,
        args=(worker_id,),
        daemon=True,
        name=f"worker-{worker_id}",
    )
    workers.append(thread)
    thread.start()
    return thread


def resize_workers(new_count: int) -> None:
    with workers_lock:
        dead_workers = cleanup_dead_workers_locked()
        # Workers that were already told to stop but have not exited yet are
        # still in the list. Counting them as live would queue another round of
        # stop commands on the next reload and shrink the pool below the
        # configured size.
        current = max(0, len(workers) - admin_queue.qsize())

        if dead_workers:
            log(
                "removed dead workers: "
                + ", ".join(t.name for t in dead_workers)
            )

        if new_count > current:
            for _ in range(new_count - current):
                start_worker_locked()
            log(f"workers increased: {current} -> {new_count}")

        elif new_count < current:
            remove_n = current - new_count
            for _ in range(remove_n):
                admin_queue.put("stop")
            log(f"workers decreased: {current} -> {new_count}")


# ------------------------------------------------------------
# Scheduler / Monitoring
# ------------------------------------------------------------

def process_reload(config_path: str, allow_extra_tasks: bool = True) -> None:
    new_cfg = load_config_file(config_path)
    validate_helper_file_paths(new_cfg, config_path)
    # The same checks the start went through. A reload may name a different
    # copy engine or a different hard_link backend, and accepting those
    # unchecked would let a reload install exactly what the startup check
    # exists to refuse. Raising here leaves the previous configuration in
    # place, which is the safe half of the choice.
    check_configured_copy_engine(new_cfg)
    check_hardlink_backends(new_cfg)
    if not apply_new_config(new_cfg):
        return

    extra_file = new_cfg.get("reload_input_file")
    if extra_file:
        if not allow_extra_tasks:
            log(
                "reload_input_file ignored after the normal hard-link "
                "discovery phase has completed"
            )
            return
        try:
            loaded = load_extra_tasks_from_file(extra_file)
            log(f"reload input file loaded: {loaded} tasks from {extra_file}")
        except Exception as e:
            input_failed_event.set()
            log(f"reload input file failed: {e}")


def _hardlink_group_signature(group: dict):
    return (
        group.get("source_backend"),
        tuple(group.get("source_identity", ())),
        tuple(group.get("source_fingerprint", ())),
        group.get("source_primary"),
        tuple(group.get("source_links", ())),
        int(group.get("expected_nlink", 0)),
    )


def collect_resolved_hardlink_groups(groups_by_identity: dict) -> Tuple[int, int, int]:
    """Drain worker discoveries and merge duplicate fully resolved groups.

    The middle return value counts MERGED GROUP RESOLUTIONS - how often two
    workers independently resolved the same inode. That is a useful number for
    the log and it is not a count of dropped input records; it was once used
    as one, and the accounting then depended on how the input happened to be
    split into batches.
    """
    collected = 0
    merged_group_resolutions = 0
    conflicts = 0

    while True:
        try:
            group = hardlink_group_queue.get_nowait()
        except queue.Empty:
            break

        try:
            collected += 1
            identity_key = tuple(group["identity_key"])
            existing = groups_by_identity.get(identity_key)
            if existing is None:
                groups_by_identity[identity_key] = group
                continue

            # Two workers resolved the same inode, each from the record it
            # was given. Concatenate: both records exist and both must be
            # accounted for. A set union lost that - two DIFFERENT names of
            # one inode, discovered in separate batches, produced one merged
            # group and a "duplicate" that had dropped nothing, so a complete
            # run reported an unbalanced accounting and exited 1. How the
            # input happens to be split into batches must not decide whether
            # a run is called a success.
            merged_group_resolutions += 1
            existing["input_paths"] = list(existing.get("input_paths", ())) + list(
                group.get("input_paths", ())
            )

            if _hardlink_group_signature(existing) != _hardlink_group_signature(group):
                conflicts += 1
                existing["resolution_error"] = (
                    "conflicting hard-link group resolutions were returned by "
                    "different workers; source namespace changed during discovery"
                )
        finally:
            hardlink_group_queue.task_done()

    return collected, merged_group_resolutions, conflicts


def emit_results(run_stats: Optional[dict] = None) -> None:
    """Emit queued batch results and optionally aggregate run statistics."""
    while True:
        try:
            result = result_queue.get_nowait()
        except queue.Empty:
            break

        if run_stats is not None:
            try:
                run_stats["total_size_bytes"] += int(result.get("bytes_total", 0) or 0)
            except (TypeError, ValueError):
                pass
            try:
                failed = int(result.get("failed_count", 0) or 0)
            except (TypeError, ValueError):
                failed = 0
            try:
                requeued = int(result.get("requeued_count", 0) or 0)
            except (TypeError, ValueError):
                requeued = 0
            try:
                count = int(result.get("count", 0) or 0)
            except (TypeError, ValueError):
                count = 0
            try:
                # Paths handed on to the hard-link phase. They are counted
                # there, when their group has actually been reconstructed;
                # counting them here as well would report them twice and would
                # report a later failing group as a success.
                deferred = int(result.get("deferred_count", 0) or 0)
            except (TypeError, ValueError):
                deferred = 0
            try:
                run_stats["ownership_not_preserved_count"] += int(
                    result.get("ownership_not_preserved_count", 0) or 0
                )
            except (TypeError, ValueError):
                pass
            try:
                # A subset of the transferred objects: nothing was written,
                # the destination already matched.
                run_stats["total_skipped_count"] += int(
                    result.get("skipped_count", 0) or 0
                )
            except (TypeError, ValueError):
                pass
            # Requeued paths are counted again when they are processed later,
            # and failed paths were not transferred at all.
            run_stats["total_object_count"] += max(
                0, count - failed - requeued - deferred
            )
            run_stats["total_failed_count"] += failed
            if result.get("status") not in ("ok", "stopped"):
                run_stats["failed_batch_count"] += 1
            if result.get("status") == "stopped":
                run_stats["stopped_batch_count"] += 1

        # ASCII escaping keeps the stream valid UTF-8 JSON even when a POSIX
        # pathname contains arbitrary non-UTF-8 bytes represented internally
        # through Python's surrogateescape convention.
        print(json.dumps(result, ensure_ascii=True), flush=True)


def emit_final_statistics(
    run_stats: dict,
    started_monotonic: float,
    aborted: Optional[BaseException] = None,
) -> None:
    """Write exactly one final JSON statistics record for the whole run.

    ``aborted`` carries the exception that ended the run, when one did. It is
    reported as run_aborted, and always present rather than only on failure,
    so a consumer can test one field instead of having to notice an absence.
    """
    total_size = int(run_stats.get("total_size_bytes", 0))
    total_objects = int(run_stats.get("total_object_count", 0))
    total_time = max(time.monotonic() - started_monotonic, 0.001)
    total_performance = total_size / total_time

    # Whether the producer ever reached the end of its tree cannot be read off
    # the transferred count: a "find" that died halfway closes its pipe, and a
    # closed pipe looks exactly like a finished listing. The reference is the
    # number of records actually accepted, and every record has to end up in
    # exactly one of the three buckets below.
    input_records = get_input_record_count()
    total_failed = int(run_stats.get("total_failed_count", 0))
    unstarted = int(run_stats.get("unstarted_path_count", 0))
    accounted = total_objects + total_failed + unstarted

    merged_group_resolutions = int(
        run_stats.get("hardlink_merged_group_resolutions", 0) or 0
    )

    record = {
        "statistics": {
            "total_size_bytes": total_size,
            "total_size_human": human_bytes(total_size),
            "total_input_record_count": input_records,
            "total_object_count": total_objects,
            "total_skipped_count": int(run_stats.get("total_skipped_count", 0)),
            "total_failed_count": total_failed,
            "failed_batch_count": int(run_stats.get("failed_batch_count", 0)),
            "stopped_batch_count": int(run_stats.get("stopped_batch_count", 0)),
            # Paths that were still queued and had never been started when the
            # run ended. Without this a stopped run reports nothing but
            # successes, because a batch that was never dispatched produces no
            # result at all.
            "unstarted_path_count": unstarted,
            "unstarted_paths_saved": bool(run_stats.get("unstarted_paths_saved", False)),
            # How often two workers resolved the same inode independently.
            # Informational: it depends on batch boundaries and says nothing
            # about how many records exist.
            "hardlink_merged_group_resolutions": merged_group_resolutions,
            # object + failed + unstarted, against the accepted records.
            #
            # A plain sum, because every input record is now represented
            # exactly once: a hard-link group carries each record that named
            # it, repetitions included. Two earlier versions got this wrong in
            # opposite directions - one dropped repeated names from the group
            # and came out short, the other added a "duplicate" for every
            # merged group resolution and came out long, which failed complete
            # runs. false means what it says: this run cannot account for
            # every record it accepted.
            "accounted_path_count": accounted,
            "path_accounting_balanced": accounted == input_records,
            # Objects whose owner/group could not be applied because the
            # process lacks the privileges.
            "ownership_not_preserved_count": int(
                run_stats.get("ownership_not_preserved_count", 0)
            ),
            "input_complete": not input_failed_event.is_set(),
            # What the restore at the end did, split into files (rsync only)
            # and directories. "missing" are files whose transfer failed or
            # never happened; they were reported as failed paths already.
            "metadata_restore": run_stats.get("metadata_restore"),
            # The one field to test: a record that could not be written, a
            # restore that failed or never ran.
            "metadata_restore_incomplete": metadata_restore_incomplete.is_set(),
            "directories_left_locked": int(
                ((run_stats.get("metadata_restore") or {}).get("directories") or {})
                .get("left_locked", 0)
            ),
            "total_time_sec": round(total_time, 3),
            "total_performance_bytes_per_sec": round(total_performance, 2),
            "total_performance_human": human_bytes(total_performance) + "/s",
            # False for every run that reached its own end, whether it
            # succeeded, failed or was stopped. True only when the run died.
            # The numbers above are then whatever had been collected by that
            # point, and the run was NOT counted to the end.
            "run_aborted": aborted is not None,
        }
    }
    # How far the destination tree was actually protected while it was
    # written: enforced, partial or not_enforced, with the reasons. Separate
    # from the copy result and from the metadata result on purpose.
    protection = run_stats.get("destination_protection_summary")
    if protection is not None:
        record["statistics"].update(protection)
    else:
        record["statistics"]["destination_protection"] = "not_applicable"
    if aborted is not None:
        record["statistics"]["run_abort_reason"] = (
            f"{type(aborted).__name__}: {aborted}"
        )
    balanced = accounted == input_records
    if not balanced:
        log(
            f"WARNING: path accounting does not add up: {input_records} input "
            f"records accepted, {accounted} accounted for "
            f"({total_objects} objects + {total_failed} failed + "
            f"{unstarted} never started)"
        )

    # Back into run_stats, because the exit code is decided from there and a
    # number that only exists inside the json record cannot influence it.
    run_stats["total_input_record_count"] = input_records
    run_stats["accounted_path_count"] = accounted
    run_stats["path_accounting_balanced"] = balanced

    print(json.dumps(record, ensure_ascii=True), flush=True)


def new_run_stats() -> dict:
    """The counters the scheduler accumulates, in one place."""
    return {
        "total_size_bytes": 0,
        "total_object_count": 0,
        "total_failed_count": 0,
        "failed_batch_count": 0,
        "stopped_batch_count": 0,
        "ownership_not_preserved_count": 0,
        "total_skipped_count": 0,
        "unstarted_path_count": 0,
        # How often two workers independently resolved the same inode. A
        # number for the log, NOT a term in the accounting: it says nothing
        # about how many input records exist, and using it as if it did made
        # the balance depend on how the input was split into batches.
        "hardlink_merged_group_resolutions": 0,
    }


def scheduler_loop(
    config_path: str,
    started_monotonic: float,
    run_stats: Optional[dict] = None,
) -> dict:
    if run_stats is None:
        run_stats = new_run_stats()

    last_status_ts = 0.0
    last_check_ts = 0.0
    last_fsync_ts = time.monotonic()

    this_pid = os.getpid()

    alive_workers_old = 0
    active_old = 0
    queuelen_old = 0
    stdin_closed_old = False
    phase = "normal"
    hardlink_groups = {}
    hardlink_duplicate_resolutions = 0
    hardlink_conflicts = 0


    while True:
        if not shutdown_event.is_set() and reload_event.is_set():
            reload_event.clear()
            try:
                process_reload(
                    config_path,
                    allow_extra_tasks=(phase == "normal"),
                )
            except Exception as e:
                log(f"reload failed: {e}")

        if phase == "normal":
            _, duplicate_count, conflict_count = collect_resolved_hardlink_groups(
                hardlink_groups
            )
            hardlink_duplicate_resolutions += duplicate_count
            run_stats["hardlink_merged_group_resolutions"] = (
                hardlink_duplicate_resolutions
            )
            hardlink_conflicts += conflict_count

        emit_results(run_stats)

        scheduler_cfg = get_config_snapshot()
        if (
            scheduler_cfg.get("fsync") == "hourly"
            and not shutdown_event.is_set()
            and time.monotonic() - last_fsync_ts >= FSYNC_INTERVAL_SEC
        ):
            # Only the mark is set here. A worker performs the flush when it
            # finishes its current batch, so this loop never waits for it and
            # the flushing thread is one that is not writing at that moment.
            last_fsync_ts = time.monotonic()
            mark_periodic_sync_due()

        now = time.time()
        if now - last_check_ts >= 5.0:
            last_check_ts = now
            target_workers = int(get_config_snapshot()["max_workers"])
            with workers_lock:
                dead_workers = cleanup_dead_workers_locked()
                alive_before_replacement = max(
                    0, len(workers) - admin_queue.qsize()
                )

                if dead_workers:
                    log(
                        "scheduler removed dead workers: "
                        + ", ".join(t.name for t in dead_workers)
                    )

                if (
                    not shutdown_event.is_set()
                    and alive_before_replacement < target_workers
                ):
                    for _ in range(target_workers - alive_before_replacement):
                        start_worker_locked()
                    log(
                        f"scheduler restored workers: "
                        f"{alive_before_replacement} -> {target_workers}"
                    )

                alive_workers = len(workers)

            active = get_active()
            queuelen = get_queue_size()
            stdin_closed = stdin_closed_event.is_set()

            if (
                alive_workers != alive_workers_old
                or active != active_old
                or queuelen != queuelen_old
                or stdin_closed != stdin_closed_old
                or now - last_status_ts >= 120.0
            ):
                log(
                    f"Scheduler PID: {this_pid} status: workers={alive_workers} active={active} "
                    f"queue={queuelen} stdin_closed={stdin_closed} "
                    f"phase={phase} resolved_hardlink_groups={len(hardlink_groups)} "
                    f"shutdown={shutdown_event.is_set()}"
                )
                last_status_ts = now
                alive_workers_old = alive_workers
                active_old = active
                queuelen_old = queuelen
                stdin_closed_old = stdin_closed

        # A stop signal that arrives while the LAST batch runs used to cut the
        # run short even though it had finished everything: the shutdown branch
        # was evaluated first, so the directory timestamps were dropped and the
        # run ended with status 1 although every object had been processed.
        # Work that is genuinely complete is completed, signal or not. That
        # holds for the hard-link phase as well: a stop that arrived while the
        # last group was finished as a unit left nothing to resume, yet it
        # discarded every recorded directory timestamp and ended with exit 2.
        everything_done = (
            stdin_closed_event.is_set()
            and get_unfinished_tasks() == 0
            and get_inflight_batches() == 0
            and not hardlink_groups
        )

        if shutdown_event.is_set() and get_inflight_batches() == 0 and not (
            everything_done and phase in ("normal", "hardlink")
        ):
            if phase == "normal":
                collect_resolved_hardlink_groups(hardlink_groups)
                if hardlink_groups:
                    force_enqueue_task(
                        {
                            "kind": HARDLINK_TASK_KIND,
                            "groups": list(hardlink_groups.values()),
                        },
                        "shutdown before hard-link phase",
                    )
            # No batch is in flight any more, so nothing writes into the
            # destination: the metadata is restored now, stop or not. Leaving
            # the tree locked would be worse than giving directories that are
            # not yet complete their final values; a resumed run locks them
            # again and restores them again at its own end.
            finalize_destination_metadata(apply_restore=True, run_stats=run_stats)
            break

        if (
            phase == "normal"
            and stdin_closed_event.is_set()
            and get_unfinished_tasks() == 0
        ):
            # Every worker publishes resolved groups before task_done(). A final
            # drain here is therefore the complete phase-1 result.
            _, duplicate_count, conflict_count = collect_resolved_hardlink_groups(
                hardlink_groups
            )
            hardlink_duplicate_resolutions += duplicate_count
            run_stats["hardlink_merged_group_resolutions"] = (
                hardlink_duplicate_resolutions
            )
            hardlink_conflicts += conflict_count

            if hardlink_groups:
                groups = sorted(
                    hardlink_groups.values(),
                    key=lambda group: group.get("identity_display", ""),
                )
                # One task per chunk instead of a single task holding every
                # group: otherwise exactly one worker processes the complete
                # hard-link phase while all others idle.
                phase_cfg = get_config_snapshot()
                chunk_count = max(
                    1,
                    min(
                        len(groups),
                        int(phase_cfg["max_workers"]) * 4,
                        int(phase_cfg["queue_maxsize"]),
                    ),
                )
                chunk_size = math.ceil(len(groups) / chunk_count)
                chunks = [
                    groups[index:index + chunk_size]
                    for index in range(0, len(groups), chunk_size)
                ]
                for chunk in chunks:
                    task_queue.put_nowait(
                        {"kind": HARDLINK_TASK_KIND, "groups": chunk}
                    )
                log(
                    f"normal phase complete; queued {len(groups)} resolved "
                    f"hard-link groups in {len(chunks)} parallel tasks; "
                    f"duplicate_resolutions="
                    f"{hardlink_duplicate_resolutions}; conflicts="
                    f"{hardlink_conflicts}"
                )
                hardlink_groups = {}
                phase = "hardlink"
            else:
                phase = "complete"
                finalize_destination_metadata(apply_restore=True, run_stats=run_stats)
                break

        elif phase == "hardlink" and get_unfinished_tasks() == 0:
            # Hard-link groups also write into destination directories, so the
            # restore must wait until this phase is done as well.
            phase = "complete"
            finalize_destination_metadata(apply_restore=True, run_stats=run_stats)
            break

        time.sleep(0.2)

    emit_results(run_stats)

    # Every mode other than per-file durability ends with one barrier, so a
    # run that reports success has its data on the storage. This one is
    # deliberately blocking: it is the last thing that happens, and the exit
    # status must not claim success before it completed.
    final_cfg = get_config_snapshot()
    if fsync_needs_barrier(final_cfg):
        sync_destination_filesystem(final_cfg, "end of run")

    emit_results(run_stats)

    # The final statistics record is written by main(), after the workers have
    # been joined and the paths that were never started have been saved or
    # counted. Emitting it here would print a summary that predates the last
    # results and could not name the unstarted work at all.
    return run_stats

def create_default_config_for_method(method: str) -> dict:
    cfg = dict(COMMON_DEFAULTS)

    if method == "rsync":
        cfg.update(DEFAULT_CONFIG_RSYNC)
    elif method == "copy":
        cfg.update(DEFAULT_CONFIG_COPY)
    elif method == "migrate":
        cfg.update(DEFAULT_CONFIG_MIGRATE)
    elif method == "diff":
        cfg.update(DEFAULT_CONFIG_DIFF)
    else:
        raise ValueError(f"unsupported method: {method}")

    cfg["method"] = method
    return cfg


def validate_project_name(name: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
    if not name or any(c not in allowed for c in name):
        raise ValueError("project name may contain only letters, digits, '_', '-', '.'")
    return name


def create_project_and_start(
    method: str,
    project: str,
    src_dir: str,
    dst_dir: str,
    input_file: Optional[str] = None,
    workers: Optional[int] = None,
) -> int:
    method = normalize_method(method)
    if method not in ("rsync", "copy", "diff"):
        raise ValueError("project shortcut -p supports only rsync, copy, and diff")


    project = validate_project_name(project)

    # Project shortcut configs use relative_path=false, therefore both roots
    # and paths produced by the automatic find pipeline must be absolute.
    src_dir = os.path.abspath(os.path.expanduser(src_dir))
    dst_dir = os.path.abspath(os.path.expanduser(dst_dir))

    if not os.path.isdir(src_dir):
        raise ValueError(
            f"SRC is not an existing directory: {src_dir}"
        )

    if roots_overlap(src_dir, dst_dir):
        raise ValueError(
            "SRC and DST must be separate, non-overlapping directory trees"
        )

    cfg = create_default_config_for_method(method)
    cfg["src_root"] = src_dir
    cfg["dst_root"] = dst_dir

    if workers is not None:
        cfg["max_workers"] = require_config_int("workers", workers, minimum=1)

    cfg_path = f"{project}.cfg"
    jsonlog_path = f"{project}.jsonlog"
    stderr_log_path = f"{project}.log"

    # Every predictable name this launcher creates is opened with O_NOFOLLOW
    # and descriptor checks, because the launcher is routinely run as root in a
    # working directory that it does not own.
    warn_if_directory_is_world_writable(os.getcwd())

    with open(
        open_protected_file(cfg_path, append=False, mode=0o644),
        "w",
        encoding="utf-8",
        closefd=True,
    ) as f:
        json.dump(cfg, f, indent=4)

    if input_file is None:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        input_file = f"{method}-files-{timestamp}.txtz"

    input_file = os.path.abspath(os.path.expanduser(input_file))

    # The three project files were just created, so an INPUT naming one of them
    # would be read back as a path list: the stderr log as input, or the
    # configuration consumed as work.
    for label, other in (
        ("configuration", cfg_path),
        ("json log", jsonlog_path),
        ("stderr log", stderr_log_path),
    ):
        if input_file == os.path.abspath(other):
            raise ValueError(
                f"INPUT must not be the project's {label}: {input_file}"
            )

    out_f = open(open_protected_file(jsonlog_path, append=True, mode=0o644), "ab")
    err_f = open(open_protected_file(stderr_log_path, append=True, mode=0o644), "ab")
    find_proc = None
    tee_proc = None
    proc = None
    input_fd = None

    # Built once, before the branches. self_invocation() names the program by
    # the descriptor it holds on its own script (/proc/self/fd/N), and that
    # descriptor only reaches the child when it is in pass_fds - subprocess
    # closes everything else, and it is opened O_CLOEXEC besides. Without it
    # the child dies before it runs a line of this file, with an interpreter
    # error about a pathname that looks like an internal detail:
    #     can't open file '/proc/self/fd/10': No such file or directory
    # argv and the descriptor it depends on are therefore built once, side by
    # side, instead of at each of the two Popen calls - where the argv was
    # written and the pass_fds beside it was left out, in both of them.
    child_argv = self_invocation("start", cfg_path)
    child_fds = self_invocation_fds()

    try:
        if os.path.exists(input_file):
            if not os.path.isfile(input_file):
                raise ValueError(f"input path exists but is not a regular file: {input_file}")

            # O_NOFOLLOW on read as well: as root, a symlink planted at this
            # name would otherwise feed an unintended file list into the run.
            input_f = open(
                os.open(os.fsencode(input_file), os.O_RDONLY | os.O_NOFOLLOW),
                "rb",
            )
            try:
                proc = subprocess.Popen(
                    child_argv,
                    stdin=input_f,
                    stdout=out_f,
                    stderr=err_f,
                    pass_fds=child_fds,
                    start_new_session=True,
                )
            finally:
                input_f.close()
            input_mode = "existing NUL-terminated file"
        else:
            input_parent = os.path.dirname(input_file) or "."
            if not os.path.isdir(input_parent):
                raise ValueError(
                    f"directory for generated input file does not exist: {input_parent}"
                )

            # The input file is created here, not by tee: O_EXCL|O_NOFOLLOW
            # makes the creation atomic, so a name planted between the
            # existence check above and this point cannot be written through.
            # tee then receives the already-open descriptor by number instead
            # of the pathname, so it never performs a second lookup.
            input_fd = open_protected_file(
                input_file, append=False, exclusive=True, mode=0o644
            )

            # Stream directly into parallel_tools while tee stores the exact
            # NUL-terminated find output for later reuse (for example diff).
            #
            # The pipeline runs under one shell rather than as two Popen
            # children, because the launcher returns immediately and can
            # therefore never wait for their exit codes. A find that dies
            # halfway closes its pipe, and the reader cannot tell a closed
            # pipe from a finished listing: the run would process a partial
            # list and report success. The shell checks both exit codes and
            # appends the abort marker, which the reader recognises and turns
            # into "input stream was not read completely".
            #
            # The marker also ends up in the saved list through tee, so
            # re-using that list later raises the same error again instead of
            # silently repeating the partial run.
            abort = INPUT_ABORT_SHELL_LITERAL
            pipeline_script = (
                f'{{ "$1" "$2" -print0 || printf "{abort}"; }} '
                f'| "$3" "/dev/fd/$4" || printf "{abort}"'
            )
            find_proc = subprocess.Popen(
                [
                    resolve_trusted_executable("sh"),
                    "-c",
                    pipeline_script,
                    "parallel_tools-input",
                    resolve_trusted_executable("find"),
                    src_dir,
                    resolve_trusted_executable("tee"),
                    str(input_fd),
                ],
                stdout=subprocess.PIPE,
                stderr=err_f,
                pass_fds=(input_fd,) + trusted_exec_fds(),
                start_new_session=True,
            )
            try:
                try:
                    proc = subprocess.Popen(
                        child_argv,
                        stdin=find_proc.stdout,
                        stdout=out_f,
                        stderr=err_f,
                        pass_fds=child_fds,
                        start_new_session=True,
                    )
                finally:
                    if find_proc.stdout is not None:
                        find_proc.stdout.close()
            except Exception:
                if find_proc.poll() is None:
                    find_proc.terminate()
                raise

            input_mode = "generated by find -print0 and saved with tee"
    except Exception:
        if tee_proc is not None and tee_proc.poll() is None:
            tee_proc.terminate()
        if find_proc is not None and find_proc.poll() is None:
            find_proc.terminate()
        raise
    finally:
        if input_fd is not None:
            try:
                os.close(input_fd)
            except OSError:
                pass
        out_f.close()
        err_f.close()

    assert proc is not None
    print(f"created config: {cfg_path}")
    print(f"started PID: {proc.pid}")
    print(f"input file: {input_file}")
    print(f"input source: {input_mode}")
    if find_proc is not None:
        print(f"input pipeline PID: {find_proc.pid}")
    print(f"json log: {jsonlog_path}")
    print(f"stderr log: {stderr_log_path}")

    return 0


# ------------------------------------------------------------
# Command line interface
# ------------------------------------------------------------

def positive_pid(value: str) -> int:
    """argparse type accepting only process IDs, never process-group IDs."""
    try:
        pid = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "PID must be an integer greater than 0"
        ) from exc
    if pid <= 0:
        raise argparse.ArgumentTypeError(
            "PID must be greater than 0; zero and negative values address process groups"
        )
    return pid


TARGET_HELP = (
    "which run to signal: the stderr log it is writing (the file its own "
    "'2>' points at), or its PID. The log names the PID, and the PID is "
    "checked against that log before anything is signalled"
)


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the command-line interface and detailed user documentation."""

    parser = argparse.ArgumentParser(
        description=(
            "Parallel tool for copying, verifying, and migrating files, especially\n"
            "on Lustre file systems. Input paths are read as a NUL-terminated\n"
            "byte stream compatible with 'find -print0'."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    prog = parser.prog

    parser.epilog = f"""
MAIN WORKFLOWS
==============

1) Project shortcut: create configuration and start detached

   {prog} -p METHOD NAME SRC DST [INPUT] [-j N]

   METHOD: rsync, copy, or diff (migrate takes no destination tree)
   NAME:   project name; creates NAME.cfg, NAME.jsonlog (stdout: the JSON
           records) and NAME.log (stderr: the operational log)
   SRC:    source root
   DST:    destination root
   INPUT:  optional NUL-terminated path-list file
   -j N:   max_workers for the generated configuration

   INPUT handling:
     * INPUT exists:
         The existing NUL-terminated file is used directly as stdin.
     * INPUT is specified but does not exist:
         The program effectively starts
             find SRC -print0 | tee INPUT | parallel_tools ...
         Paths are processed immediately and simultaneously stored in INPUT
         for later reuse, verification, or audit purposes.
     * INPUT is omitted:
         A file named METHOD-files-YYYYMMDD-HHMMSS.txtz is created automatically
         and filled from 'find SRC -print0' while the project is running.

   The -p shortcut sets relative_path=false and normalizes SRC and DST to
   absolute paths. migrate is intentionally not supported by -p.

   The find/tee pipeline runs under one shell that checks both exit codes,
   because this command returns immediately and can never wait for them. A
   find that dies halfway closes its pipe, and a closed pipe is
   indistinguishable from a finished listing, so the run would process a
   partial list and report success. The shell appends an abort marker, which
   the reader turns into "input stream was not read completely" and exit
   status 1. The marker is stored in INPUT as well, so reusing that list later
   raises the same error instead of repeating the partial run silently.

   Examples:
     {prog} -p rsync copy1 /src /dst paths.txtz
     {prog} -p copy  copy2 /src /dst paths.txtz -j 16
     {prog} -p diff  check /src /dst

2) Create a default configuration

   {prog} create METHOD CONFIG

   METHOD: rsync, copy, diff, or migrate
   CONFIG: JSON configuration file to create

   The file contains all default values for the selected method and can then
   be edited and used with 'start'. An existing CONFIG file is overwritten.

   migrate has four settings that cannot be given a default, because they
   decide which OSTs are emptied and how many mirrors a file keeps:
   src_root, stripcount, banned_osts and keep_mirroring. They are written as
   null, 'create' lists them, and a configuration that still has them null is
   refused at start by name. The generated file is a form to fill in, not a
   run waiting to happen.

   Examples:
     {prog} create rsync   rsync.cfg
     {prog} create copy    copy.cfg
     {prog} create diff    diff.cfg
     {prog} create migrate migrate.cfg

3) Start with an existing configuration

   {prog} start CONFIG < paths.txtz
   find /src -print0 | {prog} start CONFIG

   CONFIG selects the processing method and runtime parameters. stdin must
   contain NUL-terminated paths.

OTHER COMMANDS
==============
   Each takes the stderr log the run is writing - the file its own "2>"
   points at, or NAME.log under -p - or its PID. The log carries the pid, and
   the pid is verified against that log before anything is signalled.

   {prog} stop   LOG|PID   SIGTERM: finish current work, then stop
   {prog} pause  LOG|PID   hold the run; queued work is kept, nothing lost
   {prog} resume LOG|PID   let a paused run carry on
   {prog} reload LOG|PID   SIGHUP: reload the dynamic configuration values

   One log file per run. Two runs appending to the same log make the pid
   line ambiguous and the log unreadable, and gain nothing.

DETAILED HELP
=============
   {prog} start --help   complete JSON configuration and verify-mode reference
   {prog} create --help  generated defaults and create semantics
"""

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {PROGRAM_VERSION}",
        help="show program version and exit",
    )

    parser.add_argument(
        "-p",
        "--project",
        nargs="+",
        metavar="ARG",
        help=(
            "project shortcut: METHOD NAME SRC DST [INPUT]; create a "
            "configuration and start detached. METHOD: rsync, copy, diff"
        ),
    )

    parser.add_argument(
        "-j",
        "--workers",
        type=int,
        metavar="N",
        help=(
            "max_workers for -p (N >= 1). Valid only together with -p; "
            "without -j the default max_workers=8 is used"
        ),
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        description=f"Detailed help: '{prog} COMMAND --help'",
        metavar="COMMAND",
    )

    # ------------------------------------------------------------------
    # start
    # ------------------------------------------------------------------
    start_description = """Start processing with a JSON configuration.

stdin is a byte stream of NUL-terminated paths. With relative_path=false,
input paths must be absolute. With relative_path=true, input paths must be
relative and must not contain '.', '..', or empty path components.
"""

    # The same literal the project shortcut appends, so the manual pipeline
    # and the generated one cannot drift apart.
    abort = INPUT_ABORT_SHELL_LITERAL

    start_epilog = f"""
HOLDING A RUN
=============

  pause PID stops the workers from taking new files without ending the run.
  The file each worker is on is finished, the rest of its batch goes back on
  the queue, and the run keeps its queue, its statistics and its open log. It
  is not a checkpoint and nothing is written out - the process simply waits.

      {prog} pause test.log       # workers wind down within a file
      {prog} resume test.log      # they pick the queue up again

  stop, reload, pause and resume all take either a PID or the stderr log the
  run is writing - the file its own "2>" points at, or NAME.log under -p:

      {prog} stop /srv/jobs/test.log

  The log carries the pid, in the status line the scheduler writes whenever
  the worker count, the active count or the queue changes, and at least every
  two minutes:

      2026-09-22 20:58:15.203098 Scheduler PID: 4096426 status: workers=8 ...

  The last such line wins. What it says is a SUGGESTION, not an
  authorisation: a log is an ordinary file, and whoever can write it can put
  a wrong number in it. Before anything is sent, that pid is checked against
  the process that carries it - is its descriptor 2 this very file, compared
  by inode, and is it this program. Both questions are answered out of
  /proc/PID/fd, which reads the process's file table and not its memory. A
  pid that fails either check is not signalled, and the command says which
  check failed.

  Give one log file to one run. Two runs appending to the same log make the
  pid line ambiguous, make the log itself hard to read, and gain nothing:

      {prog} start a.cfg 2>a.log 1>a.jsonlog
      {prog} start b.cfg 2>b.log 1>b.jsonlog

  A PID is the other way in and is never second-guessed, but it is a
  statement about the past: a log is appended to, so it accumulates pids from
  earlier runs, and the number may since have been reused.

  A paused run still reacts to a stop signal immediately; it does not have to
  be resumed first. A hard-link group is finished as a unit rather than left
  half reconstructed, and a running migrate helper is allowed to finish, both
  exactly as during a shutdown.

  The signals are SIGUSR1 and SIGUSR2. Without a handler those TERMINATE a
  process, so a pid that belongs to something else does damage. A named pid
  is nevertheless obeyed: this program says what it is about to signal and
  then signals it. It used to refuse instead, and that refusal locked an
  operator out of their own run the moment the identification was wrong,
  which is the worse failure of the two - a control command that can decline
  to control is not one.

  What the identification still does is report. Before the signal goes out:

      WARNING: process 12345 does not look like a parallel_tools run;
      sending SIGUSR1 (pause) anyway, as asked. It is: /usr/sbin/mariadbd ...

  A run started by -p carries no script name in its command line at all - it
  was started through a descriptor and shows up as

      /usr/bin/python3.11 /proc/self/fd/5 start test.cfg

  so the check follows /proc/PID/fd/5 and compares it to this file. That is
  stricter than any name match: it is the same file or it is not.

  remaining_tasks_file follows the same rule as the directory-timestamp spill:
  null means "pick a name inside working_directory", a relative name is taken
  as relative to that directory, and an absolute one is used as given. Both
  files of one run share an identifier, so leftovers can be told apart:

      .parallel_tool_workdir/remaining_tasks_20260921-143012-9f3a.txtz
      .parallel_tool_workdir/mtdirs_20260921-143012-9f3a.log

  Nothing is created when a run ends with nothing left over, which is the
  normal case. It used to be that null meant the not-yet-started paths were
  simply dropped - a run stopped by an operator who had not configured a file
  said so and lost the work anyway.

  Interrupted batches are counted in stopped_batch_count for information. They
  do NOT by themselves make the run incomplete: their paths went back on the
  queue, and what is left over at the end is what unstarted_path_count says.

OUTPUT STREAMS
==============

  The two streams carry different things and are meant to be separated.

  stdout   The JSON log, and nothing else. One record per finished batch, and
           one final record with the whole-run statistics as the last line.
           Every line is a complete JSON object, so the file can be read with
           jq or line by line while the run is still going.

  stderr   The operational log: the applied configuration, the periodic status
           lines, warnings, and every error message. Free text, meant for a
           person. Every line begins with a local timestamp of the form
           2026-09-19 10:46:42.587002, so a reload, a stop signal and a failure
           can be put in order afterwards. Only the continuation lines of a
           traceback carry none, so the traceback stays one readable block.

  Mixing them into one file makes the JSON log unparseable, because the status
  lines end up between the records. Keep them apart:

EXAMPLES
========

  A 'find' that dies halfway closes its pipe, and a closed pipe looks exactly
  like a finished listing: the run would process a partial list and report
  success. So every example below appends the abort marker when find fails.
  The reader turns it into "input stream was not read completely" and exit
  status 1. Leaving the marker out is the one mistake that makes a truncated
  run look like a complete one.

  Define the listing once:

    listing() {{ find "$1" -print0 || printf '{abort}'; }}

  Run and keep both logs:

    listing /src | {prog} start copy.cfg >run.jsonlog 2>run.log

  Append instead of overwrite, for a job that is restarted:

    listing /src | {prog} start copy.cfg >>run.jsonlog 2>>run.log

  Keep both logs and still watch the run (bash/ksh; 'tee' copies stderr back
  to the terminal):

    listing /src | {prog} start copy.cfg >run.jsonlog 2> >(tee run.log >&2)

  The same without process substitution, for a plain POSIX shell:

    listing /src | {prog} start copy.cfg 2>&1 >run.jsonlog | tee run.log >&2

  Reading a saved list needs no marker of its own - if the list was produced
  this way, it already contains one:

    {prog} start copy.cfg <paths.txtz >run.jsonlog 2>run.log

  Read the result afterwards:

    tail -1 run.jsonlog | jq .statistics      # the whole-run figures
    jq -r 'select(.failed_files)' run.jsonlog # batches with failures
    grep -i warning run.log                   # what the run complained about

  Without a redirection, both streams go to the terminal and the JSON records
  are interleaved with the status lines. That is fine for a look, not for a
  log to be read back.

  The project shortcut does this redirection itself and writes NAME.jsonlog
  and NAME.log, so it needs none of the above:

    {prog} -p copy myrun /src /dst paths.txtz


COMMON JSON PARAMETERS
======================

  method                 string, required
                         rsync | copy | diff | migrate

  max_workers            integer >= 1, default 8
                         Number of parallel worker threads. If the value is
                         greater than NCPU-1, a CPU-oversubscription warning is
                         printed; the run is not rejected.

                         method=rsync counts differently. One local rsync
                         invocation is three processes, not one: the
                         invocation itself, the server child it forks, and the
                         generator the receiver forks. N workers therefore put
                         3N rsync processes on the run queue, and the warning
                         fires at max_workers > (NCPU-1)/3. On a 32-CPU node
                         that is max_workers=10, not 31.

  batch_max_files        integer >= 1, default 100
                         Maximum number of input paths in one internal worker
                         batch. On SIGHUP, changing this value atomically
                         re-batches all queued (not yet active) paths. Active
                         worker batches continue unchanged. If splitting makes
                         the queue temporarily larger than queue_maxsize, all
                         work is retained and producers wait until the queue
                         drains below the configured limit.

  max_retries            integer >= 1, default 7
                         Maximum number of attempts for retryable operations.

  queue_maxsize          integer >= 1, default 1000
                         Maximum number of internally buffered batches. During
                         reload the queue may be enlarged or reduced without
                         discarding already queued work.

  stdin_chunk_size       integer >= 1, default 16777216 (16 MiB)
                         Read size used for the NUL-terminated stdin byte stream.

  max_input_record_bytes integer >= 1, default 8388608 (8 MiB)
                         Maximum length of one NUL-terminated input path.
                         An unterminated last path is always an input error.

  retry_backoff_base     number >= 0, default 2.0 seconds
  retry_backoff_max      number >= 0, default 60.0 seconds
                         Parameters controlling delays between retry rounds.

  reload_input_file      string or null, default null
                         On SIGHUP, read this additional NUL-terminated path list
                         and append its paths to the work queue.

  remaining_tasks_file   string or null, default null
                         During graceful shutdown, save paths that have not yet
                         started to this file in NUL-terminated form.
                         Their number is reported as unstarted_path_count and
                         the run exits with status 2 whether or not the file is
                         configured: a batch that was never dispatched produces
                         no result record, so without this a run stopped before
                         its first batch would report nothing but successes.
                         Without the file those paths are lost, which is said
                         explicitly in the log.

  working_directory      string, default ".parallel_tool_workdir"
                         Where this run keeps its own state. Relative names are
                         resolved against the current working directory.

                         Nothing is created at start-up. The directory is only
                         judged, and only created when something actually has
                         to be written into it - most runs never need it.

                         Judged means: no directory in the chain above it may
                         be writable by group or other (the sticky bit is the
                         exception that makes /tmp usable), and if it already
                         exists it must be a directory, owned by this user and
                         not writable by group or other. The check is made on a
                         descriptor, and every file inside is then created
                         THROUGH that descriptor, so no name is resolved twice.

                         It must not lie inside src_root or dst_root: removing
                         a file there at the end would change the mtime of the
                         very directory that was just restored. An explicitly
                         configured path inside a root is refused at start-up.
                         The DEFAULT landing inside a root - which happens when
                         the program is started from within the tree - is
                         reported and the directory counts as unavailable, so
                         the run still starts.

                         State file names carry a timestamp and a short random
                         block. The random part is not what makes them safe;
                         the directory check is. It keeps two runs started in
                         the same second out of each other's files, and the
                         timestamp makes leftovers sortable.

  metadata_maxsize       byte count or size string, default "100MiB"
                         (the former name directory_times_maxsize is accepted)
                         How much COMPRESSED destination metadata is held in
                         memory before it is spilled into working_directory.
                         Accepts bytes as an integer or a string with a unit:
                         "100MiB", "512KiB", "2GB". rsync splits it between
                         the directory and the file records.

                         A compressor does not hand out every byte it is given
                         - deflate holds about a window's worth of input before
                         it emits anything - so the figure counts the bytes
                         already emitted plus the input still inside the codec.
                         Without that, a small limit would never be reached.

                         What is recorded: copy and rsync keep the destination
                         tree LOCKED while they write (see DESTINATION
                         PROTECTION), so no directory receives its final owner,
                         mode, ACL, xattrs or times during the run. Those values
                         are recorded here and applied once, after the normal
                         phase and the hard-link phase. For rsync the owner,
                         mode, ACL and xattrs of every file and symlink are
                         recorded as well.

                         The same limit bounds the external sort that orders
                         the directories bottom-up for the restore.

                         Above the limit, a file is created in
                         working_directory. If there is none that can be used,
                         recording STOPS there: growing without bound would
                         trade wrong metadata for a dead machine. What was
                         already held is still applied, the run says so, and
                         it ends with status 1.

                         This is not a crash-safe journal. What must survive a
                         crash - the original values of destination
                         directories the lock changed - is journalled
                         separately, see DESTINATION PROTECTION.

  spill_compression      "zlib" (default), "zstd", or "none"
                         Compression of the destination metadata records,
                         in memory and in the spill file alike.
                         zlib:  standard library, gzip container, level 1.
                                Measured ~6x smaller at ~185 MB/s. The file
                                stays readable with zcat and gzip -t.
                         zstd:  requires the Python module 'zstandard'.
                                Same ratio at roughly seven times the speed.
                                Relevant only for very large directory counts.
                         none:  plain records, for inspection and debugging.
                         Measured sizes per directory: ~102 bytes uncompressed,
                         ~17 bytes compressed. Path names compress about 25x;
                         the two nanosecond timestamps dominate what remains.

  run_schedule           object or null, default null
                         null means that work is allowed at all times.
                         Supported format:

      "run_schedule": {{
          "timezone": "local",
          "check_interval_sec": 60,
          "windows": {{
              "mon": [["22:00", "24:00"], ["00:00", "07:00"]],
              "tue": [["22:00", "24:00"]],
              "wed": [],
              "thu": [],
              "fri": [],
              "sat": [["00:00", "24:00"]],
              "sun": [["00:00", "24:00"]]
          }}
      }}

                         Valid day names: mon tue wed thu fri sat sun.
                         timezone currently supports only "local". A time window
                         may not cross midnight; for example 22:00-07:00 must be
                         represented as 22:00-24:00 plus 00:00-07:00. Outside
                         the configured windows workers do not start new work.

HARD-LINK POLICY FOR ALL METHODS
================================

  hard_link              null (default), "prohibited", "lustre2posix",
                         "posix2lustre", "lustre2lustre", or "posix2posix"

      null:
          Assume that no hard links exist. st_nlink is not inspected and no
          hard-link topology is preserved.

      "prohibited":
          Inspect every regular source file. A file with st_nlink > 1 is
          skipped and recorded as a hard-link policy error in the JSON log.

      "lustre2posix" / "lustre2lustre":
          Resolve all names of a source inode through its Lustre FID and
          'lfs fid2path -0'. Lustre clients without -0 support use the compatible
          '--link N' interface once per name without newline-splitting paths.

      "posix2lustre" / "posix2posix":
          Resolve all names by scanning the relevant POSIX source subtree for
          the same st_dev/st_ino pair. This can be substantially slower.

  The mode names both filesystems: "posix2lustre" is a POSIX source and a
  Lustre destination. Both halves are checked against the mounted filesystem
  types of src_root and dst_root before the run starts, so a mode that
  declares what is not there is refused rather than discovered per file.
  method=migrate has no destination tree, so only src_root is checked.

  A preservation mode requires every resolved source name to be located below
  src_root. If st_nlink says that more names exist than can be resolved inside
  that tree, the complete group fails and nothing is reported as successfully
  reconstructed. Workers resolve groups while normal batches run. After all
  normal work has finished, the scheduler supplies the deduplicated groups as
  a separate exclusive hard-link task.

  copy / rsync:
      Copy one coherent primary and reconstruct all mapped destination names as
      hard links. The final destination inode and exact st_nlink are verified.
      Every method transfers only the deterministic primary name; the aliases
      are always created by parallel_tools itself, never by the transfer tool.
      Resolved copy groups use the atomic internal copy engine even if a custom
      engine is configured. Resolved rsync groups use --ignore-times so an
      unrelated pre-existing destination inode is never reused.

  diff:
      Resolve the group, but compare only its deterministic primary pathname.
      Destination alias names and destination hard-link topology are deliberately
      not checked.

  migrate:
      Operate once on the deterministic primary/FID because every source name
      addresses the same Lustre inode. Of the preservation modes, migrate accepts
      only "lustre2lustre" and then requires an explicit src_root.

PATH-LENGTH POLICY FOR ALL METHODS
==================================

  max_path_length        "engine" (default), "rsync" for method=rsync,
                         or integer >= 1. Measured in encoded pathname bytes,
                         not Unicode characters.

      "engine":
          No parallel_tools policy limit. Internal engines use the system
          PC_PATH_MAX value reported by os.pathconf() to decide when their
          PATH_MAX-safe openat/cwd handling is needed. For method=rsync, paths
          requiring long-path handling are redirected to the internal openat
          copy engine instead of being passed to rsync.

      "rsync" (method=rsync only):
          No parallel_tools policy limit and no internal PATH_MAX fallback.
          Long paths are passed to rsync unchanged; rsync decides whether it
          can handle them. Any failure is reported in the JSON log.

      N:
          Paths longer than N bytes are rejected before processing and recorded
          as path_length_exceeded in the JSON log. Paths of length <= N use the
          normal/classic engine path; parallel_tools does NOT automatically
          switch them to openat/cwd even if they exceed system PATH_MAX. Native
          ENAMETOOLONG or other engine errors are therefore visible in the log.

                         Exactly N bytes is allowed; only lengths > N are
                         rejected. A single overlong pathname component may
                         still fail because filesystem NAME_MAX is independent
                         of total pathname length.

ROOT PATH PARAMETERS FOR rsync / copy / diff
============================================

  src_root               non-empty absolute path, required
  dst_root               non-empty absolute path, required

  For copy and rsync, source and destination roots must be separate,
  non-overlapping trees. This prevents self-copy truncation and prevents a live
  find pipeline from discovering files that the same run just created.

  relative_path          boolean, required in an explicit CONFIG
                         false: stdin contains absolute source paths. Every path
                                must be located below src_root.
                         true:  stdin contains paths relative to src_root.
                                Absolute paths and '.', '..', or empty path
                                components are rejected.

METADATA: permissions, mtime, xattr
====================================

  Three independent settings, valid for copy, rsync and diff (not migrate).
  For copy and rsync they say what is transferred, for diff what is compared.
  They cover files, symlinks and directories alike.

  permissions            string of "p", "o", "g", or null / "" (default "pog")
                         p  access mode bits, including setuid/setgid/sticky
                         o  owning user
                         g  owning group
                         The classes are separate because preserving ownership
                         needs privileges while preserving the mode does not.
                         An unprivileged run should ask for "p" alone instead
                         of failing on every chown.
                         A symlink has no meaningful mode of its own, so "p"
                         does not apply to symlinks.
                         ownership_not_preserved_count counts every refused
                         chown of the run, and a run with a non-zero count
                         exits with status 1: a destination with the wrong
                         owners is not the destination that was asked for. An
                         unprivileged run should therefore leave "o" and "g"
                         out of permissions rather than let them fail.

  mtime                  boolean, default true
                         Transfer or compare modification timestamps with
                         nanosecond resolution. The access time is carried
                         along by the same utimensat() call.
                         For copy, mtime=true also verifies the timestamp of
                         the written file together with its size.
                         For rsync it adds -t. Without -t the destination
                         carries the transfer time and rsync's quick check
                         re-transfers every file on the next run, so mtime=false
                         means every run is a full transfer. Use
                         ignore_existing or an rsync-side --size-only strategy
                         if that is what you want.
                         Destination DIRECTORY timestamps are restored at the
                         end of the run for copy and rsync (rsync runs with -O);
                         see metadata_maxsize.

  xattr                  boolean, default false
                         Copy and verify, or for diff compare, all extended
                         attributes, POSIX ACLs included. On destination
                         directories, extra user.* attributes and ACLs are
                         removed; other extra attributes make the restore of
                         that directory fail rather than be removed unreviewed.

  permissions, mtime and xattr cannot be changed by a reload: they decide what
  is recorded for the restore at the end of the run.

DESTINATION PROTECTION (copy, rsync)
====================================

  dst_root is opened once at the start and held for the whole run; every walk
  below it starts from that descriptor, and rsync is handed the descriptor as
  /proc/self/fd/N/, not the name. The chain of directories ABOVE dst_root must
  be trustworthy: owned by root or this user and not writable by group or
  other (the sticky bit excepted). Otherwise somebody could move dst_root away
  and put another directory in its place.

  While the run writes, every destination directory it touches is LOCKED:
  owned by the user running the program, permissions 0700 (special bits are
  kept, the group is left as it is - with no group permission it grants
  nothing). New directories are created locked. Nobody else can reach
  anything below a locked dst_root by path.

  One run per destination: the run holds an exclusive flock() on dst_root
  from the start until its restore is done. A second run on the same dst_root
  is refused with exit status 1. If flock() fails - Lustre mounted without
  -o flock or -o localflock - the run does not start. With -o localflock the
  lock only covers runs on the same node. If workers are still busy when the
  run ends, the restore is skipped and the lock is kept until the process and
  its writing children (rsync, an external engine) have exited. A start that
  fails removes the dst_root and parent directories it created itself, if
  they are still empty and still the directories it created.

  Directories are locked when the run first touches them. Before an existing
  directory is changed, its original owner, mode and access ACL - and its
  device and inode - are written to a journal in working_directory and
  flushed to disk. The existing parent directories of a batch are locked
  together with one flush; concurrent workers share flushes as well. Nothing
  is recorded for a directory that is already locked, so a later batch can
  never mistake the temporary 0700 for the original. Existing directories the
  input never names stay as they are. Put working_directory on a local disk:
  an fsync on Lustre is a server round trip.

  At the end - after all batches and the hard-link phase, and also after a
  stop - the restore runs: for rsync first the files and symlinks, then the
  directories bottom-up, dst_root last, independent directories in parallel.
  Each object gets, in this order: owner/group, xattrs, ACLs, mode, then a
  check of all of them, then its times. A configured metadata class takes the
  source value; for a class that is not configured, a directory gets back
  the value it had before the lock. dst_root takes the metadata of src_root
  when the input names src_root (find SRC -print0 does).

  If the process dies, the journal stays. The next start with the same
  dst_root and working_directory rolls the journalled originals back before
  it locks anything, and refuses to start if that fails. Nothing is rolled
  back onto another directory: if dst_root is not the journalled directory,
  the start is refused and the journal kept; a directory below it that was
  replaced or is no longer locked is skipped and reported. The journal is
  streamed; metadata_maxsize bounds the rollback too. Directories the dead run created stay 0700
  until a run over them restores them. With permissions lacking "p", the mode
  such a directory should get cannot be known after a crash.

  The statistics report how far the protection held:

      destination_protection   enforced      everything the run touched was
                                             locked, dst_root included
                               partial       some directories could not be
                                             locked (unprivileged run, foreign
                                             owner) or were released during
                                             the run
                               not_enforced  dst_root itself could not be
                                             locked, or its parent chain is
                                             not trustworthy

  require_destination_protection
                         boolean, default false (copy, rsync)
                         false: anything but "enforced" is a warning in the
                         log and the statistics. true: it ends the run with
                         exit status 1.

  A directory whose restore fails can stay 0700; it is named in the log with
  its path in base64, and directories_left_locked counts it.

  The lock protects the destination. It does not stop rsync from resolving
  SOURCE names again; see the README.

VERIFY MODES
============

  verify selects the CONTENT check only. Timestamps and ownership are the
  independent settings above; there is no combined size_mtime mode any more.

  copy    "size" (default), "sha256", "blake3", "blake3thread<N>",
          "blake3threadinga"
  diff    "sha256" (default), "size", "blake3", "blake3thread<N>",
          "blake3threadinga"
  rsync   "size_iferr" (default), null, "size", "sha256", "blake3",
          "blake3thread<N>", "blake3threadinga"
  migrate has no verify setting.

  copy and diff always compare at least the size, so null is not offered for
  them. For copy the size check also guards against a truncated write.

  "verify": null            (rsync only)
      Do not re-check anything; trust the rsync exit code.

  "verify": "size_iferr"    (rsync only, default)
      Re-check nothing while rsync reports success. If rsync reported a
      problem, compare the sizes of exactly the paths that were sent to it and
      record each mismatch in the JSON log. A clean run therefore costs
      nothing, a failed one is itemised per file.

  "verify": "size"
      Compare file sizes.

  "verify": "sha256"
      SHA-256 comparison using buffered read()/pread() + update(). Source and
      destination run in parallel for diff, rsync verification and external
      copy engines. Internal copy fuses source hashing into the copy stream.

  "verify": "blake3"
      BLAKE3 with max_threads=1, buffered through readv()/preadv(). Requires
      the Python module 'blake3'.

  "verify": "blake3thread<N>"
      BLAKE3 with exactly N internal threads per hasher, N >= 1, for example
      blake3thread2 or blake3thread8. Not capped, so faster storage can be
      tuned independently. These modes use parallel source/destination digest,
      so the conservative CPU estimate is max_workers * 2 * N.

  "verify": "blake3threadinga"
      Per-worker CPU-budgeted BLAKE3:

          threads_per_hasher = min(4, max(1, (NCPU - 1) // max_workers))

  For rsync, every mode other than null and size_iferr runs the diff engine
  over the transferred paths after rsync returns. It inherits mtime from the
  rsync configuration, but not permissions and xattr: those are applied by
  the restore at the end of the run, so right after a batch they are not
  there yet. This reads the data a second time on both sides.

  That inline check deliberately skips DIRECTORY metadata. A directory's mtime
  records when an entry was last created in it, so while other workers still
  write into the same destination directories the value is not yet meaningful.
  Run method=diff separately after the job to verify directory metadata.

BUFFERED I/O STRATEGY
=====================

  buffer_mb              integer >= 1, default 8

      Internal copy:
          Controls the copy read size. For verify=sha256/blake3 the same source
          copy buffer is fed directly into the fused hasher. Destination readback
          verification uses the same buffer_mb value. The PATH_MAX/openat copy
          path uses exactly the same setting.

      External copy + hash verify / diff hash verify:
          Controls each digest reader. SHA-256 uses buffered read()/pread();
          BLAKE3 uses a reusable bytearray filled by readv()/preadv(). Source
          and destination are hashed concurrently.

      N:
          Use N MiB instead of the benchmarked/recommended 8 MiB default.

      Memory warnings account for the maximum simultaneously live configured
      buffers per worker: one for normal/fused internal copy, two for parallel
      source/destination digest.

CPU OVERSUBSCRIPTION WARNING
============================

  NCPU                   Number of logical CPUs detected by the program.

  The program reserves one logical CPU. For parallel source/destination digest
  the conservative CPU request is:

      max_workers * 2 * threads_per_hasher

  Therefore sha256/blake3 parallel digest warns when max_workers is greater
  than (NCPU-1)/2. blake3thread<N> additionally multiplies the estimate by N.
  Internal copy with verify=sha256 or verify=blake3 uses fused source hashing,
  so it has only one active hasher per worker. Warnings never reject a run.

COPY PARAMETERS
===============

  method                 "copy"
  engine                 "internal" (default), or an argv template list such as
                         ["/usr/bin/cp", "$SRC", "$DST"]
                         $SRC and $DST must each be standalone argv elements.
                         Both expand to /dev/fd/N (or /proc/self/fd/N) for
                         opened regular source and temporary destination
                         files. The engine must follow these descriptor links
                         when it reads the source; cp -P or cp -d cannot be
                         used for regular-file copies through $SRC.
                         The program must be an absolute path; it is opened
                         and judged once at startup and executed through that
                         descriptor, so the file that was inspected is the one
                         that runs.

                         $DST is NOT a name in the destination tree. A
                         temporary file is created there with O_EXCL, and the
                         engine is handed /dev/fd/N for it: a descriptor has
                         no directory entry to race over, so nothing can be
                         swapped underneath the engine while it writes. The
                         file is renamed into place only after verification.

                         Two consequences for the engine: it writes into an
                         already existing, empty file rather than creating
                         one, and it must accept /dev/fd/N for both arguments.
                         cp and dd do; an engine that publishes its
                         output by renaming a name of its own does not fit
                         this contract.

                         Without /dev/fd or /proc/self/fd the external engine
                         is refused, because reopening either pathname would
                         make symlink swaps possible during the copy.

                         Path components are resolved one at a time. Up to and
                         including src_root and dst_root a symlink is followed
                         - a root reached through one is ordinary
                         administration. Below the roots a symlink is refused,
                         because those names come from the input list or from
                         the tree being written, and following one there is
                         how a copy leaves the tree it was told to stay in.
  src_root               required
  dst_root               required
  relative_path          boolean
  verify                 default "size"; values are listed above
  buffer_mb              default 8; copy + hash I/O strategy described above
  copy_timeout           number > 0 in seconds or null, default null
                         Timeout for one external engine subprocess. A file
                         that hits it is retried like any other transient
                         failure, up to max_retries.
  permissions            default "pog"; see METADATA above
  mtime                  boolean, default true; see METADATA above
  xattr                  boolean, default false
                         For external regular-file engines the metadata is
                         applied by parallel_tools after the utility returns.
  fsync                  "hourly" (default), "batch", "file", or null/false
                         Where the durability barrier for written data sits.
                         An fsync buys exactly one thing: crash consistency of
                         the rename. Without it the kernel may make the new
                         name durable before the data behind it, leaving a
                         destination that exists under the right name with
                         unwritten blocks. It does NOT make verification more
                         meaningful - the read-back is served from the page
                         cache either way.

                         The crash it protects against is the machine's: a
                         kernel panic, a power loss, a fenced client. Not the
                         program's. If this tool is killed, the kernel still
                         owns the written pages and flushes them, and an
                         interrupted copy never carries its final name, because
                         the internal engine writes to a temporary name and
                         renames. On Lustre the ordering matters more than on a
                         local filesystem, because the name lives on the MDT
                         and the data on the OSTs: two servers committing
                         independently.

                         "barrier" here means an ordering point in the writes,
                         NOT a rendezvous in the sense of MPI_Barrier. No
                         worker ever waits for another worker. The one that
                         flushes blocks in syncfs(2) until the storage confirms
                         and the others keep writing meanwhile - the GIL is
                         released for the duration. What does couple them is
                         that syncfs(2) is filesystem-wide: the worker inside
                         it waits for everybody's dirty data, its own and that
                         of any other job writing to the same filesystem.

                         What a barrier costs therefore depends on what limits
                         the run, not on how much is copied. Both measured on
                         Lustre:

                           metadata-bound (many small files): nothing. 150
                             files of 4 KiB took 2.65 s with "batch" against
                             2.83 s with no barrier at all - noise. There is
                             nothing to drain and the time goes into MDT round
                             trips.
                           bandwidth-bound (few large files): plenty. 121 GiB
                             in 132 files cost 53 % more time with "batch" than
                             with null at one worker, and 23 % at eight. The
                             flush drains the pipeline that filling the cache
                             and writing to the OSTs otherwise keep going at
                             once; more workers recover part of the overlap,
                             because the ones not inside syncfs keep writing.

                         "hourly" is the default because it keeps the
                         promise at the lowest price: in a run shorter than an
                         hour it means exactly one barrier, at the end, so the
                         run still refuses to report success before flushing,
                         and the per-batch serialisation above is gone. What it
                         gives up against "batch" is the tightness of the
                         window - a node that dies mid-run can cost up to an
                         hour of finished work instead of one batch. That work
                         is not lost, only to be done again: re-run with
                         skip_existing and what never reached the storage is
                         short and gets rewritten. Choose "batch" when the run
                         is metadata-bound anyway, where it costs nothing and
                         makes every finished batch durable.

                         "file"    fsync every file and parent directory as it
                                   is written. Measured on Lustre, one fsync of
                                   a freshly written 4 KiB file took 353 ms of
                                   the 360 ms the whole copy took: 98 % of the
                                   small-file cost. Harmless for large files,
                                   where it amortises over the data.
                                   For method=rsync this adds --fsync, because
                                   rsync writes those files and parallel_tools
                                   never sees the descriptors. --fsync exists
                                   since rsync 3.2.4; with an older rsync the
                                   run warns once and falls back to a syncfs
                                   after each batch and at the end of the run.
                                   That is a weaker promise than the setting
                                   asks for: until a batch ends, more written
                                   data is unflushed than with a per-file
                                   flush. Upgrade rsync, or choose "batch"
                                   deliberately rather than by accident.
                         "batch"   one syncfs() per finished batch. Same bytes
                                   written, but the run waits once per batch
                                   instead of once per file. The barrier is
                                   decided by the number of written OBJECTS,
                                   not bytes: directories, symlinks and empty
                                   files carry no data and are covered too.
                         "hourly"  the scheduler marks a flush due once per
                                   hour and the next worker to finish a batch
                                   performs it, plus one at the end of the run.
                                   The flush therefore lands after the current
                                   batch rather than exactly on the hour, which
                                   is what this level is for; use "batch" when
                                   the window has to be tighter.
                         null      none; the caller is responsible, e.g. with
                                   sync -f on the destination.

                         syncfs(2) flushes the filesystem holding dst_root, not
                         just that subtree - the same scope as `sync -f`.
                         os.sync() is deliberately not used because it would
                         flush every mounted filesystem and stall other jobs.
                         A flush that is attempted and fails makes the run exit
                         with status 1: data that was promised to be durable
                         and is not must not be reported as success. syncfs(2)
                         merely being absent on the platform stays a warning.

  skip_existing          boolean, default false
                         Resume support. Before writing, compare the
                         destination with one fstatat over the already-open
                         parent directory: type, size and every configured
                         metadata class come from that single call. When they
                         all match, the object is counted as skipped_count and
                         nothing is written. Applies to files and symlinks.
                         Directories are always processed: they have to be
                         locked and recorded for the restore at the end
                         whether or not their metadata already matches.

                         This is what makes a stopped job resumable even when
                         the input came from a running "find": re-run find from
                         the start and let the tool skip what is already there.
                         Counting processed files and skipping the first N
                         would NOT work, because parallel workers do not finish
                         batches in input order, so the completed set is not a
                         prefix of the input.

                         Sound because the internal engine writes to a
                         temporary name and renames: an interrupted copy never
                         appears under its final name, so a destination that
                         exists is one that was completed.

                         Measured on Lustre, one lstat costs 15.7 us against
                         about 3 ms for re-applying directory metadata, so a
                         resume scan over three million objects takes under a
                         minute instead of hours.

                         What it does NOT check is content: a source change of
                         identical size is missed with verify="size". Add
                         mtime=true for rsync-style quick-check semantics, or
                         verify the result afterwards with method=diff.

                         A symlink is skipped when its target matches, which
                         costs two readlink calls against recreating the link
                         with symlink + chown + utime + rename.

                         method=rsync has its own ignore_existing, which skips
                         by mere existence without comparing.

  sparse                 boolean, default false
                         Internal engine: a read buffer that contains nothing
                         but NUL bytes is not written; the destination file is
                         advanced with lseek() so the region stays a hole, and
                         the final size is established with ftruncate().
                         A 64 MiB source file holding 2 MiB of data occupied
                         131072 blocks at the destination without this option
                         and 4096 blocks with it.
                         Hash verification is unaffected: the hasher always
                         receives the logical byte stream including the bytes of
                         a skipped hole, so the fused source digest still
                         matches the destination readback. The saved work is
                         therefore I/O, not hashing.
                         Detection costs almost nothing for dense data because
                         the comparison stops at the first non-zero byte.

  For a resolved hard-link group, the primary is always written atomically by
  the internal engine before the other destination names are linked to it.

  Internal hash verification:
      verify=sha256 or verify=blake3 hashes source bytes on-the-fly while they
      are already read for copying; only destination is reread afterwards.

  External hash verification:
      after copy+fsync, source and destination are hashed concurrently.

  Directories and symlinks keep the internal non-recursive/openat handling.
  For regular files with max_path_length="engine": an internal engine switches
  to openat when system PATH_MAX is reached; an external engine receives the
  pathname unchanged. parallel_tools uses openat-safe post-copy verification for
  such external long paths. With numeric max_path_length no automatic long-path
  fallback is performed.

RSYNC PARAMETERS
================

  method                 "rsync"
  src_root               required
  dst_root               required
  relative_path          boolean
  copy_timeout           number > 0 in seconds or null, default null
                         Timeout for the rsync subprocess.
  rsync_zero_bytes_timeout
                         number > 0 in seconds, default 300
                         Passed to rsync as --timeout, which aborts the transfer
                         after this many seconds without I/O. The attempt is then
                         retried like any other transient failure.

  max_path_length="engine"
                         PATH_MAX-sized paths are copied with the internal
                         openat engine; normal paths remain in the rsync batch.
  max_path_length="rsync"
                         All allowed paths, including PATH_MAX-sized paths, are
                         passed directly to rsync.
  verify                  default "size_iferr"; see VERIFY MODES above
  buffer_mb               default 8; used only by the hash verify modes
  permissions             default "pog"; NOT passed to rsync. Owner, group
                          and mode of files, symlinks and directories are
                          recorded and applied by the restore at the end,
                          while the directories stay locked during the run.
                          rsync runs with --chmod=D0700.
  mtime                   boolean, default true; adds -t for files and -O:
                          directory times are set by the restore at the end
  xattr                   boolean, default false
                          Not passed to rsync either (-X is not used): xattrs
                          and ACLs are recorded and applied at the end.
  require_destination_protection
                          boolean, default false; see DESTINATION PROTECTION
  sparse                  boolean, default false
                          Run rsync with --sparse so runs of zero bytes become
                          holes at the destination instead of allocated blocks.
  ignore_existing         boolean, default false
                          Existing destination files keep their metadata as
                          well: nothing is recorded for them. Existing
                          directories still get the source values, as rsync
                          does in this mode.
                          Run rsync with --ignore-existing, which skips every
                          destination name that already exists. This trades
                          correctness for speed: an existing but outdated or
                          truncated destination file is never repaired. It is
                          deliberately NOT applied to a resolved hard-link
                          group, where the primary must be written as a fresh
                          inode.

  rsync is never asked to reason about hard links; -H is not used at all. A
  resolved group hands rsync only its primary name, with --ignore-times so the
  destination gets a fresh inode. Every alias is then created by parallel_tools
  with linkat and the resulting st_nlink is verified exactly.

DIFF PARAMETERS
===============

  method                 "diff"
  src_root               required
  dst_root               required
  relative_path          boolean
  verify                 default "sha256"; values are listed under VERIFY MODES
  buffer_mb              default 8; parallel buffered hashing described above
  permissions            default "pog"; compared, not transferred
  mtime                  boolean, default true; compared, not transferred
  xattr                  boolean, default false; compared, not transferred

  Metadata is compared for files, symlinks AND directories. A destination
  produced by copy or rsync with the same settings therefore verifies cleanly,
  while one produced with fewer settings reports the difference.

  In a preservation mode, only the lexicographically first source pathname of
  each fully resolved group is compared.

MIGRATE PARAMETERS
==================

migrate is configured with 'create migrate' and run with 'start'. It is not
available through '-p', which takes a source and a destination tree; a
migration changes the layout of one tree in place.

Four settings are written as null by 'create' and must be filled in:
src_root, stripcount, banned_osts and keep_mirroring. A configuration that
still has any of them null is refused at start, and the refusal names them.

Every Lustre layout decision is made by the helper program
lustre-migrate-file, which is started once per file. This program supplies
the policy, the batching, the schedule and the first-seen file identity; it
does not read or judge layouts itself.

'create migrate migrate.cfg' writes this file; the four nulls are the ones
to replace, and a filled-in result looks like:

  {{
    "method": "migrate",
    "src_root": "/lustre/project",
    "stripcount": 2,
    "banned_osts": [10, 11],
    "keep_mirroring": false,

    "poolname": null,
    "allowed_osts": [],
    "migrate_helper": null,
    "migrate_allocation_attempts": 32,
    "migrate_timeout": null,
    "hard_link": null,
    "max_workers": 8
  }}

The first five keys are required; the rest are shown at their defaults and
may be left out entirely.

  method                 "migrate"
  src_root               required
                         The boundary the helper resolves within: it opens
                         every path component below this directory with
                         O_NOFOLLOW and refuses anything that would leave it.
                         "/" is refused - a boundary of everything is the
                         absence of one. With hard_link="lustre2lustre" every
                         FID-resolved hard-link name must remain inside it as
                         well.
  stripcount             integer 1..2000, required
                         Stripe count for newly created mirrors. The upper
                         bound is the helper's, checked here so a run refuses
                         to start instead of failing once per file.
  banned_osts            required, list of OST indexes, e.g. [10, 11, 16]
                         OSTs from which existing allocations must be removed.
  keep_mirroring         boolean, required
                         false: end with exactly one mirror.
                         true:  keep the number of mirrors the file had.
                                Before it changes anything, the helper records
                                that number on the file itself: as
                                trusted.lustre_migrate_file.mirror_goal under
                                root, and user.lustre_migrate_file.mirror_goal
                                otherwise. The trusted namespace needs
                                CAP_SYS_ADMIN, so the owner of a file cannot
                                plant a number there and steer a root-run
                                migration with it.

                                A run resuming after a crash therefore aims at
                                the original number instead of at what is
                                left, and it does so without being told: the
                                helper reads its own record. The record is
                                removed once the end state is verified; a file
                                still carrying it is a file whose migration
                                did not finish.

                                If the record cannot be written, the helper
                                refuses to change the layout rather than doing
                                the dangerous half of the work without the
                                protection.
  poolname               string or null, default null
                         OST pool for newly created mirrors. A pool that
                         merely CONTAINS a banned OST is accepted - enough
                         others may remain, and refusing it would rule out a
                         workable configuration. Where Lustre allocates inside
                         the pool cannot be known in advance, so a mirror that
                         lands on a banned OST is removed and retried; see
                         migrate_allocation_attempts. Refused up front are a
                         pool naming another filesystem and a pool with fewer
                         usable OSTs than the mirror needs stripes.
  allowed_osts           list of OST indexes, default []
                         Where a NEW mirror may be placed. Empty means Lustre
                         chooses. Naming exactly stripcount entries pins the
                         placement; naming more lets the helper pick that many
                         and spread them across files, so a whole tree does not
                         land on the same OSTs. This says nothing about which
                         mirrors must be evacuated - that is banned_osts.

                         For both lists: indexes are 0..65535, must not repeat
                         inside one list, and must not appear in both. A
                         repeated index is not harmless arithmetic - the
                         helper counts the OSTs outside the banned set by
                         subtracting the length of that list, so a duplicate
                         can refuse the whole run with ENOSPC.
  migrate_helper         absolute path or null, default null
                         Where to find lustre-migrate-file. null looks next
                         to this script first and then along PATH. The
                         executable is opened, checked and run through its
                         own descriptor, so the file that was inspected is
                         the file that runs. It must not be writable by
                         group or other, and under root it must be owned by
                         root.
  migrate_allocation_attempts
                         integer >= 1, default 32
                         How often the helper may remove a newly created
                         mirror that landed on a banned OST and allocate
                         again.
  migrate_timeout        number >= 1 in seconds or null, default null
                         Wall-clock limit for one helper run, after which the
                         helper is terminated. Deliberately without a default,
                         and deliberately the only automatic way a helper is
                         cut short.

                         Terminating a helper cannot lose data: it deletes a
                         mirror only under a write lease and only while
                         another usable mirror exists, and a signal cannot
                         tear that operation apart. What it can leave is work
                         half done - a mirror that was added but not yet
                         verified, or, in a file that needed several
                         deletions, fewer mirrors than it started with. With
                         keep_mirroring and no explicit target mirror count a
                         later run then adopts the reduced count as the new
                         target and the redundancy is gone without a word.
                         Those files appear in the unfinished-migration
                         report, and a repair pass finishes them - but that is
                         a cost the operator has to choose, not one the
                         program picks by guessing at an honest limit for a
                         file whose size it does not know.

                         Without it the wait is unbounded in time but never
                         silent: a stop signal or the end of the run schedule
                         while a helper runs is logged with the file that is
                         still being finished.

IMPORTANT FOR MIGRATE:
  The affected OSTs must already be prevented server-side from receiving NEW
  allocations. parallel_tools removes existing allocations; it does not itself
  prevent new allocations from being placed on those OSTs.

  This is not advice, it is a precondition, and a PFL file shows why. A
  component beyond the current end of the file owns no object yet, so there is
  nothing there to evacuate - but it will be allocated somewhere the first
  time somebody writes that far, and that decision is Lustre's, long after
  this run has finished. Without a pool the target is unknown and cannot be
  judged. A pool narrows it but does not decide it: the pool is checked for
  having enough usable OSTs, not for being free of banned ones, so a later
  write can still land on one. So for such a file "no objects on a banned OST"
  describes the state at the end of the run and promises nothing about the
  next write. A lasting exclusion needs the server-side rule, or a pool that
  contains none of the banned OSTs in the first place.

FINISHING A MIGRATION LATER
---------------------------
  A file that is written while it is being migrated makes the helper stop:
  the data version moved, or the lease was broken, and neither is a reason to
  publish a mirror that matches no version of the file. After the configured
  number of attempts the run gives up on that file.

  The file is then not damaged - every step is verified before the next, and
  a mirror is only deleted under a write lease with the layout read again -
  but it may be half migrated: a mirror was added, or the old one was not
  removed yet. The batch record in the JSON log says which files those are:

      "migrate_unfinished_count": 3,
      "migrate_layout_changed_count": 1,
      "migrate_unfinished_requeued_count": 1,
      "migrate_unfinished": [
        {{"path_b64": "...", "layout_changed": true, "stage": "extend",
         "errno": 11, "retryable": true, "requeued": false,
         "initial_mirror_count": 1,
         "mirrors_added": 1, "mirrors_deleted": 0,
         "message": "lost the lease during the copy: ..."}}
      ]

  layout_changed is the distinction that matters: true means the layout was
  already altered and the file needs a later run to finish it; false means
  nothing happened to it; null means the helper gave no usable answer, so
  nothing is claimed.

  requeued says who has to act. false: the run gave up on this file, and it
  is in the list below or nowhere. true: the run was stopped while this file
  still had attempts left, so it went back on the queue and is in
  remaining_tasks_file if one is configured - a later run reaches it without
  anyone listing it by hand. Those entries are reported but are NOT counted
  as failures, because they are none; failed_count and the exit status are
  unaffected by them.

  A stop is when a half-migrated file is most likely, so this is exactly the
  case worth reading. A helper cut short by a second stop signal lands here
  with layout_changed set by its last record, or null when it was killed
  before it could answer.

  Rebuilding an input list from the log, byte for byte including names that
  are not valid UTF-8:

      jq -r '.migrate_unfinished[]? | select(.layout_changed) | .path_b64' \\
          run.jsonlog |
      while read -r b64; do
          printf '%s\\000' "$(printf '%s' "$b64" | base64 -d)"
      done > unfinished.txtz

      {prog} start migrate.cfg < unfinished.txtz

  The second run needs no special option. Every invocation starts from the
  layout that is actually there, so finishing a half-migrated file and
  migrating an untouched one are the same operation.


PREDICTABLE OUTPUT FILES AND ROOT
=================================

  The program is routinely run as root, and several of the files it writes
  have names an unprivileged user can guess: the project .cfg, .jsonlog and
  .log, the generated input list and remaining_tasks_file. The state files
  inside working_directory are the exception: their names carry a random block
  and the directory itself is checked before anything is written into it.

  The programs it STARTS are treated the same way. rsync, lfs, sh, find, tee
  and a configured external copy engine are opened once, judged on the
  resulting descriptor - regular file, executable, not writable by group or
  other, owned by root when this runs as root, and no directory in its path
  writable by anyone else - and then executed through /proc/self/fd, so the
  file that was inspected is the file that runs. Checking a pathname and then
  executing that pathname would leave a window in which the name can be
  pointed somewhere else.

  Under root a program that fails these checks is REFUSED, at startup, before
  any work begins. Unprivileged it is only reported: the exposure is then to
  the caller's own account, an ordinary package manager installs into exactly
  such a directory, and refusing would block the case the check does not
  protect anyway. Where /proc is unavailable the program is run by pathname
  and that is said once.

  None of them is validated by inspecting the path first. Checking a name and
  then opening it is a time-of-check-to-time-of-use race, and losing that race
  once is enough. The guarantees come from the open itself instead:

      O_NOFOLLOW   refuses a symlink at the final component
      O_EXCL       refuses any pre-existing name, where a fresh file is meant
      fstat()      inspects the object actually opened and rejects a
                   non-regular, hard-linked or foreign-owned file

  The generated input list is created by parallel_tools with O_CREAT|O_EXCL|
  O_NOFOLLOW and handed to tee as an already-open descriptor (/dev/fd/N), so
  tee never performs a second name lookup of its own.

  O_NOFOLLOW protects the final component only. A symlinked parent directory
  is not covered, so these outputs belong in a directory that unprivileged
  users cannot modify. The program warns when it is group- or world-writable.

RELOAD / SIGNAL BEHAVIOR
========================

  {prog} reload PID
      Send SIGHUP. CONFIG is read and validated again. method, hard_link,
      src_root, dst_root, and relative_path cannot be changed while a run is
      active. max_workers and queue_maxsize can be changed dynamically. Changing
      batch_max_files re-batches queued normal work without splitting special
      hard-link tasks. reload_input_file can inject additional paths only while
      the normal discovery phase is still active.

  {prog} stop PID
      Send SIGTERM. No new worker jobs are started. Remaining queued paths can
      be saved through remaining_tasks_file. A second stop signal says the
      operator is no longer willing to wait for the work already under way: a
      running migrate helper is then terminated and its file is reported as
      unfinished.

FINAL STATISTICS AND EXIT CODES
===============================

  The last line on stdout is one JSON record with the whole-run figures (see
  OUTPUT STREAMS for how to keep stdout and stderr apart). Every
  accepted input record ends up in exactly one of three buckets:

      total_input_record_count = total_object_count
                               + total_failed_count
                               + unstarted_path_count

  total_input_record_count   Records accepted from stdin and from
                             reload_input_file. This is the reference number.
  total_object_count         Objects processed, including those that
                             skip_existing found already current.
  total_skipped_count        How many of those were skipped, a subset of the
                             line above.
  total_failed_count         Paths that could not be processed.
  unstarted_path_count       Paths still queued that no worker had started.
                             Written to remaining_tasks_file when configured;
                             unstarted_paths_saved says whether that happened.
  accounted_path_count       The sum of the three buckets.
  hardlink_merged_group_resolutions
                             How often two workers resolved the same inode
                             independently. Informational only: it depends on
                             where the batch boundaries fell and says nothing
                             about how many input records there were.
  path_accounting_balanced   Whether the three buckets equal the reference.
                             Every input record is represented exactly once,
                             repeated names included, so a false here means
                             the run cannot account for a record it accepted -
                             and the run exits 1 for it.
  input_complete             False when the input stream ended abnormally.
  run_aborted                False for every run that reached its own end -
                             successful, failed or stopped alike. True only
                             when the run died on an unhandled exception. The
                             numbers above are then whatever had been
                             collected by that point and were not counted to
                             the end; run_abort_reason names the exception.
                             The field is always present, so a consumer can
                             test it rather than having to notice the absence
                             of this record. Before, a dying run wrote no
                             statistics record at all and "tail -1" quietly
                             returned the last batch instead.

  What the balance does NOT tell you is whether the producer reached the end
  of its tree: a "find" that dies halfway simply delivers fewer records, and
  those few add up perfectly. Two things answer that question. input_complete
  is false whenever the producer left the abort marker, which the -p launcher
  arranges. And with a saved list, the reference can be compared directly:

      tr -cd '\\0' < list.txtz | wc -c      # records in the list
      jq .statistics.total_input_record_count run.jsonlog | tail -1

  metadata_restore_incomplete is true when a metadata record could not be
  written, or the restore at the end failed or could not run. It always ends
  the run with exit status 1. A STOPPED run still restores: no batch is in
  flight any more at that point, and leaving the tree locked would be worse.

  Exit codes:

      0   Input read completely, nothing failed, nothing left over.
      1   Something failed: input incomplete, failed paths, unsaved work,
          destination metadata not restored, a flush that did not succeed,
          owner/group that could not be applied, or - with
          require_destination_protection - a destination that was not fully
          protected.
      2   The run stopped early. Nothing failed, but stopped_batch_count or
          unstarted_path_count is non-zero, so work remains to be done.
"""

    p_start = subparsers.add_parser(
        "start",
        help="process paths using a JSON configuration",
        description=start_description,
        epilog=start_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p_start.add_argument(
        "config",
        metavar="CONFIG",
        help="path to the JSON configuration file",
    )

    # ------------------------------------------------------------------
    # stop
    # ------------------------------------------------------------------
    p_stop = subparsers.add_parser(
        "stop",
        help="stop a running instance gracefully",
        description=(
            "Send SIGTERM to a running instance. Current worker jobs are allowed "
            "to finish; queued paths that have not yet started can be saved via "
            "remaining_tasks_file. Sending it a second time terminates a "
            "running migrate helper instead of waiting for it."
        ),
        epilog=(
            f"Examples:\n"
            f"  {prog} stop /srv/jobs/test.log\n"
            f"  {prog} stop 12345"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p_stop.add_argument(
        "target",
        metavar="PID|LOGFILE",
        help=TARGET_HELP,
    )

    # ------------------------------------------------------------------
    # pause / resume
    # ------------------------------------------------------------------
    for name, signal_name, what in (
        ("pause", "SIGUSR1", "hold a running instance"),
        ("resume", "SIGUSR2", "let a paused instance carry on"),
    ):
        p_hold = subparsers.add_parser(
            name,
            help=what,
            description=(
                f"Send {signal_name} to a running instance. "
                + (
                    "Workers finish the file they are on and then stop taking "
                    "new ones; the rest of their batch goes back on the queue "
                    "and nothing is discarded. The run stays alive, keeps its "
                    "queue and its statistics, and reacts to a stop signal "
                    "immediately while paused. A hard-link group is finished "
                    "as a unit, and a running migrate helper is allowed to "
                    "finish."
                    if name == "pause"
                    else "Workers pick their queued work up again where they "
                    "left off. Resuming a run that is not paused does nothing."
                )
                + " The target is identified before the signal is sent and "
                "reported when it does not look like a parallel_tools run, "
                "but a pid you name is signalled either way: without a "
                "handler these signals terminate a process, so read that line "
                "if it appears."
            ),
            epilog=(
                f"Examples:\n"
                f"  {prog} {name} /srv/jobs/test.log\n"
                f"  {prog} {name} 12345"
            ),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            allow_abbrev=False,
        )
        p_hold.add_argument(
            "target",
            metavar="PID|LOGFILE",
            help=TARGET_HELP,
        )

    # ------------------------------------------------------------------
    # reload
    # ------------------------------------------------------------------
    p_reload = subparsers.add_parser(
        "reload",
        help="reload configuration for a running instance",
        description=(
            "Send SIGHUP to a running instance. CONFIG is validated and applied "
            "again. method, hard_link, src_root, dst_root, and relative_path "
            "cannot be changed while a run is active."
        ),
        epilog=(
            f"Examples:\n"
            f"  {prog} reload /srv/jobs/test.log\n"
            f"  {prog} reload 12345"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p_reload.add_argument(
        "target",
        metavar="PID|LOGFILE",
        help=TARGET_HELP,
    )

    # ------------------------------------------------------------------
    # create
    # ------------------------------------------------------------------
    create_description = """Create a default JSON configuration.

Supported METHOD values: rsync, copy, diff, migrate.

For migrate, the four settings that decide which OSTs are emptied and how
many mirrors a file keeps - src_root, stripcount, banned_osts,
keep_mirroring - are written as null and have to be filled in before the
configuration can be started. Everything else is written at its default.
"""

    create_epilog = f"""
SYNTAX
======
  {prog} create METHOD CONFIG

EXAMPLES
========
  {prog} create rsync rsync.cfg
  {prog} create copy  copy.cfg
  {prog} create diff  diff.cfg

GENERATED DEFAULTS
==================

Common:
  max_workers          8
  batch_max_files      100
  max_retries          7
  reload_input_file    null
  remaining_tasks_file null
  queue_maxsize        1000
  stdin_chunk_size     16777216
  max_input_record_bytes 8388608
  retry_backoff_base   2.0
  retry_backoff_max    60.0
  run_schedule         null
  max_path_length      "engine"
  hard_link            null
  spill_compression    "zlib"
  working_directory    ".parallel_tool_workdir"
  metadata_maxsize     "100MiB"

rsync additionally:
  method                   "rsync"
  # max_path_length="engine" redirects PATH_MAX paths to internal openat.
  # Set max_path_length="rsync" to delegate them directly to rsync.
  verify                   "size_iferr"
  buffer_mb                8
  copy_timeout             null
  rsync_zero_bytes_timeout 300
  relative_path            false
  xattr                    false
  permissions              "pog"
  mtime                    true
  sparse                   false
  fsync                    "hourly"
  ignore_existing          false
  require_destination_protection false
  src_root                 "/src"
  dst_root                 "/dst"

copy additionally:
  method                   "copy"
  engine                   "internal"
  copy_timeout             null
  verify                   "size"
  buffer_mb                8
  relative_path            false
  xattr                    false
  permissions              "pog"
  mtime                    true
  sparse                   false
  fsync                    "hourly"
  require_destination_protection false
  src_root                 "/src"
  dst_root                 "/dst"

diff additionally:
  method                   "diff"
  verify                   "sha256"
  buffer_mb                8
  relative_path            false
  xattr                    false
  permissions              "pog"
  mtime                    true
  src_root                 "/src"
  dst_root                 "/dst"

For the meaning of every parameter:
  {prog} start --help
"""

    p_create = subparsers.add_parser(
        "create",
        help="create a new default JSON configuration",
        description=create_description,
        epilog=create_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    # Deliberately not argparse's own choices=: its message only lists what
    # is allowed, and the interesting question for the one method that is
    # missing is why. That is answered where the value is checked.
    # Deliberately not argparse's own choices=: its message only lists what
    # is allowed, while the useful part is which settings a method still
    # needs. That is said where the value is checked and after the file is
    # written.
    p_create.add_argument(
        "method",
        metavar="METHOD",
        help="processing method: rsync, copy, diff, migrate",
    )
    p_create.add_argument(
        "config",
        metavar="CONFIG",
        help="path of the JSON configuration file to create",
    )

    return parser

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == LFS_HELPER_COMMAND:
        return lfs_helper_main()

    parser = build_argument_parser()
    args = parser.parse_args()

    if getattr(args, "command", None) == "create":
        if args.method not in ("rsync", "copy", "diff", "migrate"):
            parser.error(
                f"invalid method {args.method!r} for create; choose rsync, "
                f"copy, diff or migrate"
            )

    if getattr(args, "project", None):
        if len(args.project) not in (4, 5):
            parser.error(
                "-p/--project requires METHOD NAME SRC DST and optionally INPUT"
            )
        method, project, src_dir, dst_dir = args.project[:4]
        input_file = args.project[4] if len(args.project) == 5 else None
        if method not in ("rsync", "copy", "diff"):
            parser.error("-p/--project supports only rsync, copy, and diff; migrate requires an explicit config")
        if getattr(args, "workers", None) is not None and args.workers < 1:
            parser.error("-j/--workers must be >= 1")
        try:
            return create_project_and_start(
                method,
                project,
                src_dir,
                dst_dir,
                input_file,
                workers=getattr(args, "workers", None),
            )
        except (OSError, ValueError) as e:
            parser.error(str(e))

    if getattr(args, "workers", None) is not None:
        parser.error("-j/--workers is valid only together with -p/--project")

    if args.command is None:
        parser.print_help(sys.stderr)
        return 2

    if args.command == "stop":
        return signal_target(args.target, signal.SIGTERM, "SIGTERM")

    if args.command == "reload":
        return signal_target(args.target, signal.SIGHUP, "SIGHUP")

    if args.command == "pause":
        return signal_target(args.target, signal.SIGUSR1, "SIGUSR1 (pause)")

    if args.command == "resume":
        return signal_target(args.target, signal.SIGUSR2, "SIGUSR2 (resume)")

    if args.command == "create":
        cfg = create_default_config_for_method(args.method)
        with open(
            open_protected_file(args.config, append=False, mode=0o644),
            "w",
            encoding="utf-8",
            closefd=True,
        ) as fp:
            json.dump(cfg, fp, indent=4)
        print(f"{args.config} created")

        # Only migrate has these keys at all. Asking any configuration whether
        # they are null answered "yes" for rsync, copy and diff too, and those
        # runs were then told to fill in settings their method does not have.
        unfilled = [
            key for key in MIGRATE_REQUIRED_KEYS
            if args.method == "migrate" and cfg.get(key) is None
        ]
        if unfilled:
            # Said here and not only at start time: the person who writes the
            # file is the one who knows these answers, and finding out at the
            # first run costs them a round trip.
            print(
                f"{args.config} cannot be started as it is. These settings "
                f"are null and must be filled in first:"
            )
            for key in unfilled:
                print(f"  {key:<16} {MIGRATE_REQUIRED_HINTS[key]}")
            print(
                "They have no defaults because they decide which OSTs are "
                "emptied and how many mirrors a file keeps. The remaining "
                "keys are at their defaults and may be left as they are; "
                "'parallel_tools.py start --help' describes all of them "
                "under MIGRATE PARAMETERS."
            )
        return 0

    if args.command != "start":
        parser.print_help(sys.stderr)
        return 2

    config_path = args.config

    signal.signal(signal.SIGHUP, handle_sighup)
    # Without a handler the default action for both of these is to TERMINATE
    # the process. Registering them is therefore not only how pause works, it
    # is also what keeps a mistyped pid from killing this run.
    signal.signal(signal.SIGUSR1, handle_pause)
    signal.signal(signal.SIGUSR2, handle_resume)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    try:
        initial_cfg = load_config_file(config_path)
        validate_helper_file_paths(initial_cfg, config_path)
    except Exception as e:
        log(f"initial config load failed: {e}")
        return 1

    # Pin the external programs this method will use before any work starts.
    # Finding out per batch would report the same refusal once per batch and
    # leave the operator guessing whether anything was done.
    #
    # This script itself is pinned here too, although nothing needs it yet.
    # It used to be opened lazily, at the first self-invocation - which for a
    # plain "start" run is the hard-link phase, hours in. By then the file may
    # have been renamed, moved or replaced, and the descriptor that is meant
    # to guarantee "what was inspected is what runs" would be taken of
    # whatever holds the name at that later moment, or of nothing at all. A
    # run that has begun should not depend on its own script's NAME surviving
    # to the end.
    try:
        self_script_argv0()
        check_configured_copy_engine(initial_cfg)
        check_hardlink_backends(initial_cfg)
        for program in required_external_programs(initial_cfg):
            resolve_trusted_executable(program)
        if initial_cfg["method"] == "migrate":
            log(f"migrate helper: {check_migrate_helper_version(initial_cfg)}")
    except Exception as e:
        log(f"external program check failed: {e}")
        return 1

    global config
    with config_lock:
        config = dict(initial_cfg)

    global task_queue
    task_queue = queue.Queue(maxsize=initial_cfg["queue_maxsize"])

    # Wall-clock reference for the final whole-run statistics.  This is not the
    # sum of per-worker durations; parallel worker time must not be double-counted.
    operation_started_monotonic = time.monotonic()

    warn_if_workers_exceed_cpu_budget(initial_cfg["max_workers"], initial_cfg)
    # Judged while the operator is still watching, and not created: most runs
    # never need it.
    prepare_working_directory(initial_cfg)
    warn_if_hash_threads_exceed_cpu_budget(initial_cfg)
    warn_if_buffer_exceeds_memory_budget(initial_cfg)

    # copy and rsync write into the destination, and they do so into a
    # locked tree: dst_root is opened once and held, the directories are
    # locked, and the metadata they - and for rsync the files - must carry
    # is recorded and applied at the end. diff and migrate write no
    # destination directories and need none of this.
    if initial_cfg["method"] in ("copy", "rsync"):
        global destination_guard, destination_metadata_log
        global destination_file_metadata_log
        maxsize = int(initial_cfg["metadata_maxsize"])
        if initial_cfg["method"] == "rsync":
            # Two logs share the budget: directories, and rsync's files.
            maxsize = max(1, maxsize // 2)
            destination_file_metadata_log = MetadataLog(
                maxsize, initial_cfg["spill_compression"],
                initial_cfg.get("dst_root"), label="dstfiles",
            )
        destination_metadata_log = MetadataLog(
            maxsize, initial_cfg["spill_compression"],
            initial_cfg.get("dst_root"), label="dstdirs",
        )
        guard = DestinationGuard(initial_cfg)
        destination_guard = guard
        try:
            guard.start()
        except Exception as exc:
            destination_guard = None
            guard.close()
            log(f"cannot prepare the destination: {exc}")
            return 1
        log(
            f"destination metadata: recorded in memory, compression="
            f"{initial_cfg['spill_compression']}, spilled above "
            f"{human_bytes(int(initial_cfg['metadata_maxsize']))} of "
            f"compressed data"
        )

    resize_workers(initial_cfg["max_workers"])

    verify_text = (
        f" verify_mode={initial_cfg.get('verify')}"
        if "verify" in initial_cfg
        else ""
    )
    engine_text = (
        f" engine={initial_cfg.get('engine')}"
        if initial_cfg.get("method") == "copy"
        else ""
    )
    buffer_text = (
        f" buffer_mb={initial_cfg.get('buffer_mb')}"
        if "buffer_mb" in initial_cfg
        else ""
    )
    path_text = (
        f" max_path_length={initial_cfg.get('max_path_length')}"
        f" system_PATH_MAX={get_system_path_max()}"
    )
    cpu_text = (
        f" NCPU={get_logical_cpu_count()} cpu_worker_budget={get_worker_cpu_budget()}"
    )
    blake3_thread_text = (
        f" blake3_threads_per_hasher={get_blake3_threads_for_mode(initial_cfg.get('verify'), initial_cfg['max_workers'])}"
        if is_blake3_mode(initial_cfg.get("verify"))
        else ""
    )
    log(
        f"initial config applied: max_workers={initial_cfg['max_workers']} "
        f"method={initial_cfg['method']} "
        f"hard_link={initial_cfg.get('hard_link')!r}"
        f"{engine_text}{verify_text} "
        f"queue_maxsize={initial_cfg['queue_maxsize']}"
        f"{buffer_text}{path_text}{cpu_text}{blake3_thread_text}"
    )

    reader = threading.Thread(
        target=stdin_reader_main,
        daemon=True,
        name="stdin-reader",
    )
    reader.start()

    # The exit code is decided AFTER the shutdown work below, because saving
    # the not-yet-started paths can fail and that failure has to be visible in
    # the status rather than only in the log.
    # Filled by the scheduler loop itself rather than assigned from its return
    # value: an exception never reaches the assignment, and the final record
    # would then report a run in which nothing had happened.
    run_stats = new_run_stats()
    # Kept so the final record can say that the run did not end on its own
    # terms. A statistics record that looks ordinary after a crash is worse
    # than none at all, because it reads as a complete run.
    run_error: Optional[BaseException] = None
    try:
        scheduler_loop(config_path, operation_started_monotonic, run_stats)
    except BaseException as exc:
        run_error = exc
        raise
    finally:
        shutdown_event.set()
        log("main shutdown: waiting for reader thread")
        reader.join(timeout=10.0)

        # Workers may still be running; closing is done once they are joined.
        log("main shutdown: waiting for worker threads")
        with workers_lock:
            worker_list = list(workers)

        for t in worker_list:
            t.join(timeout=WORKER_JOIN_TIMEOUT_SEC)

        # Results produced while the workers were finishing arrive after the
        # scheduler loop has returned. Without this they would neither be
        # printed nor counted.
        emit_results(run_stats)

        # Normally already done by the scheduler. Reached with the guard still
        # set only when the scheduler did not get that far - an exception.
        # The restore is safe only if every worker is gone; otherwise the
        # tree stays locked and the journal is kept for the next start.
        with workers_lock:
            writers_gone = not any(t.is_alive() for t in workers)
        finalize_destination_metadata(
            apply_restore=writers_gone, run_stats=run_stats
        )

        final_cfg = get_config_snapshot()
        configured_rest = final_cfg.get("remaining_tasks_file")
        # Asked FIRST, so a run that has nothing left over creates neither a
        # working directory nor an empty file. Most runs end that way.
        outstanding = count_unstarted_paths()
        run_stats["unstarted_path_count"] = outstanding
        run_stats["unstarted_paths_saved"] = False

        if outstanding:
            # null no longer means "throw it away". It used to, and a run that
            # was stopped without anyone having configured a file said so and
            # dropped the work anyway. A name is picked inside the checked
            # working directory instead; an absolute one the operator gave is
            # used as given.
            target = resolve_run_state_path(
                configured_rest, "remaining_tasks", ".txtz"
            )
            if target is None:
                # The working directory was refused - it sits in the data
                # tree, or somebody else owns it. That is a good reason not to
                # keep state there, and a bad reason to throw the state away:
                # the list is the only record of what was never started. It
                # goes next to the configuration instead, created the same
                # careful way, and the run says where it went.
                target = os.path.join(
                    os.path.dirname(os.path.abspath(config_path)) or ".",
                    run_state_name("remaining_tasks", ".txtz"),
                )
                log(
                    f"main shutdown: the working directory cannot be used "
                    f"({working_directory_problem()}); saving the "
                    f"{outstanding} not-yet-started paths next to the "
                    f"configuration instead"
                )
            try:
                saved = save_remaining_tasks_to_file(target)
                run_stats["unstarted_path_count"] = saved
                run_stats["unstarted_paths_saved"] = True
                log(
                    f"main shutdown: saved {saved} not-yet-started queued "
                    f"paths to {target}"
                )
            except Exception as e:
                run_stats["unstarted_path_count"] = count_unstarted_paths()
                log(
                    f"main shutdown: failed to save remaining queued paths: {e}"
                )
                shutdown_save_failed.set()

        log("main shutdown complete")

        # In the finally, not after it: an exception escaping scheduler_loop
        # used to skip this entirely, and the JSON log then ended on an
        # ordinary batch record. A reader doing "tail -1 | jq .statistics"
        # got that batch instead and no indication anything was wrong.
        try:
            emit_final_statistics(
                run_stats, operation_started_monotonic, aborted=run_error
            )
        except Exception as exc:
            # Never let this replace the exception that is already on its way
            # out; that one says why the run ended.
            log(
                f"WARNING: the final statistics record could not be written: "
                f"{exc}"
            )

    # An exit code of 0 must mean "input fully read, nothing failed, and any
    # unfinished work was preserved", otherwise callers cannot detect a
    # partially failed run at all.
    if input_failed_event.is_set():
        log("exit status 1: input stream was not read completely")
        return 1
    if shutdown_save_failed.is_set():
        log("exit status 1: not-yet-started work could not be saved")
        return 1
    if run_stats.get("failed_batch_count") or run_stats.get("total_failed_count"):
        log(
            "exit status 1: "
            f"{run_stats.get('total_failed_count', 0)} failed paths in "
            f"{run_stats.get('failed_batch_count', 0)} failed batches"
        )
        return 1

    # After the failed-path check on purpose: when a run both failed paths and
    # cannot account for every record, the failure count is what the operator
    # acts on. This one catches the case where nothing failed and the numbers
    # still do not add up, which is the more unsettling of the two.
    if not run_stats.get("path_accounting_balanced", True):
        log(
            f"exit status 1: the run cannot account for every input record "
            f"({run_stats.get('accounted_path_count')} accounted against "
            f"{run_stats.get('total_input_record_count')} accepted)"
        )
        return 1
    if metadata_restore_incomplete.is_set():
        log(
            "exit status 1: destination metadata could not be fully recorded "
            "or restored"
        )
        return 1
    protection = run_stats.get("destination_protection_summary") or {}
    if (
        get_config_snapshot().get("require_destination_protection", False)
        and protection.get("destination_protection") not in (None, "enforced")
    ):
        log(
            f"exit status 1: require_destination_protection is set and the "
            f"destination was protected only "
            f"{protection.get('destination_protection')!r}"
        )
        return 1
    if sync_failed_event.is_set():
        log("exit status 1: the destination filesystem could not be flushed")
        return 1
    if run_stats.get("ownership_not_preserved_count"):
        # Documented under "permissions": an unprivileged run should ask for
        # "p" instead of silently producing a destination with the wrong
        # owners.
        log(
            "exit status 1: owner/group could not be applied to "
            f"{run_stats['ownership_not_preserved_count']} objects; "
            "use permissions without \"o\"/\"g\" for an unprivileged run"
        )
        return 1
    # stopped_batch_count alone does NOT mean the run is incomplete. A batch
    # that was cut short put its remaining paths back on the queue: if the run
    # then ended, they are counted in unstarted_path_count, and if it carried
    # on - after a pause, or when the schedule opened again - they were
    # processed. Counting the interruption itself made a paused run that
    # afterwards copied everything report exit 2.
    if run_stats.get("unstarted_path_count"):
        # Queued paths that were never started produce no batch result at all,
        # so without this a run stopped before its first batch was dispatched
        # would report nothing but successes.
        log(
            "exit status 2: run stopped before all work was processed "
            f"({run_stats.get('unstarted_path_count', 0)} paths were never "
            f"started; {run_stats.get('stopped_batch_count', 0)} batches were "
            f"cut short)"
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
