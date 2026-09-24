# parallel_tools

Parallel copying, verification and Lustre layout migration for large file
trees. Input is a NUL-terminated byte stream of paths, exactly what
`find -print0` produces, so the set of files to process is decided outside the
tool and can be produced, stored, split and replayed.
Every record must end in NUL, including the last one. The default limit for
one record is 8 MiB (`max_input_record_bytes`); a longer record or a missing
final NUL makes the run fail with `input_complete=false`. Newline and BEL are
legal filename bytes and are never treated as record separators.

It is built for the case where a run takes hours, is started as root, and has
to survive being stopped and resumed.

```bash
# copy a tree with 8 workers, detached, logging to disk
parallel_tools.py -p copy myjob /src/tree /dst/tree -j 8

# what it created
myjob.cfg       the configuration
myjob.jsonlog   one JSON record per batch, plus the final statistics
myjob.log       the operational log, every line timestamped

# stop it gracefully, naming the log it writes
parallel_tools.py stop myjob.log
```

## Two programs

| | |
|---|---|
| `parallel_tools.py` | The scheduler. Reads paths, distributes them over worker threads, keeps the statistics, handles the control signals. One file, standard library only. |
| `lustre-migrate-file/` | A small C helper that changes the layout of **one** Lustre file. Only needed for `method=migrate`. It is the sole authority on Lustre state: the scheduler never parses `getstripe` output and never decides when a mirror may be deleted. See its own [README](lustre-migrate-file/README.md). |

## Requirements

### Python

| | |
|---|---|
| Minimum | **3.9** |
| In production | 3.11 |
| Packages | none â€” standard library only |

The syntax itself parses back to 3.7, and the only version-dependent calls are
`os.pidfd_open` and `signal.pidfd_send_signal`, both guarded by `hasattr`. So
it *runs* on 3.7 and 3.8 â€” but then a control signal cannot pin its target
process, and a PID reused between check and signal would be signalled instead.
3.9 is the lowest version where that protection exists, which is why it is the
stated minimum.

Note that the shebang line says `#!/usr/bin/env python3.11`. On a host without
a binary of exactly that name, call the interpreter explicitly
(`python3 parallel_tools.py â€¦`) or edit the line.

Two optional modules. Each is imported in a `try`, is absent without
complaint, and is only needed when the configuration asks for it:

| module | needed for | if missing |
|---|---|---|
| `blake3` | `verify="blake3"` and its threaded variants | `verify="sha256"` and the other `hashlib` hashes still work |
| `zstandard` | `spill_compression="zstd"` | zlib is used; zstd is roughly seven times faster at the same ratio, and only enters the standard library in 3.14 |

### Operating system

Linux. `/proc` carries three things this program depends on:

- running helper programs through `/proc/self/fd/N`, so that what was
  inspected is what runs;
- checking, before a signal goes out, that the target really is the run the
    operator meant â€” `/proc/PID/fd/2` against the log, and the script
    descriptor against this file;
- `/proc/self/mountinfo`, which is how a `hard_link` mode is checked against
  the filesystems that are actually mounted.

Without `/proc` the tool still runs, and says once per run which of these it
had to drop. It is not a supported configuration â€” it is what happens on a
developer's macOS machine.

### External programs

| program | needed for | from |
|---|---|---|
| `rsync` | `method=rsync` | any distribution; `--fsync` is used when the installed rsync is 3.2.4 or newer, and skipped otherwise |
| `lfs` | the Lustre `hard_link` modes | `lustre-client` |
| `find`, `tee`, `sh` | the `-p` shortcut's input pipeline | coreutils, any shell |
| `lustre-migrate-file` | `method=migrate` | built from this repository |

All of them are opened and checked at startup, never per batch, so a missing
or unsafe one is reported once before any work begins.

### Without a Lustre client

`copy`, `rsync` and `diff` need no Lustre at all â€” they are ordinary POSIX
operations and work on any filesystem. What is unavailable:

| | |
|---|---|
| `method=migrate` | Not usable. It needs the C helper, which links against `liblustreapi`, and a mounted Lustre filesystem to act on. |
| `hard_link` = `lustre2posix`, `posix2lustre`, `lustre2lustre` | Not usable. They need `lfs` to resolve a file's names through its FID, and the mode is **refused at startup** when the mounted filesystem is not Lustre â€” rather than discovered file by file. |
| `hard_link=posix2posix` | Works. Names are found by scanning the source subtree, which is slower but needs nothing from Lustre. |

### Building the C helper

Needs a C compiler, `make`, and the Lustre client development package for
`lustre/lustreapi.h` and `liblustreapi`:

```bash
# RHEL / Rocky / Alma
dnf install lustre-client-devel gcc make

cd lustre-migrate-file
make check-headers        # says whether the header is where the build expects
make                      # builds lustre-migrate-file
make test                 # builds and runs the offline tests
make install PREFIX=/usr/local
```

The package name is the one used on RHEL-family systems, where this is
deployed; on other distributions it may differ â€” `make check-headers` answers
the question directly either way.

`make test` needs the headers to compile, but **no Lustre filesystem** to run:
the offline tests cover path resolution, OST parsing and base64, the parts
that do not talk to Lustre. Everything that does has to be tested on a real
filesystem.

The scheduler finds the helper beside `parallel_tools.py`, then on `PATH`, or
wherever `migrate_helper` in the configuration points. It checks the version
before letting it touch a single file.

## Methods

| method | what it does |
|---|---|
| `copy` | Copies files itself, through descriptors, with an optional external copy engine. |
| `rsync` | Batches paths and hands them to `rsync --files-from`. |
| `diff` | Compares source and destination without writing anything. |
| `migrate` | Moves Lustre files off a set of banned OSTs and leaves them with the requested number of mirrors, one helper invocation per file. |

## Two ways to start

**Project shortcut** â€” creates a configuration and starts detached:

```bash
parallel_tools.py -p METHOD NAME SRC DST [INPUT] [-j N]
```

It generates the file list with `find -print0`, saves it with `tee` so the
same list can be replayed later, and writes `NAME.cfg`, `NAME.jsonlog` and
`NAME.log`. Note that it **overwrites `NAME.cfg`** each time â€” to repeat a run
with an edited configuration, use `start` instead.

**Explicit configuration** â€” full control, reads paths from stdin:

```bash
parallel_tools.py create copy copy.cfg     # write a default configuration
$EDITOR copy.cfg
find /src -print0 | parallel_tools.py start copy.cfg
```

`parallel_tools.py start --help` is the complete configuration reference:
every key, its default, and why it is what it is.

## Controlling a running job

All four take either the **stderr log** the run is writing â€” the file its own
`2>` points at, or `NAME.log` under `-p` â€” or its PID:

```bash
parallel_tools.py stop   /srv/jobs/myjob.log    # SIGTERM
parallel_tools.py pause  /srv/jobs/myjob.log    # SIGUSR1
parallel_tools.py resume /srv/jobs/myjob.log    # SIGUSR2
parallel_tools.py reload /srv/jobs/myjob.log    # SIGHUP
```

The log carries the PID, in the status line the scheduler writes whenever the
worker count, the active count or the queue changes, and at least every two
minutes:

```
2026-09-22 20:58:15.203098 Scheduler PID: 4096426 status: workers=8 active=8 ...
```

The last such line wins. What it says is a **suggestion, not an
authorisation** â€” a log is an ordinary file, and whoever can write it can put
a wrong number in it. Before anything is sent, that PID is checked against the
process carrying it: is its **descriptor 2 this very file**, compared by
inode, and is it this program. Both answers come out of `/proc/PID/fd`, which
reads the process's file table and not its memory â€” the distinction matters,
because the memory of a process stuck in uninterruptible I/O cannot be read at
all, and on a busy Lustre node that is not a rare state. A PID that fails
either check is not signalled, and the command says which check failed.

**Give one log file to one run.** Two runs appending to the same log make the
PID line ambiguous and the log itself unreadable, and gain nothing:

```bash
parallel_tools.py start a.cfg 2>a.log 1>a.jsonlog
parallel_tools.py start b.cfg 2>b.log 1>b.jsonlog
```

A PID is the other way in and is never second-guessed. But it is a statement
about the past: the log accumulates PIDs from earlier runs, and the number may
since have been reused.

- **stop** â€” workers finish the file they are on, a hard-link group is finished
  as a unit, a running migrate helper is allowed to complete. Paths that have
  not started are written to `remaining_tasks_file` so the run can be resumed.
  A second stop cuts a running helper short instead of waiting.
- **pause / resume** â€” the run stays alive with its queue and statistics. A
  paused run still reacts to stop immediately; it does not have to be resumed
  first.
- **reload** â€” re-reads the configuration. `method`, `hard_link`, `src_root`,
  `dst_root` and `relative_path` cannot change while a run is active;
  `max_workers` and most other values can.

## Output and exit codes

`stdout` carries JSON only â€” one record per batch and one final record with
the whole-run statistics. `stderr` carries the operational log, every line
timestamped. Keeping them apart is what makes `jq` usable on a live run.

Every accepted input record ends up in exactly one bucket:

```
total_input_record_count = total_object_count + total_failed_count
                                              + unstarted_path_count
```

| code | meaning |
|---|---|
| 0 | Input read completely, nothing failed, nothing left over. |
| 1 | Something failed: incomplete input, failed paths, unsaved work, directory timestamps not restored, a flush that did not succeed, or owner/group that could not be applied. |
| 2 | The run stopped early. Nothing failed, but work remains. |

For `migrate`, the log names every file whose layout was touched but not
finished, with a base64 copy of the path, so an input list for a repair run
can be rebuilt byte for byte â€” including names that are not valid UTF-8.

## Deferred work

Two things cannot be done while files are still being written, so they are
collected and applied at the end, in this order:

1. **Hard links.** A group of paths sharing an inode is reconstructed as a
   unit, after all ordinary files are done. Workers resolve the groups while
   normal batches are still running; only the reconstruction waits. On Lustre
   the names come from the file's FID via `lfs fid2path`, on POSIX from
   scanning the source subtree for the same `st_dev`/`st_ino`, which is
   considerably slower. The `hard_link` mode names both sides â€”
   `posix2lustre` is a POSIX source and a Lustre destination â€” and both halves
   are checked against the actual filesystem types before the run starts. If
   `st_nlink` says a group has more names than can be found below `src_root`,
   the whole group fails rather than being reconstructed incompletely.
2. **Directory timestamps.** Writing into a directory changes its mtime, so
   the recorded times are restored last. They are held in memory, compressed,
   and spilled to the working directory only when they exceed
   `directory_times_maxsize`.

Both are reported in the final statistics, and both count towards exit code 1
when they could not be completed.

## Security model

The tool is routinely started as root, and the design follows from that.

**Programs it executes** â€” `rsync`, `lfs`, an external copy engine, the migrate
helper, and its own script â€” are opened **once**, judged on the resulting
descriptor (regular file, executable, not group- or world-writable, owned by
root when running as root) including the whole directory chain, and then
executed through `/proc/self/fd/N`. What was inspected is what runs. Under
root a program that fails these checks is **refused**, not warned about.

**Files it creates itself** â€” temporary files, the directory-timestamp spill,
the remaining-task list, generated input lists â€” are created with
`O_CREAT|O_EXCL|O_NOFOLLOW` inside a directory whose chain was checked for
both mode and owner.

**Internal `copy` and `diff`** walk directories through descriptors, refusing
symlinks below the configured root. Before reading a regular file they open
its leaf with `O_NOFOLLOW|O_NONBLOCK` and compare the opened type and identity
with `lstat`. Extended attributes of regular files and directories are read
through those checked descriptors. The internal copy writes a temporary file
at mode `0600` and renames it through a held destination-parent descriptor
only after verification. With `xattr=true`, it removes stale `user.*` and
POSIX ACL attributes on destination directories, including existing ones.
Extra filesystem or security attributes make the copy fail instead of being
removed without review.

**`rsync` resolves source names again** after the scheduler has checked them.
If an untrusted user can replace entries in the source tree before `rsync`
opens them, the checked path may refer to a different object, potentially
outside `src_root`. Run this mode only with a source tree protected against
such changes; its path check is not a containment guarantee. A world-readable
destination directory remains a warning, not an access-control mechanism.

## Tests

```bash
# the C helper's offline tests (needs the Lustre headers, not a filesystem)
cd lustre-migrate-file && make test

# the Python tests
python3 test_project_mode.py        # the -p launcher actually starts its child
python3 test_signal_identity.py     # who stop/pause may and may not hit
python3 test_target_by_log.py       # finding a run through its stderr log
python3 test_passfds_invariant.py   # static: descriptor argv implies pass_fds
```

The Python tests that need `/proc` build a stand-in from directories and
symlinks, so they run on a developer machine too. That means they exercise the
logic, not the kernel interface â€” the real path has to be confirmed on a
Linux node.

## Status

In production use for Lustre OST evacuation. The parts that talk to Lustre â€”
layout, lease and mirror ioctls, hard-link group resolution through
`lfs fid2path` â€” are covered by source review and the helper's offline tests,
but cannot be exercised without a real filesystem, so they carry more risk than
the rest. `copy`, `rsync` and `diff` are exercised end to end.
