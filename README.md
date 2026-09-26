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
| Packages | none — standard library only |

The syntax itself parses back to 3.7, and the only version-dependent calls are
`os.pidfd_open` and `signal.pidfd_send_signal`, both guarded by `hasattr`. So
it *runs* on 3.7 and 3.8 — but then a control signal cannot pin its target
process, and a PID reused between check and signal would be signalled instead.
3.9 is the lowest version where that protection exists, which is why it is the
stated minimum.

Note that the shebang line says `#!/usr/bin/env python3.11`. On a host without
a binary of exactly that name, call the interpreter explicitly
(`python3 parallel_tools.py …`) or edit the line.

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
  operator meant — `/proc/PID/fd/2` against the log, and the script
  descriptor against this file;
- `/proc/self/mountinfo`, which is how a `hard_link` mode is checked against
  the filesystems that are actually mounted.

Without `/proc` the tool still runs, and says once per run which of these it
had to drop. It is not a supported configuration — it is what happens on a
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

`copy`, `rsync` and `diff` need no Lustre at all — they are ordinary POSIX
operations and work on any filesystem. What is unavailable:

| | |
|---|---|
| `method=migrate` | Not usable. It needs the C helper, which links against `liblustreapi`, and a mounted Lustre filesystem to act on. |
| `hard_link` = `lustre2posix`, `posix2lustre`, `lustre2lustre` | Not usable. They need `lfs` to resolve a file's names through its FID, and the mode is **refused at startup** when the mounted filesystem is not Lustre — rather than discovered file by file. |
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
deployed; on other distributions it may differ — `make check-headers` answers
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

**Project shortcut** — creates a configuration and starts detached:

```bash
parallel_tools.py -p METHOD NAME SRC DST [INPUT] [-j N]
```

It generates the file list with `find -print0`, saves it with `tee` so the
same list can be replayed later, and writes `NAME.cfg`, `NAME.jsonlog` and
`NAME.log`. Note that it **overwrites `NAME.cfg`** each time — to repeat a run
with an edited configuration, use `start` instead.

**Explicit configuration** — full control, reads paths from stdin:

```bash
parallel_tools.py create copy copy.cfg     # write a default configuration
$EDITOR copy.cfg
find /src -print0 | parallel_tools.py start copy.cfg
```

`parallel_tools.py start --help` is the complete configuration reference:
every key, its default, and why it is what it is.

## Controlling a running job

All four take either the **stderr log** the run is writing — the file its own
`2>` points at, or `NAME.log` under `-p` — or its PID:

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
authorisation** — a log is an ordinary file, and whoever can write it can put
a wrong number in it. Before anything is sent, that PID is checked against the
process carrying it: is its **descriptor 2 this very file**, compared by
inode, and is it this program. Both answers come out of `/proc/PID/fd`, which
reads the process's file table and not its memory — the distinction matters,
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

- **stop** — workers finish the file they are on, a hard-link group is finished
  as a unit, a running migrate helper is allowed to complete. Paths that have
  not started are written to `remaining_tasks_file` so the run can be resumed.
  A second stop cuts a running helper short instead of waiting.
- **pause / resume** — the run stays alive with its queue and statistics. A
  paused run still reacts to stop immediately; it does not have to be resumed
  first.
- **reload** — re-reads the configuration. `method`, `hard_link`, `src_root`,
  `dst_root` and `relative_path` cannot change while a run is active;
  `max_workers` and most other values can.

## Output and exit codes

`stdout` carries JSON only — one record per batch and one final record with
the whole-run statistics. `stderr` carries the operational log, every line
timestamped. Keeping them apart is what makes `jq` usable on a live run.

Every accepted input record ends up in exactly one bucket:

```
total_input_record_count = total_object_count + total_failed_count
                                              + unstarted_path_count
```

Repetitions count separately: a path listed twice is two records, and if it
fails it is two failures. A hard-link group carries every input record that
named it, so how the input happens to be split into batches does not change
the sum. `path_accounting_balanced` says whether it adds up, and a `false`
there ends the run with exit 1 — a record the run cannot place is a record
it may have lost.

| code | meaning |
|---|---|
| 0 | Input read completely, nothing failed, nothing left over. |
| 1 | Something failed: incomplete input, failed paths, unsaved work, destination metadata not restored, a flush that did not succeed, owner/group that could not be applied, or — with `require_destination_protection` — a destination that was not fully protected. |
| 2 | The run stopped early. Nothing failed, but work remains. |

For `migrate`, the log names every file whose layout was touched but not
finished, with a base64 copy of the path, so an input list for a repair run
can be rebuilt byte for byte — including names that are not valid UTF-8.

## Deferred work

Two things cannot be done while files are still being written, so they are
collected and applied at the end, in this order:

1. **Hard links.** A group of paths sharing an inode is reconstructed as a
   unit, after all ordinary files are done. Workers resolve the groups while
   normal batches are still running; only the reconstruction waits. On Lustre
   the names come from the file's FID via `lfs fid2path`, on POSIX from
   scanning the source subtree for the same `st_dev`/`st_ino`, which is
   considerably slower. The `hard_link` mode names both sides 
   `posix2lustre` is a POSIX source and a Lustre destination — and both halves
   are checked against the actual filesystem types before the run starts. If
   `st_nlink` says a group has more names than can be found below `src_root`,
   the whole group fails rather than being reconstructed incompletely.
2. **Destination metadata.** `copy` and `rsync` write into a locked tree (see
   below), so directories get their final owner, mode, ACL, xattrs and times
   only at the end — and for `rsync` so do files and symlinks. The values are
   recorded as `path → {fields}`, held in memory compressed, and spilled to
   the working directory above `metadata_maxsize`. The restore runs after the
   hard-link phase: files first, then directories bottom-up, `dst_root` last.
   It also runs after a stop; leaving the tree locked would be worse.

Both are reported in the final statistics, and both count towards exit code 1
when they could not be completed.

## Security model

The tool is routinely started as root, and the design follows from that.

**Programs it executes** — `rsync`, `lfs`, an external copy engine, the migrate
helper, and its own script — are opened **once**, judged on the resulting
descriptor (regular file, executable, not group- or world-writable, owned by
root when running as root) including the whole directory chain, and then
executed through `/proc/self/fd/N`. What was inspected is what runs. Under
root a program that fails these checks is **refused**, not warned about.

**Files it creates itself** — temporary files, the directory-timestamp spill,
the remaining-task list, generated input lists — are created with
`O_CREAT|O_EXCL|O_NOFOLLOW` inside a directory whose chain was checked for
both mode and owner.

**Internal `copy` and `diff`** walk directories through descriptors, refusing
symlinks below the configured root. Before reading a regular file they open
its leaf with `O_NOFOLLOW|O_NONBLOCK` and compare the opened type and identity
with `lstat`. Extended attributes of regular files and directories are read
through those checked descriptors. The internal copy writes a temporary file
at mode `0600` and renames it through a held destination-parent descriptor
only after verification. That descriptor pins an inode, not a path: it cannot
be given a different directory, but it follows the one it holds if somebody
renames it out of `dst_root`. **`copy` therefore assumes that no one else can
rename destination directories while a run is in progress**; see Known
limitations for what happens when that assumption does not hold.

With `xattr=true`, the copy removes stale `user.*` and POSIX ACL attributes on
destination directories, including existing ones. Extra filesystem or security
attributes make the restore of that directory fail instead of being removed
without review.

**The destination tree is locked while it is written** (`copy` and `rsync`).
`dst_root` is opened once and held; every walk below it starts from that
descriptor, and `rsync` gets it as `/proc/self/fd/N/` rather than as a name.
Every destination directory the run touches is owned by the running user and
has mode `0700` until the end — new ones from their creation, existing ones
from the moment the run first reaches them. Nobody else can reach anything
below a locked `dst_root` by path. The group is left alone: without group
permissions it grants nothing, and not changing it means one change less to
record.

**One run per destination.** At the start the run takes an exclusive
`flock()` on the held `dst_root` descriptor and keeps it until the restore at
the end is done. A second run on the same `dst_root` is refused with exit
status 1 — whatever its working directory — instead of undoing the first
run's lock or restore. If the lock cannot be taken at all, the run does not
start (see *Lustre and the run lock* below).

The lock is never given up while this program may still write. If a worker
does not finish within the shutdown wait, the restore is skipped and the tree
stays locked — and so does the run lock: it is released only when the process
has ended, which ends its worker threads, and when every child that could
still be writing has exited. `rsync` and an external copy engine are handed a
duplicate of the locked descriptor for exactly that reason. Other programs
writing into the same destination — a separate `rsync` or `rclone` job — are
not covered by this lock; keeping them apart is a matter of operations.

A start that fails — no lock, a refused rollback — removes again what it
created itself: `dst_root` and any missing parent directory, innermost first,
each only if it is still the directory it created and still empty, and only
while it holds the lock itself, so that a run that won the race for a new
`dst_root` never loses it to the one that lost. An existing `dst_root` is
never removed.

Before an existing directory is locked, its original owner, mode and ACL go
into a journal in the working directory and are flushed to disk first. A
directory this run changes is recorded once, at the change; later batches
find it locked and record nothing, so the temporary `0700` is never taken for
the original. The existing parent directories of a batch are locked
together, with one journal flush for all of them.

If the process dies, the next start with the same `dst_root` **and the same
`working_directory`** finds the journal — it is only ever looked at while the
run lock is held, so it cannot belong to a run that is still alive — and rolls
the originals back before it locks anything. Each journal records which
directories its values belong to (device and inode):

- If `dst_root` is not the directory the journal was written for — moved,
  replaced — nothing is rolled back, the journal is kept and the run does not
  start.
- A directory below it is rolled back only if it is still the journalled
  directory and still locked. One that was replaced or changed since is left
  alone and named in the log.
- The journal is read block by block into the same bounded, spillable store
  the run uses; `metadata_maxsize` limits the rollback as well.

`method=rsync` with `ignore_existing=true`: rsync leaves an existing
destination file completely alone, metadata included, and so does the
restore — nothing is recorded for it. Existing directories are brought to the
source values, as rsync itself does in that mode.

At the end every object gets its values in a fixed order — owner and group,
xattrs, ACLs, mode, a check of all of them, then the times. A configured
metadata class takes the source value; an unconfigured one gives a directory
back what it had before the lock.

How far this held is reported, separately from the copy result and from the
metadata result, as `destination_protection`:

| value | meaning |
|---|---|
| `enforced` | Everything the run touched was locked, `dst_root` included, and the directories above `dst_root` are trustworthy. |
| `partial` | Some directories could not be locked — typically an unprivileged run meeting a directory someone else owns — or were found released at the end. `dst_root` itself was locked, so none of them was reachable by path. |
| `not_enforced` | `dst_root` could not be locked, or a directory above it can be changed by somebody else, who could then move `dst_root` away and put another in its place. |

Anything but `enforced` is a warning; `require_destination_protection: true`
makes it exit status 1. A directory whose restore fails can stay at `0700`; it
is named in the log, with its path in base64, and counted in
`directories_left_locked`.

### Lustre and the run lock

The run lock is a `flock()` on `dst_root`. On Lustre it depends on the client
mount options:

| mount option | effect |
|---|---|
| `-o flock` | Coherent across all clients. Two runs on the same destination exclude each other wherever they run. |
| `-o localflock` | Only on this node. Two runs on the **same** node exclude each other; runs on different nodes do not see each other's lock. Starting runs for one destination on one node only is then the operator's job. |
| neither (`noflock`, the default on many clients) | `flock()` fails and the run **refuses to start**, naming the mount options. A run that could not exclude a second one would let the two roll back each other's journal. |

The journal itself belongs on a local filesystem. Every existing destination
directory costs a durable journal write before it is locked — batched per
batch, but a durable write nonetheless — and an `fsync` on Lustre is a round
trip to the servers. Set `working_directory` to a local disk (it must not be
inside `src_root` or `dst_root` anyway); a crash is then recovered by
starting again on the same node with the same `working_directory`.
`bench_destination_journal.py` measures the cost on your filesystems.

## Known limitations

What this program guarantees is that **each operation it performs is performed
on the object it checked** — and that it says so truthfully. What it does not
guarantee is that the trees stay still around it. Every statement about a
filesystem is a statement about a moment that has passed; chasing that to zero
is an infinite regress, and it costs a syscall per file to get no closer to an
answer. The limits below follow from that line, and they are deliberate.

**A destination directory moved out of the tree during a run.** Parent
directories are opened once per batch and held as descriptors — that is what
closes the symlink races, because a descriptor cannot be given a different
object. But a descriptor pins an *inode*, not a *path*: if somebody renames
`dst_root/sub` out of `dst_root` while the batch is running, the files of that
batch are written into the same directory they were checked against, which now
hangs somewhere else. The run reports success for them.

Doing this requires write permission on `dst_root` — and anybody who has that
can already pre-create `dst_root/sub` with whatever permissions they like and
read everything copied into it, with no race at all. So this is not an
escalation and not a new exposure; it is an incomplete destination tree
reported as complete. A `diff` run afterwards finds it: the destination path is
gone, and every file of that subtree is reported as a failure. The
verification *inside* the copy run does not, because it checks the temporary
file through its descriptor before the rename.

**The lock protects the destination, not the source.** It does not change the
next point, and a process that already held a descriptor or its working
directory inside a destination directory before the run locked it keeps what
that descriptor allows.

**After a crash, directories the dead run created stay `0700`** until a run
over them restores them; only the originals of directories that already
existed are journalled and rolled back. With `permissions` lacking `p`, the
mode such a new directory should have had cannot be known afterwards. The
journal is found only by a start with the same `working_directory`; on a
local disk, that means the same node.

**Identity is device and inode.** A directory deleted after a crash and
replaced by a new one that happens to get the same inode number and is locked
(owned by the running user, mode `0700`) would be taken for the original. Both
conditions at once do not arise by accident.

**`rsync` resolves source names again** after the scheduler has checked them.
If an untrusted user can replace entries in the source tree before `rsync`
opens them, the checked path may refer to a different object, potentially
outside `src_root`. Run this mode only with a source tree protected against
such changes; its path check is not a containment guarantee. `method=copy` is
the mode that holds descriptors all the way through.

**Exit 0 means every operation succeeded, not that the destination is in a
particular state now.** Nothing could mean the latter: a third party can change
the destination the moment the run ends, and a verification pass carries the
same caveat about its own moment. Where that matters — before deleting a
source tree, for instance — the answer is a `diff` run close in time to the
decision, not a stronger promise from the copy.

**A world-readable destination directory** is a warning, not an
access-control mechanism. The program reports what it finds and writes the
data; refusing would destroy the copy to avoid a lesser risk.

## Tests

```bash
# the C helper's offline tests (needs the Lustre headers, not a filesystem)
cd lustre-migrate-file && make test

# the Python tests
python3 test_project_mode.py        # the -p launcher actually starts its child
python3 test_signal_identity.py     # who stop/pause may and may not hit
python3 test_target_by_log.py       # finding a run through its stderr log
python3 test_passfds_invariant.py   # static: descriptor argv implies pass_fds
python3 test_destination_lock.py    # locking, restore, crash rollback (root, rsync, setfacl)

# what locking existing directories costs on your filesystems
python3 bench_destination_journal.py --tree /lustre/x/bench --workdir /var/tmp/pt --count 2000
```

The Python tests that need `/proc` build a stand-in from directories and
symlinks, so they run on a developer machine too. That means they exercise the
logic, not the kernel interface the real path has to be confirmed on a
Linux node.

## Status

In production use for Lustre OST evacuation. The parts that talk to Lustre 
layout, lease and mirror ioctls, hard-link group resolution through
`lfs fid2path` — are covered by source review and the helper's offline tests,
but cannot be exercised without a real filesystem, so they carry more risk than
the rest. `copy`, `rsync` and `diff` are exercised end to end.