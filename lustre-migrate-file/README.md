# lustre-migrate-file

Moves **one** Lustre file off a set of banned OSTs and leaves it with the
requested number of mirrors. One file per invocation, usable straight from a
shell and as the backend of a scheduler that runs many of them in parallel.

```
lustre-migrate-file \
    --root /l1fs \
    --stripe-count 4 \
    --pool-name migration_pool \
    --banned-ost l1fs-OST0014_UUID \
    --banned-ost l1fs-OST0018_UUID \
    --keep-mirroring \
    --json \
    /l1fs/project/file
```

The program is the sole authority on Lustre state. A caller neither parses
`getstripe` output nor decides when a mirror may be deleted, and every
parameter is validated here even when the caller already checked it: a helper
that is only safe because its caller was careful is not safe.

## Build

Needs `lustre-client-devel` (for `lustre/lustreapi.h` and `liblustreapi`).

```
make
make install PREFIX=/usr/local
```

`make check-headers` says whether the headers are where the build expects.

## What it does

The sequences are the ones `lfs(1)` itself uses in Lustre 2.15
(`lustre/utils/lfs.c`), not an independent interpretation of the manual:

| Step | Follows |
|---|---|
| add a mirror | `mirror_extend_layout()`: volatile victim file with the target layout, data copy under `LL_LEASE_RDLCK`, then `llapi_lease_set(LL_LEASE_LAYOUT_MERGE)` |
| delete a mirror | `mirror_split()` with `lil_ids[0] == fd`, the purge form: the mirror is discarded rather than given a name |
| resync | `lfs_mirror_resync_file()`: `llapi_mirror_find_stale()`, `LL_LEASE_RESYNC`, `llapi_mirror_resync_many()`, `LL_LEASE_RESYNC_DONE` |
| copy guard | `migrate_nonblock()`: data version before and after, refuse if it moved |

State machine, restarted from the real layout on every invocation:

```
resolve the path safely
-> check the filesystem, read the FID
-> analyse the layout
-> catch up whatever is behind, then ensure one safe, complete mirror
-> delete banned mirrors, one at a time, re-reading the layout between each
-> restore the requested mirror count
-> verify the end state
```

Nothing is remembered across runs. After a crash the next invocation looks at
the layout that actually exists, which is the only state that cannot be stale.

## Path handling

`openat2()` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS` would say this in one
call, but it needs kernel 5.6 and AlmaLinux 8 runs 4.18. The componentwise
`openat()` walk is therefore not a fallback here, it is the implementation:

* no cumulative `PATH_MAX` - only `NAME_MAX` per component, a real filesystem
  limit
* no symlink is traversed below `--root`, so the result cannot leave the tree
* `""`, `"."` and `".."` are rejected outright
* a different `st_dev` below root is a mount point and is refused unless
  `--allow-other-devices` says otherwise
* the file descriptor keeps pointing at the same inode even if the name is
  renamed underneath

After resolution the program works on descriptors. Two llapi calls take a
directory or file *name* rather than a descriptor
(`llapi_layout_file_open()` for the volatile victim); for those the program
`fchdir()`s into the already-open parent, so the name shrinks to one component
and no second path resolution happens. That is why the program is
deliberately single-threaded: the working directory is process-wide.

## Output

With `--json`, one object on stdout. Pathnames are **base64**: a pathname is a
byte string, is not necessarily valid UTF-8, and cannot be put into JSON as
text without corrupting some names.

```json
{"status":"ok","program":"lustre-migrate-file","program_version":"1.1",
 "path_b64":"L2wxZnMvcHJvamVrdC9kYXRlaQ==","root_b64":"L2wxZnM=",
 "fid":"[0x200000401:0x123:0x0]","changed":true,
 "mirror_count_before":1,"mirror_count_after":1,
 "banned_mirrors_before":1,"banned_mirrors_after":0,
 "target_mirror_count":1,"mirrors_added":1,"mirrors_deleted":1,
 "resyncs":0,"allocation_retries":0,
 "message":"already in the requested state"}
```

`message` is always present, including on success and for `--dry-run`, where
it carries the whole verdict. A count that was never determined is `null`, not
the `-1` the program holds internally, so a caller summing these across a run
never adds up sentinels. On failure the record additionally carries `stage`,
`errno` and `transient`. Argument errors produce a record too, with
`"stage":"arguments"` - as long as `--json` appears before the offending
option, since the program cannot know the output format before it has parsed
it.

`path_b64` is the authoritative pathname and round-trips every byte. The same
path inside `message` is a human hint only: bytes above 0x7f are escaped as
`\u00XX` there, which is valid JSON but decodes to those code points rather
than the original bytes.

Exit status:

| | |
|---|---|
| 0 | success, or the file was already in the requested state |
| 1 | permanent failure |
| 2 | usage error |
| 3 | transient failure (`EAGAIN`, `EBUSY`, `ESTALE`, ...) - retry this file |

## The protocol

Readable output is the default, for a person at a terminal. `--json` asks for
one JSON object on stdout: exactly one line, ASCII only, so a caller can
require one JSON value and treat anything else as a damaged protocol. The flag
is recognised before the options are parsed, so an argument error further along
the command line still answers in the form the caller can read.

| field | meaning |
|---|---|
| `schema` | protocol version, currently `1`; read this before anything else |
| `status` | `"ok"` or `"error"` - there is no third value |
| `rc` | the process exit status, repeated inside the record |
| `retryable` | `true` for `EAGAIN`, `EBUSY`, `ESTALE`, ... - requeue those and only those |
| `path_b64` | the pathname, base64, byte-exact |
| `initial_mirror_count` | mirrors found before anything was changed |
| `target_mirror_count` | what the run aimed at |
| `message` | always present, including on success and for `--inspect-only` |
| `stage`, `errno` | only on failure |

`--inspect-only` changes nothing and reports the layout. It exists so a caller
can learn `initial_mirror_count` once and pass it back as
`--target-mirror-count` on every attempt: deriving the count again on a retry
that resumed after mirrors were already deleted would silently lower the
redundancy. `--keep-mirroring` together with `--target-mirror-count` is
therefore not a contradiction but the crash-safe way to use it, and the
explicit count wins.

`--collapse-to-one` states the default (end with one mirror) explicitly, so a
caller never has to rely on the absence of an option.

`--expected-dev` and `--expected-ino` pin the file: if the name now leads to a
different inode, the run is refused with `ESTALE` instead of migrating
something the caller never inspected.

## From Python

```python
proc = subprocess.run(
    [b"lustre-migrate-file",
     b"--root", src_root_b,
     b"--stripe-count", str(cfg["stripcount"]).encode("ascii"),
     b"--pool-name", os.fsencode(cfg["poolname"]),
     b"--banned-ost", b"l1fs-OST0014_UUID",
     b"--json",
     src_b],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
```

`--banned-ost` accepts every spelling an operator actually has in front of
them:

| written as | read as | where it comes from |
|---|---|---|
| `14` | 14 | the first column of `lfs osts` |
| `0x0014`, `0X14` | 20 | explicit hexadecimal |
| `OST0014` | 20 | a UUID with the filesystem name trimmed off |
| `l1fs-OST0014_UUID` | 20 | what `lfs df` prints; `_UUID` is optional |

A bare number with a **leading zero is refused**. `strtol()` with base 0 reads
`0014` as octal 12, while somebody who wrote it has copied the four hex digits
out of a UUID and means 20. Both readings are plausible, the wrong one is
silent, and the consequence is evacuating a different OST than intended - so
the program says so instead of guessing:

```
ambiguous OST 0014: a leading zero could mean decimal, octal or the hex
digits of a UUID. Write the decimal index, or 0x014, or OST0014
```

When a UUID names a different filesystem than `--root` is on, the run is
refused rather than normalised: an index from another filesystem denotes
entirely different devices. Each banned index is also checked for existence,
because a typo would otherwise ban nothing and let the run report success.

## The rule for judging a mirror

`LCME_FL_INIT` means "instantiated" and nothing else; `LCME_FL_EXTENSION`
means "never instantiated" by definition. Neither may be read as a statement
about the data, which is why the check is per component and relative to the
file size:

| property | rule |
|---|---|
| `stale`, `nosync`, `offline` | mirror is rejected, always |
| `extension` component | no `init` required, and not part of the coverage |
| zero-length component (`end == start`) | covers no byte: no `init` required, no coverage, no gap. An extension component must follow it |
| component with `start < st_size` | `init` required |
| component with `start >= st_size` | missing `init` is fine |
| unknown flag, or `end < start` | abort; the layout is not understood |
| uninstantiated component | its pool is checked against the banned OSTs, because the OST is not chosen yet |
| coverage of `0..st_size` | must be continuous; a hole disqualifies the mirror |
| FLR state `wp` / `sp` | resync first, then re-read the layout |
| FLR state `ro` | settled; carry on |

`start < st_size` is deliberately conservative. For a fully sparse region it is
occasionally too strict, but it never calls a component that holds data safe.

Two counts follow from this, and they are not the same question. A mirror is
*safe* when it holds no object on a banned OST, and *usable* when it is
additionally a complete, in-sync copy. `safe` decides which mirrors may be
deleted; `usable` decides whether a deletion is allowed at all and which
mirror is kept when there is a surplus.

## The deletion is decided under the lease

Everything analysed before the deletion was analysed without a lease, so it
could be out of date by the time the split runs. `delete_one_mirror()`
therefore does its own check inside the lease:

```
llapi_lease_acquire(fd, LL_LEASE_WRLCK)
  re-read the layout by fd and classify it again
  the target mirror still exists and is still banned?
  at least one OTHER mirror is still usable?
  llapi_lease_check(fd) == LL_LEASE_WRLCK?
llapi_lease_set(fd, UNLCK | LAYOUT_SPLIT)      <- same lease, atomically
```

On any deviation the lease is released without deleting anything. The server
refuses a split that would leave only stale mirrors (`lod_lov.c`), which is a
good last line, but it knows nothing about banned OSTs, `nosync`, `offline` or
pools - those criteria only exist here.

## Choosing where a new mirror goes

Lustre has no notion of "allocate freely among these OSTs" - only an explicit
index per stripe, or a pool. `--allowed-ost` uses the first:
`llapi_layout_ost_index_set()` per stripe, which is what `lfs setstripe -o`
does. The number of indexes has to equal the stripe count.

* Naming exactly `--stripe-count` OSTs **pins** the placement.
* Naming more lets the program pick that many. Which ones matters: taking the
  first entries every time would put every migrated file of a whole tree on
  the same two OSTs. The starting point is therefore derived from the source
  inode, so the choice spreads across files, and from the attempt number, so a
  retry after a rejected allocation asks for different targets. Same file and
  same attempt give the same answer, which keeps a resumed run predictable.
* Naming none leaves the choice to Lustre, which knows free space and load.

`--allowed-ost` governs **creation only**. Which mirrors must be evacuated is
`--banned-ost`, and the two lists may not overlap. A new mirror that ends up on
an OST outside the allowed list is rejected the same way one on a banned OST
is.

## Which mirror is the new one

Not "the id that was not there before". When a non-FLR file gains its first
mirror the server renumbers the existing components:

```c
if (mirror_count == 1 && mirror_id_of(lcme->lcme_id) == 0) {
        /* Add mirror from a non-flr file, create new mirror ID. */
        id = pflr_id(1, i + 1);
        lcme->lcme_id = cpu_to_le32(id);
}
id = max(le32_to_cpu(lcme->lcme_id), id);
...
mirror_id = mirror_id_of(id) + 1;
```

Mirror 0 becomes mirror 1 and the merged one becomes 2, so by a
set-difference both look new and the file appears to have been changed by
somebody else. The rule above holds in both cases though: the merged mirror
always carries the **highest** mirror id. That, plus "exactly one mirror more
than before", names it without depending on how the others were numbered.

## The victim's layout must be composite

`lod_declare_layout_merge()` on the server begins with

```c
/* must be an existing layout from disk */
if (le32_to_cpu(merge_lcm->lcm_magic) != LOV_MAGIC_COMP_V1)
        RETURN(-EINVAL);
```

A layout from `llapi_layout_alloc()` is serialised as a plain
`LOV_MAGIC_V1`/`V3`, and neither `llapi_layout_stripe_count_set()` nor
`llapi_layout_pool_name_set()` changes that - only
`llapi_layout_comp_extent_set()`, `llapi_layout_comp_add()` and
`llapi_layout_mirror_count_set()` mark a layout composite. The program
therefore states the extent explicitly as `0..LUSTRE_EOF`, which is already
the default geometry: it changes nothing about the stripes, only how the
layout reaches the server.

Without it the whole file is copied and the merge is then refused with
`EINVAL`, which is what the error message says and not why.

## Finishing a migration later

A file that is written while it is being migrated makes the helper stop: the
data version moved, or the lease was broken, and neither is a reason to publish
a mirror that matches no version of the file. The failure is `retryable`, so
the scheduler tries again; after `migrate_allocation_attempts` it gives up on
that file.

The file is then not damaged - every step is verified before the next, and a
mirror is only ever deleted under a write lease with the layout read again -
but it may be half migrated. `changed` in the record says which: true means the
layout was already altered, false means the file is untouched.

The scheduler turns that into a `migrate_unfinished` list in its JSON log,
carrying `path_b64` so the pathname survives byte for byte, and
`layout_changed` so a later pass can select exactly the files that still need
finishing. No special option is needed for that pass: every invocation starts
from the layout that is actually there, so finishing a half-migrated file and
migrating an untouched one are the same operation.

## What is verified, and how

`make test` runs 27 offline checks that need no Lustre. They cover the
security-critical half:

* a symlink as the final component is refused
* a path *through* a symlinked directory is refused
* `..` is refused, including `../` reaching above root
* an absolute path outside root is refused, including the prefix trap
  (`/data-evil/x` against `--root /data`)
* a directory, and a trailing slash, are refused
* a 4231-byte relative path resolves, while `open()` on the same path fails
  with `ENAMETOOLONG`
* OST parsing in all three spellings, including the foreign-filesystem refusal
* base64 against RFC 4648 vectors and a non-UTF-8 name containing `0xff 0x00`

## What is NOT verified

Everything that talks to Lustre. This program has never been compiled against
the real headers and no `llapi_*` call in it has ever run. The `llapi`
signatures were checked mechanically against `lustreapi.h` from tag `v2_15_8`,
and the operation sequences were read out of `lfs.c` of the same tag, but
reading is not measuring.

Before it goes near production data:

1. `make && make check-headers` on the target host.
2. `--dry-run` over a representative sample; it changes nothing and prints the
   layout verdict.
3. A single file on scratch, with `lfs getstripe -y` before and after.
4. Content verification (checksum before and after) on a file large enough to
   have several stripes.
5. A file that is already mirrored, one that is not, and an **empty** one -
   an empty file has no instantiated component in any mirror, which the
   completeness check treats specially.

## Where this differs from the Python implementation

The Lustre call sequences come from `lfs.c`, not from `parallel_tools.py`. The
two readings of a layout were compared afterwards, and they are not identical:

* **Stale, nosync, offline.** Both refuse a stale mirror as the copy that
  justifies deleting another. Python additionally refuses `nosync` and
  `offline`, which is right - a nosync mirror is deliberately allowed to fall
  behind - and this program does the same.

  These are two separate questions and the program keeps two separate counts.
  A mirror is *safe* when it holds no object on a banned OST, and *usable*
  when it is additionally complete and in sync. `safe` decides which mirrors
  may be deleted; `usable` decides whether a deletion is allowed at all and
  which mirror is kept when there is a surplus. Collapsing the two would let a
  nosync mirror count as the protection that permits deleting a real copy.
* **Extension components.** Python requires *every* component of a mirror to
  carry `init`. A PFL file has components beyond the current end of file that
  are legitimately not instantiated, and an extension component never is, so
  that rule makes such files permanently unmigratable. This program instead
  requires at least one instantiated component and no stale/nosync/offline
  one, and treats an empty file as complete. That is a deliberate difference,
  not an oversight.

  A self-extending layout also contains *zero-length* components, and those
  are not an error either. `llapi_layout_sanity()` requires an extendable
  component to be zero-length before it is instantiated
  (`LSE_NOT_ZERO_LENGTH_EXTENDABLE`) and permits a zero-length component
  exactly when an extension component follows it (`LSE_ZERO_LENGTH_NORMAL`).
  Both halves of that rule are enforced here, so the program never rejects a
  layout that Lustre's own check - run a few lines earlier - has accepted.
* **Mirror count cross-check.** Python compares `lcm_mirror_count` against the
  mirror ids found in the components and refuses a disagreement. This program
  does the same through `llapi_layout_mirror_count_get()`.
* **YAML validation.** Python parses `lfs getstripe -y` output and therefore
  needs a large apparatus to reject malformed or unfamiliar documents. Through
  llapi there is no document to misparse. What carries over is the principle:
  a read that fails is an error, never "no banned OST found".
* **Deleting.** Python passes a set of mirror ids to one `lfs mirror delete`
  call. This program deletes one mirror per lease and re-reads the layout in
  between.

## Version 1.1: what changed and why

The most consequential of these was not a subtlety.

**Plain files were never checked at all.** `LCME_FL_INIT` cannot be used to
decide whether objects exist: liblustreapi sets the component flags to `0` for
a NON-composite layout (`liblustreapi_layout.c`, `llc_flags = 0` when there is
no `lcme` entry). An ordinary striped file - the most common thing there is -
therefore carries no `INIT` flag while its objects are perfectly real, the OST
scan was skipped, and the run reported "already in the requested state". The
question is now asked of the layout directly: can it name an object? That has
the same answer in both worlds, and it also fixes Data-on-MDT, where a
component holds data but owns no OST object.

**The new mirror was not flushed before the old one was deleted.** Without
`fsync(fdv)` the merge can publish a mirror whose blocks are still only in
this client's cache; deleting the old mirror right afterwards leaves a crash
or a delayed writeback error destroying the only complete copy. `lfs` calls
fsync there for exactly this reason.

**Resync could not have worked.** `lfs` opens the file `O_DIRECT | O_RDWR` for
mirror resync and the client refuses the mirror-select ioctl without it. The
resync now runs on a second descriptor opened through the already-verified
parent, with `st_dev`/`st_ino` compared against the file this program is
working on.

**`comp_array` was uninitialised.** `llapi_mirror_resync_many()` marks synced
components by setting `lrc_synced`, and the lease release only lists those.
Stack garbage would announce components as synced that never were. `lfs`
initialises the same array with `{ { 0 } }`.

Further:

* `llapi_get_data_version()` takes `__u64 *`; `uint64_t *` is a different type
  on LP64 and did not compile cleanly.
* `MAX_MIRRORS` now follows `LUSTRE_MIRROR_COUNT_MAX` (16). A target of 17
  used to create fifteen mirrors before failing on the last extend.
* `--root /` rejected every absolute path: the prefix check demanded another
  `/` behind the root.
* `--allow-other-devices` silently broke the filesystem binding, because the
  filesystem name comes from `--root` while the file could be on another
  mount. It is now refused together with `--banned-ost`.
* `llapi_layout_sanity()` and `LLAPI_LAYOUT_GET_CHECK` are used, so a layout
  that fails Lustre's own consistency rules is not one that deletion decisions
  are drawn from.
* Sparse files are copied sparsely (`llapi_file_is_sparse`, `llapi_data_seek`),
  as `lfs migrate_copy_data()` does. Writing holes out as zeros turned a
  migration that fits into one that ends in ENOSPC.
* The volatile file gets the missing `lfs` steps: `EEXIST` retry with a fresh
  tag, `unlink()` fallback for MDTs without volatile files, `fchown()` to the
  source owner for the layout swap and quota, `O_FILE_ENC`, and an `fstat()`
  taken AFTER the lease rather than from the initial path resolution.
* `--expect-dev` / `--expect-ino` let a caller pin the file it inspected. A
  name pointed at a different inode in between is refused with `ESTALE`.
* `--allocation-attempts N` removes a new mirror that landed on a banned OST
  and allocates again, instead of leaving it as one more banned mirror.
* Banned OST indexes are checked for existence; a typo used to ban nothing and
  report success. A pool written as `fsname.pool` is validated instead of
  being silently reduced to `pool`.
* `pwrite()` returning 0 no longer loops forever.
* Exit status 3 now means transient (`EAGAIN`, `EBUSY`, `ESTALE`, ...), so a
  scheduler can requeue those and only those. `--json` reports `errno` and
  `transient`, and argument errors produce JSON too.

Still open, and deliberately so:

* **`--keep-mirroring` records its goal on the file.** The target used to be
  derived from the state found at startup, which meant a run that died after
  deleting and before restoring made the reduced count the new target - the
  redundancy was gone and nothing said so. The number is now written before
  the first deletion and removed once the end state has been verified, so the
  next run finds the original count instead of counting what is left. A file
  still carrying the attribute is a file whose migration did not finish.

  The attribute is `trusted.lustre_migrate_file.mirror_goal` under root and
  `user.lustre_migrate_file.mirror_goal` otherwise, and the two are never
  mixed. `trusted.*` needs `CAP_SYS_ADMIN`, so the owner of a file cannot
  plant a goal there; a `user.*` value is writable by the owner, which is
  tolerable only because an unprivileged run can migrate nothing it could not
  already rewrite. `trusted.*` is also independent of the `user_xattr` mount
  option.

  `--inspect-only` reports the recorded goal as `initial_mirror_count`, not
  the count found today. It is what an operator wants to see before a run; the
  scheduler no longer needs it. It used to: `--keep-mirroring` derived its
  target from the state it found, which after a crash was the reduced count,
  so the caller had to read the number first and pin it with
  `--target-mirror-count`. With the goal on the file that crutch is gone, and
  the three options that state the final mirror count - `--keep-mirroring`,
  `--target-mirror-count`, `--collapse-to-one` - are now mutually exclusive.
  "As many as it had" is a question this program answers by itself.

  A failure to write the record is refused, not warned about. It is the crash
  safety of `--keep-mirroring`; changing the layout without it would be the
  dangerous half of the work with the protection left out. For the same reason
  the record is written before ANY layout change, not only before a deletion:
  step 1 may add a usable copy first, and a run that dies between that and the
  return to the original count leaves the file with one mirror too many.

  The record is removed once the end state is verified, whatever set the
  target. Clearing it only under `--keep-mirroring` meant a file extended with
  an explicit `--target-mirror-count` kept an older, smaller goal, and the next
  `--keep-mirroring` read that number and deleted a mirror somebody had just
  asked for. A removal that fails is an error, not a note on stderr: the run
  would otherwise report success and leave that trap behind.

  `--target-mirror-count` remains the stronger form: it is a statement rather
  than an observation, and it needs nothing on the file.
* **Where Lustre allocates is Lustre's decision.** This program does not pick
  OSTs; it asks, and the allocator answers. What it can do cheaply is count
  the candidates first: how many OSTs exist outside the banned set, or how
  many members a pool has that are not banned. Fewer than the mirror needs
  stripes means the request cannot be satisfied, and that is refused
  immediately, before a byte is copied. With every OST banned the run ends in
  one message instead of `--allocation-attempts` full-file copies.

  A deletion states WHY it is happening, and the re-analysis under the write
  lease re-establishes that reason rather than one fixed condition. Requiring
  every deletion to name a banned mirror made `--collapse-to-one` and any
  reduction to `--target-mirror-count` impossible - a surplus mirror is by
  definition not banned, the banned ones having been removed first - and it
  also made a new mirror that merely violates `--allowed-ost` impossible to
  roll back, so the mirror nobody wanted stayed. What holds for every reason,
  and is checked under the lease every time, is that a deletion may never
  remove the last usable copy.

  **What this program cannot promise is the future.** A component that is not
  instantiated yet owns no object, so there is nothing to evacuate - but it
  will be allocated somewhere the first time it is written, and where that is
  is Lustre's decision, taken long after this program has exited. Without a
  pool the allocation target is unknown and is therefore not treated as a
  finding; with a pool, the pool is checked against the banned set. So for a
  file with uninstantiated components, "no objects on a banned OST" is a
  statement about now, not a guarantee for later. A lasting exclusion needs
  the banned OSTs taken out of new allocations server-side, or a pool that
  does not contain them - which is what IMPORTANT FOR MIGRATE asks for, and
  this is the case where it is not optional.

  The count is taken where a mirror is about to be allocated, not at startup.
  Asking earlier meant that a sweep with more banned OSTs than the filesystem
  can spare reported `ENOSPC` for every file - including the files that were
  already clean, and those that only needed a surplus banned mirror deleted
  while a good mirror was already there. A file that needs no new mirror is
  never refused for a shortage of OSTs it does not use. A `--dry-run` still
  names the shortage, but only for the files that would actually need one.

  The count is a necessary condition, not a sufficient one: an OST that exists
  and is in the pool can still be inactive, degraded or full. A mirror that
  lands on a banned OST anyway is identified by its mirror id, removed again,
  and reallocated up to `--allocation-attempts` times - it is never left
  behind. A pool that merely *contains* a banned OST is not refused, as long
  as enough other members remain.

* **Two schedulers can start on the same FID.** Lustre's leases serialise the
  destructive steps, but nothing stops the duplicated work.

## Version 2.1: the resync was tied to the wrong condition

The resync in step 1 was conditioned on there being no usable mirror at all:

```c
if (state.usable_mirror_count == 0 && state.needs_resync)
```

That matched step 1's own job - get one good copy before deleting anything -
but the final check asks for more than that. It refuses to finish while any
mirror is stale, and nothing between the two took that job on.

A file with the right mirror count, no banned mirror and one stale mirror
therefore fell through every step and failed the final check with `EAGAIN`,
on that run and on every retry, because each retry took the identical path.
The file was not damaged: the mirror that had been written held all the data.
It simply did not have the redundancy it claimed, and the program would never
fix that by itself - `lfs mirror resync` had to be run by hand.

That state is easy to reach: the helper adds a mirror, somebody writes the
file, the new mirror goes stale.

Whether such a file was repaired depended on something unrelated - whether it
also happened to carry a banned mirror, which is what got it into the delete
loop, where the resync is not conditioned at all. Measured offline with the
same stale mirror in both cases:

```
without a banned mirror:  stage=verify   a mirror is still stale after the run
with a banned mirror:     stage=resync   (the resync is attempted)
```

The condition is now just `needs_resync`, which is only true when the layout
reports `WRITE_PENDING` or `SYNC_PENDING` or a mirror is stale, so it does not
run for files with nothing to catch up. A writer that invalidates the result
again turns this into an ordinary retryable failure, which is what the retry
exists for.

`PROTOCOL_SCHEMA` is unchanged at 2, so this does not have to be deployed
together with a matching parallel_tools.py.

## Version 2.2: three findings around resync and pending layouts

**The resync ran before the deletions.** 2.1 caught up whatever was behind at
the start of the run, which also copied mirrors the delete phase was about to
remove. On a banned OST that is pure waste; on a failing one the copy can fail
and take down a migration that would have succeeded by simply deleting the
mirror. The resync in step 1 is conditioned on there being no usable mirror
again - that is step 1's own job - and a second resync runs after the
deletions and after the count is right, so only mirrors that will actually
survive are copied. The file that 2.1 was meant to fix - correct count, no
banned mirror, one stale mirror - still finishes, now without the collateral.

**A pending layout counted as finished.** `needs_resync` is true for
`WRITE_PENDING` and `SYNC_PENDING`, but the early success check, the final
check and the optional second verification all tested `any_stale` alone. A file
caught mid-write, before any component carries `STALE`, therefore passed as
"already in the requested state". All three now test `needs_resync` as well.

**A stale NOSYNC mirror could never satisfy the run.** `scan_component()`
raised `any_stale` for it, so the helper tried to resync - but
`llapi_mirror_find_stale()` skips `NOSYNC`, so nothing was copied and the final
check rejected the same mirror again, on every attempt. The comment claiming
`NOSYNC` was exempt did not match the code. A `NOSYNC` or `OFFLINE` component
no longer raises `any_stale`; being behind is what it was configured for. It
still counts as unusable, so it can never justify deleting another mirror.

`PROTOCOL_SCHEMA` is unchanged at 2. parallel_tools now checks `program` and
`program_version` in the answer, and refuses a helper older than 2.1: the
schema number alone identified neither the program nor its behaviour.

## Version 2.3: the NOSYNC deadlock 2.2 created, and OFFLINE

**A pending layout that no resync can clear no longer fails for ever.** 2.2
rejected `WRITE_PENDING` and `SYNC_PENDING` in the final check, which was right
for a file being written and wrong for everything else. A write marks NOSYNC
components STALE as well and puts the file into `WRITE_PENDING`;
`llapi_mirror_find_stale()` skips NOSYNC by design, so the resync finds nothing,
the state stays, and the check rejects it again - on every retry and on every
later run. A file with one usable mirror and one deliberately lagging NOSYNC
mirror could never finish.

The pending state alone is no longer an error. What still catches a real writer
is the resync itself: it takes a write lease, and a concurrent writer breaks
that lease, which is a retryable failure. A pending state nothing can clear is
now reported as `pending_layout` in the JSON and named in the message, instead
of failing the run.

**LCME_FL_OFFLINE is named explicitly.** Lustre 2.15.8 does not list it in
`LCME_KNOWN_FLAGS`, so a component carrying it was rejected as "unknown" before
the code that treats OFFLINE as unusable could run. Conservative - it prevented
an unsafe deletion - but not what the comment claimed. The flag is now known,
and the mirror is classified rather than refused.

## Known limitations

* A new mirror is created with a plain layout from `--stripe-count` and
  `--pool-name`. For a PFL file this does not reproduce the component
  structure of the existing mirror - the same thing `lfs mirror extend -N -c`
  does, but worth knowing before using it on a PFL tree.
* Migration is not one atomic transaction. Each step takes and releases its
  own lease; between two steps another writer can act. What the program
  guarantees is that it never decides on stale information: the layout is
  re-read after every step, and the file is addressed by descriptor
  throughout.
* Preventing *new* allocations on the banned OSTs is a server-side job. This
  program removes existing ones.
