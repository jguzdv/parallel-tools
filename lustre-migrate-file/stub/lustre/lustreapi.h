/* Stub of <lustre/lustreapi.h>, enough to COMPILE lustre-migrate-file.c
   without lustre-client-devel. It does not link and must never be used to
   claim the program works - only that it still builds. */
#ifndef LUSTREAPI_STUB_H
#define LUSTREAPI_STUB_H
#include <stdint.h>
#include <sys/types.h>
#include <stdbool.h>

typedef uint32_t __u32;
typedef uint64_t __u64;
typedef uint16_t __u16;
typedef int32_t  __s32;

#define LOV_MAXPOOLNAME 15
#define LOV_MAGIC_V1 0x0BD10BD0
#define LOV_MAGIC_COMP_V1 0x0BD60BD0
#define LUSTRE_EOF 0xffffffffffffffffULL
#define LUSTRE_MIRROR_COUNT_MAX 16
#define LUSTRE_VOLATILE_HDR ".^L^S^T^R^:"

#define LCME_FL_INIT       0x00000010
#define LCME_FL_STALE      0x00000020
#define LCME_FL_OFFLINE    0x00000040
#define LCME_FL_NOSYNC     0x00000080
#define LCME_FL_EXTENSION  0x00000100
#define LCME_KNOWN_FLAGS   (LCME_FL_INIT | LCME_FL_STALE | LCME_FL_OFFLINE | \
                            LCME_FL_NOSYNC | LCME_FL_EXTENSION)

#define LCM_FL_NONE           0
#define LCM_FL_RDONLY         1
#define LCM_FL_WRITE_PENDING  2
#define LCM_FL_SYNC_PENDING   3
#define LCM_FL_FLR_MASK       0x3

#define LL_LEASE_RDLCK        0x1
#define LL_LEASE_WRLCK        0x2
#define LL_LEASE_UNLCK        0x4
#define LL_LEASE_RESYNC       0x10
#define LL_LEASE_RESYNC_DONE  0x20
#define LL_LEASE_LAYOUT_MERGE 0x40
#define LL_LEASE_LAYOUT_SPLIT 0x80
#define LL_DV_RD_FLUSH        0x1

#ifndef O_NOATIME
#define O_NOATIME 01000000
#endif
#ifndef O_FILE_ENC
#define O_FILE_ENC 0
#endif

#define LLAPI_LAYOUT_ITER_CONT 0
#define LLAPI_LAYOUT_ITER_STOP 1
#define LLAPI_LAYOUT_DEFAULT   ((uint64_t)-1)
#define LLAPI_LAYOUT_WIDE      ((uint64_t)-2)
#define LLAPI_LAYOUT_RAID0     0
#define LLAPI_LAYOUT_MDT       2
#define LLAPI_LAYOUT_COMP_USE_FIRST 1
#define LLAPI_LAYOUT_COMP_USE_LAST  2
#define LLAPI_LAYOUT_COMP_USE_NEXT  3
#define LLAPI_LAYOUT_COMP_USE_PREV  4
enum llapi_layout_get_flags {
        LLAPI_LAYOUT_GET_EXPECTED = 0x1,
        LLAPI_LAYOUT_GET_CHECK    = 0x2,
        LLAPI_LAYOUT_GET_COPY     = 0x4,
        LLAPI_LAYOUT_GET_STRICT   = 0x8,
};

#define DFID "[0x%llx:0x%x:0x%x]"
#define PFID(fid) (unsigned long long)(fid)->f_seq, (fid)->f_oid, (fid)->f_ver

#ifndef O_DIRECT
#define O_DIRECT 040000
#endif

enum ll_lease_mode {
        LL_LEASE_STUB_RDLCK = LL_LEASE_RDLCK,
        LL_LEASE_STUB_WRLCK = LL_LEASE_WRLCK,
        LL_LEASE_STUB_UNLCK = LL_LEASE_UNLCK,
};

struct lu_fid { __u64 f_seq; __u32 f_oid; __u32 f_ver; };

struct ll_ioc_lease {
        __u32 lil_mode;
        __u32 lil_flags;
        __u32 lil_count;
        __u32 lil_ids[0];
};

struct llapi_layout;
struct llapi_resync_comp;

struct llapi_layout *llapi_layout_alloc(void);
struct llapi_layout *llapi_layout_get_by_fd(int fd, enum llapi_layout_get_flags flags);
void llapi_layout_free(struct llapi_layout *layout);
int llapi_layout_comp_extent_get(const struct llapi_layout *l, uint64_t *s, uint64_t *e);
int llapi_layout_comp_extent_set(struct llapi_layout *l, uint64_t s, uint64_t e);
int llapi_layout_comp_flags_get(const struct llapi_layout *l, uint32_t *flags);
int llapi_layout_comp_use(struct llapi_layout *l, uint32_t pos);
int llapi_layout_flags_get(struct llapi_layout *l, uint32_t *flags);
int llapi_layout_get_flags(const struct llapi_layout *l, uint32_t *flags);
int llapi_layout_mirror_count_get(struct llapi_layout *l, uint16_t *count);
int llapi_layout_mirror_id_get(const struct llapi_layout *l, uint32_t *id);
int llapi_layout_ost_index_get(const struct llapi_layout *l, uint64_t idx, uint64_t *ost);
int llapi_layout_ost_index_set(struct llapi_layout *l, int idx, uint64_t ost);
int llapi_layout_pattern_get(const struct llapi_layout *l, uint64_t *pattern);
int llapi_layout_pool_name_get(const struct llapi_layout *l, char *name, size_t n);
int llapi_layout_pool_name_set(struct llapi_layout *l, const char *name);
int llapi_layout_sanity(struct llapi_layout *l, _Bool incomplete, _Bool flr);
int llapi_layout_stripe_count_get(const struct llapi_layout *l, uint64_t *count);
int llapi_layout_stripe_count_set(struct llapi_layout *l, uint64_t count);
int llapi_layout_stripe_size_get(const struct llapi_layout *l, uint64_t *size);
int llapi_layout_file_open(const char *path, int open_flags, mode_t mode,
                           const struct llapi_layout *layout);

typedef int (*llapi_layout_iter_cb)(struct llapi_layout *layout, void *cbdata);
int llapi_layout_comp_iterate(struct llapi_layout *l, llapi_layout_iter_cb cb, void *data);

int llapi_search_fsname(const char *pathname, char *fsname);
int llapi_search_ost(const char *fsname, const char *poolname, const char *ostname);
int llapi_get_obd_count(char *mnt, int *count, int is_mdt);
int llapi_get_poolmembers(const char *poolname, char **members, int list_size,
                          char *buffer, int buffer_size);
int llapi_fd2fid(int fd, struct lu_fid *fid);
int llapi_file_fget_mdtidx(int fd, int *mdtidx);
bool llapi_file_is_sparse(int fd);
int llapi_get_data_version(int fd, __u64 *data_version, __u64 flags);
off_t llapi_data_seek(int src_fd, off_t offset, size_t *length);
int llapi_lease_acquire(int fd, enum ll_lease_mode mode);
int llapi_lease_check(int fd);
int llapi_lease_release(int fd);
int llapi_lease_set(int fd, const struct ll_ioc_lease *data);
int llapi_mirror_find_stale(struct llapi_layout *layout,
                            struct llapi_resync_comp *comp, size_t comp_size,
                            __u16 *mirror_ids, int ids_nr);
int llapi_mirror_resync_many(int fd, struct llapi_layout *layout,
                             struct llapi_resync_comp *comp_array,
                             int comp_size, uint64_t start, uint64_t end);

struct llapi_resync_comp {
        uint64_t lrc_start;
        uint64_t lrc_end;
        uint32_t lrc_mirror_id;
        uint32_t lrc_id;
        _Bool    lrc_synced;
};
#endif
