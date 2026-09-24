/* Treibt resolve_path(), parse_ost() und base64_encode() ohne Lustre. */
#define main lmf_main_unused
#include "lustre-migrate-file.c"
#undef main

#include <assert.h>

/* --- llapi-Stubs: werden von resolve_path() nicht benutzt ------------- */
struct llapi_layout *llapi_layout_alloc(void) { return NULL; }
void llapi_layout_free(struct llapi_layout *l) { (void)l; }
/* --- Ein Ersatz-Layout: eine Tabelle von Komponenten, die die Stubs
   unten beantworten. So laeuft analyse_layout()/scan_component() ohne
   Lustre gegen genau definierte Layouts. ------------------------------ */
struct fake_comp {
        uint32_t    mirror_id;
        uint32_t    flags;
        uint64_t    start, end;
        uint64_t    pattern;
        uint64_t    stripe_count;
        uint64_t    ost[4];
        int         objects;    /* 1 = ost_index_get liefert Objekte */
        const char *pool;
};
static struct fake_comp fake_comps[16];
static int  fake_comp_count;
static int  fake_cur;
static int  fake_layout_active;
static uint32_t fake_flr_flags;
static uint16_t fake_mirror_count;
static char fake_layout_marker;

static void fake_layout_reset(void)
{
        memset(fake_comps, 0, sizeof(fake_comps));
        fake_comp_count = 0;
        fake_cur = 0;
        fake_flr_flags = 0;
        fake_mirror_count = 0;
        fake_layout_active = 1;
}

static void fake_comp_add(uint32_t mirror_id, uint32_t flags, uint64_t start,
                          uint64_t end, int objects, uint64_t ost0)
{
        struct fake_comp *c = &fake_comps[fake_comp_count++];

        c->mirror_id = mirror_id;
        c->flags = flags;
        c->start = start;
        c->end = end;
        c->pattern = 0;
        c->stripe_count = 1;
        c->ost[0] = ost0;
        c->objects = objects;
        c->pool = "";
}

struct llapi_layout *llapi_layout_get_by_fd(int fd, enum llapi_layout_get_flags f)
{ (void)fd; (void)f;
  if (!fake_layout_active) { errno = ENOTSUP; return NULL; }
  return (struct llapi_layout *)&fake_layout_marker; }
int llapi_layout_flags_get(struct llapi_layout *l, uint32_t *f)
{ (void)l; if (!fake_layout_active) return -1; *f = fake_flr_flags; return 0; }
int llapi_layout_comp_iterate(struct llapi_layout *l, llapi_layout_iter_cb cb, void *d)
{
        if (!fake_layout_active)
                return -1;
        for (fake_cur = 0; fake_cur < fake_comp_count; fake_cur++)
                if (cb(l, d) == LLAPI_LAYOUT_ITER_STOP)
                        return LLAPI_LAYOUT_ITER_STOP;
        return LLAPI_LAYOUT_ITER_CONT;
}
int llapi_layout_comp_flags_get(const struct llapi_layout *l, uint32_t *f)
{ (void)l; if (!fake_layout_active) return -1; *f = fake_comps[fake_cur].flags; return 0; }
int llapi_layout_mirror_id_get(const struct llapi_layout *l, uint32_t *i)
{ (void)l; if (!fake_layout_active) return -1; *i = fake_comps[fake_cur].mirror_id; return 0; }
int llapi_layout_ost_index_get(const struct llapi_layout *l, uint64_t s, uint64_t *o)
{
        (void)l;
        if (!fake_layout_active) return -1;
        if (!fake_comps[fake_cur].objects || s >= fake_comps[fake_cur].stripe_count) {
                errno = EINVAL;
                return -1;
        }
        *o = fake_comps[fake_cur].ost[s];
        return 0;
}
int llapi_layout_stripe_count_get(const struct llapi_layout *l, uint64_t *c)
{ (void)l; if (!fake_layout_active) return -1; *c = fake_comps[fake_cur].stripe_count; return 0; }
int llapi_layout_stripe_count_set(struct llapi_layout *l, uint64_t c) { (void)l; (void)c; return -1; }
int llapi_layout_pool_name_set(struct llapi_layout *l, const char *p) { (void)l; (void)p; return -1; }
int llapi_layout_file_open(const char *p, int of, mode_t m, const struct llapi_layout *l)
{ (void)p; (void)of; (void)m; (void)l; errno = ENOTSUP; return -1; }
static int lease_ok;            /* 1 = Leases gelingen */
static int lease_set_ergebnis = -1;
int llapi_lease_acquire(int fd, enum ll_lease_mode m)
{ (void)fd; (void)m; return lease_ok ? 0 : -1; }
int llapi_lease_release(int fd) { (void)fd; return lease_ok ? 0 : -1; }
int llapi_lease_set(int fd, const struct ll_ioc_lease *d)
{
        (void)fd;
        if (!lease_ok)
                return -1;
        /* Ein Split entfernt den genannten Spiegel wirklich aus der Tabelle,
           sonst findet die Schleife in migrate_file() nie ein Ende und der
           Weg bis zur Endpruefung bleibt untestbar. */
        if (d && d->lil_flags == LL_LEASE_LAYOUT_SPLIT && d->lil_count >= 2) {
                uint32_t weg = d->lil_ids[1];
                int schreib = 0, i;

                for (i = 0; i < fake_comp_count; i++) {
                        if (fake_comps[i].mirror_id == weg)
                                continue;
                        fake_comps[schreib++] = fake_comps[i];
                }
                fake_comp_count = schreib;
                if (fake_mirror_count > 0)
                        fake_mirror_count--;
        }
        return lease_set_ergebnis;
}
int llapi_lease_check(int fd)
{ (void)fd; return lease_ok ? LL_LEASE_WRLCK : 0; }
static int find_stale_antwort = -1;   /* 0 = nichts zu tun, wie bei reinem NOSYNC */
int llapi_mirror_find_stale(struct llapi_layout *l, struct llapi_resync_comp *c, size_t n,
                            __u16 *ids, int nr)
{ (void)l; (void)c; (void)n; (void)ids; (void)nr; return find_stale_antwort; }
int llapi_mirror_resync_many(int fd, struct llapi_layout *l, struct llapi_resync_comp *c,
                             int n, uint64_t s, uint64_t e)
{ (void)fd; (void)l; (void)c; (void)n; (void)s; (void)e; return -1; }
int llapi_get_data_version(int fd, __u64 *dv, __u64 f) { (void)fd; (void)dv; (void)f; return -1; }
int llapi_file_fget_mdtidx(int fd, int *i) { (void)fd; (void)i; return -1; }
int llapi_fd2fid(int fd, struct lu_fid *f) { (void)fd; (void)f; return -1; }
int llapi_layout_mirror_count_get(struct llapi_layout *l, uint16_t *c)
{ (void)l; if (!fake_layout_active) return -1; *c = fake_mirror_count; return 0; }
int llapi_layout_ost_index_set(struct llapi_layout *l, int n, uint64_t o)
{ (void)l; (void)n; (void)o; return 0; }
int llapi_layout_comp_extent_set(struct llapi_layout *l, uint64_t s, uint64_t e)
{ (void)l; (void)s; (void)e; return 0; }
int llapi_layout_comp_extent_get(const struct llapi_layout *l, uint64_t *s, uint64_t *e)
{ (void)l; if (!fake_layout_active) return -1;
  *s = fake_comps[fake_cur].start; *e = fake_comps[fake_cur].end; return 0; }
int llapi_layout_pool_name_get(const struct llapi_layout *l, char *n, size_t z)
{ (void)l; if (!fake_layout_active) return -1;
  snprintf(n, z, "%s", fake_comps[fake_cur].pool ? fake_comps[fake_cur].pool : "");
  return 0; }
/* Steuerbar: 1 = dieser OST ist im Pool, 0 = nicht, -1 = Fehler. */
static int suche_ost_antwort = 0;
static char suche_ost_treffer[64] = "";
static int obd_count_antwort = -1;
static const char *pool_mitglieder[8];
static int pool_mitglieder_anzahl = -1;
int llapi_get_obd_count(char *mnt, int *count, int is_mdt)
{
        (void)mnt; (void)is_mdt;
        if (obd_count_antwort < 0)
                return -1;
        *count = obd_count_antwort;
        return 0;
}
int llapi_get_poolmembers(const char *poolname, char **members, int list_size,
                          char *buffer, int buffer_size)
{
        int i;

        (void)poolname; (void)buffer; (void)buffer_size;
        if (pool_mitglieder_anzahl < 0)
                return -1;
        for (i = 0; i < pool_mitglieder_anzahl && i < list_size; i++)
                members[i] = (char *)pool_mitglieder[i];
        return i;
}
int llapi_search_ost(const char *f, const char *p, const char *o)
{
        (void)f; (void)p;
        if (suche_ost_antwort < 0)
                return -1;
        if (suche_ost_antwort > 0 && !suche_ost_treffer[0])
                return 1;       /* jeder gesuchte OST existiert */
        if (suche_ost_treffer[0] && strcmp(o, suche_ost_treffer) == 0)
                return 1;
        return 0;
}
int llapi_layout_pattern_get(const struct llapi_layout *l, uint64_t *p)
{ (void)l; if (!fake_layout_active) return -1; *p = fake_comps[fake_cur].pattern; return 0; }
int llapi_layout_comp_use(struct llapi_layout *l, uint32_t p)
{ (void)l; (void)p; return -1; }
static int sanity_antwort;      /* >0 = Layout faellt durch Lustres Pruefung */
int llapi_layout_sanity(struct llapi_layout *l, bool i, bool f)
{ (void)l; (void)i; (void)f; return sanity_antwort; }
int llapi_layout_stripe_size_get(const struct llapi_layout *l, uint64_t *z)
{ (void)l; (void)z; return -1; }
bool llapi_file_is_sparse(int fd) { (void)fd; return false; }
off_t llapi_data_seek(int fd, off_t o, size_t *n) { (void)fd; (void)o; (void)n; return -1; }
static const char *fsname_antwort;
int llapi_search_fsname(const char *p, char *n)
{
        (void)p;
        if (!fsname_antwort)
                return -1;
        strcpy(n, fsname_antwort);
        return 0;
}

/* --------------------------------------------------------------------- */
static int failures;
static char base[4096];

static void check(const char *name, int ok, const char *detail)
{
        printf("%s  %-46s %s\n", ok ? "OK  " : "FAIL", name, detail ? detail : "");
        if (!ok)
                failures++;
}

static int try_resolve(const char *root, const char *path, char *msg, size_t msglen)
{
        struct options opt;
        struct resolved r = { .parent_fd = -1, .file_fd = -1 };
        int rc;

        memset(&opt, 0, sizeof(opt));
        opt.root = root;
        opt.path = path;
        memset(&res, 0, sizeof(res));
        res.exit_code = 0;

        rc = resolve_path(&opt, &r);
        if (msg)
                snprintf(msg, msglen, "%s", res.message);
        if (r.file_fd >= 0)
                close(r.file_fd);
        if (r.parent_fd >= 0)
                close(r.parent_fd);
        return rc;
}

static void write_file(const char *path, const char *content)
{
        FILE *f = fopen(path, "w");
        assert(f);
        fputs(content, f);
        fclose(f);
}

/* read_mirror_goal() meldet jetzt ueber res; vor jedem Aufruf zuruecksetzen,
   damit ein frueherer Fehler die naechste Antwort nicht blockiert. */
static int ziel_lesen(int fd, int *ziel)
{
        memset(&res, 0, sizeof(res));
        *ziel = 0;
        return read_mirror_goal(fd, ziel);
}

int main(void)
{
        char root[4096], p[8192], msg[1024];
        int i;

        snprintf(base, sizeof(base), "/tmp/lmf_test_%d", (int)getpid());
        snprintf(p, sizeof(p), "rm -rf %s && mkdir -p %s/root/a/b %s/aussen", base, base, base);
        assert(system(p) == 0);
        {
                /* On macOS /tmp is a symlink; the componentwise walk refuses
                   to traverse it, which is correct. Use the real path so the
                   tests exercise the program and not the host layout. */
                char resolved[4096];

                if (realpath(base, resolved))
                        snprintf(base, sizeof(base), "%s", resolved);
        }
        snprintf(root, sizeof(root), "%s/root", base);

        snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
        write_file(p, "inhalt");
        snprintf(p, sizeof(p), "%s/aussen/geheim", base);
        write_file(p, "GEHEIM");

        /* 1 normale Datei */
        snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
        check("normale Datei unterhalb root", try_resolve(root, p, msg, sizeof(msg)) == 0, msg);

        /* 2 relativ angegeben */
        check("relativ zu root angegeben",
              try_resolve(root, "a/b/datei", msg, sizeof(msg)) == 0, msg);

        /* 3 Symlink als letzte Komponente */
        snprintf(p, sizeof(p), "ln -s %s/aussen/geheim %s/root/a/link", base, base);
        assert(system(p) == 0);
        snprintf(p, sizeof(p), "%s/root/a/link", base);
        check("Symlink als Blatt wird abgelehnt",
              try_resolve(root, p, msg, sizeof(msg)) != 0, msg);

        /* 4 Pfad durch ein symlinkiertes Verzeichnis */
        snprintf(p, sizeof(p), "ln -s %s/aussen %s/root/a/dirlink", base, base);
        assert(system(p) == 0);
        snprintf(p, sizeof(p), "%s/root/a/dirlink/geheim", base);
        check("Weg durch Verzeichnis-Symlink abgelehnt",
              try_resolve(root, p, msg, sizeof(msg)) != 0, msg);

        /* 5 .. im Pfad */
        check("'..' im Pfad abgelehnt",
              try_resolve(root, "a/../a/b/datei", msg, sizeof(msg)) != 0, msg);
        check("'..' ueber root hinaus abgelehnt",
              try_resolve(root, "../aussen/geheim", msg, sizeof(msg)) != 0, msg);

        /* 6 absoluter Pfad ausserhalb root */
        snprintf(p, sizeof(p), "%s/aussen/geheim", base);
        check("absoluter Pfad ausserhalb root abgelehnt",
              try_resolve(root, p, msg, sizeof(msg)) != 0, msg);

        /* 7 Praefix-Falle: /...  /root-evil vs /root */
        snprintf(p, sizeof(p), "mkdir -p %s/root-evil && echo x > %s/root-evil/datei", base, base);
        assert(system(p) == 0);
        snprintf(p, sizeof(p), "%s/root-evil/datei", base);
        check("Praefix-Falle root-evil abgelehnt",
              try_resolve(root, p, msg, sizeof(msg)) != 0, msg);

        /* 8 Verzeichnis statt Datei */
        check("Verzeichnis als Ziel abgelehnt",
              try_resolve(root, "a/b", msg, sizeof(msg)) != 0, msg);

        /* 9 abschliessender Schraegstrich */
        check("abschliessender Schraegstrich abgelehnt",
              try_resolve(root, "a/b/datei/", msg, sizeof(msg)) != 0, msg);

        /* 10 root selbst */
        check("root selbst abgelehnt",
              try_resolve(root, root, msg, sizeof(msg)) != 0, msg);

        /* 11 sehr langer Pfad, weit ueber PATH_MAX, kurze Komponenten.
           Der Baum wird mit mkdirat() gebaut, weil ein "cd"-Aufruf in der
           Shell genau an der Grenze scheitert, die das Programm umgeht. */
        {
                char deep[16384];
                char component[64];
                size_t off = 0;
                int levels = 120;
                int dir_fd, next_fd, file_fd, rc;

                dir_fd = open(root, O_RDONLY | O_DIRECTORY);
                assert(dir_fd >= 0);
                for (i = 0; i < levels; i++) {
                        snprintf(component, sizeof(component),
                                 "d%02d_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", i);
                        if (mkdirat(dir_fd, component, 0755) < 0 && errno != EEXIST) {
                                fprintf(stderr, "mkdirat %s: %s\n", component,
                                        strerror(errno));
                                assert(0);
                        }
                        next_fd = openat(dir_fd, component, O_RDONLY | O_DIRECTORY);
                        assert(next_fd >= 0);
                        close(dir_fd);
                        dir_fd = next_fd;
                        off += snprintf(deep + off, sizeof(deep) - off, "%s/",
                                        component);
                }
                file_fd = openat(dir_fd, "tiefe_datei", O_WRONLY | O_CREAT | O_TRUNC, 0644);
                assert(file_fd >= 0);
                rc = (int)write(file_fd, "tief", 4);
                assert(rc == 4);
                close(file_fd);
                close(dir_fd);
                snprintf(deep + off, sizeof(deep) - off, "tiefe_datei");

                {
                        char detail[256];
                        int ok = try_resolve(root, deep, msg, sizeof(msg)) == 0;

                        snprintf(detail, sizeof(detail),
                                 "relativ %zu Bytes, PATH_MAX ist %d%s%s",
                                 strlen(deep), PATH_MAX,
                                 ok ? "" : " -- ", ok ? "" : msg);
                        check("Pfad weit ueber PATH_MAX aufloesbar", ok, detail);
                }

                /* Gegenprobe: open() auf denselben Pfad muss scheitern. */
                {
                        char absolute[20000];
                        int fd;

                        snprintf(absolute, sizeof(absolute), "%s/%s", root, deep);
                        fd = open(absolute, O_RDONLY);
                        check("open() auf denselben Pfad scheitert erwartungsgemaess",
                              fd < 0 && errno == ENAMETOOLONG, strerror(errno));
                        if (fd >= 0)
                                close(fd);
                }
        }

        /* 11b --root / mit absolutem Pfad */
        {
                char abs[8192];

                snprintf(abs, sizeof(abs), "%s/root/a/b/datei", base);
                check("--root / akzeptiert absolute Pfade",
                      try_resolve("/", abs, msg, sizeof(msg)) == 0, msg);
        }

        /* 11c Pool-Pruefung */
        {
                struct options o;
                bool enthaelt = true;

                memset(&o, 0, sizeof(o));
                o.banned[0] = 0; o.banned[1] = 20; o.banned_count = 2;

                suche_ost_antwort = 0; suche_ost_treffer[0] = '\0';
                check("ohne Pool ist die Platzierung unbekannt, nicht verboten",
                      pool_contains_banned(&o, "l1fs", NULL, &enthaelt) == 0 &&
                      enthaelt == false, NULL);

                enthaelt = true;
                check("leerer Poolname ebenso",
                      pool_contains_banned(&o, "l1fs", "", &enthaelt) == 0 &&
                      enthaelt == false, NULL);

                enthaelt = true;
                check("Pool ohne verbotenen OST ist in Ordnung",
                      pool_contains_banned(&o, "l1fs", "gut", &enthaelt) == 0 &&
                      enthaelt == false, NULL);

                snprintf(suche_ost_treffer, sizeof(suche_ost_treffer),
                         "l1fs-OST0014_UUID");   /* = Index 20 */
                enthaelt = false;
                check("Pool mit verbotenem OST wird erkannt",
                      pool_contains_banned(&o, "l1fs", "schlecht", &enthaelt) == 0 &&
                      enthaelt == true, NULL);

                snprintf(suche_ost_treffer, sizeof(suche_ost_treffer),
                         "l1fs-OST0000_UUID");   /* = Index 0 */
                enthaelt = false;
                check("auch OST 0 wird erkannt",
                      pool_contains_banned(&o, "l1fs", "schlecht", &enthaelt) == 0 &&
                      enthaelt == true, NULL);

                suche_ost_antwort = -1;
                check("Fehler beim Nachschlagen wird durchgereicht",
                      pool_contains_banned(&o, "l1fs", "x", &enthaelt) < 0, NULL);
                suche_ost_antwort = 0; suche_ost_treffer[0] = '\0';
        }

        /* 11d Machbarkeit: wie viele OSTs bleiben uebrig */
        {
                struct options o;

                memset(&o, 0, sizeof(o));
                o.root = "/l1fs";
                o.banned[0] = 0; o.banned_count = 1;
                o.stripe_count = 2; o.stripe_count_set = true;

                obd_count_antwort = 4;
                pool_mitglieder_anzahl = -1;
                check("4 OSTs, 1 verboten -> 3 nutzbar",
                      usable_ost_count(&o, "l1fs") == 3, NULL);

                o.banned[1] = 1; o.banned[2] = 2; o.banned[3] = 3;
                o.banned_count = 4;
                check("4 OSTs, alle 4 verboten -> 0 nutzbar",
                      usable_ost_count(&o, "l1fs") == 0, NULL);

                o.banned_count = 1;
                obd_count_antwort = -1;
                check("ohne Auskunft ueber die OST-Zahl: unbestimmt",
                      usable_ost_count(&o, "l1fs") < 0, NULL);

                /* Pool: Mitglieder werden einzeln geprueft */
                obd_count_antwort = 4;
                o.pool_name = "mig";
                pool_mitglieder[0] = "l1fs-OST0000_UUID";  /* 0, verboten */
                pool_mitglieder[1] = "l1fs-OST0002_UUID";  /* 2 */
                pool_mitglieder[2] = "l1fs-OST0003_UUID";  /* 3 */
                pool_mitglieder_anzahl = 3;
                check("Pool mit 3 Mitgliedern, 1 verboten -> 2 nutzbar",
                      usable_ost_count(&o, "l1fs") == 2, NULL);

                o.banned[1] = 2; o.banned[2] = 3; o.banned_count = 3;
                check("Pool, alle Mitglieder verboten -> 0 nutzbar",
                      usable_ost_count(&o, "l1fs") == 0, NULL);

                pool_mitglieder_anzahl = -1;
                check("Pool nicht auslesbar: unbestimmt",
                      usable_ost_count(&o, "l1fs") < 0, NULL);
                obd_count_antwort = -1;
                o.pool_name = NULL;
        }

        /* 11e --allowed-ost: Auswahl und Verteilung */
        {
                struct options o;
                int chosen[8];
                int i, verschieden = 0;
                int erster[64];

                memset(&o, 0, sizeof(o));
                o.allowed[0] = 1; o.allowed[1] = 2; o.allowed[2] = 3;
                o.allowed_count = 3;

                /* genau so viele wie Streifen: festgelegt, in Reihenfolge */
                pick_allowed_osts(&o, 0, 0, 3, chosen);
                check("drei erlaubte, drei Streifen: alle benutzt",
                      chosen[0] == 1 && chosen[1] == 2 && chosen[2] == 3, NULL);

                /* mehr erlaubte als Streifen: Auswahl haengt an der Inode */
                for (i = 0; i < 64; i++) {
                        pick_allowed_osts(&o, (uint64_t)i, 0, 2, chosen);
                        erster[i] = chosen[0];
                }
                for (i = 1; i < 64; i++)
                        if (erster[i] != erster[0])
                                verschieden++;
                check("verschiedene Dateien landen nicht alle gleich",
                      verschieden > 0, NULL);

                /* gleiche Datei, gleicher Versuch: gleiche Antwort */
                pick_allowed_osts(&o, 4711, 2, 2, chosen);
                i = chosen[0];
                pick_allowed_osts(&o, 4711, 2, 2, chosen);
                check("gleiche Datei und Versuch: gleiche Auswahl",
                      chosen[0] == i, NULL);

                /* naechster Versuch waehlt andere Ziele */
                pick_allowed_osts(&o, 4711, 3, 2, chosen);
                check("naechster Versuch waehlt anders", chosen[0] != i, NULL);

                /* nie ein OST ausserhalb der Liste */
                {
                        int j, k, drin = 1;

                        for (j = 0; j < 32; j++) {
                                pick_allowed_osts(&o, (uint64_t)j, j, 2, chosen);
                                for (k = 0; k < 2; k++)
                                        if (!ost_is_allowed(&o, chosen[k]))
                                                drin = 0;
                        }
                        check("Auswahl bleibt in der Liste", drin, NULL);
                }

                memset(&o, 0, sizeof(o));
                check("ohne Liste ist jeder OST erlaubt",
                      ost_is_allowed(&o, 99), NULL);
                o.allowed[0] = 2; o.allowed_count = 1;
                check("mit Liste ist ein fremder OST nicht erlaubt",
                      !ost_is_allowed(&o, 99) && ost_is_allowed(&o, 2), NULL);
        }

        /* 11f Erkennung des neuen Spiegels, auch bei Umnummerierung */
        {
                struct layout_state st;
                struct mirror_state *m;
                uint32_t neu_id = 0;

#define RESET2(size) do { memset(&st, 0, sizeof(st)); st.file_size = (size); } while (0)
#define ADD2(idv) (m = &st.mirrors[st.mirror_count++], m->id = (idv), \
                   m->components = 1, m->init_components = 1, \
                   m->coverage_end = (uint64_t)st.file_size, m)

                /* Nicht-FLR: vorher genau ein Spiegel mit ID 0. Der Server
                   nummeriert ihn auf 1 um und gibt dem neuen die 2 - beide
                   IDs sehen neu aus. */
                RESET2(4096);
                ADD2(1)->banned_components = 1;   /* der umnummerierte alte */
                ADD2(2);                          /* der neue */
                classify_mirrors(&st);
                check("Umnummerierung 0->1: der neue ist der mit der hoechsten ID",
                      check_new_mirror(&st, 1, &neu_id) == 0 && neu_id == 2,
                      NULL);

                /* Bereits FLR: alte IDs bleiben, der neue bekommt max+1 */
                RESET2(4096);
                ADD2(1); ADD2(2); ADD2(5);
                classify_mirrors(&st);
                check("bereits FLR: hoechste ID ist der neue",
                      check_new_mirror(&st, 2, &neu_id) == 0 && neu_id == 5,
                      NULL);

                /* Der neue liegt auf einem verbotenen OST */
                RESET2(4096);
                ADD2(1);
                ADD2(2)->banned_components = 1;
                classify_mirrors(&st);
                memset(&res, 0, sizeof(res));
                check("neuer Spiegel auf verbotenem OST wird verworfen",
                      check_new_mirror(&st, 1, &neu_id) < 0 && neu_id == 2,
                      NULL);

                /* Der neue liegt ausserhalb der Erlaubnisliste */
                RESET2(4096);
                ADD2(1);
                ADD2(2)->outside_allowed_components = 1;
                classify_mirrors(&st);
                memset(&res, 0, sizeof(res));
                check("neuer Spiegel ausserhalb --allowed-ost wird verworfen",
                      check_new_mirror(&st, 1, &neu_id) < 0 && neu_id == 2,
                      NULL);

                /* Die Zahl stimmt nicht: jemand anders hat das Layout bewegt */
                RESET2(4096);
                ADD2(1); ADD2(2); ADD2(3);
                classify_mirrors(&st);
                memset(&res, 0, sizeof(res));
                check("unerwartete Spiegelzahl wird als Fremdaenderung gemeldet",
                      check_new_mirror(&st, 1, &neu_id) < 0, NULL);
#undef RESET2
#undef ADD2
        }

        /* 12 parse_ost */
        {
                int idx;
                check("parse_ost 14", parse_ost("14", NULL, &idx) == 0 && idx == 14, NULL);
                check("parse_ost 0x0014", parse_ost("0x0014", NULL, &idx) == 0 && idx == 20, NULL);
                check("parse_ost l1fs-OST0014_UUID",
                      parse_ost("l1fs-OST0014_UUID", NULL, &idx) == 0 && idx == 0x14, NULL);
                check("parse_ost l1fs-OST0014",
                      parse_ost("l1fs-OST0014", NULL, &idx) == 0 && idx == 0x14, NULL);
                check("parse_ost fremdes fsname abgelehnt",
                      parse_ost("andere-OST0014_UUID", "l1fs", &idx) == -2, NULL);
                check("parse_ost passendes fsname akzeptiert",
                      parse_ost("l1fs-OST0014_UUID", "l1fs", &idx) == 0, NULL);
                check("parse_ost Unsinn abgelehnt",
                      parse_ost("keine-zahl", NULL, &idx) == -1, NULL);
                check("parse_ost negativ abgelehnt",
                      parse_ost("-3", NULL, &idx) == -1, NULL);
                check("parse_ost OST0014 ist hex",
                      parse_ost("OST0014", NULL, &idx) == 0 && idx == 0x14, NULL);
                check("parse_ost 0014 abgelehnt (oktal waere 12)",
                      parse_ost("0014", NULL, &idx) == -1, NULL);
                check("parse_ost 010 abgelehnt (oktal waere 8)",
                      parse_ost("010", NULL, &idx) == -1, NULL);
                check("parse_ost 0 allein bleibt gueltig",
                      parse_ost("0", NULL, &idx) == 0 && idx == 0, NULL);
                check("parse_ost fuehrendes Leerzeichen abgelehnt",
                      parse_ost(" 14", NULL, &idx) == -1, NULL);
                check("parse_ost 65536 ausserhalb des Bereichs",
                      parse_ost("65536", NULL, &idx) == -1, NULL);
                check("parse_ost 0xffff ist die Obergrenze",
                      parse_ost("0xffff", NULL, &idx) == 0 && idx == 65535, NULL);
        }

        /* 13 base64 */
        {
                char *b;
                b = base64_encode((const unsigned char *)"", 0);
                check("base64 leer", b && strcmp(b, "") == 0, b);
                free(b);
                b = base64_encode((const unsigned char *)"f", 1);
                check("base64 'f' -> Zg==", b && strcmp(b, "Zg==") == 0, b);
                free(b);
                b = base64_encode((const unsigned char *)"fo", 2);
                check("base64 'fo' -> Zm8=", b && strcmp(b, "Zm8=") == 0, b);
                free(b);
                b = base64_encode((const unsigned char *)"foobar", 6);
                check("base64 'foobar' -> Zm9vYmFy", b && strcmp(b, "Zm9vYmFy") == 0, b);
                free(b);
                b = base64_encode((const unsigned char *)"\xff\xfe\x00z", 4);
                check("base64 Nicht-UTF-8 mit NUL", b && strcmp(b, "//4Aeg==") == 0, b);
                free(b);
        }

        /* 14 Spiegel-Klassifikation: stale, nosync, offline, leer */
        {
                struct layout_state st;
                struct mirror_state *m;

#define RESET(size)  do { memset(&st, 0, sizeof(st)); st.file_size = (size); } while (0)
#define ADD(idv)     (m = &st.mirrors[st.mirror_count++], m->id = (idv), \
                      m->components = 1, m->init_components = 1, \
                      m->coverage_end = (uint64_t)st.file_size, m)

                RESET(4096); ADD(1);
                classify_mirrors(&st);
                check("sauberer Spiegel ist brauchbar",
                      st.usable_mirror_count == 1 && st.safe_mirror_count == 1, NULL);

                RESET(4096); ADD(1)->stale_components = 1;
                classify_mirrors(&st);
                check("stale: sicher, aber nicht brauchbar",
                      st.safe_mirror_count == 1 && st.usable_mirror_count == 0, NULL);

                RESET(4096); ADD(1)->unusable_components = 1;   /* nosync/offline */
                classify_mirrors(&st);
                check("nosync/offline: sicher, aber nicht brauchbar",
                      st.safe_mirror_count == 1 && st.usable_mirror_count == 0, NULL);

                RESET(4096); ADD(1)->init_components = 0;
                classify_mirrors(&st);
                check("nicht instanziiert bei Inhalt: nicht brauchbar",
                      st.usable_mirror_count == 0, NULL);

                RESET(0); ADD(1)->init_components = 0;
                classify_mirrors(&st);
                check("nicht instanziiert bei leerer Datei: brauchbar",
                      st.usable_mirror_count == 1, NULL);

                RESET(4096); ADD(1)->banned_components = 1;
                classify_mirrors(&st);
                check("verbotener Spiegel zaehlt nicht als sicher",
                      st.banned_mirror_count == 1 && st.safe_mirror_count == 0 &&
                      st.usable_mirror_count == 0, NULL);

                /* verboten + nosync + sauber: nur der saubere schuetzt */
                RESET(4096);
                ADD(1)->banned_components = 1;
                ADD(2)->unusable_components = 1;
                ADD(3);
                classify_mirrors(&st);
                check("gemischt: 1 verboten, 2 sicher, 1 brauchbar",
                      st.banned_mirror_count == 1 && st.safe_mirror_count == 2 &&
                      st.usable_mirror_count == 1, NULL);

                /* PFL: Komponente unterhalb EOF ohne init */
                RESET(1 << 20);
                m = ADD(1); m->coverage_end = 1 << 20; m->missing_data_components = 1;
                classify_mirrors(&st);
                check("PFL: Datenkomponente ohne init ist nicht brauchbar",
                      st.usable_mirror_count == 0 && st.safe_mirror_count == 1, NULL);

                /* Komponente jenseits EOF ohne init ist harmlos */
                RESET(1 << 20);
                m = ADD(1); m->coverage_end = 1 << 20;
                classify_mirrors(&st);
                check("PFL: Komponente jenseits EOF darf uninstanziiert sein",
                      st.usable_mirror_count == 1, NULL);

                /* Loch in der Abdeckung */
                RESET(1 << 20);
                m = ADD(1); m->coverage_end = 1 << 20; m->has_gap = true;
                classify_mirrors(&st);
                check("Loch in der Abdeckung: nicht brauchbar",
                      st.usable_mirror_count == 0, NULL);

                /* Abdeckung endet vor EOF */
                RESET(1 << 20);
                m = ADD(1); m->coverage_end = 4096;
                classify_mirrors(&st);
                check("Abdeckung endet vor EOF: nicht brauchbar",
                      st.usable_mirror_count == 0, NULL);

                /* Pool kann auf einen verbotenen OST wachsen */
                RESET(1 << 20);
                m = ADD(1); m->coverage_end = 1 << 20; m->pool_banned_components = 1;
                classify_mirrors(&st);
                check("Pool mit verbotenem OST: nicht brauchbar",
                      st.usable_mirror_count == 0, NULL);

                /* Spiegelgrenze folgt Lustre */
                check("MAX_MIRRORS entspricht Lustres Grenze",
                      MAX_MIRRORS == 16, NULL);

                /* Ueberzahl: der nosync-Spiegel muss zuerst weichen */
                {
                        uint32_t victim = 0;

                        RESET(4096);
                        ADD(1)->unusable_components = 1;
                        ADD(2);
                        ADD(3);
                        classify_mirrors(&st);
                        check("Ueberzahl: unbrauchbarer Spiegel wird geopfert",
                              pick_surplus_safe_mirror(&st, &victim) == 0 &&
                              victim == 1, NULL);

                        RESET(4096); ADD(1); ADD(2);
                        classify_mirrors(&st);
                        check("Ueberzahl: sonst der zweite brauchbare",
                              pick_surplus_safe_mirror(&st, &victim) == 0 &&
                              victim == 2, NULL);

                        RESET(4096); ADD(1);
                        classify_mirrors(&st);
                        check("letzter brauchbarer Spiegel wird nicht angeboten",
                              pick_surplus_safe_mirror(&st, &victim) < 0, NULL);

                        RESET(4096);
                        ADD(1)->unusable_components = 1;
                        ADD(2);
                        classify_mirrors(&st);
                        check("nosync neben einem einzigen brauchbaren: nosync weicht",
                              pick_surplus_safe_mirror(&st, &victim) == 0 &&
                              victim == 1, NULL);

                        RESET(4096);
                        ADD(1)->banned_components = 1;
                        ADD(2);
                        classify_mirrors(&st);
                        check("verbotener Spiegel wird zum Loeschen gewaehlt",
                              pick_banned_mirror(&st, &victim) == 0 && victim == 1, NULL);
                }
#undef RESET
#undef ADD
        }

        /* 15 Jede im Hilfetext genannte Option muss die Tabelle kennen.
           Ein Zweig ohne Eintrag in long_options ist unerreichbar, und der
           Aufrufer bekommt "unrecognized option". */
        {
                int stumm = open("/dev/null", O_WRONLY);
                int echt = dup(1);
                int echt_err = dup(2);

                static const char *mit_wert[] = {
                        "--root", "--stripe-count", "--pool-name",
                        "--banned-ost", "--target-mirror-count",
                        "--expected-fsname", "--allocation-attempts",
                        "--allowed-ost",
                };
                static const char *ohne_wert[] = {
                        "--keep-mirroring", "--verify", "--inspect-only",
                        "--collapse-to-one", "--json", "--dry-run",
                };
                static const char *werte[] = {
                        "/", "4", "pool", "14", "2", "fs", "3", "2",
                };
                size_t k;

                for (k = 0; k < sizeof(mit_wert)/sizeof(mit_wert[0]); k++) {
                        char *argv2[8];
                        struct options o;
                        int n = 0, ok;

                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)mit_wert[k];
                        argv2[n++] = (char *)werte[k];
                        /* Fuellwert, damit "nothing to do" nicht zuschlaegt.
                           Ein anderer Index als der der Tabelle, sonst waere
                           es beim --banned-ost-Durchlauf eine Dublette. */
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"15";
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) dup2(stumm, 1);
                        ok = parse_arguments(n, argv2, &o) == 0;
                        dup2(echt, 1);
                        snprintf(msg, sizeof(msg), "%s", ok ? "" : res.message);
                        check(mit_wert[k], ok, msg);
                }
                for (k = 0; k < sizeof(ohne_wert)/sizeof(ohne_wert[0]); k++) {
                        char *argv2[7];
                        struct options o;
                        int n = 0, ok;

                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"14";
                        argv2[n++] = (char *)ohne_wert[k];
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) dup2(stumm, 1);
                        ok = parse_arguments(n, argv2, &o) == 0;
                        dup2(echt, 1);
                        snprintf(msg, sizeof(msg), "%s", ok ? "" : res.message);
                        check(ohne_wert[k], ok, msg);
                }

                /* --keep-mirroring und --target-mirror-count schliessen sich seit der
                   Aufzeichnung des Ziels auf der Datei gegenseitig aus: beide
                   nennen die Endzahl, und "so viele wie vorher" ist keine
                   offene Frage mehr. Die Kombination steht in der Tabelle
                   weiter unten bei den abgelehnten Paaren. */

                /* Bewusste Verweigerungen */
                {
                        char *argv2[8];
                        struct options o;
                        int n = 0, verweigert;

                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"14";
                        argv2[n++] = (char *)"--allow-other-devices";
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) dup2(stumm, 1);
                        verweigert = parse_arguments(n, argv2, &o) < 0;
                        dup2(echt, 1);
                        check("--allow-other-devices mit --banned-ost abgelehnt",
                              verweigert, NULL);
                }
                {
                        char *argv2[8];
                        struct options o;
                        int n = 0, verweigert;

                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"14";
                        argv2[n++] = (char *)"--expected-dev";
                        argv2[n++] = (char *)"1";
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) dup2(stumm, 1);
                        verweigert = parse_arguments(n, argv2, &o) < 0;
                        dup2(echt, 1);
                        check("--expected-dev ohne --expected-ino abgelehnt",
                              verweigert, NULL);
                }
                /* --allowed-ost und --banned-ost duerfen sich nicht
                   ueberschneiden, und es muessen genug fuer die Streifen
                   sein. */
                {
                        char *argv2[12];
                        struct options o;
                        int n, verweigert;

                        n = 0;
                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"--allowed-ost";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); }
                        verweigert = parse_arguments(n, argv2, &o) < 0;
                        fflush(stdout); dup2(echt, 1); dup2(echt_err, 2);
                        check("OST in beiden Listen abgelehnt", verweigert, NULL);

                        n = 0;
                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"0";
                        argv2[n++] = (char *)"--stripe-count";
                        argv2[n++] = (char *)"4";
                        argv2[n++] = (char *)"--allowed-ost";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"--allowed-ost";
                        argv2[n++] = (char *)"3";
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); }
                        verweigert = parse_arguments(n, argv2, &o) < 0;
                        fflush(stdout); dup2(echt, 1); dup2(echt_err, 2);
                        check("zu wenige erlaubte OSTs fuer die Streifenzahl",
                              verweigert, NULL);

                        n = 0;
                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"0";
                        argv2[n++] = (char *)"--allowed-ost";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"--allowed-ost";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); }
                        verweigert = parse_arguments(n, argv2, &o) < 0;
                        fflush(stdout); dup2(echt, 1); dup2(echt_err, 2);
                        check("doppelt genannter erlaubter OST abgelehnt",
                              verweigert, NULL);
                }

                /* Entfernte Schreibweisen muessen abgelehnt bleiben. Sonst
                   kehrt ein Alias unbemerkt zurueck und die Kommandozeile
                   hat wieder zwei Namen fuer dieselbe Sache. */
                {
                        static const char *weg[] = {
                                "--strip-count", "--text", "--expect-dev",
                                "--expect-ino",
                        };
                        size_t w;

                        for (w = 0; w < sizeof(weg)/sizeof(weg[0]); w++) {
                                char *argv2[9];
                                struct options o;
                                int n = 0, verweigert;

                                argv2[n++] = (char *)"lustre-migrate-file";
                                argv2[n++] = (char *)"--root";
                                argv2[n++] = (char *)"/";
                                argv2[n++] = (char *)"--banned-ost";
                                argv2[n++] = (char *)"14";
                                argv2[n++] = (char *)weg[w];
                                argv2[n++] = (char *)"1";
                                argv2[n++] = (char *)"/datei";
                                optind = 1;
                                opterr = 0;
                                memset(&res, 0, sizeof(res));
                                fflush(stdout);
                                if (stumm >= 0) {
                                        dup2(stumm, 1);
                                        dup2(stumm, 2);
                                }
                                verweigert = parse_arguments(n, argv2, &o) < 0;
                                fflush(stdout);
                                dup2(echt, 1);
                                dup2(echt_err, 2);
                                snprintf(msg, sizeof(msg),
                                         "%s ist wieder da", weg[w]);
                                check(weg[w], verweigert,
                                      verweigert ? "abgelehnt" : msg);
                        }
                        opterr = 1;
                }

                /* Die Kommandozeile, die der Scheduler tatsaechlich baut -
                   in genau dieser Reihenfolge, mit "--" vor dem Pfad. */
                {
                        char *argv2[24];
                        struct options o;
                        int n = 0, ok3;

                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/laborlustre/jbod1";
                        argv2[n++] = (char *)"--stripe-count";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"--allocation-attempts";
                        argv2[n++] = (char *)"32";
                        argv2[n++] = (char *)"--pool-name";
                        argv2[n++] = (char *)"migration_pool";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"0";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"20";
                        argv2[n++] = (char *)"--keep-mirroring";
                        argv2[n++] = (char *)"--expected-dev";
                        argv2[n++] = (char *)"64768";
                        argv2[n++] = (char *)"--expected-ino";
                        argv2[n++] = (char *)"17654";
                        argv2[n++] = (char *)"--";
                        argv2[n++] = (char *)"/laborlustre/jbod1/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); }
                        ok3 = parse_arguments(n, argv2, &o) == 0;
                        fflush(stdout); dup2(echt, 1); dup2(echt_err, 2);
                        check("Scheduler-Kommandozeile wird angenommen",
                              ok3 && o.allocation_attempts == 32 &&
                              o.keep_mirroring &&
                              o.target_mirror_count < 0 &&
                              o.banned_count == 2 && o.expect_identity &&
                              strcmp(o.path, "/laborlustre/jbod1/datei") == 0,
                              ok3 ? NULL : res.message);

                        /* dieselbe Zeile mit --inspect-only statt der Ziele */
                        n = 0;
                        argv2[n++] = (char *)"lustre-migrate-file";
                        argv2[n++] = (char *)"--root";
                        argv2[n++] = (char *)"/laborlustre/jbod1";
                        argv2[n++] = (char *)"--stripe-count";
                        argv2[n++] = (char *)"2";
                        argv2[n++] = (char *)"--allocation-attempts";
                        argv2[n++] = (char *)"32";
                        argv2[n++] = (char *)"--banned-ost";
                        argv2[n++] = (char *)"0";
                        argv2[n++] = (char *)"--collapse-to-one";
                        argv2[n++] = (char *)"--inspect-only";
                        argv2[n++] = (char *)"--";
                        argv2[n++] = (char *)"/laborlustre/jbod1/datei";
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); }
                        ok3 = parse_arguments(n, argv2, &o) == 0;
                        fflush(stdout); dup2(echt, 1); dup2(echt_err, 2);
                        check("Scheduler-Zeile mit --inspect-only",
                              ok3 && o.inspect_only && o.collapse_to_one,
                              ok3 ? NULL : res.message);
                }

                if (stumm >= 0) close(stumm);
                close(echt);
                close(echt_err);
        }

        /* ---- SEL/PFL: Layouts mit leeren Komponenten --------------- */
        {
                struct options o;
                struct layout_state st;
                int fd;

                snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
                fd = open(p, O_RDONLY);
                assert(fd >= 0);

                memset(&o, 0, sizeof(o));
                o.root = root;

                /* Eine normale PFL-Datei: [0,1M) belegt, dahinter eine
                   Erweiterungskomponente. Genau das, was Lustres eigene
                   Pruefung fuer ein SEL-Layout verlangt. */
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_comp_add(0, 0, 1 << 20, 1 << 20, 0, 0);
                fake_comp_add(0, LCME_FL_EXTENSION, 1 << 20, LUSTRE_EOF, 0, 0);
                fake_mirror_count = 1;
                memset(&res, 0, sizeof(res));
                check("SEL-Layout mit leerer Komponente wird angenommen",
                      analyse_layout(&o, fd, "lfs", &st, "test") == 0,
                      res.message);
                check("die leere Komponente erzeugt keine Luecke",
                      st.mirror_count == 1 && st.usable_mirror_count == 1,
                      res.message);

                /* Erweiterungskomponente vollstaendig aufgebraucht: sie ist
                   dann selbst leer. */
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_comp_add(0, LCME_FL_EXTENSION, 1 << 20, 1 << 20, 0, 0);
                fake_mirror_count = 1;
                memset(&res, 0, sizeof(res));
                check("aufgebrauchte Erweiterungskomponente wird angenommen",
                      analyse_layout(&o, fd, "lfs", &st, "test") == 0,
                      res.message);

                /* Eine leere Komponente OHNE nachfolgende Erweiterung deckt
                   nichts ab und darf nicht durchgehen. */
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_comp_add(0, 0, 1 << 20, 1 << 20, 0, 0);
                fake_mirror_count = 1;
                memset(&res, 0, sizeof(res));
                check("leere Komponente ohne Erweiterung wird abgelehnt",
                      analyse_layout(&o, fd, "lfs", &st, "test") < 0,
                      res.message);

                /* Rueckwaerts laufender Extent bleibt ein Fehler. */
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 1 << 20, 1 << 10, 1, 3);
                fake_mirror_count = 1;
                memset(&res, 0, sizeof(res));
                check("end < start bleibt ein ungueltiger Extent",
                      analyse_layout(&o, fd, "lfs", &st, "test") < 0,
                      res.message);

                /* ---- Kapazitaetspruefung erst vor der Allokation ------ */
                /* Saubere Datei, alle OSTs bis auf einen verboten, zwei
                   Stripes verlangt: es wird kein Spiegel gebraucht, also
                   darf nichts mit ENOSPC scheitern. */
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_mirror_count = 1;
                memset(&o, 0, sizeof(o));
                o.root = root;
                o.banned[0] = 0; o.banned[1] = 1; o.banned[2] = 2;
                o.banned_count = 3;
                o.stripe_count = 2;
                o.stripe_count_set = true;
                o.collapse_to_one = true;
                o.allocation_attempts = 1;
                obd_count_antwort = 4;   /* 4 OSTs, 3 verboten -> 1 nutzbar */
                memset(&res, 0, sizeof(res));
                {
                        /* Ueber main(), denn dort stand die Pruefung frueher:
                           ein Test nur ueber migrate_file() wuerde die alte
                           Platzierung gar nicht erreichen. */
                        char *av[16];
                        int n = 0, rc_main;

                        snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
                        av[n++] = (char *)"lustre-migrate-file";
                        av[n++] = (char *)"--root";
                        av[n++] = root;
                        av[n++] = (char *)"--stripe-count";
                        av[n++] = (char *)"2";
                        av[n++] = (char *)"--banned-ost";
                        av[n++] = (char *)"0";
                        av[n++] = (char *)"--banned-ost";
                        av[n++] = (char *)"1";
                        av[n++] = (char *)"--banned-ost";
                        av[n++] = (char *)"2";
                        av[n++] = (char *)"--collapse-to-one";
                        av[n++] = (char *)"--";
                        av[n++] = p;

                        fsname_antwort = "lfs";
                        suche_ost_antwort = 1;
                        suche_ost_treffer[0] = '\0';

                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        fflush(stdout);
                        rc_main = lmf_main_unused(n, av);
                        check("saubere Datei scheitert nicht an der "
                              "OST-Kapazitaet",
                              rc_main == 0 && res.errno_value != ENOSPC,
                              res.message);

                        /* Dieselbe Lage, aber die Datei liegt auf einem
                           verbotenen OST: jetzt IST ein Spiegel noetig und
                           ENOSPC ist die richtige Antwort. */
                        fake_layout_reset();
                        fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 0);
                        fake_mirror_count = 1;
                        optind = 1;
                        memset(&res, 0, sizeof(res));
                        rc_main = lmf_main_unused(n, av);
                        check("unsaubere Datei meldet die OST-Kapazitaet",
                              rc_main != 0 && res.errno_value == ENOSPC,
                              res.message);

                        fsname_antwort = NULL;
                        suche_ost_antwort = 0;
                }
                obd_count_antwort = -1;
                fake_layout_active = 0;
                close(fd);
        }

        /* ---- Befunde der vierten Runde ----------------------------- */
        {
                struct options o;
                char *av[20];
                int n, ok;
                int stumm = open("/dev/null", O_WRONLY);
                int echt = dup(1), echt_err = dup(2);

#define STUMM_AN  do { fflush(stdout); if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); } } while (0)
#define STUMM_AUS do { fflush(stdout); dup2(echt, 1); dup2(echt_err, 2); } while (0)

                /* --inspect-only allein ist Arbeit, kein "nothing to do". */
                n = 0;
                av[n++] = (char *)"lustre-migrate-file";
                av[n++] = (char *)"--root";  av[n++] = (char *)"/";
                av[n++] = (char *)"--inspect-only";
                av[n++] = (char *)"/datei";
                optind = 1; memset(&res, 0, sizeof(res));
                STUMM_AN; ok = parse_arguments(n, av, &o) == 0; STUMM_AUS;
                check("--inspect-only allein wird angenommen", ok,
                      ok ? NULL : res.message);

                /* --collapse-to-one allein ebenso. Das ist genau die Zeile,
                   die der Scheduler bei leerer banned_osts-Liste sendet. */
                n = 0;
                av[n++] = (char *)"lustre-migrate-file";
                av[n++] = (char *)"--root";  av[n++] = (char *)"/";
                av[n++] = (char *)"--collapse-to-one";
                av[n++] = (char *)"/datei";
                optind = 1; memset(&res, 0, sizeof(res));
                STUMM_AN; ok = parse_arguments(n, av, &o) == 0; STUMM_AUS;
                check("--collapse-to-one allein wird angenommen", ok,
                      ok ? NULL : res.message);

                /* Ohne jede Angabe bleibt es bei der Ablehnung. */
                n = 0;
                av[n++] = (char *)"lustre-migrate-file";
                av[n++] = (char *)"--root";  av[n++] = (char *)"/";
                av[n++] = (char *)"/datei";
                optind = 1; memset(&res, 0, sizeof(res));
                STUMM_AN; ok = parse_arguments(n, av, &o) == 0; STUMM_AUS;
                check("ohne jede Angabe weiter abgelehnt", !ok, NULL);

                /* Die drei Optionen, die die Spiegelzahl bestimmen: genau
                   eine davon darf sprechen. --keep-mirroring mit
                   --target-mirror-count ist die Ausnahme - dort sagt die
                   Zahl, WELCHE Zahl gehalten wird. */
                {
                        static const char *paare[][4] = {
                          { "--keep-mirroring", "--collapse-to-one", NULL, NULL },
                          { "--collapse-to-one", "--target-mirror-count", "3", NULL },
                          { "--keep-mirroring", "--target-mirror-count", "3", NULL },
                        };
                        static const char *erlaubt[][4] = {
                          { "--collapse-to-one", NULL, NULL, NULL },
                          { "--keep-mirroring", NULL, NULL, NULL },
                          { "--target-mirror-count", "3", NULL, NULL },
                          { "--inspect-only", NULL, NULL, NULL },
                        };
                        size_t k, j;

                        for (k = 0; k < sizeof(paare)/sizeof(paare[0]); k++) {
                                n = 0;
                                av[n++] = (char *)"lustre-migrate-file";
                                av[n++] = (char *)"--root";
                                av[n++] = (char *)"/";
                                for (j = 0; j < 4 && paare[k][j]; j++)
                                        av[n++] = (char *)paare[k][j];
                                av[n++] = (char *)"/datei";
                                optind = 1; memset(&res, 0, sizeof(res));
                                STUMM_AN;
                                ok = parse_arguments(n, av, &o) == 0;
                                STUMM_AUS;
                                check("zwei Angaben zur Spiegelzahl abgelehnt",
                                      !ok, res.message);
                        }
                        for (k = 0; k < sizeof(erlaubt)/sizeof(erlaubt[0]); k++) {
                                n = 0;
                                av[n++] = (char *)"lustre-migrate-file";
                                av[n++] = (char *)"--root";
                                av[n++] = (char *)"/";
                                for (j = 0; j < 4 && erlaubt[k][j]; j++)
                                        av[n++] = (char *)erlaubt[k][j];
                                av[n++] = (char *)"/datei";
                                optind = 1; memset(&res, 0, sizeof(res));
                                STUMM_AN;
                                ok = parse_arguments(n, av, &o) == 0;
                                STUMM_AUS;
                                check("zulaessige Spiegelzahl-Angabe angenommen",
                                      ok, ok ? NULL : res.message);
                        }
                }

                /* Doppelte --banned-ost: usable_ost_count() zieht die
                   Listenlaenge ab, eine Dublette waere ein falsches ENOSPC. */
                n = 0;
                av[n++] = (char *)"lustre-migrate-file";
                av[n++] = (char *)"--root";  av[n++] = (char *)"/";
                av[n++] = (char *)"--banned-ost"; av[n++] = (char *)"3";
                av[n++] = (char *)"--banned-ost"; av[n++] = (char *)"0x0003";
                av[n++] = (char *)"/datei";
                optind = 1; memset(&res, 0, sizeof(res));
                STUMM_AN; ok = parse_arguments(n, av, &o) == 0; STUMM_AUS;
                check("doppelt genannter verbotener OST abgelehnt", !ok,
                      res.message);

                if (stumm >= 0) close(stumm);
                close(echt); close(echt_err);
#undef STUMM_AN
#undef STUMM_AUS
        }

        /* Logische Fehler duerfen errno nicht erben. */
        {
                struct options o;
                struct resolved rr = { .parent_fd = -1, .file_fd = -1 };

                memset(&o, 0, sizeof(o));
                o.root = root;
                snprintf(p, sizeof(p), "%s/aussen/geheim", base);
                o.path = p;
                memset(&res, 0, sizeof(res));
                errno = EBUSY;          /* Rest eines frueheren Aufrufs */
                (void)resolve_path(&o, &rr);
                if (rr.file_fd >= 0) close(rr.file_fd);
                if (rr.parent_fd >= 0) close(rr.parent_fd);
                check("logischer Fehler erbt kein fremdes errno",
                      res.errno_value != EBUSY && !res.transient &&
                      res.exit_code == 1,
                      res.message);

                /* Ein echter Syscall-Fehler behaelt sein errno. */
                memset(&o, 0, sizeof(o));
                o.root = root;
                snprintf(p, sizeof(p), "%s/root/gibtsnicht", base);
                o.path = p;
                memset(&res, 0, sizeof(res));
                rr.parent_fd = -1; rr.file_fd = -1;
                errno = 0;
                (void)resolve_path(&o, &rr);
                if (rr.file_fd >= 0) close(rr.file_fd);
                if (rr.parent_fd >= 0) close(rr.parent_fd);
                check("Syscall-Fehler behaelt sein errno",
                      res.errno_value == ENOENT, res.message);
        }

        /* ---- --allowed-ost: UUID eines fremden Dateisystems --------- */
        {
                char *av[16];
                int n, rc_main;
                int stumm = open("/dev/null", O_WRONLY);
                int echt = dup(1), echt_err = dup(2);

                snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
                n = 0;
                av[n++] = (char *)"lustre-migrate-file";
                av[n++] = (char *)"--root";  av[n++] = root;
                av[n++] = (char *)"--allowed-ost";
                av[n++] = (char *)"fremd-OST0003_UUID";
                av[n++] = (char *)"--banned-ost"; av[n++] = (char *)"0";
                av[n++] = (char *)"--collapse-to-one";
                av[n++] = (char *)"--";
                av[n++] = p;

                fsname_antwort = "lfs";
                suche_ost_antwort = 1;
                suche_ost_treffer[0] = '\0';
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_mirror_count = 1;
                optind = 1; memset(&res, 0, sizeof(res));
                fflush(stdout);
                if (stumm >= 0) { dup2(stumm, 1); dup2(stumm, 2); }
                rc_main = lmf_main_unused(n, av);
                fflush(stdout); dup2(echt, 1); dup2(echt_err, 2);
                check("erlaubter OST eines fremden Dateisystems abgelehnt",
                      rc_main != 0 && strstr(res.message, "allowed OST") != NULL,
                      res.message);

                fsname_antwort = NULL;
                suche_ost_antwort = 0;
                fake_layout_active = 0;
                if (stumm >= 0) close(stumm);
                close(echt); close(echt_err);
        }

        /* ---- Loeschursache: sauberer Ueberschussspiegel ------------- */
        {
                struct options o;
                struct resolved rr;
                int fd;

                snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
                fd = open(p, O_RDONLY);
                assert(fd >= 0);
                memset(&rr, 0, sizeof(rr));
                rr.file_fd = fd;
                rr.parent_fd = -1;
                (void)fstat(fd, &rr.st);

                memset(&o, 0, sizeof(o));
                o.root = root;
                lease_ok = 1;
                lease_set_ergebnis = 1;

                /* Zwei saubere Spiegel, keiner verboten. */
#define ZWEI_SAUBERE do {                                       \
                fake_layout_reset();                            \
                fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3); \
                fake_comp_add(2, LCME_FL_INIT, 0, 1 << 20, 1, 1); \
                fake_mirror_count = 2;                          \
                memset(&res, 0, sizeof(res));                   \
        } while (0)

                ZWEI_SAUBERE;
                check("sauberer Ueberschussspiegel wird geloescht",
                      delete_one_mirror(&o, &rr, "lfs", 2,
                                        DELETE_SURPLUS, 1) == 0,
                      res.message);

                ZWEI_SAUBERE;
                check("kein Ueberschuss: Loeschung abgelehnt",
                      delete_one_mirror(&o, &rr, "lfs", 2,
                                        DELETE_SURPLUS, 2) < 0 &&
                      res.transient,
                      res.message);

                ZWEI_SAUBERE;
                check("DELETE_BANNED verlangt weiter einen verbotenen Spiegel",
                      delete_one_mirror(&o, &rr, "lfs", 2,
                                        DELETE_BANNED, 1) < 0,
                      res.message);

                /* Rueckrollen eines gerade angelegten, nicht verbotenen
                   Spiegels - der Fall, den --allowed-ost erzeugt. */
                ZWEI_SAUBERE;
                check("neu angelegter, nicht verbotener Spiegel wird "
                      "zurueckgerollt",
                      delete_one_mirror(&o, &rr, "lfs", 2,
                                        DELETE_ROLLBACK_ADDED, 0) == 0,
                      res.message);

                /* Nur ein brauchbarer Spiegel: nichts darf geloescht werden,
                   egal aus welchem Grund. */
                fake_layout_reset();
                fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_comp_add(2, LCME_FL_INIT | LCME_FL_STALE, 0, 1 << 20, 1, 1);
                fake_mirror_count = 2;
                memset(&res, 0, sizeof(res));
                check("letzter brauchbarer Spiegel bleibt, auch beim Rollback",
                      delete_one_mirror(&o, &rr, "lfs", 1,
                                        DELETE_ROLLBACK_ADDED, 0) < 0,
                      res.message);
#undef ZWEI_SAUBERE
                lease_ok = 0;
                lease_set_ergebnis = -1;
                fake_layout_active = 0;
                close(fd);
        }

        /* ---- logische Fehler erben kein errno mehr ----------------- */
        {
                struct options o;
                struct layout_state st;
                int fd;

                snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
                fd = open(p, O_RDONLY);
                assert(fd >= 0);
                memset(&o, 0, sizeof(o));
                o.root = root;

                /* llapi_layout_sanity() liefert einen LSE_*-Code, kein
                   errno. Mit vorher gesetztem EBUSY wurde daraus frueher
                   ein retryfaehiger Fehler. */
                fake_layout_reset();
                fake_comp_add(0, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_mirror_count = 1;
                sanity_antwort = 7;
                memset(&res, 0, sizeof(res));
                errno = EBUSY;
                check("Layout-Sanity-Fehler ist dauerhaft",
                      analyse_layout(&o, fd, "lfs", &st, "test") < 0 &&
                      res.exit_code == 1 && !res.transient,
                      res.message);
                sanity_antwort = 0;

                /* Spiegelzahl widerspricht den Komponenten: ebenfalls
                   logisch, ebenfalls dauerhaft. */
                fake_layout_reset();
                fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                fake_mirror_count = 3;
                memset(&res, 0, sizeof(res));
                errno = EBUSY;
                check("widerspruechliche Spiegelzahl ist dauerhaft",
                      analyse_layout(&o, fd, "lfs", &st, "test") < 0 &&
                      res.exit_code == 1 && !res.transient,
                      res.message);

                /* errno_of_rc(): ein negativer Rueckgabewert bestimmt die
                   Einstufung, nicht das zufaellige errno. */
                errno = EBUSY;
                check("errno_of_rc nimmt den Rueckgabewert",
                      errno_of_rc(-EINVAL) == EINVAL &&
                      errno_of_rc(-EAGAIN) == EAGAIN &&
                      errno_of_rc(0) == EIO, NULL);

                fake_layout_active = 0;
                close(fd);
        }

        /* ---- Inspektion braucht keinen Schreibzugriff ---------------- */
        {
                struct options o;
                struct resolved rr;
                char nurlesbar[8192];

                snprintf(nurlesbar, sizeof(nurlesbar), "%s/root/a/nurlesen",
                         base);
                write_file(nurlesbar, "inhalt");
                assert(chmod(nurlesbar, 0444) == 0);

                memset(&o, 0, sizeof(o));
                o.root = root;
                o.path = nurlesbar;
                memset(&rr, 0, sizeof(rr));
                rr.parent_fd = -1; rr.file_fd = -1;
                memset(&res, 0, sizeof(res));
                if (geteuid() == 0) {
                        check("Inspektion oeffnet nur lesend "
                              "(als root nicht pruefbar)", 1, NULL);
                } else {
                        int rc_migrate = resolve_path(&o, &rr);

                        if (rr.file_fd >= 0) close(rr.file_fd);
                        if (rr.parent_fd >= 0) close(rr.parent_fd);
                        check("Migration braucht Schreibzugriff",
                              rc_migrate < 0, res.message);

                        o.inspect_only = true;
                        memset(&rr, 0, sizeof(rr));
                        rr.parent_fd = -1; rr.file_fd = -1;
                        memset(&res, 0, sizeof(res));
                        check("Inspektion gelingt auf einer nur lesbaren Datei",
                              resolve_path(&o, &rr) == 0, res.message);
                        if (rr.file_fd >= 0) close(rr.file_fd);
                        if (rr.parent_fd >= 0) close(rr.parent_fd);
                }
                chmod(nurlesbar, 0644);
        }

        /* ---- Textausgabe zeigt die Nachricht ------------------------ */
        {
                struct options o;
                char ausgabe[8192];
                char zeile[512] = "";
                int fd_tmp, echt_err;
                FILE *f;

                snprintf(ausgabe, sizeof(ausgabe), "%s/textausgabe.txt", base);
                memset(&o, 0, sizeof(o));
                o.path = (char *)"/pfad/datei";
                o.json_output = false;
                memset(&res, 0, sizeof(res));
                res.status = "ok";
                res.exit_code = 0;
                snprintf(res.message, sizeof(res.message),
                         "would migrate: 1 of 1 mirrors on banned OSTs");

                fflush(stderr);
                echt_err = dup(2);
                fd_tmp = open(ausgabe, O_WRONLY | O_CREAT | O_TRUNC, 0600);
                assert(fd_tmp >= 0);
                dup2(fd_tmp, 2);
                emit_result(&o);
                fflush(stderr);
                dup2(echt_err, 2);
                close(fd_tmp); close(echt_err);

                f = fopen(ausgabe, "r");
                if (f) { if (!fgets(zeile, sizeof(zeile), f)) zeile[0] = '\0'; fclose(f); }
                check("die lesbare Erfolgsform nennt die Nachricht",
                      strstr(zeile, "would migrate") != NULL, zeile);
        }

        /* ---- Spiegelzahl ueberlebt den Prozess ---------------------- */
        {
                int fd;
                int gelesen = 0;

                snprintf(p, sizeof(p), "%s/root/a/b/datei", base);
                fd = open(p, O_RDWR);
                assert(fd >= 0);

                check("ohne Notiz meldet read_mirror_goal 'abwesend'",
                      ziel_lesen(fd, &gelesen) == 0 && gelesen == 0, NULL);

                check("das Schreiben der Notiz meldet Erfolg",
                      write_mirror_goal(fd, 3) == 0, strerror(errno));
                check("die geschriebene Notiz wird wiedergefunden",
                      ziel_lesen(fd, &gelesen) == 1 && gelesen == 3, NULL);
                check("der Namensraum richtet sich nach dem Recht",
                      strcmp(mirror_goal_xattr(),
                             geteuid() == 0 ? MIRROR_GOAL_XATTR_TRUSTED
                                            : MIRROR_GOAL_XATTR_USER) == 0,
                      mirror_goal_xattr());

                /* Genau der Fall aus dem Befund: die Datei hat nur noch
                   einen Spiegel, weil ein abgestuerzter Lauf einen
                   geloescht hat. Ohne Notiz waere das neue Ziel 1. */
                {
                        struct options o;
                        struct resolved rr;
                        int rc_m;

                        memset(&o, 0, sizeof(o));
                        o.root = root;
                        o.keep_mirroring = true;
                        o.allocation_attempts = 1;
                        memset(&rr, 0, sizeof(rr));
                        rr.file_fd = fd;
                        rr.parent_fd = -1;
                        (void)fstat(fd, &rr.st);

                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_mirror_count = 1;
                        memset(&res, 0, sizeof(res));
                        lease_ok = 0;
                        rc_m = migrate_file(&o, &rr, "lfs");
                        (void)rc_m;
                        check("das Ziel kommt aus der Notiz, nicht aus dem "
                              "verringerten Bestand",
                              res.target_mirror_count == 3,
                              res.message);
                        fake_layout_active = 0;
                }

                clear_mirror_goal(fd);
                check("nach dem Entfernen ist die Notiz weg",
                      ziel_lesen(fd, &gelesen) == 0 && gelesen == 0, NULL);

                /* Der kritische Punkt: die Inspektion muss das ZIEL melden,
                   nicht den verringerten Bestand - sonst reicht der Aufrufer
                   genau die falsche Zahl als --target-mirror-count zurueck. */
                {
                        struct options o;
                        struct resolved rr;

                        write_mirror_goal(fd, 3);
                        memset(&o, 0, sizeof(o));
                        o.root = root;
                        o.keep_mirroring = true;
                        o.inspect_only = true;
                        o.allocation_attempts = 1;
                        memset(&rr, 0, sizeof(rr));
                        rr.file_fd = fd; rr.parent_fd = -1;
                        (void)fstat(fd, &rr.st);

                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_mirror_count = 1;
                        memset(&res, 0, sizeof(res));
                        res.initial_mirror_count = -1;
                        (void)migrate_file(&o, &rr, "lfs");
                        check("--inspect-only meldet das aufgezeichnete Ziel",
                              res.initial_mirror_count == 3 &&
                              res.mirror_count_before == 1,
                              res.message);
                        check("die Inspektion laesst die Notiz stehen",
                              ziel_lesen(fd, &gelesen) == 1 && gelesen == 3, NULL);

                        /* Eine Notiz muss auch dann verschwinden, wenn das
                           Ziel NICHT aus ihr stammt: sonst loescht ein
                           spaeteres --keep-mirroring einen Spiegel, den
                           jemand gerade ausdruecklich angelegt hat. Hier
                           laeuft ein echter Loeschpfad bis zur Endpruefung:
                           drei Spiegel, einer auf einem verbotenen OST,
                           ausdrueckliches Ziel zwei. */
                        write_mirror_goal(fd, 2);
                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 0);
                        fake_comp_add(2, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_comp_add(3, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_mirror_count = 3;
                        o.keep_mirroring = false;
                        o.inspect_only = false;
                        o.target_mirror_count = 2;
                        o.banned[0] = 0; o.banned_count = 1;
                        lease_ok = 1; lease_set_ergebnis = 1;
                        memset(&res, 0, sizeof(res));
                        {
                                int rc_m = migrate_file(&o, &rr, "lfs");

                                check("Loeschpfad laeuft bis zur Endpruefung",
                                      rc_m == 0 && res.mirrors_deleted == 1 &&
                                      res.mirror_count_after == 2,
                                      res.message);
                                check("ausdrueckliches Ziel raeumt die fremde "
                                      "Notiz ab",
                                      ziel_lesen(fd, &gelesen) == 0, NULL);
                        }
                        o.target_mirror_count = -1;
                        o.banned_count = 0;
                        o.keep_mirroring = true;
                        lease_ok = 0; lease_set_ergebnis = -1;

                        /* Ist das Ziel wieder erreicht, muss die Notiz weg -
                           sonst gilt die Datei fuer immer als unfertig. */
                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_comp_add(2, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_comp_add(3, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_mirror_count = 3;
                        o.inspect_only = false;
                        memset(&res, 0, sizeof(res));
                        check("erreichter Zustand raeumt die Notiz ab",
                              migrate_file(&o, &rr, "lfs") == 0 &&
                              ziel_lesen(fd, &gelesen) == 0,
                              res.message);
                        fake_layout_active = 0;
                }

                /* Laesst sich die Notiz nicht schreiben, darf nichts
                   geloescht werden. */
                {
                        struct options o;
                        struct resolved rr;
                        char nurlesen[8192];
                        int fd_ro;

                        snprintf(nurlesen, sizeof(nurlesen),
                                 "%s/root/a/b/keinxattr", base);
                        write_file(nurlesen, "inhalt");
                        fd_ro = open(nurlesen, O_RDONLY);
                        assert(fd_ro >= 0);
                        assert(chmod(nurlesen, 0444) == 0);

                        if (geteuid() == 0 || write_mirror_goal(fd_ro, 2) == 0) {
                                check("ohne Notiz wird nicht geloescht "
                                      "(hier nicht pruefbar)", 1, NULL);
                        } else {
                                memset(&o, 0, sizeof(o));
                                o.root = root;
                                o.keep_mirroring = true;
                                o.allocation_attempts = 1;
                                o.banned[0] = 0; o.banned_count = 1;
                                memset(&rr, 0, sizeof(rr));
                                rr.file_fd = fd_ro; rr.parent_fd = -1;
                                (void)fstat(fd_ro, &rr.st);

                                fake_layout_reset();
                                fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 0);
                                fake_comp_add(2, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                                fake_mirror_count = 2;
                                memset(&res, 0, sizeof(res));
                                check("ohne schreibbare Notiz wird nichts "
                                      "geloescht",
                                      migrate_file(&o, &rr, "lfs") < 0 &&
                                      strstr(res.message,
                                             "cannot record the mirror goal")
                                      != NULL,
                                      res.message);
                                fake_layout_active = 0;
                        }
                        chmod(nurlesen, 0644);
                        close(fd_ro);
                }

                /* Unsinniger Inhalt zaehlt nicht als Ziel. */
                {
                        const char *muell = "keine zahl";

                        /* fsetxattr ist im Programm bereits portabel
                           ueberdeckt, also hier genau wie dort aufrufen. */
                        (void)fsetxattr(fd, mirror_goal_xattr(), muell,
                                        strlen(muell), 0);
                        check("unlesbarer Inhalt wird abgelehnt statt ignoriert",
                              ziel_lesen(fd, &gelesen) < 0, res.message);
                        clear_mirror_goal(fd);
                }

                /* Ein stale Spiegel muss auch dann nachgezogen werden, wenn
                   bereits eine brauchbare Kopie da ist. Sonst faellt eine
                   Datei mit richtiger Anzahl, ohne gesperrten Spiegel und mit
                   einem stale Spiegel durch alle Schritte und scheitert an
                   der Endpruefung - bei jedem Wiederholungsversuch aufs Neue,
                   weil jeder denselben Weg nimmt.

                   Geprueft wird die STUFE: die Stubs lassen den resync
                   scheitern, aber dass er ueberhaupt versucht wird, ist die
                   Aussage. Vorher lautete sie "verify". */
                {
                        struct options o;
                        struct resolved rr;

                        clear_mirror_goal(fd);
                        memset(&o, 0, sizeof(o));
                        o.root = root;
                        o.keep_mirroring = true;
                        o.allocation_attempts = 1;
                        memset(&rr, 0, sizeof(rr));
                        rr.file_fd = fd; rr.parent_fd = -1;
                        (void)fstat(fd, &rr.st);

                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_comp_add(2, LCME_FL_INIT | LCME_FL_STALE,
                                      0, 1 << 20, 1, 4);
                        fake_mirror_count = 2;
                        lease_ok = 1; lease_set_ergebnis = 1;
                        memset(&res, 0, sizeof(res));
                        check("stale wird auch neben einer brauchbaren Kopie "
                              "nachgezogen",
                              migrate_file(&o, &rr, "lfs") < 0 &&
                              res.stage && strcmp(res.stage, "resync") == 0,
                              res.stage ? res.stage : "(null)");
                        fake_layout_active = 0;
                        clear_mirror_goal(fd);
                }

                /* --- Zehnte Pruefrunde --- */

                /* Befund 8: ein NOSYNC-Spiegel ist konfiguriert zurueckzuhaengen.
                   llapi_mirror_find_stale() ueberspringt ihn, ein resync
                   koennte also nichts tun - er darf any_stale nicht setzen,
                   sonst lehnt die Endpruefung ihn ewig ab. */
                {
                        struct options o;
                        struct layout_state st;

                        memset(&o, 0, sizeof(o));
                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_comp_add(2, LCME_FL_INIT | LCME_FL_STALE |
                                      LCME_FL_NOSYNC, 0, 1 << 20, 1, 4);
                        fake_mirror_count = 2;
                        memset(&res, 0, sizeof(res));
                        check("stale NOSYNC setzt any_stale nicht",
                              analyse_layout(&o, fd, "lfs", &st, "test") == 0 &&
                              !st.any_stale, res.message);
                        check("der NOSYNC-Spiegel gilt trotzdem als unbrauchbar",
                              st.usable_mirror_count == 1, NULL);
                        fake_layout_active = 0;
                }

                /* Befund 6: WRITE_PENDING ohne sichtbares STALE darf nicht als
                   fertig gelten - da ist gerade ein Schreiber unterwegs. */
                {
                        struct options o;
                        struct resolved rr;

                        clear_mirror_goal(fd);
                        memset(&o, 0, sizeof(o));
                        o.root = root; o.collapse_to_one = true;
                        o.allocation_attempts = 1;
                        memset(&rr, 0, sizeof(rr));
                        rr.file_fd = fd; rr.parent_fd = -1;
                        (void)fstat(fd, &rr.st);

                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_mirror_count = 1;
                        fake_flr_flags = LCM_FL_WRITE_PENDING;
                        lease_ok = 1; lease_set_ergebnis = 1;
                        memset(&res, 0, sizeof(res));
                        check("WRITE_PENDING gilt nicht als erledigt",
                              migrate_file(&o, &rr, "lfs") < 0,
                              res.message);
                        fake_flr_flags = 0;
                        fake_layout_active = 0;
                        clear_mirror_goal(fd);
                }

                /* Befund 7: ein stale Spiegel, der ohnehin geloescht wird,
                   darf vorher nicht kopiert werden. Mit den Stubs scheitert
                   jeder resync, ein Erfolg beweist also, dass keiner lief. */
                {
                        struct options o;
                        struct resolved rr;

                        clear_mirror_goal(fd);
                        memset(&o, 0, sizeof(o));
                        o.root = root; o.collapse_to_one = true;
                        o.allocation_attempts = 1;
                        o.banned[0] = 0; o.banned_count = 1;
                        memset(&rr, 0, sizeof(rr));
                        rr.file_fd = fd; rr.parent_fd = -1;
                        (void)fstat(fd, &rr.st);

                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT | LCME_FL_STALE,
                                      0, 1 << 20, 1, 0);   /* gesperrt UND stale */
                        fake_comp_add(2, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_mirror_count = 2;
                        lease_ok = 1; lease_set_ergebnis = 1;
                        memset(&res, 0, sizeof(res));
                        {
                                int rc_m = migrate_file(&o, &rr, "lfs");

                                check("ein zu loeschender stale Spiegel wird "
                                      "nicht erst nachgezogen",
                                      rc_m == 0 && res.resyncs == 0 &&
                                      res.mirrors_deleted == 1,
                                      res.message);
                        }
                        o.banned_count = 0;
                        fake_layout_active = 0;
                        clear_mirror_goal(fd);
                }

                /* Elfte Runde: ein WRITE_PENDING, das kein resync aufloesen
                   kann, weil nur NOSYNC-Komponenten zurueckhaengen. Vorher
                   scheiterte so eine Datei bei jedem Versuch und in jedem
                   spaeteren Lauf erneut. */
                {
                        struct options o;
                        struct resolved rr;
                        struct layout_state st;
                        char verz[8192];
                        int pfd;

                        snprintf(verz, sizeof(verz), "%s/root/a/b", base);
                        pfd = open(verz, O_RDONLY | O_DIRECTORY);
                        assert(pfd >= 0);

                        clear_mirror_goal(fd);
                        memset(&o, 0, sizeof(o));
                        o.root = root; o.collapse_to_one = true;
                        o.allocation_attempts = 1;
                        memset(&rr, 0, sizeof(rr));
                        rr.file_fd = fd; rr.parent_fd = pfd;
                        snprintf(rr.leaf, sizeof(rr.leaf), "datei");
                        (void)fstat(fd, &rr.st);

                        fake_layout_reset();
                        fake_comp_add(1, LCME_FL_INIT, 0, 1 << 20, 1, 3);
                        fake_comp_add(2, LCME_FL_INIT | LCME_FL_STALE |
                                      LCME_FL_NOSYNC, 0, 1 << 20, 1, 4);
                        fake_mirror_count = 2;
                        fake_flr_flags = LCM_FL_WRITE_PENDING;
                        memset(&res, 0, sizeof(res));
                        check("NOSYNC-stale ist kein actionable stale",
                              analyse_layout(&o, fd, "lfs", &st, "test") == 0 &&
                              !st.any_stale && st.needs_resync,
                              res.message);

                        /* Der resync findet nichts - genau wie
                           llapi_mirror_find_stale() bei reinem NOSYNC. */
                        find_stale_antwort = 0;
                        lease_ok = 1; lease_set_ergebnis = 1;
                        o.collapse_to_one = false;
                        o.target_mirror_count = 2;
                        memset(&res, 0, sizeof(res));
                        {
                                int rc_m = migrate_file(&o, &rr, "lfs");

                                check("ein nicht aufloesbares Pending laesst "
                                      "den Lauf fertig werden",
                                      rc_m == 0 && res.pending_layout,
                                      res.message);
                        }
                        find_stale_antwort = -1;
                        o.target_mirror_count = -1;
                        fake_flr_flags = 0;
                        fake_layout_active = 0;
                        close(pfd);
                        clear_mirror_goal(fd);
                }
                close(fd);
        }

        snprintf(p, sizeof(p), "rm -rf %s", base);
        if (system(p) != 0)
                fprintf(stderr, "Aufraeumen fehlgeschlagen\n");

        printf("\n%s\n", failures ? "FEHLGESCHLAGEN" : "alle Tests bestanden");
        return failures ? 1 : 0;
}
