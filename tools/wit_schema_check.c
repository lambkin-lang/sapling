/*
 * wit_schema_check.c - DBI manifest and runner DBI drift checks
 *
 * Replaces:
 *   - tools/check_dbi_manifest.py
 *   - tools/check_runner_dbi_status.py
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct
{
    int dbi;
    char *name;
    char *status;
    int line_no;
} ManifestEntry;

typedef struct
{
    ManifestEntry *items;
    size_t len;
    size_t cap;
    int max_dbi;
} Manifest;

typedef struct
{
    char *name; /* SAP_WIT_DBI_* */
    int dbi;
} WitMacro;

typedef struct
{
    WitMacro *items;
    size_t len;
    size_t cap;
} MacroTable;

typedef struct
{
    int *vals;
    size_t len;
    size_t cap;
} IntList;

static int failf(const char *prefix, const char *fmt, ...)
{
    va_list ap;
    fprintf(stderr, "%s: FAIL: ", prefix);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
    return 1;
}

static char *xstrdup(const char *s)
{
    size_t n = strlen(s);
    char *out = (char *)malloc(n + 1u);
    if (!out)
        return NULL;
    memcpy(out, s, n + 1u);
    return out;
}

static int path_exists(const char *path)
{
    struct stat st;
    return (stat(path, &st) == 0);
}

static char *path_join2(const char *a, const char *b)
{
    size_t na = strlen(a);
    size_t nb = strlen(b);
    int need_slash = (na > 0u && a[na - 1u] != '/');
    size_t total = na + (need_slash ? 1u : 0u) + nb + 1u;
    char *out = (char *)malloc(total);
    if (!out)
        return NULL;
    memcpy(out, a, na);
    if (need_slash)
        out[na++] = '/';
    memcpy(out + na, b, nb);
    out[na + nb] = '\0';
    return out;
}

static int has_suffix(const char *s, const char *suffix)
{
    size_t ns = strlen(s);
    size_t nf = strlen(suffix);
    if (nf > ns)
        return 0;
    return (memcmp(s + ns - nf, suffix, nf) == 0);
}

static int starts_with(const char *s, const char *prefix)
{
    size_t np = strlen(prefix);
    return (strncmp(s, prefix, np) == 0);
}

static char *trim_ws(char *s)
{
    char *end;
    while (*s && isspace((unsigned char)*s))
        s++;
    end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1]))
        end--;
    *end = '\0';
    return s;
}

static int is_word_char(int c)
{
    return (isalnum((unsigned char)c) || c == '_');
}

static int is_macro_token_char(int c)
{
    return ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_');
}

static int str_ieq(const char *a, const char *b)
{
    while (*a && *b)
    {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b))
            return 0;
        a++;
        b++;
    }
    return (*a == '\0' && *b == '\0');
}

static int int_cmp(const void *ap, const void *bp)
{
    const int a = *(const int *)ap;
    const int b = *(const int *)bp;
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

static void manifest_init(Manifest *m)
{
    memset(m, 0, sizeof(*m));
    m->max_dbi = -1;
}

static void manifest_free(Manifest *m)
{
    size_t i;
    if (!m)
        return;
    for (i = 0; i < m->len; i++)
    {
        free(m->items[i].name);
        free(m->items[i].status);
    }
    free(m->items);
    memset(m, 0, sizeof(*m));
    m->max_dbi = -1;
}

static int manifest_push(Manifest *m, int dbi, const char *name, const char *status,
                         int line_no, const char *prefix)
{
    size_t i;
    ManifestEntry *next;
    for (i = 0; i < m->len; i++)
    {
        if (m->items[i].dbi == dbi)
            return failf(prefix, "line %d: duplicate dbi %d", line_no, dbi);
    }

    if (m->len == m->cap)
    {
        size_t new_cap = (m->cap == 0u) ? 8u : (m->cap * 2u);
        next = (ManifestEntry *)realloc(m->items, new_cap * sizeof(*next));
        if (!next)
            return failf(prefix, "out of memory");
        m->items = next;
        m->cap = new_cap;
    }

    m->items[m->len].dbi = dbi;
    m->items[m->len].name = xstrdup(name);
    m->items[m->len].status = xstrdup(status);
    m->items[m->len].line_no = line_no;
    if (!m->items[m->len].name || !m->items[m->len].status)
        return failf(prefix, "out of memory");
    m->len++;
    if (dbi > m->max_dbi)
        m->max_dbi = dbi;
    return 0;
}

static const ManifestEntry *manifest_find(const Manifest *m, int dbi)
{
    size_t i;
    for (i = 0; i < m->len; i++)
    {
        if (m->items[i].dbi == dbi)
            return &m->items[i];
    }
    return NULL;
}

static void macros_init(MacroTable *m) { memset(m, 0, sizeof(*m)); }

static void macros_free(MacroTable *m)
{
    size_t i;
    if (!m)
        return;
    for (i = 0; i < m->len; i++)
        free(m->items[i].name);
    free(m->items);
    memset(m, 0, sizeof(*m));
}

static int macros_lookup(const MacroTable *m, const char *name, int *dbi_out)
{
    size_t i;
    for (i = 0; i < m->len; i++)
    {
        if (strcmp(m->items[i].name, name) == 0)
        {
            if (dbi_out)
                *dbi_out = m->items[i].dbi;
            return 1;
        }
    }
    return 0;
}

static int macros_push(MacroTable *m, const char *name, int dbi, const char *prefix)
{
    size_t i;
    WitMacro *next;
    for (i = 0; i < m->len; i++)
    {
        if (strcmp(m->items[i].name, name) == 0)
            return failf(prefix, "duplicate macro %s", name);
    }

    if (m->len == m->cap)
    {
        size_t new_cap = (m->cap == 0u) ? 8u : (m->cap * 2u);
        next = (WitMacro *)realloc(m->items, new_cap * sizeof(*next));
        if (!next)
            return failf(prefix, "out of memory");
        m->items = next;
        m->cap = new_cap;
    }

    m->items[m->len].name = xstrdup(name);
    if (!m->items[m->len].name)
        return failf(prefix, "out of memory");
    m->items[m->len].dbi = dbi;
    m->len++;
    return 0;
}

static void intlist_init(IntList *list) { memset(list, 0, sizeof(*list)); }

static void intlist_free(IntList *list)
{
    if (!list)
        return;
    free(list->vals);
    memset(list, 0, sizeof(*list));
}

static int intlist_contains(const IntList *list, int v)
{
    size_t i;
    for (i = 0; i < list->len; i++)
    {
        if (list->vals[i] == v)
            return 1;
    }
    return 0;
}

static int intlist_add_unique(IntList *list, int v, const char *prefix)
{
    int *next;
    if (intlist_contains(list, v))
        return 0;
    if (list->len == list->cap)
    {
        size_t new_cap = (list->cap == 0u) ? 16u : (list->cap * 2u);
        next = (int *)realloc(list->vals, new_cap * sizeof(*next));
        if (!next)
            return failf(prefix, "out of memory");
        list->vals = next;
        list->cap = new_cap;
    }
    list->vals[list->len++] = v;
    return 0;
}

static int read_entire_file(const char *path, char **buf_out, size_t *len_out, const char *prefix)
{
    FILE *f = NULL;
    long sz = 0;
    size_t got = 0;
    char *buf = NULL;

    *buf_out = NULL;
    *len_out = 0u;

    f = fopen(path, "rb");
    if (!f)
        return failf(prefix, "failed to open %s: %s", path, strerror(errno));
    if (fseek(f, 0, SEEK_END) != 0)
    {
        fclose(f);
        return failf(prefix, "failed to seek %s", path);
    }
    sz = ftell(f);
    if (sz < 0)
    {
        fclose(f);
        return failf(prefix, "failed to size %s", path);
    }
    if (fseek(f, 0, SEEK_SET) != 0)
    {
        fclose(f);
        return failf(prefix, "failed to rewind %s", path);
    }

    buf = (char *)malloc((size_t)sz + 1u);
    if (!buf)
    {
        fclose(f);
        return failf(prefix, "out of memory");
    }
    if (sz > 0)
    {
        got = fread(buf, 1u, (size_t)sz, f);
        if (got != (size_t)sz)
        {
            free(buf);
            fclose(f);
            return failf(prefix, "failed to read %s", path);
        }
    }
    buf[(size_t)sz] = '\0';
    fclose(f);
    *buf_out = buf;
    *len_out = (size_t)sz;
    return 0;
}

static int parse_csv_fields(char *line, char **fields, size_t expected)
{
    size_t n = 1u;
    char *p;
    fields[0] = line;
    for (p = line; *p; p++)
    {
        if (*p == ',')
        {
            *p = '\0';
            if (n >= expected)
                return -1;
            fields[n++] = p + 1;
        }
    }
    return (n == expected) ? 0 : -1;
}

static int load_manifest_strict(const char *manifest_path, Manifest *out, const char *prefix)
{
    FILE *f = NULL;
    char line[4096];
    int line_no = 0;
    static const char *k_header = "dbi,name,key_format,value_format,owner,status";

    manifest_init(out);

    f = fopen(manifest_path, "rb");
    if (!f)
        return failf(prefix, "file not found: %s", manifest_path);

    while (fgets(line, (int)sizeof(line), f))
    {
        char *nl;
        char *row[6];
        char *dbi_field;
        char *name_field;
        char *status_field;
        char *end = NULL;
        long dbi_val;
        int i;

        line_no++;

        if (!strchr(line, '\n') && !feof(f))
        {
            fclose(f);
            manifest_free(out);
            return failf(prefix, "line %d too long", line_no);
        }
        nl = strchr(line, '\n');
        if (nl)
            *nl = '\0';
        nl = strchr(line, '\r');
        if (nl)
            *nl = '\0';

        if (line_no == 1)
        {
            if (strcmp(line, k_header) != 0)
            {
                fclose(f);
                manifest_free(out);
                return failf(prefix, "expected header %s, got %s", k_header, line);
            }
            continue;
        }

        if (parse_csv_fields(line, row, 6u) != 0)
        {
            fclose(f);
            manifest_free(out);
            return failf(prefix, "line %d: expected 6 CSV columns", line_no);
        }

        for (i = 0; i < 6; i++)
        {
            row[i] = trim_ws(row[i]);
            if (row[i][0] == '\0')
            {
                fclose(f);
                manifest_free(out);
                return failf(prefix, "line %d: empty column", line_no);
            }
        }

        dbi_field = row[0];
        name_field = row[1];
        status_field = row[5];

        errno = 0;
        dbi_val = strtol(dbi_field, &end, 10);
        if (errno != 0 || !end || *end != '\0')
        {
            fclose(f);
            manifest_free(out);
            return failf(prefix, "line %d: dbi is not an integer: %s", line_no, dbi_field);
        }
        if (dbi_val < 0 || dbi_val > INT_MAX)
        {
            fclose(f);
            manifest_free(out);
            return failf(prefix, "line %d: dbi must be >= 0", line_no);
        }

        if (manifest_push(out, (int)dbi_val, name_field, status_field, line_no, prefix) != 0)
        {
            fclose(f);
            manifest_free(out);
            return 1;
        }
    }
    fclose(f);

    if (out->len == 0u)
    {
        manifest_free(out);
        return failf(prefix, "manifest has no entries");
    }

    {
        size_t i;
        int *sorted = (int *)malloc(out->len * sizeof(*sorted));
        if (!sorted)
        {
            manifest_free(out);
            return failf(prefix, "out of memory");
        }
        for (i = 0; i < out->len; i++)
            sorted[i] = out->items[i].dbi;
        qsort(sorted, out->len, sizeof(*sorted), int_cmp);
        if (sorted[0] != 0)
        {
            free(sorted);
            manifest_free(out);
            return failf(prefix, "dbi 0 entry is required");
        }
        for (i = 1; i < out->len; i++)
        {
            if (sorted[i] != sorted[i - 1] + 1)
            {
                int prev = sorted[i - 1];
                int cur = sorted[i];
                free(sorted);
                manifest_free(out);
                return failf(prefix, "dbi sequence has a gap between %d and %d", prev, cur);
            }
        }
        out->max_dbi = sorted[out->len - 1u];
        free(sorted);
    }
    return 0;
}

static int load_wit_macros(const char *wit_header, MacroTable *macros, const char *prefix)
{
    FILE *f = NULL;
    char line[4096];
    int line_no = 0;

    macros_init(macros);
    f = fopen(wit_header, "rb");
    if (!f)
        return failf(prefix, "path not found: %s", wit_header);

    while (fgets(line, (int)sizeof(line), f))
    {
        char macro[256];
        char value[64];
        size_t mi = 0u;
        size_t vi = 0u;
        char *p = line;
        char *end = NULL;
        long dbi_val;
        size_t n;
        line_no++;

        while (*p && isspace((unsigned char)*p))
            p++;
        if (strncmp(p, "#define", 7) != 0 || !isspace((unsigned char)p[7]))
            continue;
        p += 7;
        while (*p && isspace((unsigned char)*p))
            p++;
        if (strncmp(p, "SAP_WIT_DBI_", 12) != 0)
            continue;

        while (*p && is_macro_token_char((unsigned char)*p))
        {
            if (mi + 1u >= sizeof(macro))
            {
                fclose(f);
                macros_free(macros);
                return failf(prefix, "%s:%d macro token too long", wit_header, line_no);
            }
            macro[mi++] = *p++;
        }
        macro[mi] = '\0';
        if (mi == 0u)
        {
            fclose(f);
            macros_free(macros);
            return failf(prefix, "%s:%d malformed macro line", wit_header, line_no);
        }

        while (*p && isspace((unsigned char)*p))
            p++;
        while (*p && !isspace((unsigned char)*p))
        {
            if (vi + 1u >= sizeof(value))
            {
                fclose(f);
                macros_free(macros);
                return failf(prefix, "%s:%d macro value token too long", wit_header, line_no);
            }
            value[vi++] = *p++;
        }
        value[vi] = '\0';
        if (vi == 0u)
        {
            fclose(f);
            macros_free(macros);
            return failf(prefix, "%s:%d missing macro value", wit_header, line_no);
        }

        n = strlen(value);
        if (n > 0u && (value[n - 1u] == 'u' || value[n - 1u] == 'U'))
            value[n - 1u] = '\0';
        if (value[0] == '\0')
        {
            fclose(f);
            macros_free(macros);
            return failf(prefix, "%s:%d invalid macro value", wit_header, line_no);
        }
        for (n = 0u; value[n]; n++)
        {
            if (!isdigit((unsigned char)value[n]))
            {
                fclose(f);
                macros_free(macros);
                return failf(prefix, "%s:%d non-numeric macro value %s", wit_header, line_no,
                             value);
            }
        }

        errno = 0;
        dbi_val = strtol(value, &end, 10);
        if (errno != 0 || !end || *end != '\0' || dbi_val < 0 || dbi_val > INT_MAX)
        {
            fclose(f);
            macros_free(macros);
            return failf(prefix, "%s:%d invalid macro value %s", wit_header, line_no, value);
        }
        if (macros_push(macros, macro, (int)dbi_val, prefix) != 0)
        {
            fclose(f);
            macros_free(macros);
            return 1;
        }
    }
    fclose(f);

    if (macros->len == 0u)
    {
        macros_free(macros);
        return failf(prefix, "no SAP_WIT_DBI_* macros found in %s", wit_header);
    }
    return 0;
}

static int collect_runtime_dbi_usage(const char *runner_dir, const MacroTable *macros,
                                     IntList *runtime_used, const char *prefix)
{
    DIR *dir = NULL;
    struct dirent *ent = NULL;
    int rc = 0;

    intlist_init(runtime_used);
    dir = opendir(runner_dir);
    if (!dir)
        return failf(prefix, "path not found: %s", runner_dir);

    while ((ent = readdir(dir)) != NULL)
    {
        char *path = NULL;
        char *buf = NULL;
        size_t len = 0u;
        size_t i;

        if (ent->d_name[0] == '.')
            continue;
        if (!has_suffix(ent->d_name, ".c") && !has_suffix(ent->d_name, ".h"))
            continue;

        path = path_join2(runner_dir, ent->d_name);
        if (!path)
        {
            rc = failf(prefix, "out of memory");
            break;
        }
        if (read_entire_file(path, &buf, &len, prefix) != 0)
        {
            free(path);
            rc = 1;
            break;
        }
        free(path);

        for (i = 0u; i + 12u <= len; i++)
        {
            size_t j;
            char token[256];
            int dbi;
            if (memcmp(buf + i, "SAP_WIT_DBI_", 12u) != 0)
                continue;
            if (i > 0u && is_word_char((unsigned char)buf[i - 1u]))
                continue;
            j = i + 12u;
            while (j < len && is_macro_token_char((unsigned char)buf[j]))
                j++;
            if (j < len && is_word_char((unsigned char)buf[j]))
                continue;
            if (j - i >= sizeof(token))
                continue;
            memcpy(token, buf + i, j - i);
            token[j - i] = '\0';
            if (macros_lookup(macros, token, &dbi))
            {
                if (intlist_add_unique(runtime_used, dbi, prefix) != 0)
                {
                    rc = 1;
                    break;
                }
            }
            i = j;
        }
        free(buf);
        if (rc != 0)
            break;
    }

    closedir(dir);
    if (rc != 0)
    {
        intlist_free(runtime_used);
        return 1;
    }
    return 0;
}

static int collect_runner_doc_dbi_usage(const char *docs_dir, IntList *doc_used,
                                        const char *prefix)
{
    DIR *dir = NULL;
    struct dirent *ent = NULL;
    int rc = 0;

    intlist_init(doc_used);
    dir = opendir(docs_dir);
    if (!dir)
        return failf(prefix, "path not found: %s", docs_dir);

    while ((ent = readdir(dir)) != NULL)
    {
        char *path = NULL;
        char *buf = NULL;
        size_t len = 0u;
        size_t i;

        if (ent->d_name[0] == '.')
            continue;
        if (!starts_with(ent->d_name, "RUNNER_") || !has_suffix(ent->d_name, ".md"))
            continue;

        path = path_join2(docs_dir, ent->d_name);
        if (!path)
        {
            rc = failf(prefix, "out of memory");
            break;
        }
        if (read_entire_file(path, &buf, &len, prefix) != 0)
        {
            free(path);
            rc = 1;
            break;
        }
        free(path);

        for (i = 0u; i + 3u <= len; i++)
        {
            size_t j;
            size_t k;
            long v;
            if (buf[i] != 'D' || buf[i + 1u] != 'B' || buf[i + 2u] != 'I')
                continue;
            if (i > 0u && is_word_char((unsigned char)buf[i - 1u]))
                continue;
            j = i + 3u;
            while (j < len && isspace((unsigned char)buf[j]))
                j++;
            if (j >= len || !isdigit((unsigned char)buf[j]))
                continue;
            v = 0;
            k = j;
            while (k < len && isdigit((unsigned char)buf[k]))
            {
                v = v * 10 + (long)(buf[k] - '0');
                if (v > INT_MAX)
                {
                    rc = failf(prefix, "%s contains DBI id > INT_MAX", ent->d_name);
                    break;
                }
                k++;
            }
            if (rc != 0)
                break;
            if (k < len && is_word_char((unsigned char)buf[k]))
                continue;
            if (intlist_add_unique(doc_used, (int)v, prefix) != 0)
            {
                rc = 1;
                break;
            }
            i = k;
        }

        free(buf);
        if (rc != 0)
            break;
    }

    closedir(dir);
    if (rc != 0)
    {
        intlist_free(doc_used);
        return 1;
    }
    return 0;
}

static int run_manifest_check(const char *manifest_path)
{
    Manifest manifest;
    int rc;

    rc = load_manifest_strict(manifest_path, &manifest, "dbi-manifest");
    if (rc != 0)
        return rc;
    printf("dbi-manifest: PASS (entries=%zu max_dbi=%d file=%s)\n", manifest.len,
           manifest.max_dbi, manifest_path);
    manifest_free(&manifest);
    return 0;
}

static int run_runner_status_check(const char *manifest_path, const char *wit_header,
                                   const char *repo_root)
{
    Manifest manifest;
    MacroTable macros;
    IntList runtime_used;
    IntList doc_used;
    IntList required_active;
    char *runner_dir = NULL;
    char *docs_dir = NULL;
    size_t i;
    int rc = 1;

    manifest_init(&manifest);
    macros_init(&macros);
    intlist_init(&runtime_used);
    intlist_init(&doc_used);
    intlist_init(&required_active);

    runner_dir = path_join2(repo_root, "src/runner");
    docs_dir = path_join2(repo_root, "docs");
    if (!runner_dir || !docs_dir)
    {
        failf("runner-dbi-status", "out of memory");
        goto out;
    }

    if (!path_exists(manifest_path))
    {
        failf("runner-dbi-status", "path not found: %s", manifest_path);
        goto out;
    }
    if (!path_exists(wit_header))
    {
        failf("runner-dbi-status", "path not found: %s", wit_header);
        goto out;
    }
    if (!path_exists(runner_dir))
    {
        failf("runner-dbi-status", "path not found: %s", runner_dir);
        goto out;
    }
    if (!path_exists(docs_dir))
    {
        failf("runner-dbi-status", "path not found: %s", docs_dir);
        goto out;
    }

    if (load_manifest_strict(manifest_path, &manifest, "runner-dbi-status") != 0)
        goto out;
    if (load_wit_macros(wit_header, &macros, "runner-dbi-status") != 0)
        goto out;
    if (collect_runtime_dbi_usage(runner_dir, &macros, &runtime_used, "runner-dbi-status") != 0)
        goto out;
    if (collect_runner_doc_dbi_usage(docs_dir, &doc_used, "runner-dbi-status") != 0)
        goto out;

    for (i = 0u; i < runtime_used.len; i++)
    {
        if (intlist_add_unique(&required_active, runtime_used.vals[i], "runner-dbi-status") != 0)
            goto out;
    }
    for (i = 0u; i < doc_used.len; i++)
    {
        if (intlist_add_unique(&required_active, doc_used.vals[i], "runner-dbi-status") != 0)
            goto out;
    }

    for (i = 0u; i < required_active.len; i++)
    {
        const ManifestEntry *row = manifest_find(&manifest, required_active.vals[i]);
        if (!row)
        {
            failf("runner-dbi-status",
                  "DBI %d is referenced by runner code/docs but missing in manifest",
                  required_active.vals[i]);
            goto out;
        }
        if (!str_ieq(row->status, "active"))
        {
            failf("runner-dbi-status",
                  "DBI %d (%s) is referenced by runner code/docs but has status='%s'; expected "
                  "'active'",
                  required_active.vals[i], row->name, row->status);
            goto out;
        }
    }

    for (i = 0u; i < macros.len; i++)
    {
        const ManifestEntry *row = manifest_find(&manifest, macros.items[i].dbi);
        const char *macro = macros.items[i].name;
        char macro_name[256];
        size_t j;

        if (!row)
        {
            failf("runner-dbi-status", "%s=%d missing from manifest", macro, macros.items[i].dbi);
            goto out;
        }

        if (!starts_with(macro, "SAP_WIT_DBI_"))
        {
            failf("runner-dbi-status", "internal error: invalid macro %s", macro);
            goto out;
        }

        if (strlen(macro) - 12u >= sizeof(macro_name))
        {
            failf("runner-dbi-status", "macro name too long: %s", macro);
            goto out;
        }
        for (j = 12u; macro[j] != '\0'; j++)
            macro_name[j - 12u] = (char)tolower((unsigned char)macro[j]);
        macro_name[j - 12u] = '\0';

        if (strcmp(macro_name, row->name) != 0)
        {
            failf("runner-dbi-status",
                  "%s name mismatch: macro implies '%s', manifest has '%s'",
                  macro, macro_name, row->name);
            goto out;
        }
    }

    printf("runner-dbi-status: PASS (runtime_dbis=%zu doc_dbis=%zu required_active=%zu)\n",
           runtime_used.len, doc_used.len, required_active.len);
    rc = 0;

out:
    free(runner_dir);
    free(docs_dir);
    manifest_free(&manifest);
    macros_free(&macros);
    intlist_free(&runtime_used);
    intlist_free(&doc_used);
    intlist_free(&required_active);
    return rc;
}

static int usage(const char *argv0)
{
    fprintf(stderr,
            "usage:\n"
            "  %s manifest <manifest.csv>\n"
            "  %s runner-status <manifest.csv> <generated_wit_schema_dbis.h> <repo_root>\n",
            argv0, argv0);
    return 1;
}

int main(int argc, char **argv)
{
    if (argc < 2)
        return usage(argv[0]);

    if (strcmp(argv[1], "manifest") == 0)
    {
        if (argc != 3)
            return failf("dbi-manifest", "usage: %s manifest <manifest.csv>", argv[0]);
        return run_manifest_check(argv[2]);
    }
    if (strcmp(argv[1], "runner-status") == 0)
    {
        if (argc != 5)
        {
            return failf("runner-dbi-status",
                         "usage: %s runner-status <manifest.csv> "
                         "<generated_wit_schema_dbis.h> <repo_root>",
                         argv[0]);
        }
        return run_runner_status_check(argv[2], argv[3], argv[4]);
    }

    return usage(argv[0]);
}

