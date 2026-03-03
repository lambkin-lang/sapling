/*
 * wit_codegen.c — WIT schema codegen in portable C11
 *
 * Replaces tools/wit_schema_codegen.py entirely.
 * Parses runtime-schema.wit via recursive descent and emits
 * Thatch-native C headers and source files.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <stdarg.h>

#define MAX_FIELDS     32
#define MAX_CASES      16
#define MAX_TYPES      64
#define MAX_NAME       64
#define MAX_TYPE_NODES 512

/* ------------------------------------------------------------------ */
/* Type expression AST                                                */
/* ------------------------------------------------------------------ */

typedef enum {
    TYPE_IDENT,   /* bare identifier: s64, worker-id, message-envelope */
    TYPE_OPTION,  /* option<T>                                         */
    TYPE_LIST,    /* list<T>                                           */
    TYPE_TUPLE,   /* tuple<T1, T2, ...>                                */
    TYPE_RESULT,  /* result<Ok, Err>                                   */
} WitTypeKind;

typedef struct {
    WitTypeKind kind;
    char        ident[MAX_NAME]; /* only for TYPE_IDENT */
    int         params[4];       /* indices into type pool (-1 = none) */
    int         param_count;
} WitTypeExpr;

static WitTypeExpr g_type_pool[MAX_TYPE_NODES];
static int         g_type_pool_count = 0;

static int type_alloc(void)
{
    if (g_type_pool_count >= MAX_TYPE_NODES) {
        fprintf(stderr, "wit_codegen: type pool exhausted\n");
        return -1;
    }
    int idx = g_type_pool_count++;
    memset(&g_type_pool[idx], 0, sizeof(WitTypeExpr));
    g_type_pool[idx].params[0] = -1;
    g_type_pool[idx].params[1] = -1;
    g_type_pool[idx].params[2] = -1;
    g_type_pool[idx].params[3] = -1;
    return idx;
}

/* Stringify a type expression (for diagnostics and debug). Returns bytes written. */
static int type_to_str(int idx, char *buf, int bufsize)
{
    if (idx < 0 || bufsize <= 0) return 0;
    WitTypeExpr *t = &g_type_pool[idx];
    int n = 0;

    if (t->kind == TYPE_IDENT) {
        n = snprintf(buf, bufsize, "%s", t->ident);
        return (n < bufsize) ? n : bufsize - 1;
    }

    const char *name = NULL;
    switch (t->kind) {
    case TYPE_IDENT:  name = "?";      break; /* handled above */
    case TYPE_OPTION: name = "option"; break;
    case TYPE_LIST:   name = "list";   break;
    case TYPE_TUPLE:  name = "tuple";  break;
    case TYPE_RESULT: name = "result"; break;
    }

    n = snprintf(buf, bufsize, "%s<", name);
    for (int i = 0; i < t->param_count; i++) {
        if (i > 0) n += snprintf(buf + n, bufsize - n, ", ");
        n += type_to_str(t->params[i], buf + n, bufsize - n);
    }
    n += snprintf(buf + n, bufsize - n, ">");
    return (n < bufsize) ? n : bufsize - 1;
}

static void codegen_die(const char *fmt, ...)
{
    va_list ap;
    fprintf(stderr, "wit_codegen: ERROR: ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
    exit(1);
}

static void codegen_die_type(const char *context, int type_idx)
{
    char typebuf[256];
    type_to_str(type_idx, typebuf, (int)sizeof(typebuf));
    codegen_die("%s: %s", context, typebuf);
}

static void build_result_access_paths(const char *access,
                                      char **is_ok_access_out,
                                      char **ok_access_out,
                                      char **err_access_out)
{
    const char *field;
    size_t prefix_len;
    size_t field_len;
    size_t is_ok_len;
    size_t ok_len;
    size_t err_len;
    char *is_ok_access;
    char *ok_access;
    char *err_access;

    if (!access || !is_ok_access_out || !ok_access_out || !err_access_out) {
        codegen_die("internal: invalid result access path inputs");
    }

    field = strrchr(access, '>');
    if (field) field += 1; /* skip "->" */
    else field = access;

    prefix_len = (size_t)(field - access);
    field_len = strlen(field);
    if (field_len == 0) {
        codegen_die("internal: empty field in result access path: %s", access);
    }

    is_ok_len = prefix_len + 3u + field_len + 3u + 1u;     /* prefix + is_ + field + _ok + NUL */
    ok_len = prefix_len + field_len + 9u + 1u;             /* prefix + field + _val.ok.v + NUL */
    err_len = prefix_len + field_len + 10u + 1u;           /* prefix + field + _val.err.v + NUL */

    is_ok_access = (char *)malloc(is_ok_len);
    ok_access = (char *)malloc(ok_len);
    err_access = (char *)malloc(err_len);
    if (!is_ok_access || !ok_access || !err_access) {
        free(is_ok_access);
        free(ok_access);
        free(err_access);
        codegen_die("out of memory while building result access paths");
    }

    memcpy(is_ok_access, access, prefix_len);
    memcpy(is_ok_access + prefix_len, "is_", 3u);
    memcpy(is_ok_access + prefix_len + 3u, field, field_len);
    memcpy(is_ok_access + prefix_len + 3u + field_len, "_ok", 3u);
    is_ok_access[is_ok_len - 1u] = '\0';

    memcpy(ok_access, access, prefix_len);
    memcpy(ok_access + prefix_len, field, field_len);
    memcpy(ok_access + prefix_len + field_len, "_val.ok.v", 9u);
    ok_access[ok_len - 1u] = '\0';

    memcpy(err_access, access, prefix_len);
    memcpy(err_access + prefix_len, field, field_len);
    memcpy(err_access + prefix_len + field_len, "_val.err.v", 10u);
    err_access[err_len - 1u] = '\0';

    *is_ok_access_out = is_ok_access;
    *ok_access_out = ok_access;
    *err_access_out = err_access;
}

/* ------------------------------------------------------------------ */
/* AST types                                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    char name[MAX_NAME];
    int  wit_type; /* index into g_type_pool */
} WitField;

typedef struct {
    char     name[MAX_NAME];
    WitField fields[MAX_FIELDS];
    int      field_count;
} WitRecord;

typedef struct {
    char name[MAX_NAME];
    int  payload_type; /* index into g_type_pool, or -1 */
} WitVariantCase;

typedef struct {
    char           name[MAX_NAME];
    WitVariantCase cases[MAX_CASES];
    int            case_count;
} WitVariant;

typedef struct {
    char name[MAX_NAME];
    char cases[MAX_CASES][MAX_NAME];
    int  case_count;
} WitEnum;

typedef struct {
    char name[MAX_NAME];
    char bits[MAX_CASES][MAX_NAME];
    int  bit_count;
} WitFlags;

typedef struct {
    char name[MAX_NAME];
    int  target; /* index into g_type_pool */
} WitAlias;

typedef struct {
    WitRecord  records[MAX_TYPES];
    int        record_count;
    WitVariant variants[MAX_TYPES];
    int        variant_count;
    WitEnum    enums[MAX_TYPES];
    int        enum_count;
    WitFlags   flags[MAX_TYPES];
    int        flags_count;
    WitAlias   aliases[MAX_TYPES];
    int        alias_count;
} WitRegistry;

/* ------------------------------------------------------------------ */
/* Scanner (lexical only — no bracket balancing)                      */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *src;
    int         pos;
    int         len;
    int         line;
    int         col;
} Scanner;

static void scanner_init(Scanner *s, const char *src, int len)
{
    s->src  = src;
    s->pos  = 0;
    s->len  = len;
    s->line = 1;
    s->col  = 1;
}

static int scanner_eof(const Scanner *s)
{
    return s->pos >= s->len;
}

static char scanner_peek(const Scanner *s)
{
    if (scanner_eof(s)) return '\0';
    return s->src[s->pos];
}

static char scanner_advance(Scanner *s)
{
    if (scanner_eof(s)) return '\0';
    char ch = s->src[s->pos++];
    if (ch == '\n') { s->line++; s->col = 1; }
    else            { s->col++; }
    return ch;
}

static void skip_whitespace(Scanner *s)
{
    while (!scanner_eof(s)) {
        char ch = scanner_peek(s);
        if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
            scanner_advance(s);
        } else if (ch == '/' && s->pos + 1 < s->len && s->src[s->pos + 1] == '/') {
            while (!scanner_eof(s) && scanner_peek(s) != '\n')
                scanner_advance(s);
        } else {
            break;
        }
    }
}

static int scan_ident(Scanner *s, char *buf, int bufsize)
{
    skip_whitespace(s);
    int i = 0;
    while (!scanner_eof(s) && i < bufsize - 1) {
        char ch = scanner_peek(s);
        if (isalnum((unsigned char)ch) || ch == '-' || ch == '_') {
            buf[i++] = scanner_advance(s);
        } else {
            break;
        }
    }
    buf[i] = '\0';
    return i;
}

static int expect_char(Scanner *s, char expected)
{
    skip_whitespace(s);
    if (scanner_peek(s) == expected) {
        scanner_advance(s);
        return 1;
    }
    fprintf(stderr, "wit_codegen: line %d col %d: expected '%c', got '%c'\n",
            s->line, s->col, expected, scanner_peek(s));
    return 0;
}

static int match_keyword(Scanner *s, const char *kw)
{
    skip_whitespace(s);
    int kwlen = (int)strlen(kw);
    if (s->pos + kwlen > s->len) return 0;
    if (memcmp(s->src + s->pos, kw, kwlen) != 0) return 0;
    if (s->pos + kwlen < s->len) {
        char next = s->src[s->pos + kwlen];
        if (isalnum((unsigned char)next) || next == '-' || next == '_')
            return 0;
    }
    for (int i = 0; i < kwlen; i++)
        scanner_advance(s);
    return 1;
}

/* ------------------------------------------------------------------ */
/* Recursive descent parser                                           */
/* ------------------------------------------------------------------ */

/*
 * type_expr = ident
 *           | ident '<' type_expr (',' type_expr)* '>'
 *
 * The recursion handles arbitrary nesting (option<list<u8>>) naturally
 * through the call stack — no bracket counting needed.
 */
static int parse_type_expr(Scanner *s)
{
    char name[MAX_NAME];
    if (!scan_ident(s, name, MAX_NAME)) return -1;

    skip_whitespace(s);
    if (scanner_peek(s) != '<') {
        /* bare identifier */
        int idx = type_alloc();
        if (idx < 0) return -1;
        g_type_pool[idx].kind = TYPE_IDENT;
        strncpy(g_type_pool[idx].ident, name, MAX_NAME - 1);
        return idx;
    }

    /* generic type: name<params...> */
    scanner_advance(s); /* consume '<' */

    WitTypeKind kind;
    if      (strcmp(name, "option") == 0) kind = TYPE_OPTION;
    else if (strcmp(name, "list")   == 0) kind = TYPE_LIST;
    else if (strcmp(name, "tuple")  == 0) kind = TYPE_TUPLE;
    else if (strcmp(name, "result") == 0) kind = TYPE_RESULT;
    else {
        fprintf(stderr, "wit_codegen: line %d: unknown generic '%s'\n",
                s->line, name);
        return -1;
    }

    int idx = type_alloc();
    if (idx < 0) return -1;
    g_type_pool[idx].kind = kind;

    /* parse comma-separated type parameters via recursion */
    while (g_type_pool[idx].param_count < 4) {
        int param = parse_type_expr(s);
        if (param < 0) return -1;
        g_type_pool[idx].params[g_type_pool[idx].param_count++] = param;

        skip_whitespace(s);
        if (scanner_peek(s) == ',') {
            scanner_advance(s);
            continue;
        }
        break;
    }

    if (!expect_char(s, '>')) return -1;
    return idx;
}

static int parse_record(Scanner *s, WitRecord *rec)
{
    if (!scan_ident(s, rec->name, MAX_NAME)) return 0;
    if (!expect_char(s, '{')) return 0;
    rec->field_count = 0;
    while (rec->field_count < MAX_FIELDS) {
        skip_whitespace(s);
        if (scanner_peek(s) == '}') { scanner_advance(s); return 1; }
        WitField *f = &rec->fields[rec->field_count];
        if (!scan_ident(s, f->name, MAX_NAME)) return 0;
        if (!expect_char(s, ':')) return 0;
        f->wit_type = parse_type_expr(s);
        if (f->wit_type < 0) return 0;
        expect_char(s, ',');
        rec->field_count++;
    }
    return expect_char(s, '}');
}

static int parse_variant(Scanner *s, WitVariant *var)
{
    if (!scan_ident(s, var->name, MAX_NAME)) return 0;
    if (!expect_char(s, '{')) return 0;
    var->case_count = 0;
    while (var->case_count < MAX_CASES) {
        skip_whitespace(s);
        if (scanner_peek(s) == '}') { scanner_advance(s); return 1; }
        WitVariantCase *c = &var->cases[var->case_count];
        if (!scan_ident(s, c->name, MAX_NAME)) return 0;
        c->payload_type = -1;
        skip_whitespace(s);
        if (scanner_peek(s) == '(') {
            scanner_advance(s);
            c->payload_type = parse_type_expr(s);
            if (c->payload_type < 0) return 0;
            if (!expect_char(s, ')')) return 0;
        }
        expect_char(s, ',');
        var->case_count++;
    }
    return expect_char(s, '}');
}

static int parse_enum(Scanner *s, WitEnum *en)
{
    if (!scan_ident(s, en->name, MAX_NAME)) return 0;
    if (!expect_char(s, '{')) return 0;
    en->case_count = 0;
    while (en->case_count < MAX_CASES) {
        skip_whitespace(s);
        if (scanner_peek(s) == '}') { scanner_advance(s); return 1; }
        if (!scan_ident(s, en->cases[en->case_count], MAX_NAME)) return 0;
        expect_char(s, ',');
        en->case_count++;
    }
    return expect_char(s, '}');
}

static int parse_flags(Scanner *s, WitFlags *fl)
{
    if (!scan_ident(s, fl->name, MAX_NAME)) return 0;
    if (!expect_char(s, '{')) return 0;
    fl->bit_count = 0;
    while (fl->bit_count < MAX_CASES) {
        skip_whitespace(s);
        if (scanner_peek(s) == '}') { scanner_advance(s); return 1; }
        if (!scan_ident(s, fl->bits[fl->bit_count], MAX_NAME)) return 0;
        expect_char(s, ',');
        fl->bit_count++;
    }
    return expect_char(s, '}');
}

static int parse_alias(Scanner *s, WitAlias *alias)
{
    if (!scan_ident(s, alias->name, MAX_NAME)) return 0;
    if (!expect_char(s, '=')) return 0;
    alias->target = parse_type_expr(s);
    if (alias->target < 0) return 0;
    expect_char(s, ';');
    return 1;
}

static int parse_wit(Scanner *s, WitRegistry *reg)
{
    memset(reg, 0, sizeof(*reg));

    while (!scanner_eof(s)) {
        skip_whitespace(s);
        if (scanner_eof(s)) break;

        if (match_keyword(s, "package") || match_keyword(s, "world") ||
            match_keyword(s, "interface") || match_keyword(s, "export")) {
            while (!scanner_eof(s) && scanner_peek(s) != '{' && scanner_peek(s) != ';')
                scanner_advance(s);
            if (scanner_peek(s) == ';') { scanner_advance(s); continue; }
            if (scanner_peek(s) == '{') { scanner_advance(s); continue; }
        } else if (match_keyword(s, "record")) {
            if (reg->record_count >= MAX_TYPES) {
                fprintf(stderr, "wit_codegen: too many records\n"); return 0;
            }
            if (!parse_record(s, &reg->records[reg->record_count])) return 0;
            reg->record_count++;
        } else if (match_keyword(s, "variant")) {
            if (reg->variant_count >= MAX_TYPES) {
                fprintf(stderr, "wit_codegen: too many variants\n"); return 0;
            }
            if (!parse_variant(s, &reg->variants[reg->variant_count])) return 0;
            reg->variant_count++;
        } else if (match_keyword(s, "enum")) {
            if (reg->enum_count >= MAX_TYPES) {
                fprintf(stderr, "wit_codegen: too many enums\n"); return 0;
            }
            if (!parse_enum(s, &reg->enums[reg->enum_count])) return 0;
            reg->enum_count++;
        } else if (match_keyword(s, "flags")) {
            if (reg->flags_count >= MAX_TYPES) {
                fprintf(stderr, "wit_codegen: too many flags\n"); return 0;
            }
            if (!parse_flags(s, &reg->flags[reg->flags_count])) return 0;
            reg->flags_count++;
        } else if (match_keyword(s, "type")) {
            if (reg->alias_count >= MAX_TYPES) {
                fprintf(stderr, "wit_codegen: too many aliases\n"); return 0;
            }
            if (!parse_alias(s, &reg->aliases[reg->alias_count])) return 0;
            reg->alias_count++;
        } else if (scanner_peek(s) == '}') {
            scanner_advance(s);
        } else {
            scanner_advance(s);
        }
    }
    return 1;
}

/* ------------------------------------------------------------------ */
/* Registry queries                                                   */
/* ------------------------------------------------------------------ */

static const WitAlias *find_alias(const WitRegistry *reg, const char *name)
{
    for (int i = 0; i < reg->alias_count; i++)
        if (strcmp(reg->aliases[i].name, name) == 0)
            return &reg->aliases[i];
    return NULL;
}

static const WitRecord *find_record(const WitRegistry *reg, const char *name)
{
    for (int i = 0; i < reg->record_count; i++)
        if (strcmp(reg->records[i].name, name) == 0)
            return &reg->records[i];
    return NULL;
}

static const WitVariant *find_variant(const WitRegistry *reg, const char *name)
{
    for (int i = 0; i < reg->variant_count; i++)
        if (strcmp(reg->variants[i].name, name) == 0)
            return &reg->variants[i];
    return NULL;
}

static const WitEnum *find_enum(const WitRegistry *reg, const char *name)
{
    for (int i = 0; i < reg->enum_count; i++)
        if (strcmp(reg->enums[i].name, name) == 0)
            return &reg->enums[i];
    return NULL;
}

static const WitFlags *find_flags(const WitRegistry *reg, const char *name)
{
    for (int i = 0; i < reg->flags_count; i++)
        if (strcmp(reg->flags[i].name, name) == 0)
            return &reg->flags[i];
    return NULL;
}

/* Resolve a type-expression index through aliases.
 * If the root is an ident that names an alias, follow the chain. */
static int resolve_type(const WitRegistry *reg, int type_idx)
{
    for (int depth = 0; depth < 16; depth++) {
        if (type_idx < 0) return type_idx;
        WitTypeExpr *t = &g_type_pool[type_idx];
        if (t->kind != TYPE_IDENT) return type_idx;
        const WitAlias *a = find_alias(reg, t->ident);
        if (!a) return type_idx;
        type_idx = a->target;
    }
    return type_idx;
}

/* Check if a type expression is a WIT primitive. */
static int is_primitive(const char *name)
{
    static const char *prims[] = {
        "s8","u8","s16","u16","s32","u32","s64","u64",
        "f32","f64","bool","char","string", NULL
    };
    for (const char **p = prims; *p; p++)
        if (strcmp(name, *p) == 0) return 1;
    return 0;
}

/* Check if a resolved type has fixed Thatch size (no skip pointer needed).
 * Used by writer emission to decide whether to emit skip pointers. */
/* Currently unused — all records get skip pointers for uniform skip.
 * Retained for potential future optimization of fixed-size records. */
__attribute__((unused))
static int is_fixed_size(const WitRegistry *reg, int type_idx)
{
    if (type_idx < 0) return 0;
    int resolved = resolve_type(reg, type_idx);
    if (resolved < 0) return 0;
    WitTypeExpr *t = &g_type_pool[resolved];

    switch (t->kind) {
    case TYPE_IDENT:
        if (strcmp(t->ident, "string") == 0) return 0;
        if (is_primitive(t->ident)) return 1;
        /* check named compound types */
        if (find_enum(reg, t->ident))  return 1;
        if (find_flags(reg, t->ident)) return 1;
        if (find_variant(reg, t->ident)) return 0;
        {
            const WitRecord *rec = find_record(reg, t->ident);
            if (rec) {
                for (int i = 0; i < rec->field_count; i++)
                    if (!is_fixed_size(reg, rec->fields[i].wit_type)) return 0;
                return 1;
            }
        }
        return 0;

    case TYPE_OPTION:
    case TYPE_LIST:
    case TYPE_RESULT:
        return 0;

    case TYPE_TUPLE:
        for (int i = 0; i < t->param_count; i++)
            if (!is_fixed_size(reg, t->params[i])) return 0;
        return 1;
    }
    return 0;
}

static int is_list_u8(const WitRegistry *reg, const WitTypeExpr *t)
{
    if (!reg || !t) return 0;
    if (t->kind != TYPE_LIST) return 0;
    if (t->param_count != 1 || t->params[0] < 0) return 0;
    int elem = resolve_type(reg, t->params[0]);
    if (elem < 0) return 0;
    WitTypeExpr *et = &g_type_pool[elem];
    return et->kind == TYPE_IDENT && strcmp(et->ident, "u8") == 0;
}

/* ------------------------------------------------------------------ */
/* Name conversion helpers                                            */
/* ------------------------------------------------------------------ */

/* kebab-case → snake_case: "message-envelope" → "message_envelope" */
static void kebab_to_snake(const char *in, char *out, int n)
{
    int i = 0;
    while (*in && i < n - 1) {
        out[i++] = (*in == '-') ? '_' : *in;
        in++;
    }
    out[i] = '\0';
}

/* kebab-case → UPPER_SNAKE: "message-kind" → "MESSAGE_KIND" */
static void kebab_to_upper(const char *in, char *out, int n)
{
    int i = 0;
    while (*in && i < n - 1) {
        char ch = (*in == '-') ? '_' : *in;
        out[i++] = (char)toupper((unsigned char)ch);
        in++;
    }
    out[i] = '\0';
}

/* kebab-case → CamelCase: "message-envelope" → "MessageEnvelope" */
static void kebab_to_camel(const char *in, char *out, int n)
{
    int i = 0;
    int cap = 1;
    while (*in && i < n - 1) {
        if (*in == '-') {
            cap = 1;
        } else {
            out[i++] = cap ? (char)toupper((unsigned char)*in) : *in;
            cap = 0;
        }
        in++;
    }
    out[i] = '\0';
}

/* ------------------------------------------------------------------ */
/* DBI entry extraction                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    int  dbi;
    char name[MAX_NAME];
    char key_rec[MAX_NAME];
    char val_rec[MAX_NAME];
} DbiEntry;

static int extract_dbis(const WitRegistry *reg, DbiEntry *out, int max)
{
    int count = 0;
    for (int i = 0; i < reg->record_count; i++) {
        const char *rn = reg->records[i].name;
        if (strncmp(rn, "dbi", 3) != 0) continue;

        const char *p = rn + 3;
        int dbi = 0;
        while (*p >= '0' && *p <= '9') { dbi = dbi * 10 + (*p - '0'); p++; }
        if (*p != '-') continue;
        p++;

        int len = (int)strlen(p);
        int is_key = -1;
        int name_len = 0;
        if (len > 4 && strcmp(p + len - 4, "-key") == 0) {
            is_key = 1; name_len = len - 4;
        } else if (len > 6 && strcmp(p + len - 6, "-value") == 0) {
            is_key = 0; name_len = len - 6;
        } else continue;

        int slot = -1;
        for (int j = 0; j < count; j++)
            if (out[j].dbi == dbi) { slot = j; break; }
        if (slot < 0) {
            if (count >= max) return -1;
            slot = count++;
            out[slot].dbi = dbi;
            memcpy(out[slot].name, p, name_len);
            out[slot].name[name_len] = '\0';
            out[slot].key_rec[0] = '\0';
            out[slot].val_rec[0] = '\0';
        }
        if (is_key) strncpy(out[slot].key_rec, rn, MAX_NAME - 1);
        else        strncpy(out[slot].val_rec, rn, MAX_NAME - 1);
    }
    /* Sort by DBI number */
    for (int i = 0; i < count - 1; i++)
        for (int j = i + 1; j < count; j++)
            if (out[i].dbi > out[j].dbi) {
                DbiEntry tmp = out[i]; out[i] = out[j]; out[j] = tmp;
            }
    return count;
}

/* ------------------------------------------------------------------ */
/* Topological sort for struct emission ordering                      */
/* ------------------------------------------------------------------ */

static void collect_struct_deps(const WitRegistry *reg, int type_idx,
                                const char **deps, int *ndeps, int max_deps)
{
    if (type_idx < 0) return;
    int resolved = resolve_type(reg, type_idx);
    if (resolved < 0) return;
    WitTypeExpr *t = &g_type_pool[resolved];

    switch (t->kind) {
    case TYPE_IDENT:
        if (find_record(reg, t->ident) || find_variant(reg, t->ident)) {
            for (int i = 0; i < *ndeps; i++)
                if (strcmp(deps[i], t->ident) == 0) return;
            if (*ndeps < max_deps) deps[(*ndeps)++] = t->ident;
        }
        break;
    case TYPE_OPTION:
    case TYPE_LIST:
        if (t->params[0] >= 0)
            collect_struct_deps(reg, t->params[0], deps, ndeps, max_deps);
        break;
    case TYPE_TUPLE:
    case TYPE_RESULT:
        for (int i = 0; i < t->param_count; i++)
            collect_struct_deps(reg, t->params[i], deps, ndeps, max_deps);
        break;
    }
}

static int topo_sort_types(const WitRegistry *reg, const char **order, int max_order)
{
    const char *names[MAX_TYPES * 2];
    const char *dep_buf[MAX_TYPES * 2][MAX_TYPES];
    int dep_cnt[MAX_TYPES * 2];
    int n = 0;

    for (int i = 0; i < reg->record_count; i++) {
        names[n] = reg->records[i].name;
        dep_cnt[n] = 0;
        for (int j = 0; j < reg->records[i].field_count; j++)
            collect_struct_deps(reg, reg->records[i].fields[j].wit_type,
                               dep_buf[n], &dep_cnt[n], MAX_TYPES);
        n++;
    }
    for (int i = 0; i < reg->variant_count; i++) {
        names[n] = reg->variants[i].name;
        dep_cnt[n] = 0;
        for (int j = 0; j < reg->variants[i].case_count; j++)
            if (reg->variants[i].cases[j].payload_type >= 0)
                collect_struct_deps(reg, reg->variants[i].cases[j].payload_type,
                                   dep_buf[n], &dep_cnt[n], MAX_TYPES);
        n++;
    }

    /* Kahn's algorithm */
    int in_deg[MAX_TYPES * 2];
    for (int i = 0; i < n; i++) {
        in_deg[i] = 0;
        for (int d = 0; d < dep_cnt[i]; d++)
            for (int j = 0; j < n; j++)
                if (strcmp(names[j], dep_buf[i][d]) == 0) { in_deg[i]++; break; }
    }

    int done[MAX_TYPES * 2] = {0};
    int count = 0;
    while (count < n) {
        int found = -1;
        for (int i = 0; i < n; i++)
            if (!done[i] && in_deg[i] == 0) { found = i; break; }
        if (found < 0) {
            fprintf(stderr, "wit_codegen: cycle in type dependencies\n");
            return -1;
        }
        done[found] = 1;
        if (count < max_order) order[count] = names[found];
        count++;
        for (int i = 0; i < n; i++) {
            if (done[i]) continue;
            for (int d = 0; d < dep_cnt[i]; d++)
                if (strcmp(dep_buf[i][d], names[found]) == 0) { in_deg[i]--; break; }
        }
    }
    return count;
}

/* ------------------------------------------------------------------ */
/* C type mapping                                                     */
/* ------------------------------------------------------------------ */

static const char *prim_c_type(const char *name)
{
    if (strcmp(name, "s8")   == 0) return "int8_t";
    if (strcmp(name, "u8")   == 0) return "uint8_t";
    if (strcmp(name, "s16")  == 0) return "int16_t";
    if (strcmp(name, "u16")  == 0) return "uint16_t";
    if (strcmp(name, "s32")  == 0) return "int32_t";
    if (strcmp(name, "u32")  == 0) return "uint32_t";
    if (strcmp(name, "s64")  == 0) return "int64_t";
    if (strcmp(name, "u64")  == 0) return "uint64_t";
    if (strcmp(name, "f32")  == 0) return "float";
    if (strcmp(name, "f64")  == 0) return "double";
    if (strcmp(name, "bool") == 0) return "uint8_t";
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Struct field emission                                              */
/* ------------------------------------------------------------------ */

static void emit_c_fields(FILE *out, const WitRegistry *reg,
                           int type_idx, const char *name, const char *indent)
{
    if (type_idx < 0) codegen_die("internal: negative type index in emit_c_fields");
    int resolved = resolve_type(reg, type_idx);
    if (resolved < 0) codegen_die("internal: unresolved type index %d in emit_c_fields", type_idx);
    WitTypeExpr *t = &g_type_pool[resolved];

    switch (t->kind) {
    case TYPE_IDENT: {
        if (strcmp(t->ident, "string") == 0) {
            fprintf(out, "%sconst uint8_t *%s_data;\n", indent, name);
            fprintf(out, "%suint32_t %s_len;\n", indent, name);
            return;
        }
        const char *ctype = prim_c_type(t->ident);
        if (ctype) {
            fprintf(out, "%s%s %s;\n", indent, ctype, name);
            return;
        }
        if (find_enum(reg, t->ident)) {
            fprintf(out, "%suint8_t %s;\n", indent, name);
            return;
        }
        if (find_flags(reg, t->ident)) {
            fprintf(out, "%suint32_t %s;\n", indent, name);
            return;
        }
        /* Named record or variant — by value */
        if (find_record(reg, t->ident) || find_variant(reg, t->ident)) {
            char camel[MAX_NAME];
            kebab_to_camel(t->ident, camel, MAX_NAME);
            fprintf(out, "%sSapWit%s %s;\n", indent, camel, name);
            return;
        }
        codegen_die("unsupported identifier in C field emission: %s", t->ident);
        return;
    }
    case TYPE_LIST:
        if (!is_list_u8(reg, t)) {
            codegen_die_type("unsupported list<T> (only list<u8> is supported)", resolved);
        }
        fprintf(out, "%sconst uint8_t *%s_data;\n", indent, name);
        fprintf(out, "%suint32_t %s_len;\n", indent, name);
        return;
    case TYPE_OPTION:
        if (t->param_count < 1 || t->params[0] < 0) {
            codegen_die_type("option<T> missing inner type", resolved);
        }
        fprintf(out, "%suint8_t has_%s;\n", indent, name);
        emit_c_fields(out, reg, t->params[0], name, indent);
        return;
    case TYPE_TUPLE:
        for (int i = 0; i < t->param_count; i++) {
            char sub[MAX_NAME];
            snprintf(sub, MAX_NAME, "%s_%d", name, i);
            emit_c_fields(out, reg, t->params[i], sub, indent);
        }
        return;
    case TYPE_RESULT: {
        fprintf(out, "%suint8_t is_%s_ok;\n", indent, name);
        int has_ok = (t->param_count > 0 && t->params[0] >= 0);
        int has_err = (t->param_count > 1 && t->params[1] >= 0);
        if (has_ok || has_err) {
            fprintf(out, "%sunion {\n", indent);
            if (has_ok) {
                char deeper[64];
                snprintf(deeper, sizeof(deeper), "%s    ", indent);
                fprintf(out, "%s    struct {\n", indent);
                emit_c_fields(out, reg, t->params[0], "v", deeper);
                fprintf(out, "%s    } ok;\n", indent);
            }
            if (has_err) {
                char deeper[64];
                snprintf(deeper, sizeof(deeper), "%s    ", indent);
                fprintf(out, "%s    struct {\n", indent);
                emit_c_fields(out, reg, t->params[1], "v", deeper);
                fprintf(out, "%s    } err;\n", indent);
            }
            fprintf(out, "%s} %s_val;\n", indent, name);
        }
        return;
    }
    }
    codegen_die("internal: unhandled WitTypeKind in emit_c_fields");
}

/* Emit a single variant union member for a case payload. */
static void emit_variant_payload(FILE *out, const WitRegistry *reg,
                                  int type_idx, const char *case_name)
{
    if (type_idx < 0) codegen_die("internal: negative type index in emit_variant_payload");
    int resolved = resolve_type(reg, type_idx);
    if (resolved < 0) codegen_die("internal: unresolved type index %d in emit_variant_payload", type_idx);
    WitTypeExpr *t = &g_type_pool[resolved];

    if (t->kind == TYPE_IDENT) {
        if (strcmp(t->ident, "string") == 0) {
            fprintf(out, "        struct { const uint8_t *data; uint32_t len; } %s;\n", case_name);
            return;
        }
        const char *ctype = prim_c_type(t->ident);
        if (ctype) {
            fprintf(out, "        %s %s;\n", ctype, case_name);
            return;
        }
        if (find_record(reg, t->ident) || find_variant(reg, t->ident)) {
            char camel[MAX_NAME];
            kebab_to_camel(t->ident, camel, MAX_NAME);
            fprintf(out, "        SapWit%s %s;\n", camel, case_name);
            return;
        }
        if (find_enum(reg, t->ident)) {
            fprintf(out, "        uint8_t %s;\n", case_name);
            return;
        }
        if (find_flags(reg, t->ident)) {
            fprintf(out, "        uint32_t %s;\n", case_name);
            return;
        }
    }
    if (t->kind == TYPE_LIST) {
        if (!is_list_u8(reg, t)) {
            codegen_die_type("unsupported list<T> variant payload (only list<u8> is supported)", resolved);
        }
        fprintf(out, "        struct { const uint8_t *data; uint32_t len; } %s;\n", case_name);
        return;
    }
    if (t->kind == TYPE_OPTION || t->kind == TYPE_TUPLE || t->kind == TYPE_RESULT) {
        /* Complex nested type — wrap in a struct and use emit_c_fields */
        fprintf(out, "        struct {\n");
        emit_c_fields(out, reg, type_idx, "v", "            ");
        fprintf(out, "        } %s;\n", case_name);
        return;
    }
    codegen_die_type("unsupported variant payload type", resolved);
}

/* ------------------------------------------------------------------ */
/* Header emission                                                    */
/* ------------------------------------------------------------------ */

static void emit_header(FILE *out, const WitRegistry *reg,
                        const DbiEntry *dbis, int ndbi,
                        const char *header_path)
{
    char upper[MAX_NAME], camel[MAX_NAME], snake[MAX_NAME];

    /* Derive include guard from header filename (basename, uppercased). */
    char guard[MAX_NAME];
    const char *base = header_path;
    const char *p;
    for (p = header_path; *p; p++) {
        if (*p == '/' || *p == '\\') base = p + 1;
    }
    size_t gi = 0;
    for (p = base; *p && gi < MAX_NAME - 1; p++) {
        if ((*p >= 'a' && *p <= 'z')) guard[gi++] = (char)(*p - 'a' + 'A');
        else if ((*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9')) guard[gi++] = *p;
        else guard[gi++] = '_';
    }
    guard[gi] = '\0';

    fprintf(out, "/* Auto-generated by tools/wit_codegen; DO NOT EDIT. */\n");
    fprintf(out, "#ifndef %s\n", guard);
    fprintf(out, "#define %s\n\n", guard);
    fprintf(out, "#include <stdint.h>\n");
    fprintf(out, "#include <stddef.h>\n");
    fprintf(out, "#include \"sapling/thatch.h\"\n");
    fprintf(out, "#include \"sapling/err.h\"\n\n");

    /* --- Wire version --- */
    fprintf(out, "/* Wire format version — bump on any wire format change. */\n");
    fprintf(out, "#define SAP_WIT_WIRE_VERSION 1u\n\n");

    /* --- Wire format documentation --- */
    fprintf(out, "/*\n");
    fprintf(out, " * Thatch WIT wire format (version 1):\n");
    fprintf(out, " *\n");
    fprintf(out, " *   record:      [TAG_RECORD  0x10][skip_len: 4 LE][...fields...]\n");
    fprintf(out, " *   variant:     [TAG_VARIANT 0x11][skip_len: 4 LE][case_tag: 1][...payload...]\n");
    fprintf(out, " *   enum:        [TAG_ENUM    0x12][value: 1]\n");
    fprintf(out, " *   flags:       [TAG_FLAGS   0x13][bits: 4 LE]\n");
    fprintf(out, " *   option none: [TAG_OPTION_NONE 0x14]\n");
    fprintf(out, " *   option some: [TAG_OPTION_SOME 0x15][...inner...]\n");
    fprintf(out, " *   tuple:       [TAG_TUPLE   0x16][skip_len: 4 LE][...elements...]\n");
    fprintf(out, " *   list<u8>:    [TAG_BYTES   0x2C][len: 4 LE][data: len bytes]\n");
    fprintf(out, " *   list<T!=u8>: currently unsupported by this codegen\n");
    fprintf(out, " *   result ok:   [TAG_RESULT_OK  0x18][...ok value...]\n");
    fprintf(out, " *   result err:  [TAG_RESULT_ERR 0x19][...err value...]\n");
    fprintf(out, " *   sN/uN:       [TAG_SN/UN  0x20-0x27][data: N/8 bytes LE]\n");
    fprintf(out, " *   fN:          [TAG_FN     0x28-0x29][data: N/8 bytes LE]\n");
    fprintf(out, " *   bool:        [TAG_BOOL_FALSE 0x2A] or [TAG_BOOL_TRUE 0x2B]\n");
    fprintf(out, " *   bytes:       [TAG_BYTES  0x2C][len: 4 LE][data: len bytes]\n");
    fprintf(out, " *   string:      [TAG_STRING 0x2D][len: 4 LE][data: len bytes]\n");
    fprintf(out, " *\n");
    fprintf(out, " *   skip_len: byte count of everything after the skip_len field\n");
    fprintf(out, " *             itself, up to (but not including) the next sibling.\n");
    fprintf(out, " */\n\n");

    /* --- Thatch WIT tags --- */
    fprintf(out, "/* Thatch WIT tags (0x10+, coexists with JSON 0x01-0x09) */\n");
    fprintf(out, "#define SAP_WIT_TAG_RECORD       0x10\n");
    fprintf(out, "#define SAP_WIT_TAG_VARIANT      0x11\n");
    fprintf(out, "#define SAP_WIT_TAG_ENUM         0x12\n");
    fprintf(out, "#define SAP_WIT_TAG_FLAGS        0x13\n");
    fprintf(out, "#define SAP_WIT_TAG_OPTION_NONE  0x14\n");
    fprintf(out, "#define SAP_WIT_TAG_OPTION_SOME  0x15\n");
    fprintf(out, "#define SAP_WIT_TAG_TUPLE        0x16\n");
    fprintf(out, "#define SAP_WIT_TAG_LIST         0x17\n");
    fprintf(out, "#define SAP_WIT_TAG_RESULT_OK    0x18\n");
    fprintf(out, "#define SAP_WIT_TAG_RESULT_ERR   0x19\n\n");

    fprintf(out, "#define SAP_WIT_TAG_S8           0x20\n");
    fprintf(out, "#define SAP_WIT_TAG_U8           0x21\n");
    fprintf(out, "#define SAP_WIT_TAG_S16          0x22\n");
    fprintf(out, "#define SAP_WIT_TAG_U16          0x23\n");
    fprintf(out, "#define SAP_WIT_TAG_S32          0x24\n");
    fprintf(out, "#define SAP_WIT_TAG_U32          0x25\n");
    fprintf(out, "#define SAP_WIT_TAG_S64          0x26\n");
    fprintf(out, "#define SAP_WIT_TAG_U64          0x27\n");
    fprintf(out, "#define SAP_WIT_TAG_F32          0x28\n");
    fprintf(out, "#define SAP_WIT_TAG_F64          0x29\n");
    fprintf(out, "#define SAP_WIT_TAG_BOOL_FALSE   0x2A\n");
    fprintf(out, "#define SAP_WIT_TAG_BOOL_TRUE    0x2B\n");
    fprintf(out, "#define SAP_WIT_TAG_BYTES        0x2C\n");
    fprintf(out, "#define SAP_WIT_TAG_STRING       0x2D\n\n");

    /* --- DBI index constants --- */
    for (int i = 0; i < ndbi; i++) {
        kebab_to_upper(dbis[i].name, upper, MAX_NAME);
        fprintf(out, "#define SAP_WIT_DBI_%s %du\n", upper, dbis[i].dbi);
    }
    fprintf(out, "\n");

    /* --- DBI schema metadata type --- */
    if (ndbi > 0) {
        fprintf(out, "typedef struct {\n");
        fprintf(out, "    uint32_t dbi;\n");
        fprintf(out, "    const char *name;\n");
        fprintf(out, "    const char *key_wit_record;\n");
        fprintf(out, "    const char *value_wit_record;\n");
        fprintf(out, "} SapWitDbiSchema;\n\n");
    }

    /* --- Enum case constants --- */
    for (int i = 0; i < reg->enum_count; i++) {
        const WitEnum *en = &reg->enums[i];
        kebab_to_upper(en->name, upper, MAX_NAME);
        for (int j = 0; j < en->case_count; j++) {
            char cu[MAX_NAME];
            kebab_to_upper(en->cases[j], cu, MAX_NAME);
            fprintf(out, "#define SAP_WIT_%s_%s %d\n", upper, cu, j);
        }
        fprintf(out, "\n");
    }

    /* --- Flags bit constants --- */
    for (int i = 0; i < reg->flags_count; i++) {
        const WitFlags *fl = &reg->flags[i];
        kebab_to_upper(fl->name, upper, MAX_NAME);
        for (int j = 0; j < fl->bit_count; j++) {
            char bu[MAX_NAME];
            kebab_to_upper(fl->bits[j], bu, MAX_NAME);
            fprintf(out, "#define SAP_WIT_%s_%s (1u << %d)\n", upper, bu, j);
        }
        fprintf(out, "\n");
    }

    /* --- Variant case tag constants --- */
    for (int i = 0; i < reg->variant_count; i++) {
        const WitVariant *var = &reg->variants[i];
        kebab_to_upper(var->name, upper, MAX_NAME);
        for (int j = 0; j < var->case_count; j++) {
            char cu[MAX_NAME];
            kebab_to_upper(var->cases[j].name, cu, MAX_NAME);
            fprintf(out, "#define SAP_WIT_%s_%s %d\n", upper, cu, j);
        }
        fprintf(out, "\n");
    }

    /* --- Struct typedefs in topological order --- */
    const char *order[MAX_TYPES * 2];
    int norder = topo_sort_types(reg, order, MAX_TYPES * 2);
    if (norder < 0) return;

    for (int idx = 0; idx < norder; idx++) {
        const char *tname = order[idx];
        const WitRecord *rec = find_record(reg, tname);
        if (rec) {
            kebab_to_camel(tname, camel, MAX_NAME);
            fprintf(out, "typedef struct {\n");
            for (int j = 0; j < rec->field_count; j++) {
                char fname[MAX_NAME];
                kebab_to_snake(rec->fields[j].name, fname, MAX_NAME);
                emit_c_fields(out, reg, rec->fields[j].wit_type, fname, "    ");
            }
            fprintf(out, "} SapWit%s;\n\n", camel);
            continue;
        }
        const WitVariant *var = find_variant(reg, tname);
        if (var) {
            kebab_to_camel(tname, camel, MAX_NAME);
            fprintf(out, "typedef struct {\n");
            fprintf(out, "    uint8_t case_tag;\n");
            int has_payload = 0;
            for (int j = 0; j < var->case_count; j++)
                if (var->cases[j].payload_type >= 0) { has_payload = 1; break; }
            if (has_payload) {
                fprintf(out, "    union {\n");
                for (int j = 0; j < var->case_count; j++) {
                    if (var->cases[j].payload_type < 0) continue;
                    char cname[MAX_NAME];
                    kebab_to_snake(var->cases[j].name, cname, MAX_NAME);
                    emit_variant_payload(out, reg, var->cases[j].payload_type, cname);
                }
                fprintf(out, "    } val;\n");
            }
            fprintf(out, "} SapWit%s;\n\n", camel);
            continue;
        }
    }

    /* --- Writer declarations --- */
    fprintf(out, "/* Writer functions */\n");
    for (int idx = 0; idx < norder; idx++) {
        kebab_to_camel(order[idx], camel, MAX_NAME);
        kebab_to_snake(order[idx], snake, MAX_NAME);
        fprintf(out, "int sap_wit_write_%s(ThatchRegion *region, const SapWit%s *val);\n",
                snake, camel);
    }
    fprintf(out, "\n");

    /* --- Reader declarations --- */
    fprintf(out, "/* Reader functions */\n");
    for (int idx = 0; idx < norder; idx++) {
        kebab_to_camel(order[idx], camel, MAX_NAME);
        kebab_to_snake(order[idx], snake, MAX_NAME);
        fprintf(out, "int sap_wit_read_%s(const ThatchRegion *region, ThatchCursor *cursor, SapWit%s *out);\n",
                snake, camel);
    }
    fprintf(out, "\n");

    /* --- Skip declaration --- */
    fprintf(out, "/* Universal skip */\n");
    fprintf(out, "int sap_wit_skip_value(const ThatchRegion *region, ThatchCursor *cursor);\n\n");

    /* --- DBI blob validators (extern, full structural validation) --- */
    fprintf(out, "/* DBI blob validators */\n");
    for (int i = 0; i < ndbi; i++) {
        kebab_to_snake(dbis[i].val_rec, snake, MAX_NAME);
        fprintf(out, "int sap_wit_validate_%s(const void *data, uint32_t len);\n", snake);
    }
    fprintf(out, "\n");

    /* --- Extern declarations --- */
    if (ndbi > 0) {
        fprintf(out, "extern const SapWitDbiSchema sap_wit_dbi_schema[];\n");
        fprintf(out, "extern const uint32_t sap_wit_dbi_schema_count;\n\n");
    }
    fprintf(out, "#endif /* %s */\n", guard);
}

/* ------------------------------------------------------------------ */
/* Writer emission helpers                                            */
/* ------------------------------------------------------------------ */

/*
 * emit_write_type_expr — recursively emit thatch_write_* calls for a
 * type expression.  `access` is the C expression to read the value from
 * (e.g., "val->payload" or "val->kind").
 *
 * For blob types (string/bytes/list<u8>) the access is the base name
 * and we append sep+"data" / sep+"len".  For record fields sep="_"
 * (flat fields like val->X_data); for variant payloads sep="."
 * (struct members like val->val.X.data).
 */
static void emit_write_type_expr(FILE *out, const WitRegistry *reg,
                                 int type_idx, const char *access,
                                 const char *sep, const char *indent)
{
    if (type_idx < 0) codegen_die("internal: negative type index in emit_write_type_expr");
    int resolved = resolve_type(reg, type_idx);
    if (resolved < 0) codegen_die("internal: unresolved type index %d in emit_write_type_expr", type_idx);
    WitTypeExpr *t = &g_type_pool[resolved];

    switch (t->kind) {
    case TYPE_IDENT: {
        /* string / bytes -> TAG_STRING + len + data */
        if (strcmp(t->ident, "string") == 0) {
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_STRING));\n", indent);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, &%s%slen, 4));\n", indent, access, sep);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, %s%sdata, %s%slen));\n", indent, access, sep, access, sep);
            return;
        }
        /* bool -> TAG_BOOL_TRUE or TAG_BOOL_FALSE (no payload) */
        if (strcmp(t->ident, "bool") == 0) {
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, %s ? SAP_WIT_TAG_BOOL_TRUE : SAP_WIT_TAG_BOOL_FALSE));\n", indent, access);
            return;
        }
        /* numeric primitives: tag + raw data */
        const char *ctype = prim_c_type(t->ident);
        if (ctype) {
            const char *tag = NULL;
            int size = 0;
            if      (strcmp(t->ident, "s8")  == 0) { tag = "SAP_WIT_TAG_S8";  size = 1; }
            else if (strcmp(t->ident, "u8")  == 0) { tag = "SAP_WIT_TAG_U8";  size = 1; }
            else if (strcmp(t->ident, "s16") == 0) { tag = "SAP_WIT_TAG_S16"; size = 2; }
            else if (strcmp(t->ident, "u16") == 0) { tag = "SAP_WIT_TAG_U16"; size = 2; }
            else if (strcmp(t->ident, "s32") == 0) { tag = "SAP_WIT_TAG_S32"; size = 4; }
            else if (strcmp(t->ident, "u32") == 0) { tag = "SAP_WIT_TAG_U32"; size = 4; }
            else if (strcmp(t->ident, "s64") == 0) { tag = "SAP_WIT_TAG_S64"; size = 8; }
            else if (strcmp(t->ident, "u64") == 0) { tag = "SAP_WIT_TAG_U64"; size = 8; }
            else if (strcmp(t->ident, "f32") == 0) { tag = "SAP_WIT_TAG_F32"; size = 4; }
            else if (strcmp(t->ident, "f64") == 0) { tag = "SAP_WIT_TAG_F64"; size = 8; }
            if (tag) {
                fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, %s));\n", indent, tag);
                fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, &%s, %d));\n", indent, access, size);
            }
            return;
        }
        /* enum -> TAG_ENUM + 1 byte */
        if (find_enum(reg, t->ident)) {
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_ENUM));\n", indent);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, &%s, 1));\n", indent, access);
            return;
        }
        /* flags -> TAG_FLAGS + 4 bytes */
        if (find_flags(reg, t->ident)) {
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_FLAGS));\n", indent);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, &%s, 4));\n", indent, access);
            return;
        }
        /* named record or variant — delegate to its writer */
        if (find_record(reg, t->ident) || find_variant(reg, t->ident)) {
            char snake[MAX_NAME];
            kebab_to_snake(t->ident, snake, MAX_NAME);
            fprintf(out, "%sSAP_WIT_CHECK(sap_wit_write_%s(region, &%s));\n", indent, snake, access);
            return;
        }
        codegen_die("unsupported identifier in writer emission: %s", t->ident);
        return;
    }
    case TYPE_LIST:
        if (!is_list_u8(reg, t)) {
            codegen_die_type("unsupported list<T> writer (only list<u8> is supported)", resolved);
        }
        /* list<u8> same as bytes: TAG_BYTES + len + data */
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_BYTES));\n", indent);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, &%s%slen, 4));\n", indent, access, sep);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_data(region, %s%sdata, %s%slen));\n", indent, access, sep, access, sep);
        return;
    case TYPE_OPTION: {
        if (t->param_count < 1 || t->params[0] < 0) {
            codegen_die_type("option<T> writer missing inner type", resolved);
        }
        /* When called from emit_write_record, options are handled inline there.
         * This path handles option<T> in nested positions (e.g. inside tuples). */
        char inner_indent[64];
        snprintf(inner_indent, sizeof(inner_indent), "%s    ", indent);
        fprintf(out, "%sif (%s) {\n", indent, access);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_OPTION_SOME));\n", inner_indent);
        emit_write_type_expr(out, reg, t->params[0], access, sep, inner_indent);
        fprintf(out, "%s} else {\n", indent);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_OPTION_NONE));\n", inner_indent);
        fprintf(out, "%s}\n", indent);
        return;
    }
    case TYPE_TUPLE: {
        char inner[64];
        snprintf(inner, sizeof(inner), "%s    ", indent);
        fprintf(out, "%s{\n", indent);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_TUPLE));\n", inner);
        fprintf(out, "%suint32_t _tuple_skip_loc;\n", inner);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_reserve_skip(region, &_tuple_skip_loc));\n", inner);
        for (int i = 0; i < t->param_count; i++) {
            char sub[256];
            snprintf(sub, sizeof(sub), "%s_%d", access, i);
            emit_write_type_expr(out, reg, t->params[i], sub, "_", inner);
        }
        fprintf(out, "%sSAP_WIT_CHECK(thatch_commit_skip(region, _tuple_skip_loc));\n", inner);
        fprintf(out, "%s}\n", indent);
        return;
    }
    case TYPE_RESULT: {
        char *is_ok_access = NULL;
        char *ok_access = NULL;
        char *err_access = NULL;
        build_result_access_paths(access, &is_ok_access, &ok_access, &err_access);

        char inner[64];
        snprintf(inner, sizeof(inner), "%s    ", indent);
        fprintf(out, "%sif (%s) {\n", indent, is_ok_access);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_RESULT_OK));\n", inner);
        if (t->param_count > 0 && t->params[0] >= 0)
            emit_write_type_expr(out, reg, t->params[0], ok_access, ".", inner);
        fprintf(out, "%s} else {\n", indent);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_RESULT_ERR));\n", inner);
        if (t->param_count > 1 && t->params[1] >= 0)
            emit_write_type_expr(out, reg, t->params[1], err_access, ".", inner);
        fprintf(out, "%s}\n", indent);
        free(is_ok_access);
        free(ok_access);
        free(err_access);
        return;
    }
    }
    codegen_die("internal: unhandled WitTypeKind in emit_write_type_expr");
}

/* Emit a complete writer function for a record. */
static void emit_write_record(FILE *out, const WitRegistry *reg,
                               const WitRecord *rec)
{
    char snake[MAX_NAME], camel[MAX_NAME];
    kebab_to_snake(rec->name, snake, MAX_NAME);
    kebab_to_camel(rec->name, camel, MAX_NAME);

    fprintf(out, "int sap_wit_write_%s(ThatchRegion *region, const SapWit%s *val)\n{\n",
            snake, camel);

    /* All records get skip pointers so sap_wit_skip_value works uniformly. */
    fprintf(out, "    SAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_RECORD));\n");
    fprintf(out, "    ThatchCursor skip_loc;\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_reserve_skip(region, &skip_loc));\n");

    for (int i = 0; i < rec->field_count; i++) {
        char fname[MAX_NAME], access[256];
        kebab_to_snake(rec->fields[i].name, fname, MAX_NAME);

        /* For option fields, the guard is has_X and we pass that as the condition */
        int res = resolve_type(reg, rec->fields[i].wit_type);
        WitTypeExpr *ft = (res >= 0) ? &g_type_pool[res] : NULL;

        if (ft && ft->kind == TYPE_OPTION) {
            snprintf(access, sizeof(access), "val->has_%s", fname);
            fprintf(out, "    if (%s) {\n", access);
            fprintf(out, "        SAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_OPTION_SOME));\n");
            /* Write the inner value */
            char inner_access[256];
            snprintf(inner_access, sizeof(inner_access), "val->%s", fname);
            emit_write_type_expr(out, reg, ft->params[0], inner_access, "_", "        ");
            fprintf(out, "    } else {\n");
            fprintf(out, "        SAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_OPTION_NONE));\n");
            fprintf(out, "    }\n");
        } else {
            snprintf(access, sizeof(access), "val->%s", fname);
            emit_write_type_expr(out, reg, rec->fields[i].wit_type, access, "_", "    ");
        }
    }

    fprintf(out, "    SAP_WIT_CHECK(thatch_commit_skip(region, skip_loc));\n");
    fprintf(out, "    return ERR_OK;\n}\n\n");
}

/* Emit a complete writer function for a variant. */
static void emit_write_variant(FILE *out, const WitRegistry *reg,
                                const WitVariant *var)
{
    char snake[MAX_NAME], camel[MAX_NAME], upper_var[MAX_NAME];
    kebab_to_snake(var->name, snake, MAX_NAME);
    kebab_to_camel(var->name, camel, MAX_NAME);
    kebab_to_upper(var->name, upper_var, MAX_NAME);

    fprintf(out, "int sap_wit_write_%s(ThatchRegion *region, const SapWit%s *val)\n{\n",
            snake, camel);
    fprintf(out, "    SAP_WIT_CHECK(thatch_write_tag(region, SAP_WIT_TAG_VARIANT));\n");
    fprintf(out, "    ThatchCursor skip_loc;\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_reserve_skip(region, &skip_loc));\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_write_data(region, &val->case_tag, 1));\n");
    fprintf(out, "    switch (val->case_tag) {\n");

    for (int j = 0; j < var->case_count; j++) {
        char cu[MAX_NAME], cs[MAX_NAME];
        kebab_to_upper(var->cases[j].name, cu, MAX_NAME);
        kebab_to_snake(var->cases[j].name, cs, MAX_NAME);
        fprintf(out, "    case SAP_WIT_%s_%s:\n", upper_var, cu);
        if (var->cases[j].payload_type >= 0) {
            char access[256];
            snprintf(access, sizeof(access), "val->val.%s", cs);
            emit_write_type_expr(out, reg, var->cases[j].payload_type, access, ".", "        ");
        }
        fprintf(out, "        break;\n");
    }

    fprintf(out, "    default: return ERR_TYPE;\n");
    fprintf(out, "    }\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_commit_skip(region, skip_loc));\n");
    fprintf(out, "    return ERR_OK;\n}\n\n");
}

/* ------------------------------------------------------------------ */
/* Reader emission helpers                                            */
/* ------------------------------------------------------------------ */

/*
 * emit_read_type_expr — recursively emit thatch_read_* calls for a
 * type expression.  `access` is the C lvalue to write the value to
 * (e.g., "out->payload").  `sep` works like in writers: "_" for record
 * fields (out->X_data), "." for variant payloads (out->val.X.data).
 */
static void emit_read_type_expr(FILE *out, const WitRegistry *reg,
                                int type_idx, const char *access,
                                const char *sep, const char *indent)
{
    if (type_idx < 0) codegen_die("internal: negative type index in emit_read_type_expr");
    int resolved = resolve_type(reg, type_idx);
    if (resolved < 0) codegen_die("internal: unresolved type index %d in emit_read_type_expr", type_idx);
    WitTypeExpr *t = &g_type_pool[resolved];

    switch (t->kind) {
    case TYPE_IDENT: {
        /* string / bytes -> read tag, read len, read_ptr for data */
        if (strcmp(t->ident, "string") == 0) {
            fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
            fprintf(out, "%s  if (tag != SAP_WIT_TAG_STRING && tag != SAP_WIT_TAG_BYTES) return ERR_TYPE; }\n", indent);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_read_data(region, cursor, 4, &%s%slen));\n", indent, access, sep);
            fprintf(out, "%s{ const void *p; SAP_WIT_CHECK(thatch_read_ptr(region, cursor, %s%slen, &p));\n", indent, access, sep);
            fprintf(out, "%s  %s%sdata = (const uint8_t *)p; }\n", indent, access, sep);
            return;
        }
        /* bool -> read tag, check TRUE/FALSE */
        if (strcmp(t->ident, "bool") == 0) {
            fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
            fprintf(out, "%s  if (tag == SAP_WIT_TAG_BOOL_TRUE) %s = 1;\n", indent, access);
            fprintf(out, "%s  else if (tag == SAP_WIT_TAG_BOOL_FALSE) %s = 0;\n", indent, access);
            fprintf(out, "%s  else return ERR_TYPE; }\n", indent);
            return;
        }
        /* numeric primitives */
        const char *ctype = prim_c_type(t->ident);
        if (ctype) {
            const char *tag = NULL;
            int size = 0;
            if      (strcmp(t->ident, "s8")  == 0) { tag = "SAP_WIT_TAG_S8";  size = 1; }
            else if (strcmp(t->ident, "u8")  == 0) { tag = "SAP_WIT_TAG_U8";  size = 1; }
            else if (strcmp(t->ident, "s16") == 0) { tag = "SAP_WIT_TAG_S16"; size = 2; }
            else if (strcmp(t->ident, "u16") == 0) { tag = "SAP_WIT_TAG_U16"; size = 2; }
            else if (strcmp(t->ident, "s32") == 0) { tag = "SAP_WIT_TAG_S32"; size = 4; }
            else if (strcmp(t->ident, "u32") == 0) { tag = "SAP_WIT_TAG_U32"; size = 4; }
            else if (strcmp(t->ident, "s64") == 0) { tag = "SAP_WIT_TAG_S64"; size = 8; }
            else if (strcmp(t->ident, "u64") == 0) { tag = "SAP_WIT_TAG_U64"; size = 8; }
            else if (strcmp(t->ident, "f32") == 0) { tag = "SAP_WIT_TAG_F32"; size = 4; }
            else if (strcmp(t->ident, "f64") == 0) { tag = "SAP_WIT_TAG_F64"; size = 8; }
            if (tag) {
                fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
                fprintf(out, "%s  if (tag != %s) return ERR_TYPE; }\n", indent, tag);
                fprintf(out, "%sSAP_WIT_CHECK(thatch_read_data(region, cursor, %d, &%s));\n", indent, size, access);
            }
            return;
        }
        /* enum -> read tag + 1 byte */
        if (find_enum(reg, t->ident)) {
            fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
            fprintf(out, "%s  if (tag != SAP_WIT_TAG_ENUM) return ERR_TYPE; }\n", indent);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_read_data(region, cursor, 1, &%s));\n", indent, access);
            return;
        }
        /* flags -> read tag + 4 bytes */
        if (find_flags(reg, t->ident)) {
            fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
            fprintf(out, "%s  if (tag != SAP_WIT_TAG_FLAGS) return ERR_TYPE; }\n", indent);
            fprintf(out, "%sSAP_WIT_CHECK(thatch_read_data(region, cursor, 4, &%s));\n", indent, access);
            return;
        }
        /* named record or variant — delegate to its reader */
        if (find_record(reg, t->ident) || find_variant(reg, t->ident)) {
            char sn[MAX_NAME];
            kebab_to_snake(t->ident, sn, MAX_NAME);
            fprintf(out, "%sSAP_WIT_CHECK(sap_wit_read_%s(region, cursor, &%s));\n", indent, sn, access);
            return;
        }
        codegen_die("unsupported identifier in reader emission: %s", t->ident);
        return;
    }
    case TYPE_LIST:
        if (!is_list_u8(reg, t)) {
            codegen_die_type("unsupported list<T> reader (only list<u8> is supported)", resolved);
        }
        /* list<u8> same as bytes */
        fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
        fprintf(out, "%s  if (tag != SAP_WIT_TAG_BYTES && tag != SAP_WIT_TAG_STRING) return ERR_TYPE; }\n", indent);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_read_data(region, cursor, 4, &%s%slen));\n", indent, access, sep);
        fprintf(out, "%s{ const void *p; SAP_WIT_CHECK(thatch_read_ptr(region, cursor, %s%slen, &p));\n", indent, access, sep);
        fprintf(out, "%s  %s%sdata = (const uint8_t *)p; }\n", indent, access, sep);
        return;
    case TYPE_OPTION: {
        if (t->param_count < 1 || t->params[0] < 0) {
            codegen_die_type("option<T> reader missing inner type", resolved);
        }
        char inner_indent[64];
        snprintf(inner_indent, sizeof(inner_indent), "%s    ", indent);
        fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
        fprintf(out, "%s  if (tag == SAP_WIT_TAG_OPTION_SOME) {\n", indent);
        fprintf(out, "%s    %s = 1;\n", indent, access);
        emit_read_type_expr(out, reg, t->params[0], access, sep, inner_indent);
        fprintf(out, "%s  } else if (tag == SAP_WIT_TAG_OPTION_NONE) {\n", indent);
        fprintf(out, "%s    %s = 0;\n", indent, access);
        fprintf(out, "%s  } else return ERR_TYPE; }\n", indent);
        return;
    }
    case TYPE_TUPLE: {
        char inner[64];
        snprintf(inner, sizeof(inner), "%s    ", indent);
        fprintf(out, "%s{\n", indent);
        fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", inner);
        fprintf(out, "%s  if (tag != SAP_WIT_TAG_TUPLE) return ERR_TYPE; }\n", inner);
        fprintf(out, "%suint32_t _skip_len;\n", inner);
        fprintf(out, "%sSAP_WIT_CHECK(thatch_read_skip_len(region, cursor, &_skip_len));\n", inner);
        fprintf(out, "%suint32_t _remaining = thatch_region_used(region) - *cursor;\n", inner);
        fprintf(out, "%sif (_skip_len > _remaining) return ERR_RANGE;\n", inner);
        fprintf(out, "%sThatchCursor _segment_end = *cursor + _skip_len;\n", inner);
        for (int i = 0; i < t->param_count; i++) {
            char sub[256];
            snprintf(sub, sizeof(sub), "%s_%d", access, i);
            emit_read_type_expr(out, reg, t->params[i], sub, "_", inner);
        }
        fprintf(out, "%sif (*cursor != _segment_end) return ERR_TYPE;\n", inner);
        fprintf(out, "%s}\n", indent);
        return;
    }
    case TYPE_RESULT: {
        char *is_ok_access = NULL;
        char *ok_access = NULL;
        char *err_access = NULL;
        build_result_access_paths(access, &is_ok_access, &ok_access, &err_access);

        char inner[64];
        snprintf(inner, sizeof(inner), "%s    ", indent);
        fprintf(out, "%s{ uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n", indent);
        fprintf(out, "%s  if (tag == SAP_WIT_TAG_RESULT_OK) {\n", indent);
        fprintf(out, "%s%s = 1;\n", inner, is_ok_access);
        if (t->param_count > 0 && t->params[0] >= 0)
            emit_read_type_expr(out, reg, t->params[0], ok_access, ".", inner);
        fprintf(out, "%s  } else if (tag == SAP_WIT_TAG_RESULT_ERR) {\n", indent);
        fprintf(out, "%s%s = 0;\n", inner, is_ok_access);
        if (t->param_count > 1 && t->params[1] >= 0)
            emit_read_type_expr(out, reg, t->params[1], err_access, ".", inner);
        fprintf(out, "%s  } else return ERR_TYPE; }\n", indent);
        free(is_ok_access);
        free(ok_access);
        free(err_access);
        return;
    }
    }
    codegen_die("internal: unhandled WitTypeKind in emit_read_type_expr");
}

/* Emit a complete reader function for a record. */
static void emit_read_record(FILE *out, const WitRegistry *reg,
                              const WitRecord *rec)
{
    char snake[MAX_NAME], camel[MAX_NAME];
    kebab_to_snake(rec->name, snake, MAX_NAME);
    kebab_to_camel(rec->name, camel, MAX_NAME);

    fprintf(out, "int sap_wit_read_%s(const ThatchRegion *region, ThatchCursor *cursor, SapWit%s *out)\n{\n",
            snake, camel);

    /* All records have skip pointers (uniform encoding).
     * Read skip_len and enforce segment-end: cursor must equal
     * segment_end after all fields are decoded. */
    fprintf(out, "    { uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n");
    fprintf(out, "      if (tag != SAP_WIT_TAG_RECORD) return ERR_TYPE; }\n");
    fprintf(out, "    uint32_t _skip_len;\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_read_skip_len(region, cursor, &_skip_len));\n");
    fprintf(out, "    /* overflow-safe: reject if skip_len exceeds remaining bytes */\n");
    fprintf(out, "    uint32_t _remaining = thatch_region_used(region) - *cursor;\n");
    fprintf(out, "    if (_skip_len > _remaining) return ERR_RANGE;\n");
    fprintf(out, "    ThatchCursor _segment_end = *cursor + _skip_len;\n");

    for (int i = 0; i < rec->field_count; i++) {
        char fname[MAX_NAME], access[256];
        kebab_to_snake(rec->fields[i].name, fname, MAX_NAME);

        int res = resolve_type(reg, rec->fields[i].wit_type);
        WitTypeExpr *ft = (res >= 0) ? &g_type_pool[res] : NULL;

        if (ft && ft->kind == TYPE_OPTION) {
            /* Option fields: read tag, branch on SOME/NONE */
            fprintf(out, "    { uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n");
            fprintf(out, "      if (tag == SAP_WIT_TAG_OPTION_SOME) {\n");
            fprintf(out, "        out->has_%s = 1;\n", fname);
            snprintf(access, sizeof(access), "out->%s", fname);
            emit_read_type_expr(out, reg, ft->params[0], access, "_", "        ");
            fprintf(out, "      } else if (tag == SAP_WIT_TAG_OPTION_NONE) {\n");
            fprintf(out, "        out->has_%s = 0;\n", fname);
            fprintf(out, "      } else return ERR_TYPE; }\n");
        } else {
            snprintf(access, sizeof(access), "out->%s", fname);
            emit_read_type_expr(out, reg, rec->fields[i].wit_type, access, "_", "    ");
        }
    }

    fprintf(out, "    if (*cursor != _segment_end) return ERR_TYPE;\n");
    fprintf(out, "    return ERR_OK;\n}\n\n");
}

/* Emit a complete reader function for a variant. */
static void emit_read_variant(FILE *out, const WitRegistry *reg,
                               const WitVariant *var)
{
    char snake[MAX_NAME], camel[MAX_NAME], upper_var[MAX_NAME];
    kebab_to_snake(var->name, snake, MAX_NAME);
    kebab_to_camel(var->name, camel, MAX_NAME);
    kebab_to_upper(var->name, upper_var, MAX_NAME);

    fprintf(out, "int sap_wit_read_%s(const ThatchRegion *region, ThatchCursor *cursor, SapWit%s *out)\n{\n",
            snake, camel);
    fprintf(out, "    { uint8_t tag; SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n");
    fprintf(out, "      if (tag != SAP_WIT_TAG_VARIANT) return ERR_TYPE; }\n");
    fprintf(out, "    uint32_t _skip_len;\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_read_skip_len(region, cursor, &_skip_len));\n");
    fprintf(out, "    uint32_t _remaining = thatch_region_used(region) - *cursor;\n");
    fprintf(out, "    if (_skip_len > _remaining) return ERR_RANGE;\n");
    fprintf(out, "    ThatchCursor _segment_end = *cursor + _skip_len;\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_read_data(region, cursor, 1, &out->case_tag));\n");
    fprintf(out, "    switch (out->case_tag) {\n");

    for (int j = 0; j < var->case_count; j++) {
        char cu[MAX_NAME], cs[MAX_NAME];
        kebab_to_upper(var->cases[j].name, cu, MAX_NAME);
        kebab_to_snake(var->cases[j].name, cs, MAX_NAME);
        fprintf(out, "    case SAP_WIT_%s_%s:\n", upper_var, cu);
        if (var->cases[j].payload_type >= 0) {
            char access[256];
            snprintf(access, sizeof(access), "out->val.%s", cs);
            emit_read_type_expr(out, reg, var->cases[j].payload_type, access, ".", "        ");
        }
        fprintf(out, "        break;\n");
    }

    fprintf(out, "    default: return ERR_TYPE;\n");
    fprintf(out, "    }\n");
    fprintf(out, "    if (*cursor != _segment_end) return ERR_TYPE;\n");
    fprintf(out, "    return ERR_OK;\n}\n\n");
}

/* ------------------------------------------------------------------ */
/* Universal skip emission                                            */
/* ------------------------------------------------------------------ */

static void emit_skip_function(FILE *out)
{
    fprintf(out, "int sap_wit_skip_value(const ThatchRegion *region, ThatchCursor *cursor)\n{\n");
    fprintf(out, "    uint8_t tag;\n");
    fprintf(out, "    SAP_WIT_CHECK(thatch_read_tag(region, cursor, &tag));\n");
    fprintf(out, "    switch (tag) {\n");

    /* Fixed-size primitives */
    fprintf(out, "    case SAP_WIT_TAG_S8:  case SAP_WIT_TAG_U8:\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, 1);\n");
    fprintf(out, "    case SAP_WIT_TAG_S16: case SAP_WIT_TAG_U16:\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, 2);\n");
    fprintf(out, "    case SAP_WIT_TAG_S32: case SAP_WIT_TAG_U32: case SAP_WIT_TAG_F32:\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, 4);\n");
    fprintf(out, "    case SAP_WIT_TAG_S64: case SAP_WIT_TAG_U64: case SAP_WIT_TAG_F64:\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, 8);\n");

    /* Bool: no payload */
    fprintf(out, "    case SAP_WIT_TAG_BOOL_TRUE: case SAP_WIT_TAG_BOOL_FALSE:\n");
    fprintf(out, "        return ERR_OK;\n");

    /* Enum: 1 byte payload */
    fprintf(out, "    case SAP_WIT_TAG_ENUM:\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, 1);\n");

    /* Flags: 4 byte payload */
    fprintf(out, "    case SAP_WIT_TAG_FLAGS:\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, 4);\n");

    /* String/bytes: len + data */
    fprintf(out, "    case SAP_WIT_TAG_STRING: case SAP_WIT_TAG_BYTES: {\n");
    fprintf(out, "        uint32_t len;\n");
    fprintf(out, "        SAP_WIT_CHECK(thatch_read_data(region, cursor, 4, &len));\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, len);\n");
    fprintf(out, "    }\n");

    /* Record/tuple/list/variant with skip pointer: read skip len, advance */
    fprintf(out, "    case SAP_WIT_TAG_RECORD: case SAP_WIT_TAG_TUPLE:\n");
    fprintf(out, "    case SAP_WIT_TAG_LIST: case SAP_WIT_TAG_VARIANT: {\n");
    fprintf(out, "        uint32_t skip;\n");
    fprintf(out, "        SAP_WIT_CHECK(thatch_read_skip_len(region, cursor, &skip));\n");
    fprintf(out, "        return thatch_advance_cursor(region, cursor, skip);\n");
    fprintf(out, "    }\n");

    /* Option SOME: recursively skip inner */
    fprintf(out, "    case SAP_WIT_TAG_OPTION_SOME:\n");
    fprintf(out, "        return sap_wit_skip_value(region, cursor);\n");

    /* Option NONE: done */
    fprintf(out, "    case SAP_WIT_TAG_OPTION_NONE:\n");
    fprintf(out, "        return ERR_OK;\n");

    /* Result OK/ERR: recursively skip payload */
    fprintf(out, "    case SAP_WIT_TAG_RESULT_OK: case SAP_WIT_TAG_RESULT_ERR:\n");
    fprintf(out, "        return sap_wit_skip_value(region, cursor);\n");

    fprintf(out, "    default: return ERR_TYPE;\n");
    fprintf(out, "    }\n");
    fprintf(out, "}\n\n");
}

/* ------------------------------------------------------------------ */
/* Source emission                                                    */
/* ------------------------------------------------------------------ */

static void emit_source(FILE *out, const WitRegistry *reg,
                        const DbiEntry *dbis, int ndbi,
                        const char *header_path)
{
    char snake[MAX_NAME];

    fprintf(out, "/* Auto-generated by tools/wit_codegen; DO NOT EDIT. */\n");
    fprintf(out, "#include \"%s\"\n", header_path);
    fprintf(out, "#include <string.h>\n\n");

    /* Error propagation macro */
    fprintf(out, "#define SAP_WIT_CHECK(rc) do { if ((rc) != ERR_OK) return (rc); } while (0)\n\n");

    /* DBI schema table */
    if (ndbi > 0) {
        fprintf(out, "const SapWitDbiSchema sap_wit_dbi_schema[] = {\n");
        for (int i = 0; i < ndbi; i++) {
            kebab_to_snake(dbis[i].name, snake, MAX_NAME);
            fprintf(out, "    {%du, \"%s\", \"%s\", \"%s\"},\n",
                    dbis[i].dbi, snake, dbis[i].key_rec, dbis[i].val_rec);
        }
        fprintf(out, "};\n\n");
        fprintf(out, "const uint32_t sap_wit_dbi_schema_count =\n");
        fprintf(out, "    (uint32_t)(sizeof(sap_wit_dbi_schema) / sizeof(sap_wit_dbi_schema[0]));\n\n");
    }

    /* Writer functions in topological order */
    const char *order[MAX_TYPES * 2];
    int norder = topo_sort_types(reg, order, MAX_TYPES * 2);
    if (norder < 0) return;

    fprintf(out, "/* ---- Writer functions ---- */\n\n");
    for (int idx = 0; idx < norder; idx++) {
        const WitRecord *rec = find_record(reg, order[idx]);
        if (rec) { emit_write_record(out, reg, rec); continue; }
        const WitVariant *var = find_variant(reg, order[idx]);
        if (var) { emit_write_variant(out, reg, var); continue; }
    }

    /* Reader functions in topological order */
    fprintf(out, "/* ---- Reader functions ---- */\n\n");
    for (int idx = 0; idx < norder; idx++) {
        const WitRecord *rec = find_record(reg, order[idx]);
        if (rec) { emit_read_record(out, reg, rec); continue; }
        const WitVariant *var = find_variant(reg, order[idx]);
        if (var) { emit_read_variant(out, reg, var); continue; }
    }

    /* Universal skip function */
    fprintf(out, "/* ---- Universal skip ---- */\n\n");
    emit_skip_function(out);

    /* DBI blob validators — full structural validation via typed readers */
    fprintf(out, "/* ---- DBI blob validators ---- */\n\n");
    for (int i = 0; i < ndbi; i++) {
        char val_snake[MAX_NAME], val_camel[MAX_NAME];
        kebab_to_snake(dbis[i].val_rec, val_snake, MAX_NAME);
        kebab_to_camel(dbis[i].val_rec, val_camel, MAX_NAME);
        fprintf(out, "int sap_wit_validate_%s(const void *data, uint32_t len)\n{\n", val_snake);
        fprintf(out, "    if (!data && !len) return 0;\n");
        fprintf(out, "    if (!data || !len) return -1;\n");
        fprintf(out, "    ThatchRegion view;\n");
        fprintf(out, "    if (thatch_region_init_readonly(&view, data, len) != ERR_OK) return -1;\n");
        fprintf(out, "    ThatchCursor cur = 0;\n");
        fprintf(out, "    SapWit%s scratch;\n", val_camel);
        fprintf(out, "    memset(&scratch, 0, sizeof(scratch));\n");
        fprintf(out, "    int rc = sap_wit_read_%s(&view, &cur, &scratch);\n", val_snake);
        fprintf(out, "    if (rc != ERR_OK) return -1;\n");
        fprintf(out, "    if (cur != len) return -1;\n");
        fprintf(out, "    return 0;\n");
        fprintf(out, "}\n\n");
    }
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    const char *wit_path = NULL;
    const char *header_path = NULL;
    const char *source_path = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--wit") == 0 && i + 1 < argc)
            wit_path = argv[++i];
        else if (strcmp(argv[i], "--header") == 0 && i + 1 < argc)
            header_path = argv[++i];
        else if (strcmp(argv[i], "--source") == 0 && i + 1 < argc)
            source_path = argv[++i];
        else if (argv[i][0] != '-' && !wit_path)
            wit_path = argv[i];
    }

    if (!wit_path) {
        fprintf(stderr,
            "usage: wit_codegen [--wit] <schema.wit> "
            "[--header <path>] [--source <path>]\n");
        return 1;
    }

    FILE *f = fopen(wit_path, "rb");
    if (!f) {
        fprintf(stderr, "wit_codegen: cannot open %s\n", wit_path);
        return 1;
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *src = malloc(fsize + 1);
    if (!src) { fclose(f); return 1; }
    fread(src, 1, fsize, f);
    src[fsize] = '\0';
    fclose(f);

    Scanner scanner;
    scanner_init(&scanner, src, (int)fsize);

    WitRegistry reg;
    if (!parse_wit(&scanner, &reg)) {
        fprintf(stderr, "wit_codegen: parse failed at line %d col %d\n",
                scanner.line, scanner.col);
        free(src);
        return 1;
    }

    DbiEntry dbis[MAX_TYPES];
    int ndbi = extract_dbis(&reg, dbis, MAX_TYPES);
    if (ndbi < 0) {
        fprintf(stderr, "wit_codegen: DBI extraction failed\n");
        free(src);
        return 1;
    }

    if (header_path) {
        FILE *hdr = fopen(header_path, "w");
        if (!hdr) {
            fprintf(stderr, "wit_codegen: cannot create %s\n", header_path);
            free(src); return 1;
        }
        emit_header(hdr, &reg, dbis, ndbi, header_path);
        fclose(hdr);
    }

    if (source_path && header_path) {
        FILE *csrc = fopen(source_path, "w");
        if (!csrc) {
            fprintf(stderr, "wit_codegen: cannot create %s\n", source_path);
            free(src); return 1;
        }
        emit_source(csrc, &reg, dbis, ndbi, header_path);
        fclose(csrc);
    }

    printf("wit_codegen: PASS (records=%d variants=%d enums=%d flags=%d "
           "aliases=%d dbis=%d)\n",
           reg.record_count, reg.variant_count, reg.enum_count,
           reg.flags_count, reg.alias_count, ndbi);

    free(src);
    return 0;
}
