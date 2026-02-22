#include "sapling/sapling.h"

#include <stdlib.h>
#include <string.h>

static void *smoke_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void smoke_free(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

static int expect_kv(Txn *txn, const char *key, const char *val)
{
    const void *got = NULL;
    uint32_t got_len = 0;
    uint32_t key_len = (uint32_t)strlen(key);
    uint32_t val_len = (uint32_t)strlen(val);

    if (txn_get(txn, key, key_len, &got, &got_len) != SAP_OK)
        return 0;
    if (got_len != val_len)
        return 0;
    return memcmp(got, val, val_len) == 0;
}

int main(void)
{
    PageAllocator alloc = {smoke_alloc, smoke_free, NULL};
    DB *db = db_open(&alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    Txn *outer = NULL;
    Txn *inner = NULL;
    Txn *r = NULL;
    const void *tmp = NULL;
    uint32_t tmp_len = 0;
    int rc = 0;

    if (!db)
        return 1;

    outer = txn_begin(db, NULL, 0);
    if (!outer)
    {
        rc = 2;
        goto done;
    }

    if (txn_put(outer, "outer", 5, "A", 1) != SAP_OK)
    {
        rc = 3;
        goto done;
    }

    /* Child commit: becomes visible to parent, durable after outer commit. */
    inner = txn_begin(db, outer, 0);
    if (!inner)
    {
        rc = 4;
        goto done;
    }
    if (txn_put(inner, "child_ok", 8, "B", 1) != SAP_OK)
    {
        rc = 5;
        goto done;
    }
    if (!expect_kv(inner, "outer", "A"))
    {
        rc = 6;
        goto done;
    }
    if (txn_commit(inner) != SAP_OK)
    {
        inner = NULL;
        rc = 7;
        goto done;
    }
    inner = NULL;
    if (!expect_kv(outer, "child_ok", "B"))
    {
        rc = 8;
        goto done;
    }

    /* Child abort: discarded, parent state remains unchanged. */
    inner = txn_begin(db, outer, 0);
    if (!inner)
    {
        rc = 9;
        goto done;
    }
    if (txn_put(inner, "child_no", 8, "X", 1) != SAP_OK)
    {
        rc = 10;
        goto done;
    }
    txn_abort(inner);
    inner = NULL;
    if (txn_get(outer, "child_no", 8, &tmp, &tmp_len) != SAP_NOTFOUND)
    {
        rc = 11;
        goto done;
    }

    if (txn_commit(outer) != SAP_OK)
    {
        outer = NULL;
        rc = 12;
        goto done;
    }
    outer = NULL;

    r = txn_begin(db, NULL, TXN_RDONLY);
    if (!r)
    {
        rc = 13;
        goto done;
    }
    if (!expect_kv(r, "outer", "A"))
    {
        rc = 14;
        goto done;
    }
    if (!expect_kv(r, "child_ok", "B"))
    {
        rc = 15;
        goto done;
    }
    if (txn_get(r, "child_no", 8, &tmp, &tmp_len) != SAP_NOTFOUND)
    {
        rc = 16;
        goto done;
    }

done:
    if (inner)
        txn_abort(inner);
    if (outer)
        txn_abort(outer);
    if (r)
        txn_abort(r);
    db_close(db);
    return rc;
}
