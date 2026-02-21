/*
 * scheduler_v0.c - phase-C timer wake scheduling helper scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/scheduler_v0.h"

#include "generated/wit_schema_dbis.h"

#include <stddef.h>

int sap_runner_scheduler_v0_next_due(DB *db, int64_t *due_ts_out)
{
    Txn *txn;
    Cursor *cur;
    const void *key = NULL;
    uint32_t key_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;
    uint64_t seq = 0u;
    int rc;

    if (!db || !due_ts_out)
    {
        return SAP_ERROR;
    }
    *due_ts_out = 0;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_TIMERS);
    if (!cur)
    {
        txn_abort(txn);
        return SAP_ERROR;
    }

    rc = cursor_first(cur);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    rc = cursor_get(cur, &key, &key_len, &val, &val_len);
    (void)val;
    (void)val_len;
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    rc = sap_runner_timer_v0_key_decode((const uint8_t *)key, key_len, due_ts_out, &seq);
    (void)seq;
    cursor_close(cur);
    txn_abort(txn);
    return rc;
}

int sap_runner_scheduler_v0_compute_sleep_ms(int64_t now_ts, int64_t next_due_ts,
                                             uint32_t max_sleep_ms, uint32_t *sleep_ms_out)
{
    uint64_t delta;
    uint64_t cap;

    if (!sleep_ms_out)
    {
        return SAP_ERROR;
    }
    *sleep_ms_out = 0u;

    if (next_due_ts <= now_ts)
    {
        return SAP_OK;
    }

    delta = (uint64_t)(next_due_ts - now_ts);
    cap = (max_sleep_ms == 0u) ? UINT32_MAX : (uint64_t)max_sleep_ms;
    if (delta > cap)
    {
        delta = cap;
    }
    if (delta > UINT32_MAX)
    {
        delta = UINT32_MAX;
    }
    *sleep_ms_out = (uint32_t)delta;
    return SAP_OK;
}
