#include "runner/scheduler_v0.h"
#include "runner/timer_v0.h"
#include "sapling/bept.h"
#include "generated/wit_schema_dbis.h"

#include <stddef.h>

int sap_runner_scheduler_v0_next_due(DB *db, int64_t *due_ts_out)
{
    Txn *txn;
    uint32_t bept_key[4];
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

    /* Use BEPT Min to find earliest timer */
    rc = sap_bept_min(txn, bept_key, 4, NULL, NULL);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    sap_runner_timer_v0_bept_key_decode(bept_key, due_ts_out, NULL);
    txn_abort(txn);
    return SAP_OK;
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
