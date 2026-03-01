#include "runner/scheduler_v0.h"
#include "runner/timer_v0.h"

#include <stddef.h>

int sap_runner_scheduler_v0_next_due(DB *db, int64_t *due_ts_out)
{
    if (!db || !due_ts_out)
    {
        return ERR_INVALID;
    }
    *due_ts_out = 0;
    return sap_runner_timer_v0_next_due(db, due_ts_out);
}

int sap_runner_scheduler_v0_compute_sleep_ms(int64_t now_ts, int64_t next_due_ts,
                                             uint32_t max_sleep_ms, uint32_t *sleep_ms_out)
{
    uint64_t delta;
    uint64_t cap;

    if (!sleep_ms_out)
    {
        return ERR_INVALID;
    }
    *sleep_ms_out = 0u;

    if (next_due_ts <= now_ts)
    {
        return ERR_OK;
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
    return ERR_OK;
}
