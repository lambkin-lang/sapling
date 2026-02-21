/*
 * attempt_v0.c - phase-B bounded retry attempt engine scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/attempt_v0.h"

#include <string.h>
#include <time.h>

static int is_retryable(int rc) { return rc == SAP_BUSY || rc == SAP_CONFLICT; }

static void default_sleep(uint32_t backoff_us, void *ctx)
{
    struct timespec ts;

    (void)ctx;
    ts.tv_sec = (time_t)(backoff_us / 1000000u);
    ts.tv_nsec = (long)((backoff_us % 1000000u) * 1000u);
    nanosleep(&ts, NULL);
}

static uint32_t compute_backoff_us(const SapRunnerAttemptV0Policy *policy, uint32_t retry_index)
{
    uint32_t i;
    uint32_t backoff;

    if (!policy || policy->initial_backoff_us == 0u)
    {
        return 0u;
    }

    backoff = policy->initial_backoff_us;
    for (i = 0u; i < retry_index; i++)
    {
        if (backoff > (UINT32_MAX / 2u))
        {
            backoff = UINT32_MAX;
            break;
        }
        backoff *= 2u;
    }
    if (policy->max_backoff_us > 0u && backoff > policy->max_backoff_us)
    {
        backoff = policy->max_backoff_us;
    }
    return backoff;
}

static void maybe_sleep(const SapRunnerAttemptV0Policy *policy, uint32_t retry_index)
{
    uint32_t backoff;
    sap_runner_attempt_v0_sleep_fn fn;
    void *ctx;

    if (!policy)
    {
        return;
    }
    backoff = compute_backoff_us(policy, retry_index);
    if (backoff == 0u)
    {
        return;
    }
    fn = policy->sleep_fn ? policy->sleep_fn : default_sleep;
    ctx = policy->sleep_ctx;
    fn(backoff, ctx);
}

static void stats_note_retry(SapRunnerAttemptV0Stats *stats, int rc)
{
    if (!stats)
    {
        return;
    }
    stats->retries++;
    if (rc == SAP_CONFLICT)
    {
        stats->conflict_retries++;
    }
    else if (rc == SAP_BUSY)
    {
        stats->busy_retries++;
    }
}

void sap_runner_attempt_v0_policy_default(SapRunnerAttemptV0Policy *policy)
{
    if (!policy)
    {
        return;
    }
    memset(policy, 0, sizeof(*policy));
    policy->max_retries = 3u;
    policy->initial_backoff_us = 250u;
    policy->max_backoff_us = 10000u;
    policy->sleep_fn = NULL;
    policy->sleep_ctx = NULL;
}

int sap_runner_attempt_v0_run(DB *db, const SapRunnerAttemptV0Policy *policy,
                              sap_runner_attempt_v0_atomic_fn atomic_fn, void *atomic_ctx,
                              sap_runner_attempt_v0_intent_sink_fn intent_sink, void *intent_ctx,
                              SapRunnerAttemptV0Stats *stats_out)
{
    SapRunnerAttemptV0Policy local_policy = {0};
    SapRunnerAttemptV0Stats stats = {0};
    SapRunnerTxStackV0 stack;
    uint32_t attempt_no = 0u;
    int rc = SAP_ERROR;

    if (!db || !atomic_fn)
    {
        return SAP_ERROR;
    }
    if (!policy)
    {
        sap_runner_attempt_v0_policy_default(&local_policy);
        policy = &local_policy;
    }

    rc = sap_runner_txstack_v0_init(&stack);
    if (rc != SAP_OK)
    {
        return rc;
    }

    for (;;)
    {
        Txn *rtxn;
        Txn *wtxn;
        const SapRunnerTxCtxV0 *root;
        uint32_t i;

        stats.attempts++;
        sap_runner_txstack_v0_reset(&stack);
        rc = sap_runner_txstack_v0_push(&stack);
        if (rc != SAP_OK)
        {
            break;
        }

        rtxn = txn_begin(db, NULL, TXN_RDONLY);
        if (!rtxn)
        {
            rc = SAP_ERROR;
            break;
        }

        rc = atomic_fn(&stack, rtxn, atomic_ctx);
        txn_abort(rtxn);
        if (rc != SAP_OK)
        {
            if (is_retryable(rc) && attempt_no < policy->max_retries)
            {
                stats_note_retry(&stats, rc);
                maybe_sleep(policy, attempt_no);
                attempt_no++;
                continue;
            }
            break;
        }

        wtxn = txn_begin(db, NULL, 0u);
        if (!wtxn)
        {
            rc = SAP_BUSY;
        }
        else
        {
            rc = sap_runner_txstack_v0_validate_root_reads(&stack, wtxn);
            if (rc == SAP_OK)
            {
                rc = sap_runner_txstack_v0_apply_root_writes(&stack, wtxn);
            }
            if (rc == SAP_OK)
            {
                rc = txn_commit(wtxn);
            }
            else
            {
                txn_abort(wtxn);
            }
        }

        if (rc != SAP_OK)
        {
            if (is_retryable(rc) && attempt_no < policy->max_retries)
            {
                stats_note_retry(&stats, rc);
                maybe_sleep(policy, attempt_no);
                attempt_no++;
                continue;
            }
            break;
        }

        if (intent_sink)
        {
            root = sap_runner_txstack_v0_root(&stack);
            if (!root)
            {
                rc = SAP_ERROR;
                break;
            }

            for (i = 0u; i < sap_runner_txctx_v0_intent_count(root); i++)
            {
                uint32_t frame_len = 0u;
                const uint8_t *frame = sap_runner_txctx_v0_intent_frame(root, i, &frame_len);
                if (!frame || frame_len == 0u)
                {
                    rc = SAP_ERROR;
                    break;
                }
                rc = intent_sink(frame, frame_len, intent_ctx);
                if (rc != SAP_OK)
                {
                    break;
                }
            }
            if (rc != SAP_OK)
            {
                break;
            }
        }

        rc = SAP_OK;
        break;
    }

    stats.last_rc = rc;
    if (stats_out)
    {
        *stats_out = stats;
    }
    sap_runner_txstack_v0_dispose(&stack);
    return rc;
}
