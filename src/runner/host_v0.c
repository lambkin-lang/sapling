/*
 * host_v0.c - Host API for non-Wasm and Wasm guests in atomic blocks
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/host_v0.h"
#include "runner/lease_v0.h"

#include <string.h>

void sap_host_v0_init(SapHostV0 *host, SapRunnerTxStackV0 *stack, Txn *read_txn,
                      uint64_t worker_id, int64_t now_ms)
{
    if (!host)
    {
        return;
    }
    host->stack = stack;
    host->read_txn = read_txn;
    host->worker_id = worker_id;
    host->now_ms = now_ms;
}

int sap_host_v0_get(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len,
                    const void **val_out, uint32_t *val_len_out)
{
    if (!host || !host->stack || !host->read_txn)
    {
        return SAP_ERROR;
    }
    return sap_runner_txstack_v0_read_dbi(host->stack, host->read_txn, dbi, key, key_len, val_out,
                                          val_len_out);
}

int sap_host_v0_put(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len,
                    const void *val, uint32_t val_len)
{
    if (!host || !host->stack)
    {
        return SAP_ERROR;
    }
    return sap_runner_txstack_v0_stage_put_dbi(host->stack, dbi, key, key_len, val, val_len);
}

int sap_host_v0_del(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len)
{
    if (!host || !host->stack)
    {
        return SAP_ERROR;
    }
    return sap_runner_txstack_v0_stage_del_dbi(host->stack, dbi, key, key_len);
}

int sap_host_v0_emit(SapHostV0 *host, const void *msg, uint32_t msg_len)
{
    SapRunnerIntentV0 intent = {0};

    if (!host || !host->stack || !msg || msg_len == 0u)
    {
        return SAP_ERROR;
    }

    intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    intent.flags = 0u;
    intent.due_ts = 0;
    intent.message = msg;
    intent.message_len = msg_len;

    return sap_runner_txstack_v0_push_intent(host->stack, &intent);
}

int sap_host_v0_arm(SapHostV0 *host, int64_t due_ts, const void *msg, uint32_t msg_len)
{
    SapRunnerIntentV0 intent = {0};

    if (!host || !host->stack || !msg || msg_len == 0u)
    {
        return SAP_ERROR;
    }

    intent.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
    intent.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
    intent.due_ts = due_ts;
    intent.message = msg;
    intent.message_len = msg_len;

    return sap_runner_txstack_v0_push_intent(host->stack, &intent);
}

int sap_host_v0_lease_acquire(SapHostV0 *host, const void *key, uint32_t key_len,
                              int64_t duration_ms)
{
    SapRunnerLeaseV0 lease = {0};
    if (!host || !host->stack || !host->read_txn)
    {
        return SAP_ERROR;
    }
    return sap_runner_lease_v0_stage_acquire(host->stack, host->read_txn, key, key_len,
                                             host->worker_id, host->now_ms, duration_ms, &lease);
}

int sap_host_v0_lease_release(SapHostV0 *host, const void *key, uint32_t key_len)
{
    if (!host || !host->stack || !host->read_txn)
    {
        return SAP_ERROR;
    }
    return sap_runner_lease_v0_stage_release(host->stack, host->read_txn, key, key_len,
                                             host->worker_id);
}
