/*
 * attempt_handler_v0.c - generic runner handler adapter over attempt_v0
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/attempt_handler_v0.h"

#include <string.h>

typedef struct
{
    SapRunnerAttemptHandlerV0 *handler;
    SapRunnerV0 *runner;
    const SapRunnerMessageV0 *msg;
} AttemptHandlerAtomicCtx;

static int atomic_trampoline(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    AttemptHandlerAtomicCtx *atomic = (AttemptHandlerAtomicCtx *)ctx;

    if (!atomic || !atomic->handler || !atomic->runner || !atomic->msg ||
        !atomic->handler->atomic_fn)
    {
        return ERR_INVALID;
    }
    return atomic->handler->atomic_fn(stack, read_txn, atomic->runner, atomic->msg,
                                      atomic->handler->atomic_ctx);
}

int sap_runner_attempt_handler_v0_init(SapRunnerAttemptHandlerV0 *handler, DB *db,
                                       sap_runner_attempt_handler_v0_atomic_fn atomic_fn,
                                       void *atomic_ctx,
                                       sap_runner_attempt_v0_intent_sink_fn intent_sink,
                                       void *intent_ctx)
{
    if (!handler || !db || !atomic_fn)
    {
        return ERR_INVALID;
    }
    memset(handler, 0, sizeof(*handler));
    handler->db = db;
    handler->atomic_fn = atomic_fn;
    handler->atomic_ctx = atomic_ctx;
    handler->intent_sink = intent_sink;
    handler->intent_ctx = intent_ctx;
    sap_runner_attempt_v0_policy_default(&handler->policy);
    handler->last_stats.last_rc = ERR_OK;
    return ERR_OK;
}

void sap_runner_attempt_handler_v0_set_policy(SapRunnerAttemptHandlerV0 *handler,
                                              const SapRunnerAttemptV0Policy *policy)
{
    if (!handler)
    {
        return;
    }
    if (!policy)
    {
        sap_runner_attempt_v0_policy_default(&handler->policy);
        return;
    }
    handler->policy = *policy;
}

int sap_runner_attempt_handler_v0_runner_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg,
                                                 void *ctx)
{
    SapRunnerAttemptHandlerV0 *handler = (SapRunnerAttemptHandlerV0 *)ctx;
    AttemptHandlerAtomicCtx atomic_ctx = {0};
    SapRunnerAttemptV0Stats stats = {0};
    int rc;

    if (!runner || !msg || !handler || !handler->db || !handler->atomic_fn)
    {
        return ERR_INVALID;
    }

    atomic_ctx.handler = handler;
    atomic_ctx.runner = runner;
    atomic_ctx.msg = msg;
    rc = sap_runner_attempt_v0_run(handler->db, &handler->policy, atomic_trampoline, &atomic_ctx,
                                   handler->intent_sink, handler->intent_ctx, &stats);
    handler->last_stats = stats;
    return rc;
}
