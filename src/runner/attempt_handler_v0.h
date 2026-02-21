/*
 * attempt_handler_v0.h - generic runner handler adapter over attempt_v0
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_ATTEMPT_HANDLER_V0_H
#define SAPLING_RUNNER_ATTEMPT_HANDLER_V0_H

#include "runner/attempt_v0.h"
#include "runner/runner_v0.h"

#include <stdint.h>

typedef int (*sap_runner_attempt_handler_v0_atomic_fn)(SapRunnerTxStackV0 *stack, Txn *read_txn,
                                                       SapRunnerV0 *runner,
                                                       const SapRunnerMessageV0 *msg, void *ctx);

typedef struct
{
    DB *db;
    sap_runner_attempt_handler_v0_atomic_fn atomic_fn;
    void *atomic_ctx;
    sap_runner_attempt_v0_intent_sink_fn intent_sink;
    void *intent_ctx;
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats last_stats;
} SapRunnerAttemptHandlerV0;

int sap_runner_attempt_handler_v0_init(SapRunnerAttemptHandlerV0 *handler, DB *db,
                                       sap_runner_attempt_handler_v0_atomic_fn atomic_fn,
                                       void *atomic_ctx,
                                       sap_runner_attempt_v0_intent_sink_fn intent_sink,
                                       void *intent_ctx);

void sap_runner_attempt_handler_v0_set_policy(SapRunnerAttemptHandlerV0 *handler,
                                              const SapRunnerAttemptV0Policy *policy);

/* Adapter matching sap_runner_v0_message_handler signature. */
int sap_runner_attempt_handler_v0_runner_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg,
                                                 void *ctx);

#endif /* SAPLING_RUNNER_ATTEMPT_HANDLER_V0_H */
