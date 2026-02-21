/*
 * attempt_v0.h - phase-B bounded retry attempt engine scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_ATTEMPT_V0_H
#define SAPLING_RUNNER_ATTEMPT_V0_H

#include "runner/txstack_v0.h"

#include <stdint.h>

typedef void (*sap_runner_attempt_v0_sleep_fn)(uint32_t backoff_us, void *ctx);

typedef struct
{
    uint32_t max_retries;
    uint32_t initial_backoff_us;
    uint32_t max_backoff_us;
    sap_runner_attempt_v0_sleep_fn sleep_fn;
    void *sleep_ctx;
} SapRunnerAttemptV0Policy;

typedef struct
{
    uint32_t attempts;
    uint32_t retries;
    uint32_t conflict_retries;
    uint32_t busy_retries;
    int last_rc;
} SapRunnerAttemptV0Stats;

typedef int (*sap_runner_attempt_v0_atomic_fn)(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx);

typedef int (*sap_runner_attempt_v0_intent_sink_fn)(const uint8_t *frame, uint32_t frame_len,
                                                    void *ctx);

void sap_runner_attempt_v0_policy_default(SapRunnerAttemptV0Policy *policy);

int sap_runner_attempt_v0_run(DB *db, const SapRunnerAttemptV0Policy *policy,
                              sap_runner_attempt_v0_atomic_fn atomic_fn, void *atomic_ctx,
                              sap_runner_attempt_v0_intent_sink_fn intent_sink, void *intent_ctx,
                              SapRunnerAttemptV0Stats *stats_out);

#endif /* SAPLING_RUNNER_ATTEMPT_V0_H */
