/*
 * shim_v0.h - phase-A WASI invocation shim for runner worker shell
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_WASI_SHIM_V0_H
#define SAPLING_WASI_SHIM_V0_H

#include "runner/attempt_v0.h"
#include "runner/intent_sink_v0.h"
#include "runner/runner_v0.h"
#include "wasi/runtime_v0.h"

#include <stdint.h>

#define SAP_WASI_SHIM_V0_DEFAULT_REPLY_CAP 4096u
#define SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE 8u

typedef struct
{
    uint64_t initial_outbox_seq;
    int emit_outbox_events;
    /* Optional caller-provided reply buffer; if NULL, shim uses inline storage. */
    uint8_t *reply_buf;
    /* Buffer capacity in bytes; when reply_buf is NULL this may be <= DEFAULT. */
    uint32_t reply_buf_cap;
} SapWasiShimV0Options;

typedef struct
{
    DB *db;
    SapWasiRuntimeV0 *runtime;
    SapRunnerIntentSinkV0 intent_sink;
    SapRunnerAttemptV0Policy attempt_policy;
    SapRunnerAttemptV0Stats last_attempt_stats;
    uint64_t next_outbox_seq;
    int emit_outbox_events;
    uint8_t *reply_buf;
    uint32_t reply_buf_cap;
    uint8_t reply_buf_inline[SAP_WASI_SHIM_V0_DEFAULT_REPLY_CAP];
} SapWasiShimV0;

void sap_wasi_shim_v0_options_default(SapWasiShimV0Options *options);

int sap_wasi_shim_v0_init_with_options(SapWasiShimV0 *shim, DB *db, SapWasiRuntimeV0 *runtime,
                                       const SapWasiShimV0Options *options);

int sap_wasi_shim_v0_init(SapWasiShimV0 *shim, DB *db, SapWasiRuntimeV0 *runtime,
                          uint64_t initial_outbox_seq, int emit_outbox_events);
void sap_wasi_shim_v0_set_attempt_policy(SapWasiShimV0 *shim,
                                         const SapRunnerAttemptV0Policy *policy);

/* Adapter to pass into runner worker handler callbacks. */
int sap_wasi_shim_v0_runner_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx);

/* Helper to initialize worker shell using this shim as handler context. */
int sap_wasi_shim_v0_worker_init(SapRunnerV0Worker *worker, const SapRunnerV0Config *cfg,
                                 SapWasiShimV0 *shim, uint32_t max_batch);

void sap_wasi_shim_v0_outbox_key_encode(uint64_t seq,
                                        uint8_t out[SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE]);

#endif /* SAPLING_WASI_SHIM_V0_H */
