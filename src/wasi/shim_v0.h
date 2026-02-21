/*
 * shim_v0.h - phase-A WASI invocation shim for runner worker shell
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_WASI_SHIM_V0_H
#define SAPLING_WASI_SHIM_V0_H

#include "runner/runner_v0.h"

#include <stdint.h>

#define SAP_WASI_SHIM_V0_REPLY_CAP 4096u
#define SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE 8u

typedef int (*sap_wasi_shim_v0_guest_call)(void *ctx, const SapRunnerMessageV0 *msg,
                                           uint8_t *reply_buf, uint32_t reply_buf_cap,
                                           uint32_t *reply_len_out);

typedef struct
{
    DB *db;
    sap_wasi_shim_v0_guest_call guest_call;
    void *guest_ctx;
    uint64_t next_outbox_seq;
    int emit_outbox_events;
    uint8_t reply_buf[SAP_WASI_SHIM_V0_REPLY_CAP];
} SapWasiShimV0;

int sap_wasi_shim_v0_init(SapWasiShimV0 *shim, DB *db, sap_wasi_shim_v0_guest_call guest_call,
                          void *guest_ctx, uint64_t initial_outbox_seq, int emit_outbox_events);

/* Adapter to pass into runner worker handler callbacks. */
int sap_wasi_shim_v0_runner_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx);

/* Helper to initialize worker shell using this shim as handler context. */
int sap_wasi_shim_v0_worker_init(SapRunnerV0Worker *worker, const SapRunnerV0Config *cfg,
                                 SapWasiShimV0 *shim, uint32_t max_batch);

void sap_wasi_shim_v0_outbox_key_encode(uint64_t seq,
                                        uint8_t out[SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE]);

#endif /* SAPLING_WASI_SHIM_V0_H */
