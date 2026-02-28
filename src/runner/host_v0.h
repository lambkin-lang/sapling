/*
 * host_v0.h - Host API for non-Wasm and Wasm guests in atomic blocks
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_HOST_V0_H
#define SAPLING_RUNNER_HOST_V0_H

#include "runner/txstack_v0.h"
#include "sapling/sapling.h"

#include <stdint.h>

typedef struct
{
    SapRunnerTxStackV0 *stack;
    Txn *read_txn;
    uint64_t worker_id;
    int64_t now_ms;
} SapHostV0;

/* Initialize a host API context for use within an atomic block execution phase. */
void sap_host_v0_init(SapHostV0 *host, SapRunnerTxStackV0 *stack, Txn *read_txn, uint64_t worker_id,
                      int64_t now_ms);

/* --- Data APIs (mapped to txstack staging) --- */

int sap_host_v0_get(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len,
                    const void **val_out, uint32_t *val_len_out);

int sap_host_v0_put(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len,
                    const void *val, uint32_t val_len);

int sap_host_v0_del(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len);

/* --- Intent APIs (mapped to txstack intent push) --- */

/* Emit a message to the outbox. */
int sap_host_v0_emit(SapHostV0 *host, const void *msg, uint32_t msg_len);

/* Arm a timer. */
int sap_host_v0_arm(SapHostV0 *host, int64_t due_ts, const void *msg, uint32_t msg_len);

/* --- Lease APIs (mapped to lease_v0 staging) --- */

int sap_host_v0_lease_acquire(SapHostV0 *host, const void *key, uint32_t key_len,
                              int64_t duration_ms);

int sap_host_v0_lease_release(SapHostV0 *host, const void *key, uint32_t key_len);

#endif /* SAPLING_RUNNER_HOST_V0_H */
