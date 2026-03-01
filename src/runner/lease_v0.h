/*
 * lease_v0.h - General lease management (DBI 3)
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_LEASE_V0_H
#define SAPLING_RUNNER_LEASE_V0_H

#include "sapling/sapling.h"
#include "runner/txstack_v0.h"

#include <stdint.h>

#define SAP_RUNNER_LEASE_V0_VALUE_SIZE 24u

typedef struct
{
    uint64_t owner_worker;
    int64_t deadline_ts;
    uint32_t attempts;
} SapRunnerLeaseV0;

/* --- Staged Operations (for use in atomic blocks) --- */

/* Attempt to acquire a lease.
 * Returns ERR_OK if acquired, ERR_BUSY if held by others, or ERR_INVALID.
 */
int sap_runner_lease_v0_stage_acquire(SapRunnerTxStackV0 *stack, Txn *read_txn, const void *key,
                                      uint32_t key_len, uint64_t owner_worker, int64_t now_ts,
                                      int64_t duration_ms, SapRunnerLeaseV0 *lease_out);

/* Release a lease.
 * Returns ERR_OK if released, ERR_CONFLICT if not owned by claimant, or ERR_INVALID.
 */
int sap_runner_lease_v0_stage_release(SapRunnerTxStackV0 *stack, Txn *read_txn, const void *key,
                                      uint32_t key_len, uint64_t owner_worker);

/* --- Serialization --- */

void sap_runner_lease_v0_encode(const SapRunnerLeaseV0 *lease,
                                uint8_t out[SAP_RUNNER_LEASE_V0_VALUE_SIZE]);
int sap_runner_lease_v0_decode(const uint8_t *raw, uint32_t raw_len, SapRunnerLeaseV0 *lease_out);

#endif /* SAPLING_RUNNER_LEASE_V0_H */
