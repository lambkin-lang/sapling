/*
 * dedupe_v0.h - Exactly-once message deduplication (DBI 5)
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_DEDUPE_V0_H
#define SAPLING_RUNNER_DEDUPE_V0_H

#include "runner/txstack_v0.h"
#include "sapling/sapling.h"

#include <stdint.h>

#define SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE 32u

typedef struct
{
    int accepted;
    int64_t last_seen_ts;
    uint8_t checksum[SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE];
    uint32_t checksum_len;
} SapRunnerDedupeV0;

/* --- Storage Operations --- */

/* Check if a message has already been processed. 
 * Returns SAP_OK if found (and populates dedupe_out), 
 * SAP_NOTFOUND if new, or SAP_ERROR. 
 */
int sap_runner_dedupe_v0_get(Txn *txn, const void *message_id, uint32_t message_id_len,
                             SapRunnerDedupeV0 *dedupe_out);

/* Record message processing metadata. */
int sap_runner_dedupe_v0_put(Txn *txn, const void *message_id, uint32_t message_id_len,
                             const SapRunnerDedupeV0 *dedupe);

/* Record message processing metadata (staged). */
int sap_runner_dedupe_v0_stage_put(SapRunnerTxStackV0 *stack, const void *message_id,
                                   uint32_t message_id_len, const SapRunnerDedupeV0 *dedupe);

/* --- Serialization --- */

#define SAP_RUNNER_DEDUPE_V0_VALUE_SIZE (1u + 8u + 4u + SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE)

void sap_runner_dedupe_v0_encode(const SapRunnerDedupeV0 *dedupe,
                                 uint8_t out[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE]);
int sap_runner_dedupe_v0_decode(const uint8_t *raw, uint32_t raw_len,
                                SapRunnerDedupeV0 *dedupe_out);

#endif /* SAPLING_RUNNER_DEDUPE_V0_H */
