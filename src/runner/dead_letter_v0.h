/*
 * dead_letter_v0.h - phase-C dead-letter move/record helpers
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_DEAD_LETTER_V0_H
#define SAPLING_RUNNER_DEAD_LETTER_V0_H

#include "runner/mailbox_v0.h"

#include <stdint.h>

#define SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE 16u

typedef struct
{
    int32_t failure_rc;
    uint32_t attempts;
    const uint8_t *frame;
    uint32_t frame_len;
} SapRunnerDeadLetterV0Record;

int sap_runner_dead_letter_v0_encode(int32_t failure_rc, uint32_t attempts, const uint8_t *frame,
                                     uint32_t frame_len, uint8_t *dst, uint32_t dst_len,
                                     uint32_t *written_out);

int sap_runner_dead_letter_v0_decode(const uint8_t *raw, uint32_t raw_len,
                                     SapRunnerDeadLetterV0Record *record_out);

/*
 * Move inbox(worker_id,seq) to dead-letter DBI, guarded by exact expected lease.
 * The operation atomically:
 *   1) validates expected lease token
 *   2) writes encoded dead-letter record in DBI 6
 *   3) deletes inbox and lease records
 */
int sap_runner_dead_letter_v0_move(DB *db, uint64_t worker_id, uint64_t seq,
                                   const SapRunnerLeaseV0 *expected_lease, int32_t failure_rc,
                                   uint32_t attempts);

#endif /* SAPLING_RUNNER_DEAD_LETTER_V0_H */
