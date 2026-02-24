/*
 * mailbox_v0.h - phase-C mailbox lease claim/ack/requeue scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_MAILBOX_V0_H
#define SAPLING_RUNNER_MAILBOX_V0_H

#include "runner/runner_v0.h"
#include "runner/lease_v0.h"

#include <stdint.h>

/* Use SAP_RUNNER_LEASE_V0_VALUE_SIZE and SapRunnerLeaseV0 from lease_v0.h */

/* Claim inbox(worker_id,seq) by installing/updating a lease in DBI_LEASES. */
int sap_runner_mailbox_v0_claim(DB *db, uint64_t inbox_worker_id, uint64_t seq,
                                uint64_t claimant_worker_id, int64_t now_ts,
                                int64_t lease_deadline_ts, SapRunnerLeaseV0 *lease_out);

/* Acknowledge and remove inbox+lease, guarded by exact expected lease token. */
int sap_runner_mailbox_v0_ack(DB *db, uint64_t worker_id, uint64_t seq,
                               const SapRunnerLeaseV0 *expected_lease);

/* Requeue claimed message at new_seq, guarded by exact expected lease token. */
int sap_runner_mailbox_v0_requeue(DB *db, uint64_t worker_id, uint64_t seq,
                                  const SapRunnerLeaseV0 *expected_lease, uint64_t new_seq);

#endif /* SAPLING_RUNNER_MAILBOX_V0_H */
