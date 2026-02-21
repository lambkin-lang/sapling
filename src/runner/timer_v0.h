/*
 * timer_v0.h - phase-C timer ingestion and due-drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_TIMER_V0_H
#define SAPLING_RUNNER_TIMER_V0_H

#include "runner/attempt_v0.h"

#include <stdint.h>

#define SAP_RUNNER_TIMER_KEY_V0_SIZE 16u

typedef int (*sap_runner_timer_v0_due_handler)(int64_t due_ts, uint64_t seq, const uint8_t *payload,
                                               uint32_t payload_len, void *ctx);

typedef struct
{
    DB *db;
    uint64_t next_seq;
} SapRunnerTimerV0Publisher;

void sap_runner_timer_v0_key_encode(int64_t due_ts, uint64_t seq,
                                    uint8_t out[SAP_RUNNER_TIMER_KEY_V0_SIZE]);
int sap_runner_timer_v0_key_decode(const uint8_t *key, uint32_t key_len, int64_t *due_ts_out,
                                   uint64_t *seq_out);

int sap_runner_timer_v0_append(DB *db, int64_t due_ts, uint64_t seq, const uint8_t *payload,
                               uint32_t payload_len);

int sap_runner_timer_v0_drain_due(DB *db, int64_t now_ts, uint32_t max_items,
                                  sap_runner_timer_v0_due_handler handler, void *ctx,
                                  uint32_t *processed_out);

int sap_runner_timer_v0_publisher_init(SapRunnerTimerV0Publisher *publisher, DB *db,
                                       uint64_t initial_seq);

/* Adapter for sap_runner_attempt_v0_run intent_sink callback. */
int sap_runner_timer_v0_publish_intent(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                       void *ctx);

#endif /* SAPLING_RUNNER_TIMER_V0_H */
